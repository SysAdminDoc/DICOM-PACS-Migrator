#!/usr/bin/env python3
"""
DICOM PACS Migrator v0.5.0
Bulk DICOM C-STORE migration tool with network auto-discovery,
resume support, post-migration verification, filtering, streaming
migration, decompress fallback, patient ID conflict retry, and audit trail.
Copy-only architecture — source data is NEVER modified or deleted.
"""

import sys, os, subprocess

def _bootstrap():
    """Auto-install dependencies before any imports. Skipped in frozen exe."""
    if getattr(sys, 'frozen', False):
        return
    if sys.version_info < (3, 8):
        print("Python 3.8+ required"); sys.exit(1)
    required = ['numpy', 'PyQt5', 'pydicom', 'pynetdicom', 'Pillow', 'pylibjpeg', 'pylibjpeg-openjpeg', 'pylibjpeg-libjpeg']
    for pkg in required:
        mod = pkg.lower().replace('-', '_')
        try:
            __import__(mod)
        except ImportError:
            for flags in [[], ['--user'], ['--break-system-packages']]:
                try:
                    subprocess.check_call(
                        [sys.executable, '-m', 'pip', 'install', pkg, '-q'] + flags,
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
                    break
                except subprocess.CalledProcessError:
                    continue

_bootstrap()

import json, time, logging, traceback, threading, socket, struct, ipaddress, re, csv
from pathlib import Path
from datetime import datetime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QSpinBox, QProgressBar,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QGroupBox, QGridLayout,
    QFileDialog, QTabWidget, QHeaderView, QSplitter, QFrame,
    QCheckBox, QComboBox, QStatusBar, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QAbstractItemView, QDateEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QSettings, QDate
from PyQt5.QtGui import QFont, QColor, QIcon, QPalette

import pydicom
from pydicom.uid import (
    ExplicitVRLittleEndian, ImplicitVRLittleEndian,
    ExplicitVRBigEndian, DeflatedExplicitVRLittleEndian,
    JPEGBaseline8Bit, JPEGExtended12Bit, JPEGLosslessSV1,
    JPEGLossless, JPEG2000Lossless, JPEG2000,
    RLELossless
)
from pynetdicom import AE, StoragePresentationContexts, evt
from pynetdicom.sop_class import Verification

VERSION = "0.6.0"
APP_NAME = "DICOM PACS Migrator"

DATA_SAFETY_NOTICE = (
    "COPY-ONLY MODE: Source files are opened read-only and are NEVER "
    "modified, moved, or deleted. Data is only copied to the destination "
    "PACS via C-STORE. Original files remain completely untouched."
)

logger = logging.getLogger("DICOMMigrator")
logger.setLevel(logging.DEBUG)

COMMON_DICOM_PORTS = [104, 11112, 4242, 2762, 2575, 8042, 4006, 5678, 3003, 106]

TRANSFER_SYNTAXES = [
    ExplicitVRLittleEndian, ImplicitVRLittleEndian,
    ExplicitVRBigEndian, DeflatedExplicitVRLittleEndian,
    JPEGBaseline8Bit, JPEGExtended12Bit, JPEGLosslessSV1,
    JPEGLossless, JPEG2000Lossless, JPEG2000, RLELossless,
]

# Status code returned by PACS when patient ID conflicts with existing study
CONFLICT_STATUS = 0xFFFB


def resolve_destination_patient(host, port, ae_scu, ae_scp, study_instance_uid, log_fn=None):
    """C-FIND the destination PACS to discover the PatientID already associated
    with a given StudyInstanceUID.  Returns a dict with patient demographics
    {'PatientID': ..., 'PatientName': ..., 'PatientBirthDate': ..., 'PatientSex': ...}
    or None if the query fails or returns no results."""
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

    try:
        ae = AE(ae_title=ae_scu)
        ae.acse_timeout = 10; ae.dimse_timeout = 15; ae.network_timeout = 10
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

        find_assoc = ae.associate(host, port, ae_title=ae_scp)
        if not find_assoc.is_established:
            if log_fn: log_fn("    C-FIND association rejected — cannot resolve patient")
            return None

        query = pydicom.Dataset()
        query.QueryRetrieveLevel = 'STUDY'
        query.StudyInstanceUID = study_instance_uid
        # Request patient demographics back
        query.PatientID = ''
        query.PatientName = ''
        query.PatientBirthDate = ''
        query.PatientSex = ''

        result = None
        responses = find_assoc.send_c_find(query, StudyRootQueryRetrieveInformationModelFind)
        for status, identifier in responses:
            if status and status.Status in (0xFF00, 0xFF01) and identifier:
                pid = str(getattr(identifier, 'PatientID', ''))
                if pid:
                    result = {
                        'PatientID': pid,
                        'PatientName': str(getattr(identifier, 'PatientName', '')),
                        'PatientBirthDate': str(getattr(identifier, 'PatientBirthDate', '')),
                        'PatientSex': str(getattr(identifier, 'PatientSex', '')),
                    }
                    break  # Only need the first match

        find_assoc.release()
        return result
    except Exception as e:
        if log_fn: log_fn(f"    C-FIND resolve failed: {e}")
        return None


def try_send_c_store(assoc, ds, fpath, decompress_fallback=True,
                     conflict_retry=False, conflict_suffix="_MIG", log_fn=None,
                     ae=None, host=None, port=None, ae_scp=None, ae_scu=None,
                     pid_cache=None):
    """Attempt C-STORE. On presentation context rejection, decompress in-memory and retry.
    On 0xFFFB patient ID conflict:
      1. C-FIND the destination to discover the correct PatientID for the study
      2. Remap the in-memory dataset to match (data lands under existing patient)
      3. If C-FIND fails, fall back to appending conflict_suffix (creates duplicate)
    Source files are NEVER modified.
    pid_cache: optional dict {StudyInstanceUID -> resolved_patient_dict} to avoid
    repeated C-FIND queries for the same study.
    Returns (status_value, message, decompressed_flag, conflict_retried_flag, new_assoc_or_None).
    """

    def _do_send(association, dataset):
        """Returns (status, message, was_decompressed, new_assoc_or_None)."""
        try:
            st = association.send_c_store(dataset)
            if st:
                return (st.Status, "", False, None)
            return (None, "No response from SCP", False, None)
        except Exception as e:
            err_msg = str(e)
            if 'presentation context' not in err_msg.lower() or not decompress_fallback:
                return (-1, err_msg, False, None)

            # Presentation context rejected — try decompressing to uncompressed transfer syntax
            try:
                # Only attempt decompress on datasets with actual pixel data
                if not all(hasattr(dataset, attr) for attr in ('Rows', 'Columns', 'BitsAllocated', 'PixelData')):
                    return (-1, "No presentation context (non-pixel object, decompress N/A)", False, None)

                original_tsuid = getattr(dataset.file_meta, 'TransferSyntaxUID', None) if hasattr(dataset, 'file_meta') else None
                ts_name = str(original_tsuid.name) if original_tsuid and hasattr(original_tsuid, 'name') else str(original_tsuid or 'Unknown')

                dataset.decompress()

                # Try sending on the existing association first
                try:
                    st = association.send_c_store(dataset)
                    if st:
                        if log_fn:
                            log_fn(f"  Decompressed {ts_name} -> Explicit VR LE: {os.path.basename(fpath)}")
                        return (st.Status, f"Decompressed from {ts_name}", True, None)
                except Exception:
                    pass  # Association likely dead after presentation context error — fall through

                # Original association is dead — create a fresh one if we have connection details
                if ae and host and port and ae_scp:
                    try:
                        new_assoc = ae.associate(host, port, ae_title=ae_scp)
                        if new_assoc.is_established:
                            st = new_assoc.send_c_store(dataset)
                            if st:
                                if log_fn:
                                    log_fn(f"  Decompressed {ts_name} -> Explicit VR LE (new assoc): {os.path.basename(fpath)}")
                                return (st.Status, f"Decompressed from {ts_name}", True, new_assoc)
                            else:
                                try: new_assoc.release()
                                except: pass
                                return (None, "No response after decompress + new assoc", True, None)
                        else:
                            return (-1, "Decompress OK but new association rejected", True, None)
                    except Exception as e3:
                        return (-1, f"Decompress OK but new assoc failed: {e3}", True, None)

                return (None, "No response after decompress (association dead)", True, None)
            except Exception as decomp_err:
                return (-1, f"No context + decompress failed: {decomp_err}", False, None)

    # First attempt — send as-is
    status_val, msg, was_decompressed, new_assoc = _do_send(assoc, ds)

    # Check for patient ID conflict (0xFFFB) and retry with corrected PatientID
    if status_val == CONFLICT_STATUS and conflict_retry:
        original_pid = str(getattr(ds, 'PatientID', 'N/A'))
        original_name = str(getattr(ds, 'PatientName', 'Unknown'))
        study_uid = str(getattr(ds, 'StudyInstanceUID', ''))

        resolved = None
        resolve_method = "suffix"

        # Step 1: Check cache for previously resolved patient
        if pid_cache is not None and study_uid in pid_cache:
            resolved = pid_cache[study_uid]
            resolve_method = "cached C-FIND"
        # Step 2: C-FIND the destination to find correct PatientID
        elif host and port and ae_scu and ae_scp and study_uid:
            if log_fn:
                log_fn(f"  Patient ID conflict (0xFFFB) for [{original_name}] — querying destination for correct PatientID...")
            resolved = resolve_destination_patient(host, port, ae_scu, ae_scp, study_uid, log_fn=log_fn)
            if resolved:
                resolve_method = "C-FIND"
                # Cache for subsequent images in the same study
                if pid_cache is not None:
                    pid_cache[study_uid] = resolved

        # Step 3: Apply resolved demographics or fall back to suffix
        if resolved and resolved.get('PatientID'):
            new_pid = resolved['PatientID']
            ds.PatientID = new_pid
            # Also align patient demographics so the PACS doesn't reject on name/DOB/sex mismatch
            if resolved.get('PatientName'):
                ds.PatientName = resolved['PatientName']
            if resolved.get('PatientBirthDate'):
                ds.PatientBirthDate = resolved['PatientBirthDate']
            if resolved.get('PatientSex'):
                ds.PatientSex = resolved['PatientSex']

            if log_fn:
                log_fn(f"  Remapped via {resolve_method}: PatientID [{original_pid}] -> [{new_pid}] "
                        f"(destination match)")
        else:
            # C-FIND unavailable or returned nothing — fall back to suffix
            new_pid = f"{original_pid}{conflict_suffix}"
            ds.PatientID = new_pid
            if log_fn:
                log_fn(f"  C-FIND unavailable — suffix fallback: PatientID [{original_pid}] -> [{new_pid}]")

        # Resend with corrected patient demographics
        send_assoc = new_assoc if new_assoc else assoc
        retry_status, retry_msg, retry_decomp, retry_new_assoc = _do_send(send_assoc, ds)

        final_new_assoc = retry_new_assoc or new_assoc
        final_decomp = was_decompressed or retry_decomp
        conflict_detail = f"Conflict resolved ({resolve_method}): PatientID {original_pid} -> {new_pid}"
        if retry_msg:
            conflict_detail += f" ({retry_msg})"

        return (retry_status, conflict_detail, final_decomp, True, final_new_assoc)

    return (status_val, msg, was_decompressed, False, new_assoc)

DARK_STYLE = """
QMainWindow, QWidget { background-color: #1e1e2e; color: #cdd6f4; font-family: 'Segoe UI', 'Consolas', monospace; }
QGroupBox { border: 1px solid #45475a; border-radius: 8px; margin-top: 1.2em; padding: 14px 10px 10px 10px; color: #cdd6f4; font-weight: bold; }
QGroupBox::title { subcontrol-origin: margin; left: 12px; padding: 0 6px; color: #89b4fa; }
QPushButton { background-color: #89b4fa; color: #1e1e2e; border: none; padding: 8px 18px; border-radius: 6px; font-weight: bold; font-size: 13px; }
QPushButton:hover { background-color: #74c7ec; }
QPushButton:pressed { background-color: #89dceb; }
QPushButton:disabled { background-color: #45475a; color: #6c7086; }
QPushButton[danger="true"] { background-color: #f38ba8; }
QPushButton[danger="true"]:hover { background-color: #eba0ac; }
QPushButton[success="true"] { background-color: #a6e3a1; }
QPushButton[success="true"]:hover { background-color: #94e2d5; }
QPushButton[warning="true"] { background-color: #fab387; }
QPushButton[warning="true"]:hover { background-color: #f9e2af; }
QLineEdit, QSpinBox { background-color: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 7px 10px; font-size: 13px; selection-background-color: #89b4fa; selection-color: #1e1e2e; }
QLineEdit:focus, QSpinBox:focus { border-color: #89b4fa; }
QTextEdit { background-color: #11111b; color: #a6adc8; border: 1px solid #313244; border-radius: 6px; padding: 6px; font-family: 'Cascadia Code', 'Consolas', 'Courier New', monospace; font-size: 12px; }
QProgressBar { background-color: #313244; border: none; border-radius: 6px; text-align: center; color: #cdd6f4; font-weight: bold; min-height: 22px; }
QProgressBar::chunk { background-color: #89b4fa; border-radius: 6px; }
QTreeWidget { background-color: #181825; alternate-background-color: #1e1e2e; color: #cdd6f4; border: 1px solid #313244; border-radius: 6px; font-size: 12px; outline: none; }
QTreeWidget::item { padding: 3px 0; }
QTreeWidget::item:selected { background-color: #313244; color: #89b4fa; }
QTreeWidget::item:hover { background-color: #252537; }
QHeaderView::section { background-color: #181825; color: #89b4fa; border: none; border-bottom: 2px solid #313244; padding: 6px 8px; font-weight: bold; font-size: 12px; }
QTabWidget::pane { border: 1px solid #313244; background: #1e1e2e; border-radius: 6px; }
QTabBar::tab { background: #181825; color: #6c7086; padding: 8px 20px; border: none; border-bottom: 2px solid transparent; font-weight: bold; }
QTabBar::tab:selected { color: #89b4fa; border-bottom-color: #89b4fa; background: #1e1e2e; }
QTabBar::tab:hover { color: #cdd6f4; }
QCheckBox { spacing: 8px; color: #cdd6f4; }
QCheckBox::indicator { width: 16px; height: 16px; border-radius: 3px; border: 1px solid #45475a; background: #313244; }
QCheckBox::indicator:checked { background: #89b4fa; border-color: #89b4fa; }
QComboBox { background-color: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 6px 10px; }
QComboBox::drop-down { border: none; width: 24px; }
QComboBox QAbstractItemView { background-color: #1e1e2e; color: #cdd6f4; border: 1px solid #45475a; selection-background-color: #313244; }
QSplitter::handle { background-color: #313244; height: 3px; }
QScrollBar:vertical { background: #181825; width: 10px; border: none; }
QScrollBar::handle:vertical { background: #45475a; border-radius: 5px; min-height: 30px; }
QScrollBar::handle:vertical:hover { background: #585b70; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QScrollBar:horizontal { background: #181825; height: 10px; border: none; }
QScrollBar::handle:horizontal { background: #45475a; border-radius: 5px; min-width: 30px; }
QScrollBar::handle:horizontal:hover { background: #585b70; }
QScrollBar::add-line:horizontal, QScrollBar::sub-line:horizontal { width: 0; }
QStatusBar { background-color: #11111b; color: #6c7086; border-top: 1px solid #313244; font-size: 12px; }
QLabel { color: #cdd6f4; }
QLabel[heading="true"] { font-size: 14px; font-weight: bold; color: #89b4fa; }
QLabel[subtext="true"] { color: #6c7086; font-size: 11px; }
QLabel[stat="true"] { font-size: 22px; font-weight: bold; color: #cdd6f4; }
QFrame[separator="true"] { background-color: #313244; max-height: 1px; }
QTableWidget { background-color: #181825; alternate-background-color: #1e1e2e; color: #cdd6f4; border: 1px solid #313244; border-radius: 6px; gridline-color: #313244; font-size: 12px; outline: none; }
QTableWidget::item { padding: 4px 8px; }
QTableWidget::item:selected { background-color: #313244; color: #89b4fa; }
QTableWidget::item:hover { background-color: #252537; }
QDialog { background-color: #1e1e2e; color: #cdd6f4; }
QDateEdit { background-color: #313244; color: #cdd6f4; border: 1px solid #45475a; border-radius: 4px; padding: 6px 10px; }
QDateEdit::drop-down { border: none; width: 24px; }
"""



# ═══════════════════════════════════════════════════════════════════════════════
# Migration Manifest — Resume Support & Audit Trail
# ═══════════════════════════════════════════════════════════════════════════════
class MigrationManifest:
    """Persistent JSON manifest tracking every file's migration status.
    Enables resume after crash and CSV export for audit."""

    def __init__(self, manifest_path=None, save_to_disk=True):
        self.path = manifest_path
        self.save_to_disk = save_to_disk
        self.records = {}  # sop_instance_uid -> record dict
        self.meta = {
            'created': datetime.now().isoformat(),
            'version': VERSION,
            'source_folder': '',
            'destination': '',
        }

    def set_path_from_folder(self, source_folder):
        safe = re.sub(r'[^\w\-.]', '_', os.path.basename(source_folder.rstrip('/\\')))
        fname = f"migration_manifest_{safe}_{datetime.now().strftime('%Y%m%d')}.json"
        self.path = os.path.join(source_folder, fname)
        self.meta['source_folder'] = source_folder

    def load(self):
        if self.path and os.path.exists(self.path):
            try:
                with open(self.path, 'r') as f:
                    data = json.load(f)
                self.meta = data.get('meta', self.meta)
                self.records = data.get('records', {})
                return True
            except Exception:
                return False
        return False

    def save(self):
        if not self.path or not self.save_to_disk:
            return
        try:
            with open(self.path, 'w') as f:
                json.dump({'meta': self.meta, 'records': self.records}, f, indent=2)
        except Exception:
            pass

    def record_file(self, sop_uid, path, status, message='', **extra):
        self.records[sop_uid] = {
            'path': path,
            'status': status,  # 'sent', 'failed', 'skipped'
            'message': message,
            'timestamp': datetime.now().isoformat(),
            **extra,
        }

    def is_already_sent(self, sop_uid):
        rec = self.records.get(sop_uid)
        return rec is not None and rec.get('status') == 'sent'

    def get_failed(self):
        return {uid: rec for uid, rec in self.records.items() if rec.get('status') == 'failed'}

    def get_sent_count(self):
        return sum(1 for r in self.records.values() if r['status'] == 'sent')

    def get_failed_count(self):
        return sum(1 for r in self.records.values() if r['status'] == 'failed')

    def export_csv(self, csv_path):
        fields = ['sop_instance_uid', 'path', 'status', 'message', 'timestamp',
                   'patient_name', 'patient_id', 'study_date', 'modality']
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for uid, rec in self.records.items():
                row = {'sop_instance_uid': uid, **rec}
                writer.writerow(row)


# ═══════════════════════════════════════════════════════════════════════════════
# Network Utility Functions
# ═══════════════════════════════════════════════════════════════════════════════
def get_local_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        return ip
    except Exception:
        return "127.0.0.1"

def get_local_interfaces():
    interfaces = []
    try:
        import platform
        if platform.system() == 'Windows':
            result = subprocess.run(['ipconfig'], capture_output=True, text=True, timeout=5)
            current_adapter = ""
            for line in result.stdout.splitlines():
                line = line.strip()
                if line and not line.startswith(' ') and ':' in line:
                    current_adapter = line.rstrip(':')
                match = re.search(r'IPv4.*?:\s*(\d+\.\d+\.\d+\.\d+)', line)
                if match:
                    ip = match.group(1)
                    if not ip.startswith('127.'):
                        interfaces.append({'ip': ip, 'adapter': current_adapter})
                mask_match = re.search(r'Subnet Mask.*?:\s*(\d+\.\d+\.\d+\.\d+)', line)
                if mask_match and interfaces:
                    interfaces[-1]['mask'] = mask_match.group(1)
        else:
            result = subprocess.run(['ip', '-4', 'addr'], capture_output=True, text=True, timeout=5)
            for line in result.stdout.splitlines():
                match = re.search(r'inet\s+(\d+\.\d+\.\d+\.\d+)/(\d+)', line)
                if match:
                    ip, cidr = match.group(1), match.group(2)
                    if not ip.startswith('127.'):
                        interfaces.append({'ip': ip, 'cidr': cidr, 'adapter': ''})
    except Exception:
        pass
    return interfaces

def resolve_hostname(ip):
    try: return socket.gethostbyaddr(ip)[0]
    except: return ""

def tcp_port_check(ip, port, timeout=0.5):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(timeout)
        result = s.connect_ex((ip, port)); s.close()
        return result == 0
    except: return False

def dicom_echo_probe(ip, port, ae_scu="DCMPROBE", ae_scp="ANY-SCP", timeout=3):
    try:
        ae = AE(ae_title=ae_scu)
        ae.acse_timeout = timeout; ae.dimse_timeout = timeout; ae.network_timeout = timeout
        ae.add_requested_context(Verification)
        assoc = ae.associate(ip, port, ae_title=ae_scp)
        if assoc.is_established:
            status = assoc.send_c_echo()
            remote_ae = "UNKNOWN"
            impl_class = impl_version = ""
            try:
                acc = assoc.acceptor
                if hasattr(acc, 'ae_title'):
                    raw = acc.ae_title
                    remote_ae = raw.decode('ascii').strip() if isinstance(raw, bytes) else str(raw).strip()
                if hasattr(acc, 'implementation_class_uid'):
                    impl_class = str(acc.implementation_class_uid or "")
                if hasattr(acc, 'implementation_version_name'):
                    raw = acc.implementation_version_name
                    if raw: impl_version = raw.decode('ascii').strip() if isinstance(raw, bytes) else str(raw).strip()
            except: pass
            echo_ok = status is not None and status.Status == 0x0000
            assoc.release()
            return {'ip': ip, 'port': port, 'ae_title': remote_ae,
                    'echo_status': 'Success' if echo_ok else f'0x{status.Status:04X}' if status else 'No Response',
                    'implementation_class': impl_class, 'implementation_version': impl_version,
                    'hostname': '', 'reachable': True, 'is_dicom': True}
        else:
            return {'ip': ip, 'port': port, 'ae_title': ae_scp, 'echo_status': 'Rejected',
                    'implementation_class': '', 'implementation_version': '',
                    'hostname': '', 'reachable': True, 'is_dicom': True}
    except: return None


# ═══════════════════════════════════════════════════════════════════════════════
# Network Discovery Thread
# ═══════════════════════════════════════════════════════════════════════════════
class NetworkDiscoveryThread(QThread):
    progress = pyqtSignal(int, int)
    node_found = pyqtSignal(dict)
    log = pyqtSignal(str)
    finished = pyqtSignal(list)
    phase = pyqtSignal(str)

    def __init__(self, ip_ranges, ports, max_workers=50, tcp_timeout=0.4, dicom_timeout=3):
        super().__init__()
        self.ip_ranges = ip_ranges; self.ports = ports
        self.max_workers = max_workers; self.tcp_timeout = tcp_timeout
        self.dicom_timeout = dicom_timeout; self._cancel = False

    def cancel(self): self._cancel = True

    def run(self):
        discovered = []
        total = len(self.ip_ranges) * len(self.ports)
        self.phase.emit("Phase 1/2: TCP Port Scan")
        self.log.emit(f"Scanning {len(self.ip_ranges)} IPs x {len(self.ports)} ports...")
        open_targets = []; scanned = 0
        def tcp_chk(ip, port): return (ip, port, tcp_port_check(ip, port, self.tcp_timeout))
        with ThreadPoolExecutor(max_workers=self.max_workers) as pool:
            futs = {pool.submit(tcp_chk, ip, p): 1 for ip in self.ip_ranges for p in self.ports if not self._cancel}
            for fut in as_completed(futs):
                if self._cancel: break
                scanned += 1; self.progress.emit(scanned, total)
                try:
                    ip, port, ok = fut.result(timeout=5)
                    if ok: open_targets.append((ip, port)); self.log.emit(f"  TCP open: {ip}:{port}")
                except: pass
        if self._cancel: self.finished.emit([]); return
        self.log.emit(f"Phase 1 done: {len(open_targets)} open ports")
        if not open_targets: self.finished.emit([]); return

        self.phase.emit("Phase 2/2: DICOM C-ECHO Probe")
        probed = 0
        def echo_chk(ip, port):
            for scp in ["ANY-SCP", "ANYSCP", "ORTHANC", "DCMSCP", "PACS"]:
                r = dicom_echo_probe(ip, port, "DCMPROBE", scp, self.dicom_timeout)
                if r: return r
            return None
        with ThreadPoolExecutor(max_workers=min(self.max_workers, 20)) as pool:
            futs = {pool.submit(echo_chk, ip, p): 1 for ip, p in open_targets if not self._cancel}
            for fut in as_completed(futs):
                if self._cancel: break
                probed += 1; self.progress.emit(probed, len(open_targets))
                try:
                    r = fut.result(timeout=15)
                    if r and r.get('is_dicom'):
                        r['hostname'] = resolve_hostname(r['ip'])
                        discovered.append(r); self.node_found.emit(r)
                        self.log.emit(f"  DICOM: {r['ip']}:{r['port']} AE=\"{r['ae_title']}\" [{r['echo_status']}]")
                except: pass
        self.log.emit(f"Discovery complete: {len(discovered)} DICOM nodes found")
        self.finished.emit(discovered)


# ═══════════════════════════════════════════════════════════════════════════════
# Scanner Thread (READ-ONLY)
# ═══════════════════════════════════════════════════════════════════════════════
class ScannerThread(QThread):
    progress = pyqtSignal(int, int)
    current_file = pyqtSignal(str, int, int)  # filename, dicom_count, skipped_count
    finished = pyqtSignal(list)
    error = pyqtSignal(str)
    log = pyqtSignal(str)
    status = pyqtSignal(str)

    def __init__(self, folder_path, recursive=True):
        super().__init__()
        self.folder_path = folder_path; self.recursive = recursive; self._cancel = False

    def cancel(self): self._cancel = True

    def run(self):
        try:
            results = []
            self.log.emit(f"Scanning (READ-ONLY): {self.folder_path}")
            self.status.emit("Enumerating files...")
            root = Path(self.folder_path)

            # Use os.walk instead of glob — yields incrementally on large stores
            all_files = []
            dir_count = 0
            for dirpath, dirnames, filenames in os.walk(str(root)):
                if self._cancel: self.finished.emit(results); return
                dir_count += 1
                for fname in filenames:
                    all_files.append(Path(dirpath) / fname)
                if dir_count % 50 == 0:
                    self.status.emit(f"Enumerating: {len(all_files):,} files in {dir_count:,} folders...")
                    self.current_file.emit(f"Scanning folder: {os.path.basename(dirpath)}", 0, 0)
                if not self.recursive:
                    break  # only top-level folder

            total = len(all_files)
            self.log.emit(f"Found {total:,} files in {dir_count:,} folders, reading DICOM headers...")
            dc = sk = 0
            for i, fpath in enumerate(all_files):
                if self._cancel: self.finished.emit(results); return
                self.progress.emit(i + 1, total)
                # Emit current file for live display
                try:
                    rel = fpath.relative_to(root)
                except ValueError:
                    rel = fpath.name
                self.current_file.emit(str(rel), dc, sk)
                if (i + 1) % 100 == 0:
                    self.status.emit(f"Scanning {i+1:,}/{total:,} | {dc:,} DICOM | {sk:,} skipped")
                try:
                    ds = pydicom.dcmread(str(fpath), stop_before_pixels=True, force=True)
                    if not hasattr(ds, 'SOPClassUID'): sk += 1; continue
                    results.append({
                        'path': str(fpath),
                        'patient_name': str(getattr(ds, 'PatientName', 'Unknown')),
                        'patient_id': str(getattr(ds, 'PatientID', 'N/A')),
                        'study_date': str(getattr(ds, 'StudyDate', '')),
                        'study_desc': str(getattr(ds, 'StudyDescription', '')),
                        'series_desc': str(getattr(ds, 'SeriesDescription', '')),
                        'modality': str(getattr(ds, 'Modality', 'OT')),
                        'sop_class_uid': str(ds.SOPClassUID),
                        'sop_instance_uid': str(getattr(ds, 'SOPInstanceUID', '')),
                        'study_instance_uid': str(getattr(ds, 'StudyInstanceUID', '')),
                        'series_instance_uid': str(getattr(ds, 'SeriesInstanceUID', '')),
                        'transfer_syntax': str(getattr(ds.file_meta, 'TransferSyntaxUID', ImplicitVRLittleEndian)) if hasattr(ds, 'file_meta') else str(ImplicitVRLittleEndian),
                        'file_size': fpath.stat().st_size,
                    })
                    dc += 1
                    if dc % 1000 == 0: self.log.emit(f"  {dc:,} DICOM files parsed...")
                except: sk += 1
            self.log.emit(f"Scan complete: {dc:,} DICOM, {sk:,} skipped (all read-only)")
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(f"Scan failed: {e}\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════════════════════════
# Upload Thread (COPY-ONLY) with manifest integration
# ═══════════════════════════════════════════════════════════════════════════════
class UploadThread(QThread):
    progress = pyqtSignal(int, int)
    file_sent = pyqtSignal(str, bool, str, str)  # path, success, message, sop_uid
    finished = pyqtSignal(int, int, int)
    error = pyqtSignal(str)
    log = pyqtSignal(str)
    status = pyqtSignal(str)
    speed_update = pyqtSignal(float, float)
    conflict_retry_count = pyqtSignal(int)  # running total of conflict retries

    def __init__(self, files, host, port, ae_scu, ae_scp,
                 max_pdu=0, batch_size=50, retry_count=1, manifest=None,
                 decompress_fallback=True, conflict_retry=False, conflict_suffix="_MIG"):
        super().__init__()
        self.files = files; self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.max_pdu = max_pdu; self.batch_size = batch_size
        self.retry_count = retry_count; self.manifest = manifest
        self.decompress_fallback = decompress_fallback
        self.conflict_retry = conflict_retry
        self.conflict_suffix = conflict_suffix
        self.failure_reasons = defaultdict(int)  # reason -> count
        self._conflict_retries = 0
        self._pid_cache = {}  # StudyInstanceUID -> resolved patient dict (C-FIND cache)
        self._cancel = False; self._paused = False
        self._pause_event = threading.Event(); self._pause_event.set()

    def cancel(self): self._cancel = True; self._pause_event.set()
    def pause(self): self._paused = True; self._pause_event.clear()
    def resume(self): self._paused = False; self._pause_event.set()

    def _build_ae(self, sop_classes):
        ae = AE(ae_title=self.ae_scu); ae.maximum_pdu_size = self.max_pdu
        added = set()
        for uid in sop_classes:
            if uid not in added and len(added) < 126:
                ae.add_requested_context(uid, TRANSFER_SYNTAXES); added.add(uid)
        ae.add_requested_context(Verification); return ae

    def run(self):
        total = len(self.files); sent = failed = skipped = 0
        start_time = time.time(); bytes_sent = 0
        self.log.emit(f"{'='*60}")
        self.log.emit(f"COPY-ONLY: {DATA_SAFETY_NOTICE}")
        if self.conflict_retry:
            self.log.emit(f"Patient ID conflict resolution ENABLED (C-FIND remap, suffix fallback: '{self.conflict_suffix}')")
        self.log.emit(f"{'='*60}")
        self.log.emit(f"Copying {total} files to {self.host}:{self.port}")

        file_index = 0
        for batch_start in range(0, total, self.batch_size):
            if self._cancel: break
            batch = self.files[batch_start:batch_start + self.batch_size]
            ae = self._build_ae(list(set(f['sop_class_uid'] for f in batch)))
            for attempt in range(self.retry_count + 1):
                if self._cancel: break
                try:
                    assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
                    if not assoc.is_established:
                        if attempt < self.retry_count:
                            self.log.emit(f"Association failed, retry {attempt+1}..."); time.sleep(2); continue
                        for f in batch:
                            failed += 1; file_index += 1; self.progress.emit(file_index, total)
                            sop = f.get('sop_instance_uid', '')
                            self.file_sent.emit(f['path'], False, "Association failed", sop)
                            if self.manifest: self.manifest.record_file(sop, f['path'], 'failed', 'Association failed', **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                        break

                    for f in batch:
                        self._pause_event.wait()
                        if self._cancel: assoc.release(); break

                        # Reconnect if association died (e.g. timeout during pause)
                        if not assoc.is_established:
                            self.log.emit("Association lost (timeout during pause?) — reconnecting...")
                            try: assoc.release()
                            except: pass
                            try:
                                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
                                if not assoc.is_established:
                                    self.log.emit("Reconnection failed — aborting batch")
                                    for remaining in batch[batch.index(f):]:
                                        failed += 1; file_index += 1; self.progress.emit(file_index, total)
                                        rsop = remaining.get('sop_instance_uid', '')
                                        self.file_sent.emit(remaining['path'], False, "Association lost", rsop)
                                        if self.manifest: self.manifest.record_file(rsop, remaining['path'], 'failed', 'Association lost',
                                            **{k: remaining.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                                    break
                                self.log.emit("Reconnected successfully")
                            except Exception as reconn_err:
                                self.log.emit(f"Reconnection error: {reconn_err}")
                                break

                        file_index += 1
                        fpath = f['path']; sop = f.get('sop_instance_uid', '')

                        # Resume: skip already sent
                        if self.manifest and self.manifest.is_already_sent(sop):
                            skipped += 1; self.progress.emit(file_index, total)
                            self.file_sent.emit(fpath, True, "Already sent (resumed)", sop)
                            continue

                        self.status.emit(f"Copying {file_index}/{total}: {os.path.basename(fpath)}")
                        try:
                            ds = pydicom.dcmread(fpath, force=True)
                            if not hasattr(ds, 'SOPClassUID') or not hasattr(ds, 'SOPInstanceUID'):
                                skipped += 1; self.file_sent.emit(fpath, False, "Missing SOP UIDs", sop)
                                if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', 'Missing SOP UIDs')
                                self.progress.emit(file_index, total); continue

                            sv, detail, was_decompressed, was_conflict_retried, new_assoc = try_send_c_store(
                                assoc, ds, fpath, self.decompress_fallback,
                                self.conflict_retry, self.conflict_suffix,
                                log_fn=self.log.emit,
                                ae=ae, host=self.host, port=self.port,
                                ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                                pid_cache=self._pid_cache)

                            # If a fresh association was created (e.g. after decompress),
                            # switch to it for subsequent sends in this batch
                            if new_assoc is not None:
                                try: assoc.release()
                                except: pass
                                assoc = new_assoc

                            if was_conflict_retried:
                                self._conflict_retries += 1
                                self.conflict_retry_count.emit(self._conflict_retries)

                            if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                                sent += 1; bytes_sent += f.get('file_size', 0)
                                msg = detail if detail else ("Copied" if sv == 0 else f"Pending (0x{sv:04X})")
                                if was_decompressed and not detail: msg = "Decompressed + Copied"
                                self.file_sent.emit(fpath, True, msg, sop)
                                if self.manifest: self.manifest.record_file(sop, fpath, 'sent', msg, **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                            elif sv is not None and sv >= 0:
                                failed += 1; msg = f"Status: 0x{sv:04X}"
                                if detail: msg += f" ({detail})"
                                self.file_sent.emit(fpath, False, msg, sop)
                                self.failure_reasons[f"Status: 0x{sv:04X}"] += 1
                                if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg, **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                            else:
                                failed += 1; msg = detail or "No response"
                                self.file_sent.emit(fpath, False, msg, sop)
                                self.failure_reasons[msg[:80]] += 1
                                if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg)
                        except Exception as e:
                            failed += 1; self.file_sent.emit(fpath, False, str(e), sop)
                            self.failure_reasons[str(e)[:80]] += 1
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', str(e))

                        self.progress.emit(file_index, total)
                        elapsed = time.time() - start_time
                        if elapsed > 0 and (sent+failed+skipped) > 0:
                            self.speed_update.emit((sent+failed+skipped)/elapsed, (bytes_sent/(1024*1024))/elapsed)

                        # Save manifest periodically
                        if self.manifest and file_index % 100 == 0: self.manifest.save()

                    try: assoc.release()
                    except: pass
                    break
                except Exception as e:
                    if attempt < self.retry_count: self.log.emit(f"Error: {e}, retrying..."); time.sleep(2)
                    else:
                        self.log.emit(f"Batch failed: {e}")
                        for f in batch[max(0,file_index-batch_start):]:
                            failed += 1; file_index += 1; self.progress.emit(file_index, total)
                            self.file_sent.emit(f['path'], False, str(e), f.get('sop_instance_uid',''))
                        break

        if self.manifest: self.manifest.save()
        elapsed = time.time() - start_time
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"Migration Complete (COPY-ONLY)")
        self.log.emit(f"  Copied: {sent} | Failed: {failed} | Skipped: {skipped}")
        if self._conflict_retries:
            self.log.emit(f"  Patient ID conflicts resolved: {self._conflict_retries}")
        self.log.emit(f"  Time: {elapsed:.1f}s | Source: 0 modified, 0 deleted")
        if self.failure_reasons:
            self.log.emit(f"\nFailure Summary:")
            for reason, count in sorted(self.failure_reasons.items(), key=lambda x: -x[1]):
                self.log.emit(f"  [{count:,}x] {reason}")
        self.log.emit(f"{'='*60}")
        self.finished.emit(sent, failed, skipped)


# ═══════════════════════════════════════════════════════════════════════════════
# Streaming Migration Thread — Walk + Read + Send in one pass, per-directory
# No pre-scan or enumeration required. Starts sending immediately.
# ═══════════════════════════════════════════════════════════════════════════════
class StreamingMigrationThread(QThread):
    file_sent = pyqtSignal(str, bool, str, str)  # path, success, message, sop_uid
    finished = pyqtSignal(int, int, int)          # sent, failed, skipped
    error = pyqtSignal(str)
    log = pyqtSignal(str)
    status = pyqtSignal(str)
    speed_update = pyqtSignal(float, float)
    folder_status = pyqtSignal(str, int, int, int, int)  # folder, dirs_done, sent, failed, skipped
    conflict_retry_count = pyqtSignal(int)

    def __init__(self, root_folder, host, port, ae_scu, ae_scp,
                 max_pdu=0, batch_size=50, retry_count=1, manifest=None,
                 recursive=True, decompress_fallback=True,
                 conflict_retry=False, conflict_suffix="_MIG"):
        super().__init__()
        self.root_folder = root_folder
        self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.max_pdu = max_pdu; self.batch_size = batch_size
        self.retry_count = retry_count; self.manifest = manifest
        self.recursive = recursive; self.decompress_fallback = decompress_fallback
        self.conflict_retry = conflict_retry
        self.conflict_suffix = conflict_suffix
        self.failure_reasons = defaultdict(int)
        self._conflict_retries = 0
        self._pid_cache = {}  # StudyInstanceUID -> resolved patient dict (C-FIND cache)
        self._cancel = False; self._paused = False
        self._pause_event = threading.Event(); self._pause_event.set()

    def cancel(self): self._cancel = True; self._pause_event.set()
    def pause(self): self._paused = True; self._pause_event.clear()
    def resume(self): self._paused = False; self._pause_event.set()

    def _build_ae(self, sop_classes):
        ae = AE(ae_title=self.ae_scu); ae.maximum_pdu_size = self.max_pdu
        added = set()
        for uid in sop_classes:
            if uid not in added and len(added) < 126:
                ae.add_requested_context(uid, TRANSFER_SYNTAXES); added.add(uid)
        ae.add_requested_context(Verification); return ae

    def _send_batch(self, batch, sent, failed, skipped, bytes_sent, start_time):
        """Send a batch of parsed DICOM file dicts. Returns updated counters."""
        sop_classes = list(set(f['sop_class_uid'] for f in batch))
        ae = self._build_ae(sop_classes)

        for attempt in range(self.retry_count + 1):
            if self._cancel: return sent, failed, skipped, bytes_sent
            try:
                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
                if not assoc.is_established:
                    if attempt < self.retry_count:
                        self.log.emit(f"Association failed, retry {attempt+1}..."); time.sleep(2); continue
                    for f in batch:
                        failed += 1; sop = f.get('sop_instance_uid', '')
                        self.file_sent.emit(f['path'], False, "Association failed", sop)
                        if self.manifest: self.manifest.record_file(sop, f['path'], 'failed', 'Association failed',
                            **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                    return sent, failed, skipped, bytes_sent

                for f in batch:
                    self._pause_event.wait()
                    if self._cancel: assoc.release(); return sent, failed, skipped, bytes_sent

                    # Reconnect if association died (e.g. timeout during pause)
                    if not assoc.is_established:
                        self.log.emit("Association lost (timeout during pause?) — reconnecting...")
                        try: assoc.release()
                        except: pass
                        try:
                            assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
                            if not assoc.is_established:
                                self.log.emit("Reconnection failed — aborting batch")
                                for remaining in batch[batch.index(f):]:
                                    failed += 1; rsop = remaining.get('sop_instance_uid', '')
                                    self.file_sent.emit(remaining['path'], False, "Association lost", rsop)
                                    if self.manifest: self.manifest.record_file(rsop, remaining['path'], 'failed', 'Association lost',
                                        **{k: remaining.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                                return sent, failed, skipped, bytes_sent
                            self.log.emit("Reconnected successfully")
                        except Exception as reconn_err:
                            self.log.emit(f"Reconnection error: {reconn_err}")
                            return sent, failed, skipped, bytes_sent

                    fpath = f['path']; sop = f.get('sop_instance_uid', '')

                    # Resume: skip already sent
                    if self.manifest and self.manifest.is_already_sent(sop):
                        skipped += 1
                        self.file_sent.emit(fpath, True, "Already sent (resumed)", sop)
                        continue

                    try:
                        ds = pydicom.dcmread(fpath, force=True)
                        if not hasattr(ds, 'SOPClassUID') or not hasattr(ds, 'SOPInstanceUID'):
                            skipped += 1; self.file_sent.emit(fpath, False, "Missing SOP UIDs", sop)
                            if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', 'Missing SOP UIDs')
                            continue

                        sv, detail, was_decompressed, was_conflict_retried, new_assoc = try_send_c_store(
                            assoc, ds, fpath, self.decompress_fallback,
                            self.conflict_retry, self.conflict_suffix,
                            log_fn=self.log.emit,
                            ae=ae, host=self.host, port=self.port,
                            ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                            pid_cache=self._pid_cache)

                        # If a fresh association was created (e.g. after decompress),
                        # switch to it for subsequent sends in this batch
                        if new_assoc is not None:
                            try: assoc.release()
                            except: pass
                            assoc = new_assoc

                        if was_conflict_retried:
                            self._conflict_retries += 1
                            self.conflict_retry_count.emit(self._conflict_retries)

                        if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                            sent += 1; bytes_sent += f.get('file_size', 0)
                            msg = detail if detail else ("Copied" if sv == 0 else f"Pending (0x{sv:04X})")
                            if was_decompressed and not detail: msg = "Decompressed + Copied"
                            self.file_sent.emit(fpath, True, msg, sop)
                            if self.manifest: self.manifest.record_file(sop, fpath, 'sent', msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                        elif sv is not None and sv >= 0:
                            failed += 1; msg = f"Status: 0x{sv:04X}"
                            if detail: msg += f" ({detail})"
                            self.file_sent.emit(fpath, False, msg, sop)
                            self.failure_reasons[f"Status: 0x{sv:04X}"] += 1
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','modality')})
                        else:
                            failed += 1; msg = detail or "No response"
                            self.file_sent.emit(fpath, False, msg, sop)
                            self.failure_reasons[msg[:80]] += 1
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg)
                    except Exception as e:
                        failed += 1; self.file_sent.emit(fpath, False, str(e), sop)
                        self.failure_reasons[str(e)[:80]] += 1
                        if self.manifest: self.manifest.record_file(sop, fpath, 'failed', str(e))

                    elapsed = time.time() - start_time
                    total_proc = sent + failed + skipped
                    if elapsed > 0 and total_proc > 0:
                        self.speed_update.emit(total_proc / elapsed, (bytes_sent / (1024*1024)) / elapsed)

                try: assoc.release()
                except: pass
                break
            except Exception as e:
                if attempt < self.retry_count:
                    self.log.emit(f"Error: {e}, retrying..."); time.sleep(2)
                else:
                    self.log.emit(f"Batch failed: {e}")
                    for f in batch:
                        failed += 1
                        self.file_sent.emit(f['path'], False, str(e), f.get('sop_instance_uid', ''))
                    break

        return sent, failed, skipped, bytes_sent

    def run(self):
        sent = failed = skipped = 0; bytes_sent = 0
        start_time = time.time(); dirs_done = 0
        root = self.root_folder

        self.log.emit(f"{'='*60}")
        self.log.emit(f"STREAMING MIGRATION (COPY-ONLY)")
        self.log.emit(f"{DATA_SAFETY_NOTICE}")
        if self.conflict_retry:
            self.log.emit(f"Patient ID conflict resolution ENABLED (C-FIND remap, suffix fallback: '{self.conflict_suffix}')")
        self.log.emit(f"{'='*60}")
        self.log.emit(f"Source: {root}")
        self.log.emit(f"Destination: {self.host}:{self.port}")
        self.log.emit(f"Walking directory tree and sending immediately...")

        for dirpath, dirnames, filenames in os.walk(root):
            if self._cancel: break
            if not self.recursive and dirpath != root:
                continue

            if not filenames:
                continue

            # Show which folder we're processing
            try:
                rel_dir = os.path.relpath(dirpath, root)
            except ValueError:
                rel_dir = dirpath
            if rel_dir == '.': rel_dir = os.path.basename(root)

            dirs_done += 1
            self.folder_status.emit(rel_dir, dirs_done, sent, failed, skipped)
            self.status.emit(f"Folder {dirs_done}: {rel_dir} ({len(filenames)} files)")

            # Parse DICOM headers for this directory (read-only, headers only)
            dir_files = []
            for fname in filenames:
                if self._cancel: break
                fpath = os.path.join(dirpath, fname)
                try:
                    ds = pydicom.dcmread(fpath, stop_before_pixels=True, force=True)
                    if not hasattr(ds, 'SOPClassUID'):
                        continue
                    dir_files.append({
                        'path': fpath,
                        'patient_name': str(getattr(ds, 'PatientName', 'Unknown')),
                        'patient_id': str(getattr(ds, 'PatientID', 'N/A')),
                        'study_date': str(getattr(ds, 'StudyDate', '')),
                        'study_desc': str(getattr(ds, 'StudyDescription', '')),
                        'modality': str(getattr(ds, 'Modality', 'OT')),
                        'sop_class_uid': str(ds.SOPClassUID),
                        'sop_instance_uid': str(getattr(ds, 'SOPInstanceUID', '')),
                        'study_instance_uid': str(getattr(ds, 'StudyInstanceUID', '')),
                        'series_instance_uid': str(getattr(ds, 'SeriesInstanceUID', '')),
                        'file_size': os.path.getsize(fpath),
                    })
                except:
                    pass  # not a DICOM file

            if not dir_files:
                continue

            if dirs_done <= 3 or dirs_done % 25 == 0:
                self.log.emit(f"  [{dirs_done}] {rel_dir}: {len(dir_files)} DICOM files")

            # Send this directory's files in batches
            for batch_start in range(0, len(dir_files), self.batch_size):
                if self._cancel: break
                batch = dir_files[batch_start:batch_start + self.batch_size]
                sent, failed, skipped, bytes_sent = self._send_batch(
                    batch, sent, failed, skipped, bytes_sent, start_time)

            # Save manifest periodically
            if self.manifest and dirs_done % 10 == 0:
                self.manifest.save()

        if self.manifest: self.manifest.save()
        elapsed = time.time() - start_time
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"Streaming Migration Complete (COPY-ONLY)")
        self.log.emit(f"  Folders: {dirs_done} | Copied: {sent:,} | Failed: {failed:,} | Skipped: {skipped:,}")
        if self._conflict_retries:
            self.log.emit(f"  Patient ID conflicts resolved: {self._conflict_retries:,}")
        self.log.emit(f"  Time: {elapsed:.1f}s | Source: 0 modified, 0 deleted")
        if bytes_sent > 0:
            self.log.emit(f"  Data: {bytes_sent/(1024**3):.2f} GB | Avg: {(bytes_sent/(1024**2))/elapsed:.1f} MB/s" if elapsed > 0 else "")
        if self.failure_reasons:
            self.log.emit(f"\nFailure Summary:")
            for reason, count in sorted(self.failure_reasons.items(), key=lambda x: -x[1]):
                self.log.emit(f"  [{count:,}x] {reason}")
        self.log.emit(f"{'='*60}")
        self.finished.emit(sent, failed, skipped)
# ═══════════════════════════════════════════════════════════════════════════════
class EchoThread(QThread):
    result = pyqtSignal(bool, str)
    def __init__(self, host, port, ae_scu, ae_scp):
        super().__init__()
        self.host = host; self.port = port; self.ae_scu = ae_scu; self.ae_scp = ae_scp
    def run(self):
        try:
            ae = AE(ae_title=self.ae_scu)
            ae.acse_timeout = 5; ae.dimse_timeout = 5; ae.network_timeout = 5
            ae.add_requested_context(Verification)
            assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
            if assoc.is_established:
                st = assoc.send_c_echo(); assoc.release()
                if st and st.Status == 0x0000: self.result.emit(True, f"C-ECHO success at {self.host}:{self.port}")
                else: self.result.emit(False, f"C-ECHO: 0x{st.Status:04X}" if st else "No response")
            else: self.result.emit(False, f"Rejected by {self.host}:{self.port}")
        except Exception as e: self.result.emit(False, f"Connection failed: {e}")


# ═══════════════════════════════════════════════════════════════════════════════
# Post-Migration Verification Thread (C-FIND)
# ═══════════════════════════════════════════════════════════════════════════════
class VerifyThread(QThread):
    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    study_verified = pyqtSignal(str, int, int, bool)  # study_uid, expected, found, match
    finished = pyqtSignal(int, int, int)  # total_studies, matched, mismatched

    def __init__(self, study_file_counts, host, port, ae_scu, ae_scp):
        super().__init__()
        self.study_file_counts = study_file_counts  # {study_uid: {'expected': N, 'patient': '', 'desc': ''}}
        self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp

    def run(self):
        from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

        total = len(self.study_file_counts)
        matched = mismatched = 0
        self.log.emit(f"Verifying {total} studies on {self.host}:{self.port}...")

        ae = AE(ae_title=self.ae_scu)
        ae.acse_timeout = 10; ae.dimse_timeout = 30; ae.network_timeout = 10
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

        try:
            assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
            if not assoc.is_established:
                self.log.emit("C-FIND association failed — cannot verify")
                self.finished.emit(total, 0, total); return

            for idx, (study_uid, info) in enumerate(self.study_file_counts.items()):
                self.progress.emit(idx + 1, total)
                expected = info['expected']

                # Query at IMAGE level to count instances in this study
                ds = pydicom.Dataset()
                ds.QueryRetrieveLevel = 'IMAGE'
                ds.StudyInstanceUID = study_uid
                ds.SOPInstanceUID = ''

                found = 0
                responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
                for status, identifier in responses:
                    if status and status.Status in (0xFF00, 0xFF01):
                        found += 1

                is_match = found >= expected
                if is_match: matched += 1
                else: mismatched += 1

                self.study_verified.emit(study_uid, expected, found, is_match)
                self.log.emit(
                    f"  {'OK' if is_match else 'MISMATCH'}: "
                    f"{info.get('patient', '?')} / {info.get('desc', '?')} — "
                    f"expected {expected}, found {found}"
                )

            assoc.release()
        except Exception as e:
            self.log.emit(f"Verification error: {e}")

        self.log.emit(f"\nVerification: {matched}/{total} studies matched, {mismatched} mismatched")
        self.finished.emit(total, matched, mismatched)


# ═══════════════════════════════════════════════════════════════════════════════
# Connection Assistant Dialog
# ═══════════════════════════════════════════════════════════════════════════════
class ConnectionAssistantDialog(QDialog):
    node_selected = pyqtSignal(dict)
    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Connection Assistant"); self.setMinimumSize(920, 640); self.resize(1020, 700)
        self.discovered_nodes = []; self.discovery_thread = None; self._build_ui(); self._detect_network()

    def _build_ui(self):
        layout = QVBoxLayout(self); layout.setSpacing(10)
        h = QLabel("DICOM Network Scanner"); h.setStyleSheet("font-size: 16px; font-weight: bold; color: #89b4fa;"); layout.addWidget(h)
        d = QLabel("Scans your network for DICOM servers via TCP + C-ECHO. Click 'Use This' to auto-populate settings.")
        d.setWordWrap(True); d.setProperty("subtext", True); layout.addWidget(d)

        cg = QGroupBox("Scan Range"); cl = QGridLayout(cg)
        cl.addWidget(QLabel("Subnet:"), 0, 0)
        self.subnet_input = QLineEdit(); self.subnet_input.setPlaceholderText("192.168.1.0/24"); cl.addWidget(self.subnet_input, 0, 1, 1, 2)
        cl.addWidget(QLabel("Additional IPs:"), 1, 0)
        self.custom_ips = QLineEdit(); self.custom_ips.setPlaceholderText("10.0.0.5, 172.16.0.100"); cl.addWidget(self.custom_ips, 1, 1, 1, 2)
        cl.addWidget(QLabel("Ports:"), 2, 0)
        self.ports_input = QLineEdit(); self.ports_input.setText(", ".join(str(p) for p in COMMON_DICOM_PORTS)); cl.addWidget(self.ports_input, 2, 1, 1, 2)
        cl.addWidget(QLabel("Threads:"), 3, 0)
        self.threads_spin = QSpinBox(); self.threads_spin.setRange(1, 200); self.threads_spin.setValue(60); cl.addWidget(self.threads_spin, 3, 1)
        layout.addWidget(cg)

        ctrl = QHBoxLayout()
        self.scan_btn = QPushButton("Start Scan"); self.scan_btn.clicked.connect(self._start_scan); ctrl.addWidget(self.scan_btn)
        self.cancel_btn = QPushButton("Cancel"); self.cancel_btn.setProperty("danger", True); self.cancel_btn.setEnabled(False)
        self.cancel_btn.clicked.connect(self._cancel_scan); ctrl.addWidget(self.cancel_btn)
        ctrl.addStretch()
        self.phase_lbl = QLabel(""); self.phase_lbl.setStyleSheet("color: #f9e2af; font-weight: bold;"); ctrl.addWidget(self.phase_lbl)
        layout.addLayout(ctrl)
        self.prog = QProgressBar(); self.prog.setVisible(False); layout.addWidget(self.prog)

        self.table = QTableWidget(); self.table.setColumnCount(7)
        self.table.setHorizontalHeaderLabels(["IP", "Port", "AE Title", "Hostname", "Status", "Impl", ""])
        self.table.setSelectionBehavior(QAbstractItemView.SelectRows); self.table.setAlternatingRowColors(True)
        hh = self.table.horizontalHeader(); hh.setStretchLastSection(False)
        for i, m in [(0, QHeaderView.ResizeToContents), (1, QHeaderView.ResizeToContents), (2, QHeaderView.ResizeToContents),
                      (3, QHeaderView.Stretch), (4, QHeaderView.ResizeToContents), (5, QHeaderView.ResizeToContents), (6, QHeaderView.ResizeToContents)]:
            hh.setSectionResizeMode(i, m)
        self.table.verticalHeader().setVisible(False); self.table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.table, 1)
        self.disc_log = QTextEdit(); self.disc_log.setReadOnly(True); self.disc_log.setMaximumHeight(120); layout.addWidget(self.disc_log)
        bl = QHBoxLayout(); bl.addStretch(); cb = QPushButton("Close"); cb.clicked.connect(self.close); bl.addWidget(cb); layout.addLayout(bl)

    def _detect_network(self):
        ip = get_local_ip()
        if ip != "127.0.0.1":
            parts = ip.split('.'); self.subnet_input.setText(f"{parts[0]}.{parts[1]}.{parts[2]}.0/24")
            self._dl(f"Local IP: {ip}")

    def _dl(self, msg): self.disc_log.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def _parse_ips(self):
        ips = set()
        st = self.subnet_input.text().strip()
        if st:
            try:
                if '/' in st:
                    for ip in ipaddress.IPv4Network(st, strict=False).hosts(): ips.add(str(ip))
                elif '-' in st.split('.')[-1]:
                    base = '.'.join(st.split('.')[:-1]); s, e = st.split('.')[-1].split('-')
                    for i in range(int(s), int(e)+1): ips.add(f"{base}.{i}")
                else: ips.add(st)
            except Exception as e: self._dl(f"Bad subnet: {e}")
        for part in self.custom_ips.text().split(','):
            p = part.strip()
            if not p: continue
            try: ipaddress.IPv4Address(p); ips.add(p)
            except:
                try: ips.add(socket.gethostbyname(p)); self._dl(f"Resolved {p}")
                except: pass
        ips.discard(get_local_ip()); return sorted(ips)

    def _parse_ports(self):
        return sorted(set(int(p.strip()) for p in self.ports_input.text().split(',') if p.strip().isdigit() and 1 <= int(p.strip()) <= 65535)) or COMMON_DICOM_PORTS

    def _start_scan(self):
        ips = self._parse_ips(); ports = self._parse_ports()
        if not ips: self._dl("No valid IPs"); return
        self.table.setRowCount(0); self.discovered_nodes.clear()
        self.scan_btn.setEnabled(False); self.cancel_btn.setEnabled(True); self.prog.setVisible(True)
        self.discovery_thread = NetworkDiscoveryThread(ips, ports, self.threads_spin.value())
        self.discovery_thread.progress.connect(lambda c,t: (self.prog.setMaximum(t), self.prog.setValue(c)))
        self.discovery_thread.node_found.connect(self._on_node)
        self.discovery_thread.log.connect(self._dl)
        self.discovery_thread.phase.connect(self.phase_lbl.setText)
        self.discovery_thread.finished.connect(self._on_done)
        self.discovery_thread.start()

    def _cancel_scan(self):
        if self.discovery_thread: self.discovery_thread.cancel()

    def _on_node(self, node):
        self.discovered_nodes.append(node); r = self.table.rowCount(); self.table.insertRow(r)
        items = [QTableWidgetItem(node['ip']), QTableWidgetItem(str(node['port'])), QTableWidgetItem(node['ae_title']),
                 QTableWidgetItem(node['hostname'] or '-'), QTableWidgetItem(node['echo_status']),
                 QTableWidgetItem(node['implementation_version'] or '-')]
        if 'Success' in node['echo_status']: items[4].setForeground(QColor("#a6e3a1"))
        elif 'Rejected' in node['echo_status']: items[4].setForeground(QColor("#fab387"))
        else: items[4].setForeground(QColor("#f38ba8"))
        for c, it in enumerate(items): self.table.setItem(r, c, it)
        btn = QPushButton("Use This"); btn.setProperty("success", True); btn.setStyleSheet("padding: 4px 12px; font-size: 11px;")
        btn.clicked.connect(lambda _, n=node: (self.node_selected.emit(n), self.close()))
        self.table.setCellWidget(r, 6, btn)

    def _on_done(self, nodes):
        self.scan_btn.setEnabled(True); self.cancel_btn.setEnabled(False); self.prog.setVisible(False); self.phase_lbl.setText("")


# ═══════════════════════════════════════════════════════════════════════════════
# Main Window
# ═══════════════════════════════════════════════════════════════════════════════
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"{APP_NAME} v{VERSION}")
        self.setMinimumSize(1100, 750); self.resize(1280, 850)
        self.settings = QSettings("DICOMMigrator", "DICOMMigrator")
        self.dicom_files = []; self.manifest = MigrationManifest()
        self.scanner_thread = self.upload_thread = self.verify_thread = self.streaming_thread = None
        self.scan_start_time = self.upload_start_time = None
        self._sent = self._failed = self._skipped = 0
        self._conflict_retries = 0
        self._upload_results = []  # (path, success, message, sop_uid) for retry
        self._streaming_mode = False
        self._build_ui(); self._load_settings()

    def _build_ui(self):
        central = QWidget(); self.setCentralWidget(central)
        ml = QVBoxLayout(central); ml.setContentsMargins(16, 12, 16, 8); ml.setSpacing(10)

        header = QHBoxLayout()
        t = QLabel(f"DICOM PACS Migrator"); t.setStyleSheet("font-size: 18px; font-weight: bold; color: #89b4fa;"); header.addWidget(t)
        header.addStretch()
        safety = QLabel("COPY-ONLY MODE"); safety.setStyleSheet("background-color: #1a3a1a; color: #a6e3a1; padding: 4px 12px; border-radius: 4px; font-weight: bold; font-size: 11px; border: 1px solid #2a5a2a;")
        safety.setToolTip(DATA_SAFETY_NOTICE); header.addWidget(safety)
        v = QLabel(f"v{VERSION}"); v.setStyleSheet("margin-left: 8px; color: #6c7086; font-size: 11px;"); header.addWidget(v)
        ml.addLayout(header)

        banner = QLabel(f"{DATA_SAFETY_NOTICE}"); banner.setWordWrap(True)
        banner.setStyleSheet("background-color: #11251a; color: #94e2d5; padding: 8px 12px; border-radius: 6px; font-size: 11px; border: 1px solid #1a3a2a;")
        ml.addWidget(banner)

        self.tabs = QTabWidget(); ml.addWidget(self.tabs, 1)
        self.tabs.addTab(self._build_config_tab(), "Configuration")
        self.tabs.addTab(self._build_browser_tab(), "File Browser")
        self.tabs.addTab(self._build_upload_tab(), "Upload")
        self.tabs.addTab(self._build_verify_tab(), "Verify")
        self.tabs.addTab(self._build_log_tab(), "Log")
        self.statusBar().showMessage("Ready - Copy-Only Mode Active")

    # ─── Config Tab ───────────────────────────────────────────────────────
    def _build_config_tab(self):
        w = QWidget(); layout = QVBoxLayout(w); layout.setSpacing(12)

        sg = QGroupBox("Source Folder (Read-Only Access)"); sl = QGridLayout(sg)
        sl.addWidget(QLabel("DICOM Folder:"), 0, 0)
        self.folder_input = QLineEdit(); self.folder_input.setPlaceholderText("Path to DICOM image folder...")
        sl.addWidget(self.folder_input, 0, 1)
        browse = QPushButton("Browse"); browse.clicked.connect(self._browse_folder); sl.addWidget(browse, 0, 2)
        self.recursive_check = QCheckBox("Scan subfolders recursively"); self.recursive_check.setChecked(True)
        sl.addWidget(self.recursive_check, 1, 0, 1, 2)
        self.scan_btn = QPushButton("Scan for DICOM Files"); self.scan_btn.setStyleSheet("font-size: 14px; padding: 10px 24px;")
        self.scan_btn.clicked.connect(self._start_scan); sl.addWidget(self.scan_btn, 2, 0, 1, 2)
        self.stream_btn = QPushButton("Stream Migrate Entire Store")
        self.stream_btn.setStyleSheet("background-color: #a6e3a1; color: #1e1e2e; font-size: 14px; padding: 10px 24px; font-weight: bold;")
        self.stream_btn.setToolTip("Walk + Read + Send per-folder in one pass. No pre-scan needed.\nStarts sending immediately — ideal for large image stores.")
        self.stream_btn.clicked.connect(self._start_streaming); sl.addWidget(self.stream_btn, 2, 2)
        self.scan_progress = QProgressBar(); self.scan_progress.setVisible(False); sl.addWidget(self.scan_progress, 3, 0, 1, 3)

        # Live scan activity display
        self.scan_activity_lbl = QLabel(""); self.scan_activity_lbl.setStyleSheet("color: #89b4fa; font-family: 'Consolas', monospace; font-size: 11px;")
        self.scan_activity_lbl.setWordWrap(True); sl.addWidget(self.scan_activity_lbl, 4, 0, 1, 3)
        self.scan_stats_lbl = QLabel(""); self.scan_stats_lbl.setStyleSheet("color: #a6e3a1; font-weight: bold; font-size: 12px;")
        sl.addWidget(self.scan_stats_lbl, 5, 0, 1, 3)

        # Resume info
        self.resume_label = QLabel(""); self.resume_label.setStyleSheet("color: #f9e2af; font-size: 11px;")
        sl.addWidget(self.resume_label, 6, 0, 1, 3)
        layout.addWidget(sg)

        dg = QGroupBox("Destination PACS Server"); dl = QGridLayout(dg)
        assist = QPushButton("Connection Assistant - Auto-Discover DICOM Nodes")
        assist.setStyleSheet("background-color: #cba6f7; color: #1e1e2e; font-size: 13px; padding: 10px 20px; font-weight: bold;")
        assist.clicked.connect(self._open_assistant); dl.addWidget(assist, 0, 0, 1, 4)
        dl.addWidget(QLabel("Host/IP:"), 1, 0)
        self.host_input = QLineEdit(); self.host_input.setPlaceholderText("192.168.1.100"); dl.addWidget(self.host_input, 1, 1)
        dl.addWidget(QLabel("Port:"), 1, 2)
        self.port_input = QSpinBox(); self.port_input.setRange(1, 65535); self.port_input.setValue(104); self.port_input.setMinimumWidth(100); dl.addWidget(self.port_input, 1, 3)
        dl.addWidget(QLabel("SCU AE:"), 2, 0)
        self.ae_scu = QLineEdit("DICOM_MIGRATOR"); self.ae_scu.setMaxLength(16); dl.addWidget(self.ae_scu, 2, 1)
        dl.addWidget(QLabel("SCP AE:"), 2, 2)
        self.ae_scp = QLineEdit("ANY-SCP"); self.ae_scp.setMaxLength(16); dl.addWidget(self.ae_scp, 2, 3)
        dl.addWidget(QLabel("Hostname:"), 3, 0)
        self.hostname_lbl = QLabel("-"); self.hostname_lbl.setStyleSheet("color: #6c7086;"); dl.addWidget(self.hostname_lbl, 3, 1)
        dl.addWidget(QLabel("Impl:"), 3, 2)
        self.impl_lbl = QLabel("-"); self.impl_lbl.setStyleSheet("color: #6c7086;"); dl.addWidget(self.impl_lbl, 3, 3)
        er = QHBoxLayout()
        self.echo_btn = QPushButton("C-ECHO Verify"); self.echo_btn.setProperty("warning", True); self.echo_btn.clicked.connect(self._run_echo)
        er.addWidget(self.echo_btn); self.echo_status = QLabel(""); er.addWidget(self.echo_status, 1)
        dl.addLayout(er, 4, 0, 1, 4); layout.addWidget(dg)

        ag = QGroupBox("Advanced"); al = QGridLayout(ag)
        al.addWidget(QLabel("Batch:"), 0, 0)
        self.batch_spin = QSpinBox(); self.batch_spin.setRange(1, 500); self.batch_spin.setValue(50); al.addWidget(self.batch_spin, 0, 1)
        al.addWidget(QLabel("Retries:"), 0, 2)
        self.retry_spin = QSpinBox(); self.retry_spin.setRange(0, 10); self.retry_spin.setValue(2); al.addWidget(self.retry_spin, 0, 3)
        al.addWidget(QLabel("Max PDU:"), 1, 0)
        self.pdu_combo = QComboBox(); self.pdu_combo.addItems(["0 (Unlimited)", "16384", "32768", "65536", "131072"]); al.addWidget(self.pdu_combo, 1, 1)
        self.manifest_check = QCheckBox("Save resume manifest to source folder (enables crash recovery)")
        self.manifest_check.setChecked(True)
        self.manifest_check.setToolTip("When disabled, no files are written to the DICOM source folder.\nResume and retry still work within the current session, but not across restarts.\nCSV export is always available regardless of this setting.")
        al.addWidget(self.manifest_check, 2, 0, 1, 4)
        self.decompress_check = QCheckBox("Decompress before sending if destination rejects compressed syntax")
        self.decompress_check.setChecked(True)
        self.decompress_check.setToolTip("When a PACS rejects JPEG/JPEG2000/RLE compressed files,\nautomatically decompress to Explicit VR Little Endian in memory and retry.\nSource files are never modified — decompression is in-memory only.")
        al.addWidget(self.decompress_check, 3, 0, 1, 4)

        # Patient ID conflict retry
        conflict_row = QHBoxLayout()
        self.conflict_retry_check = QCheckBox("Auto-resolve patient ID conflicts (0xFFFB) via C-FIND + remap")
        self.conflict_retry_check.setChecked(True)
        self.conflict_retry_check.setToolTip(
            "When the destination PACS rejects a study due to patient ID mismatch\n"
            "(status 0xFFFB), automatically query the destination via C-FIND to\n"
            "discover the correct PatientID for that study, then remap the incoming\n"
            "data to match. This puts images under the existing patient record\n"
            "with zero duplicates.\n\n"
            "If C-FIND is unavailable, falls back to appending a suffix (creates\n"
            "a duplicate patient that the PACS admin can merge later).\n\n"
            "Source files are never modified — all remapping is in-memory only.")
        conflict_row.addWidget(self.conflict_retry_check)
        conflict_row.addWidget(QLabel("Fallback suffix:"))
        self.conflict_suffix_input = QLineEdit("_MIG")
        self.conflict_suffix_input.setMaxLength(16)
        self.conflict_suffix_input.setMaximumWidth(120)
        self.conflict_suffix_input.setToolTip(
            "Only used when C-FIND cannot resolve the correct PatientID.\n"
            "Suffix appended to PatientID as a last resort.\n"
            "Example: PatientID '00023' becomes '00023_MIG'\n"
            "The PACS admin can then merge the duplicate.")
        conflict_row.addWidget(self.conflict_suffix_input)
        conflict_row.addStretch()
        al.addLayout(conflict_row, 4, 0, 1, 4)

        layout.addWidget(ag); layout.addStretch(); return w

    # ─── Browser Tab with Filtering ───────────────────────────────────────
    def _build_browser_tab(self):
        w = QWidget(); layout = QVBoxLayout(w)

        # Stats
        stats = QHBoxLayout()
        self.stat_patients = self._stat("Patients", "0"); self.stat_studies = self._stat("Studies", "0")
        self.stat_series = self._stat("Series", "0"); self.stat_files = self._stat("Files", "0")
        self.stat_size = self._stat("Total Size", "0 MB"); self.stat_selected = self._stat("Selected", "0")
        for s in [self.stat_patients, self.stat_studies, self.stat_series, self.stat_files, self.stat_size, self.stat_selected]:
            stats.addWidget(s)
        layout.addLayout(stats)

        # Filter controls
        fg = QGroupBox("Filters"); fl = QHBoxLayout(fg)
        fl.addWidget(QLabel("Patient:"))
        self.filter_patient = QLineEdit(); self.filter_patient.setPlaceholderText("Search patient name/ID...")
        self.filter_patient.textChanged.connect(self._apply_filters); fl.addWidget(self.filter_patient)
        fl.addWidget(QLabel("Modality:"))
        self.filter_modality = QComboBox(); self.filter_modality.addItem("All Modalities")
        self.filter_modality.currentIndexChanged.connect(self._apply_filters); fl.addWidget(self.filter_modality)
        fl.addWidget(QLabel("Date From:"))
        self.filter_date_from = QDateEdit(); self.filter_date_from.setCalendarPopup(True)
        self.filter_date_from.setDate(QDate(2000, 1, 1)); self.filter_date_from.setDisplayFormat("yyyy-MM-dd")
        self.filter_date_from.dateChanged.connect(self._apply_filters); fl.addWidget(self.filter_date_from)
        fl.addWidget(QLabel("To:"))
        self.filter_date_to = QDateEdit(); self.filter_date_to.setCalendarPopup(True)
        self.filter_date_to.setDate(QDate.currentDate()); self.filter_date_to.setDisplayFormat("yyyy-MM-dd")
        self.filter_date_to.dateChanged.connect(self._apply_filters); fl.addWidget(self.filter_date_to)
        reset = QPushButton("Reset"); reset.clicked.connect(self._reset_filters); fl.addWidget(reset)
        layout.addWidget(fg)

        # Select all / none
        sel_row = QHBoxLayout()
        sa = QPushButton("Select All"); sa.clicked.connect(lambda: self._set_all_checked(True)); sel_row.addWidget(sa)
        sn = QPushButton("Select None"); sn.clicked.connect(lambda: self._set_all_checked(False)); sel_row.addWidget(sn)
        sel_row.addStretch()
        self.selected_count_lbl = QLabel("0 files selected for upload"); self.selected_count_lbl.setStyleSheet("color: #89b4fa; font-weight: bold;")
        sel_row.addWidget(self.selected_count_lbl)
        layout.addLayout(sel_row)

        self.file_tree = QTreeWidget()
        self.file_tree.setHeaderLabels(["Patient / Study / Series", "Modality", "Files", "Date", "Description"])
        self.file_tree.setAlternatingRowColors(True); self.file_tree.setRootIsDecorated(True); self.file_tree.setAnimated(True)
        h = self.file_tree.header(); h.setStretchLastSection(True)
        h.setSectionResizeMode(0, QHeaderView.Stretch)
        for i in [1,2,3]: h.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        self.file_tree.itemChanged.connect(self._on_tree_check_changed)
        layout.addWidget(self.file_tree, 1); return w

    def _stat(self, label, value):
        f = QFrame(); f.setStyleSheet("QFrame { background-color: #181825; border: 1px solid #313244; border-radius: 8px; padding: 8px; }")
        fl = QVBoxLayout(f); fl.setContentsMargins(12, 8, 12, 8); fl.setSpacing(2)
        v = QLabel(value); v.setAlignment(Qt.AlignCenter); v.setProperty("stat", True); fl.addWidget(v)
        l = QLabel(label); l.setAlignment(Qt.AlignCenter); l.setProperty("subtext", True); fl.addWidget(l)
        return f

    # ─── Upload Tab ────────────────────────────────────────────────────────
    def _build_upload_tab(self):
        w = QWidget(); layout = QVBoxLayout(w)
        sr = QLabel("COPY-ONLY: Source files will not be modified or deleted during upload")
        sr.setStyleSheet("background-color: #11251a; color: #94e2d5; padding: 6px 12px; border-radius: 4px; font-size: 11px; border: 1px solid #1a3a2a;")
        layout.addWidget(sr)

        ctrl = QHBoxLayout()
        self.upload_btn = QPushButton("Start Copy to Destination"); self.upload_btn.setStyleSheet("font-size: 14px; padding: 10px 28px;")
        self.upload_btn.clicked.connect(self._start_upload); ctrl.addWidget(self.upload_btn)
        self.retry_btn = QPushButton("Retry Failed"); self.retry_btn.setProperty("warning", True)
        self.retry_btn.setEnabled(False); self.retry_btn.clicked.connect(self._retry_failed); ctrl.addWidget(self.retry_btn)
        self.pause_btn = QPushButton("Pause"); self.pause_btn.setProperty("warning", True)
        self.pause_btn.setEnabled(False); self.pause_btn.clicked.connect(self._toggle_pause); ctrl.addWidget(self.pause_btn)
        self.cancel_btn = QPushButton("Cancel"); self.cancel_btn.setProperty("danger", True)
        self.cancel_btn.setEnabled(False); self.cancel_btn.clicked.connect(self._cancel_upload); ctrl.addWidget(self.cancel_btn)
        ctrl.addStretch()

        # Export buttons
        self.export_csv_btn = QPushButton("Export CSV Manifest"); self.export_csv_btn.setEnabled(False)
        self.export_csv_btn.clicked.connect(self._export_csv); ctrl.addWidget(self.export_csv_btn)
        layout.addLayout(ctrl)

        pg = QGroupBox("Progress"); pl = QVBoxLayout(pg)
        self.upload_progress = QProgressBar(); self.upload_progress.setMinimumHeight(28); pl.addWidget(self.upload_progress)
        # Streaming folder status
        self.stream_folder_lbl = QLabel(""); self.stream_folder_lbl.setStyleSheet("color: #89b4fa; font-family: 'Consolas', monospace; font-size: 11px;")
        self.stream_folder_lbl.setWordWrap(True); self.stream_folder_lbl.setVisible(False); pl.addWidget(self.stream_folder_lbl)
        ir = QHBoxLayout()
        self.upload_count_lbl = QLabel("0 / 0 files"); ir.addWidget(self.upload_count_lbl); ir.addStretch()
        self.upload_speed_lbl = QLabel(""); ir.addWidget(self.upload_speed_lbl); ir.addStretch()
        self.upload_eta_lbl = QLabel(""); ir.addWidget(self.upload_eta_lbl); pl.addLayout(ir)
        rr = QHBoxLayout()
        self.sent_label = QLabel("Copied: 0"); self.sent_label.setStyleSheet("color: #a6e3a1; font-weight: bold; font-size: 14px;"); rr.addWidget(self.sent_label)
        self.failed_label = QLabel("Failed: 0"); self.failed_label.setStyleSheet("color: #f38ba8; font-weight: bold; font-size: 14px;"); rr.addWidget(self.failed_label)
        self.skipped_label = QLabel("Skipped: 0"); self.skipped_label.setStyleSheet("color: #fab387; font-weight: bold; font-size: 14px;"); rr.addWidget(self.skipped_label)
        self.conflict_label = QLabel("Conflicts: 0"); self.conflict_label.setStyleSheet("color: #cba6f7; font-weight: bold; font-size: 14px;"); rr.addWidget(self.conflict_label)
        rr.addStretch()
        self.source_safe_lbl = QLabel("Source: 0 modified, 0 deleted"); self.source_safe_lbl.setStyleSheet("color: #94e2d5; font-weight: bold; font-size: 12px;")
        rr.addWidget(self.source_safe_lbl); pl.addLayout(rr); layout.addWidget(pg)

        # Results table with error details
        self.upload_table = QTableWidget(); self.upload_table.setColumnCount(4)
        self.upload_table.setHorizontalHeaderLabels(["File", "Status", "Detail", "SOP UID"])
        self.upload_table.setAlternatingRowColors(True)
        uh = self.upload_table.horizontalHeader(); uh.setStretchLastSection(True)
        uh.setSectionResizeMode(0, QHeaderView.Stretch); uh.setSectionResizeMode(1, QHeaderView.ResizeToContents)
        uh.setSectionResizeMode(2, QHeaderView.Stretch)
        self.upload_table.verticalHeader().setVisible(False); self.upload_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.upload_table, 1); return w

    # ─── Verify Tab ────────────────────────────────────────────────────────
    def _build_verify_tab(self):
        w = QWidget(); layout = QVBoxLayout(w)
        desc = QLabel("Post-migration verification — queries destination PACS via C-FIND to confirm studies arrived with correct file counts.")
        desc.setWordWrap(True); desc.setProperty("subtext", True); layout.addWidget(desc)

        ctrl = QHBoxLayout()
        self.verify_btn = QPushButton("Verify Migration"); self.verify_btn.setStyleSheet("font-size: 14px; padding: 10px 24px;")
        self.verify_btn.clicked.connect(self._start_verify); ctrl.addWidget(self.verify_btn)
        ctrl.addStretch()
        self.verify_status_lbl = QLabel(""); self.verify_status_lbl.setStyleSheet("font-weight: bold;"); ctrl.addWidget(self.verify_status_lbl)
        layout.addLayout(ctrl)
        self.verify_progress = QProgressBar(); self.verify_progress.setVisible(False); layout.addWidget(self.verify_progress)

        self.verify_table = QTableWidget(); self.verify_table.setColumnCount(5)
        self.verify_table.setHorizontalHeaderLabels(["Patient", "Study Description", "Expected", "Found", "Status"])
        self.verify_table.setAlternatingRowColors(True)
        vh = self.verify_table.horizontalHeader(); vh.setStretchLastSection(True)
        vh.setSectionResizeMode(0, QHeaderView.Stretch); vh.setSectionResizeMode(1, QHeaderView.Stretch)
        for i in [2,3,4]: vh.setSectionResizeMode(i, QHeaderView.ResizeToContents)
        self.verify_table.verticalHeader().setVisible(False); self.verify_table.setEditTriggers(QAbstractItemView.NoEditTriggers)
        layout.addWidget(self.verify_table, 1); return w

    # ─── Log Tab ───────────────────────────────────────────────────────────
    def _build_log_tab(self):
        w = QWidget(); layout = QVBoxLayout(w)
        br = QHBoxLayout()
        cb = QPushButton("Clear Log"); cb.clicked.connect(lambda: self.log_output.clear()); br.addWidget(cb)
        eb = QPushButton("Export Log"); eb.clicked.connect(self._export_log); br.addWidget(eb); br.addStretch()
        layout.addLayout(br)
        self.log_output = QTextEdit(); self.log_output.setReadOnly(True); layout.addWidget(self.log_output, 1); return w

    # ─── Actions ──────────────────────────────────────────────────────────
    def _browse_folder(self):
        f = QFileDialog.getExistingDirectory(self, "Select DICOM Folder", self.folder_input.text() or "")
        if f: self.folder_input.setText(f)

    def _log(self, msg):
        self.log_output.append(f"[{datetime.now().strftime('%H:%M:%S')}] {msg}")

    def _open_assistant(self):
        d = ConnectionAssistantDialog(self)
        d.node_selected.connect(self._on_node_selected); d.exec_()

    def _on_node_selected(self, node):
        self.host_input.setText(node['ip']); self.port_input.setValue(node['port'])
        if node['ae_title'] and node['ae_title'] != 'UNKNOWN': self.ae_scp.setText(node['ae_title'])
        self.hostname_lbl.setText(node.get('hostname') or '-'); self.hostname_lbl.setStyleSheet("color: #a6e3a1;")
        self.impl_lbl.setText(node.get('implementation_version') or '-'); self.impl_lbl.setStyleSheet("color: #a6e3a1;")
        self._log(f"Auto-populated: {node['ip']}:{node['port']} AE=\"{node['ae_title']}\"")
        self.echo_status.setText(f"Discovered - {node['echo_status']}"); self.echo_status.setStyleSheet("color: #a6e3a1;")

    def _start_scan(self):
        folder = self.folder_input.text().strip()
        if not folder or not os.path.isdir(folder): self._log("Invalid folder path"); return
        self.dicom_files.clear(); self.file_tree.clear()
        self.scan_btn.setEnabled(False); self.scan_progress.setVisible(True); self.scan_progress.setValue(0)
        self.scan_activity_lbl.setText("Enumerating files..."); self.scan_stats_lbl.setText("")
        self.scan_start_time = time.time()

        # Load or create manifest for resume support (only if enabled)
        manifest_enabled = self.manifest_check.isChecked()
        self.manifest = MigrationManifest(save_to_disk=manifest_enabled)
        if manifest_enabled:
            self.manifest.set_path_from_folder(folder)
            if self.manifest.load():
                sc = self.manifest.get_sent_count()
                self.resume_label.setText(f"Resume manifest found: {sc} files already sent. These will be skipped automatically.")
                self._log(f"Loaded manifest: {sc} previously sent files will be skipped")
            else:
                self.resume_label.setText("")
        else:
            self.resume_label.setText("Manifest disabled — no files written to source folder")

        self.scanner_thread = ScannerThread(folder, self.recursive_check.isChecked())
        self.scanner_thread.progress.connect(self._on_scan_progress)
        self.scanner_thread.current_file.connect(self._on_scan_file)
        self.scanner_thread.log.connect(self._log)
        self.scanner_thread.finished.connect(self._on_scan_complete)
        self.scanner_thread.error.connect(lambda e: self._log(f"ERROR: {e}"))
        self.scanner_thread.status.connect(self.statusBar().showMessage)
        self.scanner_thread.start()

    def _on_scan_file(self, filename, dc, sk):
        self.scan_activity_lbl.setText(f"Reading: {filename}")
        self.scan_stats_lbl.setText(f"DICOM: {dc:,}  |  Skipped: {sk:,}")

    def _on_scan_progress(self, c, t):
        if t > 0: self.scan_progress.setMaximum(t); self.scan_progress.setValue(c)

    def _on_scan_complete(self, files):
        self.dicom_files = files
        self.scan_btn.setEnabled(True); self.scan_progress.setVisible(False)
        elapsed = time.time() - self.scan_start_time if self.scan_start_time else 0
        self._log(f"Scan: {elapsed:.1f}s, {len(files):,} DICOM files (read-only)")
        self.scan_activity_lbl.setText(f"Scan complete in {elapsed:.1f}s")
        self.scan_stats_lbl.setText(f"{len(files):,} DICOM files found")

        # Mark resume status
        if self.manifest.records:
            already = sum(1 for f in files if self.manifest.is_already_sent(f.get('sop_instance_uid', '')))
            remaining = len(files) - already
            self.resume_label.setText(f"Resume: {already} already sent, {remaining} remaining")

        # Populate modality filter
        modalities = sorted(set(f['modality'] for f in files))
        self.filter_modality.blockSignals(True); self.filter_modality.clear()
        self.filter_modality.addItem("All Modalities")
        for m in modalities: self.filter_modality.addItem(m)
        self.filter_modality.blockSignals(False)

        self._populate_tree(); self._update_stats()
        if files: self.tabs.setCurrentIndex(1)

    def _populate_tree(self):
        self.file_tree.blockSignals(True); self.file_tree.clear()
        patients = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
        for f in self.dicom_files:
            patients[f"{f['patient_name']} [{f['patient_id']}]"][f['study_instance_uid']][f['series_instance_uid']].append(f)

        for pname, studies in sorted(patients.items()):
            fc = sum(len(fl) for sr in studies.values() for fl in sr.values())
            pi = QTreeWidgetItem([pname, "", str(fc), "", ""])
            pi.setFlags(pi.flags() | Qt.ItemIsUserCheckable); pi.setCheckState(0, Qt.Checked)
            pi.setForeground(0, QColor("#89b4fa")); pi.setFont(0, QFont("Segoe UI", 11, QFont.Bold))
            pi.setData(0, Qt.UserRole, 'patient')
            self.file_tree.addTopLevelItem(pi)

            for suid, sdict in studies.items():
                fs = next(iter(sdict.values()))[0]
                sd = fs['study_date']
                if sd and len(sd) == 8: sd = f"{sd[:4]}-{sd[4:6]}-{sd[6:]}"
                si = QTreeWidgetItem([f"{fs['study_desc'] or 'No Description'}", fs['modality'],
                    str(sum(len(fl) for fl in sdict.values())), sd, suid[:40]+"..."])
                si.setFlags(si.flags() | Qt.ItemIsUserCheckable); si.setCheckState(0, Qt.Checked)
                si.setForeground(0, QColor("#a6e3a1")); si.setData(0, Qt.UserRole, 'study')
                si.setData(0, Qt.UserRole + 1, suid)
                pi.addChild(si)

                for sruid, files_list in sdict.items():
                    sri = QTreeWidgetItem([f"  {files_list[0]['series_desc'] or 'No Series Desc'}",
                        files_list[0]['modality'], str(len(files_list)), "", sruid[:40]+"..."])
                    sri.setFlags(sri.flags() | Qt.ItemIsUserCheckable); sri.setCheckState(0, Qt.Checked)
                    sri.setForeground(0, QColor("#f9e2af")); sri.setData(0, Qt.UserRole, 'series')
                    sri.setData(0, Qt.UserRole + 1, sruid)
                    si.addChild(sri)
            pi.setExpanded(True)
        self.file_tree.blockSignals(False)
        self._update_selected_count()

    def _on_tree_check_changed(self, item, column):
        if column != 0: return
        self.file_tree.blockSignals(True)
        state = item.checkState(0)
        # Propagate down
        for i in range(item.childCount()):
            child = item.child(i); child.setCheckState(0, state)
            for j in range(child.childCount()):
                child.child(j).setCheckState(0, state)
        # Propagate up
        parent = item.parent()
        if parent:
            all_checked = all(parent.child(i).checkState(0) == Qt.Checked for i in range(parent.childCount()))
            any_checked = any(parent.child(i).checkState(0) != Qt.Unchecked for i in range(parent.childCount()))
            parent.setCheckState(0, Qt.Checked if all_checked else Qt.PartiallyChecked if any_checked else Qt.Unchecked)
            gp = parent.parent()
            if gp:
                all_c = all(gp.child(i).checkState(0) == Qt.Checked for i in range(gp.childCount()))
                any_c = any(gp.child(i).checkState(0) != Qt.Unchecked for i in range(gp.childCount()))
                gp.setCheckState(0, Qt.Checked if all_c else Qt.PartiallyChecked if any_c else Qt.Unchecked)
        self.file_tree.blockSignals(False)
        self._update_selected_count()

    def _get_selected_files(self):
        """Get files matching checked series in tree."""
        selected_series = set()
        for pi in range(self.file_tree.topLevelItemCount()):
            patient = self.file_tree.topLevelItem(pi)
            for si in range(patient.childCount()):
                study = patient.child(si)
                for sri in range(study.childCount()):
                    series = study.child(sri)
                    if series.checkState(0) != Qt.Unchecked:
                        uid = series.data(0, Qt.UserRole + 1)
                        if uid: selected_series.add(uid)
        return [f for f in self.dicom_files if f['series_instance_uid'] in selected_series]

    def _update_selected_count(self):
        count = len(self._get_selected_files())
        self.selected_count_lbl.setText(f"{count} files selected for upload")
        self._set_stat(self.stat_selected, str(count))

    def _set_all_checked(self, checked):
        self.file_tree.blockSignals(True)
        state = Qt.Checked if checked else Qt.Unchecked
        for pi in range(self.file_tree.topLevelItemCount()):
            p = self.file_tree.topLevelItem(pi); p.setCheckState(0, state)
            for si in range(p.childCount()):
                s = p.child(si); s.setCheckState(0, state)
                for sri in range(s.childCount()):
                    s.child(sri).setCheckState(0, state)
        self.file_tree.blockSignals(False)
        self._update_selected_count()

    def _apply_filters(self):
        patient_filter = self.filter_patient.text().strip().lower()
        modality_filter = self.filter_modality.currentText()
        date_from = self.filter_date_from.date().toString("yyyyMMdd")
        date_to = self.filter_date_to.date().toString("yyyyMMdd")

        for pi in range(self.file_tree.topLevelItemCount()):
            patient = self.file_tree.topLevelItem(pi)
            pname = patient.text(0).lower()
            patient_match = not patient_filter or patient_filter in pname
            any_study_visible = False

            for si in range(patient.childCount()):
                study = patient.child(si)
                mod = study.text(1); date_str = study.text(3).replace('-', '')
                mod_match = modality_filter == "All Modalities" or mod == modality_filter
                date_match = True
                if date_str: date_match = date_from <= date_str <= date_to
                visible = patient_match and mod_match and date_match
                study.setHidden(not visible)
                if visible: any_study_visible = True
                for sri in range(study.childCount()):
                    study.child(sri).setHidden(not visible)

            patient.setHidden(not any_study_visible)

    def _reset_filters(self):
        self.filter_patient.clear()
        self.filter_modality.setCurrentIndex(0)
        self.filter_date_from.setDate(QDate(2000, 1, 1))
        self.filter_date_to.setDate(QDate.currentDate())
        for pi in range(self.file_tree.topLevelItemCount()):
            p = self.file_tree.topLevelItem(pi); p.setHidden(False)
            for si in range(p.childCount()):
                s = p.child(si); s.setHidden(False)
                for sri in range(s.childCount()): s.child(sri).setHidden(False)

    def _update_stats(self):
        files = self.dicom_files
        def _set(card, val):
            for c in card.findChildren(QLabel):
                if c.property("stat"): c.setText(str(val)); break
        _set(self.stat_patients, len(set(f['patient_id'] for f in files)))
        _set(self.stat_studies, len(set(f['study_instance_uid'] for f in files)))
        _set(self.stat_series, len(set(f['series_instance_uid'] for f in files)))
        _set(self.stat_files, len(files))
        ts = sum(f['file_size'] for f in files)
        _set(self.stat_size, f"{ts/(1024**3):.1f} GB" if ts > 1024**3 else f"{ts/(1024**2):.1f} MB")

    def _set_stat(self, card, val):
        for c in card.findChildren(QLabel):
            if c.property("stat"): c.setText(str(val)); break

    def _run_echo(self):
        host = self.host_input.text().strip()
        if not host: self.echo_status.setText("Enter host"); return
        self.echo_btn.setEnabled(False); self.echo_status.setText("Testing..."); self.echo_status.setStyleSheet("color: #f9e2af;")
        self.echo_thread = EchoThread(host, self.port_input.value(), self.ae_scu.text().strip() or "DICOM_MIGRATOR", self.ae_scp.text().strip() or "ANY-SCP")
        self.echo_thread.result.connect(self._on_echo); self.echo_thread.start()

    def _on_echo(self, ok, msg):
        self.echo_btn.setEnabled(True)
        self.echo_status.setText(msg); self.echo_status.setStyleSheet(f"color: {'#a6e3a1' if ok else '#f38ba8'};")
        self._log(f"C-ECHO: {msg}")

    def _start_upload(self, files_override=None):
        files_to_send = files_override or self._get_selected_files()
        if not files_to_send: self._log("No files selected. Check selections in File Browser."); return
        host = self.host_input.text().strip()
        if not host: self._log("Enter host"); self.tabs.setCurrentIndex(0); return
        self._save_settings()

        self.manifest.meta['destination'] = f"{host}:{self.port_input.value()}"
        self.upload_table.setRowCount(0); self.upload_progress.setValue(0)
        self.upload_progress.setMaximum(len(files_to_send))
        self._sent = self._failed = self._skipped = self._conflict_retries = 0; self._upload_results = []
        self._streaming_mode = False
        self.sent_label.setText("Copied: 0"); self.failed_label.setText("Failed: 0")
        self.skipped_label.setText("Skipped: 0"); self.conflict_label.setText("Conflicts: 0")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted")
        self.upload_speed_lbl.setText(""); self.upload_eta_lbl.setText("")

        self.upload_btn.setEnabled(False); self.retry_btn.setEnabled(False)
        self.pause_btn.setEnabled(True); self.cancel_btn.setEnabled(True)
        self.export_csv_btn.setEnabled(False)
        self.upload_start_time = time.time()

        self.upload_thread = UploadThread(
            files_to_send, host, self.port_input.value(),
            self.ae_scu.text().strip() or "DICOM_MIGRATOR",
            self.ae_scp.text().strip() or "ANY-SCP",
            int(self.pdu_combo.currentText().split(" ")[0]),
            self.batch_spin.value(), self.retry_spin.value(),
            manifest=self.manifest,
            decompress_fallback=self.decompress_check.isChecked(),
            conflict_retry=self.conflict_retry_check.isChecked(),
            conflict_suffix=self.conflict_suffix_input.text().strip() or "_MIG")
        self.upload_thread.progress.connect(self._on_upload_progress)
        self.upload_thread.file_sent.connect(self._on_file_sent)
        self.upload_thread.finished.connect(self._on_upload_complete)
        self.upload_thread.error.connect(lambda e: self._log(f"ERROR: {e}"))
        self.upload_thread.log.connect(self._log)
        self.upload_thread.status.connect(self.statusBar().showMessage)
        self.upload_thread.speed_update.connect(self._on_speed)
        self.upload_thread.conflict_retry_count.connect(self._on_conflict_count)
        self.upload_thread.start()
        self.tabs.setCurrentIndex(2)
        self.stream_folder_lbl.setVisible(False)

    def _start_streaming(self):
        """Stream migrate: walk + read + send per-directory, no pre-scan."""
        folder = self.folder_input.text().strip()
        if not folder or not os.path.isdir(folder):
            self._log("Enter a valid DICOM folder path"); return
        host = self.host_input.text().strip()
        if not host:
            self._log("Enter a destination host/IP"); return
        self._save_settings()

        # Set up manifest
        manifest_enabled = self.manifest_check.isChecked()
        self.manifest = MigrationManifest(save_to_disk=manifest_enabled)
        if manifest_enabled:
            self.manifest.set_path_from_folder(folder)
            if self.manifest.load():
                sc = self.manifest.get_sent_count()
                self._log(f"Loaded manifest: {sc:,} previously sent files will be skipped")
        self.manifest.meta['destination'] = f"{host}:{self.port_input.value()}"

        # Reset upload UI
        self.upload_table.setRowCount(0)
        self.upload_progress.setRange(0, 0)  # Indeterminate pulsing bar
        self._sent = self._failed = self._skipped = self._conflict_retries = 0; self._upload_results = []
        self._streaming_mode = True
        self.sent_label.setText("Copied: 0"); self.failed_label.setText("Failed: 0")
        self.skipped_label.setText("Skipped: 0"); self.conflict_label.setText("Conflicts: 0")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted")
        self.upload_speed_lbl.setText(""); self.upload_eta_lbl.setText("")
        self.upload_count_lbl.setText("Streaming...")
        self.stream_folder_lbl.setVisible(True); self.stream_folder_lbl.setText("Starting...")

        self.upload_btn.setEnabled(False); self.stream_btn.setEnabled(False)
        self.scan_btn.setEnabled(False); self.retry_btn.setEnabled(False)
        self.pause_btn.setEnabled(True); self.cancel_btn.setEnabled(True)
        self.export_csv_btn.setEnabled(False)
        self.upload_start_time = time.time()

        self.streaming_thread = StreamingMigrationThread(
            folder, host, self.port_input.value(),
            self.ae_scu.text().strip() or "DICOM_MIGRATOR",
            self.ae_scp.text().strip() or "ANY-SCP",
            int(self.pdu_combo.currentText().split(" ")[0]),
            self.batch_spin.value(), self.retry_spin.value(),
            manifest=self.manifest,
            recursive=self.recursive_check.isChecked(),
            decompress_fallback=self.decompress_check.isChecked(),
            conflict_retry=self.conflict_retry_check.isChecked(),
            conflict_suffix=self.conflict_suffix_input.text().strip() or "_MIG")
        self.streaming_thread.file_sent.connect(self._on_file_sent)
        self.streaming_thread.finished.connect(self._on_streaming_complete)
        self.streaming_thread.error.connect(lambda e: self._log(f"ERROR: {e}"))
        self.streaming_thread.log.connect(self._log)
        self.streaming_thread.status.connect(self.statusBar().showMessage)
        self.streaming_thread.speed_update.connect(self._on_speed)
        self.streaming_thread.folder_status.connect(self._on_folder_status)
        self.streaming_thread.conflict_retry_count.connect(self._on_conflict_count)
        self.streaming_thread.start()
        self.tabs.setCurrentIndex(2)

    def _on_conflict_count(self, count):
        self._conflict_retries = count
        self.conflict_label.setText(f"Conflicts: {count}")

    def _on_folder_status(self, folder, dirs_done, sent, failed, skipped):
        self.stream_folder_lbl.setText(f"Folder: {folder}")
        total = sent + failed + skipped
        self.upload_count_lbl.setText(f"{total:,} files processed | {dirs_done:,} folders")
        elapsed = time.time() - self.upload_start_time if self.upload_start_time else 0
        if elapsed > 60:
            self.upload_eta_lbl.setText(f"Elapsed: {elapsed/3600:.1f}h" if elapsed > 3600 else f"Elapsed: {elapsed/60:.0f}m {elapsed%60:.0f}s")

    def _on_streaming_complete(self, sent, failed, skipped):
        self.upload_btn.setEnabled(True); self.stream_btn.setEnabled(True); self.scan_btn.setEnabled(True)
        self.pause_btn.setEnabled(False); self.cancel_btn.setEnabled(False)
        self.export_csv_btn.setEnabled(True)
        self.upload_progress.setRange(0, 1); self.upload_progress.setValue(1)  # Full bar
        self._streaming_mode = False
        self.stream_folder_lbl.setText("Stream migration complete")
        self.retry_btn.setEnabled(failed > 0)
        conflict_msg = f" | Conflicts resolved: {self._conflict_retries}" if self._conflict_retries else ""
        self.statusBar().showMessage(f"Complete - Copied: {sent:,}, Failed: {failed:,}, Skipped: {skipped:,}{conflict_msg} | Source: UNTOUCHED")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted - ALL ORIGINALS INTACT")
        if self.manifest.path and self.manifest.save_to_disk: self._log(f"Manifest saved: {self.manifest.path}")

    def _retry_failed(self):
        """Retry only files that failed in the last run."""
        failed_sops = {uid for uid, rec in self.manifest.records.items() if rec['status'] == 'failed'}
        if not failed_sops: self._log("No failed files to retry"); return
        retry_files = [f for f in self.dicom_files if f.get('sop_instance_uid', '') in failed_sops]
        if not retry_files: self._log("Could not match failed files"); return
        # Clear failed status so they get re-attempted
        for uid in failed_sops:
            if uid in self.manifest.records: del self.manifest.records[uid]
        self._log(f"Retrying {len(retry_files)} failed files...")
        self._start_upload(files_override=retry_files)

    def _on_upload_progress(self, c, t):
        self.upload_progress.setValue(c); self.upload_count_lbl.setText(f"{c} / {t} files")
        if self.upload_start_time and c > 0:
            r = ((time.time()-self.upload_start_time)/c) * (t-c)
            self.upload_eta_lbl.setText(f"ETA: {r/3600:.1f}h" if r > 3600 else f"ETA: {r/60:.0f}m {r%60:.0f}s" if r > 60 else f"ETA: {r:.0f}s")

    def _on_file_sent(self, path, ok, msg, sop_uid):
        self._upload_results.append((path, ok, msg, sop_uid))
        row = self.upload_table.rowCount(); self.upload_table.insertRow(row)
        items = [QTableWidgetItem(os.path.basename(path)),
                 QTableWidgetItem("OK" if ok else "FAIL"),
                 QTableWidgetItem(msg),
                 QTableWidgetItem(sop_uid[:30] + "..." if len(sop_uid) > 30 else sop_uid)]
        # Color code: green=OK, purple=conflict retry success, red=fail
        if ok and "Conflict retry" in msg:
            color = QColor("#cba6f7")  # purple for conflict-retried success
        elif ok:
            color = QColor("#a6e3a1")
        else:
            color = QColor("#f38ba8")
        items[1].setForeground(color)
        if not ok: items[2].setForeground(color)
        elif "Conflict retry" in msg: items[2].setForeground(QColor("#cba6f7"))
        for c, it in enumerate(items): self.upload_table.setItem(row, c, it)
        self.upload_table.scrollToBottom()

        if ok: self._sent += 1
        elif "Already sent" in msg or "Skipped" in msg or "Missing SOP" in msg: self._skipped += 1
        else: self._failed += 1
        self.sent_label.setText(f"Copied: {self._sent}"); self.failed_label.setText(f"Failed: {self._failed}")
        self.skipped_label.setText(f"Skipped: {self._skipped}")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted")

    def _on_speed(self, fps, mbps): self.upload_speed_lbl.setText(f"{fps:.1f} files/s | {mbps:.2f} MB/s")

    def _on_upload_complete(self, sent, failed, skipped):
        self.upload_btn.setEnabled(True); self.stream_btn.setEnabled(True); self.scan_btn.setEnabled(True)
        self.pause_btn.setEnabled(False); self.cancel_btn.setEnabled(False)
        self.export_csv_btn.setEnabled(True)
        self.retry_btn.setEnabled(failed > 0)
        conflict_msg = f" | Conflicts resolved: {self._conflict_retries}" if self._conflict_retries else ""
        self.statusBar().showMessage(f"Complete - Copied: {sent}, Failed: {failed}, Skipped: {skipped}{conflict_msg} | Source: UNTOUCHED")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted - ALL ORIGINALS INTACT")
        if self.manifest.path and self.manifest.save_to_disk: self._log(f"Manifest saved: {self.manifest.path}")

    def _toggle_pause(self):
        thread = self.streaming_thread if self._streaming_mode else self.upload_thread
        if not thread: return
        if thread._paused:
            thread.resume(); self.pause_btn.setText("Pause"); self._log("Resumed")
        else:
            thread.pause(); self.pause_btn.setText("Resume"); self._log("Paused")

    def _cancel_upload(self):
        thread = self.streaming_thread if self._streaming_mode else self.upload_thread
        if thread:
            thread.cancel(); self._log("Cancelled - source files untouched")

    def _export_csv(self):
        path, _ = QFileDialog.getSaveFileName(self, "Export Migration Manifest", f"migration_manifest_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "CSV Files (*.csv)")
        if path:
            self.manifest.export_csv(path)
            self._log(f"CSV manifest exported: {path} ({len(self.manifest.records)} records)")

    # ─── Verification ─────────────────────────────────────────────────────
    def _start_verify(self):
        if not self.dicom_files: self._log("No files scanned. Scan first."); return
        host = self.host_input.text().strip()
        if not host: self._log("Enter destination host first"); return

        # Build study file counts from uploaded files
        study_counts = {}
        for f in self.dicom_files:
            suid = f['study_instance_uid']
            if suid not in study_counts:
                study_counts[suid] = {'expected': 0, 'patient': f['patient_name'], 'desc': f['study_desc'] or 'No Description'}
            study_counts[suid]['expected'] += 1

        self.verify_table.setRowCount(0)
        self.verify_btn.setEnabled(False)
        self.verify_progress.setVisible(True); self.verify_progress.setValue(0)
        self.verify_status_lbl.setText("Verifying...")
        self.verify_status_lbl.setStyleSheet("color: #f9e2af;")

        self.verify_thread = VerifyThread(study_counts, host, self.port_input.value(),
            self.ae_scu.text().strip() or "DICOM_MIGRATOR", self.ae_scp.text().strip() or "ANY-SCP")
        self.verify_thread.progress.connect(lambda c, t: (self.verify_progress.setMaximum(t), self.verify_progress.setValue(c)))
        self.verify_thread.study_verified.connect(self._on_study_verified)
        self.verify_thread.log.connect(self._log)
        self.verify_thread.finished.connect(self._on_verify_done)
        self.verify_thread.start()

    def _on_study_verified(self, study_uid, expected, found, match):
        row = self.verify_table.rowCount(); self.verify_table.insertRow(row)
        # Look up patient/desc from files
        patient = desc = ""
        for f in self.dicom_files:
            if f['study_instance_uid'] == study_uid:
                patient = f['patient_name']; desc = f['study_desc'] or 'No Description'; break
        items = [QTableWidgetItem(patient), QTableWidgetItem(desc),
                 QTableWidgetItem(str(expected)), QTableWidgetItem(str(found)),
                 QTableWidgetItem("MATCH" if match else "MISMATCH")]
        items[4].setForeground(QColor("#a6e3a1") if match else QColor("#f38ba8"))
        for c, it in enumerate(items): self.verify_table.setItem(row, c, it)

    def _on_verify_done(self, total, matched, mismatched):
        self.verify_btn.setEnabled(True); self.verify_progress.setVisible(False)
        if mismatched == 0:
            self.verify_status_lbl.setText(f"ALL {total} STUDIES VERIFIED")
            self.verify_status_lbl.setStyleSheet("color: #a6e3a1; font-size: 14px;")
        else:
            self.verify_status_lbl.setText(f"{matched}/{total} matched, {mismatched} MISMATCHED")
            self.verify_status_lbl.setStyleSheet("color: #f38ba8; font-size: 14px;")

    def _export_log(self):
        p, _ = QFileDialog.getSaveFileName(self, "Export Log", "dicom_migrator_log.txt", "Text (*.txt)")
        if p:
            with open(p, 'w') as f: f.write(self.log_output.toPlainText())
            self._log(f"Exported: {p}")

    def _save_settings(self):
        s = self.settings
        s.setValue("folder", self.folder_input.text()); s.setValue("host", self.host_input.text())
        s.setValue("port", self.port_input.value()); s.setValue("ae_scu", self.ae_scu.text())
        s.setValue("ae_scp", self.ae_scp.text()); s.setValue("batch", self.batch_spin.value())
        s.setValue("retry", self.retry_spin.value()); s.setValue("recursive", self.recursive_check.isChecked())
        s.setValue("manifest_enabled", self.manifest_check.isChecked())
        s.setValue("decompress_fallback", self.decompress_check.isChecked())
        s.setValue("conflict_retry", self.conflict_retry_check.isChecked())
        s.setValue("conflict_suffix", self.conflict_suffix_input.text())

    def _load_settings(self):
        s = self.settings
        self.folder_input.setText(s.value("folder", ""))
        self.host_input.setText(s.value("host", ""))
        p = s.value("port"); self.port_input.setValue(int(p)) if p else None
        self.ae_scu.setText(s.value("ae_scu", "DICOM_MIGRATOR"))
        self.ae_scp.setText(s.value("ae_scp", "ANY-SCP"))
        b = s.value("batch"); self.batch_spin.setValue(int(b)) if b else None
        r = s.value("retry"); self.retry_spin.setValue(int(r)) if r else None
        rc = s.value("recursive")
        if rc is not None: self.recursive_check.setChecked(rc == "true" or rc is True)
        mc = s.value("manifest_enabled")
        if mc is not None: self.manifest_check.setChecked(mc == "true" or mc is True)
        dc = s.value("decompress_fallback")
        if dc is not None: self.decompress_check.setChecked(dc == "true" or dc is True)
        cr = s.value("conflict_retry")
        if cr is not None: self.conflict_retry_check.setChecked(cr == "true" or cr is True)
        cs = s.value("conflict_suffix")
        if cs is not None: self.conflict_suffix_input.setText(cs)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    app = QApplication(sys.argv)
    app.setStyle("Fusion"); app.setStyleSheet(DARK_STYLE)
    pal = QPalette()
    for role, color in [(QPalette.Window, "#1e1e2e"), (QPalette.WindowText, "#cdd6f4"),
        (QPalette.Base, "#181825"), (QPalette.AlternateBase, "#1e1e2e"),
        (QPalette.ToolTipBase, "#313244"), (QPalette.ToolTipText, "#cdd6f4"),
        (QPalette.Text, "#cdd6f4"), (QPalette.Button, "#313244"),
        (QPalette.ButtonText, "#cdd6f4"), (QPalette.BrightText, "#f38ba8"),
        (QPalette.Highlight, "#89b4fa"), (QPalette.HighlightedText, "#1e1e2e")]:
        pal.setColor(role, QColor(color))
    app.setPalette(pal)
    w = MainWindow(); w.show(); sys.exit(app.exec_())

if __name__ == "__main__":
    import multiprocessing
    multiprocessing.freeze_support()
    main()
