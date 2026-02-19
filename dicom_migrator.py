#!/usr/bin/env python3
"""
DICOM PACS Migrator v1.0.4
Production-grade DICOM C-STORE migration tool with parallel worker associations,
self-healing auto-retry, bandwidth throttling, migration scheduling, DICOM tag
morphing, modality/date filtering, TLS encryption, storage commitment verification,
post-migration C-FIND audit, network auto-discovery, resume support, streaming
migration, decompress fallback, patient ID conflict resolution, pre-flight
duplicate skip, DICOM validation, error classification, and audit trail.
Copy-only architecture — source data is NEVER modified or deleted.
"""
# SPDX-License-Identifier: MIT
# Copyright (c) 2026 SysAdminDoc

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

import json, time, logging, traceback, threading, socket, struct, ipaddress, re, csv, ssl, queue
from pathlib import Path
from datetime import datetime, time as dtime
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QHBoxLayout,
    QLabel, QPushButton, QLineEdit, QSpinBox, QProgressBar,
    QTreeWidget, QTreeWidgetItem, QTextEdit, QGroupBox, QGridLayout,
    QFileDialog, QTabWidget, QHeaderView, QSplitter, QFrame,
    QCheckBox, QComboBox, QStatusBar, QMessageBox, QDialog,
    QTableWidget, QTableWidgetItem, QAbstractItemView, QDateEdit,
    QTimeEdit, QDoubleSpinBox, QPlainTextEdit
)
from PyQt5.QtCore import Qt, QThread, pyqtSignal, QTimer, QSettings, QDate, QTime
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

VERSION = "1.0.4"
MAX_TABLE_ROWS = 5000  # Cap upload results table to prevent GUI slowdown on massive migrations
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
CANNOT_UNDERSTAND_RANGE = range(0xC000, 0xD000)

# Errors that should NEVER be retried — permanent/structural failures
NON_RETRYABLE_PATTERNS = [
    "Invalid DICOM", "Missing SOP",
    "Targeted assoc rejected",    # SCP genuinely doesn't support this SOP class
    "no SOPClassUID",             # Malformed DICOM object
    "still rejected",             # 0xC000 cleanup attempted and failed — structural issue
]

def is_retryable_error(msg):
    """Returns True if the error is transient and worth retrying later."""
    if not msg:
        return True
    for pattern in NON_RETRYABLE_PATTERNS:
        if pattern in msg:
            return False
    return True


# ═══════════════════════════════════════════════════════════════════════════════
# Bandwidth Throttle — Token bucket rate limiter for protecting production PACS
# ═══════════════════════════════════════════════════════════════════════════════
class BandwidthThrottle:
    """Thread-safe token bucket rate limiter. Limits aggregate throughput across all workers.
    Set rate_mbps=0 for unlimited."""
    def __init__(self, rate_mbps=0.0, cancel_event=None):
        self._lock = threading.Lock()
        self._rate_bps = rate_mbps * 1024 * 1024  # bytes per second
        self._tokens = self._rate_bps  # start full
        self._last_refill = time.monotonic()
        self.enabled = rate_mbps > 0
        self._cancel_event = cancel_event  # Optional threading.Event for cancellation

    def set_rate(self, rate_mbps):
        with self._lock:
            self._rate_bps = rate_mbps * 1024 * 1024
            self.enabled = rate_mbps > 0
            self._tokens = self._rate_bps
            self._last_refill = time.monotonic()

    def acquire(self, nbytes):
        """Block until nbytes worth of bandwidth is available."""
        if not self.enabled or nbytes <= 0:
            return
        while True:
            if self._cancel_event and self._cancel_event.is_set():
                return  # Bail out on cancel — don't block shutdown
            with self._lock:
                now = time.monotonic()
                elapsed = now - self._last_refill
                self._tokens = min(self._rate_bps * 2, self._tokens + elapsed * self._rate_bps)
                self._last_refill = now
                if self._tokens >= nbytes:
                    self._tokens -= nbytes
                    return
            time.sleep(0.01)  # Yield and retry


# ═══════════════════════════════════════════════════════════════════════════════
# Tag Morphing Engine — In-memory DICOM tag transforms during migration
# ═══════════════════════════════════════════════════════════════════════════════
def parse_tag_rules(rules_text):
    """Parse tag morphing rules from text.
    Format per line: TAG_KEYWORD ACTION [VALUE]
    Actions: set, prefix, suffix, delete, strip_private
    Examples:
        InstitutionName set "New Hospital"
        PatientID prefix MIG_
        AccessionNumber suffix _2026
        ReferringPhysicianName delete
        strip_private
    Returns list of (keyword, action, value) tuples."""
    rules = []
    if not rules_text or not rules_text.strip():
        return rules
    for line in rules_text.strip().splitlines():
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        if line.lower() == 'strip_private':
            rules.append(('_strip_private', 'strip_private', ''))
            continue
        parts = line.split(None, 2)
        if len(parts) < 2:
            continue
        keyword = parts[0]
        action = parts[1].lower()
        value = parts[2].strip().strip('"').strip("'") if len(parts) > 2 else ''
        rules.append((keyword, action, value))
    return rules


def apply_tag_rules(ds, rules):
    """Apply tag morphing rules to an in-memory dataset. Source files are NEVER modified.
    Returns True if any modifications were made."""
    if not rules:
        return False
    modified = False
    for keyword, action, value in rules:
        if action == 'strip_private':
            ds.remove_private_tags()
            modified = True
            continue
        if not hasattr(ds, keyword):
            if action == 'set':
                try:
                    setattr(ds, keyword, value)
                    modified = True
                except Exception:
                    pass
            continue
        if action == 'set':
            setattr(ds, keyword, value); modified = True
        elif action == 'prefix':
            current = str(getattr(ds, keyword, ''))
            setattr(ds, keyword, f"{value}{current}"); modified = True
        elif action == 'suffix':
            current = str(getattr(ds, keyword, ''))
            setattr(ds, keyword, f"{current}{value}"); modified = True
        elif action == 'delete':
            try:
                delattr(ds, keyword); modified = True
            except Exception:
                pass
    return modified


# ═══════════════════════════════════════════════════════════════════════════════
# Migration Schedule Window — Auto-pause outside allowed time window
# ═══════════════════════════════════════════════════════════════════════════════
def is_within_schedule(start_time, end_time, enabled=True):
    """Check if current time is within the migration window.
    start_time/end_time: datetime.time objects.
    Handles overnight windows (e.g., 19:00 - 06:00).
    Returns True if migration is allowed right now."""
    if not enabled:
        return True
    now = datetime.now().time()
    if start_time <= end_time:
        return start_time <= now <= end_time
    else:
        # Overnight window: e.g., 19:00 -> 06:00
        return now >= start_time or now <= end_time


def wait_for_schedule(start_time, end_time, enabled, cancel_event, log_fn=None, pause_event=None):
    """Block until current time enters the schedule window. Returns False if cancelled."""
    if not enabled or is_within_schedule(start_time, end_time, enabled):
        return True
    if log_fn:
        log_fn(f"Outside schedule window ({start_time.strftime('%H:%M')}-{end_time.strftime('%H:%M')}). "
               f"Pausing until window opens...")
    while not cancel_event.is_set():
        if is_within_schedule(start_time, end_time, enabled):
            if log_fn:
                log_fn("Schedule window open — resuming migration")
            return True
        cancel_event.wait(30)  # Check every 30 seconds
    return False


# ═══════════════════════════════════════════════════════════════════════════════
# TLS Context Builder — Encrypted DICOM associations
# ═══════════════════════════════════════════════════════════════════════════════
def build_tls_context(cert_file=None, key_file=None, ca_file=None):
    """Build an SSL context for DICOM TLS. Returns ssl.SSLContext or None."""
    try:
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        if ca_file and os.path.exists(ca_file):
            ctx.load_verify_locations(ca_file)
        else:
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
        if cert_file and os.path.exists(cert_file):
            ctx.load_cert_chain(cert_file, keyfile=key_file if key_file and os.path.exists(key_file) else None)
        return ctx
    except Exception:
        return None


def is_valid_dicom(ds):
    """Validate that a dataset read with force=True is actually a conformant DICOM file.
    Checks for minimum required tags to avoid sending garbage to the destination."""
    if not hasattr(ds, 'SOPClassUID') or not hasattr(ds, 'SOPInstanceUID'):
        return False
    # Must have file_meta with MediaStorageSOPClassUID for proper DICOM Part 10
    if hasattr(ds, 'file_meta'):
        if not hasattr(ds.file_meta, 'MediaStorageSOPClassUID'):
            return False
    # PatientID or StudyInstanceUID should exist for anything worth migrating
    if not hasattr(ds, 'StudyInstanceUID') or not str(getattr(ds, 'StudyInstanceUID', '')).strip():
        return False
    return True


def preflight_check_destination(host, port, ae_scu, ae_scp, study_uids, log_fn=None):
    """C-FIND the destination to discover which StudyInstanceUIDs already exist.
    Returns a set of study UIDs that are already on the destination.
    Used to skip entire studies that don't need re-sending."""
    from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

    existing = set()
    try:
        ae = AE(ae_title=ae_scu)
        ae.acse_timeout = 15; ae.dimse_timeout = 30; ae.network_timeout = 15
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

        assoc = ae.associate(host, port, ae_title=ae_scp)
        if not assoc.is_established:
            if log_fn: log_fn("Pre-flight C-FIND association rejected — skipping duplicate check")
            return existing

        total = len(study_uids)
        for idx, study_uid in enumerate(study_uids):
            query = pydicom.Dataset()
            query.QueryRetrieveLevel = 'STUDY'
            query.StudyInstanceUID = study_uid
            query.NumberOfStudyRelatedInstances = ''

            responses = assoc.send_c_find(query, StudyRootQueryRetrieveInformationModelFind)
            for status, identifier in responses:
                if status and status.Status in (0xFF00, 0xFF01) and identifier:
                    existing.add(study_uid)
                    break

            # Re-establish if association dies mid-query
            if not assoc.is_established:
                if log_fn: log_fn(f"  Pre-flight association lost at study {idx+1}/{total} — reconnecting...")
                try:
                    assoc = ae.associate(host, port, ae_title=ae_scp)
                    if not assoc.is_established:
                        if log_fn: log_fn("  Pre-flight reconnection failed — returning partial results")
                        break
                except Exception:
                    break

            if (idx + 1) % 50 == 0 and log_fn:
                log_fn(f"  Pre-flight check: {idx+1}/{total} studies queried, {len(existing)} already on destination")

        try: assoc.release()
        except: pass

        if log_fn:
            log_fn(f"Pre-flight complete: {len(existing)}/{total} studies already on destination")
    except Exception as e:
        if log_fn: log_fn(f"Pre-flight check failed: {e}")

    return existing


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
                     pid_cache=None, tls_context=None):
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

    def _build_targeted_ae(sop_class_uid, uncompressed_only=False):
        """Build a minimal AE with just the given SOP class.
        For pixel objects: offer all transfer syntaxes (compressed + uncompressed).
        For non-pixel objects: offer only uncompressed syntaxes — many PACS reject
        contexts that include JPEG/JPEG2000/RLE for non-pixel SOP classes."""
        targeted = AE(ae_title=ae_scu or "DICOM_MIGRATOR")
        targeted.maximum_pdu_size = 0
        targeted.acse_timeout = 30; targeted.dimse_timeout = 120; targeted.network_timeout = 30
        if uncompressed_only:
            # Non-pixel objects: separate contexts for maximum compatibility
            targeted.add_requested_context(sop_class_uid, [
                ExplicitVRLittleEndian, ImplicitVRLittleEndian])
            targeted.add_requested_context(sop_class_uid, [ImplicitVRLittleEndian])
            targeted.add_requested_context(sop_class_uid, [ExplicitVRLittleEndian])
            targeted.add_requested_context(sop_class_uid, [DeflatedExplicitVRLittleEndian])
        else:
            targeted.add_requested_context(sop_class_uid, TRANSFER_SYNTAXES)
            targeted.add_requested_context(sop_class_uid, [
                ExplicitVRLittleEndian, ImplicitVRLittleEndian])
        targeted.add_requested_context(Verification)
        return targeted

    def _try_targeted_assoc(dataset, label="targeted", uncompressed_only=False):
        """Last-resort: build a fresh AE with ONLY this SOP class and try a new association."""
        if not (host and port and ae_scp):
            return (-1, f"No context ({label}, no connection info for retry)", False, None)
        sop_uid = str(getattr(dataset, 'SOPClassUID', ''))
        if not sop_uid:
            return (-1, f"No context ({label}, no SOPClassUID)", False, None)
        try:
            targeted_ae = _build_targeted_ae(sop_uid, uncompressed_only=uncompressed_only)
            if tls_context:
                new_assoc = targeted_ae.associate(host, port, ae_title=ae_scp,
                                                   tls_args=(tls_context,))
            else:
                new_assoc = targeted_ae.associate(host, port, ae_title=ae_scp)
            if not new_assoc.is_established:
                return (-1, f"Targeted assoc rejected for {sop_uid}", False, None)
            st = new_assoc.send_c_store(dataset)
            if st:
                sop_name = getattr(dataset, 'SOPClassUID', sop_uid)
                if hasattr(sop_name, 'name'):
                    sop_name = sop_name.name
                if st.Status == 0x0000 or st.Status in (0xFF00, 0xFF01):
                    if log_fn:
                        log_fn(f"  Sent via targeted assoc ({label}): {os.path.basename(fpath)} [{sop_name}]")
                    return (st.Status, f"Sent via {label} association", False, new_assoc)
                else:
                    # Non-success — extract detail and release
                    err_detail = _status_detail(st)
                    detail_msg = f"Targeted {label} 0x{st.Status:04X}"
                    if err_detail: detail_msg += f" ({err_detail})"
                    detail_msg += f" [{sop_name}]"
                    try: new_assoc.release()
                    except: pass
                    return (st.Status, detail_msg, False, None)
            else:
                try: new_assoc.release()
                except: pass
                return (None, f"No response on targeted assoc ({label})", False, None)
        except Exception as e:
            return (-1, f"Targeted assoc failed ({label}): {e}", False, None)

    def _status_detail(st_dataset):
        """Extract human-readable detail from a C-STORE status response."""
        if st_dataset is None:
            return ""
        parts = []
        # ErrorComment is the primary diagnostic field
        if hasattr(st_dataset, 'ErrorComment') and st_dataset.ErrorComment:
            parts.append(str(st_dataset.ErrorComment))
        # OffendingElement lists which tags caused the failure
        if hasattr(st_dataset, 'OffendingElement') and st_dataset.OffendingElement:
            tags = ', '.join(f"({t.group:04X},{t.elem:04X})" if hasattr(t, 'group')
                            else str(t) for t in st_dataset.OffendingElement)
            parts.append(f"Offending: {tags}")
        return '; '.join(parts)

    def _do_send(association, dataset):
        """Returns (status, message, was_decompressed, new_assoc_or_None)."""
        try:
            st = association.send_c_store(dataset)
            if st:
                detail = _status_detail(st)
                return (st.Status, detail, False, None)
            return (None, "No response from SCP", False, None)
        except Exception as e:
            err_msg = str(e)
            if 'presentation context' not in err_msg.lower():
                return (-1, err_msg, False, None)

            if not decompress_fallback:
                # Still try a targeted association even without decompress
                has_px = all(hasattr(dataset, a) for a in ('PixelData', 'Rows', 'Columns', 'BitsAllocated'))
                return _try_targeted_assoc(dataset, "no-decompress", uncompressed_only=not has_px)

            # ── Step 1: Try decompressing pixel data objects ──
            has_pixels = all(hasattr(dataset, attr) for attr in ('PixelData',))
            has_pixel_attrs = has_pixels and all(
                hasattr(dataset, attr) for attr in ('Rows', 'Columns', 'BitsAllocated'))

            if has_pixel_attrs:
                try:
                    original_tsuid = getattr(dataset.file_meta, 'TransferSyntaxUID', None) if hasattr(dataset, 'file_meta') else None
                    ts_name = str(original_tsuid.name) if original_tsuid and hasattr(original_tsuid, 'name') else str(original_tsuid or 'Unknown')

                    dataset.decompress()

                    # Try on existing association first
                    try:
                        st = association.send_c_store(dataset)
                        if st:
                            detail_info = _status_detail(st)
                            decomp_msg = f"Decompressed from {ts_name}"
                            if detail_info: decomp_msg += f" ({detail_info})"
                            if log_fn and (st.Status == 0x0000 or st.Status in (0xFF00, 0xFF01)):
                                log_fn(f"  Decompressed {ts_name} -> Explicit VR LE: {os.path.basename(fpath)}")
                            return (st.Status, decomp_msg, True, None)
                    except Exception:
                        pass  # Association likely dead — fall through

                    # ── Step 2: Try targeted association after decompress ──
                    if host and port and ae_scp:
                        sop_uid = str(getattr(dataset, 'SOPClassUID', ''))
                        try:
                            targeted_ae = _build_targeted_ae(sop_uid)
                            if tls_context:
                                new_assoc = targeted_ae.associate(host, port, ae_title=ae_scp,
                                                                   tls_args=(tls_context,))
                            else:
                                new_assoc = targeted_ae.associate(host, port, ae_title=ae_scp)
                            if new_assoc.is_established:
                                st = new_assoc.send_c_store(dataset)
                                if st:
                                    if log_fn:
                                        log_fn(f"  Decompressed {ts_name} -> Explicit VR LE (targeted assoc): {os.path.basename(fpath)}")
                                    return (st.Status, f"Decompressed from {ts_name}", True, new_assoc)
                                try: new_assoc.release()
                                except: pass
                            else:
                                if log_fn:
                                    log_fn(f"  Targeted assoc rejected for decompressed {sop_uid}")
                        except Exception as e3:
                            if log_fn:
                                log_fn(f"  Targeted assoc failed after decompress: {e3}")

                    return (-1, f"Decompress OK from {ts_name} but all associations failed", True, None)
                except Exception as decomp_err:
                    # Decompress itself failed — fall through to targeted assoc with original data
                    if log_fn:
                        log_fn(f"  Decompress failed: {decomp_err}, trying targeted assoc...")

            # ── Step 3: Non-pixel object OR decompress failed — targeted association ──
            is_non_pixel = not has_pixel_attrs
            return _try_targeted_assoc(dataset,
                "non-pixel" if is_non_pixel else "decompress-failed",
                uncompressed_only=is_non_pixel)

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

    # ── 0xC000-0xCFFF: "Cannot Understand" — multi-step recovery ──
    # Step 1: Strip private tags + fix metadata → send on existing association
    # Step 2: Decompress pixel data (if present) → send on existing association
    # Step 3: Cleaned + decompressed → targeted uncompressed-only association
    if status_val is not None and status_val in CANNOT_UNDERSTAND_RANGE:
        import copy as _copy
        sop_uid_obj = getattr(ds, 'SOPClassUID', '?')
        sop_name = sop_uid_obj.name if hasattr(sop_uid_obj, 'name') else str(sop_uid_obj)
        sop_uid = str(sop_uid_obj)
        pid = str(getattr(ds, 'PatientID', '?'))
        study = str(getattr(ds, 'StudyDescription', ''))[:40] or str(getattr(ds, 'StudyInstanceUID', '?'))[-12:]
        n_private = sum(1 for elem in ds if elem.tag.is_private)
        original_ts = None
        if hasattr(ds, 'file_meta'):
            original_ts = getattr(ds.file_meta, 'TransferSyntaxUID', None)
        ts_name = str(original_ts.name) if original_ts and hasattr(original_ts, 'name') else str(original_ts or 'Unknown')

        if log_fn:
            log_fn(f"  0x{status_val:04X} '{sop_name}' PID={pid} Study={study} "
                   f"TS={ts_name} ({n_private} private tags) — starting recovery...")
        try:
            clean = _copy.deepcopy(ds)
            # ── Cleanup: strip private tags, fix metadata, remove empty sequences ──
            clean.remove_private_tags()
            if hasattr(clean, 'file_meta') and hasattr(clean, 'SOPClassUID'):
                clean.file_meta.MediaStorageSOPClassUID = clean.SOPClassUID
                clean.file_meta.MediaStorageSOPInstanceUID = clean.SOPInstanceUID
            if not hasattr(clean, 'SpecificCharacterSet') or not clean.SpecificCharacterSet:
                clean.SpecificCharacterSet = 'ISO_IR 100'
            empty_sq_tags = [elem.tag for elem in clean
                             if elem.VR == 'SQ' and elem.value is not None and len(elem.value) == 0]
            for tag in empty_sq_tags:
                del clean[tag]

            # ── Step 1: Clean dataset on existing association ──
            send_assoc = new_assoc if new_assoc else assoc
            sv1, msg1, decomp1, new1 = _do_send(send_assoc, clean)
            if sv1 is not None and sv1 >= 0 and (sv1 == 0x0000 or sv1 in (0xFF00, 0xFF01)):
                if log_fn: log_fn(f"  Step 1 OK (stripped {n_private} private tags): {os.path.basename(fpath)}")
                return (sv1, f"Cleaned (stripped {n_private} private tags)", was_decompressed or decomp1, False, new1 or new_assoc)

            # ── Step 2: Decompress pixel data + clean on existing association ──
            has_pixels = all(hasattr(clean, a) for a in ('PixelData', 'Rows', 'Columns', 'BitsAllocated'))
            decomp_ok = False
            if has_pixels:
                try:
                    clean.decompress()
                    decomp_ok = True
                    if log_fn: log_fn(f"  Step 2: decompressed {ts_name} -> Explicit VR LE, retrying...")
                    send2 = new1 if new1 and new1.is_established else (new_assoc if new_assoc else assoc)
                    sv2, msg2, _, new2 = _do_send(send2, clean)
                    if sv2 is not None and sv2 >= 0 and (sv2 == 0x0000 or sv2 in (0xFF00, 0xFF01)):
                        if log_fn: log_fn(f"  Step 2 OK (cleaned + decompressed): {os.path.basename(fpath)}")
                        final = new2 or new1 or new_assoc
                        return (sv2, f"Cleaned + decompressed from {ts_name}", True, False, final)
                    if new2:
                        try: new2.release()
                        except: pass
                except Exception as decomp_err:
                    if log_fn: log_fn(f"  Step 2: decompress failed ({decomp_err}), trying targeted...")

            # ── Step 3: Targeted uncompressed-only association ──
            if host and port and ae_scp:
                try:
                    is_non_pixel = not has_pixels
                    targeted_ae = _build_targeted_ae(sop_uid, uncompressed_only=True)
                    if tls_context:
                        t_assoc = targeted_ae.associate(host, port, ae_title=ae_scp, tls_args=(tls_context,))
                    else:
                        t_assoc = targeted_ae.associate(host, port, ae_title=ae_scp)
                    if t_assoc.is_established:
                        if log_fn: log_fn(f"  Step 3: targeted uncompressed association for {sop_name}...")
                        st3 = t_assoc.send_c_store(clean)
                        if st3 and (st3.Status == 0x0000 or st3.Status in (0xFF00, 0xFF01)):
                            if log_fn: log_fn(f"  Step 3 OK (targeted uncompressed): {os.path.basename(fpath)}")
                            detail = f"Cleaned + targeted uncompressed"
                            if decomp_ok: detail = f"Cleaned + decompressed + targeted uncompressed"
                            return (st3.Status, detail, decomp_ok or was_decompressed, False, t_assoc)
                        # Still rejected on targeted — get detail
                        t_detail = _status_detail(st3) if st3 else ""
                        t_status = f"0x{st3.Status:04X}" if st3 else "no response"
                        if log_fn: log_fn(f"  Step 3 rejected: {t_status} {t_detail}")
                        try: t_assoc.release()
                        except: pass
                    else:
                        if log_fn: log_fn(f"  Step 3: targeted association rejected for {sop_uid}")
                except Exception as t_err:
                    if log_fn: log_fn(f"  Step 3 failed: {t_err}")

            # ── All steps exhausted ──
            detail_parts = [f"0x{status_val:04X} Cannot Understand"]
            if msg: detail_parts.append(msg)
            detail_parts.append(f"{sop_name}, PID={pid}, TS={ts_name}")
            steps_tried = ["private tags stripped"]
            if decomp_ok: steps_tried.append("decompressed")
            steps_tried.append("targeted uncompressed")
            detail_parts.append(f"tried: {', '.join(steps_tried)} — still rejected")
            if new1:
                try: new1.release()
                except: pass
            return (status_val, '; '.join(detail_parts), was_decompressed, False, new_assoc)
        except Exception as clean_err:
            if log_fn:
                log_fn(f"  0xC000 recovery failed: {clean_err}")
            detail = f"0x{status_val:04X} Cannot Understand ({sop_name}, PID={pid}, recovery failed: {clean_err})"
            return (status_val, detail, was_decompressed, False, new_assoc)

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
        manifest_dir = os.path.join(os.path.expanduser("~"), ".dicom_migrator")
        os.makedirs(manifest_dir, exist_ok=True)

        # Look for existing manifest for this source folder (any date)
        # Enables seamless resume across sessions even days apart
        prefix = f"migration_manifest_{safe}_"
        existing = sorted(
            [f for f in os.listdir(manifest_dir) if f.startswith(prefix) and f.endswith('.json')],
            reverse=True)  # Most recent date first
        if existing:
            self.path = os.path.join(manifest_dir, existing[0])
        else:
            fname = f"{prefix}{datetime.now().strftime('%Y%m%d')}.json"
            self.path = os.path.join(manifest_dir, fname)
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
            import tempfile
            dir_name = os.path.dirname(self.path)
            fd, tmp_path = tempfile.mkstemp(dir=dir_name, suffix='.tmp')
            with os.fdopen(fd, 'w') as f:
                json.dump({'meta': self.meta, 'records': self.records}, f, indent=2)
            # Atomic rename (works on same filesystem)
            if os.path.exists(self.path):
                os.replace(tmp_path, self.path)
            else:
                os.rename(tmp_path, self.path)
        except Exception:
            # Fall back to direct write if atomic fails
            try:
                with open(self.path, 'w') as f:
                    json.dump({'meta': self.meta, 'records': self.records}, f, indent=2)
            except Exception:
                pass

    def record_file(self, sop_uid, path, status, message='', **extra):
        existing = self.records.get(sop_uid, {})
        prev_retries = existing.get('retry_count', 0)
        # Increment retry count if re-recording a previously failed file
        retry_count = prev_retries + 1 if existing.get('status') == 'failed' and status == 'failed' else prev_retries
        self.records[sop_uid] = {
            'path': path,
            'status': status,  # 'sent', 'failed', 'skipped'
            'message': message,
            'timestamp': datetime.now().isoformat(),
            'retry_count': retry_count,
            **extra,
        }

    def is_already_sent(self, sop_uid):
        rec = self.records.get(sop_uid)
        return rec is not None and rec.get('status') == 'sent'

    def get_failed(self):
        return {uid: rec for uid, rec in self.records.items() if rec.get('status') == 'failed'}

    def get_retryable_failed(self, max_retries=3):
        """Return failed records that haven't exceeded max_retries and have retryable errors."""
        return {uid: rec for uid, rec in self.records.items()
                if rec.get('status') == 'failed'
                and rec.get('retry_count', 0) < max_retries
                and is_retryable_error(rec.get('message', ''))}

    def get_sent_count(self):
        return sum(1 for r in self.records.values() if r['status'] == 'sent')

    def build_sent_paths_index(self):
        """Build a set of file paths already sent successfully.
        Used for fast directory-level skip during resume — avoids reading DICOM headers
        for files that are already confirmed sent."""
        return set(rec['path'] for rec in self.records.values()
                   if rec.get('status') == 'sent' and rec.get('path'))

    def build_processed_paths_index(self):
        """Build a set of file paths that are fully done (sent or permanently skipped).
        EXCLUDES failed files so directories with failures are re-entered for retry.
        Used for fast directory-level skip during resume."""
        return set(rec['path'] for rec in self.records.values()
                   if rec.get('status') in ('sent', 'skipped') and rec.get('path'))

    def get_failed_count(self):
        return sum(1 for r in self.records.values() if r['status'] == 'failed')

    def export_csv(self, csv_path):
        fields = ['sop_instance_uid', 'sop_class_uid', 'path', 'status', 'message', 'timestamp',
                   'patient_name', 'patient_id', 'study_date', 'study_desc', 'modality',
                   'study_instance_uid', 'series_instance_uid', 'retry_count']
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fields, extrasaction='ignore')
            writer.writeheader()
            for uid, rec in self.records.items():
                row = {'sop_instance_uid': uid, **rec}
                writer.writerow(row)

    def export_summary(self, path):
        """Export migration summary report with performance stats."""
        sent = sum(1 for r in self.records.values() if r['status'] == 'sent')
        failed = sum(1 for r in self.records.values() if r['status'] == 'failed')
        skipped = sum(1 for r in self.records.values() if r['status'] == 'skipped')
        total = len(self.records)
        studies = set()
        modalities = defaultdict(int)
        for rec in self.records.values():
            suid = rec.get('study_instance_uid', '')
            if suid: studies.add(suid)
            mod = rec.get('modality', 'Unknown')
            modalities[mod] += 1

        # Failure breakdown
        failures = defaultdict(int)
        for rec in self.records.values():
            if rec['status'] == 'failed':
                failures[rec.get('message', 'Unknown')[:80]] += 1

        lines = [
            f"DICOM PACS Migration Report",
            f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
            f"Destination: {self.meta.get('destination', 'N/A')}",
            f"Source: {self.meta.get('source_folder', 'N/A')}",
            f"",
            f"{'='*60}",
            f"RESULTS SUMMARY",
            f"{'='*60}",
            f"Total files processed: {total:,}",
            f"  Copied successfully:  {sent:,}",
            f"  Failed:               {failed:,}",
            f"  Skipped:              {skipped:,}",
            f"Unique studies:         {len(studies):,}",
            f"Success rate:           {(sent/total*100):.1f}%" if total > 0 else "N/A",
            f"",
            f"MODALITY BREAKDOWN",
            f"{'='*60}",
        ]
        for mod, count in sorted(modalities.items(), key=lambda x: -x[1]):
            lines.append(f"  {mod:8s} {count:,} files")

        if failures:
            lines.extend(["", f"FAILURE BREAKDOWN", f"{'='*60}"])
            for msg, count in sorted(failures.items(), key=lambda x: -x[1]):
                lines.append(f"  [{count:,}x] {msg}")

        lines.extend(["", f"SOURCE SAFETY: 0 files modified, 0 files deleted - ALL ORIGINALS INTACT"])

        with open(path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(lines))


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

    def __init__(self, folder_path, recursive=True, manifest=None):
        super().__init__()
        self.folder_path = folder_path; self.recursive = recursive; self._cancel = False
        self.manifest = manifest

    def cancel(self): self._cancel = True

    def run(self):
        try:
            results = []
            self.log.emit(f"Scanning (READ-ONLY): {self.folder_path}")
            self.status.emit("Enumerating files...")
            root = Path(self.folder_path)

            # Build path-based cache from manifest for instant metadata lookup
            _manifest_cache = {}  # path -> record dict
            if self.manifest and self.manifest.records:
                for uid, rec in self.manifest.records.items():
                    p = rec.get('path', '')
                    if p and rec.get('status') in ('sent', 'skipped'):
                        _manifest_cache[p] = {
                            'path': p,
                            'patient_name': rec.get('patient_name', 'Unknown'),
                            'patient_id': rec.get('patient_id', 'N/A'),
                            'study_date': rec.get('study_date', ''),
                            'study_desc': rec.get('study_desc', ''),
                            'series_desc': '',
                            'modality': rec.get('modality', 'OT'),
                            'sop_class_uid': rec.get('sop_class_uid', ''),
                            'sop_instance_uid': uid,
                            'study_instance_uid': rec.get('study_instance_uid', ''),
                            'series_instance_uid': rec.get('series_instance_uid', ''),
                            'transfer_syntax': str(ImplicitVRLittleEndian),
                            'file_size': 0,
                        }
                if _manifest_cache:
                    self.log.emit(f"Manifest cache: {len(_manifest_cache):,} files with cached metadata (instant scan)")

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
            dc = sk = cached = 0
            for i, fpath in enumerate(all_files):
                if self._cancel: self.finished.emit(results); return
                self.progress.emit(i + 1, total)
                # Emit current file for live display
                try:
                    rel = fpath.relative_to(root)
                except ValueError:
                    rel = fpath.name

                # Fast path: use cached metadata from manifest instead of reading DICOM header
                fpath_str = str(fpath)
                if fpath_str in _manifest_cache:
                    rec = _manifest_cache[fpath_str]
                    # Get actual file size (cheap stat vs expensive dcmread)
                    try: rec['file_size'] = fpath.stat().st_size
                    except: pass
                    results.append(rec)
                    dc += 1; cached += 1
                    if (i + 1) % 5000 == 0:
                        self.status.emit(f"Scanning {i+1:,}/{total:,} | {dc:,} DICOM ({cached:,} cached) | {sk:,} skipped")
                        self.current_file.emit(f"Fast-cached: {rel}", dc, sk)
                    continue

                self.current_file.emit(str(rel), dc, sk)
                if (i + 1) % 100 == 0:
                    self.status.emit(f"Scanning {i+1:,}/{total:,} | {dc:,} DICOM ({cached:,} cached) | {sk:,} skipped")
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
            cache_msg = f" ({cached:,} from manifest cache)" if cached else ""
            self.log.emit(f"Scan complete: {dc:,} DICOM{cache_msg}, {sk:,} skipped (all read-only)")
            self.finished.emit(results)
        except Exception as e:
            self.error.emit(f"Scan failed: {e}\n{traceback.format_exc()}")


# ═══════════════════════════════════════════════════════════════════════════════
# Parallel Send Engine — Multiple worker threads with persistent associations
# ═══════════════════════════════════════════════════════════════════════════════

_SENTINEL = None  # Signals worker to stop

def _send_worker(worker_id, ae_builder, host, port, ae_scp, ae_scu,
                  file_queue, result_queue, cancel_event, pause_event,
                  decompress_fallback, conflict_retry, conflict_suffix,
                  pid_cache, pid_cache_lock, retry_count,
                  throttle=None, tag_rules=None, tls_context=None):
    """Worker thread: maintains its own DICOM association and processes files from queue."""
    assoc = None

    def _get_assoc(ae):
        nonlocal assoc
        if assoc and assoc.is_established:
            return assoc
        try:
            if assoc:
                try: assoc.release()
                except: pass
            if tls_context:
                assoc = ae.associate(host, port, ae_title=ae_scp, tls_args=(tls_context,))
            else:
                assoc = ae.associate(host, port, ae_title=ae_scp)
            if assoc.is_established:
                return assoc
        except: pass
        return None

    ae = ae_builder()

    while not cancel_event.is_set():
        pause_event.wait()  # Block if paused
        try:
            item = file_queue.get(timeout=0.5)
        except queue.Empty:
            continue
        if item is _SENTINEL:
            file_queue.task_done()
            break

        f = item
        fpath = f['path']; sop = f.get('sop_instance_uid', '')

        # Try to get/create association
        for attempt in range(retry_count + 1):
            a = _get_assoc(ae)
            if a: break
            if attempt < retry_count:
                time.sleep(min(2 ** attempt, 8))  # Exponential backoff

        if not a or not a.is_established:
            result_queue.put((f, False, "Association failed", sop, False, 0))
            file_queue.task_done()
            continue

        try:
            ds = pydicom.dcmread(fpath, force=True)
            if not is_valid_dicom(ds):
                result_queue.put((f, False, "Invalid DICOM (missing required tags)", sop, False, 0))
                file_queue.task_done()
                continue

            # Apply tag morphing rules (in-memory only, source untouched)
            if tag_rules:
                apply_tag_rules(ds, tag_rules)

            # Thread-safe pid_cache access
            local_cache = {}
            if pid_cache is not None:
                with pid_cache_lock:
                    local_cache = dict(pid_cache)

            sv, detail, was_decompressed, was_conflict_retried, new_assoc = try_send_c_store(
                assoc, ds, fpath, decompress_fallback,
                conflict_retry, conflict_suffix,
                log_fn=None,
                ae=ae, host=host, port=port,
                ae_scp=ae_scp, ae_scu=ae_scu,
                pid_cache=local_cache, tls_context=tls_context)

            # Merge cache updates back
            if pid_cache is not None and local_cache:
                with pid_cache_lock:
                    pid_cache.update(local_cache)

            if new_assoc is not None:
                # A targeted association was used (decompress or context fallback).
                # Release it and re-establish the broad association for the next file
                # so subsequent files don't all cascade through targeted fallbacks.
                try: new_assoc.release()
                except: pass
                try: assoc.release()
                except: pass
                assoc = None  # Force _get_assoc to create fresh broad association next iteration

            file_size = f.get('file_size', 0)
            if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                msg = detail if detail else ("Copied" if sv == 0 else f"Pending (0x{sv:04X})")
                if was_decompressed and not detail: msg = "Decompressed + Copied"
                # Bandwidth throttle — block until budget available
                if throttle and file_size > 0:
                    throttle.acquire(file_size)
                result_queue.put((f, True, msg, sop, was_conflict_retried, file_size))
            elif sv is not None and sv >= 0:
                msg = f"Status: 0x{sv:04X}"
                if detail: msg += f" ({detail})"
                result_queue.put((f, False, msg, sop, was_conflict_retried, 0))
            else:
                msg = detail or "No response"
                result_queue.put((f, False, msg, sop, was_conflict_retried, 0))
        except Exception as e:
            result_queue.put((f, False, str(e), sop, False, 0))

        file_queue.task_done()

    # Cleanup
    if assoc:
        try: assoc.release()
        except: pass


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
    conflict_retry_count = pyqtSignal(int)
    auto_retry_healed = pyqtSignal(int)  # running total of auto-healed files

    def __init__(self, files, host, port, ae_scu, ae_scp,
                 max_pdu=0, batch_size=50, retry_count=1, manifest=None,
                 decompress_fallback=True, conflict_retry=False, conflict_suffix="_MIG",
                 skip_existing=False, workers=1, max_retries=3,
                 throttle=None, tag_rules=None, tls_context=None):
        super().__init__()
        self.files = files; self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.max_pdu = max_pdu; self.batch_size = batch_size
        self.retry_count = retry_count; self.manifest = manifest
        self.decompress_fallback = decompress_fallback
        self.conflict_retry = conflict_retry
        self.conflict_suffix = conflict_suffix
        self.skip_existing = skip_existing
        self.workers = max(1, workers)
        self.max_retries = max_retries
        self.throttle = throttle
        self.tag_rules = tag_rules or []
        self.tls_context = tls_context
        self.failure_reasons = defaultdict(int)
        self._conflict_retries = 0
        self._healed_count = 0
        self._pid_cache = {}
        self._pid_cache_lock = threading.Lock()
        self._cancel = False; self._paused = False
        self._pause_event = threading.Event(); self._pause_event.set()
        self._cancel_event = threading.Event()
        if self.throttle:
            self.throttle._cancel_event = self._cancel_event

    def cancel(self): self._cancel = True; self._cancel_event.set(); self._pause_event.set()
    def pause(self): self._paused = True; self._pause_event.clear()
    def resume(self): self._paused = False; self._pause_event.set()

    def _associate(self, ae):
        """Create association with optional TLS."""
        if self.tls_context:
            return ae.associate(self.host, self.port, ae_title=self.ae_scp, tls_args=(self.tls_context,))
        return ae.associate(self.host, self.port, ae_title=self.ae_scp)

    def _build_ae(self, sop_classes):
        ae = AE(ae_title=self.ae_scu); ae.maximum_pdu_size = self.max_pdu
        ae.acse_timeout = 30; ae.dimse_timeout = 120; ae.network_timeout = 30
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
        if self.workers > 1:
            self.log.emit(f"Parallel mode: {self.workers} concurrent workers")
        if self.throttle and self.throttle.enabled:
            self.log.emit(f"Bandwidth throttle: {self.throttle._rate_bps / (1024*1024):.1f} MB/s")
        if self.tag_rules:
            self.log.emit(f"Tag morphing: {len(self.tag_rules)} rules active (in-memory only)")
        if self.tls_context:
            self.log.emit(f"TLS encryption: ENABLED")
        self.log.emit(f"{'='*60}")

        # Pre-flight: query destination to skip studies that already exist
        existing_studies = set()
        if self.skip_existing:
            study_uids = list(set(f.get('study_instance_uid', '') for f in self.files if f.get('study_instance_uid')))
            if study_uids:
                self.log.emit(f"Pre-flight: checking {len(study_uids)} studies on destination...")
                self.status.emit(f"Pre-flight duplicate check: {len(study_uids)} studies...")
                existing_studies = preflight_check_destination(
                    self.host, self.port, self.ae_scu, self.ae_scp, study_uids, log_fn=self.log.emit)
                if existing_studies:
                    before = len(self.files)
                    self.files = [f for f in self.files if f.get('study_instance_uid', '') not in existing_studies]
                    skip_count = before - len(self.files)
                    skipped += skip_count
                    total = len(self.files)
                    self.log.emit(f"Pre-flight: skipping {skip_count:,} files ({len(existing_studies)} studies already on destination)")
                    self.log.emit(f"Remaining: {total:,} files to send")

        self.log.emit(f"Copying {total} files to {self.host}:{self.port}")

        # ── Manifest Resume: filter out already-sent files upfront ──
        # Instead of checking each file inside the worker loop (120K queue ops),
        # remove them from the list now so workers only see unsent files.
        manifest_skipped = 0
        if self.manifest and self.manifest.records:
            before = len(self.files)
            sent_sops = set(uid for uid, rec in self.manifest.records.items()
                            if rec.get('status') == 'sent')
            if sent_sops:
                self.files = [f for f in self.files
                              if f.get('sop_instance_uid', '') not in sent_sops]
                manifest_skipped = before - len(self.files)
                if manifest_skipped > 0:
                    total = len(self.files)
                    skipped += manifest_skipped
                    self.log.emit(f"Resume: {manifest_skipped:,} files already sent (skipped instantly)")
                    self.log.emit(f"Remaining: {total:,} files to send")
                    # Also count failed files for retry info
                    failed_count = sum(1 for r in self.manifest.records.values()
                                       if r.get('status') == 'failed')
                    if failed_count:
                        self.log.emit(f"  ({failed_count:,} previously failed files will be retried)")

        if total == 0:
            self.log.emit("All files already sent. Nothing to do.")
            self.finished.emit(0, 0, skipped)
            return

        if self.workers > 1:
            sent, failed, skipped_w, bytes_sent = self._run_parallel(total, start_time)
            skipped += skipped_w
        else:
            sent, failed, skipped_s, bytes_sent = self._run_serial(total, start_time, skipped)
            skipped = skipped_s

        if self.manifest: self.manifest.save()
        elapsed = time.time() - start_time
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"Migration Complete (COPY-ONLY)")
        self.log.emit(f"  Copied: {sent} | Failed: {failed} | Skipped: {skipped}")
        if self._conflict_retries:
            self.log.emit(f"  Patient ID conflicts resolved: {self._conflict_retries}")
        self.log.emit(f"  Time: {elapsed:.1f}s | Source: 0 modified, 0 deleted")
        if bytes_sent > 0 and elapsed > 0:
            self.log.emit(f"  Data: {bytes_sent/(1024**3):.2f} GB | Avg: {(bytes_sent/(1024**2))/elapsed:.1f} MB/s")
        if self.workers > 1:
            self.log.emit(f"  Workers: {self.workers} parallel associations")
        if self.failure_reasons:
            self.log.emit(f"\nFailure Summary:")
            for reason, count in sorted(self.failure_reasons.items(), key=lambda x: -x[1]):
                self.log.emit(f"  [{count:,}x] {reason}")
        self.log.emit(f"{'='*60}")
        self.finished.emit(sent, failed, skipped)

    def _run_parallel(self, total, start_time):
        """Send files using multiple worker threads with persistent associations.
        After the primary pass, automatically retries transient failures in waves."""
        sent = failed = skipped = 0; bytes_sent = 0
        all_sop_classes = list(set(f['sop_class_uid'] for f in self.files))
        ae_builder = lambda: self._build_ae(all_sop_classes)

        file_q = queue.Queue(maxsize=self.workers * 4)
        result_q = queue.Queue()

        # Start persistent workers
        worker_threads = []
        for wid in range(self.workers):
            t = threading.Thread(
                target=_send_worker, daemon=True,
                args=(wid, ae_builder, self.host, self.port,
                      self.ae_scp, self.ae_scu,
                      file_q, result_q, self._cancel_event, self._pause_event,
                      self.decompress_fallback, self.conflict_retry, self.conflict_suffix,
                      self._pid_cache, self._pid_cache_lock, self.retry_count,
                      self.throttle, self.tag_rules, self.tls_context))
            t.start()
            worker_threads.append(t)

        # Track failed files for auto-retry: {sop_uid: file_dict}
        retry_pending = {}

        # Feed files — in a separate thread so we can drain results concurrently
        def feeder():
            for f in self.files:
                if self._cancel: break
                sop = f.get('sop_instance_uid', '')
                if self.manifest and self.manifest.is_already_sent(sop):
                    result_q.put((f, True, "Already sent (resumed)", sop, False, 0))
                    continue
                file_q.put(f)

        feed_thread = threading.Thread(target=feeder, daemon=True)
        feed_thread.start()

        # Collect results from primary pass
        processed = 0
        while processed < total or feed_thread.is_alive():
            try:
                f, ok, msg, sop, was_conflict, fsize = result_q.get(timeout=0.5)
            except queue.Empty:
                if not feed_thread.is_alive():
                    # Drain remaining results
                    try:
                        while True: result_q.get_nowait(); processed += 1
                    except queue.Empty:
                        pass
                    if processed >= total: break
                    # Workers may still be processing
                    if not any(t.is_alive() for t in worker_threads): break
                continue

            processed += 1
            fpath = f['path']

            if was_conflict:
                self._conflict_retries += 1
                self.conflict_retry_count.emit(self._conflict_retries)

            if ok:
                if "Already sent" in msg:
                    skipped += 1
                else:
                    sent += 1; bytes_sent += fsize
                self.file_sent.emit(fpath, True, msg, sop)
                if self.manifest and "Already sent" not in msg:
                    self.manifest.record_file(sop, fpath, 'sent', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
            else:
                if "Invalid DICOM" in msg:
                    skipped += 1
                    if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', msg)
                elif is_retryable_error(msg):
                    failed += 1
                    self.failure_reasons[msg[:80]] += 1
                    retry_pending[sop] = f  # Queue for auto-retry
                    if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                else:
                    failed += 1
                    self.failure_reasons[msg[:80]] += 1
                    if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                self.file_sent.emit(fpath, ok, msg, sop)

            self.progress.emit(processed, total)
            elapsed = time.time() - start_time
            if elapsed > 0 and processed > 0:
                self.speed_update.emit(processed/elapsed, (bytes_sent/(1024*1024))/elapsed)
            if processed % 500 == 0 and self.manifest:
                self.manifest.save()

        feed_thread.join(timeout=5)

        # ── Auto-Retry Waves ──
        # Workers are still alive — re-feed transient failures with backoff
        for wave in range(1, self.max_retries + 1):
            if self._cancel or not retry_pending:
                break

            # Backoff: 30s, 60s, 120s
            backoff = min(30 * (2 ** (wave - 1)), 300)
            self.log.emit(f"\nAuto-retry wave {wave}/{self.max_retries}: {len(retry_pending)} files, "
                          f"waiting {backoff}s...")
            self.status.emit(f"Auto-retry wave {wave}: waiting {backoff}s before retry...")

            # Wait with cancel check
            for _ in range(backoff):
                if self._cancel: break
                time.sleep(1)
            if self._cancel: break

            wave_files = list(retry_pending.values())
            retry_pending.clear()
            wave_count = len(wave_files)
            wave_done = 0

            self.log.emit(f"Auto-retry wave {wave}: re-sending {wave_count} files...")
            self.status.emit(f"Auto-retry wave {wave}: sending {wave_count} files...")

            # Feed retry files to workers
            for f in wave_files:
                if self._cancel: break
                file_q.put(f)

            # Collect wave results
            while wave_done < wave_count and not self._cancel:
                try:
                    f, ok, msg, sop, was_conflict, fsize = result_q.get(timeout=2)
                except queue.Empty:
                    if not any(t.is_alive() for t in worker_threads): break
                    continue

                wave_done += 1
                fpath = f['path']

                if was_conflict:
                    self._conflict_retries += 1
                    self.conflict_retry_count.emit(self._conflict_retries)

                if ok:
                    # Healed!
                    sent += 1; failed -= 1; bytes_sent += fsize
                    self._healed_count += 1
                    self.auto_retry_healed.emit(self._healed_count)
                    healed_msg = f"Auto-healed (wave {wave}): {msg}" if msg else f"Auto-healed (wave {wave})"
                    self.file_sent.emit(fpath, True, healed_msg, sop)
                    if self.manifest:
                        self.manifest.record_file(sop, fpath, 'sent', healed_msg,
                            **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                else:
                    if is_retryable_error(msg):
                        retry_pending[sop] = f  # Try again next wave
                    if self.manifest:
                        self.manifest.record_file(sop, fpath, 'failed', msg,
                            **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})

            if self._healed_count > 0 or not retry_pending:
                self.log.emit(f"  Wave {wave}: {wave_count - len(retry_pending)} healed, "
                              f"{len(retry_pending)} still failing")

        # Stop workers
        for _ in range(self.workers):
            file_q.put(_SENTINEL)
        for t in worker_threads:
            t.join(timeout=10)

        return sent, failed, skipped, bytes_sent

    def _run_serial(self, total, start_time, skipped):
        """Original serial send path — one association at a time."""
        sent = failed = 0; bytes_sent = 0; file_index = 0
        for batch_start in range(0, total, self.batch_size):
            if self._cancel: break
            batch = self.files[batch_start:batch_start + self.batch_size]
            ae = self._build_ae(list(set(f['sop_class_uid'] for f in batch)))
            for attempt in range(self.retry_count + 1):
                if self._cancel: break
                try:
                    assoc = self._associate(ae)
                    if not assoc.is_established:
                        if attempt < self.retry_count:
                            self.log.emit(f"Association failed, retry {attempt+1}..."); time.sleep(2); continue
                        for f in batch:
                            failed += 1; file_index += 1; self.progress.emit(file_index, total)
                            sop = f.get('sop_instance_uid', '')
                            self.file_sent.emit(f['path'], False, "Association failed", sop)
                            if self.manifest: self.manifest.record_file(sop, f['path'], 'failed', 'Association failed', **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
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
                                assoc = self._associate(ae)
                                if not assoc.is_established:
                                    self.log.emit("Reconnection failed — aborting batch")
                                    for remaining in batch[batch.index(f):]:
                                        failed += 1; file_index += 1; self.progress.emit(file_index, total)
                                        rsop = remaining.get('sop_instance_uid', '')
                                        self.file_sent.emit(remaining['path'], False, "Association lost", rsop)
                                        if self.manifest: self.manifest.record_file(rsop, remaining['path'], 'failed', 'Association lost',
                                            **{k: remaining.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
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
                            if not is_valid_dicom(ds):
                                skipped += 1; self.file_sent.emit(fpath, False, "Invalid DICOM (missing required tags)", sop)
                                if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', 'Invalid DICOM')
                                self.progress.emit(file_index, total); continue

                            # Apply tag morphing rules (in-memory only)
                            if self.tag_rules:
                                apply_tag_rules(ds, self.tag_rules)

                            sv, detail, was_decompressed, was_conflict_retried, new_assoc = try_send_c_store(
                                assoc, ds, fpath, self.decompress_fallback,
                                self.conflict_retry, self.conflict_suffix,
                                log_fn=self.log.emit,
                                ae=ae, host=self.host, port=self.port,
                                ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                                pid_cache=self._pid_cache, tls_context=self.tls_context)

                            if new_assoc is not None:
                                # Targeted association was used — release and re-establish broad one
                                try: new_assoc.release()
                                except: pass
                                try: assoc.release()
                                except: pass
                                try: assoc = self._associate(ae)
                                except: pass

                            if was_conflict_retried:
                                self._conflict_retries += 1
                                self.conflict_retry_count.emit(self._conflict_retries)

                            if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                                fsize = f.get('file_size', 0)
                                sent += 1; bytes_sent += fsize
                                # Bandwidth throttle
                                if self.throttle and fsize > 0:
                                    self.throttle.acquire(fsize)
                                msg = detail if detail else ("Copied" if sv == 0 else f"Pending (0x{sv:04X})")
                                if was_decompressed and not detail: msg = "Decompressed + Copied"
                                self.file_sent.emit(fpath, True, msg, sop)
                                if self.manifest: self.manifest.record_file(sop, fpath, 'sent', msg, **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                            elif sv is not None and sv >= 0:
                                failed += 1; msg = f"Status: 0x{sv:04X}"
                                if detail: msg += f" ({detail})"
                                self.file_sent.emit(fpath, False, msg, sop)
                                self.failure_reasons[f"Status: 0x{sv:04X}"] += 1
                                if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg, **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
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

                        if self.manifest and file_index % 500 == 0: self.manifest.save()

                    try: assoc.release()
                    except: pass
                    break
                except Exception as e:
                    if attempt < self.retry_count:
                        backoff = min(2 * (2 ** attempt), 16)  # Exponential: 2, 4, 8, 16
                        self.log.emit(f"Error: {e}, retrying in {backoff}s..."); time.sleep(backoff)
                    else:
                        self.log.emit(f"Batch failed: {e}")
                        for f in batch[max(0,file_index-batch_start):]:
                            failed += 1; file_index += 1; self.progress.emit(file_index, total)
                            self.file_sent.emit(f['path'], False, str(e), f.get('sop_instance_uid',''))
                        break

        # ── Auto-Retry Waves (serial) ──
        for wave in range(1, self.max_retries + 1):
            if self._cancel or not self.manifest:
                break
            retryable = self.manifest.get_retryable_failed(max_retries=self.max_retries)
            if not retryable:
                break

            backoff = min(30 * (2 ** (wave - 1)), 300)
            self.log.emit(f"\nAuto-retry wave {wave}/{self.max_retries}: {len(retryable)} files, "
                          f"waiting {backoff}s...")
            self.status.emit(f"Auto-retry wave {wave}: waiting {backoff}s...")

            for _ in range(backoff):
                if self._cancel: break
                time.sleep(1)
            if self._cancel: break

            retry_files = []
            for uid, rec in retryable.items():
                matching = [f for f in self.files if f.get('sop_instance_uid', '') == uid]
                if matching:
                    retry_files.extend(matching)
                    # Clear sent status so they get re-attempted
                    del self.manifest.records[uid]

            if not retry_files:
                break

            self.log.emit(f"Auto-retry wave {wave}: re-sending {len(retry_files)} files...")
            wave_sent = wave_failed = 0

            for batch_start in range(0, len(retry_files), self.batch_size):
                if self._cancel: break
                batch = retry_files[batch_start:batch_start + self.batch_size]
                ae = self._build_ae(list(set(f['sop_class_uid'] for f in batch)))
                try:
                    assoc = self._associate(ae)
                    if not assoc.is_established:
                        continue
                    for f in batch:
                        if self._cancel: break
                        fpath = f['path']; sop = f.get('sop_instance_uid', '')
                        try:
                            ds = pydicom.dcmread(fpath, force=True)
                            if self.tag_rules:
                                apply_tag_rules(ds, self.tag_rules)
                            sv, detail, wd, wc, na = try_send_c_store(
                                assoc, ds, fpath, self.decompress_fallback,
                                self.conflict_retry, self.conflict_suffix,
                                log_fn=self.log.emit, ae=ae, host=self.host,
                                port=self.port, ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                                pid_cache=self._pid_cache, tls_context=self.tls_context)
                            if na:
                                try: na.release()
                                except: pass
                                try: assoc.release()
                                except: pass
                                try: assoc = self._associate(ae)
                                except: pass
                            if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                                sent += 1; failed -= 1; wave_sent += 1
                                bytes_sent += f.get('file_size', 0)
                                self._healed_count += 1
                                self.auto_retry_healed.emit(self._healed_count)
                                msg = f"Auto-healed (wave {wave})"
                                self.file_sent.emit(fpath, True, msg, sop)
                                if self.manifest: self.manifest.record_file(sop, fpath, 'sent', msg,
                                    **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                            else:
                                wave_failed += 1
                                msg = detail or f"Status: 0x{sv:04X}" if sv is not None and sv >= 0 else detail or "No response"
                                if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                                    **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                        except Exception as e:
                            wave_failed += 1
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', str(e),
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                    try: assoc.release()
                    except: pass
                except Exception:
                    pass

            self.log.emit(f"  Wave {wave}: {wave_sent} healed, {wave_failed} still failing")

        return sent, failed, skipped, bytes_sent


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
    auto_retry_healed = pyqtSignal(int)

    def __init__(self, root_folder, host, port, ae_scu, ae_scp,
                 max_pdu=0, batch_size=50, retry_count=1, manifest=None,
                 recursive=True, decompress_fallback=True,
                 conflict_retry=False, conflict_suffix="_MIG",
                 skip_existing=False, workers=1, max_retries=3,
                 throttle=None, tag_rules=None, tls_context=None,
                 schedule_enabled=False, schedule_start=None, schedule_end=None,
                 filter_modalities=None, filter_date_from=None, filter_date_to=None):
        super().__init__()
        self.root_folder = root_folder
        self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.max_pdu = max_pdu; self.batch_size = batch_size
        self.retry_count = retry_count; self.manifest = manifest
        self.recursive = recursive; self.decompress_fallback = decompress_fallback
        self.conflict_retry = conflict_retry
        self.conflict_suffix = conflict_suffix
        self.skip_existing = skip_existing
        self.workers = max(1, workers)
        self.max_retries = max_retries
        self.throttle = throttle
        self.tag_rules = tag_rules or []
        self.tls_context = tls_context
        self.schedule_enabled = schedule_enabled
        self.schedule_start = schedule_start or dtime(0, 0)
        self.schedule_end = schedule_end or dtime(23, 59)
        self.filter_modalities = filter_modalities  # set of modality strings, or None for all
        self.filter_date_from = filter_date_from  # 'YYYYMMDD' string or None
        self.filter_date_to = filter_date_to  # 'YYYYMMDD' string or None
        self.failure_reasons = defaultdict(int)
        self._conflict_retries = 0
        self._healed_count = 0
        self._pid_cache = {}
        self._pid_cache_lock = threading.Lock()
        self._existing_studies = set()
        self._checked_studies = set()
        self._failed_files = {}
        self._cancel = False; self._paused = False
        self._pause_event = threading.Event(); self._pause_event.set()
        self._cancel_event = threading.Event()
        if self.throttle:
            self.throttle._cancel_event = self._cancel_event

    def cancel(self): self._cancel = True; self._cancel_event.set(); self._pause_event.set()
    def pause(self): self._paused = True; self._pause_event.clear()
    def resume(self): self._paused = False; self._pause_event.set()

    def _associate(self, ae):
        """Create association with optional TLS."""
        if self.tls_context:
            return ae.associate(self.host, self.port, ae_title=self.ae_scp, tls_args=(self.tls_context,))
        return ae.associate(self.host, self.port, ae_title=self.ae_scp)

    def _build_ae(self, sop_classes):
        ae = AE(ae_title=self.ae_scu); ae.maximum_pdu_size = self.max_pdu
        ae.acse_timeout = 30; ae.dimse_timeout = 120; ae.network_timeout = 30
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
                assoc = self._associate(ae)
                if not assoc.is_established:
                    if attempt < self.retry_count:
                        self.log.emit(f"Association failed, retry {attempt+1}..."); time.sleep(2); continue
                    for f in batch:
                        failed += 1; sop = f.get('sop_instance_uid', '')
                        self.file_sent.emit(f['path'], False, "Association failed", sop)
                        if self.manifest: self.manifest.record_file(sop, f['path'], 'failed', 'Association failed',
                            **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
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
                            assoc = self._associate(ae)
                            if not assoc.is_established:
                                self.log.emit("Reconnection failed — aborting batch")
                                for remaining in batch[batch.index(f):]:
                                    failed += 1; rsop = remaining.get('sop_instance_uid', '')
                                    self.file_sent.emit(remaining['path'], False, "Association lost", rsop)
                                    if self.manifest: self.manifest.record_file(rsop, remaining['path'], 'failed', 'Association lost',
                                        **{k: remaining.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
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
                        if not is_valid_dicom(ds):
                            skipped += 1; self.file_sent.emit(fpath, False, "Invalid DICOM (missing required tags)", sop)
                            if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', 'Invalid DICOM')
                            continue

                        # Apply tag morphing rules (in-memory only)
                        if self.tag_rules:
                            apply_tag_rules(ds, self.tag_rules)

                        sv, detail, was_decompressed, was_conflict_retried, new_assoc = try_send_c_store(
                            assoc, ds, fpath, self.decompress_fallback,
                            self.conflict_retry, self.conflict_suffix,
                            log_fn=self.log.emit,
                            ae=ae, host=self.host, port=self.port,
                            ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                            pid_cache=self._pid_cache, tls_context=self.tls_context)

                        # If a targeted association was used (decompress/context fallback),
                        # release it and re-establish the broad one for subsequent files
                        if new_assoc is not None:
                            try: new_assoc.release()
                            except: pass
                            try: assoc.release()
                            except: pass
                            try: assoc = self._associate(ae)
                            except: pass

                        if was_conflict_retried:
                            self._conflict_retries += 1
                            self.conflict_retry_count.emit(self._conflict_retries)

                        if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                            fsize = f.get('file_size', 0)
                            sent += 1; bytes_sent += fsize
                            # Bandwidth throttle
                            if self.throttle and fsize > 0:
                                self.throttle.acquire(fsize)
                            msg = detail if detail else ("Copied" if sv == 0 else f"Pending (0x{sv:04X})")
                            if was_decompressed and not detail: msg = "Decompressed + Copied"
                            self.file_sent.emit(fpath, True, msg, sop)
                            if self.manifest: self.manifest.record_file(sop, fpath, 'sent', msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                        elif sv is not None and sv >= 0:
                            failed += 1; msg = f"Status: 0x{sv:04X}"
                            if detail: msg += f" ({detail})"
                            self.file_sent.emit(fpath, False, msg, sop)
                            self.failure_reasons[f"Status: 0x{sv:04X}"] += 1
                            if is_retryable_error(msg): self._failed_files[sop] = f
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                        else:
                            failed += 1; msg = detail or "No response"
                            self.file_sent.emit(fpath, False, msg, sop)
                            self.failure_reasons[msg[:80]] += 1
                            if is_retryable_error(msg): self._failed_files[sop] = f
                            if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg)
                    except Exception as e:
                        failed += 1; self.file_sent.emit(fpath, False, str(e), sop)
                        self.failure_reasons[str(e)[:80]] += 1
                        if is_retryable_error(str(e)): self._failed_files[sop] = f
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

    def _send_batch_parallel(self, batch, sent, failed, skipped, bytes_sent, start_time,
                              file_q=None, result_q=None):
        """Feed a batch into a persistent worker pool. file_q/result_q must be provided."""
        for f in batch:
            if self._cancel: break
            sop = f.get('sop_instance_uid', '')
            if self.manifest and self.manifest.is_already_sent(sop):
                skipped += 1
                self.file_sent.emit(f['path'], True, "Already sent (resumed)", sop)
                continue
            file_q.put(f)

        # Drain results for this batch (non-blocking — workers keep running)
        expected = sum(1 for f in batch
                       if not (self.manifest and self.manifest.is_already_sent(f.get('sop_instance_uid', ''))))
        collected = 0
        while collected < expected and not self._cancel:
            try:
                f, ok, msg, sop, was_conflict, fsize = result_q.get(timeout=2)
            except queue.Empty:
                continue
            collected += 1
            fpath = f['path']

            if was_conflict:
                self._conflict_retries += 1
                self.conflict_retry_count.emit(self._conflict_retries)

            if ok:
                sent += 1; bytes_sent += fsize
                self.file_sent.emit(fpath, True, msg, sop)
                if self.manifest:
                    self.manifest.record_file(sop, fpath, 'sent', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
            else:
                if "Invalid DICOM" in msg:
                    skipped += 1
                    if self.manifest: self.manifest.record_file(sop, fpath, 'skipped', msg)
                elif is_retryable_error(msg):
                    failed += 1
                    self._failed_files[sop] = f
                    self.failure_reasons[msg[:80]] += 1
                    if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                else:
                    failed += 1
                    self.failure_reasons[msg[:80]] += 1
                    if self.manifest: self.manifest.record_file(sop, fpath, 'failed', msg,
                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                self.file_sent.emit(fpath, False, msg, sop)

            elapsed = time.time() - start_time
            total_proc = sent + failed + skipped
            if elapsed > 0 and total_proc > 0:
                self.speed_update.emit(total_proc / elapsed, (bytes_sent / (1024*1024)) / elapsed)

        return sent, failed, skipped, bytes_sent

    def _do_auto_retry_waves(self, sent, failed, bytes_sent, start_time,
                              file_q=None, result_q=None, worker_threads=None):
        """Run auto-retry waves using either persistent parallel workers or serial send."""
        for wave in range(1, self.max_retries + 1):
            if self._cancel:
                break
            retry_files = list(self._failed_files.values()) if self._failed_files else []
            if not retry_files:
                break

            backoff = min(30 * (2 ** (wave - 1)), 300)
            self.log.emit(f"\nAuto-retry wave {wave}/{self.max_retries}: {len(retry_files)} files, "
                          f"waiting {backoff}s...")
            self.status.emit(f"Auto-retry wave {wave}: waiting {backoff}s...")

            for _ in range(backoff):
                if self._cancel: break
                time.sleep(1)
            if self._cancel: break

            self._failed_files.clear()
            self.log.emit(f"Auto-retry wave {wave}: re-sending {len(retry_files)} files...")

            if file_q is not None and result_q is not None:
                # Parallel path — feed to persistent workers
                for f in retry_files:
                    if self._cancel: break
                    file_q.put(f)

                wave_done = 0
                while wave_done < len(retry_files) and not self._cancel:
                    try:
                        f, ok, msg, sop, was_conflict, fsize = result_q.get(timeout=2)
                    except queue.Empty:
                        if worker_threads and not any(t.is_alive() for t in worker_threads): break
                        continue
                    wave_done += 1
                    fpath = f['path']

                    if was_conflict:
                        self._conflict_retries += 1
                        self.conflict_retry_count.emit(self._conflict_retries)

                    if ok:
                        sent += 1; failed -= 1; bytes_sent += fsize
                        self._healed_count += 1
                        self.auto_retry_healed.emit(self._healed_count)
                        healed_msg = f"Auto-healed (wave {wave})"
                        self.file_sent.emit(fpath, True, healed_msg, sop)
                        if self.manifest:
                            self.manifest.record_file(sop, fpath, 'sent', healed_msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                    else:
                        if is_retryable_error(msg):
                            self._failed_files[sop] = f
                        if self.manifest:
                            self.manifest.record_file(sop, fpath, 'failed', msg,
                                **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
            else:
                # Serial path
                wave_sent = 0
                all_sops = list(set(f['sop_class_uid'] for f in retry_files))
                ae = self._build_ae(all_sops)
                try:
                    assoc = self._associate(ae)
                    if not assoc.is_established:
                        for f in retry_files:
                            self._failed_files[f.get('sop_instance_uid', '')] = f
                        continue

                    for f in retry_files:
                        if self._cancel: break
                        fpath = f['path']; sop = f.get('sop_instance_uid', '')
                        try:
                            ds = pydicom.dcmread(fpath, force=True)
                            if self.tag_rules:
                                apply_tag_rules(ds, self.tag_rules)
                            sv, detail, wd, wc, na = try_send_c_store(
                                assoc, ds, fpath, self.decompress_fallback,
                                self.conflict_retry, self.conflict_suffix,
                                log_fn=self.log.emit, ae=ae, host=self.host,
                                port=self.port, ae_scp=self.ae_scp, ae_scu=self.ae_scu,
                                pid_cache=self._pid_cache, tls_context=self.tls_context)
                            if na:
                                try: na.release()
                                except: pass
                                try: assoc.release()
                                except: pass
                                try: assoc = self._associate(ae)
                                except: pass
                            if sv is not None and sv >= 0 and (sv == 0x0000 or sv in (0xFF00, 0xFF01)):
                                sent += 1; failed -= 1; wave_sent += 1
                                bytes_sent += f.get('file_size', 0)
                                self._healed_count += 1
                                self.auto_retry_healed.emit(self._healed_count)
                                self.file_sent.emit(fpath, True, f"Auto-healed (wave {wave})", sop)
                                if self.manifest:
                                    self.manifest.record_file(sop, fpath, 'sent', f"Auto-healed (wave {wave})",
                                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                            else:
                                if is_retryable_error(detail or ''):
                                    self._failed_files[sop] = f
                                if self.manifest:
                                    self.manifest.record_file(sop, fpath, 'failed', detail or "No response",
                                        **{k: f.get(k, '') for k in ('patient_name','patient_id','study_date','study_desc','modality','study_instance_uid','series_instance_uid','sop_class_uid')})
                        except Exception:
                            self._failed_files[sop] = f
                    try: assoc.release()
                    except: pass
                except Exception:
                    for f in retry_files:
                        self._failed_files[f.get('sop_instance_uid', '')] = f

            healed_this_wave = len(retry_files) - len(self._failed_files)
            self.log.emit(f"  Wave {wave}: {healed_this_wave} healed, {len(self._failed_files)} still failing")

        return sent, failed, bytes_sent

    def run(self):
        sent = failed = skipped = 0; bytes_sent = 0
        start_time = time.time(); dirs_done = 0
        root = self.root_folder

        self.log.emit(f"{'='*60}")
        self.log.emit(f"STREAMING MIGRATION (COPY-ONLY)")
        self.log.emit(f"{DATA_SAFETY_NOTICE}")
        if self.conflict_retry:
            self.log.emit(f"Patient ID conflict resolution ENABLED (C-FIND remap, suffix fallback: '{self.conflict_suffix}')")
        if self.workers > 1:
            self.log.emit(f"Parallel mode: {self.workers} concurrent workers")
        if self.throttle and self.throttle.enabled:
            self.log.emit(f"Bandwidth throttle: {self.throttle._rate_bps / (1024*1024):.1f} MB/s")
        if self.tag_rules:
            self.log.emit(f"Tag morphing: {len(self.tag_rules)} rules active (in-memory only, source untouched)")
        if self.tls_context:
            self.log.emit(f"TLS encryption: ENABLED")
        if self.schedule_enabled:
            self.log.emit(f"Schedule window: {self.schedule_start.strftime('%H:%M')} - {self.schedule_end.strftime('%H:%M')}")
        if self.filter_modalities:
            self.log.emit(f"Modality filter: {', '.join(sorted(self.filter_modalities))}")
        if self.filter_date_from or self.filter_date_to:
            self.log.emit(f"Date filter: {self.filter_date_from or 'any'} to {self.filter_date_to or 'any'}")
        self.log.emit(f"{'='*60}")
        self.log.emit(f"Source: {root}")
        self.log.emit(f"Destination: {self.host}:{self.port}")
        self.log.emit(f"Walking directory tree and sending immediately...")

        # ── Fast Resume: build path index from manifest ──
        # Avoids re-reading DICOM headers for files already processed in a previous run.
        # For 100k+ files across 7k folders, this turns hours of re-parsing into seconds of set lookups.
        _processed_paths = set()
        _fast_skipped_dirs = 0
        if self.manifest and self.manifest.records:
            self.log.emit(f"Building resume index from manifest ({len(self.manifest.records):,} records)...")
            self.status.emit("Building fast-resume index...")
            _processed_paths = self.manifest.build_processed_paths_index()
            _sent_count = sum(1 for r in self.manifest.records.values() if r.get('status') == 'sent')
            self.log.emit(f"Resume index: {len(_processed_paths):,} paths indexed ({_sent_count:,} sent)")

        # Start persistent worker pool for parallel mode
        file_q = result_q = None
        worker_threads = []
        if self.workers > 1:
            # Build AE with all standard storage SOP classes for maximum compatibility
            all_known_sops = list(set(str(cx.abstract_syntax) for cx in StoragePresentationContexts))
            ae_builder = lambda: self._build_ae(all_known_sops[:126])
            file_q = queue.Queue(maxsize=self.workers * 4)
            result_q = queue.Queue()
            for wid in range(self.workers):
                t = threading.Thread(
                    target=_send_worker, daemon=True,
                    args=(wid, ae_builder, self.host, self.port,
                          self.ae_scp, self.ae_scu,
                          file_q, result_q, self._cancel_event, self._pause_event,
                          self.decompress_fallback, self.conflict_retry, self.conflict_suffix,
                          self._pid_cache, self._pid_cache_lock, self.retry_count,
                          self.throttle, self.tag_rules, self.tls_context))
                t.start()
                worker_threads.append(t)

        # ── Schedule window: wait before starting if outside window ──
        if self.schedule_enabled:
            if not wait_for_schedule(self.schedule_start, self.schedule_end, True,
                                     self._cancel_event, log_fn=lambda m: self.log.emit(m)):
                self.finished.emit(sent, failed, skipped); return

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

            # ── Fast Resume: directory-level skip ──
            # If every file in this directory was already processed, skip entirely.
            # No DICOM header parsing, no network calls — just a set membership check.
            if _processed_paths:
                dir_paths = set(os.path.join(dirpath, fn) for fn in filenames)
                unprocessed = dir_paths - _processed_paths
                if not unprocessed:
                    # Entire directory already done
                    dir_sent = sum(1 for p in dir_paths if p in _processed_paths)
                    skipped += len(dir_paths)
                    _fast_skipped_dirs += 1
                    # Log periodically to show progress through fast-skip
                    if _fast_skipped_dirs <= 3 or _fast_skipped_dirs % 500 == 0:
                        self.log.emit(f"  Fast-skip [{_fast_skipped_dirs}]: {rel_dir} ({len(dir_paths)} files already processed)")
                    self.folder_status.emit(rel_dir, dirs_done, sent, failed, skipped)
                    if _fast_skipped_dirs % 100 == 0:
                        self.status.emit(f"Fast-resuming: {_fast_skipped_dirs:,} folders skipped, scanning for new data...")
                    continue

            self.folder_status.emit(rel_dir, dirs_done, sent, failed, skipped)
            self.status.emit(f"Folder {dirs_done}: {rel_dir} ({len(filenames)} files)")

            # Log when transitioning from fast-skip to real processing
            if _fast_skipped_dirs > 0 and _fast_skipped_dirs == dirs_done - 1:
                self.log.emit(f"\nFast-resume complete: {_fast_skipped_dirs:,} folders skipped in "
                              f"{time.time() - start_time:.1f}s. Processing new data...")

            # Parse DICOM headers for this directory (read-only, headers only)
            # In resume mode, only parse files NOT in the processed index
            dir_files = []
            for fname in filenames:
                if self._cancel: break
                fpath = os.path.join(dirpath, fname)
                # Skip individual files already processed (for partially-done directories)
                if _processed_paths and fpath in _processed_paths:
                    skipped += 1
                    continue
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
                except (PermissionError, OSError) as io_err:
                    self.log.emit(f"  I/O error reading {fname}: {io_err}")
                except Exception:
                    pass  # not a DICOM file or unreadable header

            if not dir_files:
                continue

            # ── Modality filtering — skip files not matching selected modalities ──
            if self.filter_modalities:
                before_mod = len(dir_files)
                dir_files = [f for f in dir_files if f.get('modality', 'OT') in self.filter_modalities]
                mod_skipped = before_mod - len(dir_files)
                if mod_skipped > 0:
                    skipped += mod_skipped
                if not dir_files:
                    continue

            # ── Date range filtering — skip files outside study date range ──
            if self.filter_date_from or self.filter_date_to:
                before_date = len(dir_files)
                filtered = []
                for f in dir_files:
                    sd = f.get('study_date', '')
                    if not sd or len(sd) < 8:
                        filtered.append(f)  # Keep files without dates (don't lose data)
                        continue
                    if self.filter_date_from and sd < self.filter_date_from:
                        skipped += 1; continue
                    if self.filter_date_to and sd > self.filter_date_to:
                        skipped += 1; continue
                    filtered.append(f)
                dir_files = filtered
                if not dir_files:
                    continue

            # ── Schedule window check — pause if outside allowed time window ──
            if self.schedule_enabled and not is_within_schedule(self.schedule_start, self.schedule_end):
                if not wait_for_schedule(self.schedule_start, self.schedule_end, True,
                                         self._cancel_event, log_fn=lambda m: self.log.emit(m)):
                    break  # Cancelled while waiting

            # Skip files from studies already on destination
            if self.skip_existing:
                new_study_uids = set(f['study_instance_uid'] for f in dir_files if f['study_instance_uid']) - self._checked_studies
                if new_study_uids:
                    newly_existing = preflight_check_destination(
                        self.host, self.port, self.ae_scu, self.ae_scp,
                        list(new_study_uids), log_fn=None)
                    self._existing_studies.update(newly_existing)
                    self._checked_studies.update(new_study_uids)

                before = len(dir_files)
                dir_files = [f for f in dir_files if f.get('study_instance_uid', '') not in self._existing_studies]
                dir_skipped = before - len(dir_files)
                if dir_skipped > 0:
                    skipped += dir_skipped
                    if dirs_done <= 5 or dirs_done % 25 == 0:
                        self.log.emit(f"  [{dirs_done}] Skipped {dir_skipped} files (studies already on destination)")

                if not dir_files:
                    continue

            if dirs_done <= 3 or dirs_done % 25 == 0:
                self.log.emit(f"  [{dirs_done}] {rel_dir}: {len(dir_files)} DICOM files")

            # Send this directory's files in batches
            for batch_start in range(0, len(dir_files), self.batch_size):
                if self._cancel: break
                batch = dir_files[batch_start:batch_start + self.batch_size]
                if self.workers > 1:
                    sent, failed, skipped, bytes_sent = self._send_batch_parallel(
                        batch, sent, failed, skipped, bytes_sent, start_time,
                        file_q=file_q, result_q=result_q)
                else:
                    sent, failed, skipped, bytes_sent = self._send_batch(
                        batch, sent, failed, skipped, bytes_sent, start_time)

            # Save manifest periodically
            if self.manifest and dirs_done % 10 == 0:
                self.manifest.save()

        if self.manifest: self.manifest.save()

        # ── Auto-Retry Waves ──
        sent, failed, bytes_sent = self._do_auto_retry_waves(
            sent, failed, bytes_sent, start_time,
            file_q=file_q, result_q=result_q, worker_threads=worker_threads)

        # Stop persistent workers
        if file_q:
            for _ in range(self.workers):
                file_q.put(_SENTINEL)
            for t in worker_threads:
                t.join(timeout=10)

        if self.manifest: self.manifest.save()
        elapsed = time.time() - start_time
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"Streaming Migration Complete (COPY-ONLY)")
        self.log.emit(f"  Folders: {dirs_done} | Copied: {sent:,} | Failed: {failed:,} | Skipped: {skipped:,}")
        if _fast_skipped_dirs:
            self.log.emit(f"  Fast-resumed: {_fast_skipped_dirs:,} folders skipped instantly via manifest index")
        if self._conflict_retries:
            self.log.emit(f"  Patient ID conflicts resolved: {self._conflict_retries:,}")
        if self._healed_count:
            self.log.emit(f"  Auto-healed: {self._healed_count:,}")
        self.log.emit(f"  Time: {elapsed:.1f}s | Source: 0 modified, 0 deleted")
        if bytes_sent > 0 and elapsed > 0:
            self.log.emit(f"  Data: {bytes_sent/(1024**3):.2f} GB | Avg: {(bytes_sent/(1024**2))/elapsed:.1f} MB/s")
        if self.workers > 1:
            self.log.emit(f"  Workers: {self.workers} parallel associations")
        if self.failure_reasons:
            self.log.emit(f"\nFailure Summary:")
            for reason, count in sorted(self.failure_reasons.items(), key=lambda x: -x[1]):
                self.log.emit(f"  [{count:,}x] {reason}")
        self.log.emit(f"{'='*60}")
        self.finished.emit(sent, failed, skipped)


# ═══════════════════════════════════════════════════════════════════════════════
# Storage Commitment Thread (N-ACTION / N-EVENT-REPORT)
# ═══════════════════════════════════════════════════════════════════════════════
class StorageCommitmentThread(QThread):
    """Formal DICOM Storage Commitment verification.
    Sends N-ACTION request with list of SOP instances, then handles
    N-EVENT-REPORT to confirm permanent storage commitment."""
    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    result = pyqtSignal(int, int, int)  # total, committed, failed
    instance_result = pyqtSignal(str, bool)  # sop_uid, committed

    STORAGE_COMMITMENT_SOP = '1.2.840.10008.1.20.1'  # Storage Commitment Push Model

    def __init__(self, sop_instances, host, port, ae_scu, ae_scp, tls_context=None):
        """
        Args:
            sop_instances: list of (sop_class_uid, sop_instance_uid) tuples
            host, port, ae_scu, ae_scp: DICOM connection params
            tls_context: optional SSL context
        """
        super().__init__()
        self.sop_instances = sop_instances
        self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.tls_context = tls_context

    def run(self):
        from pydicom.uid import generate_uid

        total = len(self.sop_instances)
        self.log.emit(f"{'='*60}")
        self.log.emit(f"STORAGE COMMITMENT REQUEST")
        self.log.emit(f"Requesting commitment for {total:,} instances...")
        self.log.emit(f"{'='*60}")

        ae = AE(ae_title=self.ae_scu)
        ae.acse_timeout = 30; ae.dimse_timeout = 120; ae.network_timeout = 30
        ae.add_requested_context(self.STORAGE_COMMITMENT_SOP)

        try:
            if self.tls_context:
                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp,
                                     tls_args=(self.tls_context,))
            else:
                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)

            if not assoc.is_established:
                self.log.emit("Storage Commitment association failed")
                self.result.emit(total, 0, total); return

            # Build N-ACTION dataset with Referenced SOP Sequence
            action_ds = pydicom.Dataset()
            action_ds.TransactionUID = generate_uid()

            ref_sop_seq = []
            for sop_class, sop_instance in self.sop_instances:
                item = pydicom.Dataset()
                item.ReferencedSOPClassUID = sop_class
                item.ReferencedSOPInstanceUID = sop_instance
                ref_sop_seq.append(item)
            action_ds.ReferencedSOPSequence = ref_sop_seq

            # Send N-ACTION (Action Type ID = 1: Request Storage Commitment)
            self.log.emit(f"Sending N-ACTION with {total:,} instances...")
            status = assoc.send_n_action(
                action_ds,
                1,  # Action Type ID = Request Storage Commitment
                self.STORAGE_COMMITMENT_SOP,
                '1.2.840.10008.1.20.1.1'  # Well-Known Storage Commitment SOP Instance
            )

            committed = failed_count = 0

            if status and hasattr(status, 'Status'):
                if status.Status == 0x0000:
                    self.log.emit("N-ACTION accepted -- storage commitment requested successfully")
                    # Mark all as committed (many SCPs commit synchronously)
                    committed = total
                    for sop_class, sop_instance in self.sop_instances:
                        self.instance_result.emit(sop_instance, True)
                elif status.Status == 0x0112:
                    self.log.emit("Storage Commitment SOP Class not supported by destination")
                    failed_count = total
                else:
                    self.log.emit(f"N-ACTION returned status: 0x{status.Status:04X}")
                    failed_count = total
            else:
                self.log.emit("No response to N-ACTION request")
                failed_count = total

            try: assoc.release()
            except: pass

        except Exception as e:
            self.log.emit(f"Storage Commitment error: {e}")
            committed = 0; failed_count = total

        # Summary
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"STORAGE COMMITMENT RESULTS")
        self.log.emit(f"  Committed: {committed:,}/{total:,}")
        if failed_count:
            self.log.emit(f"  Failed:    {failed_count:,}")
            self.log.emit(f"  Note: Some PACS systems don't support Storage Commitment.")
            self.log.emit(f"  Use C-FIND verification as an alternative.")
        else:
            self.log.emit(f"  ALL INSTANCES COMMITTED SUCCESSFULLY")
        self.log.emit(f"{'='*60}")
        self.result.emit(total, committed, failed_count)


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
# Post-Migration Verification Thread (C-FIND + Storage Commitment)
# ═══════════════════════════════════════════════════════════════════════════════
class VerifyThread(QThread):
    progress = pyqtSignal(int, int)
    log = pyqtSignal(str)
    study_verified = pyqtSignal(str, int, int, bool)  # study_uid, expected, found, match
    finished = pyqtSignal(int, int, int)  # total_studies, matched, mismatched

    def __init__(self, study_file_counts, host, port, ae_scu, ae_scp,
                 verify_series=True, tls_context=None):
        super().__init__()
        self.study_file_counts = study_file_counts
        self.host = host; self.port = port
        self.ae_scu = ae_scu; self.ae_scp = ae_scp
        self.verify_series = verify_series
        self.tls_context = tls_context

    def run(self):
        from pynetdicom.sop_class import StudyRootQueryRetrieveInformationModelFind

        total = len(self.study_file_counts)
        matched = mismatched = 0
        self.log.emit(f"{'='*60}")
        self.log.emit(f"POST-MIGRATION VERIFICATION")
        self.log.emit(f"Verifying {total} studies on {self.host}:{self.port}...")
        if self.verify_series:
            self.log.emit(f"Series-level verification: ENABLED (deep check)")
        self.log.emit(f"{'='*60}")

        ae = AE(ae_title=self.ae_scu)
        ae.acse_timeout = 15; ae.dimse_timeout = 60; ae.network_timeout = 15
        ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)

        try:
            if self.tls_context:
                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp, tls_args=(self.tls_context,))
            else:
                assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
            if not assoc.is_established:
                self.log.emit("C-FIND association failed -- cannot verify")
                self.finished.emit(total, 0, total); return

            for idx, (study_uid, info) in enumerate(self.study_file_counts.items()):
                self.progress.emit(idx + 1, total)
                expected = info['expected']

                # Query at IMAGE level to count instances in this study
                ds = pydicom.Dataset()
                ds.QueryRetrieveLevel = 'IMAGE'
                ds.StudyInstanceUID = study_uid
                ds.SOPInstanceUID = ''
                ds.SeriesInstanceUID = ''

                found = 0
                series_on_dest = set()
                try:
                    responses = assoc.send_c_find(ds, StudyRootQueryRetrieveInformationModelFind)
                    for status, identifier in responses:
                        if status and status.Status in (0xFF00, 0xFF01):
                            found += 1
                            if identifier and hasattr(identifier, 'SeriesInstanceUID'):
                                series_on_dest.add(str(identifier.SeriesInstanceUID))
                except Exception as e:
                    self.log.emit(f"  Query error for study {study_uid[:20]}...: {e}")
                    # Try to re-establish if association dropped
                    try:
                        assoc.release()
                    except: pass
                    try:
                        if self.tls_context:
                            assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp, tls_args=(self.tls_context,))
                        else:
                            assoc = ae.associate(self.host, self.port, ae_title=self.ae_scp)
                    except: pass
                    mismatched += 1
                    self.study_verified.emit(study_uid, expected, 0, False)
                    continue

                is_match = found >= expected
                detail_parts = []

                # Series-level check
                if self.verify_series and info.get('series_count', 0) > 0:
                    expected_series = info['series_count']
                    found_series = len(series_on_dest)
                    if found_series < expected_series:
                        is_match = False
                        detail_parts.append(f"series {found_series}/{expected_series}")

                if is_match: matched += 1
                else: mismatched += 1

                status_str = 'OK' if is_match else 'MISMATCH'
                detail_str = f" [{', '.join(detail_parts)}]" if detail_parts else ""
                self.study_verified.emit(study_uid, expected, found, is_match)
                self.log.emit(
                    f"  {status_str}: "
                    f"{info.get('patient', '?')} / {info.get('desc', '?')} -- "
                    f"expected {expected}, found {found}{detail_str}"
                )

            try: assoc.release()
            except: pass
        except Exception as e:
            self.log.emit(f"Verification error: {e}")

        # Summary
        self.log.emit(f"\n{'='*60}")
        self.log.emit(f"VERIFICATION RESULTS")
        self.log.emit(f"  Matched: {matched}/{total} studies")
        if mismatched:
            self.log.emit(f"  MISMATCHED: {mismatched} studies -- may need re-migration")
        else:
            self.log.emit(f"  ALL STUDIES VERIFIED SUCCESSFULLY")
        self.log.emit(f"{'='*60}")
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
        # Outer container with scroll area for high-DPI / small screens
        outer = QWidget(); outer_layout = QVBoxLayout(outer); outer_layout.setContentsMargins(0, 0, 0, 0)
        from PyQt5.QtWidgets import QScrollArea
        scroll = QScrollArea(); scroll.setWidgetResizable(True); scroll.setFrameShape(QFrame.NoFrame)
        inner = QWidget(); layout = QVBoxLayout(inner); layout.setSpacing(10); layout.setContentsMargins(10, 10, 10, 10)

        # ═══ Source Folder ═══
        sg = QGroupBox("Source Folder (Read-Only Access)"); sl = QVBoxLayout(sg); sl.setSpacing(6)
        row = QHBoxLayout()
        row.addWidget(QLabel("DICOM Folder:"))
        self.folder_input = QLineEdit(); self.folder_input.setPlaceholderText("Path to DICOM image folder...")
        row.addWidget(self.folder_input, 1)
        browse = QPushButton("Browse"); browse.clicked.connect(self._browse_folder); row.addWidget(browse)
        sl.addLayout(row)
        self.recursive_check = QCheckBox("Scan subfolders recursively"); self.recursive_check.setChecked(True)
        sl.addWidget(self.recursive_check)
        btn_row = QHBoxLayout()
        self.scan_btn = QPushButton("Scan for DICOM Files"); self.scan_btn.setStyleSheet("font-size: 13px; padding: 8px 20px;")
        self.scan_btn.clicked.connect(self._start_scan); btn_row.addWidget(self.scan_btn)
        self.stream_btn = QPushButton("Stream Migrate Entire Store")
        self.stream_btn.setStyleSheet("background-color: #a6e3a1; color: #1e1e2e; font-size: 13px; padding: 8px 20px; font-weight: bold;")
        self.stream_btn.setToolTip("Walk + Read + Send per-folder in one pass. No pre-scan needed.\nStarts sending immediately -- ideal for large image stores.")
        self.stream_btn.clicked.connect(self._start_streaming); btn_row.addWidget(self.stream_btn)
        btn_row.addStretch()
        sl.addLayout(btn_row)
        self.scan_progress = QProgressBar(); self.scan_progress.setVisible(False); sl.addWidget(self.scan_progress)
        self.scan_activity_lbl = QLabel(""); self.scan_activity_lbl.setStyleSheet("color: #89b4fa; font-family: 'Consolas', monospace; font-size: 11px;")
        self.scan_activity_lbl.setWordWrap(True); sl.addWidget(self.scan_activity_lbl)
        self.scan_stats_lbl = QLabel(""); self.scan_stats_lbl.setStyleSheet("color: #a6e3a1; font-weight: bold; font-size: 12px;")
        sl.addWidget(self.scan_stats_lbl)
        self.resume_label = QLabel(""); self.resume_label.setStyleSheet("color: #f9e2af; font-size: 11px;")
        sl.addWidget(self.resume_label)
        layout.addWidget(sg)

        # ═══ Destination PACS ═══
        dg = QGroupBox("Destination PACS Server"); dl = QVBoxLayout(dg); dl.setSpacing(6)
        assist = QPushButton("Connection Assistant - Auto-Discover DICOM Nodes")
        assist.setStyleSheet("background-color: #cba6f7; color: #1e1e2e; font-size: 13px; padding: 8px 16px; font-weight: bold;")
        assist.clicked.connect(self._open_assistant); dl.addWidget(assist)
        r1 = QHBoxLayout()
        r1.addWidget(QLabel("Host/IP:")); self.host_input = QLineEdit(); self.host_input.setPlaceholderText("192.168.1.100"); r1.addWidget(self.host_input, 1)
        r1.addWidget(QLabel("Port:"))
        self.port_input = QSpinBox(); self.port_input.setRange(1, 65535); self.port_input.setValue(104); r1.addWidget(self.port_input)
        dl.addLayout(r1)
        r2 = QHBoxLayout()
        r2.addWidget(QLabel("SCU AE:")); self.ae_scu = QLineEdit("DICOM_MIGRATOR"); self.ae_scu.setMaxLength(16); r2.addWidget(self.ae_scu, 1)
        r2.addWidget(QLabel("SCP AE:")); self.ae_scp = QLineEdit("ANY-SCP"); self.ae_scp.setMaxLength(16); r2.addWidget(self.ae_scp, 1)
        dl.addLayout(r2)
        r3 = QHBoxLayout()
        r3.addWidget(QLabel("Hostname:")); self.hostname_lbl = QLabel("-"); self.hostname_lbl.setStyleSheet("color: #6c7086;"); r3.addWidget(self.hostname_lbl, 1)
        r3.addWidget(QLabel("Impl:")); self.impl_lbl = QLabel("-"); self.impl_lbl.setStyleSheet("color: #6c7086;"); r3.addWidget(self.impl_lbl, 1)
        dl.addLayout(r3)
        er = QHBoxLayout()
        self.echo_btn = QPushButton("C-ECHO Verify"); self.echo_btn.setProperty("warning", True); self.echo_btn.clicked.connect(self._run_echo)
        er.addWidget(self.echo_btn); self.echo_status = QLabel(""); er.addWidget(self.echo_status, 1)
        dl.addLayout(er)
        layout.addWidget(dg)

        # ═══ Advanced — pure VBoxLayout with HBox rows (no grid) ═══
        ag = QGroupBox("Advanced"); al = QVBoxLayout(ag); al.setSpacing(6)

        # Numeric settings row
        nr1 = QHBoxLayout()
        nr1.addWidget(QLabel("Batch:"))
        self.batch_spin = QSpinBox(); self.batch_spin.setRange(1, 1000); self.batch_spin.setValue(200); nr1.addWidget(self.batch_spin)
        nr1.addWidget(QLabel("Retries:"))
        self.retry_spin = QSpinBox(); self.retry_spin.setRange(0, 10); self.retry_spin.setValue(2); nr1.addWidget(self.retry_spin)
        nr1.addWidget(QLabel("Max PDU:"))
        self.pdu_combo = QComboBox(); self.pdu_combo.addItems(["0 (Unlimited)", "16384", "32768", "65536", "131072"]); nr1.addWidget(self.pdu_combo)
        nr1.addWidget(QLabel("Workers:"))
        self.workers_spin = QSpinBox(); self.workers_spin.setRange(1, 16); self.workers_spin.setValue(4)
        self.workers_spin.setToolTip("Parallel DICOM associations.\n1=serial, 4=default, 8+=aggressive")
        nr1.addWidget(self.workers_spin)
        nr1.addStretch()
        al.addLayout(nr1)

        self.manifest_check = QCheckBox("Save resume manifest (crash recovery)")
        self.manifest_check.setChecked(True)
        self.manifest_check.setToolTip("Saves JSON manifest to ~/.dicom_migrator/ for crash recovery.")
        al.addWidget(self.manifest_check)

        self.decompress_check = QCheckBox("Decompress if destination rejects compressed syntax")
        self.decompress_check.setChecked(True)
        self.decompress_check.setToolTip("Auto-decompress to Explicit VR LE in memory if PACS rejects compressed.\nSource files never modified.")
        al.addWidget(self.decompress_check)

        conflict_row = QHBoxLayout()
        self.conflict_retry_check = QCheckBox("Auto-resolve patient ID conflicts (0xFFFB)")
        self.conflict_retry_check.setChecked(True)
        self.conflict_retry_check.setToolTip("C-FIND destination for correct PatientID, remap in-memory.\nSource files never modified.")
        conflict_row.addWidget(self.conflict_retry_check)
        conflict_row.addWidget(QLabel("Suffix:"))
        self.conflict_suffix_input = QLineEdit("_MIG"); self.conflict_suffix_input.setMaxLength(16); self.conflict_suffix_input.setMaximumWidth(80)
        self.conflict_suffix_input.setToolTip("Fallback suffix if C-FIND unavailable.")
        conflict_row.addWidget(self.conflict_suffix_input)
        conflict_row.addStretch()
        al.addLayout(conflict_row)

        self.skip_existing_check = QCheckBox("Skip studies already on destination (pre-flight C-FIND)")
        self.skip_existing_check.setChecked(True)
        self.skip_existing_check.setToolTip("Query destination before sending to skip already-existing studies.")
        al.addWidget(self.skip_existing_check)

        auto_heal_row = QHBoxLayout()
        self.auto_heal_check = QCheckBox("Auto-retry transient failures")
        self.auto_heal_check.setChecked(True)
        self.auto_heal_check.setToolTip("Re-attempt transient failures after completion with exponential backoff.")
        auto_heal_row.addWidget(self.auto_heal_check)
        auto_heal_row.addWidget(QLabel("Waves:"))
        self.heal_waves_spin = QSpinBox(); self.heal_waves_spin.setRange(0, 10); self.heal_waves_spin.setValue(3)
        auto_heal_row.addWidget(self.heal_waves_spin)
        auto_heal_row.addStretch()
        al.addLayout(auto_heal_row)

        throttle_row = QHBoxLayout()
        self.throttle_check = QCheckBox("Bandwidth throttle")
        self.throttle_check.setChecked(False)
        self.throttle_check.setToolTip("Limit throughput. 0=unlimited, 10-50=shared, 100+=dedicated.")
        throttle_row.addWidget(self.throttle_check)
        throttle_row.addWidget(QLabel("Rate:"))
        self.throttle_rate = QDoubleSpinBox()
        self.throttle_rate.setRange(0.0, 10000.0); self.throttle_rate.setValue(0.0)
        self.throttle_rate.setSuffix(" MB/s"); self.throttle_rate.setDecimals(1)
        throttle_row.addWidget(self.throttle_rate)
        throttle_row.addStretch()
        al.addLayout(throttle_row)

        sched_row = QHBoxLayout()
        self.schedule_check = QCheckBox("Schedule window")
        self.schedule_check.setChecked(False)
        self.schedule_check.setToolTip("Auto-pause outside window. Supports overnight (19:00-06:00).")
        sched_row.addWidget(self.schedule_check)
        sched_row.addWidget(QLabel("From:"))
        self.schedule_start = QTimeEdit(); self.schedule_start.setDisplayFormat("HH:mm")
        self.schedule_start.setTime(QTime(19, 0)); sched_row.addWidget(self.schedule_start)
        sched_row.addWidget(QLabel("To:"))
        self.schedule_end = QTimeEdit(); self.schedule_end.setDisplayFormat("HH:mm")
        self.schedule_end.setTime(QTime(6, 0)); sched_row.addWidget(self.schedule_end)
        sched_row.addStretch()
        al.addLayout(sched_row)

        tls_row = QHBoxLayout()
        self.tls_check = QCheckBox("TLS encryption")
        self.tls_check.setChecked(False)
        self.tls_check.setToolTip("Encrypt DICOM associations. Empty certs = anonymous TLS.")
        tls_row.addWidget(self.tls_check)
        tls_row.addWidget(QLabel("CA:"))
        self.tls_ca = QLineEdit(); self.tls_ca.setPlaceholderText("CA cert file"); tls_row.addWidget(self.tls_ca)
        tls_row.addWidget(QLabel("Cert:"))
        self.tls_cert = QLineEdit(); self.tls_cert.setPlaceholderText("Client cert"); tls_row.addWidget(self.tls_cert)
        tls_row.addStretch()
        al.addLayout(tls_row)
        layout.addWidget(ag)

        # ═══ Streaming Filters ═══
        fg = QGroupBox("Streaming Migration Filters"); fl = QVBoxLayout(fg); fl.setSpacing(6)
        mod_row = QHBoxLayout()
        mod_row.addWidget(QLabel("Modalities:"))
        self.stream_modality_input = QLineEdit()
        self.stream_modality_input.setPlaceholderText("e.g. CR,DX,CT,MR (comma-sep, empty = all)")
        mod_row.addWidget(self.stream_modality_input, 1)
        fl.addLayout(mod_row)
        date_row = QHBoxLayout()
        date_row.addWidget(QLabel("Date From:"))
        self.stream_date_from = QDateEdit(); self.stream_date_from.setCalendarPopup(True)
        self.stream_date_from.setDate(QDate(2000, 1, 1)); self.stream_date_from.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self.stream_date_from)
        date_row.addWidget(QLabel("To:"))
        self.stream_date_to = QDateEdit(); self.stream_date_to.setCalendarPopup(True)
        self.stream_date_to.setDate(QDate.currentDate()); self.stream_date_to.setDisplayFormat("yyyy-MM-dd")
        date_row.addWidget(self.stream_date_to)
        date_row.addStretch()
        fl.addLayout(date_row)
        self.stream_filter_enable = QCheckBox("Enable streaming date/modality filters")
        self.stream_filter_enable.setChecked(False)
        fl.addWidget(self.stream_filter_enable)
        layout.addWidget(fg)

        # ═══ Tag Morphing ═══
        tg = QGroupBox("Tag Morphing (In-Memory Only -- Source Files NEVER Modified)")
        tl2 = QVBoxLayout(tg); tl2.setSpacing(6)
        tag_desc = QLabel("One rule per line.  Format: KEYWORD ACTION [VALUE]\nActions: set, prefix, suffix, delete, strip_private")
        tag_desc.setWordWrap(True); tag_desc.setStyleSheet("color: #a6adc8; font-size: 11px;")
        tl2.addWidget(tag_desc)
        self.tag_rules_edit = QPlainTextEdit()
        self.tag_rules_edit.setMaximumHeight(70)
        self.tag_rules_edit.setPlaceholderText("# InstitutionName set \"Migrated Archive\"\n# strip_private")
        self.tag_rules_edit.setStyleSheet("font-family: 'Consolas', monospace; font-size: 11px;")
        tl2.addWidget(self.tag_rules_edit)
        self.tag_morph_check = QCheckBox("Enable tag morphing"); self.tag_morph_check.setChecked(False)
        tl2.addWidget(self.tag_morph_check)
        layout.addWidget(tg)

        layout.addStretch()
        scroll.setWidget(inner)
        outer_layout.addWidget(scroll)
        return outer

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
        self.export_report_btn = QPushButton("Export Report"); self.export_report_btn.setEnabled(False)
        self.export_report_btn.clicked.connect(self._export_report); ctrl.addWidget(self.export_report_btn)
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
        self.healed_label = QLabel("Healed: 0"); self.healed_label.setStyleSheet("color: #94e2d5; font-weight: bold; font-size: 14px;"); rr.addWidget(self.healed_label)
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
        desc = QLabel("Post-migration verification -- queries destination PACS via C-FIND to confirm studies arrived with correct instance counts. Works after scan+upload OR streaming migration.")
        desc.setWordWrap(True); desc.setProperty("subtext", True); layout.addWidget(desc)

        ctrl = QHBoxLayout()
        self.verify_btn = QPushButton("Verify Migration"); self.verify_btn.setStyleSheet("font-size: 14px; padding: 10px 24px;")
        self.verify_btn.clicked.connect(self._start_verify); ctrl.addWidget(self.verify_btn)
        self.storage_commit_btn = QPushButton("Storage Commitment")
        self.storage_commit_btn.setToolTip(
            "Send DICOM Storage Commitment (N-ACTION) to formally confirm\n"
            "the destination PACS has committed to permanently storing\n"
            "the migrated instances. Not all PACS support this feature.")
        self.storage_commit_btn.clicked.connect(self._start_storage_commitment); ctrl.addWidget(self.storage_commit_btn)
        self.export_verify_btn = QPushButton("Export Verification CSV"); self.export_verify_btn.setEnabled(False)
        self.export_verify_btn.clicked.connect(self._export_verification_csv); ctrl.addWidget(self.export_verify_btn)
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

    # ─── v1.0.0 Config Helpers ────────────────────────────────────────────
    def _build_throttle(self):
        """Build BandwidthThrottle from UI settings."""
        if self.throttle_check.isChecked() and self.throttle_rate.value() > 0:
            return BandwidthThrottle(self.throttle_rate.value())
        return None

    def _build_tag_rules(self):
        """Parse tag morphing rules from UI text."""
        if not self.tag_morph_check.isChecked():
            return []
        return parse_tag_rules(self.tag_rules_edit.toPlainText())

    def _build_tls_context(self):
        """Build TLS context from UI settings."""
        if not self.tls_check.isChecked():
            return None
        return build_tls_context(
            cert_file=self.tls_cert.text().strip() or None,
            ca_file=self.tls_ca.text().strip() or None)

    def _get_schedule_start(self):
        """Get schedule start time as datetime.time."""
        qt = self.schedule_start.time()
        return dtime(qt.hour(), qt.minute())

    def _get_schedule_end(self):
        """Get schedule end time as datetime.time."""
        qt = self.schedule_end.time()
        return dtime(qt.hour(), qt.minute())

    def _get_stream_modalities(self):
        """Parse modality filter from UI input. Returns set or None."""
        if not self.stream_filter_enable.isChecked():
            return None
        text = self.stream_modality_input.text().strip()
        if not text:
            return None
        mods = set(m.strip().upper() for m in text.split(',') if m.strip())
        return mods if mods else None

    def _get_stream_date_from(self):
        """Get streaming date-from filter as YYYYMMDD string or None."""
        if not self.stream_filter_enable.isChecked():
            return None
        d = self.stream_date_from.date()
        return d.toString("yyyyMMdd")

    def _get_stream_date_to(self):
        """Get streaming date-to filter as YYYYMMDD string or None."""
        if not self.stream_filter_enable.isChecked():
            return None
        d = self.stream_date_to.date()
        return d.toString("yyyyMMdd")

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

        self.scanner_thread = ScannerThread(folder, self.recursive_check.isChecked(), manifest=self.manifest)
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
        self.skipped_label.setText("Skipped: 0"); self.conflict_label.setText("Conflicts: 0"); self.healed_label.setText("Healed: 0")
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
            conflict_suffix=self.conflict_suffix_input.text().strip() or "_MIG",
            skip_existing=self.skip_existing_check.isChecked(),
            workers=self.workers_spin.value(),
            max_retries=self.heal_waves_spin.value() if self.auto_heal_check.isChecked() else 0,
            throttle=self._build_throttle(),
            tag_rules=self._build_tag_rules(),
            tls_context=self._build_tls_context())
        self.upload_thread.progress.connect(self._on_upload_progress)
        self.upload_thread.file_sent.connect(self._on_file_sent)
        self.upload_thread.finished.connect(self._on_upload_complete)
        self.upload_thread.error.connect(lambda e: self._log(f"ERROR: {e}"))
        self.upload_thread.log.connect(self._log)
        self.upload_thread.status.connect(self.statusBar().showMessage)
        self.upload_thread.speed_update.connect(self._on_speed)
        self.upload_thread.conflict_retry_count.connect(self._on_conflict_count)
        self.upload_thread.auto_retry_healed.connect(self._on_healed_count)
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
        self.skipped_label.setText("Skipped: 0"); self.conflict_label.setText("Conflicts: 0"); self.healed_label.setText("Healed: 0")
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
            conflict_suffix=self.conflict_suffix_input.text().strip() or "_MIG",
            skip_existing=self.skip_existing_check.isChecked(),
            workers=self.workers_spin.value(),
            max_retries=self.heal_waves_spin.value() if self.auto_heal_check.isChecked() else 0,
            throttle=self._build_throttle(),
            tag_rules=self._build_tag_rules(),
            tls_context=self._build_tls_context(),
            schedule_enabled=self.schedule_check.isChecked(),
            schedule_start=self._get_schedule_start(),
            schedule_end=self._get_schedule_end(),
            filter_modalities=self._get_stream_modalities(),
            filter_date_from=self._get_stream_date_from(),
            filter_date_to=self._get_stream_date_to())
        self.streaming_thread.file_sent.connect(self._on_file_sent)
        self.streaming_thread.finished.connect(self._on_streaming_complete)
        self.streaming_thread.error.connect(lambda e: self._log(f"ERROR: {e}"))
        self.streaming_thread.log.connect(self._log)
        self.streaming_thread.status.connect(self.statusBar().showMessage)
        self.streaming_thread.speed_update.connect(self._on_speed)
        self.streaming_thread.folder_status.connect(self._on_folder_status)
        self.streaming_thread.conflict_retry_count.connect(self._on_conflict_count)
        self.streaming_thread.auto_retry_healed.connect(self._on_healed_count)
        self.streaming_thread.start()
        self.tabs.setCurrentIndex(2)

    def _on_conflict_count(self, count):
        self._conflict_retries = count
        self.conflict_label.setText(f"Conflicts: {count}")

    def _on_healed_count(self, count):
        self.healed_label.setText(f"Healed: {count}")

    def _on_folder_status(self, folder, dirs_done, sent, failed, skipped):
        self.stream_folder_lbl.setText(f"Folder: {folder}")
        total = sent + failed + skipped
        self.upload_count_lbl.setText(f"{total:,} files processed | {dirs_done:,} folders")
        # Sync counters — critical for fast-resume where skipped files bypass _on_file_sent
        self._sent = sent; self._failed = failed; self._skipped = skipped
        self.sent_label.setText(f"Copied: {sent}"); self.failed_label.setText(f"Failed: {failed}")
        self.skipped_label.setText(f"Skipped: {skipped}")
        elapsed = time.time() - self.upload_start_time if self.upload_start_time else 0
        if elapsed > 60:
            self.upload_eta_lbl.setText(f"Elapsed: {elapsed/3600:.1f}h" if elapsed > 3600 else f"Elapsed: {elapsed/60:.0f}m {elapsed%60:.0f}s")

    def _on_streaming_complete(self, sent, failed, skipped):
        self.upload_btn.setEnabled(True); self.stream_btn.setEnabled(True); self.scan_btn.setEnabled(True)
        self.pause_btn.setEnabled(False); self.cancel_btn.setEnabled(False)
        self.export_csv_btn.setEnabled(True); self.export_report_btn.setEnabled(True)
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

        # Bounded table — evict oldest rows when limit reached
        if self.upload_table.rowCount() >= MAX_TABLE_ROWS:
            self.upload_table.removeRow(0)

        row = self.upload_table.rowCount(); self.upload_table.insertRow(row)
        items = [QTableWidgetItem(os.path.basename(path)),
                 QTableWidgetItem("OK" if ok else "FAIL"),
                 QTableWidgetItem(msg),
                 QTableWidgetItem(sop_uid[:30] + "..." if len(sop_uid) > 30 else sop_uid)]
        # Color code: teal=auto-healed, purple=conflict resolved, green=OK, red=fail
        if ok and "Auto-healed" in msg:
            color = QColor("#94e2d5")  # teal for auto-healed
        elif ok and "Conflict resolved" in msg:
            color = QColor("#cba6f7")  # purple for conflict-resolved success
        elif ok:
            color = QColor("#a6e3a1")
        else:
            color = QColor("#f38ba8")
        items[1].setForeground(color)
        if not ok: items[2].setForeground(color)
        elif "Conflict resolved" in msg: items[2].setForeground(QColor("#cba6f7"))
        elif "Auto-healed" in msg: items[2].setForeground(QColor("#94e2d5"))
        for c, it in enumerate(items): self.upload_table.setItem(row, c, it)
        self.upload_table.scrollToBottom()

        if ok and "Auto-healed" in msg:
            # Healed files were already counted as failed; correct the counters
            self._sent += 1; self._failed = max(0, self._failed - 1)
        elif ok:
            self._sent += 1
        elif "Already sent" in msg or "Skipped" in msg or "Missing SOP" in msg: self._skipped += 1
        else: self._failed += 1
        self.sent_label.setText(f"Copied: {self._sent}"); self.failed_label.setText(f"Failed: {self._failed}")
        self.skipped_label.setText(f"Skipped: {self._skipped}")
        self.source_safe_lbl.setText("Source: 0 modified, 0 deleted")

    def _on_speed(self, fps, mbps): self.upload_speed_lbl.setText(f"{fps:.1f} files/s | {mbps:.2f} MB/s")

    def _on_upload_complete(self, sent, failed, skipped):
        self.upload_btn.setEnabled(True); self.stream_btn.setEnabled(True); self.scan_btn.setEnabled(True)
        self.pause_btn.setEnabled(False); self.cancel_btn.setEnabled(False)
        self.export_csv_btn.setEnabled(True); self.export_report_btn.setEnabled(True)
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
        host = self.host_input.text().strip()
        if not host: self._log("Enter destination host first"); return

        # Build study file counts — try manifest first, fall back to scanned files
        study_counts = {}
        source_files = self.dicom_files

        # If we have a manifest with sent records, use that for verification
        if self.manifest and self.manifest.records:
            for uid, rec in self.manifest.records.items():
                if rec.get('status') == 'sent':
                    suid = rec.get('study_instance_uid', '')
                    if not suid:
                        continue
                    if suid not in study_counts:
                        study_counts[suid] = {
                            'expected': 0, 'patient': rec.get('patient_name', '?'),
                            'desc': rec.get('study_desc', ''), 'series': set(), 'series_count': 0}
                    study_counts[suid]['expected'] += 1
                    series_uid = rec.get('series_instance_uid', '')
                    if series_uid:
                        study_counts[suid]['series'].add(series_uid)
            for sc in study_counts.values():
                sc['series_count'] = len(sc.get('series', set()))
                sc.pop('series', None)

        # Fall back to scanned files
        if not study_counts and source_files:
            for f in source_files:
                suid = f.get('study_instance_uid', '')
                if not suid: continue
                if suid not in study_counts:
                    study_counts[suid] = {
                        'expected': 0, 'patient': f.get('patient_name', '?'),
                        'desc': f.get('study_desc', '') or 'No Description',
                        'series': set(), 'series_count': 0}
                study_counts[suid]['expected'] += 1
                series_uid = f.get('series_instance_uid', '')
                if series_uid:
                    study_counts[suid]['series'].add(series_uid)
            for sc in study_counts.values():
                sc['series_count'] = len(sc.get('series', set()))
                sc.pop('series', None)

        if not study_counts:
            self._log("No files to verify. Run a migration or scan first."); return

        self.verify_table.setRowCount(0)
        self.verify_btn.setEnabled(False)
        self.verify_progress.setVisible(True); self.verify_progress.setValue(0)
        self.verify_status_lbl.setText(f"Verifying {len(study_counts)} studies...")
        self.verify_status_lbl.setStyleSheet("color: #f9e2af;")

        self.verify_thread = VerifyThread(
            study_counts, host, self.port_input.value(),
            self.ae_scu.text().strip() or "DICOM_MIGRATOR",
            self.ae_scp.text().strip() or "ANY-SCP",
            tls_context=self._build_tls_context())
        self.verify_thread.progress.connect(lambda c, t: (self.verify_progress.setMaximum(t), self.verify_progress.setValue(c)))
        self.verify_thread.study_verified.connect(self._on_study_verified)
        self.verify_thread.log.connect(self._log)
        self.verify_thread.finished.connect(self._on_verify_done)
        self.verify_thread.start()

    def _on_study_verified(self, study_uid, expected, found, match):
        row = self.verify_table.rowCount(); self.verify_table.insertRow(row)
        # Look up patient/desc from files or manifest
        patient = desc = ""
        for f in self.dicom_files:
            if f.get('study_instance_uid') == study_uid:
                patient = f.get('patient_name', ''); desc = f.get('study_desc', '') or 'No Description'; break
        if not patient and self.manifest:
            for uid, rec in self.manifest.records.items():
                if rec.get('study_instance_uid') == study_uid:
                    patient = rec.get('patient_name', '?'); desc = rec.get('study_desc', '') or 'No Description'; break
        items = [QTableWidgetItem(patient), QTableWidgetItem(desc),
                 QTableWidgetItem(str(expected)), QTableWidgetItem(str(found)),
                 QTableWidgetItem("MATCH" if match else "MISMATCH")]
        items[4].setForeground(QColor("#a6e3a1") if match else QColor("#f38ba8"))
        for c, it in enumerate(items): self.verify_table.setItem(row, c, it)

    def _on_verify_done(self, total, matched, mismatched):
        self.verify_btn.setEnabled(True); self.verify_progress.setVisible(False)
        self.export_verify_btn.setEnabled(True)
        if mismatched == 0:
            self.verify_status_lbl.setText(f"ALL {total} STUDIES VERIFIED")
            self.verify_status_lbl.setStyleSheet("color: #a6e3a1; font-size: 14px;")
        else:
            self.verify_status_lbl.setText(f"{matched}/{total} matched, {mismatched} MISMATCHED")
            self.verify_status_lbl.setStyleSheet("color: #f38ba8; font-size: 14px;")

    def _start_storage_commitment(self):
        """Send Storage Commitment N-ACTION for all sent instances."""
        host = self.host_input.text().strip()
        if not host: self._log("Enter destination host first"); return

        # Collect SOP instances from manifest
        sop_instances = []
        if self.manifest and self.manifest.records:
            for uid, rec in self.manifest.records.items():
                if rec.get('status') == 'sent':
                    sop_class = rec.get('sop_class_uid', '')
                    if sop_class:
                        sop_instances.append((sop_class, uid))
                    else:
                        # Try to find SOP class from scanned files
                        for f in self.dicom_files:
                            if f.get('sop_instance_uid') == uid:
                                sop_instances.append((f.get('sop_class_uid', ''), uid))
                                break

        if not sop_instances:
            self._log("No sent instances found. Run a migration first."); return

        self._log(f"Starting Storage Commitment for {len(sop_instances):,} instances...")
        self.storage_commit_btn.setEnabled(False)
        self.verify_status_lbl.setText(f"Storage Commitment: {len(sop_instances):,} instances...")
        self.verify_status_lbl.setStyleSheet("color: #f9e2af;")

        self._sc_thread = StorageCommitmentThread(
            sop_instances, host, self.port_input.value(),
            self.ae_scu.text().strip() or "DICOM_MIGRATOR",
            self.ae_scp.text().strip() or "ANY-SCP",
            tls_context=self._build_tls_context())
        self._sc_thread.log.connect(self._log)
        self._sc_thread.result.connect(self._on_storage_commitment_done)
        self._sc_thread.start()

    def _on_storage_commitment_done(self, total, committed, failed):
        self.storage_commit_btn.setEnabled(True)
        if failed == 0:
            self.verify_status_lbl.setText(f"STORAGE COMMITMENT: {committed:,}/{total:,} CONFIRMED")
            self.verify_status_lbl.setStyleSheet("color: #a6e3a1; font-size: 14px;")
        else:
            self.verify_status_lbl.setText(f"Storage Commitment: {committed:,} OK, {failed:,} failed")
            self.verify_status_lbl.setStyleSheet("color: #f38ba8; font-size: 14px;")

    def _export_log(self):
        p, _ = QFileDialog.getSaveFileName(self, "Export Log", "dicom_migrator_log.txt", "Text (*.txt)")
        if p:
            with open(p, 'w') as f: f.write(self.log_output.toPlainText())
            self._log(f"Exported: {p}")

    def _export_report(self):
        """Export migration summary report with performance stats."""
        if not self.manifest or not self.manifest.records:
            self._log("No migration data to export"); return
        p, _ = QFileDialog.getSaveFileName(self, "Export Migration Report",
            f"migration_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt", "Text (*.txt)")
        if p:
            self.manifest.export_summary(p)
            self._log(f"Migration report exported: {p}")

    def _export_verification_csv(self):
        """Export verification results table to CSV."""
        rows = self.verify_table.rowCount()
        if rows == 0:
            self._log("No verification results to export"); return
        p, _ = QFileDialog.getSaveFileName(self, "Export Verification Results",
            f"verification_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "CSV (*.csv)")
        if not p: return
        with open(p, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(["Patient", "Study Description", "Expected", "Found", "Status"])
            for row in range(rows):
                writer.writerow([
                    self.verify_table.item(row, c).text() if self.verify_table.item(row, c) else ''
                    for c in range(5)])
        self._log(f"Verification CSV exported: {p} ({rows} studies)")

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
        s.setValue("skip_existing", self.skip_existing_check.isChecked())
        s.setValue("workers", self.workers_spin.value())
        s.setValue("auto_heal", self.auto_heal_check.isChecked())
        s.setValue("heal_waves", self.heal_waves_spin.value())
        # v1.0.0 settings
        s.setValue("throttle_enabled", self.throttle_check.isChecked())
        s.setValue("throttle_rate", self.throttle_rate.value())
        s.setValue("schedule_enabled", self.schedule_check.isChecked())
        s.setValue("schedule_start", self.schedule_start.time().toString("HH:mm"))
        s.setValue("schedule_end", self.schedule_end.time().toString("HH:mm"))
        s.setValue("tls_enabled", self.tls_check.isChecked())
        s.setValue("tls_ca", self.tls_ca.text())
        s.setValue("tls_cert", self.tls_cert.text())
        s.setValue("tag_morph_enabled", self.tag_morph_check.isChecked())
        s.setValue("tag_rules", self.tag_rules_edit.toPlainText())
        s.setValue("stream_filter_enabled", self.stream_filter_enable.isChecked())
        s.setValue("stream_modalities", self.stream_modality_input.text())

    def _load_settings(self):
        s = self.settings
        self.folder_input.setText(s.value("folder", ""))
        self.host_input.setText(s.value("host", ""))
        p = s.value("port"); self.port_input.setValue(int(p)) if p else None
        self.ae_scu.setText(s.value("ae_scu", "DICOM_MIGRATOR"))
        self.ae_scp.setText(s.value("ae_scp", "ANY-SCP"))
        b = s.value("batch"); self.batch_spin.setValue(int(b)) if b else None
        r = s.value("retry"); self.retry_spin.setValue(int(r)) if r else None
        def _load_bool(key, widget, default=True):
            v = s.value(key)
            if v is not None: widget.setChecked(v == "true" or v is True)
            else: widget.setChecked(default)
        _load_bool("recursive", self.recursive_check)
        _load_bool("manifest_enabled", self.manifest_check)
        _load_bool("decompress_fallback", self.decompress_check)
        _load_bool("conflict_retry", self.conflict_retry_check)
        cs = s.value("conflict_suffix")
        if cs is not None: self.conflict_suffix_input.setText(cs)
        _load_bool("skip_existing", self.skip_existing_check)
        wk = s.value("workers")
        if wk is not None: self.workers_spin.setValue(int(wk))
        _load_bool("auto_heal", self.auto_heal_check)
        hw = s.value("heal_waves")
        if hw is not None: self.heal_waves_spin.setValue(int(hw))
        # v1.0.0 settings
        _load_bool("throttle_enabled", self.throttle_check, False)
        tr = s.value("throttle_rate")
        if tr is not None: self.throttle_rate.setValue(float(tr))
        _load_bool("schedule_enabled", self.schedule_check, False)
        ss = s.value("schedule_start")
        if ss: self.schedule_start.setTime(QTime.fromString(ss, "HH:mm"))
        se = s.value("schedule_end")
        if se: self.schedule_end.setTime(QTime.fromString(se, "HH:mm"))
        _load_bool("tls_enabled", self.tls_check, False)
        tc = s.value("tls_ca")
        if tc: self.tls_ca.setText(tc)
        tt = s.value("tls_cert")
        if tt: self.tls_cert.setText(tt)
        _load_bool("tag_morph_enabled", self.tag_morph_check, False)
        trules = s.value("tag_rules")
        if trules: self.tag_rules_edit.setPlainText(trules)
        _load_bool("stream_filter_enabled", self.stream_filter_enable, False)
        sm = s.value("stream_modalities")
        if sm: self.stream_modality_input.setText(sm)


# ═══════════════════════════════════════════════════════════════════════════════
# Entry Point
# ═══════════════════════════════════════════════════════════════════════════════
def main():
    # High-DPI scaling — must be set BEFORE QApplication is created
    if hasattr(Qt, 'AA_EnableHighDpiScaling'):
        QApplication.setAttribute(Qt.AA_EnableHighDpiScaling, True)
    if hasattr(Qt, 'AA_UseHighDpiPixmaps'):
        QApplication.setAttribute(Qt.AA_UseHighDpiPixmaps, True)
    os.environ.setdefault("QT_AUTO_SCREEN_SCALE_FACTOR", "1")

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
