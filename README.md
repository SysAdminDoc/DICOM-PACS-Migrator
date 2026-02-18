# DICOM PACS Migrator

![Version](https://img.shields.io/badge/version-0.3.0-blue)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows%20Server%202008+-0078D4)
![License](https://img.shields.io/badge/license-MIT-green)

> Bulk DICOM C-STORE migration tool with network auto-discovery, crash-safe resume, post-migration verification, and audit trail. Copy-only architecture — source data is **never** modified or deleted.

## Quick Start

```bash
python dicom_migrator.py  # Auto-installs dependencies
```

Or build a portable exe:

```powershell
powershell -ExecutionPolicy Bypass -File Build-DICOMPACSMigrator.ps1
# Output: dist\DICOMPACSMigrator.exe
```

## v0.3.0 Features

| Feature | Description | Tab |
|---------|-------------|-----|
| Network Auto-Discovery | TCP + C-ECHO scanner finds DICOM nodes | Configuration |
| Connection Assistant | Click-to-populate from discovered nodes | Configuration |
| DICOM File Scanner | Read-only header parsing, patient/study/series tree | File Browser |
| **Filtering** | Filter by patient name, modality, date range | File Browser |
| **Checkbox Selection** | Select/deselect patients, studies, or series | File Browser |
| **Resume Support** | JSON manifest — skip already-sent files after crash | Upload |
| **Retry Failed** | One-click retry of failed files only | Upload |
| **Per-File Error Detail** | Status, error message, SOP UID per file | Upload |
| **CSV Manifest Export** | Audit trail with patient, status, timestamp | Upload |
| **Post-Migration Verify** | C-FIND confirms studies arrived with correct counts | Verify |
| Batch Associations | Configurable batch sizes, auto SOP negotiation | Upload |
| C-ECHO Testing | One-click connectivity test | Configuration |
| Copy-Only Safety | Zero source modification at every layer | All |
| Speed Metrics | files/sec, MB/s, ETA | Upload |

## How It Works

```
Discover ──> Scan ──> Filter & Select ──> Copy (C-STORE)
   │                                           │
   │           Export CSV <── Verify (C-FIND) <─┘
   │                              │
   └── TCP+ECHO            Manifest (Resume)
```

## Resume Support

A JSON manifest in the source folder tracks every SOP Instance UID:

1. Re-scan the same folder after interruption
2. Manifest loads automatically
3. Already-sent files are skipped
4. Status shows "Resume: X already sent, Y remaining"

## Post-Migration Verification

The Verify tab queries the destination via C-FIND at IMAGE level, comparing expected vs actual file counts per study. Color-coded MATCH/MISMATCH results.

## Data Safety Guarantee

| Layer | Enforcement |
|-------|-------------|
| Scanner | `dcmread(stop_before_pixels=True, force=True)` — read-only |
| Upload | `dcmread()` read-only, `send_c_store()` network-only |
| File System | Zero write/rename/unlink/move on source |
| UI | Persistent COPY-ONLY badge, per-tab reminders |
| Log | "Source: 0 modified, 0 deleted — ALL ORIGINALS INTACT" |

## Compatibility

Windows Server 2008+ / Windows 7+ / Linux (source). Portable exe requires no installation.

## Dependencies

PyQt5 5.15.10, pydicom 2.4.4, pynetdicom 2.0.2 — auto-installed or bundled.

## License

MIT
