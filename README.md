# DICOM PACS Migrator

![Version](https://img.shields.io/badge/version-0.4.0-blue)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows%20Server%202008+-0078D4)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-active-success)

> Bulk DICOM C-STORE migration tool with streaming migration, network auto-discovery, crash-safe resume, automatic decompression fallback, post-migration verification, and audit trail. Copy-only architecture — source data is **never** modified or deleted.

![Screenshot](screenshot.png)

## Quick Start

### Run from source (auto-installs dependencies)

```bash
git clone https://github.com/SysAdminDoc/DICOM-PACS-Migrator.git
cd DICOM-PACS-Migrator
python dicom_migrator.py
```

### Build portable .exe (Windows Server 2008+ compatible)

```powershell
powershell -ExecutionPolicy Bypass -File Build-DICOMPACSMigrator.ps1
# Output: dist\DICOMPACSMigrator.exe — single-file, no install required
```

## Features

| Feature | Description | Tab |
|---------|-------------|-----|
| **Streaming Migration** | Walk + Read + Send per-folder in one pass — starts sending immediately with no pre-scan. Ideal for large image stores with 35,000+ studies | Configuration |
| **Decompress Fallback** | Automatically decompresses JPEG/JPEG2000/RLE to Explicit VR Little Endian in memory when destination rejects compressed syntax. Source files never modified | Advanced |
| **Resume Support** | JSON manifest tracks every file by SOP Instance UID — resume after crash, skip already-sent files on re-run | Upload |
| **Retry Failed** | One-click retry of only failed files without re-sending successful ones | Upload |
| **Post-Migration Verification** | C-FIND queries destination PACS to confirm studies arrived with correct file counts | Verify |
| **Filtering** | Filter by patient name/ID, modality, date range before sending | File Browser |
| **Checkbox Selection** | Select/deselect individual patients, studies, or series for granular upload control | File Browser |
| **Per-File Error Detail** | Every file shows individual status, error message, and SOP UID in results table | Upload |
| **Failure Summary** | Grouped failure reasons with counts at end of migration — instantly see what the destination rejected | Log |
| **CSV Manifest Export** | Export complete audit trail with patient, status, timestamp for every file sent | Upload |
| Network Auto-Discovery | Two-phase TCP + C-ECHO scanner finds all DICOM nodes on your network | Configuration |
| Connection Assistant | Click-to-populate dialog — select a discovered node and settings auto-fill | Configuration |
| Batch Association Management | Configurable batch sizes (1-500), automatic SOP class negotiation per batch | Upload |
| C-ECHO Testing | One-click connectivity verification before migration | Configuration |
| Copy-Only Safety | Source files opened read-only, zero modification/deletion enforced at every layer | All |
| Persistent Settings | Connection settings, advanced options saved between sessions via QSettings | Configuration |
| Speed Metrics | Real-time files/sec, MB/s, and ETA during upload | Upload |
| Dark Theme | Catppuccin Mocha palette throughout | All |

## How It Works

```
                    ┌──────────────────────────────────────────────┐
                    │          Two Migration Modes                 │
                    └──────────┬──────────────────┬────────────────┘
                               │                  │
              ┌────────────────▼───┐    ┌─────────▼──────────────┐
              │   Standard Mode    │    │   Streaming Mode       │
              │                    │    │                        │
              │  Scan ──> Browse   │    │  Walk + Read + Send    │
              │  ──> Filter ──>    │    │  per-directory in      │
              │  Select ──> Send   │    │  one pass, no pre-scan │
              └────────┬───────────┘    └─────────┬──────────────┘
                       │                          │
                       └──────────┬───────────────┘
                                  │
                    ┌─────────────▼─────────────────┐
                    │         C-STORE Send           │
                    │                                │
                    │  1. Try original syntax        │
                    │  2. If rejected + decompress   │
                    │     enabled: decompress in     │
                    │     memory, retry as           │
                    │     Explicit VR Little Endian  │
                    │  3. Record to manifest         │
                    └─────────────┬─────────────────┘
                                  │
              ┌───────────────────┼───────────────────┐
              │                   │                    │
    ┌─────────▼────────┐ ┌───────▼────────┐ ┌────────▼────────┐
    │  Resume Manifest  │ │  CSV Export    │ │  C-FIND Verify  │
    │                   │ │               │ │                 │
    │  JSON per-file    │ │  Audit trail  │ │  Confirm study  │
    │  status tracking  │ │  for every    │ │  file counts    │
    │  Skip on re-run   │ │  file sent    │ │  on destination │
    └───────────────────┘ └───────────────┘ └─────────────────┘
```

## Usage

### Streaming Migration (Large Image Stores)

For large image stores with thousands of studies, streaming mode is the recommended approach:

1. Enter the root image store path in **DICOM Folder**
2. Configure destination host/port/AE titles
3. Click the green **"Stream Migrate Entire Store"** button
4. Files start sending within seconds — no enumeration wait

The tool walks each subdirectory, reads DICOM headers, and sends files immediately via C-STORE before moving to the next directory. Nothing is accumulated in memory.

During streaming you'll see:
- Which folder is being processed right now
- Running totals of files processed and folders completed
- Speed metrics (files/sec, MB/s)
- Per-file results in the Upload tab table

### Standard Mode (Selective Migration)

For granular control over what gets sent:

1. **Configuration Tab** — Browse to source folder, configure destination, click **"Scan for DICOM Files"**
2. **File Browser Tab** — Filter by patient/modality/date, use checkboxes to select specific studies/series
3. **Upload Tab** — Click **"Start Copy to Destination"** to send only selected files

### Post-Migration Verification

After migration completes, switch to the **Verify** tab and click **"Verify Migration"**. The tool queries the destination PACS via C-FIND at IMAGE level, comparing expected vs actual file counts per study. Color-coded MATCH/MISMATCH results.

## Decompress Fallback

Enabled by default in Advanced settings:

> **"Decompress before sending if destination rejects compressed syntax"**

When the destination PACS rejects a compressed file (JPEG Baseline, JPEG 2000, JPEG Lossless, RLE Lossless, etc.), the tool:

1. Catches the "No presentation context" error
2. Decompresses the dataset to Explicit VR Little Endian **in memory only**
3. Retries the C-STORE with the uncompressed data
4. Shows "Decompressed + Copied" in results

Source files are **never** modified. `ds.decompress()` operates on the in-memory pydicom Dataset object only.

For JPEG decompression, `Pillow` is recommended:

```bash
pip install Pillow
```

Some SOP classes like Encapsulated PDF Storage or Grayscale Softcopy Presentation State may still fail if the destination PACS doesn't support those object types at all. The failure summary at the end groups these by reason so you can see exactly what was rejected.

## Resume Support

The tool creates a JSON manifest in the source folder (when enabled):

```
migration_manifest_image_store_20260218.json
```

This tracks every SOP Instance UID and its send status. On re-run:

1. Point to the same source folder
2. Manifest loads automatically
3. Previously sent files are skipped ("Already sent (resumed)")
4. Only remaining/failed files are attempted

To disable manifest creation (no files written to source folder), uncheck **"Save resume manifest to source folder"** in Advanced settings. In-memory tracking still works for retry and CSV export within the current session.

## Failure Summary

At the end of any migration, the log shows grouped failures:

```
Failure Summary:
  [47x] No context + decompress failed: Encapsulated PDF Storage
  [12x] No context + decompress failed: Grayscale Softcopy Presentation State
  [3x]  Status: 0xA700
```

This tells you instantly what the destination won't accept, rather than scrolling through thousands of individual rows.

## Migration Manifest (Audit Trail)

Export as CSV via the **"Export CSV Manifest"** button. Fields:

| Field | Description |
|-------|-------------|
| `sop_instance_uid` | Unique DICOM identifier |
| `path` | Source file path |
| `status` | `sent`, `failed`, or `skipped` |
| `message` | Status detail or error message |
| `timestamp` | ISO 8601 timestamp |
| `patient_name` | Patient name from DICOM header |
| `patient_id` | Patient ID |
| `study_date` | Study date |
| `modality` | Modality (CR, DX, MR, CT, US, etc.) |

## Data Safety Guarantee

The copy-only architecture is enforced at every layer:

| Layer | Enforcement |
|-------|-------------|
| File Scanner | `pydicom.dcmread(stop_before_pixels=True, force=True)` — read-only |
| Upload Engine | `pydicom.dcmread()` read-only, `send_c_store()` network-only |
| Decompress | `ds.decompress()` modifies in-memory Dataset only, never writes to disk |
| File System | Zero `write()`, `rename()`, `unlink()`, `move()` calls on source files |
| Streaming | `os.walk()` for directory listing (read-only), same read-only send pipeline |
| UI | Persistent "COPY-ONLY MODE" badge, per-tab safety reminders |
| Log | Final line: "Source: 0 modified, 0 deleted — ALL ORIGINALS INTACT" |

The codebase contains zero calls to `os.remove`, `os.unlink`, `os.rename`, `os.replace`, `os.chmod`, `shutil.*`, `dcmwrite`, or `ds.save_as`. This has been verified by automated code audit.

## Network Discovery

The Connection Assistant scans your network in two phases:

| Setting | Default | Description |
|---------|---------|-------------|
| Subnet | Auto-detected | CIDR (`192.168.1.0/24`) or range (`192.168.1.1-254`) |
| Additional IPs | Empty | Comma-separated IPs or hostnames |
| Ports | DICOM standard | 104, 11112, 4242, 2762, 2575, 8042, 4006, 5678, 3003, 106 |
| Threads | 60 | Parallel TCP scan workers (1-200) |

**Phase 1**: TCP port scan across all IP/port combinations
**Phase 2**: DICOM C-ECHO probe on every open port to confirm DICOM service and retrieve AE title

## Advanced Settings

| Setting | Default | Description |
|---------|---------|-------------|
| Batch Size | 50 | Files per DICOM association (1-500) |
| Retries | 2 | Retry count on association failure (0-10) |
| Max PDU | Unlimited | Maximum PDU size for association negotiation |
| Resume Manifest | Enabled | Save JSON manifest to source folder for crash recovery |
| Decompress Fallback | Enabled | Auto-decompress when destination rejects compressed syntax |

## Compatibility

| OS | Status |
|----|--------|
| Windows Server 2008 / 2008 R2 | Supported (Python 3.8 target) |
| Windows Server 2012 / 2012 R2 | Supported |
| Windows Server 2016 / 2019 / 2022 / 2025 | Supported |
| Windows 7 / 8 / 10 / 11 | Supported |
| Linux (source only) | Supported via `python dicom_migrator.py` |

## Build Script

`Build-DICOMPACSMigrator.ps1` produces a single portable `.exe`:

- Targets **Python 3.8.10** — last version with official Windows binary installers
- Downloads and installs Python locally (no admin required)
- Falls back to embeddable zip if installer fails
- Pins all dependency versions for reproducible builds
- Excludes 25+ unused PyQt5 modules to minimize exe size (~40-60 MB)
- Output: `dist\DICOMPACSMigrator.exe` — copy to any Windows machine and run

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| PyQt5 | 5.15.10 | GUI framework |
| pydicom | 2.4.4 | DICOM file parsing |
| pynetdicom | 2.0.2 | DICOM networking (C-STORE, C-ECHO, C-FIND) |
| Pillow | (optional) | JPEG decompression for decompress fallback |
| PyInstaller | 5.13.2 | Exe compilation (build only) |

All auto-installed on first run (source) or bundled in the portable exe.

## FAQ / Troubleshooting

**Q: Can I send an entire image store with 35,000+ studies without waiting?**
Yes. Use the green "Stream Migrate Entire Store" button. It walks each directory and sends immediately — no enumeration or pre-scan required. First files start sending within seconds.

**Q: The migration was interrupted. Do I have to start over?**
No. Re-run with the same source folder and manifest enabled. Already-sent files are automatically skipped.

**Q: I'm getting "No presentation context" errors for some files.**
The destination is rejecting certain SOP class + transfer syntax combinations. If "Decompress before sending" is enabled, compressed images will be automatically retried as uncompressed. Objects like Encapsulated PDFs or Presentation States may still fail if the destination doesn't support those types — check the failure summary in the log.

**Q: How do I know my source files weren't modified?**
The log prints "Source: 0 modified, 0 deleted — ALL ORIGINALS INTACT" after every run. The codebase contains zero file-write operations on source paths. Decompression is in-memory only.

**Q: C-FIND verification shows MISMATCH — what do I do?**
Common causes: rejected SOP classes, deduplication on the destination, or JPEG files the destination couldn't store. Check the Upload tab results for per-file errors, use the failure summary to identify patterns, then use Retry Failed.

**Q: What about Encapsulated PDFs and Presentation State objects that keep failing?**
These aren't diagnostic images — they're embedded documents and display presets. If the destination PACS doesn't support them, they can't be sent via DICOM. You may need to configure the destination to accept those SOP classes, or accept that those non-image objects won't transfer.

**Q: How do I add Pillow for JPEG decompression?**
```bash
pip install Pillow
```
Or add `'Pillow'` to the `required` list in the bootstrap function and the build script's dependencies.

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

Issues and PRs welcome. Test against a local [Orthanc](https://www.orthanc-server.com/) instance before submitting changes.
