# DICOM PACS Migrator

![Version](https://img.shields.io/badge/version-0.4.0-blue)
![Python](https://img.shields.io/badge/Python-3.8+-3776AB?logo=python&logoColor=white)
![Platform](https://img.shields.io/badge/platform-Windows%20Server%202008+-0078D4)
![License](https://img.shields.io/badge/license-MIT-green)
![Status](https://img.shields.io/badge/status-active-success)

> Bulk DICOM C-STORE migration tool with streaming migration, network auto-discovery, crash-safe resume, automatic decompression fallback, post-migration verification, and audit trail. Copy-only architecture — source data is **never** modified or deleted.

<img width="1529" height="1045" alt="image" src="https://github.com/user-attachments/assets/63ff1b80-8999-4338-962e-e55d308a0b19" />

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
| **Decompress Fallback** | Automatically decompresses JPEG/JPEG2000/RLE/JPEG Lossless to uncompressed transfer syntax in memory when destination rejects compressed syntax. Handles transfer syntax negotiation, pixel-less objects, and codec edge cases. Source files never modified | Advanced |
| **Resume Support** | JSON manifest tracks every file by SOP Instance UID — resume after crash, skip already-sent files on re-run | Upload |
| **Retry Failed** | One-click retry of only failed files without re-sending successful ones | Upload |
| **Post-Migration Verification** | C-FIND queries destination PACS to confirm studies arrived with correct file counts | Verify |
| **Filtering** | Filter by patient name/ID, modality, date range before sending | File Browser |
| **Checkbox Selection** | Select/deselect individual patients, studies, or series for granular upload control | File Browser |
| **Per-File Error Detail** | Every file shows individual status, error message, and SOP UID in results table | Upload |
| **Failure Summary** | Grouped failure reasons with counts logged at end of migration — instantly see what the destination rejected | Log |
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
                    │  2. If rejected: decompress    │
                    │     in memory, open fresh      │
                    │     association with only       │
                    │     uncompressed syntaxes,     │
                    │     match negotiated TS        │
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

When the destination PACS rejects a compressed file, the tool handles the full negotiation cycle:

1. **Initial send** on the batch association with all transfer syntaxes proposed
2. **Catches "No presentation context"** error — the SCP rejected that SOP class + transfer syntax combination
3. **Decompresses pixel data in memory** using NumPy + Pillow/pylibjpeg codecs
4. **Opens a fresh association** proposing only Explicit VR Little Endian and Implicit VR Little Endian
5. **Reads the negotiated transfer syntax** from `accepted_contexts` and stamps the dataset to match exactly what the SCP picked
6. **Sends on the fresh association** — the SCP receives uncompressed data in its preferred encoding
7. **Releases the retry association** immediately after the single send

This handles several real-world edge cases:

| Scenario | How It's Handled |
|----------|-----------------|
| SCP only accepts Implicit VR LE | Fresh association negotiates Implicit; dataset TS is set to match |
| SCP only accepts Explicit VR LE | Fresh association negotiates Explicit; dataset TS is set to match |
| File has no pixel data (SR, KOS, PR) | Skips decompression, re-tags as uncompressed, sends on fresh association |
| Missing codec (rare JPEG format) | Falls through gracefully, reports specific error in failure summary |
| Duplicate SOP UID on destination | Normal PACS behavior — destination overwrites with identical data, returns Success |

Source files are **never** modified. `ds.decompress()` and transfer syntax re-tagging operate on the in-memory pydicom Dataset object only.

### Supported Codecs

| Transfer Syntax | Codec Required |
|----------------|---------------|
| JPEG Baseline (8-bit) | Pillow |
| JPEG Extended (12-bit) | pylibjpeg-libjpeg |
| JPEG Lossless (SV1) | pylibjpeg-libjpeg |
| JPEG Lossless | pylibjpeg-libjpeg |
| JPEG 2000 Lossless | pylibjpeg-openjpeg |
| JPEG 2000 | pylibjpeg-openjpeg |
| RLE Lossless | NumPy (built-in to pydicom) |
| Deflated Explicit VR LE | zlib (Python stdlib) |

All codecs require **NumPy** as a base dependency.

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
  [47x] Decompress retry assoc rejected for Encapsulated PDF Storage
  [12x] Decompress retry assoc rejected for Grayscale Softcopy Presentation State
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
| Decompress Fallback | `ds.decompress()` and TS re-tagging modify in-memory Dataset only, never writes to disk |
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
- Excludes 25+ unused PyQt5 modules to minimize exe size
- Output: `dist\DICOMPACSMigrator.exe` — copy to any Windows machine and run

## Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| numpy | latest | Pixel array operations — required by all decompression handlers |
| PyQt5 | 5.15.10 | GUI framework |
| pydicom | 2.4.4 | DICOM file parsing |
| pynetdicom | 2.0.2 | DICOM networking (C-STORE, C-ECHO, C-FIND) |
| Pillow | latest | JPEG Baseline decompression |
| pylibjpeg | latest | JPEG codec framework for pydicom |
| pylibjpeg-openjpeg | latest | JPEG 2000 decompression |
| pylibjpeg-libjpeg | latest | JPEG Lossless/Extended decompression |
| PyInstaller | 5.13.2 | Exe compilation (build only) |

All auto-installed on first run (source) or bundled in the portable exe.

## FAQ / Troubleshooting

**Q: Can I send an entire image store with 35,000+ studies without waiting?**
Yes. Use the green "Stream Migrate Entire Store" button. It walks each directory and sends immediately — no enumeration or pre-scan required. First files start sending within seconds.

**Q: The migration was interrupted. Do I have to start over?**
No. Re-run with the same source folder and manifest enabled. Already-sent files are automatically skipped.

**Q: I'm getting "No presentation context" errors for some files.**
The destination is rejecting certain SOP class + transfer syntax combinations. If "Decompress before sending" is enabled, compressed images will be automatically decompressed and retried on a fresh association. The tool negotiates the correct uncompressed transfer syntax with the SCP automatically.

**Q: How does the decompress retry actually work?**
When a compressed file is rejected, the tool: (1) decompresses pixel data in memory, (2) opens a brand new association proposing only uncompressed transfer syntaxes, (3) reads which syntax the SCP actually accepted, (4) stamps the dataset to match, and (5) sends. This handles SCPs that only accept Implicit VR LE, Explicit VR LE, or either.

**Q: I see "Duplicate detected / Will overwrite" in the destination PACS logs.**
Normal and safe. This happens when the decompressed retry sends the same SOP Instance UID that was partially received on the first attempt. The PACS overwrites with the identical data and returns Success. No data loss.

**Q: Some files fail with "Decompress retry assoc rejected".**
The destination PACS doesn't support that SOP class at all (not a transfer syntax issue). Common with Encapsulated PDF Storage, Grayscale Softcopy Presentation State, and other non-image objects. These need to be enabled on the destination PACS configuration, or accepted as non-transferable.

**Q: Files fail with "Unable to convert the pixel data".**
These are metadata-only objects (structured reports, key object selections) with a compressed transfer syntax in the header but no actual pixel data. The tool handles this automatically by re-tagging and sending on an uncompressed association.

**Q: How do I know my source files weren't modified?**
The log prints "Source: 0 modified, 0 deleted — ALL ORIGINALS INTACT" after every run. The codebase contains zero file-write operations on source paths. Decompression and transfer syntax re-tagging are in-memory only.

**Q: C-FIND verification shows MISMATCH — what do I do?**
Common causes: rejected SOP classes, deduplication on the destination, or files the destination couldn't store. Check the Upload tab results for per-file errors, use the failure summary to identify patterns, then use Retry Failed.

**Q: How do I install the decompression codecs manually?**
```bash
pip install numpy Pillow pylibjpeg pylibjpeg-openjpeg pylibjpeg-libjpeg
```
The compiled .exe bundles all of these automatically.

## License

MIT License — see [LICENSE](LICENSE) for details.

## Contributing

Issues and PRs welcome. Test against a local [Orthanc](https://www.orthanc-server.com/) instance before submitting changes.
