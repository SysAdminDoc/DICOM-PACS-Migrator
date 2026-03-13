# -*- mode: python ; coding: utf-8 -*-
# PyInstaller spec for DICOM PACS Migrator v3.3.0
# Build: pyinstaller dicom_migrator.spec

import sys
import os

_icon = None
if sys.platform == 'win32' and os.path.exists('icon.ico'):
    _icon = 'icon.ico'

a = Analysis(
    ['dicom_migrator.py'],
    pathex=[],
    binaries=[],
    datas=[],
    hiddenimports=[
        # pydicom codec plugins (not auto-detected by PyInstaller)
        'pydicom',
        'pydicom.encoders',
        'pydicom.encoders.gdcm',
        'pydicom.encoders.pylibjpeg',
        'pydicom.encoders.native',
        'pydicom.uid',
        'pydicom.valuerep',

        # pynetdicom internals
        'pynetdicom',
        'pynetdicom.sop_class',
        'pynetdicom.presentation',
        'pynetdicom._globals',

        # Image codec plugins for JPEG/JPEG2000/RLE decompression
        'pylibjpeg',
        'pylibjpeg.py',
        'openjpeg',
        'libjpeg',

        # PIL/Pillow
        'PIL',
        'PIL._imaging',

        # numpy (required by pydicom pixel handling)
        'numpy',

        # Qt platform plugins
        'PyQt5.sip',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        'tkinter', 'matplotlib', 'scipy', 'pandas',
        'IPython', 'jupyter', 'notebook',
        'pytest', 'unittest',
    ],
    noarchive=False,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='DICOM_PACS_Migrator',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,        # No console window (GUI app)
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=_icon,
)
