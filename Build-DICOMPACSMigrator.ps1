#Requires -Version 5.1
<#
.SYNOPSIS
    Build script for DICOM PACS Migrator v2.1.0
    Compiles to a portable Windows .exe compatible with Server 2008+
.DESCRIPTION
    - Downloads Python 3.8.20 embeddable if needed (last version supporting Server 2008)
    - Creates isolated venv with pinned compatible dependencies
    - Runs PyInstaller to produce a single-file portable .exe
    - No admin rights required, no system Python modification
.NOTES
    Run from the project directory containing dicom_migrator.py
    Output: dist\DICOMPACSMigrator.exe
#>

$ErrorActionPreference = "Continue"
Set-StrictMode -Version Latest

# ============================================================================
# Configuration
# ============================================================================
$ProjectName     = "DICOMPACSMigrator"
$Version         = "2.2.0"
$ScriptFile      = "dicom_migrator.py"
$PythonVersion   = "3.8.10"
$PythonMajorMin  = "38"

# Pinned dependency versions known to work with Python 3.8 + Server 2008
# 3.8.10 is the LAST Python 3.8 release with official Windows binaries
$Dependencies = @(
    "numpy",
    "PyQt5==5.15.10",
    "pydicom==2.4.4",
    "pynetdicom==2.0.2",
    "Pillow",
    "pylibjpeg",
    "pylibjpeg-openjpeg",
    "pylibjpeg-libjpeg",
    "PyInstaller==5.13.2"
)

$ScriptDir  = Split-Path -Parent $MyInvocation.MyCommand.Definition
$BuildDir   = Join-Path $ScriptDir "build_env"
$VenvDir    = Join-Path $BuildDir "venv"
$DistDir    = Join-Path $ScriptDir "dist"
$PythonDir  = Join-Path $BuildDir "python-$PythonVersion"
$LogFile    = Join-Path $ScriptDir "build.log"

# ============================================================================
# Logging
# ============================================================================
function Write-Step {
    param([string]$Message)
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] $Message"
    Write-Host $line -ForegroundColor Cyan
    Add-Content -Path $LogFile -Value $line
}

function Write-OK {
    param([string]$Message)
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] [OK] $Message"
    Write-Host $line -ForegroundColor Green
    Add-Content -Path $LogFile -Value $line
}

function Write-Err {
    param([string]$Message)
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] [ERROR] $Message"
    Write-Host $line -ForegroundColor Red
    Add-Content -Path $LogFile -Value $line
}

function Write-Warn {
    param([string]$Message)
    $ts = Get-Date -Format "HH:mm:ss"
    $line = "[$ts] [WARN] $Message"
    Write-Host $line -ForegroundColor Yellow
    Add-Content -Path $LogFile -Value $line
}

# ============================================================================
# Init
# ============================================================================
"" | Set-Content -Path $LogFile
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host "  $ProjectName v$Version - Build Script" -ForegroundColor Cyan
Write-Host "  Target: Windows Server 2008+ Portable .exe" -ForegroundColor Cyan
Write-Host "  Python: $PythonVersion (last with Windows binaries)" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host ""

# Verify source file exists
$SourcePath = Join-Path $ScriptDir $ScriptFile
if (-not (Test-Path $SourcePath)) {
    Write-Err "$ScriptFile not found in $ScriptDir"
    Write-Host "Place this build script in the same directory as $ScriptFile" -ForegroundColor Yellow
    exit 1
}
Write-OK "Source: $SourcePath"

# Create build directory
if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir -Force | Out-Null
}

# ============================================================================
# Step 1: Locate or Download Python 3.8
# ============================================================================
Write-Step "Step 1/5: Locating Python $PythonVersion..."

$PythonExe = $null

# Check 1: py launcher (most reliable on Windows with multiple Pythons)
$pyLauncher = Get-Command py -ErrorAction SilentlyContinue
if ($pyLauncher) {
    try {
        $pyVer = & py -3.8 --version 2>&1 | Out-String
        if ($pyVer -match "3\.8\.\d+") {
            $pyPath = (& py -3.8 -c "import sys; print(sys.executable)" 2>&1 | Out-String).Trim()
            if (Test-Path $pyPath) {
                $PythonExe = $pyPath
                Write-OK "Found via py launcher: $PythonExe"
            }
        }
    }
    catch {
        # py launcher doesn't have 3.8 - continue to other checks
    }
}

# Check 2: System python (not python3/python3.8 - those trigger Windows Store stub)
if (-not $PythonExe) {
    $found = Get-Command python -ErrorAction SilentlyContinue
    if ($found) {
        try {
            $pyVer = & python --version 2>&1 | Out-String
            if ($pyVer -match "3\.8\.\d+") {
                $PythonExe = $found.Source
                Write-OK "Found system Python 3.8: $PythonExe"
            }
        }
        catch {}
    }
}

# Check 3: Common install paths
if (-not $PythonExe) {
    $searchPaths = @(
        "$env:LOCALAPPDATA\Programs\Python\Python38\python.exe",
        "C:\Python38\python.exe",
        "$env:ProgramFiles\Python38\python.exe",
        "${env:ProgramFiles(x86)}\Python38\python.exe",
        "$env:LOCALAPPDATA\Programs\Python\Python38-32\python.exe"
    )
    foreach ($p in $searchPaths) {
        if (Test-Path $p) {
            $PythonExe = $p
            Write-OK "Found Python 3.8: $PythonExe"
            break
        }
    }
}

# Check 3: Local build env
$localPython = Join-Path $PythonDir "python.exe"
if (-not $PythonExe -and (Test-Path $localPython)) {
    $PythonExe = $localPython
    Write-OK "Found local Python: $PythonExe"
}

# Download if not found
if (-not $PythonExe) {
    Write-Step "Python 3.8 not found - downloading..."

    # Strategy 1: Official 3.8.10 installer (last version with Windows binaries)
    $installerUrl = "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-amd64.exe"
    $installerPath = Join-Path $BuildDir "python-$PythonVersion-installer.exe"

    # Strategy 2: Embeddable zip (lighter, no installer needed)
    $embedUrl = "https://www.python.org/ftp/python/$PythonVersion/python-$PythonVersion-embed-amd64.zip"
    $embedZip = Join-Path $BuildDir "python-$PythonVersion-embed-amd64.zip"

    # Try installer first
    $downloaded = $false
    if (-not (Test-Path $installerPath)) {
        Write-Step "Downloading installer from python.org..."
        try {
            [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
            $wc = New-Object System.Net.WebClient
            $wc.DownloadFile($installerUrl, $installerPath)
            Write-OK "Downloaded installer"
            $downloaded = $true
        }
        catch {
            Write-Warn "Installer download failed: $_"
            Write-Step "Trying embeddable zip instead..."
        }
    }
    else {
        $downloaded = $true
    }

    if ($downloaded -and (Test-Path $installerPath)) {
        # Install to local directory (no admin needed)
        Write-Step "Installing Python $PythonVersion to $PythonDir (local, no admin)..."
        $installArgs = @(
            "/quiet",
            "InstallAllUsers=0",
            "TargetDir=$PythonDir",
            "AssociateFiles=0",
            "Shortcuts=0",
            "Include_launcher=0",
            "Include_test=0",
            "Include_doc=0",
            "Include_pip=1"
        )
        $proc = Start-Process -FilePath $installerPath -ArgumentList $installArgs -Wait -PassThru -NoNewWindow
        if ($proc.ExitCode -ne 0) {
            Write-Warn "Quiet install returned code $($proc.ExitCode), trying interactive..."
            Write-Host "Please install Python 3.8 to: $PythonDir" -ForegroundColor Yellow
            Start-Process -FilePath $installerPath -Wait
        }
    }

    # Fallback: embeddable zip + manual pip bootstrap
    if (-not (Test-Path (Join-Path $PythonDir "python.exe"))) {
        Write-Step "Attempting embeddable zip approach..."
        try {
            if (-not (Test-Path $embedZip)) {
                [Net.ServicePointManager]::SecurityProtocol = [Net.SecurityProtocolType]::Tls12
                $wc = New-Object System.Net.WebClient
                $wc.DownloadFile($embedUrl, $embedZip)
            }

            if (-not (Test-Path $PythonDir)) {
                New-Item -ItemType Directory -Path $PythonDir -Force | Out-Null
            }
            Expand-Archive -Path $embedZip -DestinationPath $PythonDir -Force

            # Enable import of site-packages in embeddable distribution
            $pthFile = Get-ChildItem -Path $PythonDir -Filter "python*._pth" | Select-Object -First 1
            if ($pthFile) {
                $pthContent = Get-Content $pthFile.FullName
                $pthContent = $pthContent -replace '#import site', 'import site'
                Set-Content -Path $pthFile.FullName -Value $pthContent
                Write-OK "Enabled site-packages in embeddable distribution"
            }

            # Bootstrap pip via get-pip.py
            $getPipUrl = "https://bootstrap.pypa.io/pip/3.8/get-pip.py"
            $getPipPath = Join-Path $PythonDir "get-pip.py"
            $wc.DownloadFile($getPipUrl, $getPipPath)
            & (Join-Path $PythonDir "python.exe") $getPipPath --no-warn-script-location 2>&1 | Out-Null
            Write-OK "pip bootstrapped in embeddable distribution"
        }
        catch {
            Write-Err "Embeddable zip also failed: $_"
        }
    }

    if (Test-Path (Join-Path $PythonDir "python.exe")) {
        $PythonExe = Join-Path $PythonDir "python.exe"
        Write-OK "Python ready: $PythonExe"
    }
    else {
        Write-Err "All download methods failed"
        Write-Host ""
        Write-Host "Manual install required:" -ForegroundColor Yellow
        Write-Host "  1. Download Python 3.8.10 from https://www.python.org/downloads/release/python-3810/" -ForegroundColor Yellow
        Write-Host "     (scroll down to 'Windows installer (64-bit)')" -ForegroundColor Yellow
        Write-Host "  2. Install to default location or C:\Python38" -ForegroundColor Yellow
        Write-Host "  3. Re-run this build script" -ForegroundColor Yellow
        exit 1
    }
}

# Verify Python version
$verCheck = & $PythonExe --version 2>&1 | Out-String
$verCheck = $verCheck.Trim()
Write-OK "Using: $verCheck"

# ============================================================================
# Step 2: Create Virtual Environment
# ============================================================================
Write-Step "Step 2/5: Creating virtual environment..."

$VenvPython = Join-Path $VenvDir "Scripts\python.exe"
$VenvPip    = Join-Path $VenvDir "Scripts\pip.exe"
$UseVenv    = $true

if (-not (Test-Path $VenvPython)) {
    # Try creating venv - may fail with embeddable distribution
    $venvResult = & $PythonExe -m venv $VenvDir 2>&1 | Out-String
    $venvExit = $LASTEXITCODE

    if ($venvExit -ne 0 -or -not (Test-Path $VenvPython)) {
        Write-Warn "venv unavailable (embeddable Python) - installing directly"
        $UseVenv = $false
        $VenvPython = $PythonExe
        # Ensure pip Scripts dir exists
        $scriptsDir = Join-Path (Split-Path $PythonExe -Parent) "Scripts"
        if (-not (Test-Path $scriptsDir)) {
            New-Item -ItemType Directory -Path $scriptsDir -Force | Out-Null
        }
    }
    else {
        Write-OK "Venv created: $VenvDir"
    }
}
else {
    Write-OK "Venv exists: $VenvDir"
}

# Upgrade pip
Write-Step "Upgrading pip..."
& $VenvPython -m pip install --upgrade pip setuptools wheel -q 2>&1 | Out-Null
Write-OK "pip upgraded"

# ============================================================================
# Step 3: Install Dependencies
# ============================================================================
Write-Step "Step 3/5: Installing dependencies..."

foreach ($dep in $Dependencies) {
    $pkgName = ($dep -split "==")[0]
    Write-Step "  Installing $dep..."
    & $VenvPython -m pip install $dep -q 2>&1 | Out-Null
    if ($LASTEXITCODE -ne 0) {
        Write-Err "Failed to install $dep"
        exit 1
    }
    Write-OK "  $pkgName installed"
}

# Verify all imports work
Write-Step "Verifying imports..."
$importTest = @"
import sys
try:
    from PyQt5.QtCore import PYQT_VERSION_STR; print(f'PyQt5 {PYQT_VERSION_STR}')
    import pydicom; print(f'pydicom {pydicom.__version__}')
    import pynetdicom; print(f'pynetdicom {pynetdicom.__version__}')
    import PyInstaller; print(f'PyInstaller {PyInstaller.__version__}')
    print('ALL_OK')
except Exception as e:
    print(f'FAIL: {e}')
    sys.exit(1)
"@
$importResult = & $VenvPython -c $importTest 2>&1 | Out-String
$importLines = $importResult.Trim() -split "`n" | ForEach-Object { $_.Trim() } | Where-Object { $_ }
$importLines | ForEach-Object { Write-OK "  $_" }
if ($importLines -notcontains "ALL_OK") {
    Write-Err "Import verification failed"
    exit 1
}

# ============================================================================
# Step 4: Build with PyInstaller
# ============================================================================
Write-Step "Step 4/5: Compiling with PyInstaller..."

# Clean previous builds
$specFile = Join-Path $ScriptDir "$ProjectName.spec"
if (Test-Path $specFile) { Remove-Item $specFile -Force }
if (Test-Path $DistDir) { Remove-Item $DistDir -Recurse -Force }
$piBuildDir = Join-Path $ScriptDir "build"
if (Test-Path $piBuildDir) { Remove-Item $piBuildDir -Recurse -Force }

# PyInstaller arguments
$piArgs = @(
    "--onefile",
    "--windowed",
    "--clean",
    "--noconfirm",
    "--name", $ProjectName,
    "--icon", "`"$(Join-Path $ScriptDir 'icon.ico')`"",
    "--distpath", "`"$DistDir`"",

    # Hidden imports for pynetdicom internals
    "--hidden-import", "pynetdicom",
    "--hidden-import", "pynetdicom.sop_class",
    "--hidden-import", "pynetdicom.service_class",
    "--hidden-import", "pynetdicom.service_class_n",
    "--hidden-import", "pynetdicom._handlers",
    "--hidden-import", "pynetdicom.transport",
    "--hidden-import", "pynetdicom.pdu",
    "--hidden-import", "pynetdicom.pdu_primitives",
    "--hidden-import", "pynetdicom.presentation",
    "--hidden-import", "pynetdicom.dimse",
    "--hidden-import", "pynetdicom.dimse_primitives",
    "--hidden-import", "pynetdicom.status",
    "--hidden-import", "pynetdicom.utils",

    # Hidden imports for pydicom
    "--hidden-import", "pydicom",
    "--hidden-import", "pydicom.encoders",
    "--hidden-import", "pydicom.encoders.gdcm",
    "--hidden-import", "pydicom.encoders.pylibjpeg",
    "--hidden-import", "pydicom.encoders.native",
    "--hidden-import", "pydicom.uid",

    # PyQt5 - targeted imports only (collect-all copies 500MB+ of unused Qt modules)
    "--hidden-import", "PyQt5",
    "--hidden-import", "PyQt5.QtCore",
    "--hidden-import", "PyQt5.QtGui",
    "--hidden-import", "PyQt5.QtWidgets",
    "--hidden-import", "PyQt5.sip",

    # Network/socket modules
    "--hidden-import", "socket",
    "--hidden-import", "ipaddress",
    "--hidden-import", "concurrent.futures",
    "--hidden-import", "threading",
    "--hidden-import", "multiprocessing",

    # Exclude bloat - general
    "--exclude-module", "tkinter",
    "--exclude-module", "matplotlib",
    "--exclude-module", "scipy",
    "--exclude-module", "cv2",
    "--exclude-module", "unittest",
    "--exclude-module", "test",
    "--exclude-module", "xmlrpc",
    "--exclude-module", "pydoc",

    # Exclude bloat - unused PyQt5 modules (saves 200MB+)
    "--exclude-module", "PyQt5.QtWebEngine",
    "--exclude-module", "PyQt5.QtWebEngineCore",
    "--exclude-module", "PyQt5.QtWebEngineWidgets",
    "--exclude-module", "PyQt5.QtWebKit",
    "--exclude-module", "PyQt5.QtWebKitWidgets",
    "--exclude-module", "PyQt5.QtNetwork",
    "--exclude-module", "PyQt5.QtMultimedia",
    "--exclude-module", "PyQt5.QtMultimediaWidgets",
    "--exclude-module", "PyQt5.QtBluetooth",
    "--exclude-module", "PyQt5.QtDBus",
    "--exclude-module", "PyQt5.QtDesigner",
    "--exclude-module", "PyQt5.QtHelp",
    "--exclude-module", "PyQt5.QtLocation",
    "--exclude-module", "PyQt5.QtNfc",
    "--exclude-module", "PyQt5.QtOpenGL",
    "--exclude-module", "PyQt5.QtPositioning",
    "--exclude-module", "PyQt5.QtQuick",
    "--exclude-module", "PyQt5.QtQuickWidgets",
    "--exclude-module", "PyQt5.QtSensors",
    "--exclude-module", "PyQt5.QtSerialPort",
    "--exclude-module", "PyQt5.QtSql",
    "--exclude-module", "PyQt5.QtSvg",
    "--exclude-module", "PyQt5.QtTest",
    "--exclude-module", "PyQt5.QtXml",
    "--exclude-module", "PyQt5.Qt3DCore",
    "--exclude-module", "PyQt5.Qt3DRender",
    "--exclude-module", "PyQt5.QtWebChannel",
    "--exclude-module", "PyQt5.QtWebSockets",
    "--exclude-module", "PyQt5.QtRemoteObjects",

    # Source
    "`"$SourcePath`""
)

Write-Step "Running PyInstaller (this takes 1-3 minutes)..."
$piArgString = $piArgs -join ' '

# Use a temp batch file to avoid both:
#   1. PowerShell NativeCommandError on stderr
#   2. .NET Process ReadToEnd() deadlock on large output
$batFile = Join-Path $BuildDir "run_pyinstaller.bat"
$batLog  = Join-Path $BuildDir "pyinstaller_output.log"
$batContent = "@echo off`r`n`"$VenvPython`" -m PyInstaller $piArgString > `"$batLog`" 2>&1`r`necho EXIT_CODE:%ERRORLEVEL%>> `"$batLog`""
Set-Content -Path $batFile -Value $batContent -Encoding ASCII

$batProc = Start-Process -FilePath "cmd.exe" -ArgumentList "/c `"$batFile`"" -Wait -PassThru -NoNewWindow -WorkingDirectory $ScriptDir

# Read and display output
$piExitCode = 0
if (Test-Path $batLog) {
    $allOutput = Get-Content $batLog -ErrorAction SilentlyContinue
    foreach ($line in $allOutput) {
        if (-not $line.Trim()) { continue }
        if ($line -match "^EXIT_CODE:(\d+)") {
            $piExitCode = [int]$Matches[1]
            continue
        }
        if ($line -match "ERROR" -and $line -notmatch "INFO:") {
            Write-Err "  $line"
        }
        elseif ($line -match "Building|completed successfully|COLLECT|EXE") {
            Write-Step "  $line"
        }
        Add-Content -Path $LogFile -Value $line
    }
}

if ($piExitCode -ne 0) {
    Write-Err "PyInstaller exited with code $piExitCode"
    Write-Host "Check build.log for details" -ForegroundColor Yellow
}

# Cleanup temp files
Remove-Item $batFile -Force -ErrorAction SilentlyContinue
Remove-Item $batLog -Force -ErrorAction SilentlyContinue

$ExePath = Join-Path $DistDir "$ProjectName.exe"
if (-not (Test-Path $ExePath)) {
    Write-Err "Build failed - $ExePath not found"
    Write-Host "Check build.log for details" -ForegroundColor Yellow
    exit 1
}

$exeSize = [math]::Round((Get-Item $ExePath).Length / 1MB, 1)
Write-OK "Build successful: $ExePath ($exeSize MB)"

# ============================================================================
# Step 5: Verify Build
# ============================================================================
Write-Step "Step 5/5: Verifying executable..."

# Quick smoke test - check PE header
$bytes = [System.IO.File]::ReadAllBytes($ExePath)
if ($bytes[0] -eq 0x4D -and $bytes[1] -eq 0x5A) {
    Write-OK "Valid PE executable"
}
else {
    Write-Warn "PE header check failed - file may be corrupt"
}

# Clean up PyInstaller temp files
Write-Step "Cleaning build artifacts..."
$piBuildDir2 = Join-Path $ScriptDir "build"
if (Test-Path $piBuildDir2) { Remove-Item $piBuildDir2 -Recurse -Force -ErrorAction SilentlyContinue }
if (Test-Path $specFile) { Remove-Item $specFile -Force -ErrorAction SilentlyContinue }

# ============================================================================
# Summary
# ============================================================================
Write-Host ""
Write-Host "============================================================" -ForegroundColor Green
Write-Host "  BUILD COMPLETE" -ForegroundColor Green
Write-Host "============================================================" -ForegroundColor Green
Write-Host ""
Write-Host "  Executable:  $ExePath" -ForegroundColor White
Write-Host "  Size:        $exeSize MB" -ForegroundColor White
Write-Host "  Python:      $verCheck" -ForegroundColor White
Write-Host "  Target:      Windows Server 2008+ (x64)" -ForegroundColor White
Write-Host ""
Write-Host "  Compatibility:" -ForegroundColor Cyan
Write-Host "    - Windows Server 2008 / 2008 R2" -ForegroundColor White
Write-Host "    - Windows Server 2012 / 2012 R2" -ForegroundColor White
Write-Host "    - Windows Server 2016 / 2019 / 2022" -ForegroundColor White
Write-Host "    - Windows 7 / 8 / 10 / 11" -ForegroundColor White
Write-Host ""
Write-Host "  Portable - no installation required." -ForegroundColor Yellow
Write-Host "  Copy $ProjectName.exe to any compatible machine and run." -ForegroundColor Yellow
Write-Host ""
Write-Host "  Build log: $LogFile" -ForegroundColor Gray
Write-Host "============================================================" -ForegroundColor Green
