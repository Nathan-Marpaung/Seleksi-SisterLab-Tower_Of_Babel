<#
.SYNOPSIS
    Menjalankan demonstrasi Babel Gateway dari PowerShell.

.DESCRIPTION
    Skrip demonstrasi aslinya ditulis dalam bash, karena itu yang tersedia di
    environment penilaian. PowerShell tidak bisa mengeksekusi file .sh secara
    langsung: mengetik "./demo/run-all.sh" hanya akan membuka file itu, bukan
    menjalankannya. Wrapper ini mencari bash lalu mendelegasikan ke sana.

    Git Bash yang diutamakan, bukan bash bawaan WSL. Alasannya, Git Bash
    memakai filesystem dan daemon Docker yang sama dengan PowerShell, sementara
    WSL punya root filesystem sendiri sehingga path relatif di dalam skrip bisa
    menunjuk tempat yang salah.

.PARAMETER Mode
    Kosongkan untuk menjalankan semuanya, atau isi "--core" / "--bonus".

.EXAMPLE
    .\demo\run-all.ps1
    .\demo\run-all.ps1 --core
#>

[CmdletBinding()]
param(
    [ValidateSet('', '--core', '--bonus')]
    [string]$Mode = ''
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

# Kandidat lokasi Git Bash, diurutkan dari yang paling umum.
$candidates = @(
    "$env:ProgramFiles\Git\bin\bash.exe",
    "${env:ProgramFiles(x86)}\Git\bin\bash.exe",
    "$env:LOCALAPPDATA\Programs\Git\bin\bash.exe"
)

$bash = $candidates | Where-Object { Test-Path $_ } | Select-Object -First 1

if (-not $bash) {
    # Terakhir, coba bash apa pun yang ada di PATH. Kalau yang ketemu ternyata
    # bash bawaan WSL, beri tahu penggunanya supaya tidak bingung kalau path
    # atau Docker berperilaku lain.
    $onPath = Get-Command bash -ErrorAction SilentlyContinue
    if ($onPath) {
        $bash = $onPath.Source
        if ($bash -like "$env:SystemRoot\*") {
            Write-Warning "Git Bash tidak ditemukan, memakai $bash (kemungkinan WSL)."
            Write-Warning "Kalau demonstrasinya gagal, install Git for Windows lalu ulangi."
        }
    }
}

if (-not $bash) {
    Write-Error @"
Tidak menemukan bash di sistem ini.

Skrip demonstrasi membutuhkan bash, curl, dan python3. Pilihannya:

  1. Install Git for Windows (https://git-scm.com/download/win), lalu jalankan
     lagi perintah ini dari PowerShell.
  2. Atau buka Git Bash, masuk ke folder repositori, lalu jalankan:
       ./demo/run-all.sh
"@
    exit 1
}

Write-Host "Menjalankan demonstrasi dengan $bash" -ForegroundColor Cyan
Write-Host "Pastikan stack sudah hidup: docker compose up -d" -ForegroundColor DarkGray
Write-Host ""

Push-Location $repoRoot
try {
    if ($Mode) {
        & $bash 'demo/run-all.sh' $Mode
    }
    else {
        & $bash 'demo/run-all.sh'
    }
    $code = $LASTEXITCODE
}
finally {
    Pop-Location
}

exit $code
