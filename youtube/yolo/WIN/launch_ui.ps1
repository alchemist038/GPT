param(
  [string]$ConfigPath = ""
)

$root = Split-Path -Parent $MyInvocation.MyCommand.Path
$winRoot = Resolve-Path (Join-Path $root ".")

if ($ConfigPath -ne "") {
  Copy-Item -Force $ConfigPath (Join-Path $winRoot "config.json")
}

if (-not (Test-Path (Join-Path $winRoot "config.json"))) {
  Copy-Item -Force (Join-Path $winRoot "config.example.json") (Join-Path $winRoot "config.json")
  Write-Host "Created config.json from config.example.json"
}

python (Join-Path $winRoot "ui\app.py")
