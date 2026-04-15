$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot
$logDir = Join-Path $repoRoot 'logs'
New-Item -ItemType Directory -Force -Path $logDir | Out-Null

$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$logFile = Join-Path $logDir "report-$timestamp.log"

Set-Location $repoRoot

try {
  $npm = (Get-Command npm.cmd -ErrorAction Stop).Source
  "[$(Get-Date -Format o)] Starting scheduled report update" | Tee-Object -FilePath $logFile
  & $npm run report:2h:sheets 2>&1 | Tee-Object -FilePath $logFile -Append
  $exitCode = if ($null -eq $LASTEXITCODE) { 0 } else { $LASTEXITCODE }
  "[$(Get-Date -Format o)] Finished with exit code $exitCode" | Tee-Object -FilePath $logFile -Append
  exit $exitCode
} catch {
  "[$(Get-Date -Format o)] ERROR: $($_.Exception.Message)" | Tee-Object -FilePath $logFile -Append
  throw
}
