param(
  [string]$Repo = ''
)

$ErrorActionPreference = 'Stop'

$repoRoot = Split-Path -Parent $PSScriptRoot

function Read-DotEnv {
  param([string]$Path)

  $result = @{}
  $raw = New-Object System.Collections.Generic.List[string]

  if (-not (Test-Path $Path)) {
    return @{ Values = $result; Raw = @() }
  }

  foreach ($line in Get-Content -LiteralPath $Path) {
    $trimmed = $line.Trim()
    if (-not $trimmed -or $trimmed.StartsWith('#')) {
      continue
    }

    $idx = $trimmed.IndexOf('=')
    if ($idx -lt 0) {
      $raw.Add($trimmed)
      continue
    }

    $key = $trimmed.Substring(0, $idx).Trim()
    $value = $trimmed.Substring($idx + 1).Trim()
    $result[$key] = $value
  }

  return @{ Values = $result; Raw = @($raw) }
}

function Get-GitHubRepoFromRemote {
  Push-Location $repoRoot
  try {
    $remote = git remote get-url origin 2>$null
  } finally {
    Pop-Location
  }

  if (-not $remote) {
    return ''
  }

  $remote = $remote.Trim()
  if ($remote -match 'github\.com[:/]([^/]+)/([^/]+?)(?:\.git)?$') {
    return "$($Matches[1])/$($Matches[2])"
  }

  return ''
}

function Require-Value {
  param(
    [hashtable]$Secrets,
    [string]$Name
  )

  if (-not $Secrets[$Name]) {
    throw "Missing required secret value: $Name"
  }
}

function Set-GitHubSecret {
  param(
    [string]$Name,
    [string]$Value,
    [string]$RepoName
  )

  $tempFile = [System.IO.Path]::GetTempFileName()
  try {
    [System.IO.File]::WriteAllText($tempFile, $Value, [System.Text.UTF8Encoding]::new($false))
    Write-Host "Setting GitHub secret: $Name"
    gh secret set $Name --repo $RepoName --body-file $tempFile | Out-Null
  } finally {
    if (Test-Path $tempFile) {
      Remove-Item -LiteralPath $tempFile -Force
    }
  }
}

if (-not (Get-Command gh -ErrorAction SilentlyContinue)) {
  throw 'GitHub CLI is not installed. Install it from https://cli.github.com/ and run gh auth login.'
}

if (-not $Repo) {
  $Repo = Get-GitHubRepoFromRemote
}

if (-not $Repo) {
  throw 'GitHub repo is unknown. Pass -Repo owner/repo or configure git remote origin.'
}

Push-Location $repoRoot
try {
  gh auth status | Out-Null
} catch {
  throw 'GitHub CLI is not authenticated. Run gh auth login first.'
} finally {
  Pop-Location
}

$bitrix = Read-DotEnv (Join-Path $repoRoot 'bitrix.env')
$skorozvon = Read-DotEnv (Join-Path $repoRoot 'skorozvon.env')
$googleEnv = Read-DotEnv (Join-Path $repoRoot 'google.env')

$bitrixValues = $bitrix.Values
$skorozvonValues = $skorozvon.Values
$googleValues = $googleEnv.Values
$bitrixWebhookUrl = $bitrixValues['BITRIX_WEBHOOK_URL']
if (-not $bitrixWebhookUrl) {
  $bitrixWebhookUrl = $bitrixValues['BITRIX_WEBHOOK']
}
if (-not $bitrixWebhookUrl -and $bitrix.Raw.Count -gt 0) {
  $bitrixWebhookUrl = $bitrix.Raw[0]
}

$googleServiceAccountJson = ''
$googleJsonValue = $googleValues['GOOGLE_SERVICE_ACCOUNT_JSON']
if ($googleJsonValue -and $googleJsonValue.Trim().StartsWith('{')) {
  $googleServiceAccountJson = $googleJsonValue
} elseif ($googleJsonValue) {
  $googleJsonPath = Join-Path $repoRoot $googleJsonValue
  if (-not (Test-Path $googleJsonPath)) {
    throw "Google service account JSON not found: $googleJsonPath"
  }
  $googleServiceAccountJson = Get-Content -LiteralPath $googleJsonPath -Raw
}

$secrets = @{
  BITRIX_WEBHOOK_URL = $bitrixWebhookUrl
  SKOROZVON_USERNAME = $skorozvonValues['SKOROZVON_USERNAME']
  SKOROZVON_API_KEY = $skorozvonValues['SKOROZVON_API_KEY']
  SKOROZVON_CLIENT_ID = $skorozvonValues['SKOROZVON_CLIENT_ID']
  SKOROZVON_CLIENT_SECRET = $skorozvonValues['SKOROZVON_CLIENT_SECRET']
  GOOGLE_SERVICE_ACCOUNT_JSON = $googleServiceAccountJson
  GOOGLE_SPREADSHEET_ID = $googleValues['GOOGLE_SPREADSHEET_ID']
}

foreach ($name in $secrets.Keys) {
  Require-Value -Secrets $secrets -Name $name
}

foreach ($name in $secrets.Keys | Sort-Object) {
  Set-GitHubSecret -Name $name -Value $secrets[$name] -RepoName $Repo
}

if ($googleValues['GOOGLE_SPREADSHEET_TITLE']) {
  Write-Host 'Setting GitHub variable: GOOGLE_SPREADSHEET_TITLE'
  gh variable set GOOGLE_SPREADSHEET_TITLE --repo $Repo --body $googleValues['GOOGLE_SPREADSHEET_TITLE'] | Out-Null
}

Write-Host "GitHub secrets are configured for $Repo."
