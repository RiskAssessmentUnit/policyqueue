param(
  [string]$Model = "llama3.1:8b-instruct-q4_K_M",
  [string]$HostIP = "100.124.222.121"
)

$ErrorActionPreference = "Stop"

$root    = "$HOME\policyqueue"
$inbox   = Join-Path $root "inbox"
$facts   = Join-Path $root "facts"
$archive = Join-Path $root "archive"
$logs    = Join-Path $root "logs"
$logFile = Join-Path $logs "extract.log"
$pyPath  = Join-Path $root "pdf_to_text.py"
$Base    = "http://$HostIP`:11434"

function Log($msg) {
  $ts = (Get-Date).ToString("yyyy-MM-dd HH:mm:ss")
  "$ts  $msg" | Out-File -Append -Encoding utf8 $logFile
}

function PdfToText([string]$pdfFile) {
  if (-not (Test-Path $pyPath)) {
    Log "Missing pdf_to_text.py at $pyPath"
    return ""
  }

  # Run python without letting PowerShell treat stderr as a terminating error
  $out = cmd /c ""python """"$pyPath"""" """"$pdfFile"""" 2>&1"""
  $code = $LASTEXITCODE

  if ($code -ne 0) {
    Log ("PDF->text FAILED: " + (Split-Path $pdfFile -Leaf) + " exit=" + $code)
    # Log a few lines for debugging
    $preview = ($out | Select-Object -First 8) -join " | "
    if ($preview) { Log ("PDF->text output: " + $preview) }
    return ""
  }

  return ($out -join "`n").Trim()
}

function CallOllama([string]$prompt) {
  $body = @{ model=$Model; prompt=$prompt; stream=$false } | ConvertTo-Json -Depth 6
  $resp = Invoke-RestMethod -Uri "$Base/api/generate" -Method Post -ContentType "application/json" -Body $body -TimeoutSec 300
  return ($resp.response.Trim())
}

function ExtractFacts([string]$text) {
  $prompt = @"
You are an information extraction engine.
Output MUST be valid JSON only. No markdown. No extra text.

TASK:
Extract Kansas public-policy / public-finance facts from the provided TEXT.

RULES:
- If a field is unknown, use null.
- Use numbers (not strings) for numeric fields.
- Do not guess. Only extract what is in the TEXT.

Return JSON with exactly this top-level shape:
{
  "program_type": "STAR_BOND" | "BILL" | "FISCAL_NOTE" | "AUDIT" | "NEWS" | "OTHER",
  "title": string|null,
  "jurisdiction": "Kansas"|null,
  "locations": [string],
  "entities": [
    { "name": string, "type": "PERSON"|"ORG"|"GOV_BODY"|"PROJECT"|"OTHER" }
  ],
  "key_numbers": [
    { "label": string, "value": number, "unit": "USD"|"PERCENT"|"JOBS"|"YEAR"|"OTHER", "year": number|null }
  ],
  "events": [
    { "date": string|null, "year": number|null, "description": string }
  ],
  "evidence": [
    { "quote": string, "note": string }
  ],
  "uncertainties": [string],
  "recommended_next_queries": [string]
}

TEXT:
"""$text"""
"@
  return (CallOllama -prompt $prompt)
}

# ---- Main ----
Log ("Starting extract run. Host=" + $HostIP + " Model=" + $Model)

# Verify API
$null = Invoke-RestMethod -Uri "$Base/api/tags" -Method Get -TimeoutSec 10

$files = Get-ChildItem $inbox -File | Where-Object { $_.Extension -in @(".txt",".pdf") } | Sort-Object LastWriteTime
if (-not $files) {
  Log "No .txt or .pdf files found in inbox."
  Write-Host "No .txt or .pdf files found in inbox."
  exit 0
}

foreach ($f in $files) {
  Log ("Processing: " + $f.Name)

  $text = ""
  if ($f.Extension -eq ".txt") {
    $text = Get-Content $f.FullName -Raw -Encoding utf8
  } elseif ($f.Extension -eq ".pdf") {
    $text = PdfToText $f.FullName
  }

  if (-not $text) {
    Log ("No extracted text for: " + $f.Name + " (skipping)")
    continue
  }

  $json = ExtractFacts $text

  try { $null = $json | ConvertFrom-Json -ErrorAction Stop } catch {
    Log ("JSON parse FAILED for: " + $f.Name + " (writing raw)")
    $json | Out-File -Encoding utf8 (Join-Path $facts ($f.BaseName + ".raw.txt"))
    continue
  }

  $outPath = Join-Path $facts ($f.BaseName + ".json")
  $json | Out-File -Encoding utf8 $outPath
  Log ("Wrote facts: " + $outPath)

  $stamp = (Get-Date).ToString("yyyyMMdd-HHmmss")
  $archPath = Join-Path $archive ($f.BaseName + "." + $stamp + $f.Extension)
  Move-Item $f.FullName $archPath -Force
  Log ("Archived input: " + $archPath)
}

Write-Host "Done. Facts in: $facts"


