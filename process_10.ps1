$root = "$HOME\policyqueue"
$inbox = Join-Path $root "inbox"
$tmp = Join-Path $root "inbox_batch"

if (Test-Path $tmp) { Remove-Item $tmp -Recurse -Force }
New-Item -ItemType Directory -Path $tmp | Out-Null

$batch = Get-ChildItem $inbox -File | Where-Object { $_.Extension -in @(".pdf",".txt") } | Sort-Object LastWriteTime | Select-Object -First 10
if (-not $batch) { Write-Host "No files to process."; exit 0 }

foreach ($f in $batch) {
  Move-Item $f.FullName $tmp -Force
}

Write-Host ("Batch size: " + $batch.Count)
python "$root\pq.py" run

# Anything left in tmp (e.g. if pq.py errored) goes back
Get-ChildItem $tmp -File | ForEach-Object { Move-Item $_.FullName $inbox -Force }

Remove-Item $tmp -Recurse -Force
