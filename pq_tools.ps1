function pq_last {
  $root = Join-Path $HOME "policyqueue"
  $log  = Join-Path $root "logs\runner.log"
  "`n=== Last PROCESS lines (start/posted/cycle) ==="
  (Get-Content $log -Tail 800 | Select-String -Pattern "PROCESS start|PROCESS posted|CYCLE done" | Select-Object -Last 30).Line
  "`n=== Latest Ollama chunk progress ==="
  (Get-Content $log -Tail 800 | Select-String -Pattern "Ollama chunk" | Select-Object -Last 30).Line
}

function pq_chunk_code {
  $root = Join-Path $HOME "policyqueue"
  Select-String -Path (Join-Path $root "runner.py") -Pattern "Ollama chunk" -Context 8,25
}
