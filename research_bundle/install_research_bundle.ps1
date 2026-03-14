$ErrorActionPreference = "Stop"
if (-not (Test-Path ".\facts")) {
  throw "Run this from C:\Users\meta4\policyqueue"
}
New-Item -ItemType Directory -Force -Path ".\research_md" | Out-Null
New-Item -ItemType Directory -Force -Path ".\research_md\drafts" | Out-Null
New-Item -ItemType Directory -Force -Path ".\research_md\verified" | Out-Null
New-Item -ItemType Directory -Force -Path ".\research_md\logs" | Out-Null
New-Item -ItemType Directory -Force -Path ".\research_md\_state" | Out-Null
Copy-Item -Force "$PSScriptRoot\research_config.json" ".\research_config.json"
Copy-Item -Force "$PSScriptRoot\research_drafts.py" ".\research_drafts.py"
Copy-Item -Force "$PSScriptRoot\research_verify.py" ".\research_verify.py"
Copy-Item -Force "$PSScriptRoot\researchctl.ps1" ".\researchctl.ps1"
Write-Host "Installed research bundle."
Write-Host "Run next: .\researchctl.ps1 all"
