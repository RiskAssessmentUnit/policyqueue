param(
  [ValidateSet("draft","verify","all")]
  [string]$cmd = "all"
)

$ErrorActionPreference = "Stop"

switch ($cmd) {
  "draft"  { py .\research_drafts.py }
  "verify" { py .\research_verify.py }
  "all"    { py .\research_drafts.py; py .\research_verify.py }
}
