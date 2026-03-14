Get-CimInstance Win32_Process | Where-Object { $_.CommandLine -match 'runner_focus_v4\.py|approver\.py' } | Select-Object ProcessId,Name,CommandLine
