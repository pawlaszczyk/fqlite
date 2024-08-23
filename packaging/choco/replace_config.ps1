(Get-Content chocolateyinstall_template.ps1) | Foreach-Object { $_ -replace '{{VERSION}}', $env:VERSION ` -replace '{{HASH}}', "$env:HASH" } | Out-File -encoding ASCII fqlite/tools/chocolateyinstall.ps1


