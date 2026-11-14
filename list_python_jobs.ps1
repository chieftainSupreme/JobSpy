Get-CimInstance Win32_Process -Filter "Name = 'python.exe'" |
    Where-Object { $_.CommandLine } |
    Select-Object ProcessId, CommandLine |
    ConvertTo-Json -Compress
