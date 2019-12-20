param(
    [string]$JsonString
)

Write-Host "json: $JsonString"

$Json = ConvertFrom-Json $JsonString

Write-Host "Infrastructure output: $Json"