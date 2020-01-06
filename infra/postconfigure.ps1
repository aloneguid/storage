param(
    [string] $JsonString
)

#Import-Module Az.DataLakeStore

$Json = ConvertFrom-Json $JsonString
$Gen1AccountName = $Json.azureGen1StorageName.value
$OperatorObjectId = $Json.operatorObjectId.value
$TestUserObjectId = $Json.testUserObjectId.value

Write-Host "setting permissions for Data Lake Gen 1 ($Gen1AccountName)..."

Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $OperatorObjectId -Permissions All -Recurse -Concurrency 128

Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $TestUserObjectId -Permissions All -Recurse -Concurrency 128