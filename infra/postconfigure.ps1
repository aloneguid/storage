param(
    [string] $JsonString,
    [string] $RgName
)

#Import-Module Az.DataLakeStore

$Json = ConvertFrom-Json $JsonString

$Gen1AccountName = $Json.azureGen1StorageName.value
$Gen2AccountName = $json.azureGen2StorageName.value
$OperatorObjectId = $Json.operatorObjectId.value
$TestUserObjectId = $Json.testUserObjectId.value

Write-Host "setting permissions for Data Lake Gen 1 ($Gen1AccountName)..."
# fails when ACL is already set
Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $OperatorObjectId -Permissions All -Recurse -Concurrency 128 -ErrorAction SilentlyContinue
Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $TestUserObjectId -Permissions All -Recurse -Concurrency 128 -ErrorAction SilentlyContinue

#Write-Host "settings permissions for Data Lake Gen 2 ($Gen2AccountName)..."
#see https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-rbac-powershell
#New-AzRoleAssignment -ObjectId $TestUserObjectId `
#    -RoleDefinitionName "Storage Blob Data Contributor" `
#    -ResourceGroupName $RgName