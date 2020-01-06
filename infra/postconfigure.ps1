param(
    [string]$Gen1AccountName,
    [string]$OperatorObjectId,
    [string]$TestUserObjectId
)

Import-Module Az.DataLakeStore

Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $OperatorObjectId -Permissions All -Recurse -Concurrency 128

Set-AzDataLakeStoreItemAclEntry -Account $Gen1AccountName -Path / -AceType User `
    -Id $TestUserObjectId -Permissions All -Recurse -Concurrency 128