param(
    [string] $JsonString,
    [string] $Organisation,
    [string] $Project,
    [string] $GroupId,
    [string] $Pat    
)

function Get-AzPipelinesVariableGroup(
    [string] $Organisation,
    [string] $Project,
    [string] $GroupId,
    [string] $Pat
) {

    # Base64-encodes the Personal Access Token (PAT) appropriately
    $Base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f "", $Pat)))

    # GET https://dev.azure.com/{organization}/{project}/_apis/distributedtask/variablegroups/{groupId}?api-version=5.1-preview.1

    $vg = Invoke-RestMethod `
        -Uri "https://dev.azure.com/$($Organisation)/$($Project)/_apis/distributedtask/variablegroups/$($GroupId)?api-version=5.1-preview.1" `
        -Method Get `
        -ContentType "application/json" `
        -Headers @{Authorization=("Basic {0}" -f $Base64AuthInfo)}

    $vg
}

function Set-AzPipelinesVariableGroup(
    [string] $Organisation,
    [string] $Project,
    [string] $GroupId,
    [string] $Pat,
    $VariableGroup
) {

    # Base64-encodes the Personal Access Token (PAT) appropriately
    $Base64AuthInfo = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes(("{0}:{1}" -f "", $Pat)))

    # PUT https://dev.azure.com/{organization}/{project}/_apis/distributedtask/variablegroups/{groupId}?api-version=5.1-preview.1

    $body = $VariableGroup | ConvertTo-Json

    Invoke-RestMethod `
        -Uri "https://dev.azure.com/$($Organisation)/$($Project)/_apis/distributedtask/variablegroups/$($GroupId)?api-version=5.1-preview.1" `
        -Method Put `
        -ContentType "application/json" `
        -Headers @{Authorization=("Basic {0}" -f $Base64AuthInfo)} `
        -Body $body

}

function SetOrCreate(
    $VariableSet,
    [string]$Name,
    [string]$Value
)
{
    $vo = $VariableSet.variables.$Name

    if($null -eq $vo) {
        Write-Host "  creating new member"
        # create 'value' object
        $vo = New-Object -TypeName psobject
        $vo | Add-Member value $value
        
        $VariableSet.variables | Add-Member $Name $vo
    } else {
        Write-Host "  updating to $Value"
        $vo.value = $Value
    }
}

Write-Host "reading var set..."
$vset = Get-AzPipelinesVariableGroup -Organisation $Organisation -Project $Project -GroupId $GroupId -Pat $Pat
Write-Host "vset: $vset"

# enumerate arm output variables and update/create them in the variable set
$Json = ConvertFrom-Json $JsonString
foreach($armMember in $Json | Get-Member -MemberType NoteProperty) {
    $name = $armMember.Name
    $value = $Json.$name.value

    Write-Host "arm| $name = $value"
    SetOrCreate $vset $name $value
}

Write-Host "upating var set..."
Set-AzPipelinesVariableGroup -Organisation $Organisation -Project $Project -GroupId $GroupId -Pat $Pat -VariableGroup $vset