﻿<#
.SYNOPSIS
	Create Data Factory Input Dataset json 

.DESCRIPTION
	Create Data Factory Pipeline Input Dataset Json from Database Table 

.PARAMETER 
    DatabaseName
	TableName 

.EXAMPLE
	 & ".\Create-DF-InputDataSet.ps1" -ServerName "" -DatabaseName "Reporting_Extract" -TableName "IMOS_Port"

.NOTE
	Enter the database password when prompted
#>

Param
(
    [Parameter(Mandatory=$True)][String] $ServerName,
    [Parameter(Mandatory=$True)][String] $DatabaseName,
    [Parameter(Mandatory=$True)][String] $TableName,
    [Parameter(Mandatory=$False)][String] $UserName    
)

. "$PSScriptRoot\Sql-Commands.ps1"

#Import-Module Sqlps -DisableNameChecking;
Add-Type -Path "$PSScriptRoot\Newtonsoft.Json.9.0.1\lib\net45\Newtonsoft.Json.dll"


$outputPath = "$PSScriptRoot\DataFactory"
$input = New-Object -TypeName PSObject
$properties = New-Object -TypeName PSObject
$typeProperties = New-Object -TypeName PSObject
$availability =  New-Object -TypeName PSObject
$policy = New-Object -TypeName PSObject

[System.Array]$fields = @()

if(!(Test-Path -Path $outputPath))
{
     New-Item -ItemType directory -Path $outputPath
}



$columnNames = (Get-TableColumns -Server $ServerName -Database $DatabaseName -TableName $TableName -UserName $UserName)
$columnNames | ForEach {
    
    $field = New-Object -TypeName PSObject
    if($_.Name -ne $null -and $_.Name -ne "")
    {
        $field | Add-Member -MemberType NoteProperty -Name "name" -Value  $_.Name 
        $field | Add-Member -MemberType NoteProperty -Name "type" -Value  (Convert-Datatype -DataType $_.DataType) 

        $fields += $field
    }
}

$typeProperties | Add-Member -MemberType NoteProperty -Name "tableName" -Value $TableName

$availability | Add-Member -MemberType NoteProperty -Name "frequency" -Value "Day"
$availability | Add-Member -MemberType NoteProperty -Name "interval" -Value 1

$properties | Add-Member -MemberType NoteProperty -Name "structure" -Value @($fields)
$properties | Add-Member -MemberType NoteProperty -Name "published" -Value "false"
$properties | Add-Member -MemberType NoteProperty -Name "type" -Value "AzureSqlTable"
$properties | Add-Member -MemberType NoteProperty -Name "linkedServiceName" -Value $DatabaseName
$properties | Add-Member -MemberType NoteProperty -Name "typeProperties" -Value  $typeProperties
$properties | Add-Member -MemberType NoteProperty -Name "availability" -Value $availability
$properties | Add-Member -MemberType NoteProperty -Name "external" -Value true
$properties | Add-Member -MemberType NoteProperty -Name "policy" -Value  $policy


$input | Add-Member -MemberType NoteProperty -Name "name" -Value ("InputDatasets-"+$TableName)
$input | Add-Member -MemberType NoteProperty -Name "properties" -Value $properties

$result = ConvertTo-Json $input -Depth 5

$json = [Newtonsoft.Json.Linq.JObject]::Parse($result);  # Required for Pretty print


$fileName = "$outputPath\" +$TableName+".json"

$json.ToString() | Out-File $fileName

Create-UserDefinedType -Data $columnNames -Name $TableName -OutputPath $outputPath
Create-MergeScript -Data $columnNames -KeyColumns @('SamAccountName') -SourceName $TableName -TargetName $TableName -OutputPath $outputPath