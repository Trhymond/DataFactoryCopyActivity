﻿<#
.SYNOPSIS
	Create Data Factory Output Dataset json 

.DESCRIPTION
	Create Data Factory Pipeline Output Dataset Json from Database Table 

.PARAMETER 
	TableName 
	LinkedServiceName

.EXAMPLE
	 & ".\Create-DF-OutputDataSet.ps1" -TableName "DimPort" -LinkedServiceName "ISS_DW" 

.NOTE
	Enter the database password when prompted
#>

Param
(
    [Parameter(Mandatory=$True)][String] $TableName,
    [Parameter(Mandatory=$True)][String] $LinkedServiceName
)

. "$PSScriptRoot\Sql-Commands.ps1"

#Import-Module Sqlps -DisableNameChecking;
Add-Type -Path "$PSScriptRoot\Newtonsoft.Json.9.0.1\lib\net45\Newtonsoft.Json.dll"

$outputPath = "$PSScriptRoot\DataFactory"
$input = New-Object -TypeName PSObject
$properties = New-Object -TypeName PSObject
$typeProperties = New-Object -TypeName PSObject
$availability = New-Object -TypeName PSObject
$policy = New-Object -TypeName PSObject

[System.Array]$fields = @()

if(!(Test-Path -Path $outputPath))
{
     New-Item -ItemType directory -Path $outputPath
}


<# This method will not work on Azure Sql Databases #> 
 <#
$columnNames = dir 'SQLSERVER:\SQL\RHYMOND-01\DEFAULT\Databases\ISS_DW\Tables' | 
    Where-Object {$_.DisplayName -match "dbo."+$TableName} | 
        ForEach-Object {$_.Columns} |
            Select-Object Name, DataType
#>

$columnNames = (Get-TableColumns -Database "ISS_DW" -TableName $TableName)

$columnNames | ForEach {
    
    $field = New-Object -TypeName PSObject
    $field | Add-Member -MemberType NoteProperty -Name "name" -Value $_.Name
    $field | Add-Member -MemberType NoteProperty -Name "type" -Value (Convert-Datatype -DataType $_.DataType)

    $fields +=  $field 
}

$typeProperties | Add-Member -MemberType NoteProperty -Name "tableName" -Value $TableName

$availability | Add-Member -MemberType NoteProperty -Name "frequency" -Value "Day"
$availability | Add-Member -MemberType NoteProperty -Name "interval" -Value  1

$properties | Add-Member -MemberType NoteProperty -Name "structure" -Value @($fields)
$properties | Add-Member -MemberType NoteProperty -Name "published" -Value "false"
$properties | Add-Member -MemberType NoteProperty -Name "type" -Value "AzureSqlTable"
$properties | Add-Member -MemberType NoteProperty -Name "linkedServiceName" -Value $LinkedServiceName
$properties | Add-Member -MemberType NoteProperty -Name "typeProperties" -Value  $typeProperties
$properties | Add-Member -MemberType NoteProperty -Name "availability" -Value $availability
$properties | Add-Member -MemberType NoteProperty -Name "external" -Value false
$properties | Add-Member -MemberType NoteProperty -Name "policy" -Value  $policy


$input | Add-Member -MemberType NoteProperty -Name "name" -Value ("OutputDatasets-e7f-"+$TableName)
$input | Add-Member -MemberType NoteProperty -Name "properties" -Value $properties

$result = ConvertTo-Json $input -Depth 5

$json = [Newtonsoft.Json.Linq.JObject]::Parse($result);  # Required for Pretty print


$fileName = "$outputPath\" +$TableName+".json"

$json.ToString() | Out-File $fileName