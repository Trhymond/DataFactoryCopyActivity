<#
.SYNOPSIS
	Create Data Factory Pipeline Activity json 

.DESCRIPTION
	Create Data Factory Pipeline Activity Json from Source and Destination Tables for Copy

.PARAMETER 
	SourceTableName 
	DestTableName
	Id

.EXAMPLE
	 & ".\Create-DF-Activity.ps1" -SourceTableName "IMOS_Port" -DestTableName "DimPort" -Id "7" 

.NOTE
	Enter the database password when prompted
#>
Param
(
    [Parameter(Mandatory=$True)][String] $ServerName,
    [Parameter(Mandatory=$True)][String] $DatabaseName,
    [Parameter(Mandatory=$True)][String] $SourceTableName,
    [Parameter(Mandatory=$True)][String] $DestTableName,
    [Parameter(Mandatory=$True)][String] $Id,
    [Parameter(Mandatory=$False)][String] $UserName    
)

. "$PSScriptRoot\Sql-Commands.ps1"

#Import-Module Sqlps -DisableNameChecking;
Add-Type -Path "$PSScriptRoot\Newtonsoft.Json.9.0.1\lib\net45\Newtonsoft.Json.dll"

$outputPath = "$PSScriptRoot\DataFactory"

if(!(Test-Path -Path $outputPath))
{
     New-Item -ItemType directory -Path $outputPath
}

$columnNames = (Get-TableColumns -Server $ServerName -Database $DatabaseName -TableName $DestTableName -UserName $UserName)
$columnNames | ForEach {
    
    $column_mapping +=  $_.Name+ ":" + $_.Name + ";"
}

$column_mapping = $column_mapping.Trim().Substring(0, $column_mapping.Trim().Length -1)
       
$activity = New-Object -TypeName PSObject
$typeProperties = New-Object -TypeName PSObject
$source = New-Object -TypeName PSObject
$sink = New-Object -TypeName PSObject
$translator = New-Object -TypeName PSObject
$input = New-Object -TypeName PSObject
$output = New-Object -TypeName PSObject
$policy = New-Object -TypeName PSObject
$scheduler = New-Object -TypeName PSObject

$source | Add-Member -MemberType NoteProperty -Name "type" -Value "SqlSource"
$source | Add-Member -MemberType NoteProperty -Name "sqlReaderQuery" -Value "`$`$Text.Format('select * from [$SourceTableName] where [LastUpdateGmt] >= \'{0:yyyy-MM-dd HH:mm}\' AND [LastUpdateGmt] < \'{1:yyyy-MM-dd HH:mm}\'', WindowStart, WindowEnd)"


$sink | Add-Member -MemberType NoteProperty -Name "type" -Value "SqlSink"
$sink | Add-Member -MemberType NoteProperty -Name "sqlWriterStoredProcedureName" -Value ("Load" + $DestTableName)
$sink | Add-Member -MemberType NoteProperty -Name "sqlWriterTableType" -Value ($DestTableName+"Data")
$sink | Add-Member -MemberType NoteProperty -Name "writeBatchSize" -Value "0"
$sink | Add-Member -MemberType NoteProperty -Name "writeBatchTimeout" -Value "00:00:00"

$translator | Add-Member -MemberType NoteProperty -Name "type" -Value "TabularTranslator"
$translator | Add-Member -MemberType NoteProperty -Name "columnMappings" -Value $column_mapping

$typeProperties | Add-Member -MemberType NoteProperty -Name "source" -Value $source
$typeProperties | Add-Member -MemberType NoteProperty -Name "sink" -Value $sink
$typeProperties | Add-Member -MemberType NoteProperty -Name "translator" -Value $translator


$input | Add-Member -MemberType NoteProperty -Name "name" -Value "InputDatasets-$SourceTableName"

$output | Add-Member -MemberType NoteProperty -Name "name" -Value "OutputDatasets-$DestTableName"

$policy | Add-Member -MemberType NoteProperty -Name "timeout" -Value "02:00:00"
$policy | Add-Member -MemberType NoteProperty -Name "concurrency" -Value "1"
$policy | Add-Member -MemberType NoteProperty -Name "executionPriorityOrder" -Value "NewestFirst"
$policy | Add-Member -MemberType NoteProperty -Name "style" -Value "StartOfInterval"
$policy | Add-Member -MemberType NoteProperty -Name "retry" -Value "3"
$policy | Add-Member -MemberType NoteProperty -Name "longRetry" -Value "0"
$policy | Add-Member -MemberType NoteProperty -Name "longRetryInterval" -Value "00:00:00"

$scheduler | Add-Member -MemberType NoteProperty -Name "frequency" -Value "Day"
$scheduler | Add-Member -MemberType NoteProperty -Name "interval" -Value "1"

$activity | Add-Member -MemberType NoteProperty -Name "type" -Value "Copy"
$activity | Add-Member -MemberType NoteProperty -Name "typeProperties" -Value $typeProperties
$activity | Add-Member -MemberType NoteProperty -Name "inputs" -Value @($input)
$activity | Add-Member -MemberType NoteProperty -Name "outputs" -Value @($output)
$activity | Add-Member -MemberType NoteProperty -Name "policy" -Value $policy
$activity | Add-Member -MemberType NoteProperty -Name "scheduler" -Value $scheduler
$activity | Add-Member -MemberType NoteProperty -Name "name" -Value "CopyActivity-$DestTableName-$Id"



$result = ConvertTo-Json  $activity

$json = [Newtonsoft.Json.Linq.JObject]::Parse($result);  # Required for Pretty print


$fileName = "$outputPath\CopyActivity" + -$Id +".json"

$json.ToString() | Out-File $fileName