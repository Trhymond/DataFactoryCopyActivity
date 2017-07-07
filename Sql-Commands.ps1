<#
.SYNOPSIS
	Create and Open Sql Server Connection

.DESCRIPTION
	Create and Open Sql Server Connection

.PARAMETER 

.EXAMPLE
	 Set-SqlConnection -ConnectionString "Server=[your srver];initial catalog=[your database];User ID=[your user name];Password=[your password]"

.NOTE
	For Azure Sql user server name as tcp:[servername].database.secure.windows.net or tcp:[servername].database.windows.net
#>

function Set-SqlConnection 
{
	[CmdletBinding()]
	Param (
		[Parameter(Mandatory=$True)][string] $ConnectionString
	)
	
	$SqlConnection.ConnectionString = $ConnectionString

	if (-not ($SqlConnection.State -like "Open"))
	{
		$SqlConnection.Open()
	}
}

<#
.SYNOPSIS
	Close and dispose Sql Server Connection

.DESCRIPTION
	Close and dispose Sql Server Connection

.PARAMETER 

.EXAMPLE
	 Close-SqlConnection
#>

function Close-SqlConnection 
{
	$SqlConnection.Close()
	$SqlConnection.Dispose()

    $SqlConnection = $null
}


<#
.SYNOPSIS
	Read Password

.DESCRIPTION
	Read password from host as secure string and decrypt to normal string. 
	Azure Sql Connection string requires text password.

.PARAMETER 

.EXAMPLE
	 Read-Password
#>
function Read-Password
{
    $password=read-host -AsSecurestring
    $securePassword = [System.Runtime.InteropServices.Marshal]::SecureStringToBSTR($password)
    $decodedpassword = [System.Runtime.InteropServices.Marshal]::PtrToStringAuto($securePassword)

    return $decodedpassword
}

<#
.SYNOPSIS
	Execute Sql Query 

.DESCRIPTION
	Execute a Sql Query and returns a PSObject Array

.PARAMETER 
	Query

.EXAMPLE
	 Execute-SqlQuery -Query "select * from schema.table"
#>
function Execute-SqlQuery 
{

	[CmdletBinding()]
	Param(
		[Parameter(Mandatory=$True)][string]$Query
	)
	if ($SqlConnection.State -like "Open")
	{
		$result = @()

		$SqlCmd = New-Object System.Data.SqlClient.SqlCommand $Query, $SqlConnection
        $SqlCmd.CommandTimeout = 0
		$datareader = $SqlCmd.ExecuteReader()
		while($datareader.Read())
		{
			$row = New-Object PSObject
			for($id = 0; $id -lt $datareader.FieldCount; $id++) {
				$row | Add-Member -MemberType NoteProperty -Name $datareader.GetName($id) -Value $datareader.GetValue($id)
			}
			$result += $row
		}
		$datareader.Close()
		$datareader.Dispose()

		return @($result)
	}
	else
	{
		Write-Host "Connection Failure";
	}
}

<#
.SYNOPSIS
	Execute Sql Non-Query

.DESCRIPTION
	Execute a Sql non-Query 

.PARAMETER 
	Query

.EXAMPLE
	 Execute-SqlNonQuery -Query "update schema.table set column = ''"
#>
function Execute-SqlNonQuery 
{
	[CmdletBinding()]
	Param(
		[Parameter(Mandatory=$True)][string]$Query,
		[Parameter(Mandatory=$True)][string]$Source
	)

	try {
		if ($SqlConnection.State -like "Open")
		{
			$SqlCmd = New-Object System.Data.SqlClient.SqlCommand $Query, $SqlConnection
            $SqlCmd.CommandTimeout = 0

			$result = $SqlCmd.ExecuteNonQuery()
		}
		else
		{
			Write-Output "Connection Failure";
		}
	}
	catch [System.Exception] {
        Write-Output $Query
		Write-Output ($Source + ": " + $_.Exception.Message)
	}
}

<#
.SYNOPSIS
	Convert DataTypes 

.DESCRIPTION
	Convert Sql DataTypes to DataFactory Data Types

.PARAMETER 
	DataType

.EXAMPLE
	 Convert-Datatype -DataType "varchar"
#>
function Convert-Datatype
{
    Param
    (
         [Parameter(Mandatory=$True)][String] $DataType 
    )

    $conversionTable = @{'bigint'='Int64';'binary'='Byte[]';'bit'='Boolean';'char'='String';'date'='Datetime';'datetime'='Datetime';'datetime2'='Datetime';'decimal'='Decimal';'float'='Double';'int'='Int32';'money'='Decimal';'nchar'='String';'ntext'='String';'numeric'='Decimal';'nvarchar'='String';'real'='Single';'smalldatetime'='Datetime';'smallint'='Int16';'smallmoney'='Decimal';'text'='String';'time'='TimeSpan';'timestamp'='Byte[]';'tinyint'='Byte';'uniqueidentifier'='Guid';'varbinary'='Byte[]';'varchar'='String';'image'='Byte[]'}

    $DataType = $DataType.ToLower()

    $netType = $DataType 

    if($conversionTable.ContainsKey($DataType))
    {
        $netType = $conversionTable.Get_Item($DataType)
    }

    return $netType
}

<#
.SYNOPSIS
	Get table column names and data types

.DESCRIPTION
	Get table column names and data types

.PARAMETER 
	Database
	TableName

.EXAMPLE
	 Get-TableColumns -Database "IIS_DW" -TableName "DimPort"
#>
function Get-TableColumns
{
   	[CmdletBinding()]
	Param(
        [Parameter(Mandatory=$True)][string]$Server,
        [Parameter(Mandatory=$True)][string]$Database,
		[Parameter(Mandatory=$True)][string]$TableName,
        [Parameter(Mandatory=$False)][string]$UserName
	)

    #tcp:interlake-bi.database.secure.windows.net
    Set-Variable SqlConnection (New-Object System.Data.SqlClient.SqlConnection) -Scope Global -Option AllScope -Description "Global Variable for Sql Query functions"
 
    try
    { 
        if($UserName) {
            $password = Read-Password 
        }

        #Set-SqlConnection -ConnectionString "Server=$Server;initial catalog=$Database;User ID=$UserName;Password=$password"
        Set-SqlConnection -ConnectionString "Server=$Server;initial catalog=$Database; Integrated Security=SSPI"

        $query = "SELECT COLUMN_NAME Name, DATA_TYPE DataType, '[' + COLUMN_NAME + '] ' + (case when CHARACTER_MAXIMUM_LENGTH is null and  DATA_TYPE = 'decimal' then  '['+DATA_TYPE + '] (' + convert(varchar(5), NUMERIC_PRECISION) +', ' +  convert(varchar(5), NUMERIC_SCALE) + ')' when CHARACTER_MAXIMUM_LENGTH is null then '[' + DATA_TYPE + ']' 	when CHARACTER_MAXIMUM_LENGTH = -1 then '['+DATA_TYPE + '] (max)' else '['+DATA_TYPE + '] (' + convert(varchar(5), CHARACTER_MAXIMUM_LENGTH) + ')'  end) + ' ' + (case when IS_NULLABLE = 'NO' then 'NOT NULL' else 'NULL' end) [ColText] FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$TableName' AND TABLE_SCHEMA='dbo'"
        $columnNames = (Execute-SqlQuery  -Query $query)

        return $columnNames
    }
    finally
    {
         Close-SqlConnection 
    }
}

<#
.SYNOPSIS
	Create T-SQL Merge  script

.DESCRIPTION
	Create T-SQL Merge statement for the table

.PARAMETER 
	Data
	KeyColumns
    SourceName
    TargetName
    OutputPath

.EXAMPLE
	 Create-MergeScript -Data  -KeyColumns @("", "") -SourceName "" -TargetName "" -OutputPath ""
#>

function Create-MergeScript
{
   	[CmdletBinding()]
	Param(
        [Parameter(Mandatory=$True)][PSObject[]]$Data,
        [Parameter(Mandatory=$True)][string[]]$KeyColumns,
        [Parameter(Mandatory=$True)][string]$SourceName,
        [Parameter(Mandatory=$True)][string]$TargetName,
        [Parameter(Mandatory=$True)][string]$OutputPath
	)

    $first = $true
    $comma = " "
    $fieldList1 = ""
    $fieldList2 = ""
    $fieldList3 = ""
    $fieldList_q = ""
    
    $sp = "CREATE Procedure [dbo].[Load$TableName]"+ "`r`n"
    $sp += "(" +"`r`n"
    $sp += "`t@$TableName $TableName" +"Data READONLY " + "`r`n" 
    $sp += ")" + "`r`n"
    $sp += "AS" + "`r`n"
    $sp += "BEGIN" + "`r`n"
    $sp += "`r`n"
    $sp += "`tMERGE [dbo].[$TargetName] as target " + "`r`n"
    $sp += "`t`tUSING (" + "`r`n"
    $sp += "`t`t`t select " + "`r`n"

    $Data | ForEach {
        if($_.Name -ne $null -and $_.Name -ne "")
        {
            if($first -eq $false) {
                $comma = ","
            } 

            $fieldList_q += "`t`t`t" + $comma + "["+$_.Name+"]" + "`r`n"
            $fieldList_s += $comma + "["+$_.Name+"]" 
            $fieldList_i += $comma + "source.["+$_.Name+"]" 
            $fieldList_u += "`t`t`t " + $comma + "["+$_.Name+"] = source.["+$_.Name+"]" + "`r`n"
            

            $first = $false
        }
    }

    $sp += $fieldList_q + "`r`n"
    $sp += "`t`t`tfrom @$SourceName" + "`r`n"
    $sp += "`t`t)" + "`r`n"
    $sp += "`t`tAS source ($fieldList_s)" + "`r`n"
    $sp += "`t`tON" + "`r`n"
    $sp += "`t`t(" + "`r`n"

    # key columns
    $first = $true
    $KeyColumns | ForEach {
        if($first -eq $true) {
            $sp += "`t`t`t [$_] = [$_]" +"`r`n"
        } else {
            $sp += "`t`t`t AND [$_] = [$_]" +"`r`n"
            $first = $false
       }
    }

    $sp += "`t`t)" + "`r`n"
    $sp += "`t`tWHEN MATCHED THEN " + "`r`n"
    $sp += "`t`t`tUPDATE SET " + "`r`n"
    $sp += "  " + "$fieldList_u" + "`r`n"
    $sp += "`t`tWHEN NOT MATCHED BY TARGET THEN " + "`r`n"
    $sp += "`t`t`tINSERT($fieldList_s)" + "`r`n"
    $sp += "`t`t`t`VALUES($fieldList_i)" + "`r`n"
    $sp += ";" + "`r`n"
    $sp += "END" + "`r`n"
    $udType += "GO"

    $fileName = "$OutputPath\Load" +$TargetName+".procedure.sql"
    $sp | Out-File $fileName

}

<#
.SYNOPSIS
	Create T-SQL User Defined Type  script

.DESCRIPTION
	Create User defined table type

.PARAMETER 
	Data
	KeyColumns
    SourceName
    TargetName
    OutputPath

.EXAMPLE
	 Create-UserDefinedType  -Data -Name ""   -OutputPath ""
#>

function Create-UserDefinedType 
{
   	[CmdletBinding()]
	Param(
        [Parameter(Mandatory=$True)][PSObject[]]$Data,
        [Parameter(Mandatory=$True)][string]$Name,
        [Parameter(Mandatory=$True)][string]$OutputPath
	)

    $first = $true
    $udType = "CREATE TYPE [dbo].[$TableName] AS TABLE ( " + "`r`n"

    $Data | ForEach {
        if($_.Name -ne $null -and $_.Name -ne "")
        {
            if($first -eq $true){
                $udType +=  "`t`t" + ($_.ColText + "`r`n")
            } else {
               $udType +=  "`t`t,"+($_.ColText + "`r`n") 
            }

            $first = $false
        }
    }

    $udType += ")" + "`r`n"
    $udType += "GO"

    $fileName = "$OutputPath\" +$Name+"Data.Funtion.sql"
    $udType | Out-File $fileName

}
