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
        [Parameter(Mandatory=$True)][string]$Database,
		[Parameter(Mandatory=$True)][string]$TableName
	)

    Set-Variable SqlConnection (New-Object System.Data.SqlClient.SqlConnection) -Scope Global -Option AllScope -Description "Global Variable for Sql Query functions"
 
    try
    { 
        $password = Read-Password
        Set-SqlConnection -ConnectionString "Server=tcp:interlake-bi.database.secure.windows.net;initial catalog=$Database;User ID=BIAdmin;Password=$password"
        
        $query = "SELECT COLUMN_NAME Name, DATA_TYPE DataType FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '$TableName' AND TABLE_SCHEMA='dbo'"
        $columnNames = (Execute-SqlQuery  -Query $query)

        return $columnNames
    }
    finally
    {
         Close-SqlConnection 
    }
}