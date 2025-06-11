-- Drop table If exists
	DROP TABLE IF EXISTS ELPaso_TX.dbo.PrimaryKeyScripts;
-- Create Table to store scripts
	CREATE TABLE ELPaso_TX.dbo.PrimaryKeyScripts
		(
			 ScriptType NVARCHAR(50)-- 'PK' or 'NOT_NULL'
			,DatabaseName SYSNAME
			,SchemaName SYSNAME
			,TableName SYSNAME
			,Script NVARCHAR(MAX)
		);

 -- Generate PK scripts using FOR XML PATH 
	INSERT INTO ELPaso_TX.dbo.PrimaryKeyScripts (ScriptType,DatabaseName,SchemaName,TableName,Script)
			SELECT 'PK','ELPaso_TX',S.[NAME],T.[NAME],
				'ALTER TABLE [ELPaso_TX].[' + s.name + '].[' + t.name + '] ADD CONSTRAINT [' + kc.name + '] PRIMARY KEY (' +STUFF((SELECT ', [' + c2.name + ']' FROM Justice.sys.index_columns ic2 JOIN Justice.sys.columns c2 ON c2.object_id=ic2.object_id AND c2.column_id=ic2.column_id WHERE ic2.object_id=t.object_id AND ic2.index_id=kc.unique_index_id ORDER BY ic2.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'), 1, 2, '') + ')' AS Script 
			FROM 
				Justice.sys.tables t
					INNER JOIN Justice.sys.schemas s ON t.schema_id=s.schema_id
					INNER JOIN Justice.sys.key_constraints kc ON kc.parent_object_id=t.object_id AND kc.type='PK';

	INSERT INTO ELPaso_TX.dbo.PrimaryKeyScripts (ScriptType,DatabaseName,SchemaName,TableName,Script)
			SELECT 'NOT_NULL','ELPaso_TX',S.[NAME],T.[NAME],
				'ALTER TABLE [ELPaso_TX].[' + s.name + '].[' + t.name + '] ALTER COLUMN [' + c.name + '] ' + UPPER(case tp.system_type_id when 36 then 'uniqueidentifier' when 48 then 'tinyint' when 52 then 'smallint' when 56 then 'int' when 61 then 'datetime' when 104 then 'flag' when 127 then 'bigint' when 167 then 'varchar' when 175 then 'char' else tp.[name] end) + CASE WHEN case tp.system_type_id when 36 then 'uniqueidentifier' when 48 then 'tinyint' when 52 then 'smallint' when 56 then 'int' when 61 then 'datetime' when 104 then 'flag' when 127 then 'bigint' when 167 then 'varchar' when 175 then 'char' else tp.[name] end IN ('varchar', 'char', 'varbinary', 'binary') THEN '(' + CASE WHEN c.max_length=-1 THEN 'MAX' ELSE CAST(c.max_length AS VARCHAR(10)) END + ')' WHEN case tp.system_type_id when 36 then 'uniqueidentifier' when 48 then 'tinyint' when 52 then 'smallint' when 56 then 'int' when 61 then 'datetime' when 104 then 'flag' when 127 then 'bigint' when 167 then 'varchar' when 175 then 'char' else tp.[name] end IN ('nvarchar', 'nchar') THEN '(' + CASE WHEN c.max_length=-1 THEN 'MAX' ELSE CAST(c.max_length / 2 AS VARCHAR(10)) END + ')' WHEN case tp.system_type_id when 36 then 'uniqueidentifier' when 48 then 'tinyint' when 52 then 'smallint' when 56 then 'int' when 61 then 'datetime' when 104 then 'flag' when 127 then 'bigint' when 167 then 'varchar' when 175 then 'char' else tp.[name] end IN ('decimal', 'numeric') THEN '(' + CAST(c.precision AS VARCHAR) + ',' + CAST(c.scale AS VARCHAR) + ')' ELSE '' END + ' NOT NULL' AS Script
			FROM 
				Justice.sys.tables t
					INNER JOIN Justice.sys.schemas s ON t.schema_id=s.schema_id
					INNER JOIN Justice.sys.key_constraints kc ON kc.parent_object_id=t.object_id AND kc.type='PK'
					INNER JOIN Justice.sys.index_columns ic ON ic.object_id=kc.parent_object_id AND ic.index_id=kc.unique_index_id
					INNER JOIN Justice.sys.columns c ON c.object_id=t.object_id AND c.column_id=ic.column_id
					INNER JOIN Justice.sys.types tp ON c.user_type_id=tp.user_type_id;
