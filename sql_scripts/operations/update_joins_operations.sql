
	ALTER TABLE ELPaso_TX.dbo.TablesToConvert_Operations ALTER COLUMN Select_Into TEXT;
	ALTER TABLE ELPaso_TX.dbo.TablesToConvert_Operations ALTER COLUMN Select_Only TEXT;
	ALTER TABLE ELPaso_TX.dbo.TablesToConvert_Operations ALTER COLUMN Joins TEXT;

	UPDATE ELPaso_TX.dbo.TableUsedSelects_Operations SET Freq=LTRIM(RTRIM(REPLACE(REPLACE(Freq,',',''),'nan',0)));
	UPDATE ELPaso_TX.dbo.TableUsedSelects_Operations SET InScopeFreq=LTRIM(RTRIM(REPLACE(REPLACE(InScopeFreq,',',''),'nan',0)));
	UPDATE ELPaso_TX.dbo.TableUsedSelects_Operations SET fConvert=LTRIM(RTRIM(REPLACE(REPLACE(fConvert,'.0',''),'nan',0)));

	ALTER TABLE ELPaso_TX.dbo.TableUsedSelects_Operations ALTER COLUMN Freq INT NOT NULL;
	ALTER TABLE ELPaso_TX.dbo.TableUsedSelects_Operations ALTER COLUMN InScopeFreq INT NOT NULL;
	ALTER TABLE ELPaso_TX.dbo.TableUsedSelects_Operations ALTER COLUMN fConvert BIT NOT NULL;

	UPDATE TTC SET
		  Joins			=REPLACE(LTRIM(RTRIM(SUBSTRING(S.SELECT_ONLY,CHARINDEX('A WITH (NOLOCK)',S.SELECT_ONLY)+15,8000))),'() AS YoDate','')
		 ,ScopeRowCount	=S.InScopeFreq
		 ,ScopeComment	=S.Comment
		 ,fConvert		=S.fConvert
	FROM
		ELPaso_TX.dbo.TableUsedSelects_Operations S 
			INNER JOIN ELPaso_TX.DBO.TablesToConvert_Operations TTC WITH (NOLOCK) ON S.DatabaseName=TTC.DatabaseName AND S.SchemaName=TTC.SchemaName AND S.TableName=TTC.TableName;
