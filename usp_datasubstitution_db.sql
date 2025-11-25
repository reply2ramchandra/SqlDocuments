USE [master]
GO

/****** Object:  StoredProcedure [dbo].[RandomizeAllTableData_SameLen_col_match_ex_pk_e_db]    Script Date: 11/11/2025 6:56:39 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


CREATE   PROCEDURE [dbo].[RandomizeAllTableData_SameLen_col_match_ex_pk_e_db]
      @DbName        sysname
    , @IncludeFks    bit  = 0
    , @CommitChanges bit  = 1
    , @Verbose       bit  = 0
    , @Salt          nvarchar(100) = N'MaskSalt1'
AS
BEGIN
    SET NOCOUNT ON;

    IF DB_ID(@DbName) IS NULL
    BEGIN
        RAISERROR('Database %s not found.',16,1,@DbName);
        RETURN;
    END;

    DECLARE @Cols TABLE
    (
        SchemaName sysname,
        TableName  sysname,
        ColumnName sysname,
        DataType   sysname,
        MaxLen     int,
        Prec       tinyint,
        Scale      tinyint
    );

    DECLARE @MetaSql nvarchar(MAX) =
    N'SELECT s.name AS SchemaName,
             t.name AS TableName,
             c.name AS ColumnName,
             ty.name AS DataType,
             c.max_length,
             c.[precision],
             c.scale
      FROM ' + QUOTENAME(@DbName) + N'.sys.tables t
      JOIN ' + QUOTENAME(@DbName) + N'.sys.schemas s ON s.schema_id = t.schema_id
      JOIN ' + QUOTENAME(@DbName) + N'.sys.columns c ON c.object_id = t.object_id
      JOIN ' + QUOTENAME(@DbName) + N'.sys.types ty ON ty.user_type_id = c.user_type_id
      LEFT JOIN (
            SELECT kc.parent_object_id, ic.column_id
            FROM ' + QUOTENAME(@DbName) + N'.sys.key_constraints kc
            JOIN ' + QUOTENAME(@DbName) + N'.sys.index_columns ic
              ON ic.object_id = kc.parent_object_id
             AND ic.index_id  = kc.unique_index_id
            WHERE kc.type = ''PK''
      ) pk ON pk.parent_object_id = c.object_id AND pk.column_id = c.column_id
      LEFT JOIN (
            SELECT fkc.parent_object_id, fkc.parent_column_id AS column_id
            FROM ' + QUOTENAME(@DbName) + N'.sys.foreign_key_columns fkc
      ) fk ON fk.parent_object_id = c.object_id AND fk.column_id = c.column_id
      WHERE t.is_ms_shipped = 0
        AND pk.column_id IS NULL
        AND c.is_identity = 0
        AND c.is_computed = 0
        AND (' + CASE WHEN @IncludeFks=1 THEN '1=1' ELSE 'fk.column_id IS NULL' END + N')
        AND (
               LOWER(c.name) LIKE ''%email%'' OR
               LOWER(c.name) LIKE ''%ssn%''   OR
               LOWER(c.name) LIKE ''%dob%''   OR
               LOWER(c.name) LIKE ''%tin%''   OR
               LOWER(c.name) LIKE ''%fax%''   OR
               LOWER(c.name) LIKE ''%taxid%'' OR
               LOWER(c.name) LIKE ''%npi%''   OR
               LOWER(c.name) LIKE ''%name%''  OR
               LOWER(c.name) LIKE ''%phone%'' OR
               LOWER(c.name) LIKE ''%zip%''   OR
               LOWER(c.name) LIKE ''%state%'' OR
               LOWER(c.name) LIKE ''%city%''  OR
               LOWER(c.name) LIKE ''%contact%'' OR
               LOWER(c.name) LIKE ''%address%'' OR
               LOWER(c.name) LIKE ''%date%''  OR
               LOWER(c.name) LIKE ''%license%'' OR
               LOWER(c.name) LIKE ''%patient%''
            )
        AND ty.name NOT IN (''timestamp'',''rowversion'')
        AND ty.name NOT LIKE ''%xml%'';';

    INSERT INTO @Cols (SchemaName,TableName,ColumnName,DataType,MaxLen,Prec,Scale)
        EXEC sys.sp_executesql @MetaSql;

    IF @Verbose = 1
        SELECT * FROM @Cols ORDER BY SchemaName,TableName,ColumnName;

    IF NOT EXISTS (SELECT 1 FROM @Cols)
    BEGIN
        PRINT 'No columns matched mask criteria.';
        RETURN;
    END;

    DECLARE
          @Schema sysname
        , @Table  sysname
        , @Col    sysname
        , @Type   sysname
        , @MaxLen int
        , @Prec   tinyint
        , @Scale  tinyint
        , @Expr   nvarchar(MAX)
        , @SQL    nvarchar(MAX)
        , @CharLen int
        , @LenSpec varchar(20)
        , @Where  nvarchar(400)
        , @SaltLiteral nvarchar(200);

    SET @SaltLiteral = REPLACE(@Salt,'''','''''');

    DECLARE cur CURSOR FAST_FORWARD FOR
        SELECT SchemaName, TableName, ColumnName, DataType, MaxLen, Prec, Scale
        FROM @Cols
        ORDER BY SchemaName, TableName, ColumnName;

    OPEN cur;
    FETCH NEXT FROM cur INTO @Schema,@Table,@Col,@Type,@MaxLen,@Prec,@Scale;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        SET @Expr = NULL;

        SET @CharLen = CASE WHEN @MaxLen = -1 THEN -1
                            WHEN @Type LIKE 'n%' THEN @MaxLen/2
                            ELSE @MaxLen END;
        SET @LenSpec = CASE WHEN @CharLen = -1 THEN 'MAX' ELSE CAST(@CharLen AS varchar(10)) END;

        -- EMAIL
        IF LOWER(@Col) LIKE '%email%' AND @Type IN ('char','nchar','varchar','nvarchar')
        BEGIN
            SET @Expr = N'(
  SELECT CASE
     WHEN ' + QUOTENAME(@Col) + N' LIKE ''%@%'' AND CHARINDEX(''@'',' + QUOTENAME(@Col) + N') > 1 THEN
        (
           SELECT (
             SELECT CASE
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[0-9]'' THEN CHAR(48 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''D'')))) % 10))
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[A-Z]'' THEN CHAR(65 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''U'')))) % 26))
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[a-z]'' THEN CHAR(97 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''L'')))) % 26))
                ELSE SUBSTRING(' + QUOTENAME(@Col) + N',N,1)
             END
             FROM (SELECT TOP (CHARINDEX(''@'',' + QUOTENAME(@Col) + N') - 1)
                       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
                   FROM ' + QUOTENAME(@DbName) + N'.sys.all_objects) AS pat
             FOR XML PATH(''''),TYPE
           ).value(''.'',''nvarchar(max)'')
           + SUBSTRING(' + QUOTENAME(@Col) + N', CHARINDEX(''@'',' + QUOTENAME(@Col) + N'), LEN(' + QUOTENAME(@Col) + N'))
        )
     ELSE
        (
           SELECT (
             SELECT CASE
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[0-9]'' THEN CHAR(48 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''D'')))) % 10))
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[A-Z]'' THEN CHAR(65 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''U'')))) % 26))
                WHEN SUBSTRING(' + QUOTENAME(@Col) + N',N,1) LIKE ''[a-z]'' THEN CHAR(97 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'', CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''L'')))) % 26))
                ELSE SUBSTRING(' + QUOTENAME(@Col) + N',N,1)
             END
             FROM (SELECT TOP (LEN(' + QUOTENAME(@Col) + N'))
                       ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
                   FROM ' + QUOTENAME(@DbName) + N'.sys.all_objects) AS pat
             FOR XML PATH(''''),TYPE
           ).value(''.'',''nvarchar(max)'')
        )
  END)';
            IF @Type IN ('char','varchar') AND @CharLen <> -1
                SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
            ELSE IF @Type IN ('nchar','nvarchar') AND @CharLen <> -1
                SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
            ELSE
                SET @Expr = N'CAST('+@Expr+' AS '+@Type+'(MAX))';
        END
        -- AGE numeric
        ELSE IF LOWER(@Col) LIKE '%age%' AND @Type IN ('tinyint','smallint','int','bigint','decimal','numeric','float','real')
        BEGIN
            DECLARE @BaseHash nvarchar(4000)=
                N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''AGE''))))';

            IF @Type IN ('tinyint','smallint','int','bigint')
                SET @Expr = N'CAST(CASE WHEN '+QUOTENAME(@Col)+' IS NULL THEN NULL ELSE
                       CASE WHEN '+QUOTENAME(@Col)+'<10 THEN ('+@BaseHash+'%10)
                            WHEN '+QUOTENAME(@Col)+'<100 THEN 10+('+@BaseHash+'%90)
                            ELSE ('+@BaseHash+'%121) END
                     END AS '+@Type+')';
            ELSE IF @Type IN ('decimal','numeric')
                SET @Expr = N'CAST(CASE WHEN '+QUOTENAME(@Col)+' IS NULL THEN NULL ELSE
                       CASE WHEN '+QUOTENAME(@Col)+'<10 THEN ('+@BaseHash+'%10)
                            WHEN '+QUOTENAME(@Col)+'<100 THEN 10+('+@BaseHash+'%90)
                            ELSE ('+@BaseHash+'%121) END
                     END AS '+@Type+'('+CAST(@Prec AS varchar(3))+','+CAST(@Scale AS varchar(3))+'))';
            ELSE
                SET @Expr = N'CAST(CASE WHEN '+QUOTENAME(@Col)+' IS NULL THEN NULL ELSE
                       CONVERT(int,CASE WHEN '+QUOTENAME(@Col)+'<10 THEN ('+@BaseHash+'%10)
                            WHEN '+QUOTENAME(@Col)+'<100 THEN 10+('+@BaseHash+'%90)
                            ELSE ('+@BaseHash+'%121) END)
                     END AS '+@Type+')';
        END
        -- AGE char
        ELSE IF LOWER(@Col) LIKE '%age%' AND @Type IN ('char','nchar','varchar','nvarchar')
        BEGIN
            SET @Expr = N'(
   SELECT (
      SELECT CHAR(48 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'',
               CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+
               ''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''AGED'')))) % 10))
      FROM (SELECT TOP (LEN('+QUOTENAME(@Col)+N'))
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
            FROM '+QUOTENAME(@DbName)+N'.sys.all_objects) d
      FOR XML PATH(''''),TYPE
   ).value(''.'',''nvarchar(max)'')
)';
            IF @Type IN ('char','varchar') AND @CharLen <> -1
                SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
            ELSE IF @Type IN ('nchar','nvarchar') AND @CharLen <> -1
                SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
            ELSE
                SET @Expr = N'CAST('+@Expr+' AS '+@Type+'(MAX))';
        END
        -- GENERIC
        ELSE IF (
             LOWER(@Col) LIKE '%ssn%' OR LOWER(@Col) LIKE '%dob%' OR LOWER(@Col) LIKE '%tin%' OR
             LOWER(@Col) LIKE '%fax%' OR LOWER(@Col) LIKE '%taxid%' OR LOWER(@Col) LIKE '%npi%' OR
             LOWER(@Col) LIKE '%name%' OR LOWER(@Col) LIKE '%phone%' OR LOWER(@Col) LIKE '%zip%' OR
             LOWER(@Col) LIKE '%state%' OR LOWER(@Col) LIKE '%city%' OR LOWER(@Col) LIKE '%contact%' OR
             LOWER(@Col) LIKE '%address%' OR LOWER(@Col) LIKE '%date%' OR LOWER(@Col) LIKE '%license%' OR
             LOWER(@Col) LIKE '%patient%'
        )
        BEGIN
            IF @Type IN ('char','nchar','varchar','nvarchar')
            BEGIN
                SET @Expr = N'(
   SELECT (
      SELECT CASE
         WHEN SUBSTRING('+QUOTENAME(@Col)+N',N,1) LIKE ''[0-9]''
             THEN CHAR(48 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''D'')))) % 10))
         WHEN SUBSTRING('+QUOTENAME(@Col)+N',N,1) LIKE ''[A-Z]''
             THEN CHAR(65 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''U'')))) % 26))
         WHEN SUBSTRING('+QUOTENAME(@Col)+N',N,1) LIKE ''[a-z]''
             THEN CHAR(97 + (ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'','+QUOTENAME(@Col)+N','':'',N,'':'',''L'')))) % 26))
         ELSE SUBSTRING('+QUOTENAME(@Col)+N',N,1)
      END
      FROM (SELECT TOP (LEN('+QUOTENAME(@Col)+N'))
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS N
            FROM '+QUOTENAME(@DbName)+N'.sys.all_objects) AS pat
      FOR XML PATH(''''),TYPE
   ).value(''.'',''nvarchar(max)'')
)';
                IF @Type IN ('char','varchar') AND @CharLen <> -1
                    SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
                ELSE IF @Type IN ('nchar','nvarchar') AND @CharLen <> -1
                    SET @Expr = N'LEFT(CAST('+@Expr+' AS '+@Type+'('+@LenSpec+')), LEN('+QUOTENAME(@Col)+'))';
                ELSE
                    SET @Expr = N'CAST('+@Expr+' AS '+@Type+'(MAX))';
            END
            ELSE IF @Type='tinyint'
                SET @Expr = N'CAST(ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''TINY'')))) % 256 AS tinyint)';
            ELSE IF @Type='smallint'
                SET @Expr = N'CAST(ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''SMALL'')))) % 32768 AS smallint)';
            ELSE IF @Type='int'
                SET @Expr = N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''INT'')))) % 2147483647';
            ELSE IF @Type='bigint'
                SET @Expr = N'ABS(CAST(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''BIG''))) AS bigint))';
            ELSE IF @Type IN ('decimal','numeric')
            BEGIN
                DECLARE @IntMax int = @Prec - @Scale;
                -- Build integer + fraction with preserved digit count
                SET @Expr =
                N'CAST((
    -- integer digit count original (>=1)
    CASE WHEN ' + QUOTENAME(@Col) + N' IS NULL THEN NULL ELSE
      (
        -- capped digit count
        CASE
          WHEN (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) > ' + CAST(@IntMax AS varchar(10)) + N'
               THEN ' + CAST(@IntMax AS varchar(10)) + N'
          ELSE (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END)
        END
      ) END) AS int)'; -- temp (not used)
                -- Final expression:
                SET @Expr =
N'CAST(
  CASE WHEN ' + QUOTENAME(@Col) + N' IS NULL THEN NULL ELSE
    (
      -- derive capped digit count (D)
      (CASE
         WHEN (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) > ' + CAST(@IntMax AS varchar(10)) + N'
              THEN ' + CAST(@IntMax AS varchar(10)) + N'
         ELSE (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END)
       END) AS DUMMY_IGNORE -- (inline use; no alias actually kept)
      +
      0 -- anchor
    )
  END AS decimal(38,' + CAST(@Scale AS varchar(3)) + N'))'; -- placeholder

                -- Real final (replace placeholder with full arithmetic)
                SET @Expr =
N'CAST(
  CASE WHEN ' + QUOTENAME(@Col) + N' IS NULL THEN NULL ELSE
    (
      -- digit count D
      CASE
         WHEN (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) > ' + CAST(@IntMax AS varchar(10)) + N'
              THEN ' + CAST(@IntMax AS varchar(10)) + N'
         ELSE (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END)
       END
      -- integer core
      AS BIGINT -- will be ignored; building below
    )
  END AS decimal(38,' + CAST(@Scale AS varchar(3)) + N'))'; -- placeholder again

                -- Build integerPart expression (repeat D inline)
                DECLARE @D nvarchar(1000) =
                  N'(CASE WHEN (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) > ' + CAST(@IntMax AS varchar(10)) +
                  N' THEN ' + CAST(@IntMax AS varchar(10)) +
                  N' ELSE (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) END)';

                DECLARE @BaseHashInt nvarchar(400) =
                  N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+
                  ''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''DECINT''))))';

                DECLARE @BaseHashFrac nvarchar(400) =
                  N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+
                  ''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''DECF''))))';

                DECLARE @IntegerPart nvarchar(MAX) =
                  N'CASE WHEN ' + @D + N' = 1 THEN (' + @BaseHashInt + N' % 10)
                        ELSE (POWER(CAST(10 AS decimal(38,0)),' + @D + N'-1)
                              + ((' + @BaseHashInt + N') % (POWER(CAST(10 AS decimal(38,0)),' + @D + N'-1)*9)))
                   END';

                DECLARE @FracPart nvarchar(MAX) =
                  CASE WHEN @Scale > 0 THEN
                    N' + ((' + @BaseHashFrac + N') % POWER(CAST(10 AS bigint),' + CAST(@Scale AS varchar(3)) + N')) / POWER(10.0,' + CAST(@Scale AS varchar(3)) + N')'
                  ELSE N''
                  END;

                SET @Expr =
N'CAST(
  CASE WHEN ' + QUOTENAME(@Col) + N' IS NULL THEN NULL ELSE
    ( (' + @IntegerPart + N')' + @FracPart + N')
      * CASE WHEN ' + QUOTENAME(@Col) + N' < 0 THEN -1 ELSE 1 END
  END
AS ' + @Type + '(' + CAST(@Prec AS varchar(3)) + ',' + CAST(@Scale AS varchar(3)) + '))';
            END
            ELSE IF @Type IN ('float','real')
            BEGIN
                -- float/real version (cap digits at 15)
                DECLARE @Dflt nvarchar(1000) =
                  N'(CASE WHEN (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) > 15
                        THEN 15
                        ELSE (CASE WHEN ABS(FLOOR(' + QUOTENAME(@Col) + N'))=0 THEN 1 ELSE LEN(CONVERT(varchar(38),ABS(FLOOR(' + QUOTENAME(@Col) + N')))) END) END)';

                DECLARE @HashInt nvarchar(400) =
                  N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+
                  ''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''FLTINT''))))';

                DECLARE @HashFrac nvarchar(400) =
                  N'ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+
                  ''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''FLTF''))))';

                DECLARE @IntPartF nvarchar(MAX) =
                  N'CASE WHEN ' + @Dflt + N' = 1 THEN (' + @HashInt + N' % 10)
                        ELSE (POWER(CAST(10 AS float),' + @Dflt + N'-1)
                              + ((' + @HashInt + N') % (POWER(CAST(10 AS float),' + @Dflt + N'-1)*9)))
                   END';

                SET @Expr =
N'CAST(
  CASE WHEN ' + QUOTENAME(@Col) + N' IS NULL THEN NULL ELSE
     ((' + @IntPartF + N') + ((' + @HashFrac + N') % 1000000) / 1000000.0)
       * CASE WHEN ' + QUOTENAME(@Col) + N' < 0 THEN -1 ELSE 1 END
  END
AS ' + @Type + N')';
            END
            ELSE IF @Type='date'
                SET @Expr = N'DATEADD(DAY, ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''DATE'')))) % 365, ''2000-01-01'')';
            ELSE IF @Type IN ('datetime','smalldatetime','datetime2')
                SET @Expr = N'DATEADD(MINUTE, ABS(CHECKSUM(HASHBYTES(''SHA2_256'',CONCAT('''+@SaltLiteral+''','':'','''+@Schema+'.'+@Table+'.'+@Col+''','':'',CAST('+QUOTENAME(@Col)+N' AS nvarchar(4000)), '':'',''DT'')))) % (365*24*60), ''2000-01-01'')';
        END

        IF @Expr IS NOT NULL
        BEGIN
            SET @Where = QUOTENAME(@Col) + N' IS NOT NULL';
            IF @Type IN ('char','nchar','varchar','nvarchar')
                SET @Where += N' AND LTRIM(RTRIM(' + QUOTENAME(@Col) + N')) <> ''''';

            SET @SQL = N'UPDATE ' + QUOTENAME(@DbName) + N'.' + QUOTENAME(@Schema) + N'.' + QUOTENAME(@Table)
                     + N' SET ' + QUOTENAME(@Col) + N' = ' + @Expr
                     + N' WHERE ' + @Where + N';';

            IF @CommitChanges = 0
                PRINT '-- ' + @Schema + '.' + @Table + '.' + @Col + CHAR(13) + @SQL;
            ELSE
                EXEC sys.sp_executesql @SQL;
        END;

        FETCH NEXT FROM cur INTO @Schema,@Table,@Col,@Type,@MaxLen,@Prec,@Scale;
    END;

    CLOSE cur;
    DEALLOCATE cur;

    IF @CommitChanges = 0
        PRINT 'Dry run only. Re-run with @CommitChanges = 1 to apply.';
END
GO


