USE [TESARO_CONTROLLER]
GO

/****** Object:  StoredProcedure [dbo].[uspExtractGSKBottleOnHandSold]    Script Date: 5/25/2021 3:02:04 PM ******/
DROP PROCEDURE if exists [dbo].[uspExtractGSKBottleOnHandSold]
GO

/****** Object:  StoredProcedure [dbo].[uspExtractGSKBottleOnHandSold]    Script Date: 5/25/2021 3:02:04 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO





CREATE PROCEDURE [dbo].[uspExtractGSKBottleOnHandSold]
/*******************************************************************************************
Purpose: EXtract the Bottle Sold or Bottle On Hand data
Inputs:
Author: Israel Rodriguez
Created: 2021.05.10
exec [dbo].[uspExtractGSKBottleOnHandSold]
Copyright:
Change History:  
2021 11 11 Lrafferty TES-8780
*******************************************************************************************/
AS
BEGIN
	SET NOCOUNT ON 
	-----------
	--Logging
	-----------	
	INSERT INTO AGD.tblMdParentStoreProcedure
	SELECT @@PROCID,(SELECT AGD.udfGetStoreProcedure(@@PROCID))
	DECLARE @tblDataRunLog AS AGD.typDataRunLog
	INSERT INTO @tblDataRunLog
	SELECT *
	FROM AGD.udfGetDataRunLogTable(2, @@PROCID, NULL) ---The 2 is the TPSExecProcesTypeId which represents logging for store procedure
	------
	--Code Start
	--------

	BEGIN TRY
	declare @const as decimal(3,2)=4.4
	declare @SpSection as nvarchar(30),@TotalSection as nvarchar(30),@concept as nvarchar(30)
	declare @vrSQL AS NVARCHAR(MAX), @vrmonths as varchar(max),@month as nvarchar(max),@aux as nvarchar(max),@global as nvarchar(40)
		
	--getting ready 	

		drop TABLE IF EXISTS ##BottlesSold
		drop TABLE IF EXISTS ##BottlesOnHand
		drop TABLE IF EXISTS ##WeekOnHand
		drop TABLE IF EXISTS ##BottlesReceived
		drop TABLE IF EXISTS ##TotalWeekOnHand
		drop table if exists #data		

	--#getmonths
		select *
		into #getmonths from (
						select distinct DATEFROMPARTS( datepart (yyyy,cast (InvFromDate as date)),datepart(m,cast (InvFromDate as date)),datepart (dd,cast (InvFromDate as date)))			as formatdate
						,FORMAT (CAST (InvFromDate AS DATE) ,'MMM-yy')	as formatreport
						from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory 
						where Invfromdate like concat ('%',year (GETDATE()),'%')
							or Invfromdate like concat ('%',year (dateadd(year,-1,getdate())),'%')
				) un

		declare months_string cursor 
		for select formatreport
			from #getmonths 
			order by formatdate

		open months_string
		fetch next from months_string into @vrmonths
		select @vrmonths = '['''+@vrmonths+']'
		while @@FETCH_STATUS =0
		begin
		fetch next from months_string into @month
		if charindex (@month,@vrmonths) = 0
		select @vrmonths = @vrmonths+',['''+@month+']'
		end
		close months_string
		deallocate months_string
		
--		PRINT @vrmonths
--
---- bottle Sold extract
--
	--if @Extract = 'BS' or @Extract = 'A'
	begin
		select @SpSection = '#bottlesbyshipdate'
		select @TotalSection = '#bottlesbydate'
		select @concept=N'Bottles Sold'
		select @global='##BottlesSold'
		
		--select * from #bobuild
		SELECT ShipFromID
				, InvFromDate 
				,[Bottles Sold]
		into #bobuild
			FROM (
					select  ShipFromID
							,concat('''',format (cast (Invfromdate as date),'MMM-yy') ) Invfromdate
							,sum(  cast (i.QtySold as int)) as 'Bottles Sold' 
					from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory i
					group by ShipFromID,InvFromDate
				) a
		order by ShipFromID,InvFromDate

				--select * from #bottlesformat
				select  ShipFromID
						,Invfromdate
						,[Bottles Sold]
				into #bottlesformat  
				from #bobuild

				--#bottlesbyshipdate
				select ShipFromID
						,Invfromdate
						,[Bottles Sold] Total
				into #bottlesbyshipdate 
				from #bottlesformat

				 --#bottlesbydate
				 --sumar por fecha
				 select 'Total' as lbl,Invfromdate
						,[Bottles Sold] 
						 into #bottlesbydate 
				 from #bottlesformat
 
				SELECT @vrSQL = 'select * 
							into '+ @global +'
						from (select ShipFromID as SP,'''+@concept+''' as Concept
							,'+@vrmonths+'
							from '+@SpSection+' pivot (sum (Total)
							for invfromdate 
							in ('+@vrmonths+')
						)as sectionsp
						union
						select lbl,'''+@concept+''' as Concept
						,'+@vrmonths+'
						from '+@TotalSection+' pivot ( sum (['+@concept+']) 
						for invfromdate in ('+@vrmonths+') 
						)as sectionTotal
				)t		
				order by '+@vrmonths+'
		
					'
				--PRINT(@vrSQL)
				EXEC(@vrSQL)
				drop table if exists #bobuild
				drop table if exists #bottlesformat
				drop table if exists #bottlesbyshipdate
				drop table if exists #orderedframe
				--drop table if exists ##BottlesSold
				

				select case when SP='Total' then 9999 else Row_number() OVER (order by (select 0))end  RNBS ,*
				into #tem1
				from ##BottlesSold bs
				
				drop table ##BottlesSold

				select * into ##BottlesSold
				from #tem1	
				order by RNBS

				drop table #tem1
				create clustered index ix_RNBS on ##BottlesSold([RNBS])
				--select * from ##BottlesSold

end
--
-- Bottle on Hand Extract
--	
--	
--if  @Extract = 'BOH' or @Extract = 'A'
begin
select @SpSection = '#byshipdate'
select @TotalSection = '#bydate'
select @concept=N'Bottles On Hand'
select @global='##BottlesOnHand'

--#bobuild
select ShipFromID
	  ,InvFromDate
	  ,[Bottles On Hand]
into #buildbo
 FROM (
			select  ShipFromID
					,concat('''',format (cast (Invfromdate as date),'MMM-yy') ) Invfromdate
					,sum(  cast (QtyOnHand as int)) as 'Bottles On Hand'
			from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory 
					group by ShipFromID,InvFromDate
				) b
		order by ShipFromID,InvFromDate

				--#bo
				select  ShipFromID
						,Invfromdate
						,[Bottles On Hand]
				into #format 
				from #buildbo

				--#bottlesbyshipdate
				--sumar por nombre y fecha
				select ShipFromID
						,Invfromdate
						,[Bottles On Hand] Total
				into #byshipdate 
				 from #format

				 --#bottlesbydate
				 select 'Total' as lbl,Invfromdate
						,[Bottles On Hand] 
						 into #bydate
				 from #format
 
						SELECT @vrSQL = 'select * 
										into '+@global+' 
								 from (select ShipFromID as SP,'''+@concept+''' as Concept
										,'+@vrmonths+'
										from '+@SpSection+' pivot (sum (Total)
										for invfromdate 
										in ('+@vrmonths+')
									)as sectionsp
									union
									select lbl,'''+@concept+''' as Concept
									,'+@vrmonths+'
									from '+@TotalSection+' pivot ( sum (['+@concept+']) 
								  for invfromdate in ('+@vrmonths+') 
								   )as sectionTotal
						)t		
						order by '+@vrmonths+'
		
								'
	--					PRINT(@vrSQL)
						EXEC(@vrSQL)
						drop table if exists #format
						drop table if exists #buildbo
						drop table if exists #byshipdate
						drop table if exists #bydate
						drop table if exists #monthframe
						
				select case when SP='Total' then 9999 else Row_number() OVER (order by (select 0))end  RNBOH ,*
				into #tem2
				from ##BottlesOnHand  boh

				drop table ##BottlesOnHand 

				select * into ##BottlesOnHand 
				from #tem2
				order by RNboh

				drop table #tem2
				create clustered index ix_RNBOH on ##BottlesOnHand ([RNBOH])
	end
--
-- Bottle Received
--	
--	
--if  @Extract = 'BR' or @Extract = 'A'
begin
select @SpSection = '#byshipdate_r'
select @TotalSection = '#bydate_r'
select @concept=N'Bottles Received'
select @global='##BottlesReceived'

--#bobuild
select ShipFromID
	  ,InvFromDate
	  ,[Bottles Received]
into #buildbo_r
 FROM (
			select  ShipFromID
					,concat('''',format (cast (Invfromdate as date),'MMM-yy') ) Invfromdate
					,sum(  cast (QtyReceived as int)) as 'Bottles Received'
			from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory 
					group by ShipFromID,InvFromDate
				) b
		order by ShipFromID,InvFromDate

				--#bo
				select  ShipFromID
						,Invfromdate
						,[Bottles Received]
				into #format_r  
				from #buildbo_r

				--#bottlesbyshipdate
				--sumar por nombre y fecha
				select ShipFromID
						,Invfromdate
						,[Bottles Received] Total
				into #byshipdate_r  
				 from #format_r

				 --select * from #bottlesformatt

				 --#bottlesbydate
				 --sumar por fecha
				 select 'Total' as lbl,Invfromdate
						,[Bottles Received] 
						 into #bydate_r 
				 from #format_r

						SELECT @vrSQL = 'select * 
										into '+@global+' 
								 from (select ShipFromID as SP,'''+@concept+''' as Concept
										,'+@vrmonths+'
										from '+@SpSection+' pivot (sum (Total)
										for invfromdate 
										in ('+@vrmonths+')
									)as sectionsp
									union
									select lbl,'''+@concept+''' as Concept
									,'+@vrmonths+'
									from '+@TotalSection+' pivot ( sum (['+@concept+']) 
								  for invfromdate in ('+@vrmonths+') 
								   )as sectionTotal
						)t		
						order by '+@vrmonths+'
		
								'
	--					PRINT(@vrSQL)
						EXEC(@vrSQL)
						drop table if exists #buildbo_r
						drop table if exists #format_r
						drop table if exists #byshipdate_r
						drop table if exists #bydate_r
						drop table if exists #monthframe_r
						

						select case when SP='Total' then 9999 else Row_number() OVER (order by (select 0))end  RNBR ,*
						into #tem3
						from ##BottlesReceived br

						drop table ##BottlesReceived
						select * into ##BottlesReceived
						from #tem3
						order by RNbr

						drop table #tem3
						create clustered index ix_RNBR on ##BottlesReceived([RNBR])
						--select * from ##BottlesReceived

	end
--
-- Week Hand Extract
--	
--	
--if  @Extract = 'WOH' or @Extract = 'A'
begin
select @SpSection = '#byshipdate_w'
select @TotalSection = '#bydate_w'
select @concept=N'Week On Hand'
select @global='##WeekOnHand'

--#bobuild
select ShipFromID
	  ,InvFromDate
	  ,[Week On Hand]
into #buildbo_w
 FROM (
			select  ShipFromID
					,concat('''',format (cast (Invfromdate as date),'MMM-yy') ) Invfromdate
					,(sum(cast (QtyOnHand as decimal) ) / nullif ( sum( cast (QtySold as decimal) ),0 ) ) * @const as 'Week On Hand'
			from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory 
					group by ShipFromID,InvFromDate
				) b
		order by ShipFromID,InvFromDate

				--#bo
				select  ShipFromID
						,Invfromdate
						,[Week On Hand]
				into #format_w
				from #buildbo_w

				--#bottles by ship date
				select ShipFromID
						,Invfromdate
						,[Week On Hand] Total
				into #byshipdate_w 
				 from #format_w

				 --select * from #bottlesformatt

				 --#bottlesbydate
				 --sumar por fecha
				 select 'Total' as lbl,Invfromdate
						,[Week On Hand] 
						 into #bydate_w
				 from #format_w
 
						SELECT @vrSQL = 'select * 
										into '+@global+' 
								 from (select ShipFromID as SP,'''+@concept+''' as Concept
										,'+@vrmonths+'
										from '+@SpSection+' pivot (sum (Total)
										for invfromdate 
										in ('+@vrmonths+')
									)as sectionsp
									union
									select lbl,'''+@concept+''' as Concept
									,'+@vrmonths+'
									from '+@TotalSection+' pivot ( sum (['+@concept+']) 
								  for invfromdate in ('+@vrmonths+') 
								   )as sectionTotal
						)t		
						order by '+@vrmonths+'
		
								'
	--					PRINT(@vrSQL)
						EXEC(@vrSQL)
						select case when SP='Total' then 9999 else Row_number() OVER (order by (select 0))end  RNWOH ,*
						into #tem4
						from ##WeekOnHand woh

						drop table ##WeekOnHand

						select * into ##WeekOnHand
						from #tem4
						order by RNwoh

						drop table #tem4
						create clustered index ix_RNWOH on ##WeekOnHand([RNWOH])
						--select * from ##WeekOnHand

	end
--
-- Total Week Hand Extract
--	
--	
--if  @Extract = 'TWOH' or @Extract = 'A' 
begin
select @SpSection = '#byshipdate_tw'
select @TotalSection = '#bydate_tw'
select @concept=N'Total Week On Hand'
select @global=N'##TotalWeekOnHand'

--#bobuild
select ShipFromID
	  ,InvFromDate
	  ,[Total Week On Hand]
into #buildbo_tw
 FROM (
		select  ShipFromID
			,concat('''',format (cast (Invfromdate as date),'MMM-yy') )Invfromdate
			,( sum( cast (QtyOnHand as decimal)) / nullif(sum( cast (QtySold as decimal)),0))*@const 'Total Week On Hand' 
		from TESARO_SPDI_INTEGRATOR.dbo.vwAdhoc_SPInventory 
			group by ShipFromID,InvFromDate
		) b
		order by ShipFromID,InvFromDate
				--#bo
				select  ShipFromID
						,Invfromdate
						,[Total Week On Hand]
				into #format_tw
				from #buildbo_tw

				--#bottlesbyshipdate
				--sumar por nombre y fecha
				select ShipFromID
						,Invfromdate
						,[Total Week On Hand] Total
				into #byshipdate_tw
				 from #format_tw

				 --#bottlesbydate
				 --sumar por fecha
				 select 'Total' as lbl,Invfromdate
						,[Total Week On Hand] 
						 into #bydate_tw
				 from #format_tw

						SELECT @vrSQL = 'select * 
										into '+@global+' from (select ShipFromID as SP,'''+@concept+''' as Concept
										,'+@vrmonths+'
										from '+@SpSection+' pivot (sum (Total)
										for invfromdate 
										in ('+@vrmonths+')
									)as sectionsp
									union
									select lbl,'''+@concept+''' as Concept
									,'+@vrmonths+'
									from '+@TotalSection+' pivot ( sum (['+@concept+']) 
								  for invfromdate in ('+@vrmonths+') 
								   )as sectionTotal
						)t		
						order by '+@vrmonths+'
					
								'
	--					PRINT(@vrSQL)
						EXEC(@vrSQL)

				select case when SP='Total' then 9999 else Row_number() OVER (order by (select 0))end  RNBtwoh ,*
				into #tem6
				from ##TotalWeekOnHand twoh
				
				drop table ##TotalWeekOnHand 

				select * into ##TotalWeekOnHand 
				from #tem6
				order by RNBtwoh

				drop table #tem6
				create clustered index ix_RNBtwoh on ##TotalWeekOnHand ([RNBtwoh])
				--select * from ##BottlesSold

	end
	
	--if @Extract = 'A' 
	begin
		select top 250000 * into #data 
		from (
		select * from ##BottlesReceived r
		union all
		select * from ##WeekOnHand w
		union all
		select * from ##BottlesSold s
		Union all
		select * from ##BottlesOnHand h
		union all
		select * from ##TotalWeekOnHand t
		)a
		


		drop table if exists Tesaro_controller.dbo.tblGSKBottleExtract
		select * into Tesaro_controller.dbo.tblGSKBottleExtract
		from #data d
		where ISNUMERIC(d.[SP]) = 0

		alter table Tesaro_controller.dbo.tblGSKBottleExtract drop column [RNBR]

	end

--Final Total Bottles correction

DECLARE @SQL nvarchar(1000)
 
declare @Column varchar(50)
declare @ColumnCount int = 1
WHILE @ColumnCount <= (SELECT COUNT(COLUMN_NAME) FROM INFORMATION_SCHEMA.COLUMNS WHERE 
														TABLE_CATALOG = 'TESARO_CONTROLLER' 
														AND TABLE_SCHEMA = 'dbo' 
														AND TABLE_NAME = 'tblGSKBottleExtract'
														AND COLUMN_NAME NOT IN ('Concept','SP'))
BEGIN
set @Column =  '['+ (SELECT  COLUMN_NAME  FROM INFORMATION_SCHEMA.COLUMNS WHERE 
														TABLE_CATALOG = 'TESARO_CONTROLLER' 
														AND TABLE_SCHEMA = 'dbo' 
														AND TABLE_NAME = 'tblGSKBottleExtract' 
														AND COLUMN_NAME NOT IN ('Concept','SP')
														AND ORDINAL_POSITION = @ColumnCount+2)
				 +']'

--PRINT @Column
SET @SQL = 'UPDATE Tesaro_controller.dbo.tblGSKBottleExtract
SET '+@Column+' = (
(SELECT  (SELECt SUM('+@Column+') from Tesaro_controller.dbo.tblGSKBottleExtract  WHERE Concept  = ''Bottles On Hand'' and SP <> ''Total'')  / (SELECT SUM('+@Column+') from Tesaro_controller.dbo.tblGSKBottleExtract  WHERE Concept = ''Bottles Sold'' and SP <> ''Total'')) * '+CAST(@const as varchar)+'
)
WHERE SP = ''Total'' AND Concept = ''Total Week On Hand'''
 
EXEC (@SQL)
print @SQL
SET @ColumnCount = @ColumnCount +1
END

	END TRY
	--------
	--Code End
	--------

	-----------
	--Logging
	-----------	
	BEGIN CATCH
		----------
		--Update table variable with error message
		----------					   
		UPDATE @tblDataRunLog
		SET ErrorMessage = ERROR_MESSAGE(),ErrorNumber = ERROR_NUMBER()
	END CATCH
	----------
	--Log
	----------					   		   	
	EXEC AGD.uspInsertDataRunLog @tblDataRunLog,1 -----AGD.uspInsertDataRunLog will raise error if there was an error
END



GO


