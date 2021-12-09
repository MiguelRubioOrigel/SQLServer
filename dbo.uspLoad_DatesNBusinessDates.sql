USE TESARO_CDW
GO

DROP PROCEDURE IF EXISTS dbo.uspLoad_DatesNBusinessDates
GO

CREATE PROCEDURE dbo.uspLoad_DatesNBusinessDates
/***************************************************************************************************************************
Purpose: Create a table that has a mapping of every US business day for calculations
Inputs:
Author:  Miguel Rubio
Created: 20210630
Copyright:
Execution:				EXEC dbo.uspLoad_DatesNBusinessDates

Change History:		Date		User		Ticket		Action

				Source tables:
					SELECT * FROM TESARO_CDW.agd.tblTimePeriod

				Staging tables:

				Destination tables:
					SELECT * FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays WHERE Holiday=1 ORDER BY DateValue DESC

***************************************************************************************************************************/
AS
BEGIN
	SET NOCOUNT ON 
	-----------
	--Logging
	-----------	
	INSERT INTO AGD.tblMdParentStoreProcedure
	SELECT @@PROCID, (SELECT AGD.udfGetStoreProcedure(@@PROCID))
	
	DECLARE @tblDataRunLog AS AGD.typDataRunLog
	INSERT INTO @tblDataRunLog  
	SELECT * FROM AGD.udfGetDataRunLogTable (2, @@PROCID,null) ---The 2 is the TPSExecProcesTypeId which represents logging for store procedure
	
	--------
	--Place code in between Code Start and Code End
	--------
	------
	--Code Start
	--------
	BEGIN TRY

	DROP TABLE IF EXISTS TESARO_CDW.dbo.tblTimePeriod_BusinessDays;

	--Initialize table
	SELECT 0 AS Holiday, CASE WHEN DayOfWeekDesc IN ('Saturday','Sunday') THEN 0 ELSE 1 END AS BusinessDay,CAST(NULL AS NVARCHAR(50)) AS HolidayName,*
		INTO TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	FROM TESARO_CDW.agd.tblTimePeriod

	--There are a total of 12 federal US holidays: 11 annually and 1 every four years:

	--New Year's Day (January 1st fixed)
	UPDATE TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'New Year''s Day'
	WHERE MONTH(DateValue) = 1 and DAY(DateValue) = 1

	--Martin Luther King's Day (Third Monday of January)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Martin Luther King''s Day'
	FROM 
	(
	SELECT DateValue, MonthDateValue, DayOfWeekDesc
	,(SELECT COUNT(*) FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays b 
			  WHERE a.MonthDateValue = b.MonthDateValue AND a.DayOfWeekDesc = b.DayOfWeekDesc AND a.DateValue > b.DateValue)+1 AS WeekdayOfMonth
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays a
	) ordinal_weekday
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON ordinal_weekday.DateValue = dates.DateValue
	WHERE ordinal_weekday.DayOfWeekDesc = 'Monday' AND ordinal_weekday.WeekdayOfMonth = 3 AND MONTH(ordinal_weekday.DateValue) = 1

	--Inauguration Day (January 20th of every 4 years - January 21st if the 20th falls on a Sunday - Y2021 had this holiday)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Inauguration Day'
	FROM 
	(SELECT CASE WHEN MONTH(DateValue) = 1 AND DAY(DateValue) = 20 THEN 
				CASE WHEN DayOfWeekDesc = 'Sunday' THEN DATEADD(d,1,DateValue) ELSE DateValue END
			END AS InaugurationDay
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	WHERE YEAR(DateValue) % 4 = 1
	) base
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON base.InaugurationDay = dates.DateValue
	WHERE base.InaugurationDay IS NOT NULL

	--Presidents' Day (Third Monday of February)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Presidents'' Day'
	FROM 
	(
	SELECT DateValue, MonthDateValue, DayOfWeekDesc
	,(SELECT COUNT(*) FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays b 
			  WHERE a.MonthDateValue = b.MonthDateValue AND a.DayOfWeekDesc = b.DayOfWeekDesc AND a.DateValue > b.DateValue)+1 AS WeekdayOfMonth
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays a
	) ordinal_weekday
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON ordinal_weekday.DateValue = dates.DateValue
	WHERE ordinal_weekday.DayOfWeekDesc = 'Monday' AND ordinal_weekday.WeekdayOfMonth = 3 AND MONTH(ordinal_weekday.DateValue) = 2

	--Memorial Day (Last Monday of May)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Memorial Day'
	FROM 
	(SELECT CASE WHEN DayOfWeekDesc = 'Monday' AND MONTH(DATEADD(d,7,DateValue)) = 6 AND MONTH(DateValue) = 5 
			THEN DateValue END AS MemorialDay
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	) base
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON base.MemorialDay = dates.DateValue
	WHERE base.MemorialDay IS NOT NULL

	--Emancipation Day (June 19th - June 20th if the 19th falls on a Sunday - starting on 2021)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Emancipation Day'
	FROM 
	(SELECT CASE WHEN MONTH(DateValue) = 6 AND DAY(DateValue) = 19 THEN 
				CASE WHEN DayOfWeekDesc = 'Sunday' THEN DATEADD(d,1,DateValue) ELSE DateValue END
			END AS EmancipationDay
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	WHERE YEAR(DateValue) >= 2021
	) base
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON base.EmancipationDay = dates.DateValue
	WHERE base.EmancipationDay IS NOT NULL

	--Independence Day (July 4th)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Independence Day'
	FROM 
	(SELECT CASE WHEN MONTH(DateValue) = 7 AND DAY(DateValue) = 4 THEN 
				CASE WHEN DayOfWeekDesc = 'Sunday' THEN DATEADD(d,1,DateValue) ELSE DateValue END
			END AS IndependenceDay
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	) base
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON base.IndependenceDay = dates.DateValue
	WHERE base.IndependenceDay IS NOT NULL

	--Labor Day (First Monday of September)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Labor Day'
	FROM 
	(
	SELECT DateValue, MonthDateValue, DayOfWeekDesc
	,(SELECT COUNT(*) FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays b 
			  WHERE a.MonthDateValue = b.MonthDateValue AND a.DayOfWeekDesc = b.DayOfWeekDesc AND a.DateValue > b.DateValue)+1 AS WeekdayOfMonth
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays a
	) ordinal_weekday
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON ordinal_weekday.DateValue = dates.DateValue
	WHERE ordinal_weekday.DayOfWeekDesc = 'Monday' AND ordinal_weekday.WeekdayOfMonth = 1 AND MONTH(ordinal_weekday.DateValue) = 9

	--Columbus Day (Second Monday of October)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Columbus Day'
	FROM 
	(
	SELECT DateValue, MonthDateValue, DayOfWeekDesc
	,(SELECT COUNT(*) FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays b 
			  WHERE a.MonthDateValue = b.MonthDateValue AND a.DayOfWeekDesc = b.DayOfWeekDesc AND a.DateValue > b.DateValue)+1 AS WeekdayOfMonth
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays a
	) ordinal_weekday
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON ordinal_weekday.DateValue = dates.DateValue
	WHERE ordinal_weekday.DayOfWeekDesc = 'Monday' AND ordinal_weekday.WeekdayOfMonth = 2 AND MONTH(ordinal_weekday.DateValue) = 10

	--Veterans Day (November 11th, or the previous Friday if it falls on a Saturday, or the following Monday if it falls on a Sunday)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Veterans Day'
	FROM 
	(SELECT CASE WHEN MONTH(DateValue) = 11 AND DAY(DateValue) = 11 THEN 
				CASE WHEN DayOfWeekDesc = 'Saturday' THEN DATEADD(d,-1,DateValue) 
				WHEN DayOfWeekDesc = 'Sunday' THEN DATEADD(d,1,DateValue)
				ELSE DateValue END
			END AS EmancipationDay
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	) base
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON base.EmancipationDay = dates.DateValue
	WHERE base.EmancipationDay IS NOT NULL

	--Thanksgiving Day (Fourth Thursday of November)
	UPDATE dates
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Thanksgiving Day'
	FROM 
	(
	SELECT DateValue, MonthDateValue, DayOfWeekDesc
	,(SELECT COUNT(*) FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays b 
			  WHERE a.MonthDateValue = b.MonthDateValue AND a.DayOfWeekDesc = b.DayOfWeekDesc AND a.DateValue > b.DateValue)+1 AS WeekdayOfMonth
	FROM TESARO_CDW.dbo.tblTimePeriod_BusinessDays a
	) ordinal_weekday
	INNER JOIN TESARO_CDW.dbo.tblTimePeriod_BusinessDays dates
		ON ordinal_weekday.DateValue = dates.DateValue
	WHERE ordinal_weekday.DayOfWeekDesc = 'Thursday' AND ordinal_weekday.WeekdayOfMonth = 4 AND MONTH(ordinal_weekday.DateValue) = 11

	--Christmas Day (December 25th fixed)
	UPDATE TESARO_CDW.dbo.tblTimePeriod_BusinessDays
	SET Holiday = 1, BusinessDay = 0, HolidayName = 'Christmas Day'
	WHERE MONTH(DateValue) = 12 and DAY(DateValue) = 25
	
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
		SET ErrorMessage=ERROR_MESSAGE() 
		  , ErrorNumber =ERROR_NUMBER()	
	END CATCH

	----------
	--Log
	----------					   		   	
	EXEC AGD.uspInsertDataRunLog  @tblDataRunLog, 1 -----AGD.uspInsertDataRunLog will raise error if there was an error
END