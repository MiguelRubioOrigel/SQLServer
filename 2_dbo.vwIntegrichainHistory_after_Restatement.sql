USE TESARO_CDW
GO

IF EXISTS (SELECT 1 FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = 'vwIntegrichainHistory_after_Restatement' AND TABLE_SCHEMA = 'dbo')
BEGIN
	DROP VIEW dbo.vwIntegrichainHistory_after_Restatement
END
GO

--SELECT isRestatement,* FROM dbo.vwIntegrichainHistory_after_Restatement where src_id=348265761

CREATE VIEW	dbo.vwIntegrichainHistory_after_Restatement
AS

WITH base AS (
SELECT 
 src_id
,HistoryID
,HASHBYTES('SHA2_256',UPPER(CONCAT(ISNULL([ShipFromDEAHINCustomerId],'')
				  ,ISNULL([ShipFromIdentifierType],'')
				  ,ISNULL([TradePartnerName],'')
				  ,ISNULL([ShipFromJuniorParentName],'')
				  ,ISNULL([DistributionCenterChannel],'')
				  ,ISNULL([DistributionCenterClassofTrade],'')
				  ,ISNULL([DistributionCenterIDIC],'')
				  ,ISNULL([DistributionCenterName],'')
				  ,ISNULL([DistributionCenterAddress],'')
				  ,ISNULL([DistributionCenterCity],'')
				  ,ISNULL([DistributionCenterState],'')
				  ,ISNULL([DistributionCenterZip],'')
				  ,ISNULL([ShipToDEAHINCustomerId],'')
				  ,ISNULL([ShipToIdentifierType],'')
				  ,ISNULL([ShipToSeniorParentName],'')
				  ,ISNULL([ShipToJuniorParentName],'')
				  ,ISNULL([ShipToPointofCareChannel],'')
				  ,ISNULL([ShipToPointofCareClassofTrade],'')
				  ,ISNULL([ShipToPOCId],'')
				  ,ISNULL([ShipToPOCName],'')
				  ,ISNULL([ShipToPOCAddress],'')
				  ,ISNULL([ShipToPOCCity],'')
				  ,ISNULL([ShipToPOCState],'')
				  ,ISNULL([ShipToPOCZip],'')
				  ,ISNULL([IsClinic],'')
				  ,ISNULL([BusinessUnit],'')
				  ,ISNULL([Brand],'')
				  ,ISNULL([Strength],'')
				  ,ISNULL([PackSize],'')
				  ,ISNULL([DosageDescription],'')
				  ,ISNULL([PackageDescription],'')
				  ,ISNULL([NDC],'')
				  ,ISNULL([TransactionType],'')
				  ,ISNULL([InvoiceNumber],'')
				  ,ISNULL([ContractNumber],'')
				  ,ISNULL([DayDate],'')
				  ,ISNULL([sum867QtySoldPU],'')
				  ,ISNULL([sum867QtySoldWACc],'')
				  ,ISNULL([sum867QtySoldWACh],'')))) AS HashValue
FROM TESARO_IM.dbo.tbldfIntegrichain_867sales_history
)   --SELECT * FROM base
,step1 AS (
SELECT Src_ID
	,HistoryID
	,HashValue
	,RN = DENSE_RANK() OVER(PARTITION BY Src_ID,HashValue ORDER BY HistoryID DESC)
	,(SELECT COUNT(DISTINCT HashValue) AS Records FROM base a WHERE a.Src_ID = base.Src_ID) AS Records
FROM base
)   --SELECT * FROM step1 where src_id=348265761
,step2 AS (
SELECT Src_ID,MAX(HistoryID) AS MaxHistoryID  --We want to know what was the latest version of the record that arrived for each src_id
FROM step1
WHERE RN = 1  --among all the different versions of the same src_id that might have arrived
GROUP BY Src_ID
)   --SELECT * FROM step2
,step3 AS (
SELECT step1.Src_ID,step1.HashValue
FROM step2 INNER JOIN step1 ON step2.MaxHistoryID = step1.HistoryID
)   --SELECT * FROM step3
,step4 AS (
SELECT step3.Src_ID,step3.HashValue,MIN(step1.HistoryID) as MinHistoryID  --We want to know the minimum date in which the latest record arrived to avoid sending it over and over in the email report
FROM step3 INNER JOIN step1 ON step3.HashValue = step1.HashValue
GROUP BY step3.Src_ID,step3.HashValue
)   --SELECT * FROM step4

,finalstep AS (
SELECT history.*, CASE WHEN step1.Records = 1 THEN 'N' ELSE 'Y' END AS isRestatement
FROM TESARO_IM.dbo.tbldfIntegrichain_867sales_history history INNER JOIN step4
	ON history.Src_ID = step4.Src_ID AND history.HistoryID = step4.MinHistoryID
INNER JOIN step1
	ON step1.Src_ID = step4.Src_ID AND step1.HistoryID = step4.MinHistoryID
)
,Override_Check AS (
SELECT H.*, isRestatement = 'N'
FROM TESARO_IM.dbo.tbldfIntegrichain_867sales_history H
INNER JOIN TESARO_IM.dbo.tbldfIntegrichainManualOverrides M on H.HistoryID=M.RefHistoryID AND H.src_id=M.Src_ID
)
SELECT * FROM finalstep
UNION
SELECT * FROM Override_Check
GO