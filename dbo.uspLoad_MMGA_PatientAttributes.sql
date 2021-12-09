USE TESARO_CDW
GO

DROP PROCEDURE IF EXISTS dbo.uspLoad_MMGA_PatientAttributes
GO

CREATE PROCEDURE dbo.uspLoad_MMGA_PatientAttributes
/**********************************************************************************************************
Purpose:			Populate dimension Component for new MMGA Scorecard
Inputs:		
Author:				Miguel Rubio
Created:			07/12/2021
Copyright:	
Execution:			EXEC dbo.uspLoad_MMGA_PatientAttributes

History:			Date				Name				Comment


Helpful Selects:

					---- Source Tables:
						SELECT TOP 1000 * FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
					
					---- Staging Tables:

					---- Reporting Tables:
						SELECT * FROM TESARO_CDW.dbo.tblDim_PatientAttributesMMGA
						SELECT * FROM TESARO_ADHOC.dbo.tblShyft_Dim_PatientAttributesMMGA
						SELECT * FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA
						SELECT * FROM TESARO_ADHOC.dbo.tblShyft_Dim_PatientTherapyStintsMMGA

*******************************************************************************************/
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
	BEGIN TRY

	DECLARE @DataDate DATE = (SELECT agd.udfGetSetting('DataDate'))

	DROP TABLE IF EXISTS TESARO_CDW.dbo.tblDim_PatientAttributesMMGA
	DROP TABLE IF EXISTS TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA
	DROP TABLE IF EXISTS #MMGA_base
	DROP TABLE IF EXISTS #MMGA_shipped
	DROP TABLE IF EXISTS #MMGA_approved
	DROP TABLE IF EXISTS #MMGA_cancelled
	DROP TABLE IF EXISTS #MMGA_transferred
	DROP TABLE IF EXISTS #MMGA_denied
	DROP TABLE IF EXISTS #MMGA_appealed
	DROP TABLE IF EXISTS #MMGA_pending
	DROP TABLE IF EXISTS #MMGA_incompleteReferral_pre
	DROP TABLE IF EXISTS #MMGA_incompleteReferral
	DROP TABLE IF EXISTS #MMGA_discontinued
	DROP TABLE IF EXISTS #MMGA_patient_shipments
	DROP TABLE IF EXISTS #MMGA_patient_discontinuations
	DROP TABLE IF EXISTS #MMGA_patient_transferred

	SELECT SPID = CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END
	,SPName = CASE	WHEN SPID='BLG' THEN 'Biologics'
					WHEN SPID='USB' THEN 'US Bioservices'
					WHEN SPID='DIP' THEN 'Optum/Diplomat'
					WHEN SPID='MSP' THEN 'McKesson'
					WHEN SPID='TWT' THEN 'Together With Tesaro'
					WHEN SPID='AVL' THEN 'Avella'
					WHEN SPID='OPT' THEN 'Optum/Diplomat'
					WHEN SPID='CVS' THEN 'CVS'
					ELSE 'Other'
			  END
	,CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
				,MIN(ReferralStartDate) AS ReferralDate
		INTO #MMGA_base
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE ReferralStartDate IS NOT NULL
	GROUP BY CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END
			,CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END
			,CASE	WHEN SPID='BLG' THEN 'Biologics'
					WHEN SPID='USB' THEN 'US Bioservices'
					WHEN SPID='DIP' THEN 'Optum/Diplomat'
					WHEN SPID='MSP' THEN 'McKesson'
					WHEN SPID='TWT' THEN 'Together With Tesaro'
					WHEN SPID='AVL' THEN 'Avella'
					WHEN SPID='OPT' THEN 'Optum/Diplomat'
					WHEN SPID='CVS' THEN 'CVS'
					ELSE 'Other'
			  END

	--select * from #MMGA_base

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS SPID
			,MIN(StatusStartDate) AS FirstShipDate
		INTO #MMGA_shipped
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus='Shipped' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END
			,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_shipped

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstApprovalDate
		INTO #MMGA_approved
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Approved' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_approved

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstCancelledDate
		INTO #MMGA_cancelled
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Cancelled' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_cancelled

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstTransferredDate
		INTO #MMGA_transferred
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Transferred'
	AND ReferralStartDate IS NOT NULL
	AND SPID <> 'TwT'
	AND StatusArchiveId NOT IN (SELECT StatusArchiveId FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive WHERE SPID = 'DIP' AND ISNULL(ImplicitTransferToSPID,'') = 'OPT')
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_transferred

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstDeniedDate
		INTO #MMGA_denied
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Denied' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_denied

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstPendingDate
		INTO #MMGA_pending
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Pending' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_pending

	SELECT b.MMGA_ID
			,MIN(b.StatusStartDate) AS FirstAppealDate
		INTO #MMGA_appealed
	FROM (SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,StatusStartDate
			FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
			WHERE RxStatus = 'Denied' AND ReferralStartDate IS NOT NULL
		) a
	INNER JOIN (SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,StatusStartDate
			FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
			WHERE RxStatus = 'Pending' AND RxSubStatus = 'Appeal' AND ReferralStartDate IS NOT NULL
		) b
		ON a.MMGA_ID = b.MMGA_ID
			AND a.StatusStartDate <= b.StatusStartDate
	GROUP BY b.MMGA_ID

	--select * from #MMGA_pending

	SELECT 
	CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
	,StatusStartDate
	,RxStatus
	,RxSubStatus
	,RN = ROW_NUMBER() OVER (PARTITION BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END 
							 ORDER BY StatusStartDate)
		INTO #MMGA_incompleteReferral_pre
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RecordType = 'Status' AND ReferralStartDate IS NOT NULL

	--select * from #MMGA_incompleteReferral_pre

	SELECT MMGA_ID, StatusStartDate AS IncompleteReferralStartDate, @DataDate AS IncompleteReferralEndDate
		INTO #MMGA_incompleteReferral
	FROM #MMGA_incompleteReferral_pre base
	WHERE RN = 1
		AND RxStatus = 'Pending'
		AND RxSubStatus IN ('Patient Contact','Physician Contact','Missing Information')

	UPDATE referral
	SET IncompleteReferralEndDate = pre.IncompleteReferralEndDate
	FROM #MMGA_incompleteReferral referral
	INNER JOIN (SELECT MMGA_ID, MIN(StatusStartDate) AS IncompleteReferralEndDate
				FROM #MMGA_incompleteReferral_pre
				WHERE RN > 1
					AND RxSubStatus NOT IN ('Patient Contact','Physician Contact','Missing Information')
				GROUP BY MMGA_ID
				) pre
		ON referral.MMGA_ID = pre.MMGA_ID

	--select * from #MMGA_incompleteReferral

	SELECT CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS MMGA_ID
			,MIN(StatusStartDate) AS FirstDiscontinuedDate
		INTO #MMGA_discontinued
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Discontinued' AND ReferralStartDate IS NOT NULL
	GROUP BY CAST(InternalPatientId AS VARCHAR) + '_' + REPLACE(CONVERT(VARCHAR,ReferralStartDate,111),'/','') + '_' + CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END

	--select * from #MMGA_discontinued

	SELECT   MMGA_ID						= base.MMGA_ID
			,SPID							= base.SPID
			,SPName							= base.SPName
			,ReferralDate					= base.ReferralDate
			,AnchorMonth					= DATEFROMPARTS(YEAR(base.ReferralDate),MONTH(base.ReferralDate),1)
			,isShipped						= CASE WHEN shipped.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstShipDate					= shipped.FirstShipDate
			,ShipAnchorMonth				= DATEFROMPARTS(YEAR(shipped.FirstShipDate),MONTH(shipped.FirstShipDate),1)
			,isApproved						= CASE WHEN approved.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstApprovalDate				= approved.FirstApprovalDate
			,isCancelled					= CASE WHEN cancelled.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstCancelledDate				= cancelled.FirstCancelledDate
			,CancellationAnchorMonth		= DATEFROMPARTS(YEAR(cancelled.FirstCancelledDate),MONTH(cancelled.FirstCancelledDate),1)
			,isTransferred					= CASE WHEN transferred.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstTransferDate				= transferred.FirstTransferredDate
			,isDenied						= CASE WHEN denied.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstDeniedDate				= denied.FirstDeniedDate
			,DenialAnchorMonth				= DATEFROMPARTS(YEAR(denied.FirstDeniedDate),MONTH(denied.FirstDeniedDate),1)
			,isAppealed						= CASE WHEN appealed.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstAppealDate				= appealed.FirstAppealDate
			,isPending						= CASE WHEN pending.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstPendingDate				= pending.FirstPendingDate
			,isIncompleteReferral			= CASE WHEN referral.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,IncompleteReferralStartDate	= referral.IncompleteReferralStartDate
			,IncompleteReferralEndDate		= CASE	WHEN referral.IncompleteReferralEndDate = @DataDate
													THEN NULL 
											  ELSE	referral.IncompleteReferralEndDate 
											  END
			,IncompleteReferralDays			= CASE	WHEN referral.IncompleteReferralEndDate = @DataDate AND DATEDIFF(dd,referral.IncompleteReferralStartDate,@DataDate) > 90
													THEN NULL 
											  ELSE
													CASE WHEN DATEDIFF(dd,referral.IncompleteReferralStartDate,referral.IncompleteReferralEndDate) < 0 THEN 0 
													ELSE 
															DATEDIFF(dd,referral.IncompleteReferralStartDate,referral.IncompleteReferralEndDate) + 1
													END
											  END
			,isDiscontinued					= CASE WHEN discontinued.MMGA_ID IS NOT NULL THEN 1 ELSE 0 END
			,FirstDiscontinuedDate			= discontinued.FirstDiscontinuedDate
			,DiscontinuedAnchorMonth		= DATEFROMPARTS(YEAR(discontinued.FirstDiscontinuedDate),MONTH(discontinued.FirstDiscontinuedDate),1)

		INTO TESARO_CDW.dbo.tblDim_PatientAttributesMMGA

	FROM #MMGA_base base
	LEFT JOIN #MMGA_shipped shipped
		ON base.MMGA_ID = shipped.MMGA_ID
	LEFT JOIN #MMGA_approved approved
		ON base.MMGA_ID = approved.MMGA_ID
	LEFT JOIN #MMGA_cancelled cancelled
		ON base.MMGA_ID = cancelled.MMGA_ID
	LEFT JOIN #MMGA_transferred transferred
		ON base.MMGA_ID = transferred.MMGA_ID
	LEFT JOIN #MMGA_denied denied
		ON base.MMGA_ID = denied.MMGA_ID
	LEFT JOIN #MMGA_appealed appealed
		ON base.MMGA_ID = appealed.MMGA_ID
	LEFT JOIN #MMGA_pending pending
		ON base.MMGA_ID = pending.MMGA_ID
	LEFT JOIN #MMGA_incompleteReferral referral
		ON base.MMGA_ID = referral.MMGA_ID
	LEFT JOIN #MMGA_discontinued discontinued
		ON base.MMGA_ID = discontinued.MMGA_ID

	--STARTS THE CONSTRUCTION OF SECOND MMGA TABLE FOR DURATION OF THERAPY CHART

	SELECT DISTINCT
			 InternalPatientId AS PatientID
			,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS SPID
			,StatusStartDate AS ShipmentDate
			,RxStatus
		INTO #MMGA_patient_shipments
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus='Shipped'
	--this condition is to avoid infinite loops in the case we have shipments after the current DataDate, as this table is used below to control a while loop
		AND StatusStartDate <= @DataDate

	--select * from #MMGA_patient_shipments

	SELECT DISTINCT
			 InternalPatientId AS PatientID
			,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS SPID
			,StatusStartDate AS DiscontinuationDate
			,RxStatus
		INTO #MMGA_patient_discontinuations
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus='Discontinued'

	--select * from #MMGA_patient_discontinuations

	SELECT DISTINCT InternalPatientId AS PatientID
		INTO #MMGA_patient_transferred
	FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
	WHERE RxStatus = 'Transferred'
	AND SPID <> 'TwT'
	AND StatusArchiveId NOT IN (SELECT StatusArchiveId FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive WHERE SPID = 'DIP' AND ISNULL(ImplicitTransferToSPID,'') = 'OPT')

	--select * from #MMGA_patient_transferred

	SELECT   PatientID
			,SPID
			,1 AS Stint
			,MIN(ShipmentDate) AS StartDate
			,CAST(NULL AS DATE) AS EndDate
			,0 AS isLatestStint
			,0 AS OnTherapyDays
			,DATEFROMPARTS(YEAR(MIN(ShipmentDate)),MONTH(MIN(ShipmentDate)),1) AS AnchorMonth
			,0 AS isTransferred
			,0 AS NumberOfShipments
			,CAST(NULL AS DATE) AS RetentionRateAnchorMonth

		INTO TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA
	FROM #MMGA_patient_shipments
	GROUP BY PatientID, SPID

	UPDATE mmga
	SET EndDate = discontinuations.FirstDiscontinuedDate
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA mmga
	INNER JOIN 
	(SELECT discontinued.PatientID, discontinued.SPID, MIN(discontinued.DiscontinuationDate) AS FirstDiscontinuedDate
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
	INNER JOIN #MMGA_patient_discontinuations discontinued
		ON therapy.PatientID = discontinued.PatientID
			AND therapy.SPID = discontinued.SPID
			AND therapy.StartDate <= discontinued.DiscontinuationDate
	GROUP BY discontinued.PatientID, discontinued.SPID
	) discontinuations
		ON mmga.PatientID = discontinuations.PatientID
			AND mmga.SPID = discontinuations.SPID

	--DECLARE @DataDate DATE = (SELECT agd.udfGetSetting('DataDate'))

	UPDATE therapy
	SET isLatestStint = 1, EndDate = @DataDate
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
	WHERE EndDate IS NULL

	--select * from TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA

	--DECLARE @DataDate DATE = (SELECT agd.udfGetSetting('DataDate'))
	DECLARE @i INT = 2

	WHILE EXISTS (select * from #MMGA_patient_shipments)
	BEGIN

		UPDATE shipments
		SET RxStatus = 'To be deleted'
		FROM #MMGA_patient_shipments shipments 
		INNER JOIN TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
		ON shipments.PatientID = therapy.PatientID
			AND shipments.SPID = therapy.SPID
			AND shipments.ShipmentDate <= therapy.EndDate

		DELETE FROM #MMGA_patient_shipments WHERE RxStatus = 'To be deleted'

		INSERT INTO TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA
		SELECT   PatientID
				,SPID
				,@i AS Stint
				,MIN(ShipmentDate) AS StartDate
				,CAST(NULL AS DATE) AS EndDate
				,0 AS isLatestStint
				,0 AS OnTherapyDays
				,DATEFROMPARTS(YEAR(MIN(ShipmentDate)),MONTH(MIN(ShipmentDate)),1) AS AnchorMonth
				,0 AS isTransferred
				,0 AS NumberOfShipments
				,CAST(NULL AS DATE) AS RetentionRateAnchorMonth

		FROM #MMGA_patient_shipments
		GROUP BY PatientID,SPID
		
		UPDATE therapy
		SET therapy.EndDate = step.EndDate
		FROM 
		(SELECT discontinued.PatientID,discontinued.SPID,MIN(discontinued.DiscontinuationDate) AS EndDate
		FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA stints
		INNER JOIN #MMGA_patient_discontinuations discontinued 
			ON stints.PatientID = discontinued.PatientID
				AND stints.SPID = discontinued.SPID
				AND stints.StartDate <= discontinued.DiscontinuationDate
		WHERE stints.Stint = @i
		GROUP BY discontinued.PatientID,discontinued.SPID) step 
			INNER JOIN TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
				ON therapy.PatientID = step.PatientID
					AND therapy.SPID = step.SPID
		WHERE therapy.Stint = @i

		UPDATE therapy
		SET isLatestStint = 1, EndDate = @DataDate
		FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
		WHERE EndDate IS NULL AND therapy.Stint = @i

		SET @i = @i + 1

	END

	UPDATE therapy
	SET isLatestStint = 1
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
	INNER JOIN
		(SELECT PatientID, MAX(Stint) AS LatestStint
		FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA
		WHERE PatientID NOT IN (SELECT PatientID FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA WHERE isLatestStint = 1)
		GROUP BY PatientID) latest
	ON therapy.PatientID = latest.PatientID AND therapy.Stint = latest.LatestStint

	UPDATE therapy
	SET OnTherapyDays = DATEDIFF(dd,StartDate,EndDate)+1
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy

	UPDATE therapy
	SET isTransferred = 1
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
	INNER JOIN #MMGA_patient_transferred transferred
		ON therapy.PatientID = transferred.PatientID

	UPDATE therapy
	SET NumberOfShipments = shipments.NumberOfShipments, RetentionRateAnchorMonth = shipments.RetentionRateAnchorMonth
	FROM TESARO_CDW.dbo.tblDim_PatientTherapyStintsMMGA therapy
	INNER JOIN (SELECT   InternalPatientId
						,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END AS SPID
						,COUNT(DISTINCT StatusArchiveId) AS NumberOfShipments
						,DATEFROMPARTS(YEAR(MIN(StatusStartDate)),MONTH(MIN(StatusStartDate)),1) AS RetentionRateAnchorMonth
				FROM TESARO_SPDI_INTEGRATOR.dbo.tblStgPatientStatusArchive
				WHERE RxStatus='Shipped' AND StatusStartDate <= @DataDate
				GROUP BY InternalPatientId,CASE WHEN SPID = 'DIP' THEN 'OPT' ELSE SPID END
				) shipments
		ON therapy.PatientID = shipments.InternalPatientId
			AND therapy.SPID = shipments.SPID

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
                + ' Line:' + CONVERT(VARCHAR,ERROR_LINE())
                + ' Error#:' + CONVERT(VARCHAR,ERROR_NUMBER())
                + ' Severity:' + CONVERT(VARCHAR,ERROR_SEVERITY())
                + ' State:' + CONVERT(VARCHAR,ERROR_STATE())
                + ' user:' + SUSER_NAME()
                + ' in proc:' + ISNULL(ERROR_PROCEDURE(),'N/A')
			 + CASE WHEN OBJECT_NAME(@@PROCID) <> ERROR_PROCEDURE() THEN '<--' + OBJECT_NAME(@@PROCID) ELSE '' END   -- will display error from sub stored procedures
		  , ErrorNumber =ERROR_NUMBER()

	END CATCH

	----------
	--Log
	----------					   		   	
	EXEC AGD.uspInsertDataRunLog  @tblDataRunLog, 1 -----AGD.uspInsertDataRunLog will raise error if there was an error
END