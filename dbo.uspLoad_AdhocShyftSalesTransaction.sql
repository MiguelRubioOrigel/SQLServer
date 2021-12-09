USE [TESARO_CDW]
GO

DROP PROCEDURE IF EXISTS dbo.uspLoad_AdhocShyftSalesTransaction
GO

CREATE PROCEDURE [dbo].[uspLoad_AdhocShyftSalesTransaction]
/*******************************************************************************************************************************************************
Purpose:	Load Base table for adhoc push to populate Shyft_SalesTransaction for GSK including the weekly DDD feed to be depracated in a few months
Inputs:		TESARO_CDW.AGD.tblStgSalesTransaction + Weekly DDD Feed for Zejula
													+ IQVIA DDD MD feed for Blenrep
													+ IQVIA DDD by Indication feed for Jemperli
Author:		Miguel Rubio
Created:	12/02/2020
History:			Date		User			Action
					12/10/2020	JKrendel		TES-6363: Include Blenrep DDD competitor data to TESARO_CDW.dbo.tblShyft_SalesTransaction
					01/18/2021	MRubio			TES-6374: Include Jemperli DDD by indication data to TESARO_CDW.dbo.tblShyft_SalesTransaction
					01/21/2021	MRubio			TES-6656: Remove MM market competitors from DDD feed, only Blenrep DDD by indication data needs to be added
					01/21/2021	MRubio			TES-6656: Fix Jemperli DDD by Indication Quantity and DoseUnits calculation as it was affected by same prod issue as Blenrep
					01/26/2021	MRubio			TES-6677: Added logic to remove dupes from HCO Hierarchy base
					01/26/2021	MRubio			TES-6688: Removed joins not needed due that isExcluded will always default to '1' for Blenrep & Jemperli
					01/26/2021	MRubio			TES-6647: Removed DoseUnits calculation for MM and OC markets
					01/29/2021	MRubio			TES-6699: Fixed data type for QTY field in the dbo.tbltmpSalesTransaction_DDD temp table
					02/08/2021	MRubio			TES-6715: Fixed dupes due to products mapping to two different markets in the final insert to the destination table
					02/08/2021	MRubio			TES-6715: Fixed dupes due to products mapping to two different markets in the final insert to the destination table
					03/18/2021	MRubio			TES-6759: Include MM Market IOD Data
					07/30/2021  Lrafferty		TES-8179: removed Diplomat Names populate


Copyright:	
RunTime:	
Execution:	1 min

					EXEC dbo.uspLoad_AdhocShyftSalesTransaction
 
Helpful Selects:

					---- Source Tables:
						SELECT * FROM TESARO_CDW.agd.tblProduct
						SELECT * FROM TESARO_CDW.dbo.tblProductAttributes
						SELECT * FROM TESARO_CDW.dbo.tblMDChannel
						SELECT * FROM TESARO_CDW.agd.tblMeasure
						SELECT * FROM TESARO_CONTROLLER.agd.tblMdDataFeed
						SELECT * FROM TESARO_CDW.dbo.tblProgram
						SELECT * FROM TESARO_CDW.dbo.tblHCPMaster
						SELECT * FROM TESARO_CDW.dbo.tblHCPHCOAffiliation
						SELECT * FROM TESARO_CDW_CM.dbo.tblHCOFourTierHierarchy
					
					---- Staging Tables:
						SELECT * FROM TESARO_CDW.agd.tblStgSalesTransaction

					---- Reporting Tables:
						SELECT * FROM TESARO_CDW.dbo.tblShyft_SalesTransaction

*******************************************************************************************************************************************************/

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
		DECLARE @LEN INT = (SELECT MAX(LEN(PRODUCTKEY)) FROM vwADHOCProduct)

		--Zejula IMS DDD Competitor data
		DECLARE @Zejula_IMS_DDD_CM_DFID INT = (
		SELECT TPSDatafeedid
		FROM TESARO_CDW_CM.cm.tbldatafeed
		WHERE datafeedtable = 'dbo.tbldfIMS_DDD_LYNPARZA_DATA_PKUNITS'
		)
		DECLARE @Zejula_IMS_DDD_IM_DFID INT = (
		SELECT TPSDatafeedid
		FROM tesaro_controller.agd.tblmddatafeed
		WHERE parsename(importtablename, 1) = 'tbldfIMS_DDD_LYNPARZA_DATA_PKUNITS'
		)
		DECLARE @Zejula_IQVIA_DDD_IM_DFID INT = (
		SELECT TPSDatafeedid
		FROM tesaro_controller.agd.tblmddatafeed
		WHERE parsename(importtablename, 1) = 'tbldfGSK_OVARIAN_DDD_By_Indication'
		)

		--Blenrep IQVIA DDD MD Competitor data
		DECLARE @Blenrep_IQVIA_DDDMD_CM_DFID INT = (
		SELECT TPSDatafeedid
		FROM TESARO_CDW_CM.cm.tbldatafeed
		WHERE datafeedtable = 'dbo.tblIQVIACompetitorDDDMDoutlet'
		)
		DECLARE @Blenrep_IQVIA_DDDMD_IM_DFID INT = (
		SELECT TPSDatafeedid
		FROM tesaro_controller.agd.tblmddatafeed
		WHERE parsename(importtablename, 1) = 'tbldfBelamafGSK_DDDMD'
		)

		--MM Market Intrisiq IOD data
		DECLARE @MMMarket_Intrinsiq_IOD_IM_DFID INT = (
		SELECT TPSDatafeedid
		FROM tesaro_controller.agd.tblmddatafeed
		WHERE parsename(importtablename, 1) = 'tbldfMMMarket_AllAdmins'
		)

		--Jemperli IQVIA DDD by Indication data
		DECLARE @Jemperli_IQVIA_DDDMD_CM_DFID INT = (
		SELECT TPSDatafeedid
		FROM TESARO_CDW_CM.cm.tbldatafeed
		WHERE datafeedtable = 'dbo.tbldfGSK_ENDO_DDD_By_Indication'
		)
		DECLARE @Jemperli_IQVIA_DDDMD_IM_DFID INT = (
		SELECT TPSDatafeedid
		FROM tesaro_controller.agd.tblmddatafeed
		WHERE parsename(importtablename, 1) = 'tbldfGSK_ENDO_DDD_By_Indication'
		)

		DROP TABLE IF EXISTS dbo.tblIQVIA_DDD_Competitor
		SELECT *
		INTO  dbo.tblIQVIA_DDD_Competitor
		FROM  TESARO_IM.dbo.tbldfBelamafGSK_DDDMD ddd
		INNER JOIN TESARO_IM.dbo.tbldfBelamafGSK_DDDMDDemo demo
			ON ddd.AccountID = demo.DDDOutletNumber
		INNER JOIN TESARO_IM.dbo.tbldfTesaro_V1P_CLI_SUBCAT_GRP subCat
			ON ddd.Subcategory=subCat.SUBCAT_CODE 
		WHERE ISNULL(TimePeriod, '') <> ''

		-- TES-6656 Production issue: we only want blenrep here, all the competitors come from TESARO_CDW.agd.tblStgSalesTransaction already
		DROP TABLE IF EXISTS #tmpProductsIn_MM_Market
		SELECT DISTINCT ShyftProductId,REPLICATE('0',@LEN-LEN(ProductKey))+ProductKey AS ProductKey,MKT_CD,DDDFactor
		INTO	#tmpProductsIn_MM_Market
		FROM	dbo.vwADHOCProduct
		WHERE	Brand IN ('Blenrep')


		DROP TABLE IF EXISTS dbo.tbltmpSalesTransaction_without_DDD;
		DROP TABLE IF EXISTS dbo.tbltmpSalesTransaction_DDD;

		SELECT    RowId
				, SourceTransactionID
				, DatafeedID
				, ProductId
				, MeasureID
				, TransactionDate
				, Qty
				, IsNewPatient
				, IsNewFill
				, EntityID
				, EntityTypeID
				, PayerID
				, ProgramID
				, IsExcluded
				, DDDIndication
				, ChannelID
				, OutletSubcatCode = CAST(NULL AS NVARCHAR)
				, MKT_CD = CAST(
								CASE WHEN DatafeedID = @Blenrep_IQVIA_DDDMD_IM_DFID  THEN 'MM'
									 WHEN DatafeedID = @Jemperli_IQVIA_DDDMD_IM_DFID THEN 'EC'
									WHEN DatafeedID = @Zejula_IQVIA_DDD_IM_DFID THEN 'OC'
									WHEN DatafeedID = @MMMarket_Intrinsiq_IOD_IM_DFID AND ProductId =  '6300' THEN 'EC'
									WHEN DatafeedID = @MMMarket_Intrinsiq_IOD_IM_DFID AND ProductId <> '6300' THEN 'MM'
								END 
							AS NVARCHAR)
				, OrgTypeDesc = CAST(NULL AS NVARCHAR)
				, ProductKey
				, NDC
				, DDDFactor
				, PatientEquivalenceFactor
				, DoseUnits
				, ConfidenceLevel
				, SplitMethodology
			INTO dbo.tbltmpSalesTransaction_without_DDD
		FROM agd.tblStgSalesTransaction

		--select * from dbo.tbltmpSalesTransaction_without_DDD


		--Add IQVIA Blenrep DDD by Indication data
		SELECT RowID =  ROW_NUMBER() OVER(ORDER BY a.TimePeriod)
			, SourceTransactionID = '-1'
			, DatafeedID = @Blenrep_IQVIA_DDDMD_IM_DFID --43
			, ProductId = b.ShyftProductId
			, MeasureID = 1
			, TransactionDate = try_cast(cast(right(a.TimePeriod, 4) AS VARCHAR(4)) + cast(left(a.timeperiod, 2) AS VARCHAR(2)) + cast(substring(a.TimePeriod, 3, 2) AS VARCHAR(2)) AS DATE)
			, Qty = SUM(CAST(a.Pack_Units AS DECIMAL(12,4)) * ISNULL(CAST(b.DDDFactor AS DECIMAL(12,4)), 1))
			, IsNewPatient = '0'
			, IsNewFill = '0'
			, EntityID = COALESCE(c.TPSEntityID, OA.TPSEntityID)
			, EntityTypeID = '2'
			, PayerID = NULL
			, ProgramID = NULL
			, IsExcluded = '1'  --By default GSK's drug will always have isExcluded=1 in TESARO_CDW.agd.tblStgSalesTransaction and TESARO_ADHOC.dbo.tblShyft_SalesTransaction
			, DDDIndication = CAST(NULL AS NVARCHAR(20))
			, ChannelID = 3
			, OutletSubcatCode = a.SubcategoryCode
			, MKT_CD = b.MKT_CD
			, OrgTypeDesc = c.OrgTypeDesc
			, ProductKey = b.ProductKey
			, NDC = CAST(RefProduct.NDCCD AS VARCHAR(20))
			, DDDFactor = CAST(ISNULL(b.DDDFactor, 1) AS DECIMAL(12,4))
			, PatientEquivalenceFactor = CAST(NDCMap.PatientEquivalenceFactor AS DECIMAL(12,4))
			, DoseUnits = CAST(NULL AS decimal(12,4))
			, ConfidenceLevel = CAST(NULL AS NVARCHAR(10))
			, SplitMethodology = CAST(NULL AS NVARCHAR(100))
		
		INTO dbo.tbltmpSalesTransaction_DDD
		FROM TESARO_CDW.dbo.tblIQVIA_DDD_Competitor a
		INNER JOIN Tesaro_IM.dbo.tbldfBelamafGSK_APLD_RefProduct_HISTORY RefProduct
			ON	REPLICATE('0',@LEN-LEN(a.NDCCode))+a.NDCCode = REPLICATE('0',@LEN-LEN(RefProduct.NDCCD))+RefProduct.NDCCD
			--SHYFT Product IDs for Mapping Later--
		INNER JOIN #tmpProductsIn_MM_Market b
			ON b.ProductKey = REPLICATE('0',@LEN-LEN(RefProduct.CMFPRODNBR))+RefProduct.CMFPRODNBR
				AND ISNULL(b.MKT_CD,'MM') = 'MM'
			--Limiting to Competitors/Dynamic File--
		INNER JOIN (SELECT DISTINCT RIGHT('0000000000' + REPLACE(NDC, '-', ''), 11) AS NDC, Product, ProductDetailName, Factor, PatientEquivalenceFactor, NormalizedFactor
			    FROM TESARO_IM.dbo.tblDataFeed_Tesaro_ProductNDC_Mapping
			) NDCMap
			ON NDCMAP.NDC = a.NDCCode
		INNER JOIN AGD.tblOutletAlternateID OA
			ON A.AccountID = OA.DataProviderUniqueIdentifier
				AND OA.TPSDataFeedId = @Blenrep_IQVIA_DDDMD_CM_DFID --100004
		LEFT JOIN (SELECT Parent_HCO_Id,Parent_CID,Child_HCO_Id,Child_CID,Child_Org_Type,OverrideFlag
				 ,RN = ROW_NUMBER() OVER(PARTITION BY Child_HCO_Id,Parent_HCO_Id,Child_Org_Type ORDER BY OverrideFlag DESC)
			  FROM dbo.tblHCOHierarchyBase
			) oH
			ON OA.TPSEntityId = oH.Child_HCO_Id
				AND oH.Child_Org_Type = 'Outlet'
				AND oH.RN = 1
		LEFT JOIN dbo.tblHCOMaster c
			ON oH.Parent_HCO_Id = c.TPSEntityID
		GROUP BY b.ShyftProductId
			,a.TimePeriod
			,b.DDDFactor
			,COALESCE(c.TPSEntityID, OA.TPSEntityID)
			,b.ProductKey
			,RefProduct.NDCCD
			,NDCMap.PatientEquivalenceFactor
			,a.SubcategoryCode
			,b.MKT_CD
			,c.OrgTypeDesc

		--select * from dbo.tbltmpSalesTransaction_DDD


		--Add IQVIA Jemperli DDD by Indication data
		INSERT INTO dbo.tbltmpSalesTransaction_DDD (
			SourceTransactionID
			, DatafeedID
			, ProductId
			, MeasureID
			, TransactionDate
			, Qty
			, IsNewPatient
			, IsNewFill
			, EntityID
			, EntityTypeID
			, PayerID
			, ProgramID
			, IsExcluded
			, DDDIndication
			, ChannelID
			, OutletSubcatCode
			, MKT_CD
			, OrgTypeDesc 
			, ProductKey
			, NDC
			, DDDFactor
			, PatientEquivalenceFactor
			, DoseUnits
			, ConfidenceLevel
			, SplitMethodology
			)

		SELECT
			SourceTransactionID			= '-1'
			, DatafeedID				= @Jemperli_IQVIA_DDDMD_IM_DFID
			, ProductId					= product.ShyftProductId
			, MeasureID					= 1
			, TransactionDate			= EOMONTH(DATEFROMPARTS(LEFT(a.MONTH_ID,4),RIGHT(a.MONTH_ID,2),'01'))
			, Qty						= SUM(CAST(a.DDD_P_UNITS AS DECIMAL(12,4)) * CAST(a.INDICATION_Perc AS DECIMAL(12,4)) * ISNULL(CAST(product.DDDFactor AS DECIMAL(12,4)), 1))
			, IsNewPatient				= '0'
			, IsNewFill					= '0'
			, EntityID					= COALESCE(hcoParent.TPSEntityID, OA.TPSEntityID)
			, EntityTypeID				= '2'
			, PayerID					= NULL
			, ProgramID 					= NULL
			, IsExcluded				= '1'  -- TES-6374 Acceptance criteria 2
			, DDDIndication				= a.Indication
			, ChannelID					= chan.ChannelID
			, OutletSubcatCode			= a.SUBCAT
			, MKT_CD				= product.MKT_CD
			, OrgTypeDesc				= hcoParent.OrgTypeDesc
			, ProductKey				= product.ProductKey
			, NDC						= CAST(RefProduct.NDC_CD AS VARCHAR(20))
			, DDDFactor					= ISNULL(product.DDDFactor, 1)
			, PatientEquivalenceFactor	= CAST(mapping.PatientEquivalenceFactor AS decimal(12,4))
			, DoseUnits					= SUM(CAST(a.DDD_P_UNITS AS decimal(12,4)) * CAST(a.INDICATION_Perc AS decimal(12,4)) * ISNULL(product.DDDFactor, 1) * ISNULL(CAST(mapping.PatientEquivalenceFactor AS decimal(12,4)),1))
			, ConfidenceLevel			= CASE  WHEN (a.SPLIT_METHODOLOGY LIKE '%RX%MATCH%OUTLET%') OR (a.SPLIT_METHODOLOGY LIKE '%MATCH%ORG%') THEN 'High'
												WHEN (a.SPLIT_METHODOLOGY LIKE '%RX%MATCH%AFF%HCP%') OR (a.SPLIT_METHODOLOGY LIKE '%RX%MATCH%AFF%PARENT%') THEN 'Medium'
												WHEN (a.SPLIT_METHODOLOGY LIKE '%GEOGRAPHIC%') THEN 'Low' END
			, SplitMethodology				= a.SPLIT_METHODOLOGY
 
		-- select a.*
		FROM TESARO_IM.dbo.tbldfGSK_ENDO_DDD_By_Indication a
		INNER JOIN TESARO_IM.dbo.tbldfDostarlimabGSK_APLD_RefProduct_Endo_HISTORY RefProduct
			ON RefProduct.NDC_CD = RIGHT('0000000000' + cast(a.NDC AS VARCHAR), 11)
		INNER JOIN TESARO_CDW.dbo.vwADHOCProduct product
			ON product.ProductKey = RefProduct.CMF_PROD_NBR
				AND product.MKT_CD='EC' 
				AND product.INCLUSION_IND='Y'
				AND product.BRAND IN ('Dostarlimab','Jemperli')
		LEFT JOIN TESARO_IM.dbo.tblDataFeed_Tesaro_ProductNDC_Mapping mapping
			ON RefProduct.NDC_CD = RIGHT('0000000000' + CAST(mapping.NDC AS VARCHAR), 11)
		INNER JOIN (
			SELECT DISTINCT TPSEntityID, DataProviderUniqueIdentifier 
			FROM  TESARO_CDW.AGD.tblOutletAlternateID alt
			INNER JOIN TESARO_CDW_CM.CM.tbldatafeed d
				ON alt.TPSdataFeedID = d.TPSDataFeedId
			WHERE d.DataFeedDescription like '%DDD%'
			)OA 
			ON a.OUTLET_ID= OA.DataProviderUniqueIdentifier 
		INNER JOIN TESARO_CDW.dbo.tblMDChannel Chan
			ON Chan.ChannelName = 'DDD'
		LEFT JOIN (SELECT Parent_HCO_Id,Parent_CID,Child_HCO_Id,Child_CID,Child_Org_Type,OverrideFlag
				 ,RN = ROW_NUMBER() OVER(PARTITION BY Child_HCO_Id,Parent_HCO_Id,Child_Org_Type ORDER BY OverrideFlag DESC)
			  FROM dbo.tblHCOHierarchyBase
			) oH
			ON OA.TPSEntityId = oH.Child_HCO_Id
				AND oH.Child_Org_Type = 'Outlet'
				AND oH.RN = 1
		LEFT JOIN TESARO_CDW.dbo.tblHCOMaster hcoParent
			ON oH.Parent_HCO_Id = hcoParent.TPSEntityID
		GROUP BY product.ShyftProductId
			,a.MONTH_ID
			,a.INDICATION_Perc
			,product.DDDFactor
			,hcoParent.TPSEntityID
			,OA.TPSEntityID
			,product.Brand
			,chan.ChannelID
			,RefProduct.NDC_CD
			,a.INDICATION
			,product.ProductKey
			,mapping.PatientEquivalenceFactor
			,a.SPLIT_METHODOLOGY
			,a.SUBCAT
			,product.MKT_CD
			,hcoParent.OrgTypeDesc


		--Add Zejula IMS DDD legacy data
		INSERT INTO dbo.tbltmpSalesTransaction_DDD (
				RowId
				, SourceTransactionID
				, DatafeedID
				, ProductId
				, MeasureID
				, TransactionDate
				, Qty
				, IsNewPatient
				, IsNewFill
				, EntityID
				, EntityTypeID
				, PayerID
				, ProgramID
				, IsExcluded
				, DDDIndication
				, ChannelID
				, OutletSubcatCode
				, MKT_CD
				, OrgTypeDesc 
				, ProductKey
				, NDC
				, DDDFactor
				, PatientEquivalenceFactor
				, DoseUnits
				, ConfidenceLevel
				, SplitMethodology
							)

		--legacy code from dbo.uspLoad_IMS_DDD_AGDStgSalesTransaction
		SELECT   RowId = ROW_NUMBER() OVER(ORDER BY TransactionDate)
			, SourceTransactionID = '-1' 
			, DatafeedID = a.DataFeedId
			, ProductId = b.ShyftProductId
			, MeasureID = 1 
			, a.TransactionDate 
			, Qty = CAST(Qty AS DECIMAL(12,4)) * ISNULL(CAST(DDDFactor AS DECIMAL(12,4)),1)
			, IsNewPatient = '0' 
			, IsNewFill = '0' 
			, EntityID = COALESCE(c.TPSEntityID,OA.TPSEntityID)
			, EntityTypeID = '2'
			, PayerID = NULL
			, ProgramID = NULL
			, IsExcluded = 0
			, DDDIndication = NULL
			, ChannelID = 3  --select ChannelID from tblMDChannel where ChannelName='DDD'
			, OutletSubcatCode = a.OutletSubcatCode
			, MKT_CD = b.MKT_CD
			, OrgTypeDesc = c.OrgTypeDesc
			, ProductKey = b.ProductKey
			, NDC = NULL
			, DDDFactor = NULL
			, PatientEquivalenceFactor = NULL
			, DoseUnits = NULL
			, ConfidenceLevel = NULL
			, SplitMethodology = NULL
	
		FROM (select * 
			  from dbo.tblStgIMS_DDD_Unpivot
			  where TransactionDate IS NOT NULL AND DataFeedID = @Zejula_IMS_DDD_IM_DFID --'30140'
			  ) a
		INNER JOIN	vwADHOCProduct b 
			ON	REPLICATE('0',@LEN-LEN(a.ProductGroup))+a.ProductGroup = REPLICATE('0',@LEN-LEN(b.ProductKey))+b.ProductKey
				AND ISNULL(b.MKT_CD,'OC') = 'OC'
		INNER JOIN  AGD.tblOutletAlternateID OA 
			ON	CASE WHEN LEFT(A.INSNumber,2) = 'NS' THEN (A.OutletNumber+'^I'+ A.INSNumber) ELSE A.OutletNumber END = OA.DataProviderUniqueIdentifier 
				AND OA.TPSDataFeedId = @Zejula_IMS_DDD_CM_DFID --'5011'
		LEFT JOIN (SELECT Parent_HCO_Id,Parent_CID,Child_HCO_Id,Child_CID,Child_Org_Type,OverrideFlag
				 ,RN = ROW_NUMBER() OVER(PARTITION BY Child_HCO_Id,Parent_HCO_Id,Child_Org_Type ORDER BY OverrideFlag DESC)
			  FROM dbo.tblHCOHierarchyBase
			) oH
			ON OA.TPSEntityId = oH.Child_HCO_Id
				AND oH.Child_Org_Type = 'Outlet'
				AND oH.RN = 1
		LEFT JOIN  dbo.tblHCOMaster c 
			ON oH.Parent_HCO_Id = c.TPSEntityID

		--select * from dbo.tbltmpSalesTransaction_DDD
		
		-- isExcluded is updated separately because the join to the SubCatExclusion and GSKAcct_Type tables seems to randomly cause performance issues
		-- this DDD weekly feed has other products than just Zejula, for that reason isExcluded needs to be calculated and not defaulted to 1
		UPDATE tmpDDD
		SET IsExcluded =	IIF((tmpDDD.ProductID <> '1840'),0,1)
							+
							IIF(f.Inclusion_flag= 'Y' and ISNULL(g.Inclusion_Flag,'Y') = 'Y',0,1)
		FROM dbo.tbltmpSalesTransaction_DDD tmpDDD
		LEFT JOIN	TESARO_IM.dbo.tbldfIMS_DDD_SubCatExclusion f 
			on tmpDDD.OutletSubcatCode = f.SUBCAT_CODE 
				and tmpDDD.MKT_CD = f.Market
		LEFT JOIN	TESARO_CDW.dbo.tblMd_GSKACCT_TYPE g 
			on tmpDDD.OrgTypeDesc = g.ACCT_TYP_DESC
		WHERE DataFeedID = @Zejula_IMS_DDD_IM_DFID

		--select * from dbo.tbltmpSalesTransaction_DDD

		

		
		DROP TABLE IF EXISTS TESARO_CDW.dbo.tblShyft_SalesTransaction;

		--Legacy view TESARO_CDW.dbo.vwADHOCSalesTransaction definition

		SELECT	RowId						=	RowId
					,SourceTransactionID			=	IIF(DataFeedName LIKE '%BIO_DISP_%',NULL,SourceTransactionID)
					,DatafeedID						=	DatafeedID
					,DataFeedName					=   CASE WHEN STG.DatafeedID = 209 THEN 'Import SP2 Dispense File' WHEN STG.DatafeedID = 210 THEN 'Import SP2 Status File' WHEN STG.DatafeedID = 314 THEN 'Import SP2 NonComm Dispense File' ELSE DataFeedName END
					,ProductId						=	P.ProductId
					,ProductName					=	P.ProductNameDisplay
					,MeasureID						=	ME.MeasureID
					,MeasureName					=	ME.MeasureNAme
					,TransactionDate				=	TransactionDate
					,Qty							=   Qty
					,IsNewPatient					=	IsNewPatient
					,IsNewFill						=	IsNewFill
					,HCPID							=	IIF(EntityTypeID = 1,EntityID,NULL)
					,OutletID						=	NULL
					,OrgID							=	F.Organization_Id
					,ParentOrgID					=	F.Parent_Id
					,GrandParentOrgID				=	F.GrandParent_Id
					,EntityID						=	Stg.EntityID
					,PayerID						=	PayerID
					,IsPDRP							=	PDRP
					,IsExcluded						=	IsExcluded
					,ChannelID						=   Ch.ChannelID
					,ChannelName					=	Ch.ChannelName
					,ProgramID						=	Stg.ProgramID
					,ProgramCode					=	Pgr.ProgramCode
					,MKT_CD							=	COALESCE(Stg.MKT_CD,Pa.CustomColumn_MKT_CD)
					,HCP_CID						=	Pdrp.CID
					,ProductKey						=	Stg.ProductKey
					,NDC							=	Stg.NDC
					,DDDFactor						=	Stg.DDDFactor
					,DDDIndication					=	Stg.DDDIndication
					,PatientEquivalenceFactor		=	Stg.PatientEquivalenceFactor
					,DoseUnits						=	Stg.DoseUnits
					,ConfidenceLevel				=	Stg.ConfidenceLevel
					,SplitMethodology				=	Stg.SplitMethodology

					INTO TESARO_CDW.dbo.tblShyft_SalesTransaction

				--select *
				FROM (select * from dbo.tbltmpSalesTransaction_DDD
					  union all
					  select * from dbo.tbltmpSalesTransaction_without_DDD
					  ) Stg
				LEFT JOIN TESARO_CDW.AGD.tblProduct P
				ON Stg.PRoductID = P.ProductID
				LEFT JOIN (SELECT DISTINCT ProductID, CustomColumn_MKT_CD FROM TESARO_CDW.DBO.tblProductAttributes P) Pa
				ON P.PRoductID = Pa.ProductID
					AND ISNULL(Stg.MKT_CD,'') = ISNULL(Pa.CustomColumn_MKT_CD,'')
				LEFT JOIN  TESARO_CDW.[dbo].[tblMDChannel] Ch
				ON Stg.ChannelID = Ch.ChannelID
				LEFT JOIN  TESARO_CDW.AGD.tblMeasure ME
				ON Stg.MeasureID = ME.MeasureID
				LEFT JOIN TESARO_CONTROLLER.AGD.tblMdDataFeed DF
				ON Stg.DataFeedID = DF.TPSDataFeedID
				LEFT JOIN   TESARO_CDW.dbo.tblProgram Pgr 
				ON Stg.ProgramID = Pgr.ProgramID
				LEFT JOIN  TESARO_CDW.dbo.tblHCPMaster Pdrp
				ON pdrp.TPSEntityID = Stg.EntityID 
				LEFT JOIN	TESARO_CDW.dbo.tblHCPHCOAffiliation	af
				ON af.HCP_ID = Stg.EntityID AND af.PrimaryFlag = 'Y'
				LEFT JOIN	(SELECT DISTINCT Organization_Id,	Parent_Id,	Grandparent_Id FROM TESARO_CDW_CM.dbo.tblHCOFourTierHierarchy) F
				ON F.Organization_Id = COALESCE(IIF(EntityTypeID <> 2, NULL,Stg.EntityID),af.HCO_Id)

			DROP TABLE IF EXISTS #tmpProductsIn_MM_Market
			DROP TABLE IF EXISTS dbo.tbltmpSalesTransaction_without_DDD;
			DROP TABLE IF EXISTS dbo.tbltmpSalesTransaction_DDD;


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

GO


