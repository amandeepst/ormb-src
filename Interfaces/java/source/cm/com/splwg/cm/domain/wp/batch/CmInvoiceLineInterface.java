package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 *
 * @BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billId, type = string)
 *            , @BatchJobSoftParameter (name = chunkSize, type = integer)
 *            , @BatchJobSoftParameter (name = billDate, type = date)})
 */

public class CmInvoiceLineInterface extends CmInvoiceLineInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CmInvoiceLineInterface.class);
	private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = null;

	public JobWork getJobWork() {
		invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		threadWorkUnitList = getInvoiceData();
		invoiceDataInterfaceLookUp = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	private List<ThreadWorkUnit> getInvoiceData() {
		PreparedStatement preparedStatement = null;
		//DateTime ilmDateTime = getIlmDate();
		String invLowBsegId = "";
		String invHighBsegId = "";
		int chunkSize = getParameters().getChunkSize().intValue();
		int threadCount = getParameters().getThreadCount().intValue();
		Date billDate = getParameters().getBillDate();
		String billId = getParameters().getBillId();
		StringBuilder stringBuilder = new StringBuilder();
		InvoiceBseg_Id invoiceData = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();

		chunkSize = getChunkSize(billDate, chunkSize, threadCount, billId);

		try {

			stringBuilder.append("WITH TBL AS (SELECT B.BSEG_ID FROM CM_BSEG_INFO B ");
			stringBuilder.append("WHERE NOT EXISTS (SELECT 1 FROM CM_INVOICE_DATA_LN WHERE BSEG_ID=B.BSEG_ID AND BILL_ID=B.BILL_ID AND ILM_DT = B.ILM_DT) ");

			if (billDate != null && billId == null){
				stringBuilder.append("AND B.BILL_DT =:billDate ");
			}
			else if (billDate == null && billId != null){
				stringBuilder.append("AND B.BILL_ID=:billId ");
			}

			stringBuilder.append("ORDER BY B.BSEG_ID)  ");
			stringBuilder.append("SELECT /*+ PARALLEL(4) */ THREAD_NUM, MIN(BSEG_ID) AS LOW_INV_BSEG_ID, ");
			stringBuilder.append("MAX(BSEG_ID) AS HIGH_INV_BSEG_ID FROM (SELECT BSEG_ID, ");
			stringBuilder.append("CEIL((ROWNUM)/:CHUNKSIZE) AS THREAD_NUM FROM TBL) ");
			stringBuilder.append("GROUP BY THREAD_NUM ORDER BY 1 ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.bindBigInteger("CHUNKSIZE", new BigInteger(String.valueOf(chunkSize)));
			if (billDate != null && billId == null){
				preparedStatement.bindDate("billDate",billDate);
			}
			else if (billDate == null && billId != null){
				preparedStatement.bindString("billId", billId,"BILL_ID");
			}
			preparedStatement.setAutoclose(false);

			for (SQLResultRow sqlRow : preparedStatement.list()) {
				invLowBsegId = sqlRow.getString("LOW_INV_BSEG_ID");
				invHighBsegId = sqlRow.getString("HIGH_INV_BSEG_ID");
				invoiceData = new InvoiceBseg_Id(invLowBsegId, invHighBsegId);

				// *************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(invoiceData);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				invoiceData = null;
				// *************************

			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getInvoiceData() method of CmInvoiceLineInterface, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occurred in getJobWork() while picking Bseg Ids - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside invoice data line interface method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}

		return threadWorkUnitList;
	}


		public int getChunkSize(Date billDate, int chunkSize, int threadCount, String billId) {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		BigInteger maxRecordcount = BigInteger.valueOf(0);
		int countPerThread;
		try {
			stringBuilder.append(" SELECT /*+ PARALLEL(4) */ COUNT(BSEG_ID) AS COUNT FROM CM_BSEG_INFO B");
			stringBuilder.append(" WHERE NOT EXISTS (SELECT 1 FROM CM_INVOICE_DATA_LN WHERE BSEG_ID=B.BSEG_ID AND BILL_ID=B.BILL_ID AND ILM_DT = B.ILM_DT) ");
			if (billDate != null && billId == null){
				stringBuilder.append(" AND BILL_DT=:billDate ");
			}
			else if (billDate == null && billId != null){
				stringBuilder.append(" AND BILL_ID=:billId ");
			}

			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");

			preparedStatement.setAutoclose(false);
			if (billDate != null && billId == null){
				preparedStatement.bindDate("billDate",billDate);
			}
			else if (billDate == null && billId != null){
				preparedStatement.bindString("billId",billId,"BILL_ID");
			}

			SQLResultRow sqlRow = preparedStatement.firstRow();
			maxRecordcount = sqlRow.getInteger("COUNT");
			countPerThread = (int) Math.ceil(maxRecordcount.doubleValue() / threadCount);
			if (countPerThread < chunkSize) {
				chunkSize = countPerThread;
			}

		} catch (RuntimeException e) {
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.toString()));
		} catch (Exception e) {
			logger.error("Calling gather count, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return chunkSize;
	}


	public Class<CmInvoiceLineInterfaceWorker> getThreadWorkerClass() {
		return CmInvoiceLineInterfaceWorker.class;
	}

	public static class CmInvoiceLineInterfaceWorker extends CmInvoiceLineInterfaceWorker_Gen {

		private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = null;
		String errMsg;

		@Override
		public void initializeThreadWork(boolean initializationPreviouslySuccessful)
				throws ThreadAbortedException, RunAbortedException {
			errMsg = null;
			logger.debug("Inside initializeThreadWork() method for batch thread number: " + getBatchThreadNumber());
			if (invoiceDataInterfaceLookUp == null) {
				invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();
			}
			super.initializeThreadWork(initializationPreviouslySuccessful);

		}

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {

			DateTime ilmDateTime = (DateTime) unit.getSupplementallData("ilmDateTime");
			String invLowBsegId = "";
			String invHighBsegId = "";
			InvoiceBseg_Id invoiceData = (InvoiceBseg_Id) unit.getPrimaryId();
			invLowBsegId = invoiceData.getInvLowBsegId();
			invHighBsegId = invoiceData.getInvHighBsegId();
			Date billDate = getParameters().getBillDate();
			String billId = getParameters().getBillId();


			/***
			 * For bills retrieved in CM_INV_BILL_PER_INFO ,fetch bill segment related
			 * information along with product info for bill segments coming from TFM or
			 * Billable charge upload interface. Fetch information such as Bill
			 * identifier,bill segment identifier,account type,parent service agreement
			 * id,child service agreement id, child person identifier,merchant currency,bill
			 * segment calculation amount,billable charge identifier,charge type,charge type
			 * description, pricing assignment identifier & pricing currency. Also fetch
			 * Funding Currency UDF_CHAR_25. This table will act as base table to fetch bill
			 * calculation,service quantity and rate info Copy all this into
			 * CM_INV_BSEG_PROD_INFO
			 */

			insertIntoCmInvProdInfo(invLowBsegId,invHighBsegId,billDate,billId);

			/***
			 * For bills retrieved in CM_INV_BILL_PER_INFO ,fetch bill segment related
			 * information along with product info for minimum charge bill segments. Fetch
			 * information such as Bill identifier,bill segment identifier,account
			 * type,parent service agreement id,child service agreement id, Parent person
			 * identifier,merchant currency,bill segment calculation amount,billable charge
			 * identifier,charge type,charge type description, pricing assignment identifier
			 * & pricing currency. Also set funding currency UDF_CHAR_25 as blank. This
			 * table will act as base table to fetch bill calculation,service quantity and
			 * rate info Copy all this into CM_INV_BSEG_PROD_INFO
			 */

			insertIntoCmInvProdInfoPost(invLowBsegId,invHighBsegId,billDate,billId);


			/*********** Insert data in Invoice Staging tables *************************/


			insertIntoInvoiceStagingTables();
			

			return true;
		}

		public void insertIntoCmInvProdInfo(String lowBsegId,String highBsegId,Date billDate,String billId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("	INSERT INTO CM_INV_BSEG_PROD_INFO ");
				stringBuilder.append("	(BILL_ID, BSEG_ID, ACCT_TYPE, PARENT_SA_ID, CHILD_SA_ID, BILLING_PARTY_ID, CURRENCY_CD, CALC_AMT, ");
				stringBuilder.append("	BILLABLE_CHG_ID, PRICE_CATEGORY, PRICE_CATEGORY_DESCR, PRICE_ASGN_ID, PRICE_CURRENCY_CD, STATUS_COD,");
				stringBuilder.append("	MESSAGE_NBR, ERROR_INFO, UDF_CHAR_25,TXN_CALC_ID,UDF_NBR_19,SETT_LEVEL_GRANULARITY,ILM_DT) ");
				stringBuilder.append("	SELECT ");
				
				stringBuilder.append("	/*+  ");
				stringBuilder.append("	    LEADING(@\"SEL$B2D943A5\" \"TBL1\"@\"SEL$1\" \"CM_INVOICE_DATA_LN\"@\"SEL$15\" \"A\"@\"SEL$1\" \"ASGN\"@\"SEL$1\" \"PITEML\"@\"SEL$1\" \"G\"@\"SEL$7\" \"M\"@\"SEL$8\"  \"H\"@\"SEL$7\" \"I\"@\"SEL$9\" \"J\"@\"SEL$10\" \"TXM\"@\"SEL$1\" \"K\"@\"SEL$11\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"CM_INVOICE_DATA_LN\"@\"SEL$15\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"A\"@\"SEL$1\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"ASGN\"@\"SEL$1\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"PITEML\"@\"SEL$1\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"G\"@\"SEL$7\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"M\"@\"SEL$8\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"H\"@\"SEL$7\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"I\"@\"SEL$9\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"J\"@\"SEL$10\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"TXM\"@\"SEL$1\") ");
				stringBuilder.append("	    USE_NL(@\"SEL$B2D943A5\" \"K\"@\"SEL$11\") ");
				stringBuilder.append("	    PUSH_PRED(A 3)  ");
				stringBuilder.append("	    NO_PUSH_PRED(A 6 4)  ");
				stringBuilder.append("	*/  ");
				
				stringBuilder.append("	DISTINCT TBL1.BILL_ID, TBL1.BSEG_ID, TBL1.ACCT_TYPE, TBL1.SA_ID as PARENT_SA_ID, A.SA_ID as CHILD_SA_ID,");
				stringBuilder.append("	ACC.PARTY_ID PER_ID_NBR, TBL1.CURRENCY_CD, TBL1.CALC_AMT, TBL1.BILLABLE_CHG_ID, ");
				stringBuilder.append("	NVL(TRIM(TXM.CHILD_PRODUCT),TRIM(A.PRICEITEM_CD)) AS PRICE_CATEGORY,");
				stringBuilder.append("	NVL((SELECT PTML.DESCR FROM CI_PRICEITEM_L PTML WHERE TRIM(PTML.PRICEITEM_CD)=TXM.CHILD_PRODUCT),PITEML.DESCR) AS PRICE_CATEGORY_DESCR,A.PRICE_ASGN_ID,");
				stringBuilder.append("	CASE WHEN ASGN.PRICE_CURRENCY_CD IS NULL THEN TBL1.CURRENCY_CD ");
				stringBuilder.append("	WHEN ASGN.PA_OWNER_TYPE_FLG='PLST' ");
				stringBuilder.append("	THEN NVL((SELECT trim(PCHAR.ADHOC_CHAR_VAL)  from CI_PER_CHAR PCHAR,CI_ACCT_PER ACCTP WHERE ACCTP.PER_ID=PCHAR.PER_ID ");
				stringBuilder.append("	AND TBL1.ACCT_ID=ACCTP.ACCT_ID  AND PCHAR.CHAR_TYPE_CD='PRCCY' ),TBL1.CURRENCY_CD) ");
				stringBuilder.append("	ELSE ASGN.PRICE_CURRENCY_CD END AS PRICE_CURRENCY_CD, ");
				stringBuilder.append("	:initStatus AS STATUS_COD, 0   AS MESSAGE_NBR, ' ' AS ERROR_INFO, NVL(TXM.UDF_CHAR_25,' ') AS UDF_CHAR_25 ,TXM.TXN_CALC_ID, TXM.UDF_NBR_19, ");
				stringBuilder.append("  TXM.SETT_LEVEL_GRANULARITY,TBL1.ILM_DT ");
				stringBuilder.append("	FROM CM_BSEG_INFO TBL1,	CI_BILL_CHG A, VW_MERCH_ACCT_REF_DATA_RMB ACC,CI_PRICEITEM_L  PITEML ,CI_PRICEASGN ASGN, CM_TXN_ATTRIBUTES_MAP TXM ");
				stringBuilder.append("	WHERE ((TBL1.BSEG_STAT_FLG =:frozen) OR (TBL1.BSEG_STAT_FLG='60' AND TBL1.REBILL_SEG_ID=' '))  ");
				stringBuilder.append("	AND TBL1.STATUS_COD = :initStatus AND TBL1.BILLABLE_CHG_ID=A.BILLABLE_CHG_ID ");
				stringBuilder.append("	AND A.SA_ID=ACC.SUB_ACCT_ID ");
				stringBuilder.append("	AND TBL1.BILLABLE_CHG_ID!=' ' AND PITEML.PRICEITEM_CD = A.PRICEITEM_CD ");
				stringBuilder.append("	AND TBL1.BSEG_TYPE_FLG!='POST' ");
				stringBuilder.append("	AND PITEML.LANGUAGE_CD = :langCode AND A.PRICE_ASGN_ID = ASGN.PRICE_ASGN_ID(+) ");
				stringBuilder.append("	AND TXM.BILLABLE_CHG_ID(+)=TBL1.BILLABLE_CHG_ID ");
				stringBuilder.append("	AND TBL1.BSEG_ID BETWEEN :lowBsegId AND :highBsegId ");
				if (billDate != null && billId == null){
					stringBuilder.append(" AND TBL1.BILL_DT =:billDate ");
				}
				else if (billDate == null && billId != null){
					stringBuilder.append(" AND TBL1.BILL_ID=:billId ");
				}
				stringBuilder.append(" AND NOT EXISTS (SELECT 1 FROM CM_INVOICE_DATA_LN WHERE BSEG_ID=TBL1.BSEG_ID AND BILL_ID=TBL1.BILL_ID  AND ILM_DT = TBL1.ILM_DT) ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(),"STATUS_COD");
				preparedStatement.bindString("langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(),"LANGUAGE_CD");
				preparedStatement.bindLookup("frozen", BillSegmentStatusLookup.constants.FROZEN);
				preparedStatement.bindString("lowBsegId", lowBsegId.trim(), "LOW_BSEG_ID");
				preparedStatement.bindString("highBsegId", highBsegId.trim(), "HIGH_BSEG_ID");
				if (billDate != null && billId == null){
					preparedStatement.bindDate("billDate",billDate);
				}
				else if (billDate == null && billId != null){
					preparedStatement.bindString("billId",billId,"BILL_ID");
				}
				preparedStatement.executeUpdate();

			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INV_BSEG_PROD_INFO data extraction - ", e);
				errMsg = "Insert err: CM_INV_BSEG_PROD_INFO";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BSEG_PROD_INFO - " + e.toString()));
			} catch (Exception e) {
				errMsg = "Insert err:CM_INV_BSEG_PROD_INFO";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BSEG_PROD_INFO - "  + e.toString()));

			} finally {
				closeConnection(preparedStatement);
			}
		}

		public void insertIntoCmInvProdInfoPost(String lowBsegId,String highBsegId, Date billDate, String billId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("	INSERT INTO CM_INV_BSEG_PROD_INFO 	");
				stringBuilder.append("	(BILL_ID, BSEG_ID, ACCT_TYPE, PARENT_SA_ID, CHILD_SA_ID, BILLING_PARTY_ID, CURRENCY_CD, CALC_AMT, 	");
				stringBuilder.append("	BILLABLE_CHG_ID, PRICE_CATEGORY, PRICE_CATEGORY_DESCR, PRICE_ASGN_ID, PRICE_CURRENCY_CD, STATUS_COD, 	");
				stringBuilder.append("	MESSAGE_NBR, ERROR_INFO, UDF_CHAR_25, ILM_DT) 	");
				stringBuilder.append("	SELECT DISTINCT TBL1.BILL_ID, TBL1.BSEG_ID, TBL1.ACCT_TYPE, TBL1.SA_ID as PARENT_SA_ID, NULL as CHILD_SA_ID, 	");
				stringBuilder.append("	PRID.PER_ID_NBR AS BILLING_PARTY_ID, TBL1.CURRENCY_CD, TBL1.CALC_AMT, TBL1.BILLABLE_CHG_ID,	");
				stringBuilder.append("	TRIM(piteml.PRICEITEM_CD) AS PRICE_CATEGORY, PITEML.DESCR AS PRICE_CATEGORY_DESCR ,TBL1.PRICE_ASGN_ID,	");
				stringBuilder.append("	ASGN.PRICE_CURRENCY_CD AS PRICE_CURRENCY_CD,	");
				stringBuilder.append("	:initStatus AS STATUS_COD, 0   AS MESSAGE_NBR, ' ' AS ERROR_INFO, ' ' AS UDF_CHAR_25,TBL1.ILM_DT 	");
				stringBuilder.append("	FROM CM_BSEG_INFO TBL1,CM_ACCT_REF AR,CI_PRICEITEM_L  PITEML,CI_PRICEASGN ASGN, CI_PARTY PRT, CI_PER_ID PRID	");
				stringBuilder.append("	WHERE TBL1.ACCT_TYPE=AR.ACCT_TYPE AND AR.TYPE_VAL=:typeVal AND TBL1.BILL_ID = TBL1.BILL_ID AND TBL1.BSEG_ID = TBL1.BSEG_ID	");
				stringBuilder.append("	AND ((TBL1.BSEG_STAT_FLG = :frozen) OR (TBL1.BSEG_STAT_FLG='60' AND TBL1.REBILL_SEG_ID=' '))	");
				stringBuilder.append("	AND TBL1.STATUS_COD = :initStatus AND TBL1.BSEG_ID = TBL1.BSEG_ID	");
				stringBuilder.append("	AND PRT.PARTY_UID = ASGN.OWNER_ID AND PRT.PARTY_ID = PRID.PER_ID	");
				stringBuilder.append("	AND PRID.ID_TYPE_CD = :idType	");
				stringBuilder.append("	AND TBL1.BSEG_TYPE_FLG='POST' AND TBL1.PRICE_ASGN_ID = ASGN.PRICE_ASGN_ID	");
				stringBuilder.append("	AND TBL1.BILLABLE_CHG_ID=' '  AND TBL1.PRICEITEM_CD = PITEML.PRICEITEM_CD	");
				stringBuilder.append("	AND PITEML.LANGUAGE_CD= :langCode AND TBL1.RS_CD!='TAX     '	");
				stringBuilder.append("	AND TBL1.BSEG_ID BETWEEN :lowBsegId AND :highBsegId ");
				if (billDate != null && billId == null){
					stringBuilder.append(" AND TBL1.BILL_DT =:billDate ");
				}
				else if (billDate == null && billId != null){
					stringBuilder.append(" AND TBL1.BILL_ID=:billId ");
				}
				stringBuilder.append(" AND NOT EXISTS (SELECT 1 FROM CM_INVOICE_DATA_LN WHERE BSEG_ID=TBL1.BSEG_ID AND BILL_ID=TBL1.BILL_ID AND ILM_DT = TBL1.ILM_DT) ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(),
						"STATUS_COD");
				preparedStatement.bindString("langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(),
						"LANGUAGE_CD");
				preparedStatement.bindString("typeVal", "POST", "TYPE_VAL");
				preparedStatement.bindString("idType", invoiceDataInterfaceLookUp.getExternalPartyId().trim(),
						"ID_TYPE_CD");
				preparedStatement.bindLookup("frozen", BillSegmentStatusLookup.constants.FROZEN);
				preparedStatement.bindString("lowBsegId", lowBsegId.trim(), "LOW_BSEG_ID");
				preparedStatement.bindString("highBsegId", highBsegId.trim(), "HIGH_BSEG_ID");
				if (billDate != null && billId == null){
					preparedStatement.bindDate("billDate",billDate);
				}
				else if (billDate == null && billId != null){
					preparedStatement.bindString("billId",billId,"BILL_ID");
				}
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INV_BSEG_PROD_INFO data extraction - ", e);
				errMsg = "Insert err:CM_INV_BSEG_PROD_INFO";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BSEG_PROD_INFO - "  + e.toString()));
			} catch (Exception e) {
				errMsg = "Insert err:CM_INV_BSEG_PROD_INFO";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BSEG_PROD_INFO - "  + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}
		}

		public void insertIntoBsegBclForChrg() {

			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();

			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_BCL ");
			stringBuilder.append("(BILL_ID, BSEG_ID, BCL_TYPE, BCL_DESCR, CALC_AMT, TAX_STAT, TAX_STAT_DESCR, TAX_RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
			stringBuilder.append("SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, CLCHAR.CHAR_VAL AS BCL_TYPE, VAL.DESCR AS BCL_DESCR, LN.CALC_AMT, ");
			stringBuilder.append("TAX.TAX_STAT, TAX.TAX_STAT_DESCR, TAX.TAX_RATE, SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT, 'Y' AS ILM_ARCH_SW ");
			stringBuilder.append("FROM CM_INV_BSEG_PROD_INFO TBL5, CI_BSEG_CL_CHAR CLCHAR, CI_CHAR_VAL_L VAL, CI_BSEG_CALC_LN LN , ");
			stringBuilder.append("CI_PRICEITEM_CHAR PITEM,CM_INV_DATA_TAX TAX ");
			stringBuilder.append("WHERE TBL5.BSEG_ID = CLCHAR.BSEG_ID AND LN.BSEG_ID = CLCHAR.BSEG_ID ");
			stringBuilder.append("AND VAL.CHAR_VAL = CLCHAR.CHAR_VAL AND VAL.CHAR_TYPE_CD = CLCHAR.CHAR_TYPE_CD ");
			stringBuilder.append("AND LN.HEADER_SEQ = CLCHAR.HEADER_SEQ AND LN.SEQNO = CLCHAR.SEQNO ");
			stringBuilder.append("AND VAL.LANGUAGE_CD = :langCode AND CLCHAR.CHAR_TYPE_CD = :bclType ");
			stringBuilder.append("AND TBL5.STATUS_COD = :initStatus AND TRIM(TBL5.PRICE_CATEGORY)=TRIM(PITEM.PRICEITEM_CD) ");
			stringBuilder.append("AND PITEM.CHAR_TYPE_CD=TAX.TAX_STAT_CHAR AND PITEM.CHAR_VAL=TAX.CHAR_VAL AND TBL5.BILL_ID=TAX.BILL_ID AND TBL5.ILM_DT=TAX.ILM_DT");
			// Added Change for NAP Defect-27307

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(), "LANGUAGE_CD");
			addParams(paramsList, "bclType", invoiceDataInterfaceLookUp.getBclTypeCharType().trim(), "CHAR_TYPE_CD");
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_DATA_LN_BCL ", paramsList);

		}

		public void insertIntoBsegBcl() {
			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();

			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_BCL ");
			stringBuilder.append("(BILL_ID, BSEG_ID, BCL_TYPE, BCL_DESCR, CALC_AMT, TAX_STAT, TAX_STAT_DESCR, TAX_RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
			stringBuilder.append("SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, CLCHAR.CHAR_VAL AS BCL_TYPE, VAL.DESCR AS BCL_DESCR, LN.CALC_AMT, ");
			stringBuilder.append("NULL,NULL,NULL, SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT, 'Y' AS ILM_ARCH_SW ");
			stringBuilder.append("FROM CM_INV_BSEG_PROD_INFO TBL5, CI_BSEG_CL_CHAR CLCHAR, CI_CHAR_VAL_L VAL, CI_BSEG_CALC_LN LN ");
			stringBuilder.append("WHERE TBL5.BSEG_ID = CLCHAR.BSEG_ID AND LN.BSEG_ID = CLCHAR.BSEG_ID ");
			stringBuilder.append("AND VAL.CHAR_VAL = CLCHAR.CHAR_VAL AND VAL.CHAR_TYPE_CD = CLCHAR.CHAR_TYPE_CD ");
			stringBuilder.append("AND LN.HEADER_SEQ = CLCHAR.HEADER_SEQ AND LN.SEQNO = CLCHAR.SEQNO ");
			stringBuilder.append("AND VAL.LANGUAGE_CD = :langCode AND CLCHAR.CHAR_TYPE_CD = :bclType ");
			stringBuilder.append("AND TBL5.STATUS_COD = :initStatus AND TBL5.ACCT_TYPE!='CHRG' ");
			// Added Change for NAP Defect-27307

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(), "LANGUAGE_CD");
			addParams(paramsList, "bclType", invoiceDataInterfaceLookUp.getBclTypeCharType().trim(), "CHAR_TYPE_CD");
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_DATA_LN_BCL  (BILL_ID, BSEG_ID,...", paramsList);
			/**
			 * Fetch F_M_AMT from txn calc line into CM_INV_BSEG_BCL_INFO.
			 */

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_INV_DATA_LN_BCL ");
			stringBuilder.append("(BILL_ID, BSEG_ID, BCL_TYPE, BCL_DESCR, CALC_AMT, TAX_STAT, TAX_STAT_DESCR, TAX_RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
			stringBuilder.append(" SELECT TBL5.BILL_ID, TBL5.BSEG_ID, UPPER(QTY.SQI_CD) AS BCL_TYPE, VAL.DESCR AS BCL_DESCR, ");
			stringBuilder.append(" NVL(QTY.SVC_QTY*TBL5.UDF_NBR_19,QTY.SVC_QTY) AS CALC_AMT, ");
			stringBuilder.append(" NULL,NULL,NULL,SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT, 'Y' AS ILM_ARCH_SW ");
			stringBuilder.append(" FROM CM_INV_BSEG_PROD_INFO TBL5, CI_CHAR_VAL_L VAL, CM_BILLABLE_ITEM_SERVICE_QTY QTY ");
			stringBuilder.append(" WHERE TBL5.BILLABLE_CHG_ID = QTY.BILLABLE_CHARGE_ID   ");
			stringBuilder.append(" AND TRIM(VAL.CHAR_VAL) = UPPER(QTY.SQI_CD) AND UPPER(QTY.SQI_CD)='F_M_AMT'   ");
			stringBuilder.append(" AND VAL.LANGUAGE_CD = :langCode AND TBL5.STATUS_COD = :initStatus ");

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(), "LANGUAGE_CD");			
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_DATA_LN_BCL  (BILL_ID, BSEG_ID,...", paramsList);
		}

		private void insertIntoBsegSQIInfo() {

			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();
			
			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY ");
			stringBuilder.append("(BILL_ID, BSEG_ID, SQI_CD, SVC_QTY, SQI_DESCR, ");
			stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG,  EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
			stringBuilder.append(" SELECT TBL5.BILL_ID, TBL5.BSEG_ID, UPPER(QTY.SQI_CD) AS SQI_CD, QTY.SVC_QTY, ");
			stringBuilder.append("  L.DESCR, SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT, 'Y' AS ILM_ARCH_SW ");
			stringBuilder.append(" FROM CM_INV_BSEG_PROD_INFO TBL5,  CM_BILLABLE_ITEM_SERVICE_QTY QTY ,CM_INV_SQI_MAP SM, CI_SQI_L L  ");
			stringBuilder.append(" WHERE TBL5.BILLABLE_CHG_ID = QTY.BILLABLE_CHARGE_ID AND TBL5.BILLABLE_CHG_ID!=' ' ");
			stringBuilder.append(" AND UPPER(QTY.SQI_CD)=TRIM(SM.SQI_CD) AND TBL5.STATUS_COD= :initStatus  AND SM.SQI_CD = L.SQI_CD AND L.LANGUAGE_CD = :languageCode  ");
			stringBuilder.append(" AND 1 = CASE WHEN ");
			stringBuilder.append(" RULE_CRITERIA = '<>' AND QTY.SVC_QTY = SM.RULE_VALUE THEN 0 ELSE 1 END ");

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");		
			addParams(paramsList, "languageCode", invoiceDataInterfaceLookUp.getLanguageCode(),	"LANGUAGE_CD");			
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_BSEG_SQI_INFO (BILL_ID, BSEG_ID,...", paramsList);

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			paramsList = null;
			paramsList = new ArrayList<>();
			/**
			 * For bill segments retrieved in CM_INV_BSEG_PROD_INFO ,Fetch service quantity
			 * information for bill segments coming from Billable charge Upload interface.
			 * Fetch bill identifier,bill segment identifier,calculation
			 * amount,currency,characteristics type code as service quantity identifier
			 * code, adhoc character value as service quantity into CM_INV_BSEG_SQI_INFO
			 */
			// Join with CM_INV_SQI_MAP table containing expected SQIs
			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY ");
			stringBuilder.append("(BILL_ID, BSEG_ID, SQI_CD, SVC_QTY, SQI_DESCR, ");
			stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG,  EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
			stringBuilder.append("SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, SQ.CHAR_TYPE_CD AS SQI_CD, ");
			stringBuilder.append("SQ.ADHOC_CHAR_VAL AS SVC_QTY, L.DESCR, SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT, 'Y' AS ILM_ARCH_SW ");
			stringBuilder.append("FROM CI_BSEG_CL_CHAR SQ, CM_INV_BSEG_PROD_INFO TBL5, CM_INV_SQI_MAP SM, CI_SQI_L L  ");
			stringBuilder.append("WHERE SQ.CHAR_TYPE_CD=SM.SQI_CD AND SQ.BSEG_ID =TBL5.BSEG_ID AND SQ.CHAR_TYPE_CD=L.SQI_CD AND L.LANGUAGE_CD = :languageCode ");
			stringBuilder.append("AND TBL5.STATUS_COD = :initStatus ");
			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "languageCode", invoiceDataInterfaceLookUp.getLanguageCode(),	"LANGUAGE_CD");			
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_BSEG_SQI_INFO (BILL_ID, BSEG_ID,...", paramsList);

		}

		private void insertIntoBsegRateInfo() {

			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();

			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_RATE ");
			stringBuilder.append("(BILL_ID, BSEG_ID, RATE_TP, RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, PRICE_ASGN_ID) ");
			stringBuilder.append("SELECT DISTINCT TBL15.BILL_ID, TBL15.BSEG_ID, RCCHAR.CHAR_VAL AS RATE_TP, ");
			stringBuilder.append("NVL((SELECT TO_NUMBER(PC.SRCH_CHAR_VAL) FROM CI_PRICEASGN_CHAR PC ");
			stringBuilder.append("WHERE COMP.PRICE_ASGN_ID =PC.PRICE_ASGN_ID AND PC.CHAR_TYPE_CD = :charTypeCd),COMP.VALUE_AMT) AS RATE, ");
			stringBuilder.append("SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL15.ILM_DT, 'Y' AS ILM_ARCH_SW, COMP.PRICE_ASGN_ID ");
			stringBuilder.append("FROM CI_RC_MAP RCMAP, CI_RC_CHAR RCCHAR, CI_PRICECOMP COMP, CM_INV_BSEG_PROD_INFO TBL15 ");
			stringBuilder.append("WHERE RCMAP.RC_SEQ = RCCHAR.RC_SEQ AND RCCHAR.CHAR_TYPE_CD= :rateType ");
			stringBuilder.append("AND RCMAP.RS_CD = RCCHAR.RS_CD AND RCMAP.RC_MAP_ID = COMP.RC_MAP_ID ");
			stringBuilder.append("AND TBL15.PRICE_ASGN_ID = COMP.PRICE_ASGN_ID AND RCMAP.TIERED_FLAG ='FLAT' ");
			stringBuilder.append("AND TBL15.STATUS_COD = :initStatus AND RCCHAR.CHAR_VAL not in ('ASF_PC','REB_PC','MSC_PC') ");

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "rateType", invoiceDataInterfaceLookUp.getRateTpCharType().trim(), "CHAR_TYPE_CD");
			addParams(paramsList, "charTypeCd", new CharacteristicType_Id("CMVALAMT"), " ");		
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_DATA_LN_RATE (BILL_ID, BSEG_ID,...", paramsList);

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			paramsList = null;
			paramsList = new ArrayList<>();

			/**
			 * For bill segment retrieved in CM_INV_BSEG_PROD_INFO ,Fetch Rate info for bill
			 * segments whose rate type characteristics value is of %PC% type. Here we are
			 * multiplying rate with service quantity of rate signage service quantity
			 * identifier.Fetch bill identifier, bill segment identifier,rate type,rate
			 * schedule code,rate,price component identifier,rate component mapping
			 * identifier & tiered flag into CM_INV_DATA_LN_RATE
			 */

			stringBuilder.append("INSERT INTO CM_INV_DATA_LN_RATE ");
			stringBuilder.append("(BILL_ID, BSEG_ID, RATE_TP, RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, PRICE_ASGN_ID) ");
			stringBuilder.append("SELECT DISTINCT TBL15.BILL_ID, TBL15.BSEG_ID, RCCHAR.CHAR_VAL AS RATE_TP, NVL(COMP.VALUE_AMT*TBL15.UDF_NBR_19,COMP.VALUE_AMT) AS RATE, ");
			stringBuilder.append(" SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL15.ILM_DT, 'Y' AS ILM_ARCH_SW, COMP.PRICE_ASGN_ID ");
			stringBuilder.append("FROM CI_RC_MAP RCMAP, CI_RC_CHAR RCCHAR, CI_PRICECOMP COMP, CM_INV_BSEG_PROD_INFO TBL15 ");
			stringBuilder.append("WHERE RCMAP.RC_SEQ = RCCHAR.RC_SEQ AND RCCHAR.CHAR_TYPE_CD= :rateType ");
			stringBuilder.append("AND RCMAP.RS_CD = RCCHAR.RS_CD AND RCMAP.RC_MAP_ID = COMP.RC_MAP_ID ");
			stringBuilder.append("AND TBL15.PRICE_ASGN_ID = COMP.PRICE_ASGN_ID AND RCMAP.TIERED_FLAG ='FLAT' ");
			stringBuilder.append("AND TBL15.STATUS_COD = :initStatus AND RCCHAR.CHAR_VAL in ('ASF_PC','REB_PC','MSC_PC') ");
			// stringBuilder.append("AND TBL15.BILLABLE_CHG_ID = TXM.BILLABLE_CHG_ID ");
			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "rateType", invoiceDataInterfaceLookUp.getRateTpCharType().trim(), "CHAR_TYPE_CD");		
			addParams(paramsList, "extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_DATA_LN_RATE (BILL_ID, BSEG_ID,...", paramsList);

		}

		public ArrayList<ArrayList<Object>> addParams(ArrayList<ArrayList<Object>> arrayList, String arg0, Object arg1,
													  String arg2) {
			ArrayList<Object> list = new ArrayList<>();
			list.add(arg0);
			list.add(arg1);
			list.add(arg2);
			arrayList.add(list);
			return arrayList;
		}

		public void executeQuery(StringBuilder query, String message, ArrayList<ArrayList<Object>> list) {
			logger.debug("Executing query: " + message);
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement(query.toString(), "");
				for (ArrayList<Object> objList : list) {
					if (objList != null) {
						if (objList.get(1) instanceof String) {
							preparedStatement.bindString(objList.get(0) + "", objList.get(1) + "", objList.get(2) + "");
						} else if (objList.get(1) instanceof DateTime) {
							preparedStatement.bindDateTime(objList.get(0) + "", (DateTime) objList.get(1));
						} else if (objList.get(1) instanceof BigInteger) {
							preparedStatement.bindBigInteger(objList.get(0) + "", (BigInteger) objList.get(1));
						} else if (objList.get(1) instanceof BigDecimal) {
							preparedStatement.bindBigDecimal(objList.get(0) + "", (BigDecimal) objList.get(1));
						} else if (objList.get(1) instanceof Date) {
							preparedStatement.bindDate(objList.get(0) + "", (Date) objList.get(1));
						} else if (objList.get(1) instanceof CharacteristicType_Id) {
							preparedStatement.bindId(objList.get(0) + "", (CharacteristicType_Id) objList.get(1));
						} else if ("NullableDateTime".equalsIgnoreCase(objList.get(2) + "")) {

							preparedStatement.bindDateTime(objList.get(0) + "", (DateTime) objList.get(1));
						}
					}
				}
				int count = preparedStatement.executeUpdate();
				logger.debug("Rows inserted by query " + message + " are: " + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(" Error occurred while inserting records in Table/s in executeQuery() method - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table/s in executeQuery() method  - "  + e.toString()));
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		public void commit() {
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement(" COMMIT", "");
				preparedStatement.execute();
			} catch (RuntimeException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occurred while committing records - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}
		}

		public void closeConnection(PreparedStatement preparedStatement) {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		public void insertIntoInvoiceStagingTables() {

			/**
			 * Insert bill segment information from CM_INV_BSEG_PROD_INFO table into
			 * CM_INVOICE_DATA_LN for all the bills which do not exists into CM_INV_DATA_ERR
			 * table
			 */
			// Added Funding Currency column
			insertInvoiceLines();

			
			/**
			 * For bill segment retrieved in CM_INV_BSEG_PROD_INFO,Fetch bill segments which
			 * have bill calculation line type info. Fetch bill identifier,bill segment
			 * identifier,account type,bill calculation line type,distribution identifier,
			 * bill calculation line type description,bill segment line amount,printing
			 * witch,bill segment sequence number into CM_INV_BSEG_BCL_INFO This query will
			 * fetch BCL Types for charging accounts
			 */

			insertIntoBsegBclForChrg();

			/**
			 * This query will fetch BCL Types for other accounts except charging
			 */

			insertIntoBsegBcl();
			
			/**
			 * Insert service quantity information for bill segments coming from TFM or
			 * billable charge upload interface into CM_INV_DATA_LN_SVC_QTY for all the
			 * bills which do not exists in CM_INV_DATA_ERR table
			 */
			// Removed currency column

			insertInvoiceSvcQty();


			/**
			 * Insert rate information for all the bill segments present in
			 * CM_INV_BSEG_RATE_INFO table into CM_INV_DATA_LN_RATE table for all the bills
			 * which do not exists in CM_INV_DATA_ERR table.
			 */

			insertInvoiceRate();
			

			/**
			 * For bill segment retrieved in CM_INV_BSEG_PROD_INFO ,Fetch service quantity
			 * information for bill segments coming from TFM. This will not have information
			 * for F_M_AMT SQI because F_M_AMT SQI won't be present in CI_TXN_SQ Fetch bill
			 * identifier,bill segment identifier,calculation amount,transaction
			 * currency,service quantity identifier code,service quantity into
			 * CM_INV_BSEG_SQI_INFO
			 */

			insertIntoBsegSQIInfo();

			/**
			 * For bill segment retrieved in CM_INV_BSEG_PROD_INFO ,Fetch Rate info for bill
			 * segments whose rate type characteristics value is not of %PC% type. Fetch
			 * bill identifier,bill segment identifier,rate type,rate schedule
			 * code,rate,price component identifier,rate component mapping identifier &
			 * tiered flag into CM_INV_BSEG_RATE_INFO
			 */

			insertIntoBsegRateInfo();


			commit();

		}

		public void insertInvoiceRate() {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			
			/**
			 * Insert rate information for all the bill segments present in
			 * CM_INV_BSEG_SQI_INFO table into CM_INV_DATA_LN_RATE table for all the bills
			 * which do not exists in CM_INV_DATA_ERR table.
			 */

			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_RATE  ");
				stringBuilder.append(
						"(BILL_ID, BSEG_ID, RATE_TP, RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, PRICE_ASGN_ID)  ");
				stringBuilder.append("SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, 'M_PI'  AS RATE_TP,  ");
				stringBuilder.append(
						"CASE WHEN SQ.CHAR_TYPE_CD='TXN_VOL' THEN (TBL5.CALC_AMT/to_number(SQ.ADHOC_CHAR_VAL)) ELSE to_number(SQ.ADHOC_CHAR_VAL) END AS RATE,   ");
				stringBuilder.append(
						"SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT AS ILM_DT, 'Y' AS ILM_ARCH_SW, TBL5.PRICE_ASGN_ID ");
				stringBuilder.append("FROM CI_BSEG_CL_CHAR SQ, CM_INV_BSEG_PROD_INFO TBL5  ");
				stringBuilder
						.append("WHERE SQ.CHAR_TYPE_CD in ('TXN_VOL','RECRRATE') AND TBL5.STATUS_COD= :initStatus  ");
				stringBuilder.append(
						"AND SQ.BSEG_ID =TBL5.BSEG_ID AND (TBL5.PRICE_ASGN_ID=' ' OR TBL5.PRICE_ASGN_ID IS NULL) ");
				stringBuilder
						.append("AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL5.BILL_ID)  ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error(
						"Exception in executeWorkUnit() : Query 2 for CM_INVOICE_DATA_LN_RATE data insertion : Tier(MSC_PC) - ",
						e);
				errMsg = "CM_INV_DATA_LN_RATE: violated";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INVOICE_DATA_LN_RATE - " + e.toString()));
			} catch (Exception e) {
				errMsg = "CM_INV_DATA_LN_RATE: violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INVOICE_DATA_LN_RATE - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}
			stringBuilder = null;

		}

		public void insertInvoiceSvcQty() {

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;


			/**
			 * Insert service quantity information for bill segments of minimum charge type
			 * into CM_INV_DATA_LN_SVC_QTY for all the bills which do not exists in
			 * CM_INV_DATA_ERR table
			 */
			// Removed currency column
			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY ");
				stringBuilder.append("(BILL_ID, BSEG_ID, SQI_CD, SVC_QTY, SQI_DESCR, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
				stringBuilder.append(
						"SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, :txnVol AS SQI_CD, 1 AS SVC_QTY, L.DESCR, ");
				stringBuilder.append(
						"SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag as EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT AS ILM_DT, 'Y' AS ILM_ARCH_SW ");
				stringBuilder.append("FROM CM_INV_BSEG_PROD_INFO TBL5, CI_SQI_L L ");
				stringBuilder.append(
						"WHERE TBL5.BILLABLE_CHG_ID=' ' AND TBL5.STATUS_COD= :initStatus AND L.SQI_CD = :txnVol ");
				stringBuilder.append(
						"AND L.LANGUAGE_CD = :languageCode AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL5.BILL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.bindString("languageCode", invoiceDataInterfaceLookUp.getLanguageCode(),
						"LANGUAGE_CD");
				preparedStatement.bindString("txnVol", invoiceDataInterfaceLookUp.getTxnVol().trim(), "SQI_CD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error(
						"Exception in executeWorkUnit() : Query 2 for CM_INV_DATA_LN_SVC_QTY data insertion :Billable Charge - ",
						e);
				errMsg = "CM_INV_DATA_LN_SVC_QTY: violated";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_DATA_LN_SVC_QTY - " + e.toString()));
			} catch (Exception e) {
				errMsg = "CM_INV_DATA_LN_SVC_QTY: violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_DATA_LN_SVC_QTY - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}

			stringBuilder = null;

		}


		public void insertInvoiceLines() {

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try {
				stringBuilder.append("INSERT INTO CM_INVOICE_DATA_LN ");
				stringBuilder.append("(BILL_ID, BSEG_ID, BILLING_PARTY_ID, PRICE_CATEGORY, CURRENCY_CD, ");
				stringBuilder.append(
						"PRICE_CATEGORY_DESCR, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, UDF_CHAR_25,BILLABLE_CHG_ID,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT DISTINCT TBL5.BILL_ID, TBL5.BSEG_ID, TBL5.BILLING_PARTY_ID, ");
				stringBuilder.append("TBL5.PRICE_CATEGORY, TBL5.PRICE_CURRENCY_CD, TBL5.PRICE_CATEGORY_DESCR, ");
				stringBuilder.append(
						"SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, TBL5.ILM_DT AS ILM_DT, 'Y' AS ILM_ARCH_SW, ");
				stringBuilder.append(
						"CASE WHEN TBL5.UDF_CHAR_25=' ' THEN TBL5.CURRENCY_CD ELSE TBL5.UDF_CHAR_25 END AS UDF_CHAR_25,TBL5.BILLABLE_CHG_ID,TBL5.SETT_LEVEL_GRANULARITY ");
				stringBuilder.append("FROM  CM_INV_BSEG_PROD_INFO TBL5 ");
				stringBuilder.append(
						"WHERE TBL5.STATUS_COD= :initStatus AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL5.BILL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				errMsg = "CM_INVOICE_DATA_LN : violated";
				logger.error("Exception in executeWorkUnit() : Query  for CM_INVOICE_DATA_LN data insertion - ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INVOICE_DATA_LN - " + e.toString()));

			} catch (Exception e) {
				errMsg = "CM_INVOICE_DATA_LN : violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INVOICE_DATA_LN - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}
		}

		@Override
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			super.finalizeThreadWork();
		}

	}

	public static final class InvoiceBseg_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String invLowBsegId;
		private String invHighBsegId;

		public InvoiceBseg_Id(String invLowBsegId, String invHighBsegId) {
			setInvLowBsegId(invLowBsegId);
			setInvHighBsegId(invHighBsegId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getInvLowBsegId() {
			return invLowBsegId;
		}

		public void setInvLowBsegId(String invLowBsegId) {
			this.invLowBsegId = invLowBsegId;
		}

		public String getInvHighBsegId() {
			return invHighBsegId;
		}

		public void setInvHighBsegId(String invHighBsegId) {
			this.invHighBsegId = invHighBsegId;
		}

	}

}
