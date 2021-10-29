/*******************************************************************************
 * FileName                   : EventPriceRevisedStepsInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Sep 15, 2017
 * Version Number             : 1.9
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Sep 15, 2017        Vienna Rom    Initial version. Steps redesign.
0.2      NA             Oct 19, 2017        Vienna Rom    PAM-15894 chunking query perf fix
0.3      NA             Nov 22, 2017        Vienna Rom    PAM-16070 added indexes, perf fix
0.4      NA             Feb 02, 2018        Vienna Rom    NAP-22269,NAP-22270 populate ILM_DT and ILM_ARCH_SW
0.5      NA             Feb 07, 2018        Vienna Rom    NAP-22526 reduce final lines inserted
0.6      NA             Mar 13, 2018        Vienna Rom    NAP-22269,NAP-22270 ILM_ARCH_SW must be Y
0.7      NA             Mar 29, 2018        Ankur Jain    NAP-25173 Fix to fetch correct account for multiple derivaion account logic
0.8      NA             Apr 06, 2018        Ankur Jain    PAM-17673 Wrong Pricing currency issue
0.9      NA             Apr 09, 2018        Ankur Jain    NAP-25484 replace CM_BILL_CYC_SCH with CI_BILL_CYC_SCH
1.0      NA             Apr 13, 2018        Ankur Jain    NAP-24476 & NAP-24649 fix
1.1      NA             May 01, 2018        Ankur Jain    NAP-26385 event price 0/-1 calc_amt issue fix & NAP-26526 & NAP-26537 changed to complete_dttm
1.2      NA             May 02, 2018        Ankur Jain    NAP-26684 used SYSTIMESTAMP in place of TRUNC(SYSTIMESTAMP)
1.3      NA             May 14, 2018        Ankur         NAP-27294 Fixed
1.4      NA             Jun 13, 2018        Ankur         NAP-26575 Fixed
1.5		 NA				Jul 02, 2018		Somya		  NAP-29759 Event Price Interface change to support accounting for Settlement Control/FX Income	
1.6		 NA				Jul 31, 2018		Rakesh		  NAP-29591 - Issue fixed
1.7 	 NA				Oct 15, 2018		Amandeep	  NAP-33633 - L0 Product Mapped from CM_TXN_ATTRIBUTES_MAP
1.8      NA             Dec 05, 2018        RIA           NAP-36537 - Fetch PARENT_ACCT_ID from CM_TXN_ATTRIBUTES_MAP
1.9		 NA				Dec 18, 2018		Vikalp		  NAP-37818 - Duplicate Records truncated in EPRCI(Prod Issue)
2.0 	 NA				Feb 21, 2019		RIA		      NAP-39322 - Implement Hummingbbird Changes
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (multiThreaded = true, rerunnable = false, 
 *     modules = {"demo"},
 *     softParameters = { @BatchJobSoftParameter (name = billDate, type = date)
 *      , @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *      , @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 */
public class EventPriceRevisedStepsInterface extends EventPriceRevisedStepsInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(EventPriceRevisedStepsInterface.class);
	private static final EventPriceDataInterfaceLookUp eventPriceDataInterfaceLookUp = new EventPriceDataInterfaceLookUp();

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {

		//Truncate error table only. Global temp tables are automatically cleared on commit.
		deleteFromEventPriceTmpTable(eventPriceDataInterfaceLookUp.getTempTable6().trim());
		
		List<ThreadWorkUnit> threadWorkUnitList = getEventPriceData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * deleteFromEventPriceTmpTable() method will delete from the table provided as
	 * input.
	 * 
	 * @param inputEventPriceTmpTable
	 */
	private void deleteFromEventPriceTmpTable(String inputEventPriceTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("DELETE FROM "+ inputEventPriceTmpTable, "");
			preparedStatement.execute();
						
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteFromEventPriceTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside deleteFromEventPriceTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
			}

	/**
	 * getEventPriceData() method selects Event Price IDs for processing by this Interface.
	 * 
	 * @return List Event_Price_Id
	 */
	private List<ThreadWorkUnit> getEventPriceData() {
		
		//Retrieve batch parameters
		BigInteger chunkSize = getParameters().getCustomThreadCount();
		DateTime ilmDateTime = getSystemDateTime();
		Date intialDate = null;
		//Date intialDate = getProcessDateTime().getDate();
  		Date billDate  = getParameters().getBillDate();
		//NAP-30094
		if(notNull(billDate)){
			intialDate = billDate;
		}
		else{
			intialDate = getProcessDateTime().getDate();
		}
		
		Date finalDate = intialDate.addDays(1);
		String txnSourceCode = isNull(getParameters().getTxnSourceCode()) ? "" : getParameters().getTxnSourceCode().trim();
		
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		String lowEventPriceId = "";
		String highEventPriceId = "";
		EventPriceRevisedStepsData_Id eventPriceData = null;		

		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();

		/* Chunking query
		 * Subquery gets event ids using the same conditions for populating first temp table in executeWorkUnit.
		 * From these event ids, the min and max ids of each chunk (work unit) are determined based on the row number.
		 */
		try {
	      	if(isNull(billDate))
			{
			stringBuilder.append("WITH TBL AS ( ");
			stringBuilder.append("	SELECT DISTINCT B.EXT_TXN_NBR AS EVENT_ID  ");
			stringBuilder.append("	FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION MAP ");
			stringBuilder.append("	WHERE B.TXN_DETAIL_ID = MAP.ORG_TXN_ID ");
			stringBuilder.append("	AND MAP.SUMMARY_ID=A.TXN_DETAIL_ID ");
			stringBuilder.append("	AND A.BO_STATUS_CD='COMP' ");
			stringBuilder.append("	AND MAP.BO_STATUS_CD='COMP' ");
			stringBuilder.append("	AND A.CURR_SYS_PRCS_DT=:processDate ");
			if(notBlank(txnSourceCode)) {
				stringBuilder.append("	AND A.TXN_SOURCE_CD = :txnSourceCode ");
			}
			stringBuilder.append("	AND NOT EXISTS ( SELECT 1 FROM CM_EVENT_PRICE X ");
			stringBuilder.append("	                WHERE B.EXT_TXN_NBR = X.EVENT_ID  ");
			stringBuilder.append("	                AND SUBSTR(MAP.ACCT_NBR,14,4) = X.ACCT_TYPE  ");
			stringBuilder.append("	                AND X.ILM_DT >=:processDate ");
			stringBuilder.append("	                AND X.CREDIT_NOTE_FLG='N')  ");
			stringBuilder.append("	GROUP BY B.EXT_TXN_NBR ORDER BY B.EXT_TXN_NBR)  ");
			stringBuilder.append("SELECT THREAD_NUM, MIN(EVENT_ID) AS LOW_EVENT_PRICE_ID, MAX(EVENT_ID) AS HIGH_EVENT_PRICE_ID  ");
			stringBuilder.append("FROM (SELECT EVENT_ID, CEIL(ROWNUM/:chunkSize) AS THREAD_NUM FROM TBL)  ");
			stringBuilder.append("GROUP BY THREAD_NUM ORDER BY 1 ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			if(notBlank(txnSourceCode)){
				preparedStatement.bindString("txnSourceCode", txnSourceCode, "TXN_SOURCE_CD");
			}
			preparedStatement.bindDate("processDate", intialDate);
			//preparedStatement.bindDate("intialDate", intialDate);
			//preparedStatement.bindDate("finalDate", finalDate);
			preparedStatement.bindBigInteger("chunkSize", chunkSize);
			preparedStatement.setAutoclose(false);
	}
			else
			{
				stringBuilder.append("WITH TBL AS ( ");
				stringBuilder.append("	SELECT DISTINCT B.EXT_TXN_NBR AS EVENT_ID  ");
				stringBuilder.append("	FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION MAP ");
				stringBuilder.append("	WHERE B.TXN_DETAIL_ID = MAP.ORG_TXN_ID ");
				stringBuilder.append("	AND MAP.SUMMARY_ID=A.TXN_DETAIL_ID ");
				stringBuilder.append("	AND A.BO_STATUS_CD='COMP' ");
				stringBuilder.append("	AND MAP.BO_STATUS_CD='COMP' ");
				stringBuilder.append("	AND A.CURR_SYS_PRCS_DT=:processDate ");
				if(notBlank(txnSourceCode)) {
					stringBuilder.append("	AND A.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append("	AND NOT EXISTS ( SELECT 1 FROM CM_EVENT_PRICE X ");
				stringBuilder.append("	                WHERE B.EXT_TXN_NBR = X.EVENT_ID  ");
				stringBuilder.append("	                AND SUBSTR(MAP.ACCT_NBR,14,4) = X.ACCT_TYPE  ");
				stringBuilder.append("	                AND X.ILM_DT >=:processDate ");
				stringBuilder.append("	                AND X.CREDIT_NOTE_FLG='N')  ");
				stringBuilder.append("	GROUP BY B.EXT_TXN_NBR ORDER BY B.EXT_TXN_NBR)  ");
				stringBuilder.append("SELECT THREAD_NUM, MIN(EVENT_ID) AS LOW_EVENT_PRICE_ID, MAX(EVENT_ID) AS HIGH_EVENT_PRICE_ID  ");
				stringBuilder.append("FROM (SELECT EVENT_ID, CEIL(ROWNUM/:chunkSize) AS THREAD_NUM FROM TBL)  ");
				stringBuilder.append("GROUP BY THREAD_NUM ORDER BY 1 ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				if(notBlank(txnSourceCode)){
					preparedStatement.bindString("txnSourceCode", txnSourceCode, "TXN_SOURCE_CD");
				}
				preparedStatement.bindDate("processDate", intialDate);
				//preparedStatement.bindDate("finalDate", finalDate);
				preparedStatement.bindBigInteger("chunkSize", chunkSize);
				preparedStatement.setAutoclose(false);
			}

			for (SQLResultRow sqlRow : preparedStatement.list()) {
				lowEventPriceId = sqlRow.getString("LOW_EVENT_PRICE_ID");
				highEventPriceId = sqlRow.getString("HIGH_EVENT_PRICE_ID");
				eventPriceData = new EventPriceRevisedStepsData_Id(lowEventPriceId, highEventPriceId);
				
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(eventPriceData);
				threadworkUnit.addSupplementalData("ilmDateTime", ilmDateTime);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				eventPriceData = null;
			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getEventPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
				.exceptionInExecution("Error occurred in getJobWork() while fetching Event Ids" + e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getEventPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("getJobWork():fetching Event Ids"));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}	

		return threadWorkUnitList;
	}

	public Class<EventPriceRevisedStepsInterfaceWorker> getThreadWorkerClass() {
		return EventPriceRevisedStepsInterfaceWorker.class;
	}

	public static class EventPriceRevisedStepsInterfaceWorker extends
	EventPriceRevisedStepsInterfaceWorker_Gen {

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			String lowEventPriceId = "";
			String highEventPriceId = "";
			PreparedStatement preparedStatement = null;
			PreparedStatement preparedStatement2 = null;
			StringBuilder stringBuilder = new StringBuilder();

			EventPriceRevisedStepsData_Id eventPriceData = (EventPriceRevisedStepsData_Id) unit.getPrimaryId();
			lowEventPriceId = eventPriceData.getLowEventPriceId();
			highEventPriceId = eventPriceData.getHighEventPriceId();
			DateTime ilmDateTime = (DateTime)unit.getSupplementallData("ilmDateTime"); 
			//Date intialDate = getProcessDateTime().getDate();
			Date intialDate = getParameters().getBillDate();
			if(isNull(intialDate)){
				intialDate = getProcessDateTime().getDate();
			}
			Date finalDate = intialDate.addDays(1);

			String txnSourceCode = isNull(getParameters().getTxnSourceCode()) ? "" : getParameters().getTxnSourceCode().trim();
			
			//Variable for tiering processing
			int tierRowsCount = 0;
			
			try {
				/* 
				 * Select all completed transaction details of events (funds and charges) for the process date 
				 * that do not have tiered txn legs or previously extracted. Store them in CM_EVENT_TXN_DETAIL.
				 * Note: the query has been modified to assume that non-tiered data are always daily rated.
				 */
				stringBuilder.append("INSERT INTO CM_EVENT_TXN_DETAIL (EVENT_ID, CREDIT_NOTE_FLG, ACCRUED_DATE,UDF_CHAR_11,UDF_CHAR_12, STATUS_CD,TXN_DETAIL_ID,PARENT_DETAIL_ID, UDF_CHAR_3, CURR_SYS_PRCS_DT) "); //DOM CHANGE
				stringBuilder.append("SELECT  /*+ LEADING (C, B) USE_HASH (C B)  USE_HASH (C A) */ DISTINCT B.EXT_TXN_NBR AS EVENT_ID,'N',A.PROCESSING_DT,A.UDF_CHAR_11,A.UDF_CHAR_12,:initialStatus AS STATUS_CD,B.TXN_DETAIL_ID,A.TXN_DETAIL_ID, A.UDF_CHAR_3, A.CURR_SYS_PRCS_DT-1 as CURR_SYS_PRCS_DT "); //DOM CHANGE
				stringBuilder.append("FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION C ");
				stringBuilder.append(" WHERE C.BO_STATUS_CD     ='COMP' AND A.BO_STATUS_CD='COMP' AND B.TXN_DETAIL_ID=C.ORG_TXN_ID AND A.TXN_DETAIL_ID=C.SUMMARY_ID ");
				if(notBlank(txnSourceCode)) {
					stringBuilder.append("AND B.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append("AND A.CURR_SYS_PRCS_DT = :processDate ");
				stringBuilder.append("AND exists (SELECT 1 FROM CI_TXN_DTL_PRITM C WHERE C.TXN_DETAIL_ID = B.TXN_DETAIL_ID ");
				stringBuilder.append("AND C.BILLABLE_CHG_ID <> ' '  AND C.TXN_RATING_CRITERIA<>'DNRT' AND C.CURR_SYS_PRCS_DT = :processDate) ");
				stringBuilder.append("AND NOT exists(SELECT 1 FROM CM_EVENT_PRICE X WHERE X.EVENT_ID = B.EXT_TXN_NBR ");
				stringBuilder.append("AND SUBSTR(C.ACCT_NBR,14,4) = X.ACCT_TYPE AND X.ILM_DT >=:processDate AND X.CREDIT_NOTE_FLG  ='N') ");
				stringBuilder.append("AND B.EXT_TXN_NBR BETWEEN trim(:lowId) AND trim(:highId) ");
				
				stringBuilder.append("UNION ");
				
				stringBuilder.append("SELECT /*+ LEADING (A C B) */ DISTINCT B.EXT_TXN_NBR AS EVENT_ID,'N',A.PROCESSING_DT,A.UDF_CHAR_11,A.UDF_CHAR_12,:initialStatus AS STATUS_CD,B.TXN_DETAIL_ID,A.TXN_DETAIL_ID, A.UDF_CHAR_3, A.CURR_SYS_PRCS_DT-1  as CURR_SYS_PRCS_DT "); //DOM CHANGE
				stringBuilder.append("FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION C ");
				stringBuilder.append("WHERE C.BO_STATUS_CD ='COMP' AND A.BO_STATUS_CD='COMP' and B.TXN_DETAIL_ID=C.ORG_TXN_ID AND A.TXN_DETAIL_ID=C.SUMMARY_ID AND A.CURR_SYS_PRCS_DT = :processDate ");
				if(notBlank(txnSourceCode)) {
					stringBuilder.append("AND B.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append("AND NOT exists(SELECT 1 FROM CM_EVENT_PRICE X WHERE X.EVENT_ID = B.EXT_TXN_NBR  ");
				stringBuilder.append("AND SUBSTR(C.ACCT_NBR,14,4) = X.ACCT_TYPE AND X.ILM_DT >=:processDate AND X.CREDIT_NOTE_FLG  ='N')  ");
				stringBuilder.append("AND B.EXT_TXN_NBR BETWEEN trim(:lowId) AND trim(:highId) ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindDate("processDate", intialDate);
				//preparedStatement.bindDate("intialDate", intialDate);
				//preparedStatement.bindDate("finalDate", finalDate);
				preparedStatement.bindString("lowId", lowEventPriceId, "EVENT_ID");
				preparedStatement.bindString("highId", highEventPriceId, "EVENT_ID");
				if(notBlank(txnSourceCode)) {
					preparedStatement.bindString("txnSourceCode",txnSourceCode, "TXN_SOURCE_CD");
				}
				preparedStatement.executeUpdate();

				/*stringBuilder = null;
				stringBuilder = new StringBuilder();

				//TIERING DATA EXTRACTION
				/*
				 * Select all completed transaction details of events (funds and charges) 
				 * with tiered txn legs billed on the process date. Store them in CM_EVENT_TXN_DETAIL.
				 
				stringBuilder.append("INSERT INTO CM_EVENT_TXN_DETAIL (EVENT_ID, CREDIT_NOTE_FLG, ACCRUED_DATE,UDF_CHAR_11,UDF_CHAR_12, STATUS_CD,TXN_DETAIL_ID) ");
				stringBuilder.append("SELECT DISTINCT B.EXT_TXN_NBR AS EVENT_ID,'N',A.PROCESSING_DT,NULL,NULL,'T' AS STATUS_CD,B.TXN_DETAIL_ID ");
				stringBuilder.append("FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION C ");
				stringBuilder.append(" WHERE B.BO_STATUS_CD     ='COMP' AND B.TXN_DETAIL_ID=C.ORG_TXN_ID AND A.TXN_DETAIL_ID=C.SUMMARY_ID ");
				if(notBlank(txnSourceCode)) {
					stringBuilder.append("AND B.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append("AND A.CURR_SYS_PRCS_DT = :processDate ");
				stringBuilder.append("AND exists (SELECT 1 FROM CI_TXN_DTL_PRITM C , CI_BSEG_CALC D, CI_BSEG E, CI_BILL F ");
				stringBuilder.append("WHERE C.TXN_DETAIL_ID = B.TXN_DETAIL_ID  AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID ");
				stringBuilder.append("AND C.BILLABLE_CHG_ID <> ' '  AND D.RS_CD LIKE 'TIE%' AND D.BSEG_ID=E.BSEG_ID  ");
				stringBuilder.append("AND E.BILL_ID=F.BILL_ID AND TRUNC(F.COMPLETE_DTTM) = :processDate ) ");
				stringBuilder.append("AND NOT exists(SELECT 1 FROM CM_EVENT_PRICE X WHERE X.EVENT_ID = A.EXT_TXN_NBR ");
				stringBuilder.append("AND X.ILM_DT >=:processDate AND X.CREDIT_NOTE_FLG  ='N') ");
				stringBuilder.append("AND A.EXT_TXN_NBR BETWEEN trim(:lowId) AND trim(:highId) ");
				
				/*stringBuilder.append("UNION ");
				
				stringBuilder.append("SELECT DISTINCT B.EXT_TXN_NBR AS EVENT_ID,'N',A.PROCESSING_DT,NULL,NULL,'T' AS STATUS_CD,B.TXN_DETAIL_ID ");
				stringBuilder.append("FROM CI_TXN_DETAIL A, CI_TXN_DETAIL_STG B, CM_TXN_INTERSECTION C ");
				stringBuilder.append("WHERE B.BO_STATUS_CD     ='COMP' and B.TXN_DETAIL_ID=C.ORG_TXN_ID AND A.TXN_DETAIL_ID=C.SUMMARY_ID AND A.CURR_SYS_PRCS_DT = :processDate ");
				if(notBlank(txnSourceCode)) {
					stringBuilder.append("AND B.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append("AND NOT exists(SELECT 1 FROM CM_EVENT_PRICE X WHERE X.EVENT_ID = A.EXT_TXN_NBR  ");
				stringBuilder.append("AND X.ILM_DT >=:processDate AND X.CREDIT_NOTE_FLG  ='N')  ");
				stringBuilder.append("AND A.EXT_TXN_NBR BETWEEN trim(:lowId) AND trim(:highId) ");
				preparedStatement2 = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement2.bindDate("processDate", getProcessDateTime().getDate());
				preparedStatement2.bindString("lowId", lowEventPriceId, "EVENT_ID");
				preparedStatement2.bindString("highId", highEventPriceId, "EVENT_ID");
				if(notBlank(txnSourceCode)) {
					preparedStatement2.bindString("txnSourceCode",txnSourceCode.trim(), "TXN_SOURCE_CD");
				}
				tierRowsCount = preparedStatement2.executeUpdate();*/

			} catch (ThreadAbortedException e) {
				logger.error("GetEventPriceData error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("GetEventPriceData error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_TXN_DETAIL:insert error"));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
//				if (preparedStatement2 != null) {
//					preparedStatement2.close();
//					preparedStatement2 = null;
//				}
			}

			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/*
			 * Select transaction calculation id and priceitem code of transaction details in CM_EVENT_TXN_DETAIL. 
			 * Records are stored into CM_EVENT_TXN_CALC. 
			 */
			try {
				stringBuilder.append(" INSERT INTO CM_EVENT_TXN_CALC ");
				stringBuilder.append(" (EVENT_ID, TXN_CALC_ID, PRICEITEM_CD, STATUS_CD, ACCT_ID, TXN_DTTM, ");
				stringBuilder.append(" CREDIT_NOTE_FLG,ACCRUED_DATE,UDF_CHAR_11,UDF_CHAR_12,UDF_CHAR_3,TXN_DETAIL_ID, SETT_LEVEL_GRANULARITY, GRANULARITY_REFERENCE) ");
				stringBuilder.append(" SELECT /*+ LEADING (A E TXMAP B) */  A.EVENT_ID,  "); 
				stringBuilder.append(" B.TXN_CALC_ID, NVL(TXMAP.CHILD_PRODUCT, trim(E.INITIAL_PRICE_ITEM_CD)) AS PRICEITEM_CD,  ");
				//stringBuilder.append("TRIM(B.INIT_PRICEITEM_CD), 
				//code changes made to incorporate the logic to handle parent level pricing
				//code changes for NAP-33633		
				stringBuilder.append(" (CASE WHEN TRIM(B.INIT_PRICEITEM_CD) IS NULL THEN :errorStatus ELSE :initialStatus END) AS STATUS_CD, ");
				stringBuilder.append("  TXMAP.PARENT_ACCT_ID AS ACCT_ID, (A.CURR_SYS_PRCS_DT), A.CREDIT_NOTE_FLG, ");
				stringBuilder.append(" A.ACCRUED_DATE,TRIM(A.UDF_CHAR_11),TRIM(A.UDF_CHAR_12), A.UDF_CHAR_3, A.TXN_DETAIL_ID, TXMAP.SETT_LEVEL_GRANULARITY, "); //DOM CHANGE
				//TODO: ADDED SETT_LEVEL_GRANULARITY FROM CM_TXN_ATTRIBUTES_MAP TABLE.	
				stringBuilder.append(" ORA_HASH(CONCAT(CONCAT(TRIM(TXMAP.CHILD_PRODUCT), TRIM(TXMAP.SETT_LEVEL_GRANULARITY)), TRIM(TXMAP.PRICE_ASGN_ID))) AS GRANULARITY_REFERENCE ");
				//stringBuilder.append(" FROM CM_EVENT_TXN_DETAIL A, CI_TXN_CALC B, CI_TXN_DETAIL D, CI_TXN_DTL_PRITM E, CM_TXN_ATTRIBUTES_MAP TXMAP, "); //DOM CHANGE
				stringBuilder.append(" FROM CM_EVENT_TXN_DETAIL A, CI_TXN_CALC B,  CI_TXN_DTL_PRITM E, CM_TXN_ATTRIBUTES_MAP TXMAP "); //DOM CHANGE
				//stringBuilder.append(" CI_TXN_DETAIL_STG F, CM_TXN_INTERSECTION G "); //DOM CHANGE
				//stringBuilder.append(" WHERE A.EVENT_ID=F.EXT_TXN_NBR "); //DOM CHANGE
				//stringBuilder.append(" AND A.TXN_DETAIL_ID=F.TXN_DETAIL_ID "); //DOM CHANGE
				//stringBuilder.append(" AND D.TXN_DETAIL_ID=E.TXN_DETAIL_ID "); //DOM CHANGE
				stringBuilder.append(" 	WHERE A.PARENT_DETAIL_ID=E.TXN_DETAIL_ID "); //DOM CHANGE
				stringBuilder.append(" AND E.BILLABLE_CHG_ID=TXMAP.BILLABLE_CHG_ID ");
				//stringBuilder.append(" AND D.TXN_DETAIL_ID = G.SUMMARY_ID ");//DOM CHANGE
				
				//stringBuilder.append(" AND G.ORG_TXN_ID=F.TXN_DETAIL_ID "); //DOM CHANGE
				stringBuilder.append(" AND E.TXN_CALC_ID = B.TXN_CALC_ID ");
				stringBuilder.append(" AND E.TXN_CALC_ID IS NOT NULL ");
				stringBuilder.append(" AND A.STATUS_CD=:initialStatus ");		
				//stringBuilder.append(" and D.BO_STATUS_CD='COMP' "); //DOM CHANGE
				//stringBuilder.append(" and D.CURR_SYS_PRCS_DT = :processDate  "); //DOM CHANGE
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				preparedStatement.bindString("errorStatus", eventPriceDataInterfaceLookUp.getErrorStatus().trim(), "STATUS_COD");
				//preparedStatement.bindDate("processDate", intialDate);
				
				preparedStatement.executeUpdate();
				} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			
			//TMP_32 MERGED TO TMP_3 INSERT
			/*
			 * For non-tiered data, retrieve aggregated calc line info and price category by selecting txn calc line 
			 * and bcl type of CM_EVENT_TXN_CALC records. Check that bcl type char value maps to an SQI. 
			 * Records are stored in CM_EVENT_CAT_AMOUNT. 
			 */
			
			//Change in the Query to Take EVT-PRC instead of BCL-TYPE RC char
			
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_EVENT_CAT_AMOUNT  ");
				stringBuilder.append(" (EVENT_ID, TXN_CALC_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, ");
				stringBuilder.append(" INVOICEABLE_FLG, CURRENCY_CD, STATUS_CD, ACCT_ID, TXN_DTTM, ");
				//COLUMN GRANULARITY_REFERENCE AND VALUE FETCHED FROM CM_EVENT_TXN_CALC
				stringBuilder.append(" CREDIT_NOTE_FLG,ACCRUED_DATE,UDF_CHAR_11,UDF_CHAR_12, UDF_CHAR_3,TXN_DETAIL_ID,SETT_LEVEL_GRANULARITY,CALC_PRECS_LENGTH, GRANULARITY_REFERENCE) ");
				stringBuilder.append(" SELECT A.EVENT_ID, A.TXN_CALC_ID, A.PRICEITEM_CD, CHR.CHAR_VAL, ");
				stringBuilder.append(" SUM(E.CALC_AMT), E.PRT_SW, ACCT.CURRENCY_CD, :initialStatus AS STATUS_CD, ");
				
				stringBuilder.append(" A.ACCT_ID, A.TXN_DTTM, A.CREDIT_NOTE_FLG,A.ACCRUED_DATE,A.UDF_CHAR_11,A.UDF_CHAR_12, A.UDF_CHAR_3,A.TXN_DETAIL_ID,A.SETT_LEVEL_GRANULARITY, ");
				stringBuilder.append(" CASE WHEN max(length(ABS(e.calc_amt)-TRUNC(ABS(e.calc_amt)))-1) > 5 THEN max(length(ABS(e.calc_amt)-TRUNC(ABS(e.calc_amt)))-1) ELSE 5 END, ");
				stringBuilder.append(" A.GRANULARITY_REFERENCE ");
				stringBuilder.append(" FROM  CM_EVENT_TXN_CALC A, CI_TXN_CALC_LN E,CI_TXN_CALC_LN_CHAR CHR , CM_BCL_SQI_TMP X, CI_TXN_CALC Y,CI_ACCT ACCT ");
				stringBuilder.append(" WHERE A.TXN_CALC_ID=E.TXN_CALC_ID ");
				stringBuilder.append(" AND E.TXN_CALC_ID=CHR.TXN_CALC_ID ");
				stringBuilder.append(" AND CHR.CHAR_VAL = X.BCL_TYPE ");
				stringBuilder.append(" AND E.SEQNO=CHR.SEQNO ");
				stringBuilder.append(" AND CHR.CHAR_TYPE_CD=:eprciTypeCode ");
				stringBuilder.append(" and CHR.CHAR_VAL = X.BCL_TYPE ");
				stringBuilder.append(" AND Y.RS_CD=X.RS_CD ");
				stringBuilder.append(" AND A.TXN_CALC_ID=Y.TXN_CALC_ID  ");
				//Added Change for NAP Defect-29759
				stringBuilder.append("AND A.ACCT_ID=ACCT.ACCT_ID AND A.STATUS_CD=:initialStatus ");					
				
				stringBuilder.append("GROUP BY A.EVENT_ID, A.TXN_CALC_ID, A.PRICEITEM_CD, CHR.CHAR_VAL, E.PRT_SW, ACCT.CURRENCY_CD, ");
				stringBuilder.append("STATUS_CD, A.ACCT_ID, A.TXN_DTTM, A.CREDIT_NOTE_FLG, A.ACCRUED_DATE, A.UDF_CHAR_11, A.UDF_CHAR_12, A.UDF_CHAR_3,A.TXN_DETAIL_ID, A.SETT_LEVEL_GRANULARITY, A.GRANULARITY_REFERENCE ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				preparedStatement.bindString("eprciTypeCode", eventPriceDataInterfaceLookUp.getEvtPriceCode().trim(), "CHAR_TYPE_CD");
				preparedStatement.executeUpdate();
			} 
			catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/*
			 * For non-tiered data, retrieve service quantity for the price category of lines 
			 * in CM_EVENT_CAT_AMOUNT and insert into CM_EVENT_QUANTITY.
			 */
			try {
				stringBuilder.append("INSERT INTO CM_EVENT_QUANTITY "); 
				stringBuilder.append("(EVENT_ID, PRICE_CATEGORY, TXN_VOL, TXN_CALC_ID, SQI_CD, TOTAL_VOL, OLD_CALC_AMT, ");
				stringBuilder.append("NEW_CALC_AMT, STATUS_CD, TXN_DETAIL_ID)  ");
				stringBuilder.append("SELECT TMP.EVENT_ID, TMP.PRICE_CATEGORY, NULL, TMP.TXN_CALC_ID, D.SQI_CD, D.SVC_QTY, "); 
				stringBuilder.append("TMP.CALC_AMT AS OLD_CALC_AMT, 0 AS NEW_CALC_AMT, :initialStatus , TMP.TXN_DETAIL_ID  ");
				stringBuilder.append("FROM CM_BCL_SQI_TMP E, CM_EVENT_CAT_AMOUNT TMP, CI_TXN_CALC F, CI_TXN_SQ D ");
				stringBuilder.append("WHERE E.RS_CD = F.RS_CD AND E.BCL_TYPE = TMP.PRICE_CATEGORY AND E.SQI_CD=D.SQI_CD ");
				stringBuilder.append("AND TMP.STATUS_CD=:initialStatus AND TMP.TXN_CALC_ID = F.TXN_CALC_ID ");
				stringBuilder.append("AND F.TXN_CALC_ID=D.TXN_CALC_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				preparedStatement.executeUpdate();
				} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_QUANTITY:insert error"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			/*
			 * Select column names that may be aggregated as transaction calc SQs and iterate through them.
			 */
			PreparedStatement preparedStatement1 = null;
			try {
				preparedStatement = createPreparedStatement("SELECT TRIM(TXN_PRM) AS COLUMN_NAME FROM CI_TXN_SQI_FRAG" +
						" ORDER BY TXN_PRM","");
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String columnName = CommonUtils.CheckNull(resultSet.getString("COLUMN_NAME"));
					
					stringBuilder = null;
					stringBuilder = new StringBuilder();
					/*
					 * For non-tiered data, update txn vol in CM_EVENT_QUANTITY 
					 * as the quantity contributed by the transaction detail.
					 */
					stringBuilder.append("UPDATE CM_EVENT_QUANTITY A  ");
					stringBuilder.append("SET A.TXN_VOL=(SELECT  STG." +columnName+ " FROM CI_TXN_DTL_PRITM PR, CI_TXN_DETAIL_STG STG, CM_TXN_INTERSECTION INTR ");
					stringBuilder.append("WHERE A.EVENT_ID=STG.EXT_TXN_NBR ");
					stringBuilder.append("AND PR.TXN_DETAIL_ID=INTR.SUMMARY_ID ");
					stringBuilder.append("AND INTR.ORG_TXN_ID = STG.TXN_DETAIL_ID ");
					stringBuilder.append("AND PR.TXN_CALC_ID=A.TXN_CALC_ID ");
					stringBuilder.append("AND STG.TXN_DETAIL_ID=A.TXN_DETAIL_ID) ");
					stringBuilder.append("WHERE exists (select 1 FROM CI_TXN_SQI_FRAG C ");
					stringBuilder.append("WHERE C.SQI_CD=A.SQI_CD AND C.TXN_PRM='" +columnName+"') ");
					stringBuilder.append("AND A.STATUS_CD=:initialStatus ");
					preparedStatement1 = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement1.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
					preparedStatement1.executeUpdate();
					
					if (preparedStatement1 != null) {
						preparedStatement1.close();
						preparedStatement1 = null;
					}
					
				}//FOR LOOP
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_QUANTITY:Update error"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}	
			
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();

			
			//MERGE OF PREVIOUS 2 STEPS PLUS NEGATE STEP
			/*
			 * Update new calc amt in CM_EVENT_CAT_AMOUNT as the negated amount contributed by the transaction detail.
			 */
			try {
				stringBuilder.append("UPDATE CM_EVENT_CAT_AMOUNT a ");
				stringBuilder.append("SET A.CALC_AMT=(SELECT (CASE WHEN TOTAL_VOL!=0 ");
				stringBuilder.append("                        THEN ((C.OLD_CALC_AMT/C.TOTAL_VOL)*C.TXN_VOL*-1) ");
				stringBuilder.append("                        ELSE C.NEW_CALC_AMT END) ");
				stringBuilder.append("					FROM CM_EVENT_QUANTITY c ");
				stringBuilder.append("					WHERE a.EVENT_ID=c.EVENT_ID AND a.PRICE_CATEGORY=c.PRICE_CATEGORY ");
				stringBuilder.append("					AND a.TXN_CALC_ID=c.TXN_CALC_ID AND C.TXN_DETAIL_ID=A.TXN_DETAIL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_CAT_AMOUNT:update error"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			//TMP4+TMP MERGE
			/*
			 * Extract bill reference and settlement granularity details of calc lines 
			 * together with calc amt (rounded to 8 d.p.) and extract flag 'Y'. Records are stored into CM_EVENT_BILL_GRANULARITY.
			 * BILL_REFERENCE = person id + account id + bill cycle code + window start date
			 * SETT_LEVEL_GRANULARITY = window end date <YYMMDD>.account id.ORA_HASH(udf chars 11,12,13) [first 30 chars] 
			 */
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_EVENT_BILL_GRANULARITY "); 
				stringBuilder.append(" (EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, INVOICEABLE_FLG, CURRENCY_CD, ACCT_TYPE, ");
				stringBuilder.append(" BILL_REFERENCE, CREDIT_NOTE_FLG, ACCRUED_DATE, UDF_CHAR_11, UDF_CHAR_12, PRICING_SEGMENT, ");
				stringBuilder.append(" TXN_DETAIL_ID, SETT_LEVEL_GRANULARITY, EXTRACT_FLG, EXTRACT_DTTM, GRANULARITY_REFERENCE)  ");
				stringBuilder.append(" SELECT TMP.EVENT_ID, TMP.PRICEITEM_CD,  ");
				stringBuilder.append(" TMP.PRICE_CATEGORY, ROUND(TMP.CALC_AMT, CALC_PRECS_LENGTH), TMP.INVOICEABLE_FLG, TMP.CURRENCY_CD, B.ACCT_NBR, "); 
				stringBuilder.append(" concat(X.PER_ID, concat('+' , concat(TMP.ACCT_ID, concat('+', concat(Y.BILL_CYC_CD , concat('+', W.WIN_START_DT)))))), "); 
				stringBuilder.append(" TMP.CREDIT_NOTE_FLG, TMP.ACCRUED_DATE, TMP.UDF_CHAR_11, TMP.UDF_CHAR_12, TMP.UDF_CHAR_3, TMP.TXN_DETAIL_ID, ");
				//stringBuilder.append("SUBSTR(concat(CONCAT(TO_CHAR(W.WIN_END_DT, 'YYMMDD'), TMP.ACCT_ID), ORA_HASH(CONCAT(TMP.UDF_CHAR_11, TMP.UDF_CHAR_12))), 1, 30) ");
				//TODO: ADDED COLUMN SETT_LEVEL GRANULARITY AND VALUE FETCHED FROM CM_EVENT_TXN_CALC
				stringBuilder.append(" TMP.SETT_LEVEL_GRANULARITY");
				stringBuilder.append(" AS SETT_LEVEL_GRANULARITY, :yesStatus , NULL, TMP.GRANULARITY_REFERENCE  ");
				stringBuilder.append(" FROM CI_BILL_CYC_SCH W ,CM_EVENT_CAT_AMOUNT TMP, CI_ACCT_PER X, CI_ACCT Y, CI_ACCT_NBR B ");
				stringBuilder.append(" WHERE W.BILL_CYC_CD = Y.BILL_CYC_CD AND TMP.TXN_DTTM BETWEEN W.WIN_START_DT AND W.WIN_END_DT ");
				stringBuilder.append(" AND TMP.ACCT_ID=X.ACCT_ID AND TMP.CALC_AMT IS NOT NULL ");
				stringBuilder.append(" AND X.ACCT_ID=Y.ACCT_ID AND Y.ACCT_ID=B.ACCT_ID  "); 
				stringBuilder.append(" AND B.ACCT_NBR_TYPE_CD=:acctNbrTypeCode  ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.executeUpdate();
				
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_BILL_GRANULARITY:insert err"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
	
			
			//CHANGED TMP TO TMP_40 FOR TMP4+TMP MERGE
			/*
			 * Creates records into the final table from TMP records with aggregated sum of calc amt.
			 */
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			Date billDate = getParameters().getBillDate();
			try {
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE ");
				stringBuilder.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY, PRICING_SEGMENT, GRANULARITY_REFERENCE, ILM_DT, ILM_ARCH_SW, ORG_TXN_ID) ");
				stringBuilder.append("SELECT A.EVENT_ID, A.PRICEITEM_CD, TRIM(A.PRICE_CATEGORY), SUM(A.CALC_AMT), A.CURRENCY_CD, ");
				stringBuilder.append("A.ACCT_TYPE, A.BILL_REFERENCE, A.INVOICEABLE_FLG, A.CREDIT_NOTE_FLG, ");
				if(isNull(billDate)){
					stringBuilder.append("SYSTIMESTAMP ");
				}
				else{
					stringBuilder.append(":billDate");
				}
				stringBuilder.append(", A.EXTRACT_FLG, A.EXTRACT_DTTM,A.ACCRUED_DATE,A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT, A.GRANULARITY_REFERENCE,  ");	
				stringBuilder.append(":ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, A.TXN_DETAIL_ID ");	
				stringBuilder.append("FROM CM_EVENT_BILL_GRANULARITY A ");
				stringBuilder.append("GROUP BY A.EVENT_ID, A.PRICEITEM_CD, A.PRICE_CATEGORY, A.CURRENCY_CD, ");
				stringBuilder.append("A.ACCT_TYPE, A.BILL_REFERENCE, A.INVOICEABLE_FLG, A.CREDIT_NOTE_FLG, ");
				stringBuilder.append("SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM,A.ACCRUED_DATE,A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT, A.GRANULARITY_REFERENCE, A.TXN_DETAIL_ID ");
				//stringBuilder.append("HAVING A.INVOICEABLE_FLG='Y' OR (A.INVOICEABLE_FLG='N' AND SUM(A.CALC_AMT)<>0) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				if(notNull(billDate))
				{
					preparedStatement.bindDate("billDate", billDate);
				}
				int count = preparedStatement.executeUpdate();
				logger.debug("Rows inserted into table CM_EVENT_PRICE -" + count);

			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_PRICE:insertion error"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			/*
			 * Validate and insert into final ERROR table
			 */
			insertIntoEventPriceErrorTable();
			

			return true;
		}
		
		/**
		 * Validate events based on presence of records in temp tables and log errors into CM_EVENT_PRICE_ERR table.
		 */
		private void insertIntoEventPriceErrorTable() {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			try {
				/*
				 * Insert event id into error table if priceitem code is not retrieved or corresponding records 
				 * do not exist in CM_EVENT_TXN_CALC.
				 * For non-tiered, priceitem comes from txn calc while for tiered, it comes from CI_TXN_DTL_PRITM.
				 */
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_ERR (EVENT_ID, ");
				stringBuilder.append("MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder.append("SELECT A.EVENT_ID, 90000, :msgNbr2301, :msg2301 ");
				stringBuilder.append("FROM CM_EVENT_TXN_DETAIL A, CM_EVENT_TXN_CALC B, CM_EVENT_PRICE_ERR C ");
				stringBuilder.append("WHERE B.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND (B.EVENT_ID IS NULL OR B.STATUS_CD=:errorStatus) ");
				stringBuilder.append("AND C.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND C.EVENT_ID IS NULL ");
				stringBuilder.append("GROUP BY A.EVENT_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("errorStatus", eventPriceDataInterfaceLookUp.getErrorStatus().trim(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2301", String.valueOf(CustomMessages.PRICEITEM_CD).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2301", getEventPriceErrorDescription(String.valueOf(CustomMessages.PRICEITEM_CD)).trim(), "ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_PRICE_ERR:insert err"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				/*
				 * Insert event id into error table if calc amt is null or corresponding records 
				 * do not exist in CM_EVENT_CAT_AMOUNT.
				 */
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_ERR (EVENT_ID, ");
				stringBuilder.append("MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder.append("SELECT A.EVENT_ID, 90000, :msgNbr2302, :msg2302 ");
				stringBuilder.append("FROM CM_EVENT_TXN_DETAIL A, CM_EVENT_CAT_AMOUNT B, CM_EVENT_PRICE_ERR C ");
				stringBuilder.append("WHERE B.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND (B.EVENT_ID IS NULL OR B.CALC_AMT IS NULL) ");
				stringBuilder.append("AND C.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND C.EVENT_ID IS NULL ");
				stringBuilder.append("GROUP BY A.EVENT_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("msgNbr2302", String.valueOf(CustomMessages.CALC_AMT).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2302", getEventPriceErrorDescription(String.valueOf(CustomMessages.CALC_AMT)).trim(), "ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_PRICE_ERR:insert err"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				/*
				 * Insert event id into error table if corresponding records 
				 * do not exist in CM_EVENT_BILL_GRANULARITY.
				 */
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_ERR (EVENT_ID, ");
				stringBuilder.append("MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder.append("SELECT A.EVENT_ID, 90000, :msgNbr2303, :msg2303 ");
				stringBuilder.append("FROM CM_EVENT_TXN_DETAIL A, CM_EVENT_BILL_GRANULARITY B, CM_EVENT_PRICE_ERR C ");
				stringBuilder.append("WHERE B.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND (B.EVENT_ID IS NULL) ");
				stringBuilder.append("AND C.EVENT_ID (+)= A.EVENT_ID ");
				stringBuilder.append("AND C.EVENT_ID IS NULL ");
				stringBuilder.append("GROUP BY A.EVENT_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("msgNbr2303", String.valueOf(CustomMessages.BILL_REFERENCE).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2303", getEventPriceErrorDescription(String.valueOf(CustomMessages.BILL_REFERENCE)).trim(), "ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("CM_EVENT_PRICE_ERR:insert err"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
		}

		private String getEventPriceErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.getEventPriceErrorMessage(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

	}
	
	public static final class EventPriceRevisedStepsData_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String lowEventPriceId;

		private String highEventPriceId;

		public EventPriceRevisedStepsData_Id(String lowEventPriceId, String highEventPriceId) {
			setLowEventPriceId(lowEventPriceId);
			setHighEventPriceId(highEventPriceId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getHighEventPriceId() {
			return highEventPriceId;
		}

		public void setHighEventPriceId(String highEventPriceId) {
			this.highEventPriceId = highEventPriceId;
		}

		public String getLowEventPriceId() {
			return lowEventPriceId;
		}

		public void setLowEventPriceId(String lowPayBillId) {
			this.lowEventPriceId = lowPayBillId;
		}
	}
}
