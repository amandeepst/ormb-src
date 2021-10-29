/*******************************************************************************
 * FileName                   : NonTransPriceDataInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Apr 12, 2016
 * Version Number             : 2.2
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Apr 12, 2016        Preeti       Separate batch job for Non Transaction Price and Bill Mapping data.
0.2      NA             Oct 21, 2016        Preeti       Fix to handle null event id.
0.3      NA             Dec 06, 2016        Preeti       Fix to handle null win end date.
0.4      NA             Jan 09, 2017        Preeti       Removed credit note logic.
0.5      NA             Jan 30, 2017        Preeti       Duplicate Non Event ID fix.
0.6      NA             Mar 02, 2017        Preeti       PAM-10844 fix.
0.7      NA             Mar 07, 2017        Ankur		 Performance changes,implemented global temp table changes. 
0.8	     NA             Mar 10, 2017	    Ankur        Global temp table bug:changed CM_PAY_REQ_TMP to CM_PAY_BILL_MAP table.
0.9	     NA             Apr 12, 2017	    Preeti       Settlement granularity update to avoid duplicate result. 
1.0      NA             May 22, 2017        Ankur		 PAM-12834 Gather statics implementation
1.1      NA             Jun 19, 2017        Ankur		 NAP-17065 disaggregation logic implementation,NAP-16883 fix & modified SQls for Performance improvement
1.2      NA             Jan 29, 2018        Preeti		 NAP-22186 Utilize FT and BCHG Mappings
1.3      NA             Feb 02, 2018        Vienna       NAP-22269,NAP-22270 populate ILM_DT and ILM_ARCH_SW
1.4      NA             Mar 13, 2018        Vienna       NAP-22269,NAP-22270 ILM_ARCH_SW must be Y
1.5      NA             Apr 09, 2018        Ankur        NAP-25484 remove CM_BILL_CYC_SCH
1.6      NA             May 01, 2018        Ankur        NAP-26526 & NAP-26537 changed to complete_dttm
1.7      NA             May 02, 2018        Ankur        NAP-26684 used SYSTIMESTAMP in place of TRUNC(SYSTIMESTAMP)
1.8      NA             May 14, 2018        Ankur        NAP-27294 Fixed
1.9      NA             Jun 13, 2018        Ankur        NAP-28756 Fixed
2.0      NA             Jul 18, 2018        Vikalp       NAP-29991 NTPRC Rerunnable with ILM_DT inclusion
2.1		 NA             Aug 20, 2018        Rakesh       NAP-29727/NAP-30663 Tiering logic and output tax
2.2		 NA				Sep 12, 2018		Amandeep 	 Removal Of ILM_DATE in job work query
2.3		 NA				Sep 12, 2018		Amandeep	 Performance fix for minimum charge query
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

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
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billDate, type = date)
 *            , @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *            , @BatchJobSoftParameter (name = txnSourceCode, type = string)})
 */


public class NonTransPriceDataInterface extends NonTransPriceDataInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(NonTransPriceDataInterface.class);
	private static final EventPriceDataInterfaceLookUp eventPriceDataInterfaceLookUp = new EventPriceDataInterfaceLookUp();

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		logger.info("Inside getJobWork()");

		// To truncate error tables and temporary tables
		deleteFromEventPriceTmpTable("CM_EVENT_PRICE_TMP_51");
		deleteFromEventPriceTmpTable("CM_EVENT_PRICE_TMP_52");
		deleteFromEventPriceTmpTable("CM_EVENT_PRICE_TMP_53");
		deleteFromEventPriceTmpTable("CM_EVENT_PRICE_TMP_54");
		resetSourceKeySQ();
		resetBillMapIdSq();
		logger.info("Rows deleted from temporary tables");

		List<ThreadWorkUnit> threadWorkUnitList = getEventPriceData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.info("No of rows selected for processing are - "+ rowsForProcessing);	
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * deleteFromEventPriceTmpTable() method will delete from the table provided as
	 * input.
	 * 
	 * @param inputEventPriceTmpTable
	 */
	@SuppressWarnings("deprecation")
	private void deleteFromEventPriceTmpTable(String inputEventPriceTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("DELETE FROM "+ inputEventPriceTmpTable);
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside deleteFromEventPriceTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	@SuppressWarnings("deprecation")
	private void resetSourceKeySQ() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_SOURCE_KEY}");
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside resetSourceKeySQ() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	
	private void resetBillMapIdSq() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_BILL_MAP_ID}");
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside resetBillMapIdSq() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	// *********************** getNonTransPriceData Method******************************

	/**
	 * getNonTransPriceData() method selects Bill IDs for processing by this Interface.
	 * 
	 * @return List Event_Price_Id
	 */
	private List<ThreadWorkUnit> getEventPriceData() {
		logger.info("Inside getEventPriceData() method");
		PreparedStatement preparedStatement = null;
		DateTime ilmDateTime = getSystemDateTime();
		StringBuilder stringBuilder = new StringBuilder();
		String lowEventPriceId = "";
		String highEventPriceId = "";
		int totalEventPriceRecords = 0;
		int totalEventPriceThreads = getParameters().getCustomThreadCount().intValue();
		Date billDate = getParameters().getBillDate();
		EventPriceData eventPriceData = null;

		//If Batch process date is not passed 
		if (isNull(billDate)){
			billDate = getSystemDateTime().getDate().addDays(-1);
					logger.info("Default Batch Process Date :" + billDate);
			}
		 
		try {			
			stringBuilder.append("INSERT INTO CM_EVENT_PRICE_TMP_51 ");
			stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
			stringBuilder.append("ACCT_ID, BILL_CYC_CD, STATUS_CD) ");
			stringBuilder.append("SELECT BILL.BILL_ID, BILL.WIN_START_DT, BILL.ALT_BILL_ID, ");
			stringBuilder.append("BILL.BILL_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.ACCT_ID, BILL.BILL_CYC_CD, ");
			stringBuilder.append(":initialStatus AS STATUS_CD ");
			stringBuilder.append("FROM CI_BILL BILL WHERE BILL.BILL_STAT_FLG='C' AND BILL.CR_NOTE_FR_BILL_ID=' ' ");
			stringBuilder.append("AND (BILL.COMPLETE_DTTM > (SELECT MAX(CM.UPLOAD_DTTM) FROM CM_BILL_ID_MAP CM ");
			stringBuilder.append("WHERE CM.CR_NOTE_FR_BILL_ID=' ') ");
			stringBuilder.append("OR (BILL.BILL_DT=:billDate AND ");
			stringBuilder.append("NOT EXISTS (SELECT 1 FROM CM_BILL_ID_MAP ");
			stringBuilder.append("WHERE ilm_dt in (select distinct ilm_dt from CM_BILL_ID_MAP where bill_dt = :billDate and bill.bill_id = bill_id))))");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("initialStatus",(eventPriceDataInterfaceLookUp.getInitStatus()).trim(), "STATUS_COD");
			preparedStatement.bindDate("billDate", billDate);
			//preparedStatement.bindDate("ilmDateTime", ilmDateTime.getDate());
			totalEventPriceRecords = preparedStatement.executeUpdate();
			logger.info("No. of rows selected for Non Trans price processing are:- "+ totalEventPriceRecords);
		} catch (Exception e) {
			logger.error("Inside getEventPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
		/*********Gather statics*******/
		try {
			preparedStatement = createPreparedStatement("{call cisadm.pkg_stats.gather_within_job('CM_NTPRC',1)}","");
			preparedStatement.execute();
			logger.info("begin gather statics in CM_NTPRC");
		} 
		catch (RuntimeException e) {
			logger.error("Calling gather stats, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} 
		catch (Exception e) {
			logger.error("Calling gather stats, Error -", e);
		}
		finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		/*****end of gather statics****/

		//*************************
		//logic to divide rows between threads

		//***********************
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();

		//***********************

		try {
			
					
			preparedStatement = createPreparedStatement("WITH TBL AS (SELECT BILL_ID FROM CM_EVENT_PRICE_TMP_51 ORDER BY 1) "
			 + " SELECT THREAD_NUM, MIN(BILL_ID) AS LOW_EVENT_PRICE_ID, MAX(BILL_ID) AS HIGH_EVENT_PRICE_ID "
		     + " FROM (SELECT BILL_ID, NTILE(:THREADCOUNT) OVER (ORDER BY NULL) AS THREAD_NUM FROM TBL)  "
			 + " GROUP BY THREAD_NUM ORDER BY 1 ","");					
			 

			 preparedStatement.bindBigInteger("THREADCOUNT", new BigInteger(String.valueOf(totalEventPriceThreads)));
			 preparedStatement.setAutoclose(false);
			
			 
			 for (SQLResultRow sqlRow : preparedStatement.list()) {
				
				 lowEventPriceId = sqlRow.getString("LOW_EVENT_PRICE_ID");
				 highEventPriceId = sqlRow.getString("HIGH_EVENT_PRICE_ID");
				 eventPriceData = new EventPriceData(lowEventPriceId, highEventPriceId);
				
				 logger.debug("billId1 = " + lowEventPriceId);
				 logger.debug("billId2 = " + highEventPriceId);
				
				
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(eventPriceData);
				threadworkUnit.addSupplementalData("ilmDateTime", ilmDateTime);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				eventPriceData = null;
				//*************************
				logger.debug("Event IDs added to Id class");
			}
			 
		} catch (ThreadAbortedException e) {
			logger.error("Inside getEventPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getEventPriceData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}	

			return threadWorkUnitList;
	}

	public Class<NonTransPriceDataInterfaceWorker> getThreadWorkerClass() {
		return NonTransPriceDataInterfaceWorker.class;
	}

	public static class NonTransPriceDataInterfaceWorker extends
	NonTransPriceDataInterfaceWorker_Gen {
		String errMsg = null;

		//Default constructor
		public NonTransPriceDataInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("Inside initializeThreadWork() method for batch thread number: "+ getBatchThreadNumber());
			super.initializeThreadWork(arg0);
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.info("Inside createExecutionStrategy() method");
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("Inside executeWorkUnit()");
			logger.info("ver 2 Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());
			String lowEventPriceId = "";
			String highEventPriceId = "";

			EventPriceData eventPriceData = (EventPriceData) unit.getPrimaryId();
			lowEventPriceId = eventPriceData.getLowEventPriceId();
			highEventPriceId = eventPriceData.getHighEventPriceId();
			DateTime ilmDateTime = (DateTime)unit.getSupplementallData("ilmDateTime"); 

			logger.info("eventId1 = " + lowEventPriceId);
			logger.info("eventId2 = " + highEventPriceId);

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			//NEED TO ADD LOGIC TO RETRIEVE WIN START DATE FROM CUSTOM TABLE INSTEAD OF GETTING IT FROM CI_BILL TABLE 
			try {						
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_TMP_52 ");
				stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("ACCT_ID, BILL_CYC_CD, PER_ID, PER_ID_NBR, STATUS_CD) ");
				stringBuilder.append("SELECT BILL.BILL_ID, BILL.BILL_START_DT, BILL.ALT_BILL_ID, ");
				stringBuilder.append("BILL.BILL_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.ACCT_ID, BILL.BILL_CYC_CD, ");
				stringBuilder.append("X.PER_ID, A.PER_ID_NBR, :initialStatus AS STATUS_CD ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_51 BILL, CI_ACCT_PER X, CI_PER_ID A ");
				stringBuilder.append("WHERE BILL.BILL_ID BETWEEN :lowEventPriceId and :highEventPriceId ");
				stringBuilder.append("AND BILL.ACCT_ID=X.ACCT_ID AND X.PER_ID=A.PER_ID ");
				stringBuilder.append("AND A.ID_TYPE_CD=:externalPartyId ");	
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowEventPriceId", lowEventPriceId, "BILL_ID");
				preparedStatement.bindString("highEventPriceId", highEventPriceId, "BILL_ID");
				preparedStatement.bindString("externalPartyId", eventPriceDataInterfaceLookUp.getExternalPartyId().trim(), "ID_TYPE_CD");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVENT_PRICE_TMP_52 -" + count);
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));

			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {						
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_TMP_53 ");
				stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("ACCT_ID, BILL_CYC_CD, PER_ID, PER_ID_NBR, CIS_DIVISION, WIN_END_DT, CURRENCY_CD, STATUS_CD) ");
				stringBuilder.append("SELECT BILL.BILL_ID, BILL.BILL_START_DT, BILL.ALT_BILL_ID, ");
				stringBuilder.append("BILL.BILL_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.ACCT_ID, BILL.BILL_CYC_CD, ");
				stringBuilder.append("BILL.PER_ID, BILL.PER_ID_NBR, Y.CIS_DIVISION, BILL.BILL_DT, Y.CURRENCY_CD,");
				stringBuilder.append(":initialStatus AS STATUS_CD ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_52 BILL, CI_ACCT Y  ");
				stringBuilder.append("WHERE BILL.ACCT_ID=Y.ACCT_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVENT_PRICE_TMP_53 -" + count);
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));

			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			//Bill amount should have both bill segment and Adjustments addition
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {	
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE_TMP_54 ");
				stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("ACCT_ID, BILL_CYC_CD, PER_ID, PER_ID_NBR, CIS_DIVISION, WIN_END_DT, ");
				stringBuilder.append("BILL_AMT, CURRENCY_CD, STATUS_CD) ");
				stringBuilder.append("SELECT BILL.BILL_ID, CASE WHEN BILL.BILL_CYC_CD=' ' THEN SYSTIMESTAMP ELSE BILL.BILL_START_DT END AS BILL_START_DT, BILL.ALT_BILL_ID, ");
				stringBuilder.append("BILL.BILL_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.ACCT_ID, CASE WHEN BILL.BILL_CYC_CD=' ' THEN 'ADHC' ELSE BILL.BILL_CYC_CD END AS BILL_CYC_CD, "); 
				stringBuilder.append("BILL.PER_ID, BILL.PER_ID_NBR, BILL.CIS_DIVISION, CASE WHEN BILL.BILL_CYC_CD=' ' THEN SYSTIMESTAMP ELSE BILL.WIN_END_DT END AS WIN_END_DT, ");
				stringBuilder.append("B.SRCH_CHAR_VAL AS AMT, BILL.CURRENCY_CD AS CURRENCY_CD, :initialStatus AS STATUS_CD ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_53 BILL, CI_BILL_CHAR B ");
				stringBuilder.append("WHERE B.BILL_ID=BILL.BILL_ID AND B.CHAR_TYPE_CD='BILL_AMT'");			
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("initialStatus", eventPriceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVENT_PRICE_TMP_54 -" + count);
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));

			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {						
				stringBuilder.append("INSERT INTO CM_BILL_ID_MAP ");
				stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("PER_ID_NBR, CIS_DIVISION, BILL_END_DT, ");
				stringBuilder.append("BILL_AMT, CURRENCY_CD, EVENT_TYPE_ID, EVENT_PROCESS_ID, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, BILL_REFERENCE, ACCT_TYPE, ILM_DT, ILM_ARCH_SW, BILL_MAP_ID ) ");
				stringBuilder.append("SELECT BILL.BILL_ID, BILL.BILL_START_DT, BILL.ALT_BILL_ID, ");
				stringBuilder.append("BILL.BILL_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.PER_ID_NBR, ");
				stringBuilder.append("BILL.CIS_DIVISION, BILL.WIN_END_DT, ");
				stringBuilder.append("BILL.BILL_AMT, BILL.CURRENCY_CD, :eventTypeId, :eventProcessId, ");
				stringBuilder.append("SYSTIMESTAMP, :yesStatus, NULL, ");
				stringBuilder.append("CONCAT(BILL.PER_ID,CONCAT('+',CONCAT(BILL.ACCT_ID,CONCAT('+',CONCAT(BILL.BILL_CYC_CD,CONCAT('+',BILL.BILL_START_DT)))))) ");
				stringBuilder.append("AS BILL_REFERENCE, A.ACCT_NBR, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, bill_id_map_seq.nextval ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_54 BILL, CI_ACCT_NBR A ");
				stringBuilder.append("WHERE BILL.ACCT_ID = A.ACCT_ID AND A.ACCT_NBR_TYPE_CD=:acctNbrTypeCode ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindString("eventTypeId", eventPriceDataInterfaceLookUp.getEventTypeId().trim(), "EVENT_TYPE_ID");
				preparedStatement.bindString("eventProcessId", eventPriceDataInterfaceLookUp.getEventProcessId().trim(), "EVENT_PROCESS_ID");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_BILL_ID_MAP -" + count);
			} 
			catch (ThreadAbortedException e){
				logger.error("Insert query for CM_BILL_ID_MAP", e);
				errMsg = "Insert error: CM_BILL_ID_MAP";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_BILL_ID_MAP"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_BILL_ID_MAP", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_BILL_ID_MAP"));
			}
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			//Minimum charge
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_NON_TRANS_PRICE ");
				stringBuilder.append(
						" (NON_EVENT_ID,PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append(
						"BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, "
								+ "SOURCE_KEY,CREDIT_NOTE_FLG,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT C.BSEG_ID, A.PER_ID_NBR, W.ACCT_NBR, TRIM(G.PRICEITEM_CD), ");
				stringBuilder.append("D.CHAR_VAL, (C.CALC_AMT)*-1, C.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID,CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD,CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append(
						"C.PRT_SW, SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW,CM_SOURCE_KEY_SQ.NEXTVAL,'N',0 ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_BSEG B, CI_BSEG_CALC_LN C, CI_PRICEITEM G, CI_BSEG_SQ H, ");
				stringBuilder.append("CI_BSEG_CL_CHAR D, CI_ACCT_NBR W ");
				stringBuilder.append("WHERE A.ACCT_ID=W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD=:acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BSEG_ID=C.BSEG_ID ");
				stringBuilder.append("AND C.BSEG_ID=D.BSEG_ID ");
				stringBuilder.append("AND C.SEQNO=D.SEQNO ");
				stringBuilder.append("AND D.CHAR_TYPE_CD=:bclTypeCode ");
				stringBuilder.append("AND D.CHAR_VAL!=:charVal ");
				stringBuilder.append("AND C.BSEG_ID = H.BSEG_ID ");
				stringBuilder.append("AND H.UOM_CD<> ' ' ");
				stringBuilder.append("AND H.UOM_CD = G.UOM_CD ");
				stringBuilder
						.append("AND NOT EXISTS (SELECT 1 FROM CI_BSEG_CALC D1,CM_BCHG_ATTRIBUTES_MAP C ");
				stringBuilder.append("WHERE C.BILLABLE_CHG_ID=D1.BILLABLE_CHG_ID ");
				stringBuilder.append("AND D.BSEG_ID=D1.BSEG_ID) ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("bclTypeCode", eventPriceDataInterfaceLookUp.getBclTypeCode().trim(), "CHAR_TYPE_CD");
				preparedStatement.bindString("charVal", eventPriceDataInterfaceLookUp.getCharVal().trim(), "CHAR_VAL");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for minimum charges -" + count);
			} 
			catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}	
			
						
			
			//For ADHOC AND INVOICE ADJUSTMENTS-RS CODE NULL
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_EVT_PRICE ");
				stringBuilder.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
				stringBuilder.append(
						"UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT, ILM_DT, "
								+ "ILM_ARCH_SW, SOURCE_KEY) ");
				stringBuilder.append("SELECT X.EVENT_ID, X.PRICEITEM_CD, ");
				stringBuilder
						.append("'PI_MBA', ROUND(X.SVC_QTY*(X.CHARGE_AMT)*-1,5), Y.CURRENCY_CD, W.ACCT_NBR, ");
				stringBuilder.append("CONCAT(A.PER_ID ,CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD,CONCAT('+', A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append("'Y', 'N', SYSTIMESTAMP, :yesStatus,NULL,A.BILL_DT, ");
				stringBuilder.append("CASE WHEN X.GRANULARITY_HASH = 0 or  X.GRANULARITY_HASH is null ");
				stringBuilder.append("THEN CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID) ");
				stringBuilder.append(
						"ELSE CONCAT(CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID),X.GRANULARITY_HASH)  END AS SETT_LEVEL_GRANULARITY, NULL ,");
				stringBuilder
						.append(":ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_54 A, CI_BSEG B, ");
				stringBuilder.append("CI_ACCT_NBR W, CM_BCHG_ATTRIBUTES_MAP X, CI_BSEG_CALC Y ");
				stringBuilder.append("WHERE  A.ACCT_ID=W.ACCT_ID ");
				stringBuilder.append("AND  W.ACCT_NBR_TYPE_CD=:acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append(
						"AND B.BSEG_ID=Y.BSEG_ID AND Y.BILLABLE_CHG_ID=X.BILLABLE_CHG_ID AND X.EVENT_ID IS NOT NULL");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVT_PRICE for Adhoc Billable charges-" + count);
			} 
			catch (ThreadAbortedException e){
				logger.error("Insert query fot CM_EVT_PRICE",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution("Error while inserting data into CM_EVENT_PRICE"));
			}
			catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				logger.error("Insert query fot CM_EVT_PRICE",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution("Error while inserting data into CM_EVT_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}		
			
			//*************************Adjustment data extraction-*****************************************
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {		
				stringBuilder.append("INSERT INTO CM_EVT_PRICE ");
				stringBuilder.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT, "
						+ "ILM_DT, ILM_ARCH_SW, SOURCE_KEY) ");
				stringBuilder.append("SELECT Y.EVENT_ID, D.ADJ_TYPE_CD, ");
				stringBuilder.append("C.SA_TYPE_CD, (D.ADJ_AMT)*-1, D.CURRENCY_CD, W.ACCT_NBR, ");
				stringBuilder.append("CONCAT(A.PER_ID,CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder.append("CONCAT(A.BILL_CYC_CD,CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append("'Y', 'N',  SYSTIMESTAMP, :yesStatus, NULL,A.BILL_DT, ");
				stringBuilder.append("CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID) AS SETT_LEVEL_GRANULARITY,NULL, ");
				stringBuilder.append(":ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL ");
				stringBuilder.append("FROM CM_EVENT_PRICE_TMP_54 A, CI_ADJ_CHAR B, CI_ADJ D, CI_SA C, CI_ACCT_NBR W,"
						+ " CI_ADJ_STG_UP X, CM_ADJ_STG Y ");
				stringBuilder.append("WHERE A.ACCT_ID=W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD=:acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.SRCH_CHAR_VAL and B.CHAR_TYPE_CD='BILL_ID' ");
				stringBuilder.append("AND B.ADJ_ID=D.ADJ_ID AND D.SA_ID=C.SA_ID ");
				stringBuilder.append("AND B.ADJ_ID=X.ADJ_ID ");
				stringBuilder.append("AND X.ADJ_STG_CTL_ID=Y.ADJ_STG_CTL_ID ");
				stringBuilder.append("AND X.ADJ_STG_UP_ID=Y.ADJ_STG_UP_ID and Y.EVENT_ID is not null ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVT_PRICE for External Adjustments -" + count);
			}catch (ThreadAbortedException e){
				logger.error("Insert query fot CM_EVT_PRICE",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution("Error while inserting data into CM_EVENT_PRICE"));
			}catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				logger.error("Insert query fot CM_EVT_PRICE",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution("Error while inserting data into CM_EVT_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		
			
			//waf or wrof adjustments-FLAG NOT NEEDED
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_NON_TRANS_PRICE  ");
				stringBuilder.append(
						"(NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD,BILL_REFERENCE,  ");
				stringBuilder.append(
						" INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, SOURCE_KEY,CREDIT_NOTE_FLG,"
								+ "SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT D.ADJ_ID, A.PER_ID_NBR, W.ACCT_NBR, D.ADJ_TYPE_CD, ");
				stringBuilder.append("C.SA_TYPE_CD, (D.ADJ_AMT)*-1, D.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID,CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+',  ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD,CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE,  ");
				stringBuilder.append(
						"'Y', SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL,'N' , ");
				stringBuilder
						.append("CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID) AS SETT_LEVEL_GRANULARITY  ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_ADJ_CHAR B, CI_ADJ D, CI_SA C, CI_ACCT_NBR W  ");
				stringBuilder.append("WHERE A.ACCT_ID=W.ACCT_ID  ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD=:acctNbrTypeCode  ");
				stringBuilder.append("AND A.BILL_ID=B.SRCH_CHAR_VAL AND B.CHAR_TYPE_CD='BILL_ID'  ");
				stringBuilder.append("AND B.ADJ_ID=D.ADJ_ID AND D.SA_ID=C.SA_ID  ");
				stringBuilder
						.append("AND NOT EXISTS (SELECT 1 FROM CI_ADJ_STG_UP X WHERE X.ADJ_ID=D.ADJ_ID )  ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for Internal Adjustments -" + count);
			}catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			//***************Event ID Null scenarios-cm_bchg_stg and cm_adj_stg**********************//			
			//for adjustments
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_NON_TRANS_PRICE ( ");
				stringBuilder.append(
						" NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append(
						"BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM,EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, "
								+ " SOURCE_KEY,CREDIT_NOTE_FLG,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT D.ADJ_ID, A.PER_ID_NBR, W.ACCT_NBR, D.ADJ_TYPE_CD, ");
				stringBuilder.append("C.SA_TYPE_CD, (D.ADJ_AMT)*-1, D.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID, CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD, CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append(
						"'Y', SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL,'N', ");
				stringBuilder
						.append("CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID) AS SETT_LEVEL_GRANULARITY ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_ADJ_CHAR B, CI_ADJ D, CI_SA C, CI_ACCT_NBR W ");
				stringBuilder.append("WHERE A.ACCT_ID= W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD =:acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.SRCH_CHAR_VAL AND B.CHAR_TYPE_CD='BILL_ID' ");
				stringBuilder.append("AND B.ADJ_ID=D.ADJ_ID AND D.SA_ID=C.SA_ID ");
				stringBuilder.append("AND EXISTS (SELECT 1 FROM CI_ADJ_STG_UP X, CM_ADJ_STG Y ");
				stringBuilder.append(
						"WHERE X.ADJ_STG_UP_ID=Y.ADJ_STG_UP_ID AND Y.EVENT_ID IS NULL AND X.ADJ_ID=D.ADJ_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for Internal Adjustments -" + count);
			} 			catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			//for BILLABLE CHARGES
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_NON_TRANS_PRICE ");
				stringBuilder.append(
						"(NON_EVENT_ID,PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append(
						"BILL_REFERENCE, INVOICEABLE_FLG,UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, "
								+ "SOURCE_KEY, CREDIT_NOTE_FLG,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT B.BSEG_ID, A.PER_ID_NBR, W.ACCT_NBR, TRIM(I.PRICEITEM_CD), ");
				stringBuilder.append("C.SA_TYPE_CD, (Y.CALC_AMT)*-1, Y.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID ,CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD,CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE , ");
				stringBuilder.append(
						"'Y', SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL,'N' ,");
				stringBuilder.append("0 AS SETT_LEVEL_GRANULARITY ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_BSEG B, CI_SA C, CI_ACCT_NBR W, CI_BSEG_CALC Y, CI_BILL_CHG I ");
				stringBuilder.append("WHERE A.ACCT_ID= W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD= :acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID =B.BILL_ID ");
				stringBuilder.append("AND B.SA_ID=C.SA_ID ");
				stringBuilder.append(
						"AND B.BSEG_ID = Y.BSEG_ID and I.BILLABLE_CHG_ID=Y.BILLABLE_CHG_ID and C.SA_TYPE_CD='RECR' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for Internal Adjustments -" + count);
			} 			catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			//code added for SDS/Adhoc Billable Charges
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_NON_TRANS_PRICE ");
				stringBuilder.append(
						"(NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append(
						"BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, "
								+ "SOURCE_KEY,SOURCE_TYPE, SOURCE_ID,CREDIT_NOTE_FLG,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT B.BSEG_ID, A.PER_ID_NBR, W.ACCT_NBR, TRIM(I.PRICEITEM_CD), ");
				stringBuilder.append("C.SA_TYPE_CD, (Y.CALC_AMT)*-1, Y.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID,CONCAT('+',CONCAT(A.ACCT_ID, CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD ,CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append(
						"'Y', SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL,"
								+ " J.SOURCE_TYPE, J.SOURCE_ID,'N', ");
				stringBuilder.append("CASE WHEN J.GRANULARITY_HASH = 0 or  J.GRANULARITY_HASH is null ");
				stringBuilder.append("THEN CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID) ");
				stringBuilder.append(
						"ELSE CONCAT(CONCAT(TO_CHAR(A.BILL_DT,'YYMMDD'),A.ACCT_ID),J.GRANULARITY_HASH)  END AS SETT_LEVEL_GRANULARITY ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_BSEG B, CI_SA C, CI_ACCT_NBR W, CI_BSEG_CALC Y, CI_BILL_CHG I, CM_BCHG_ATTRIBUTES_MAP J ");
				stringBuilder.append("WHERE A.ACCT_ID =W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD= :acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID AND B.SA_ID=C.SA_ID  ");
				stringBuilder.append("AND I.BILLABLE_CHG_ID=J.BILLABLE_CHG_ID ");
				stringBuilder.append(
						"AND B.BSEG_ID = Y.BSEG_ID and I.BILLABLE_CHG_ID=Y.BILLABLE_CHG_ID and J.SOURCE_TYPE IS NOT NULL AND J.EVENT_ID IS NULL ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for SDS activity -" + count);
			} 			catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			//NAP-30663 for TAX CHARGES
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_NON_TRANS_PRICE ");
				stringBuilder.append(
						"(NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append(
						"BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, SOURCE_KEY,"
								+ "CREDIT_NOTE_FLG,SETT_LEVEL_GRANULARITY) ");
				stringBuilder.append("SELECT C.BSEG_ID, A.PER_ID_NBR, W.ACCT_NBR, TRIM(G.PRICEITEM_CD), ");
				stringBuilder.append("'TAX', (C.CALC_AMT)*-1, C.CURRENCY_CD, ");
				stringBuilder.append("CONCAT(A.PER_ID , CONCAT('+',CONCAT(A.ACCT_ID,CONCAT('+', ");
				stringBuilder
						.append("CONCAT(A.BILL_CYC_CD , CONCAT('+',A.BILL_START_DT)))))) AS BILL_REFERENCE, ");
				stringBuilder.append(
						"'Y', SYSTIMESTAMP, :yesStatus, NULL, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, CM_SOURCE_KEY_SQ.NEXTVAL,'N', ");
				stringBuilder.append("0 AS SETT_LEVEL_GRANULARITY ");
				stringBuilder.append(
						"FROM CM_EVENT_PRICE_TMP_54 A, CI_BSEG B, CI_BSEG_CALC C, CI_PRICEITEM G, CI_BSEG_SQ H,  CI_ACCT_NBR W ");
				stringBuilder.append("WHERE A.ACCT_ID =W.ACCT_ID ");
				stringBuilder.append("AND W.ACCT_NBR_TYPE_CD =:acctNbrTypeCode ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID AND B.BSEG_ID=C.BSEG_ID ");
				stringBuilder.append("AND TRIM(C.RS_CD)='TAX'  AND C.BSEG_ID = H.BSEG_ID ");
				stringBuilder.append("AND H.UOM_CD<> ' ' AND H.UOM_CD = G.UOM_CD ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbrTypeCode", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for TAX Charges -" + count);
			}catch (ThreadAbortedException e){
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				errMsg = "Insert error: CM_NON_TRANS_PRICE";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}
			catch (Exception e) {
				logger.error("Insert query for CM_NON_TRANS_PRICE", e);
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_NON_TRANS_PRICE"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			try {
				preparedStatement = createPreparedStatement("commit","");
				preparedStatement.execute();
							
			} catch (RuntimeException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
						
			
			return true;
		}



		public static String getEventPriceErrorDescription(String messageNumber) {
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


		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.info("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}

	}
	public static final class EventPriceData implements Id {

		private static final long serialVersionUID = 1L;

		private String lowEventPriceId;

		private String highEventPriceId;

		public EventPriceData(String lowEventPriceId, String highEventPriceId) {
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
