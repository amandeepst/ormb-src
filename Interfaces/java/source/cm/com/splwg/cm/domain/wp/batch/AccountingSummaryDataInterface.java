/*******************************************************************************
* FileName                   : AccountingSummaryDataInterface.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Jun 10, 2015 
* Version Number             : 1.4
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Jun 10, 2015        Preeti Tiwari        Implemented all requirements for CD2.
0.2      NA             Oct 01, 2015        Preeti Tiwari        Implemented all requirements as per latest FD v2.13.
0.3      NA             May 09, 2016        Preeti Tiwari        Updated as per NAP-5225 to add columns into error table.    
0.4      NA             Mar 20, 2017        Ankur Jain			 Updated as per NAP-14148
0.5      NA             Mar 21, 2017        Ankur Jain			 Global temp table and performance changes
0.6		 NA             Apr 20, 2017	    Ankur Jain	         Updated as per NAP-15725
0.7      NA             May 22, 2017        Ankur Jain		     PAM-12834 Gather statics implementation
0.8      NA             Sep 14, 2017        Preeti Tiwari        Adding GL_ACCT assignment functionality.
0.9      NA             Sep 22, 2017        Ankur Jain		     PAM-15043 Minimum charge issue fix
1.0      NA             Oct 05, 2017        Ankur Jain		     NAP-19528 Accounting Data interface Redesign & PAM-15657 fix
1.1      NA             Nov 09, 2017        Ankur Jain		     PAM-16204
1.2      NA             Dec 18, 2017        Ankur Jain		     PAM-16664 Fix
1.3      NA             Jan 23, 2017        Preeti Tiwari		 NAP-21721: Txn Mapping update
1.4      NA             Mar 23, 2018        Ankur Jain		 	 PAM-16683 & PAM-16440 Performance issue
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
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *            , @BatchJobSoftParameter (name = txnSourceCode, type = string)})
 */

public class AccountingSummaryDataInterface extends AccountingSummaryDataInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(AccountingSummaryDataInterface.class);

	private static final PaymentsRequestInterfaceLookUp paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp(); 

	public JobWork getJobWork() {
		logger.debug("Inside getJobWork()");
		paymentsRequestInterfaceLookUp.setLookUpConstants(); 

		// To truncate error tables and temporary tables
		truncateFromAccountingTmpTable("CM_FT_GL_ASL_TMP");
		truncateFromAccountingTmpTable("CM_FT_GL_FX_TMP");
		logger.debug("Rows truncated from temporary and error tables");

		List<ThreadWorkUnit> threadWorkUnitList = getAccountingData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.debug("No of rows selected for processing are - "+ rowsForProcessing);
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * truncateFromAccountingTmpTable() method will truncate from the table provided as
	 * input.
	 * 
	 * @param inputAccountingTmpTable
	 */
	@SuppressWarnings("deprecation")
	private void truncateFromAccountingTmpTable(String inputAccountingTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("TRUNCATE TABLE "+ inputAccountingTmpTable);
			preparedStatement.execute();
			}
		catch (ThreadAbortedException e) {
			logger.error("Inside truncateFromAccountingTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside truncateFromAccountingTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	// *********************** getAccounting Method******************************

	/**
	 * getAccountingData() method selects Accounting IDs for processing by this Interface.
	 * 
	 * @return List
	 */
	private List<ThreadWorkUnit> getAccountingData() {
		logger.debug("Inside getAccountingData() method");
		PreparedStatement preparedStatement = null;		
		String lowAccountingId = "";
		String highAccountingId = "";
		//String ftTypeFlag=CommonUtils.CheckNull(getParameters().getFtTypeFlag()).trim();
		AccountingIdData accountingIdData = null;
		StringBuilder stringBuilder = new StringBuilder();
		int chunkSize = getParameters().getCustomThreadCount().intValue();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
				
		
		try {
			/**
			 * We are selecting freezed financial transaction which are not present in CM_FT_GL_ASL for processing on the basis of chunk size.
			 * Chunk size strategy is being used to figure out lower and higher FT in a range and this is being passed as unit to executeWorkUnit method
			 */
			stringBuilder.append("WITH TBL AS (SELECT FT_ID FROM CI_FT WHERE FREEZE_SW='Y' ");
			stringBuilder.append("AND ((TO_CHAR(FREEZE_DTTM, 'YYYY-MM-DD HH24:MI:SS') > (SELECT MAX(TO_CHAR(UPLOAD_DTTM, 'YYYY-MM-DD HH24:MI:SS')) FROM CM_FT_GL_ASL)) ");
			stringBuilder.append("OR NOT EXISTS(SELECT 1 FROM CM_FT_GL_ASL )) ORDER BY FT_ID) ");
			stringBuilder.append("SELECT THREAD_NUM, MIN(FT_ID) AS LOW_ACCOUNTING_ID, ");
			stringBuilder.append("MAX(FT_ID) AS HIGH_ACCOUNTING_ID FROM (SELECT FT_ID, ");
			stringBuilder.append("CEIL((ROWNUM)/:CHUNKSIZE) AS THREAD_NUM FROM TBL) ");
			stringBuilder.append("GROUP BY THREAD_NUM ORDER BY 1 ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),""); 
			preparedStatement.bindBigInteger("CHUNKSIZE", new BigInteger(String.valueOf(chunkSize)));
			preparedStatement.setAutoclose(false);
			
			 
			 for (SQLResultRow sqlRow : preparedStatement.list()) {
				 lowAccountingId = sqlRow.getString("LOW_ACCOUNTING_ID");
				 highAccountingId = sqlRow.getString("HIGH_ACCOUNTING_ID");
				 accountingIdData = new AccountingIdData(lowAccountingId, highAccountingId);
				 
				//*************************
				 threadworkUnit = new ThreadWorkUnit();
				 threadworkUnit.setPrimaryId(accountingIdData);
				 threadWorkUnitList.add(threadworkUnit);
				 threadworkUnit = null;
				 accountingIdData = null;
				//*************************
				
				}

		} catch (ThreadAbortedException e){
			logger.error("Inside getAccountingData() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside getAccountingData() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		
		return threadWorkUnitList;
	}

	public Class<AccountingSummaryDataInterfaceWorker> getThreadWorkerClass() {
		return AccountingSummaryDataInterfaceWorker.class;
	}

	public static class AccountingSummaryDataInterfaceWorker extends
	AccountingSummaryDataInterfaceWorker_Gen {

		public AccountingSummaryDataInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork() method for batch thread number: "+ getBatchThreadNumber());
			super.initializeThreadWork(arg0);
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * extracting accounting data  details from ORMB. It validates the
		 * extracted data and populates the target tables accordingly.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			AccountingIdData accountingIdData = (AccountingIdData) unit.getPrimaryId();
			String lowAccountingId = accountingIdData.getLowAccountingId();
			String highAccountingId = accountingIdData.getHighAccountingId();

			logger.debug("accountingId1 = " + lowAccountingId);
			logger.debug("accountingId2 = " + highAccountingId);
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();	
			/**
			 * We are selecting freezed financial transaction which are not present in CM_FT_GL_ASL for processing on the basis of lower and higher 
			 * accounting id passed as thread work unit and fetching FT related details like sibling_id,sa_id,cis_division  and inserting into 
			 * global temp table CM_FT_INITIAL_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_INITIAL_INFO ");
				stringBuilder.append("(FT_ID, SIBLING_ID, SA_ID, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT,STATUS_CD) ");
				stringBuilder.append("SELECT FT_ID,SIBLING_ID,SA_ID,CIS_DIVISION,CURRENCY_CD,FT_TYPE_FLG,ACCOUNTING_DT,:initialStatus AS STATUS_CD ");
				stringBuilder.append("FROM CI_FT  ");
				stringBuilder.append("WHERE FREEZE_SW='Y' ");
				stringBuilder.append("AND ((TO_CHAR(FREEZE_DTTM, 'YYYY-MM-DD HH24:MI:SS') > (SELECT MAX(TO_CHAR(UPLOAD_DTTM, 'YYYY-MM-DD HH24:MI:SS')) FROM CM_FT_GL_ASL)) ");
				stringBuilder.append("OR NOT EXISTS(SELECT 1 FROM CM_FT_GL_ASL )) AND FT_ID BETWEEN :lowAccountingId and :highAccountingId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowAccountingId", lowAccountingId, "FT_ID");
				preparedStatement.bindString("highAccountingId", highAccountingId, "FT_ID");
				preparedStatement.bindString("initialStatus",paymentsRequestInterfaceLookUp.getInitialStatus().trim(),"STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			//*********************GL Account assignment:Update GL_ACCT**************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();	
			//Get job work only passing the FTs where Freeze switch is Y.	
			
			//This update will set gl_acct same as dst_id in case of Adjustments/Payments and Bill segments where dst_id falls under list stored in mapping table.
			try {
				stringBuilder.append("UPDATE CI_FT_GL A SET A.GL_ACCT=TRIM(A.DST_ID) ");
				stringBuilder.append("WHERE A.FT_ID BETWEEN :lowAccountingId and :highAccountingId ");
				stringBuilder.append("AND ((EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B WHERE A.FT_ID=B.FT_ID AND B.FT_TYPE_FLG IN ('BS','BX')) ");
				stringBuilder.append("AND EXISTS (SELECT C.DST_ID FROM CM_FT_GL_TMP C WHERE A.DST_ID=C.DST_ID AND C.DIST_TYPE='REG')) ");
				stringBuilder.append("OR (EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B WHERE A.FT_ID=B.FT_ID AND B.FT_TYPE_FLG IN ('AD','AX','PS','PX')))) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowAccountingId", lowAccountingId, "FT_ID");
				preparedStatement.bindString("highAccountingId", highAccountingId, "FT_ID");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();	
			//This update will set gl_Acct for tax lines. Here gl_Acct is being derived from bill Segment characteristic value for Tax Regime. 
			try {
				stringBuilder.append("UPDATE CI_FT_GL A SET A.GL_ACCT=NVL((SELECT TRIM(D.CHAR_VAL) FROM CM_FT_INITIAL_INFO C, CI_BSEG_CL_CHAR D ");
				stringBuilder.append("WHERE A.FT_ID=C.FT_ID AND C.SIBLING_ID=D.BSEG_ID ");
				stringBuilder.append("AND D.CHAR_TYPE_CD='TAX RGME' GROUP BY D.CHAR_VAL),' ') ");
				stringBuilder.append("WHERE A.FT_ID BETWEEN :lowAccountingId and :highAccountingId ");
				stringBuilder.append("AND A.DST_ID='TAX' AND EXISTS (SELECT 1 FROM CM_FT_INITIAL_INFO B WHERE A.FT_ID=B.FT_ID ) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowAccountingId", lowAccountingId, "FT_ID");
				preparedStatement.bindString("highAccountingId", highAccountingId, "FT_ID");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();			
			//This update will set gl_Acct for MSC charges. Here gl_acct is being derived from product characteristic value for Distribution code. Should be only where dst_id is BASE_CHG. 				
			try {
				stringBuilder.append("UPDATE CI_FT_GL A SET A.GL_ACCT=NVL((SELECT TRIM(X.ADHOC_CHAR_VAL) ");
				stringBuilder.append("FROM CI_PRICEITEM_CHAR X, CI_PRICEITEM P, CI_BSEG_SQ Z, CM_FT_INITIAL_INFO Q ");
				stringBuilder.append("WHERE X.CHAR_TYPE_CD='OV_DSTCD' AND X.PRICEITEM_CD=P.PRICEITEM_CD ");
				stringBuilder.append("AND P.UOM_CD=Z.UOM_CD AND Z.BSEG_ID=Q.SIBLING_ID AND Q.FT_ID=A.FT_ID),' ') ");
				stringBuilder.append("WHERE A.FT_ID BETWEEN :lowAccountingId and :highAccountingId ");
				stringBuilder.append("AND EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B WHERE A.FT_ID=B.FT_ID AND B.FT_TYPE_FLG IN ('BS','BX')) ");
				stringBuilder.append("AND NOT EXISTS (SELECT C.DST_ID FROM CM_FT_GL_TMP C WHERE A.DST_ID=C.DST_ID) ");
				stringBuilder.append("AND EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B, CI_BSEG_CALC C WHERE A.FT_ID=B.FT_ID ");
				stringBuilder.append("AND B.SIBLING_ID=C.BSEG_ID AND C.EFFDT IS NOT NULL) ");				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowAccountingId", lowAccountingId, "FT_ID");
				preparedStatement.bindString("highAccountingId", highAccountingId, "FT_ID");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append("UPDATE CI_FT_GL A SET A.GL_ACCT=NVL((SELECT TRIM(X.ADHOC_CHAR_VAL) ");
				stringBuilder.append("FROM CI_PRICEITEM_CHAR X, CI_BILL_CHG P, CI_BSEG_CALC Z, CM_FT_INITIAL_INFO Q ");
				stringBuilder.append("WHERE X.CHAR_TYPE_CD='OV_DSTCD' AND X.PRICEITEM_CD=P.PRICEITEM_CD ");
				stringBuilder.append("AND P.BILLABLE_CHG_ID=Z.BILLABLE_CHG_ID AND Z.BSEG_ID=Q.SIBLING_ID AND Q.FT_ID=A.FT_ID),' ') ");
				stringBuilder.append("WHERE A.FT_ID BETWEEN :lowAccountingId and :highAccountingId ");
				stringBuilder.append("AND EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B WHERE A.FT_ID=B.FT_ID AND B.FT_TYPE_FLG IN ('BS','BX')) ");
				stringBuilder.append("AND NOT EXISTS (SELECT C.DST_ID FROM CM_FT_GL_TMP C WHERE A.DST_ID=C.DST_ID) ");
				stringBuilder.append("AND EXISTS (SELECT B.FT_ID FROM CM_FT_INITIAL_INFO B, CI_BSEG_CALC C WHERE A.FT_ID=B.FT_ID ");
				stringBuilder.append("AND B.SIBLING_ID=C.BSEG_ID AND C.EFFDT IS NULL) ");				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowAccountingId", lowAccountingId, "FT_ID");
				preparedStatement.bindString("highAccountingId", highAccountingId, "FT_ID");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			//**********************Extraction of accounting data******************//
			// VALIDATIONS --------------------------------
			
			/**
			 * We are retrieving GL account and amount detail along  with all the details from  CM_FT_INITAIL_INFO and inserting into 
			 * global temp table CM_FT_GL_INITIAL_INFO
			 */

			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_INITIAL_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, SA_ID, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");
				stringBuilder.append("SELECT  B.FT_ID,A.GL_ACCT,A.AMOUNT,B.SIBLING_ID,B.SA_ID,B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CI_FT_GL A, CM_FT_INITIAL_INFO B ");
				stringBuilder.append("WHERE B.FT_ID=A.FT_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			/**
			 * We are retrieving counter party,account id ,person id & per id number  along  with all the details from  CM_FT_GL_INITAIL_INFO and inserting into 
			 * global temp table CM_FT_GL_COUNTERPARTY_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_COUNTERPARTY_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, ");
				stringBuilder.append("COUNTERPARTY, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.SA_ID,E.ACCT_ID,F.PER_ID,F.PER_ID_NBR, ");
				stringBuilder.append("TRIM(C.CHAR_VAL) AS COUNTERPARTY,B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_INITIAL_INFO B, CI_CIS_DIV_CHAR C, CI_SA D, CI_ACCT_PER E,CI_PER_ID F ");
				stringBuilder.append("WHERE B.CIS_DIVISION=C.CIS_DIVISION AND C.CHAR_TYPE_CD='BOLE' ");
				stringBuilder.append("AND B.SA_ID=D.SA_ID AND D.ACCT_ID=E.ACCT_ID AND E.PER_ID=F.PER_ID ");
				stringBuilder.append("AND F.ID_TYPE_CD='EXPRTYID' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving inter-company  along  with all the details from  CM_FT_GL_COUNTERPARTY_INFO  for FTs where FT's division does not have 
			 * intercompany characteristics and inserting into global temp table CM_FT_GL_INTERCOMPANY_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_INTERCOMPANY_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, INTERCOMPANY, ");
				stringBuilder.append("CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,'NA' AS INTERCOMPANY,B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_COUNTERPARTY_INFO B ");
				stringBuilder.append("WHERE B.PER_ID_NBR!=B.COUNTERPARTY OR NOT EXISTS ");
				stringBuilder.append("( SELECT 'X'  FROM CI_CIS_DIV_CHAR G WHERE B.CIS_DIVISION=G.CIS_DIVISION AND G.CHAR_TYPE_CD='INTERC' AND G.CHAR_VAL='Y')  ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving inter-company  along  with all the details from  CM_FT_GL_COUNTERPARTY_INFO  for FTs where FT's division  have 
			 * intercompany characteristics and inserting into global temp table CM_FT_GL_INTERCOMPANY_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_INTERCOMPANY_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, INTERCOMPANY, ");
				stringBuilder.append("CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR,B.COUNTERPARTY, ");
				stringBuilder.append("CASE WHEN EXISTS (SELECT 1 FROM CM_FT_GL_MAP X WHERE X.DST_ID=B.GL_ACCT AND X.INTERCOMPANY_FLG='N')  ");
				stringBuilder.append("THEN 'NA' ELSE B.COUNTERPARTY END ,B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_COUNTERPARTY_INFO B,CI_CIS_DIV_CHAR G ");
				stringBuilder.append("WHERE B.PER_ID_NBR=B.COUNTERPARTY AND B.CIS_DIVISION=G.CIS_DIVISION ");
				stringBuilder.append("AND G.CHAR_TYPE_CD='INTERC' AND G.CHAR_VAL='Y' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving world pay business unit  along  with all the details from  CM_FT_GL_INTERCOMPANY_INFO 
			 * and inserting into global temp table CM_FT_GL_WPBU_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_WPBU_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, INTERCOMPANY, ");
				stringBuilder.append("BUSINESS_UNIT, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD ) ");
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,C.ADHOC_CHAR_VAL AS BUSINESS_UNIT, B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_INTERCOMPANY_INFO B,CI_PER_CHAR C ");
				stringBuilder.append("WHERE B.PER_ID=C.PER_ID AND C.CHAR_TYPE_CD='WPBU' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}	
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving scheme along  with all the details from  CM_FT_GL_WPBU_INFO  for bill segment fts except minimum charges and taxes
			 * and inserting into global temp table CM_FT_GL_SCHEME_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_SCHEME_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, BILLABLE_CHG_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, ");
				stringBuilder.append("INTERCOMPANY, BUSINESS_UNIT, SCHEME, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,C.BILLABLE_CHG_ID,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,B.BUSINESS_UNIT, ");
				stringBuilder.append("CASE WHEN EXISTS (select 1 from CM_FT_GL_MAP X WHERE X.DST_ID=B.GL_ACCT AND X.SCHEME_FLG='N') ");
				stringBuilder.append("THEN 'NA' ELSE E.CHAR_VAL END , ");
				stringBuilder.append("B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_WPBU_INFO B,CI_BSEG_CALC C,CI_BILL_CHG D,CI_PRICEITEM_CHAR E ");
				stringBuilder.append("WHERE B.SIBLING_ID=C.BSEG_ID AND C.BILLABLE_CHG_ID=D.BILLABLE_CHG_ID AND B.FT_TYPE_FLG IN ('BS','BX') ");
				stringBuilder.append("AND D.PRICEITEM_CD=E.PRICEITEM_CD AND E.CHAR_TYPE_CD='SCHEME' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving scheme along  with all the details from  CM_FT_GL_WPBU_INFO  for minimum charges and taxes bill segment fts 
			 * and inserting into global temp table CM_FT_GL_SCHEME_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_SCHEME_INFO "); 
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, BILLABLE_CHG_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, ");  
				stringBuilder.append("INTERCOMPANY, BUSINESS_UNIT, SCHEME, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) "); 
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,C.BILLABLE_CHG_ID,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, "); 
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,B.BUSINESS_UNIT, "); 
				stringBuilder.append("CASE WHEN EXISTS(select 1 from CM_FT_GL_MAP X WHERE X.DST_ID=B.GL_ACCT AND X.SCHEME_FLG='N') ");  
				stringBuilder.append("THEN 'NA' ELSE 'N/A             ' END , "); 
				stringBuilder.append("B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD "); 
				stringBuilder.append("FROM CM_FT_GL_WPBU_INFO B,CI_BSEG_CALC C ");
				stringBuilder.append("WHERE B.SIBLING_ID=C.BSEG_ID AND C.BILLABLE_CHG_ID=' '  AND B.FT_TYPE_FLG IN ('BS','BX') ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving scheme along  with all the details from  CM_FT_GL_WPBU_INFO  for payment and adjustment FTs 
			 * and inserting into global temp table CM_FT_GL_SCHEME_INFO
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_SCHEME_INFO ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, BILLABLE_CHG_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, "); 
				stringBuilder.append("INTERCOMPANY, BUSINESS_UNIT, SCHEME, CIS_DIVISION, CURRENCY_CD, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");	
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,' ' AS BILLABLE_CHG_ID ,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,B.BUSINESS_UNIT,'NA' AS SCHEME, ");
				stringBuilder.append("B.CIS_DIVISION,B.CURRENCY_CD,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_WPBU_INFO B ");
				stringBuilder.append("WHERE B.FT_TYPE_FLG IN ('AD','AX','PS','PX') ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * Update GL_DISTRIB_STATUS flag to 'D' once financial transaction has been extracted into CM_FT_GL_ASL table
			 */
			try {
				stringBuilder.append("UPDATE CI_FT FT SET GL_DISTRIB_STATUS='D' WHERE EXISTS(SELECT 1 FROM CM_FT_GL_SCHEME_INFO SCHEME WHERE SCHEME.FT_ID=FT.FT_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving funding currency for funding merchant along  with all the details from  CM_FT_GL_SCHEME_INFO 
			 * and inserting into global temp table CM_FT_GL_FUND_CURRENCY
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_FUND_CURRENCY ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, BILLABLE_CHG_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, ");
				stringBuilder.append("INTERCOMPANY, BUSINESS_UNIT, SCHEME, CIS_DIVISION, FUND_CURRENCY, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) ");	
				stringBuilder.append("SELECT  B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.BILLABLE_CHG_ID ,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,B.BUSINESS_UNIT,B.SCHEME, ");
				stringBuilder.append("B.CIS_DIVISION,B.CURRENCY_CD AS FUND_CURRENCY,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_SCHEME_INFO B,CI_ACCT_NBR H ");
				stringBuilder.append("WHERE B.ACCT_ID=H.ACCT_ID AND H.ACCT_NBR='FUND' ");
				stringBuilder.append("AND H.ACCT_NBR_TYPE_CD='ACCTTYPE' AND B.GL_ACCT='FND-FX-INC' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * We are retrieving BIN_SETTLE_CURRENCY for funding merchant along  with all the details from  CM_FT_GL_FUND_CURRENCY 
			 * and inserting into global temp table CM_FT_GL_BIN_SETT_CURR
			 * FX table is populated for Funding FX Income which can only be associated with entries related to Billing not payments.
			 * Also for Billing it can only be derived for events coming via Transaction staging table.
			 */
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_BIN_SETT_CURR ");
				stringBuilder.append("(FT_ID, GL_ACCT, AMOUNT, SIBLING_ID, BILLABLE_CHG_ID, SA_ID, ACCT_ID, PER_ID, PER_ID_NBR, COUNTERPARTY, INTERCOMPANY, BUSINESS_UNIT, ");
				stringBuilder.append("SCHEME, CIS_DIVISION, FUND_CURRENCY, BIN_SETTLE_CURRENCY, FT_TYPE_FLG, ACCOUNTING_DT, STATUS_CD) "); 				
				stringBuilder.append("SELECT B.FT_ID,B.GL_ACCT,B.AMOUNT,B.SIBLING_ID,B.BILLABLE_CHG_ID ,B.SA_ID,B.ACCT_ID,B.PER_ID,B.PER_ID_NBR, ");
				stringBuilder.append("B.COUNTERPARTY,B.INTERCOMPANY,B.BUSINESS_UNIT,B.SCHEME, ");
				stringBuilder.append("B.CIS_DIVISION,B.FUND_CURRENCY,T.UDF_CHAR_15,B.FT_TYPE_FLG,B.ACCOUNTING_DT,B.STATUS_CD ");
				stringBuilder.append("FROM CM_FT_GL_FUND_CURRENCY B,CM_TXN_ATTRIBUTES_MAP T ");
				stringBuilder.append("WHERE B.BILLABLE_CHG_ID!=' ' AND B.BILLABLE_CHG_ID=T.BILLABLE_CHG_ID  ");
				stringBuilder.append("AND B.FUND_CURRENCY!=T.UDF_CHAR_15 and T.UDF_CHAR_15 is not NULL ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_ASL_TMP "); 
				stringBuilder.append("(CURRENCY_CD, ACCOUNTING_DT, COUNTERPARTY, BUSINESS_UNIT, COST_CENTRE, INTERCOMPANY, GL_ACCT, ");
				stringBuilder.append("SCHEME, AMOUNT, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,FT_TYPE_FLG) ");
				stringBuilder.append("SELECT A.CURRENCY_CD, A.ACCOUNTING_DT, A.COUNTERPARTY, A.BUSINESS_UNIT, "); 						
				stringBuilder.append("'NA', A.INTERCOMPANY, A.GL_ACCT, A.SCHEME, A.AMOUNT, ");
				stringBuilder.append("SYSTIMESTAMP, :yesStatus, NULL, ");
				stringBuilder.append("CASE  ");
				stringBuilder.append("WHEN A.FT_TYPE_FLG IN ('AD','AX','BS','BX') THEN 'BS' ");
				stringBuilder.append("WHEN A.FT_TYPE_FLG IN ('PS','PX') THEN 'PS' END ");
				stringBuilder.append("FROM CM_FT_GL_SCHEME_INFO A ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("yesStatus",paymentsRequestInterfaceLookUp.getYesStatus().trim(),"STATUS_COD");				
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_FX_TMP  ");
				stringBuilder.append("(FUND_CURRENCY, BIN_SETTLE_CURRENCY, ACCOUNTING_DT, COUNTERPARTY, AMOUNT, BUSINESS_UNIT, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,GL_ACCT) ");
				stringBuilder.append("SELECT A.FUND_CURRENCY,A.BIN_SETTLE_CURRENCY, A.ACCOUNTING_DT, ");
				stringBuilder.append("A.COUNTERPARTY, A.AMOUNT, A.BUSINESS_UNIT,SYSTIMESTAMP, :yesStatus, NULL,A.GL_ACCT ");
				stringBuilder.append("FROM CM_FT_GL_BIN_SETT_CURR A ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("yesStatus",paymentsRequestInterfaceLookUp.getYesStatus().trim(),"STATUS_COD");	
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			
			try {
				preparedStatement = createPreparedStatement("commit","");
				preparedStatement.execute();
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}			
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

		public static String getAccountingErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.getPayReqErrorMessage(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}


		/**
		 * finalizeThreadWork() is execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}
	}

	public static final class AccountingIdData implements Id {

		private static final long serialVersionUID = 1L;

		private String lowAccountingId;

		private String highAccountingId;

		public AccountingIdData(String lowAccountingId, String highAccountingId) {
			setLowAccountingId(lowAccountingId);
			setHighAccountingId(highAccountingId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getHighAccountingId() {
			return highAccountingId;
		}

		public void setHighAccountingId(String highAccountingId) {
			this.highAccountingId = highAccountingId;
		}

		public String getLowAccountingId() {
			return lowAccountingId;
		}

		public void setLowAccountingId(String lowAccountingId) {
			this.lowAccountingId = lowAccountingId;
		}
	}
}