/*******************************************************************************
* FileName                   : AccountingDataAggregationInterface.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : May 09, 2016
* Version Number             : 0.6
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             May 09, 2016        Preeti Tiwari        Separate Batch Job to aggregate Accounting data. 
0.2      NA             Mar 20, 2017        Ankur Jain			 Updated as per NAP-14148  
0.3      NA             Mar 21, 2017        Ankur Jain			 Global temp table and performance changes
0.4      NA             Oct 05, 2017        Ankur Jain		     NAP-19528 Accounting Data interface Redesign
0.5      NA             Dec 18, 2017        Ankur Jain		     PAM-16664 Fix
0.6		 NAP-24109		Mar 21, 2018		Nitika Sharma		 Included ILM_ARCH_SW, ILM_DT
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

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
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (multiThreaded = true, rerunnable = false, 
 *     modules = {"demo"},
 *     softParameters = { @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *      , @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 */

public class AccountingDataAggregationInterface extends AccountingDataAggregationInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(AccountingDataAggregationInterface.class);

	private static final PaymentsRequestInterfaceLookUp paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp(); 

	public JobWork getJobWork() {
		logger.debug("Inside getJobWork()");
		paymentsRequestInterfaceLookUp.setLookUpConstants(); 

		List<ThreadWorkUnit> threadWorkUnitList = getAccountingData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.debug("No of rows selected for processing are - "+ rowsForProcessing);
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
		
	}

	// *********************** getAccounting Method******************************

	/**
	 * getAcounting() method selects Accounting IDs for processing by this Interface.
	 * 
	 * @return List
	 */
	private List<ThreadWorkUnit> getAccountingData() {
		logger.debug("Inside getAccountingData() method");		
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		AccountingIdData accountingIdData = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		String glAcct = "";		
		try {
			    stringBuilder.append("SELECT DISTINCT GL_ACCT FROM CM_FT_GL_ASL_TMP ");
				preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
				preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				glAcct = CommonUtils.CheckNull(resultSet.getString("GL_ACCT"));
				if(!(glAcct.trim().equals("")))
				{	
					accountingIdData = new AccountingIdData(glAcct);				
					threadworkUnit = new ThreadWorkUnit();
					threadworkUnit.setPrimaryId(accountingIdData);
					threadWorkUnitList.add(threadworkUnit);
					threadworkUnit = null;
					accountingIdData = null;
				}
			}
		} catch (ThreadAbortedException e) {
			logger.error("Inside getAccountingData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getAccountingData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}			
		return threadWorkUnitList;
	}

	public Class<AccountingDataAggregationInterfaceWorker> getThreadWorkerClass() {
		return AccountingDataAggregationInterfaceWorker.class;
	}

	public static class AccountingDataAggregationInterfaceWorker extends
	AccountingDataAggregationInterfaceWorker_Gen {

		public AccountingDataAggregationInterfaceWorker() {
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
		 * extracting payments request details from ORMB. It validates the
		 * extracted data and populates the target tables accordingly.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			AccountingIdData accountingIdData = (AccountingIdData) unit.getPrimaryId();
			String accountingId = accountingIdData.getGlAcct();
			logger.debug("accountingId = " + accountingId);
			
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_ASL (ACCOUNTING_ID, CURRENCY_CD, ACCOUNTING_DT, ");
				stringBuilder.append(" COUNTERPARTY, BUSINESS_UNIT, COST_CENTRE, INTERCOMPANY, GL_ACCT, ");
				stringBuilder.append("SCHEME, AMOUNT, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,FT_TYPE_FLG,ILM_DT, ILM_ARCH_SW) ");
				stringBuilder.append("SELECT DBMS_RANDOM.RANDOM, "); 
				stringBuilder.append("B.CURRENCY_CD, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT, "); 						
				stringBuilder.append("B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, SUM(B.AMOUNT) AS AMOUNT, ");
				stringBuilder.append("SYSTIMESTAMP, B.EXTRACT_FLG, B.EXTRACT_DTTM,B.FT_TYPE_FLG,TRUNC(SYSDATE), 'Y' ");
				stringBuilder.append("FROM CM_FT_GL_ASL_TMP B ");
				stringBuilder.append("WHERE B.GL_ACCT = :accountingId ");
				stringBuilder.append("GROUP BY B.CURRENCY_CD, B.ACCOUNTING_DT, B.COUNTERPARTY, ");
				stringBuilder.append("B.BUSINESS_UNIT, B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, SYSTIMESTAMP, B.EXTRACT_FLG, B.EXTRACT_DTTM,B.FT_TYPE_FLG ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingId", accountingId, "GL_ACCT");
				int count=preparedStatement.executeUpdate();
				logger.debug("Rows inserted into table CM_FT_FL_ASL - "+ count);
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
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_FT_GL_FX (ACCOUNTING_ID, FUND_CURRENCY, BIN_SETTLE_CURRENCY, "); 
				stringBuilder.append("ACCOUNTING_DT, COUNTERPARTY, AMOUNT, BUSINESS_UNIT, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ILM_DT, ILM_ARCH_SW) ");
				stringBuilder.append("SELECT DBMS_RANDOM.RANDOM, B.FUND_CURRENCY, "); 
				stringBuilder.append("B.BIN_SETTLE_CURRENCY, "); 
				stringBuilder.append("B.ACCOUNTING_DT, B.COUNTERPARTY, SUM(B.AMOUNT) AS AMOUNT, B.BUSINESS_UNIT, ");
				stringBuilder.append("SYSTIMESTAMP, B.EXTRACT_FLG, B.EXTRACT_DTTM,TRUNC(SYSDATE), 'Y' ");
				stringBuilder.append("FROM CM_FT_GL_FX_TMP B ");
				stringBuilder.append("WHERE B.GL_ACCT = :accountingId ");
				stringBuilder.append("GROUP BY B.FUND_CURRENCY, B.BIN_SETTLE_CURRENCY, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT, ");
				stringBuilder.append("SYSTIMESTAMP, B.EXTRACT_FLG, B.EXTRACT_DTTM ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingId", accountingId, "GL_ACCT");
				int count=preparedStatement.executeUpdate();
				logger.debug("Rows inserted into table CM_FT_FL_FX - "+ count);
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

		private String glAcct;

		public AccountingIdData(String glAcct) {
			setGlAcct(glAcct);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getGlAcct() {
			return glAcct;
		}

		public void setGlAcct(String glAcct) {
			this.glAcct = glAcct;
		}
	}
}