/*******************************************************************************
 * FileName                   : CmAccountingDataAggregationInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 09, 2016
 * Version Number             : 0.6
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1		 NA				May	21, 2018		Kaustubh Kale		 Initial Version
0.2		 NA				Jun 14, 2018		Kaustubh Kale		 Changes for batch rerun 
0.3		 NA				Jul 23, 2018		Kaustubh Kale		 Added ILM_DT and ILM_ARCH_SW for Insert
0.4		 NA				Aug 07, 2018		Kaustubh Kale		 Added group by SIGN(B.AMOUNT)  
0.5		 NA			    Aug 21, 2018		Kaustubh Kale		 Added ACCT_NBR extra column (NAP-31893) 
0.6		 NA			    Nov 13, 2018		Vienna Rom			 Set UPLOAD_DTTM as systimestamp 
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
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author RIA
 *
 * @BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *            , @BatchJobSoftParameter (name = txnSourceCode, type = string)})
 */

public class CmAccountingDataAggregationInterface extends CmAccountingDataAggregationInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CmAccountingDataAggregationInterface.class);

	private static final PaymentsRequestInterfaceLookUp paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp(); 

	public JobWork getJobWork() {
		logger.debug("Inside getJobWork()");
		paymentsRequestInterfaceLookUp.setLookUpConstants(); 
		
		BigInteger batchRerunNumber = getBatchRerunNumber();
		if(notNull(batchRerunNumber) || batchRerunNumber.intValue() != 0) {
			deleteOutboundStagingRecords();
		}

		List<ThreadWorkUnit> threadWorkUnitList = getAccountingData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.debug("No of rows selected for processing are - "+ rowsForProcessing);
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	/**
	 * Delete CM_FT_GL_ASL and CM_FT_GL_FX is batch rerun number is not null or 0
	 */
	private void deleteOutboundStagingRecords() {
		logger.debug("Inside deleteOutboundStagingRecords() method");
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			stringBuilder.append(" DELETE FROM CM_FT_GL_ASL WHERE BATCH_CD=:batchCode AND BATCH_NBR=:batchNumber ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindId("batchCode", getBatchControlId());
			preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
			preparedStatement.execute();
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteOutboundStagingRecords() method, Error deleting CM_FT_GL_ASL -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside deleteOutboundStagingRecords() method, Error deleting CM_FT_GL_ASL -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}	
		}
		
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append(" DELETE FROM CM_FT_GL_FX WHERE BATCH_CD=:batchCode AND BATCH_NBR=:batchNumber ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindId("batchCode", getBatchControlId());
			preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
			preparedStatement.execute();
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteOutboundStagingRecords() method, Error deleting CM_FT_GL_FX -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside deleteOutboundStagingRecords() method, Error deleting CM_FT_GL_FX -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}	
		}
	}

	/**
	 * getAcounting() method selects Accounting IDs for processing by this Interface.
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
			stringBuilder.append(" SELECT DISTINCT(FTGL.GL_ACCT) ");
			stringBuilder.append(" FROM CM_FT_GL_ASL_STG FTGL, CI_FT_PROC FP ");
			stringBuilder.append(" WHERE FP.BATCH_CD =:batchCode ");
			stringBuilder.append(" AND FP.BATCH_NBR =:batchNumber ");
			stringBuilder.append(" AND FP.FT_ID = FTGL.FT_ID ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindId("batchCode", getBatchControlId());
			preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
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

	public Class<CmAccountingDataAggregationInterfaceWorker> getThreadWorkerClass() {
		return CmAccountingDataAggregationInterfaceWorker.class;
	}

	public static class CmAccountingDataAggregationInterfaceWorker extends
	CmAccountingDataAggregationInterfaceWorker_Gen {

		public CmAccountingDataAggregationInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0) throws ThreadAbortedException, RunAbortedException {
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
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());	
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			AccountingIdData accountingIdData = (AccountingIdData) unit.getPrimaryId();
			String accountingId = accountingIdData.getGlAcct();
			logger.debug("accountingId = " + accountingId);

			// Insert data into CM_FT_GL_ASL
			try {
				stringBuilder.append(" INSERT INTO CM_FT_GL_ASL ");
				stringBuilder.append(" (ACCOUNTING_ID, CURRENCY_CD, ACCOUNTING_DT, COUNTERPARTY, BUSINESS_UNIT, ");
				stringBuilder.append(" COST_CENTRE, INTERCOMPANY, GL_ACCT, SCHEME, AMOUNT, ");
				stringBuilder.append(" UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, FT_TYPE_FLG, ILM_DT, ILM_ARCH_SW, BATCH_CD, BATCH_NBR, ACCT_NBR) "); // NAP-31893
				stringBuilder.append(" SELECT ");
				stringBuilder.append(" B.ACCOUNTING_ID, B.CURRENCY_CD, B.ACCOUNTING_DT, ");
				stringBuilder.append(" B.COUNTERPARTY, B.BUSINESS_UNIT, B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, SUM(B.AMOUNT) AS AMOUNT, ");
				stringBuilder.append(" SYSTIMESTAMP, :extractFlagParameter, :processDate, B.FT_TYPE_FLG, :processDate, 'N', :batchCode, :batchNumber, B.ACCT_NBR "); // NAP-31893
				stringBuilder.append(" FROM CM_FT_GL_ASL_STG B, CI_FT_PROC FP ");
				stringBuilder.append(" WHERE B.GL_ACCT = :accountingId ");
				stringBuilder.append(" AND FP.FT_ID = B.FT_ID ");
				stringBuilder.append(" AND FP.BATCH_CD = :batchCode ");
				stringBuilder.append(" AND FP.BATCH_NBR = :batchNumber ");
				stringBuilder.append(" GROUP BY B.CURRENCY_CD, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT, ");
				stringBuilder.append(" B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, B.FT_TYPE_FLG, B.ACCOUNTING_ID, SIGN(B.AMOUNT), B.ACCT_NBR "); // NAP-31893

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
				preparedStatement.bindString("extractFlagParameter","Y", "");
				preparedStatement.bindString("accountingId", accountingId, "GL_ACCT");
				preparedStatement.bindId("batchCode", getBatchControlId());
				preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
				int count=preparedStatement.executeUpdate();

				logger.debug("Rows inserted into table CM_FT_GL_ASL - "+ count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error inserting CM_FT_GL_ASL -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error inserting CM_FT_GL_ASL -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			commit();
			
			// Insert data into CM_FT_GL_FX
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_FT_GL_FX ");
				stringBuilder.append(" (ACCOUNTING_ID, FUND_CURRENCY, BIN_SETTLE_CURRENCY, ACCOUNTING_DT, ");
				stringBuilder.append(" COUNTERPARTY, AMOUNT, BUSINESS_UNIT, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, BATCH_CD, BATCH_NBR) ");
				stringBuilder.append(" SELECT  ");
				stringBuilder.append(" B.ACCOUNTING_ID, B.FUND_CURRENCY,  ");
				stringBuilder.append(" B.BIN_SETTLE_CURRENCY, B.ACCOUNTING_DT, B.COUNTERPARTY, SUM(B.AMOUNT) AS AMOUNT,  ");
				stringBuilder.append(" B.BUSINESS_UNIT, :processDate, :extractFlagParameter, :processDate, :processDate, 'N', :batchCode, :batchNumber ");
				stringBuilder.append(" FROM CM_FT_GL_FX_STG B ");
				stringBuilder.append(" WHERE B.GL_ACCT = :accountingId ");
				stringBuilder.append(" GROUP BY B.FUND_CURRENCY, B.BIN_SETTLE_CURRENCY, ");
				stringBuilder.append(" B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT, B.ACCOUNTING_ID ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
				preparedStatement.bindString("extractFlagParameter", "Y", "");
				preparedStatement.bindString("accountingId", accountingId, "GL_ACCT");
				preparedStatement.bindId("batchCode", getBatchControlId());
				preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
				int count=preparedStatement.executeUpdate();

				logger.debug("Rows inserted into table CM_FT_GL_FX - "+ count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error inserting CM_FT_GL_FX -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error inserting CM_FT_GL_FX -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			commit();

			return true;
		}

		/**
		 * Commit data
		 */
		private void commit() {
			PreparedStatement ps = null;
			try {
				ps = createPreparedStatement("commit","");
				ps.execute();
			} catch (RuntimeException e) {
				logger.error("Inside commit() method, Error commiting data -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (ps != null) {
					ps.close();
					ps = null;
				}
			}
		}

		/**
		 * Get Accounting Error Description
		 * @param messageNumber
		 * @return
		 */
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
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
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