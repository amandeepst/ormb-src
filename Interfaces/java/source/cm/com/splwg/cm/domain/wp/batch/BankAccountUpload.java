/*******************************************************************************
 * FileName                   : BankAccountUpload.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : August 13, 2015
 * Version Number             : 0.2
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             13-Aug-15           Sunaina       Implemented all requirement as per CD2.
 0.2 	  NA			 05-04-2016		     Sunaina	   Updated as per Oracle's Code review.
 0.3	  NA			 Jun 11, 2018		 RIA		   Prepared Statement close
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
import com.splwg.base.domain.common.algorithm.Algorithm_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author rainas403
 *
 @BatchJob (multiThreaded = false, rerunnable = false,
 *      modules = { "demo"})
 */
public class BankAccountUpload extends BankAccountUpload_Gen {
	public static final Logger logger = LoggerFactory.getLogger(BankAccountUpload.class);


/**
* getJobWork() method sends a threadworkunitlist to executeWorkUnit() for processing.
*/
	public JobWork getJobWork() {

		logger.debug("Inside getJobWork() method");
		
		List<ThreadWorkUnit> threadWorkUnitList = getBankAccountData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "
				+ threadWorkUnitList.size());
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}
	
	
//	*********************** getBankAccountData Method******************************
	
	/**
	 * getBankAccountData() method retrieves all the elements from CM_BANK_ACCOUNT table.
	 * 
	 * @return List BankAccount_Id
	 */
	
	private List<ThreadWorkUnit> getBankAccountData() {
		logger.debug("Inside getBankAccountData() method");
		StringBuilder stringBuilder = new StringBuilder();
		PreparedStatement preparedStatement = null;
		BankAccount_Id bankAcctId = null;
		List<BankAccount_Id> rowsForProcessingList = new ArrayList<BankAccount_Id>();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		
		try {
			stringBuilder.append(" SELECT TXN_HEADER_ID, BANK_CD, CIS_DIVISION, " );
			stringBuilder.append(" DESCR, CURRENCY_CD FROM CM_BANK_ACCOUNT " );
			stringBuilder.append(" WHERE TRIM(BO_STATUS_CD) = TRIM(:selectBoStatus1) " );
			stringBuilder.append(" ORDER BY BANK_CD, TXN_HEADER_ID ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("selectBoStatus1", "UPLD", "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);
			
			for (SQLResultRow resultSet : preparedStatement.list()) {
				String transactionHeaderId = resultSet.getString("TXN_HEADER_ID");
				String bankAcctCode = resultSet.getString("BANK_CD");
				String cisDivision = resultSet.getString("CIS_DIVISION");
				String descr = resultSet.getString("DESCR");
				String currencyCode = resultSet.getString("CURRENCY_CD");
				
				bankAcctId = new BankAccount_Id(transactionHeaderId,
						bankAcctCode, cisDivision, descr, currencyCode);
				rowsForProcessingList.add(bankAcctId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(bankAcctId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				bankAcctId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getBankAccountData()", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			stringBuilder = null;
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
		return threadWorkUnitList;
	}

	public Class<BankAccountUploadWorker> getThreadWorkerClass() {
		return BankAccountUploadWorker.class;
	}

	public static class BankAccountUploadWorker extends
	BankAccountUploadWorker_Gen {
		
		public BankAccountUploadWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork() method");
		}
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.debug("Inside createExecutionStrategy() method");
			return new StandardCommitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * every row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to create / update
		 * bank accounts and respective distribution codes.
		 */
		
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() ");
			PreparedStatement preparedStatement = null;
			PreparedStatement preparedStatement1 = null;
			String seq = "";
			int sequence = 0;
			boolean updateToHappen = false;
			
			StringBuilder stringBuilder = null;
			BankAccount_Id bankAcctId = (BankAccount_Id) unit.getPrimaryId();	
			
			try {
				
				validateData(bankAcctId.getTransactionHeaderId());
				
				removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
				setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));

				setPendingState();
				
				stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT NVL((MAX(BANK_ACCT_KEY)+1),9000) AS SEQ FROM CI_BANK_ACCOUNT ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.setAutoclose(false);
				if(notNull(preparedStatement.firstRow())) {
				sequence = Integer.parseInt(preparedStatement.firstRow().getString("SEQ").trim());
				} else {
					sequence = 9000;
				}
				seq = String.valueOf(sequence);
				
					logger.debug("Transaction Header Id - " + bankAcctId.getTransactionHeaderId());
					logger.debug("Bank Account code for processing- " + bankAcctId.getBankAccountCode());

					preparedStatement.close();
					
					stringBuilder = null;
					stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT BANK_CD, CURRENCY_CD FROM CI_BANK_ACCOUNT WHERE ACCOUNT_NBR = :bankCode ");
					
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("bankCode", bankAcctId.getBankAccountCode(), "ACCOUNT_NBR");
					preparedStatement.setAutoclose(false);
					if(notNull(preparedStatement.firstRow())) {
						updateToHappen = true;
						String currency = preparedStatement.firstRow().getString("CURRENCY_CD").trim();
						if(!(currency.equalsIgnoreCase(bankAcctId.getCurrencyCode()))) {
							stringBuilder = null;
							stringBuilder = new StringBuilder();
							stringBuilder.append("UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD = :error, " );
							stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
							stringBuilder.append(" WHERE TXN_HEADER_ID = :txnHdrId AND BANK_CD = :bankCd ");
							
							preparedStatement1 = createPreparedStatement(stringBuilder.toString(),"");
							preparedStatement1.bindString("error", "ERROR", "BO_STATUS_CD");
							preparedStatement1.bindString("msgCatNbr", String.valueOf(CustomMessages.MESSAGE_CATEGORY), "MESSAGE_CAT_NBR");
							preparedStatement1.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CURRENCY_CD_CAN_NOT_BE_UPDATED)), "MESSAGE_NBR");
							preparedStatement1.bindString("errorInfo", getErrorDescription(CustomMessages.BK_CURRENCY_CD_CAN_NOT_BE_UPDATED), "ERROR_INFO");
							preparedStatement1.bindString("txnHdrId", bankAcctId.getTransactionHeaderId(), "TXN_HEADER_ID");
							preparedStatement1.bindString("bankCd", bankAcctId.getBankAccountCode(), "ACCOUNT_NBR");
							preparedStatement1.executeUpdate();
							
								preparedStatement1.close();
								preparedStatement1 = null;
							
						}
					}
					
						preparedStatement.close();
						preparedStatement = null;
					
					
					if(!updateToHappen){
						
						logger.debug("Updating Bank Accounts");
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_DST_CODE " );
						stringBuilder.append(" (DST_ID,GL_CONST_ALG_CD,VERSION,CIS_DIVISION,SA_TYPE_CD,OVRD_SW," );
						stringBuilder.append(" CASH_ACCTNG_DST_ID,CASH_ACCTNG_SW) " );
						stringBuilder.append(" VALUES (:bankAcctNbr, :glAcct, 1, '     ', '        ','N','          ','N') ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindId("glAcct",new Algorithm_Id("CM-GLCON"));
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_DST_CODE_EFF " );
						stringBuilder.append(" (DST_ID,EFFDT,EFF_STATUS,GL_ACCT,VERSION,STATISTICS_CD,FUND_CD) " );
						stringBuilder.append(" VALUES (:bankAcctNbr, TO_TIMESTAMP('01-JAN-01','DD-MON-RR HH24.MI.SSXFF'),'A'," );
						stringBuilder.append(" :bankAcctNbr, 1,'        ','            ') ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_DST_CODE_L " );
						stringBuilder.append(" (LANGUAGE_CD,DST_ID,VERSION,DESCR) " );
						stringBuilder.append(" VALUES (:languageCode, :bankAcctNbr, 1, :descr) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BANK (BANK_CD, VERSION) VALUES (:bankCode, 1)  ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BANK_ACCOUNT " );
						stringBuilder.append(" (BANK_CD,BANK_ACCT_KEY,DST_ID,CURRENCY_CD," );
						stringBuilder.append(" ACCOUNT_NBR,CHECK_DIGIT,BRANCH_ID,DFI_ID_NUM,VERSION) " );
						stringBuilder.append(" VALUES (:bankCode,:bankAcctKey,:bankAcctNbr,:currencyCode,:bankAcctNbr,'  ','          ',' ',1)  ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.bindString("bankAcctKey", seq, "BANK_ACCT_KEY");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindString("currencyCode", bankAcctId.getCurrencyCode(), "CURRENCY_CD");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
								

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BANK_ACCOUNT_L " );
						stringBuilder.append(" (BANK_ACCT_KEY,BANK_CD,LANGUAGE_CD,VERSION,DESCR) " );
						stringBuilder.append(" VALUES (:bankAcctKey, :bankCode, :languageCode, 1, :descr) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.bindString("bankAcctKey", seq, "BANK_ACCT_KEY");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BANK_L " );
						stringBuilder.append(" (BANK_CD,LANGUAGE_CD,VERSION,DESCR) VALUES " );
						stringBuilder.append(" (:bankCode, :languageCode, 1, :descr) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" Insert into CI_TNDR_SRCE " );
						stringBuilder.append(" (TNDR_SOURCE_CD,TNDR_SRCE_TYPE_FLG,SA_ID," );
						stringBuilder.append(" CURRENCY_CD,EXT_SOURCE_ID,BANK_ACCT_KEY,BANK_CD,VERSION,DFLT_START_BALANCE,MAX_AMT_BALANCE) " );
						stringBuilder.append(" values (:bankCode,:adhcSource,'          ',:currency,:bankAcctNbr,:bankAcctKey,:bankCode,1,0,999999999) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.bindString("bankAcctKey", seq, "BANK_ACCT_KEY");
						preparedStatement.bindString("adhcSource", "ADHC", "TNDR_SRCE_TYPE_FLG");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "EXT_SOURCE_ID");
						preparedStatement.bindString("currency", bankAcctId.getCurrencyCode(), "CURRENCY_CD");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					

						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" Insert into CI_TNDR_SRCE_L " );
						stringBuilder.append(" (TNDR_SOURCE_CD,LANGUAGE_CD,DESCR,VERSION) " );
						stringBuilder.append(" values (:bankCode,:languageCode,:descr,1) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankCode", "B".concat(seq), "BANK_CD");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append("UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP " );
						stringBuilder.append( "WHERE BO_STATUS_CD = :selectBoStatus AND TXN_HEADER_ID = :txnHeaderId " );
						stringBuilder.append(" AND BANK_CD = :bankAcct");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("newBoStatus", "COMPLETED", "BO_STATUS_CD");
						preparedStatement.bindString("selectBoStatus", "PENDING", "BO_STATUS_CD");
						preparedStatement.bindString("txnHeaderId", bankAcctId.getTransactionHeaderId(), "txnHeaderId");
						preparedStatement.bindString("bankAcct", bankAcctId.getBankAccountCode(), "ACCOUNT_NBR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
					} else {
						
						logger.debug("Updating the descriptions in table");
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_DST_CODE_L SET DESCR = :descr " );
						stringBuilder.append(" WHERE DST_ID = :bankAcctNbr AND LANGUAGE_CD = :languageCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_BANK_ACCOUNT_L SET DESCR = :descr " );
						stringBuilder.append("WHERE BANK_CD = " );
						stringBuilder.append(" (SELECT BANK_CD FROM CI_BANK_ACCOUNT WHERE ACCOUNT_NBR = :bankAcctNbr)  " );
						stringBuilder.append(" AND LANGUAGE_CD = :languageCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_BANK_L SET DESCR = :descr " );
						stringBuilder.append("WHERE BANK_CD = " );
						stringBuilder.append(" (SELECT BANK_CD FROM CI_BANK_ACCOUNT WHERE ACCOUNT_NBR = :bankAcctNbr)  " );
						stringBuilder.append(" AND LANGUAGE_CD = :languageCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "DST_ID");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_TNDR_SRCE_L SET DESCR = :descr " );
						stringBuilder.append("WHERE TNDR_SOURCE_CD = " );
						stringBuilder.append(" (SELECT TNDR_SOURCE_CD FROM CI_TNDR_SRCE WHERE EXT_SOURCE_ID = :bankAcctNbr)  " );
						stringBuilder.append(" AND LANGUAGE_CD = :languageCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("bankAcctNbr", bankAcctId.getBankAccountCode(), "EXT_SOURCE_ID");
						preparedStatement.bindString("languageCode", "ENG", "LANGUAGE_CD");
						preparedStatement.bindString("descr", bankAcctId.getDescr(), "DESCR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
						stringBuilder = null;
						stringBuilder = new StringBuilder();
						stringBuilder.append("UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP " );
						stringBuilder.append("WHERE BO_STATUS_CD = :selectBoStatus AND TXN_HEADER_ID = :txnHeaderId " );
						stringBuilder.append(" AND BANK_CD = :bankAcct");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("newBoStatus", "COMPLETED", "BO_STATUS_CD");
						preparedStatement.bindString("selectBoStatus", "PENDING", "BO_STATUS_CD");
						preparedStatement.bindString("txnHeaderId", bankAcctId.getTransactionHeaderId(), "txnHeaderId");
						preparedStatement.bindString("bankAcct", bankAcctId.getBankAccountCode(), "ACCOUNT_NBR");
						preparedStatement.executeUpdate();
					
							preparedStatement.close();
							preparedStatement = null;
					
						
					}

				
				
			} catch (Exception e){
				logger.error("Exception in executeWorkUnit data ", e);
				rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP," );
				stringBuilder.append( " MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo "); 
				stringBuilder.append( "WHERE BO_STATUS_CD = :selectBoStatus AND TXN_HEADER_ID = :txnHeaderId " );
				stringBuilder.append(" AND BANK_CD = :bankAcct");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("newBoStatus", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("selectBoStatus", "PENDING", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", String.valueOf(CustomMessages.MESSAGE_CATEGORY), "MESSAGE_CAT_NBR");
				preparedStatement.bindString("msgNbr", String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", "Run Time error in execution", "ERROR_INFO");
				preparedStatement.bindString("txnHeaderId", bankAcctId.getTransactionHeaderId(), "txnHeaderId");
				preparedStatement.bindString("bankAcct", bankAcctId.getBankAccountCode(), "DST_ID");
				preparedStatement.executeUpdate();
				
					preparedStatement.close();
					preparedStatement = null;
				
			}
			
			return true;
		} // end of execute work unit
		
		protected final void removeSavepoint(String savePointName)
		{
			FrameworkSession session = (FrameworkSession)SessionHolder.getSession();
			if (session.hasActiveSavepointWithName(savePointName)) {
				session.removeSavepoint(savePointName);
			}
		}
		protected final void setSavePoint(String savePointName){
			// Create save point before any change is done for the current transaction.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.setSavepoint(savePointName);
		}

		protected final void rollbackToSavePoint(String savePointName){
			// In case error occurs, rollback all changes for the current transaction and log error.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}

		private void setPendingState(){
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			
//			Set records that would be processed as PENDING
			try {
				
				stringBuilder.append("UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP "); 
				stringBuilder.append("WHERE TRIM(BO_STATUS_CD) = TRIM(:selectBoStatus) ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("newBoStatus", "PENDING", "BO_STATUS_CD");
				preparedStatement.bindString("selectBoStatus", "UPLD", "BO_STATUS_CD");
				preparedStatement.executeUpdate();
				
			} catch (Exception e) {
				logger.error("Exception in getBankAccountData()", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		public void commit() {
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement("COMMIT","");
				preparedStatement.execute();
			} catch (RuntimeException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally{
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		
		/**
		 * validateData() method will validate the records for Bank Account Upload.
		 * 
		 * @param inputTable
		 */
		
		private void validateData(String txnHeaderId) {
			logger.debug("Inside Valdidate Data");
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String messageCatNbr = String.valueOf(CustomMessages.MESSAGE_CATEGORY);
			try {
				
				stringBuilder.append(" UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD = :error, STATUS_UPD_DTTM = SYSTIMESTAMP," );
				stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo "); 
				stringBuilder.append(" WHERE  TRIM(BANK_CD) IS NULL ");
				stringBuilder.append(" AND TXN_HEADER_ID = :txnHeaderId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("txnHeaderId", txnHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_BANK_ACCT_NOT_FOUND)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BK_BANK_ACCT_NOT_FOUND), "ERROR_INFO");
				int count=preparedStatement.executeUpdate();
				
					preparedStatement.close();
					preparedStatement = null;
				
				commit();
				if (count>0){
					logger.debug("count"+count);
					addError(CustomMessageRepository.merchantError(
							CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_BANK_ACCT_NOT_FOUND))));
				}
				
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append(" UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD = :error,STATUS_UPD_DTTM = SYSTIMESTAMP," );
				stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
				stringBuilder.append(" WHERE  (TRIM(CIS_DIVISION) IS NULL " );
				stringBuilder.append(" OR TRIM(CIS_DIVISION) NOT IN (SELECT DISTINCT CIS_DIVISION FROM CI_CIS_DIVISION)) ");
				stringBuilder.append(" AND TXN_HEADER_ID = :txnHeaderId ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("txnHeaderId", txnHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CIS_DIVISION_NOT_FOUND)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BK_CIS_DIVISION_NOT_FOUND), "ERROR_INFO");
				int count1=preparedStatement.executeUpdate();
				
					preparedStatement.close();
					preparedStatement = null;
				
				commit();
				if (count1>0){
				addError(CustomMessageRepository.merchantError(
						CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CIS_DIVISION_NOT_FOUND))));
				}
				
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append(" UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD = :error,STATUS_UPD_DTTM = SYSTIMESTAMP," );
				stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
				stringBuilder.append(" WHERE  TRIM(DESCR) IS NULL " );
				stringBuilder.append(" AND TXN_HEADER_ID = :txnHeaderId ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("txnHeaderId", txnHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_DESCR_NOT_FOUND)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BK_DESCR_NOT_FOUND), "ERROR_INFO");
				int count2=preparedStatement.executeUpdate();
				
					preparedStatement.close();
					preparedStatement = null;
				
				commit();
				if (count2>0){
				addError(CustomMessageRepository.merchantError(
						CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_DESCR_NOT_FOUND))));
				}
				
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append(" UPDATE CM_BANK_ACCOUNT SET BO_STATUS_CD = :error,STATUS_UPD_DTTM = SYSTIMESTAMP," );
				stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
				stringBuilder.append(" WHERE  (TRIM(CURRENCY_CD) IS NULL " );
				stringBuilder.append(" OR TRIM(CURRENCY_CD) NOT IN (SELECT DISTINCT CURRENCY_CD FROM CI_CURRENCY_CD)) ");
				stringBuilder.append(" AND TXN_HEADER_ID = :txnHeaderId ");
						
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("txnHeaderId", txnHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CURRENCY_CD_NOT_FOUND)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BK_CURRENCY_CD_NOT_FOUND), "ERROR_INFO");
				int count3=preparedStatement.executeUpdate();
				
					preparedStatement.close();
					preparedStatement = null;
				
				commit();
				if (count3>0){
				addError(CustomMessageRepository.merchantError(
						CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CURRENCY_CD_NOT_FOUND))));
				}
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("UPDATE CM_BANK_ACCOUNT BK1 SET BO_STATUS_CD = :error,STATUS_UPD_DTTM = SYSTIMESTAMP, " );
				stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
				stringBuilder.append(" WHERE EXISTS(SELECT 1 FROM CI_BANK_ACCOUNT BK2 " );
				stringBuilder.append(" WHERE BK2.ACCOUNT_NBR = BK1.BANK_CD " );
				stringBuilder.append(" AND BK2.DST_ID = BK1.BANK_CD " );
				stringBuilder.append(" AND BK2.CURRENCY_CD <> BK1.CURRENCY_CD ) ");
				stringBuilder.append(" AND TXN_HEADER_ID = :txnHeaderId ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("txnHeaderId", txnHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CURRENCY_CD_CAN_NOT_BE_UPDATED)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BK_CURRENCY_CD_CAN_NOT_BE_UPDATED), "ERROR_INFO");
				int count4=preparedStatement.executeUpdate();
				
				
				
				commit();
				if (count4>0){
				addError(CustomMessageRepository.merchantError(
						CommonUtils.CheckNull(String.valueOf(CustomMessages.BK_CURRENCY_CD_CAN_NOT_BE_UPDATED))));
				}
				
			} catch (Exception e) {
				logger.error("Exception in validateData()", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
		}
		
		//Extracting error info from Application server
		public static String getErrorDescription(int messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.billCycleError(CommonUtils.CheckNull(String.valueOf(messageNumber))).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}
		
		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");	
			super.finalizeThreadWork();
		}
		
	} // end of worker class
	public static final class BankAccount_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String transactionHeaderId;
		private String bankAccountCode;
		private String cisDivision;
		private String descr;
		private String currencyCode;
		
		
		public BankAccount_Id(String transactionHeaderId, String bankAccountCode,
				String cisDivision, String descr, String currencyCode) {
			setTransactionHeaderId(transactionHeaderId);
			setBankAccountCode(bankAccountCode);
			setCisDivision(cisDivision);
			setDescr(descr);
			setCurrencyCode(currencyCode);
		} 

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}
		
		public String getTransactionHeaderId() {
			return transactionHeaderId;
		}

		public void setTransactionHeaderId(String transactionHeaderId) {
			this.transactionHeaderId = transactionHeaderId;
		}

		public String getBankAccountCode() {
			return bankAccountCode;
		}

		public void setBankAccountCode(String bankAccountCode) {
			this.bankAccountCode = bankAccountCode;
		}

		public String getCisDivision() {
			return cisDivision;
		}

		public void setCisDivision(String cisDivision) {
			this.cisDivision = cisDivision;
		}

		public String getDescr() {
			return descr;
		}

		public void setDescr(String descr) {
			this.descr = descr;
		}

		public String getCurrencyCode() {
			return currencyCode;
		}

		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}

		
	}// end of Id Class
	
}
