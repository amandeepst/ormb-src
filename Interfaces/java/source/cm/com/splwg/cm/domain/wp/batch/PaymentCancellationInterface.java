/*******************************************************************************
* FileName                   : PaymentCancellationInterface.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : April 25, 2013
* Version Number             : 0.5
* Revision History     		 : 
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             31-May-2013         Gaurav Sood    Implemented all requirement as in TS version 1.0.
0.2      NA             12-Aug-2015         Preeti Tiwari  Implemented all requirements as per CD2.
0.4      NA             02-Nov-2015         Preeti Tiwari  Corrected the changes done in v0.3.
0.5      NA             15-Dec-2017         Vienna Rom     Redesigned to multi-thread, perf fixes
0.6		 NA				20-Apr-2018			Kaustubh K	   Threading at account level and ILM changes
******************************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.businessObject.COTSInstanceNode;
import com.splwg.base.api.businessService.BusinessServiceDispatcher;
import com.splwg.base.api.businessService.BusinessServiceInstance;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.domain.admin.paymentCancelReason.PaymentCancelReason_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
 *  @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"})
 */

public class PaymentCancellationInterface extends
		PaymentCancellationInterface_Gen { 
	
	public static final Logger logger = LoggerFactory.getLogger(PaymentCancellationInterface.class);
	
	public JobWork getJobWork() {
		
		List<ThreadWorkUnit> threadWorkUnitList = getStagingData();
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}
	public Class<PaymentCancellationInterfaceWorker> getThreadWorkerClass() {
		return PaymentCancellationInterfaceWorker.class;
	}

	public static class PaymentCancellationInterfaceWorker extends
			PaymentCancellationInterfaceWorker_Gen {
		
		private static final String PAY_CANCEL_BUSINESS_SERVICE = "CMPayDel";
		private static final String CREATE_FROZEN_ADJ_BUSINESS_SERVICE = "C1-CreateFrozenAdjustment";
		private static final String ZERO = "0";
		
		private ArrayList<ArrayList<String>> updatePaymentStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> eachPaymentStatusList = null;
		
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
//			PaymentCancelStagingMatchVal_Id matchValId = (PaymentCancelStagingMatchVal_Id) unit.getPrimaryId();
			PaymentCancelStaging_Id paymentRow;
			// RIA: Threading on Account ID
			Account_Id acctId = (Account_Id) unit.getPrimaryId();
			List<SQLResultRow> billList = fetchBillsForAccount(acctId);
			
			for(SQLResultRow bill: billList) {
				String txnHeaderId = "";
				String boStatusCd = "";
				String currencyCd = "";
				BigDecimal tenderAmt = null;
				String extReferenceId = "";
				String matchVal = "";
				String canRsnCd = "";
				PreparedStatement preparedStatement = null;
				String billId = bill.getString("BILL_ID").trim();
				
				// RIA: Check CM_BILL_DUE_DT if records exist for current bill
				int count = checkBillDueData(billId);
				if(count > 0) {
					try {
						//Retrieve staging records having the same match value
						StringBuilder stringBuilder = new StringBuilder();
						stringBuilder.append("SELECT TXN_HEADER_ID, BO_STATUS_CD, ");
						stringBuilder.append("CURRENCY_CD, TENDER_AMT, EXT_REFERENCE_ID, ");
						stringBuilder.append("MATCH_VAL, CAN_RSN_CD FROM CM_PAY_RJCT_STG ");
						stringBuilder.append("WHERE BO_STATUS_CD=:selectBoStatus1 AND MATCH_VAL=:matchValId ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
						preparedStatement.bindString("selectBoStatus1", "UPLD", "BO_STATUS_CD");
						preparedStatement.bindString("matchValId", billId, "MATCH_VAL");
						preparedStatement.setAutoclose(false);

						for (SQLResultRow resultSet : preparedStatement.list()) {
							txnHeaderId = resultSet.getString("TXN_HEADER_ID");
							boStatusCd = resultSet.getString("BO_STATUS_CD");
							currencyCd = resultSet.getString("CURRENCY_CD");
							tenderAmt = resultSet.getBigDecimal("TENDER_AMT");
							extReferenceId = resultSet.getString("EXT_REFERENCE_ID");
							matchVal = resultSet.getString("MATCH_VAL");
							canRsnCd = resultSet.getString("CAN_RSN_CD");

							paymentRow = new PaymentCancelStaging_Id(txnHeaderId, boStatusCd, currencyCd, tenderAmt,
									extReferenceId, matchVal, canRsnCd);

							//Reset savepoint per staging record
							removeSavepoint("Rollback".concat(getParameters().getThreadCount().toString()));
							setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));

							//Process staging record
							processPaymentCancelStagingRecord(paymentRow);
						}	
					} catch(Exception e) {
						logger.error("Exception occurred in executeWorkUnit()" , e);
						throw new RunAbortedException(CustomMessageRepository
								.exceptionInExecution(e.getMessage()));
					} finally {
						if (preparedStatement != null) {
							preparedStatement.close();
							preparedStatement = null;
						}
					}	
					// RIA: Update IS_MERCH_BALANCED to "N" for bill
					updateCmBillDueDtToCncl(billId);
				}
				else {
					// RIA: Update CM_PAY_RJCT_STG for current bill
					updateStagingStatusForBill(billId, "ERROR", ZERO, ZERO, " ");
				}	
			}
			return true;
		}

		/**
		 * Fetch List of Bills for account
		 * @param acctId
		 * @return List of Bills
		 */
		private List<SQLResultRow> fetchBillsForAccount(Account_Id acctId) {
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("SELECT DISTINCT B.BILL_ID ");
			stringBuilder.append("FROM CI_BILL B, CM_PAY_RJCT_STG PR ");
			stringBuilder.append("WHERE B.BILL_ID = PR.MATCH_VAL ");
			stringBuilder.append("AND PR.BO_STATUS_CD =:boStatus ");
			stringBuilder.append("AND B.ACCT_ID =:acctId ");
			PreparedStatement preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("boStatus", "UPLD", "BO_STATUS_CD");
			preparedStatement.bindId("acctId", acctId);
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			
			if(notNull(preparedStatement)){
				preparedStatement.close();
			}
			
			return resultList;
		}

		/**
		 * Check if records exist in CM_BILL_DUE_DT table for match val
		 * @param matchVal
		 * @return count
		 */
		private int checkBillDueData(String matchVal) {
			int count = 0;
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("SELECT BILL_ID FROM CM_BILL_DUE_DT ");
			stringBuilder.append("WHERE BILL_ID =:matchValId ");
			PreparedStatement preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("matchValId", matchVal, "MATCH_VAL");
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> row = preparedStatement.list();
			if(notNull(row)) {
				count = row.size();
			}
			
			if(notNull(preparedStatement)){
				preparedStatement.close();
			}
			
			return count;
		}

		/**
		 * Validate payment cancellation staging record and if valid, cancel payment and update CM_BILL_DUE_DT table.
		 * @param paymentRow
		 */
		private void processPaymentCancelStagingRecord(PaymentCancelStaging_Id paymentRow) {
			String messageCategoryNumber = ZERO;
			String messageNumber = ZERO;

			//Error if payment cancel reason is not valid
			if(isNull((new PaymentCancelReason_Id(paymentRow.getCanRsnCd())).getEntity())) {
				logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
						CustomMessages.PAYMENT_INVALID_CANCEL_REASON, 
						getErrorDescription(CustomMessages.PAYMENT_INVALID_CANCEL_REASON));
			}

			//Validate payment cancellation
			String paymentId = validatePayment(paymentRow);
			String paymentStatus = "";
			if (notBlank(paymentId)){	
				//Cancel payment
				paymentStatus = cancelPayment(paymentRow,paymentId);
			
				//Error if payment cancellation is not successful
				if (CommonUtils.CheckNull(paymentStatus).trim().startsWith("false")) {
					String[] returnStatusArray = paymentStatus.split("~");
					if(returnStatusArray[1].contains("Text:")){
						returnStatusArray[1] = returnStatusArray[1].replace("Text:", "");
					}
					logError(paymentRow.getTxnHeaderId(), Integer.parseInt(returnStatusArray[2].trim()), 
							Integer.parseInt(returnStatusArray[3].trim()), returnStatusArray[1].trim());
				}
				//Else, proceed to update CM tables
				else if (CommonUtils.CheckNull(paymentStatus).trim().startsWith("true")) {
					//Set merchant as Not Balanced in Bill Due Date table
//					int dueDtRowUpdated = updateCmBillDueDtToCncl(paymentRow.getMatchVal());
//					if (dueDtRowUpdated==0) {
//						logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
//								CustomMessages.PAY_CNF_DUE_DT, 
//								getErrorDescription(CustomMessages.PAY_CNF_DUE_DT));
//					}

					//Update staging status to COMPLETED
					updateStagingStatus(paymentRow.getTxnHeaderId(), "COMPLETED", messageCategoryNumber, messageNumber, " ");
				}
			}
		}
		
		/**
		 * Retrieve payment event based on external reference id and use it to validate the staging record.
		 * Then fetch the corresponding payment id.
		 * @param paymentRow
		 * @param paymentId
		 * @return String
		 */
		private String validatePayment(PaymentCancelStaging_Id paymentRow){
			String paymentId = "";
			String paymentEventId = "";
			String currencyCd="";
			BigDecimal tenderAmt=null;
			String matchVal="";
			PreparedStatement preparedStatement = null;
			try{
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT B.CURRENCY_CD, B.TENDER_AMT, B.MATCH_VAL, B.PAY_EVENT_ID ");
				stringBuilder.append("FROM CM_PAY_RJCT_STG A, CI_PEVT_DTL_ST B ");
				stringBuilder.append("WHERE trim(A.EXT_REFERENCE_ID) = trim(B.EXT_REFERENCE_ID) ");
				stringBuilder.append("AND trim(A.EXT_REFERENCE_ID) = trim(:refId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("refId", paymentRow.getExtReferenceId(), "ext_reference_id");
				preparedStatement.setAutoclose(false);
				List<SQLResultRow> rows = preparedStatement.list();
				if(rows.size() == 0){
					//Error if ext ref id is not found
					logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
							CustomMessages.PAYMENT_UPLD_STG_NO_DATA_FOUND, getErrorDescription(CustomMessages.PAYMENT_UPLD_STG_NO_DATA_FOUND));
					return "" ;
				}
				for (SQLResultRow resultSet : rows) {

					currencyCd = resultSet.getString("CURRENCY_CD");
					tenderAmt = resultSet.getBigDecimal("TENDER_AMT");
					matchVal = resultSet.getString("MATCH_VAL");
					paymentEventId = resultSet.getString("PAY_EVENT_ID");
					
					if(!currencyCd.equalsIgnoreCase(paymentRow.getCurrencyCd())){
						//Error if CURRENCY_CD is not matching
						logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
								CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES, getErrorDescription(CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES));
						return "" ;
					}
					if(!(tenderAmt.compareTo(paymentRow.getTenderAmt())==0)){
						//Error if TENDER_AMT is not matching
						logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
								CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES, getErrorDescription(CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES));
						return "" ;
					}

					if(!matchVal.equalsIgnoreCase(paymentRow.getMatchVal())){
						//Error if MATCH_VAL is not matching
						logError(paymentRow.getTxnHeaderId(), CustomMessages.MESSAGE_CATEGORY,
								CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES, getErrorDescription(CustomMessages.PAYMENT_UPLD_NO_MATCH_STG_VALUES));
						return "" ;
					}
				}
				
				//Fetch payment id
				paymentId = fetchPaymentId(paymentRow, paymentEventId);
			}
			catch (Exception e){
				logger.error("Inside catch block of createPayments() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}
			finally{
				if(preparedStatement != null){
					preparedStatement.close();
				}
			}
			return paymentId;
		}
		
		/**
		 * Fetch corresponding payment id of payment event id
		 * @param paymentRow
		 * @param paymentEventId
		 * @return
		 */
		private String fetchPaymentId(PaymentCancelStaging_Id paymentRow, String paymentEventId){
			String paymentId = "";
			PreparedStatement preparedStatement = null;
			try{
				preparedStatement = createPreparedStatement("SELECT PAY_ID FROM ci_pay WHERE pay_event_id = :eventId","");
				preparedStatement.bindString("eventId", paymentEventId, "pay_event_id");
				preparedStatement.setAutoclose(false);
				List<SQLResultRow> rows = preparedStatement.list();
				if(rows.size() == 0){
					//Error if payment id is not found				
					logError(paymentRow.getTxnHeaderId(),  CustomMessages.MESSAGE_CATEGORY, 
							CustomMessages.PAYMENT_UPLD_NO_PAYMENT_ID, getErrorDescription(CustomMessages.PAYMENT_UPLD_NO_PAYMENT_ID));
					return "" ;
				}
				for (SQLResultRow resultSet : rows) {
					paymentId = resultSet.getString("PAY_ID");
				}
			}
			catch (Exception e){
				logger.error("Exception in fetchPayment()");
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}
			finally{
				if(preparedStatement != null){
					preparedStatement.close();
					preparedStatement= null;
				}
			}

			return paymentId;
		}			
	
		
		/**
		 * Invoke Business Service to cancel the payment and payment adjustment (if any)
		 * @param paymentRow
		 * @param paymentId
		 * @return String
		 */
		private String cancelPayment(PaymentCancelStaging_Id paymentRow,String paymentId){
			
			//Invoke Payment Cancellation business service
			try{
				BusinessServiceInstance bs = BusinessServiceInstance.create(PAY_CANCEL_BUSINESS_SERVICE);
				bs.set("payId", paymentId);
				bs.set("paymentCancelFlag", Bool.TRUE);
				bs.set("cancelReasonCode", paymentRow.getCanRsnCd());
				BusinessServiceDispatcher.execute(bs);				
				bs=null;
				
			} catch (Exception e){
				logger.error("Exception in cancelPayment()");
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}
			
			//Invoke Create Frozen Adjustment business service to reverse the payment adjustment
			PreparedStatement preparedStatement = null;
			try{
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT A.ADJ_ID, B.SA_ID, B.ADJ_TYPE_CD, B.ADJ_AMT ");
				stringBuilder.append("FROM CI_ADJ_CHAR A, CI_ADJ B WHERE A.CHAR_TYPE_CD='PAYID' ");
				stringBuilder.append("AND A.ADHOC_CHAR_VAL=:paymentId AND B.ADJ_STATUS_FLG='50' ");
				stringBuilder.append("AND A.ADJ_ID=B.ADJ_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("paymentId", paymentId,"ADHOC_CHAR_VAL");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					String serviceAgreementId = CommonUtils.CheckNull(row.getString("SA_ID"));
					String adjTypeCode = CommonUtils.CheckNull(row.getString("ADJ_TYPE_CD"));
					BigDecimal adjustmentAmount = row.getBigDecimal("ADJ_AMT");
					adjustmentAmount = adjustmentAmount.multiply(BigDecimal.valueOf(-1));
					
					BusinessServiceInstance bis = BusinessServiceInstance.create(CREATE_FROZEN_ADJ_BUSINESS_SERVICE);
					COTSInstanceNode inputAdjustmentList = bis.getGroup("Input");
					inputAdjustmentList.set("ServiceAgreement", serviceAgreementId);
					inputAdjustmentList.set("AdjustmentType", adjTypeCode);
					inputAdjustmentList.set("AdjustmentAmount", adjustmentAmount); 
								
					BusinessServiceDispatcher.execute(bis);
					bis = null;
				}
			} catch (Exception e){
				logger.error("Exception in cancelPayment()");
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			} finally{
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}				

			}
			return "true";
		}
		
		/**
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 * 
		 * @return errorInfo
		 */
		public static String getErrorDescription(int messageNumber) {
			String errorInfo = " ";
			
			errorInfo = CustomMessageRepository.merchantError(String.valueOf(
					messageNumber)).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}
		
		/**
		 * Parse error message
		 * @param errorMessage
		 * @return
		 */
		public Map<String, String> errorList(String errorMessage) {
			Map<String, String> errorMap = new HashMap<String, String>();
			String errorMessageNumber = "";
			String errorMessageCategory = "";
			if (errorMessage.contains("Number:")) {
				errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));
				errorMap.put("Number", errorMessageNumber);
			}
			if (errorMessage.contains("Category:")) {
				errorMessageCategory = errorMessage.substring(errorMessage
						.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));
				errorMap.put("Category", errorMessageCategory);
			}
			if (errorMessage.contains("Text:")
					&& errorMessage.contains("Description:")) {
				errorMessage = errorMessage
						.substring(errorMessage.indexOf("Text:"), errorMessage
								.indexOf("Description:"));
			}
			if (errorMessage.length() > 250) {
				errorMessage = errorMessage.substring(0, 250);
				errorMap.put("Text", errorMessage);
			} else {
				errorMessage = errorMessage.substring(0, errorMessage.length());
				errorMap.put("Text", errorMessage);
			}
			return errorMap;
		}
		
		/**
		 * logError() method stores the error information in the List and does rollback all the database transaction of this unit.
		 * 
		 * @param errorMessage
		 * @param transactionHeaderId
		 * @param messageCategory
		 * @param messageNumber
		 * @return
		 */
		private boolean logError(String transactionHeaderId, int messageCategory, int messageNumber, String errorMessage) {
			
			eachPaymentStatusList = new ArrayList<String>();
			eachPaymentStatusList.add(0,transactionHeaderId);
			eachPaymentStatusList.add(1,"ERROR");
			eachPaymentStatusList.add(2,String.valueOf(messageCategory));
			eachPaymentStatusList.add(3,String.valueOf(messageNumber));
			eachPaymentStatusList.add(4, errorMessage);
			updatePaymentStatusList.add(eachPaymentStatusList);
			
	
			//Excepted to do rollback
			rollbackToSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));
			//addError(CustomMessageRepository.billCycleError(String.valueOf(messageNumber)));

			//intentionally kept false as rollback has to occur here
			return false; 	
		}
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
		
		/**
		 * At the end of thread work processing, update each error staging record
		 */
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			// Logic to update erroneous records			
			for(ArrayList<String> rowList : updatePaymentStatusList) {
				updateStagingStatus(String.valueOf(rowList.get(0)), String.valueOf(rowList.get(1)), String.valueOf(rowList.get(2)), 
						String.valueOf(rowList.get(3)), String.valueOf(rowList.get(4)));
				rowList = null;				
			}
			updatePaymentStatusList = null;
		}
		
		/**
		 * Update status of staging record
		 * @param transactionHeaderId
		 * @param status
		 * @param messageCategoryNumber
		 * @param messageNumber
		 * @param errorDescription
		 */
		private void updateStagingStatus(String transactionHeaderId, String status, 
				String messageCategoryNumber, String messageNumber, String errorDescription) {
			PreparedStatement preparedStatement = null;
			try {		
				if (CommonUtils.CheckNull(errorDescription).trim().length() > 250) {
					errorDescription = errorDescription.substring(0, 250);
				}
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_PAY_RJCT_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append("MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription, ");
				// RIA: Mark record as ILM_DT as process date and ILM_ARCH_SW='Y' for every COMPLETED record
				stringBuilder.append("ILM_DT  =:processDate, ILM_ARCH_SW =:ilmSw ");
				stringBuilder.append("WHERE TXN_HEADER_ID =:headerId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status", status, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", messageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", errorDescription, "ERROR_INFO");
				preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
				if("COMPLETED".equalsIgnoreCase(status.trim())) 
					preparedStatement.bindString("ilmSw", "Y", "ILM_ARCH_SW");
				else 
					preparedStatement.bindString("ilmSw", "N", "ILM_ARCH_SW");
				preparedStatement.bindString("headerId", transactionHeaderId, "TXN_HEADER_ID");
				preparedStatement.executeUpdate();
		
			} catch(Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
		
		/**
		 * Update status of staging record For Bill ID
		 * @param transactionHeaderId
		 * @param status
		 * @param messageCategoryNumber
		 * @param messageNumber
		 * @param errorDescription
		 */
		private void updateStagingStatusForBill(String matchVal, String status, 
				String messageCategoryNumber, String messageNumber, String errorDescription) {
			PreparedStatement preparedStatement = null;
			try {		
				if (CommonUtils.CheckNull(errorDescription).trim().length() > 250) {
					errorDescription = errorDescription.substring(0, 250);
				}
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_PAY_RJCT_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append("MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription, ");
				stringBuilder.append("ILM_DT  =:processDate, ILM_ARCH_SW =:ilmSw ");
				stringBuilder.append("WHERE MATCH_VAL =:matchVal AND BO_STATUS_CD =:boStatus ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status", status, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", messageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", errorDescription, "ERROR_INFO");
				preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
				preparedStatement.bindString("ilmSw", "N", "ILM_ARCH_SW");
				preparedStatement.bindString("matchVal", matchVal, "MATCH_VAL");
				preparedStatement.bindString("boStatus", "UPLD", "BO_STATUS_CD");
				preparedStatement.executeUpdate();
		
			} catch(Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
		
		/**
		 * updateCmBillDueDtTbl() method will set Merchant Balanced flag 
		 * for a particular bill as 'N' if the bill is not settled.
		 *
		 * @param finDocId
		 */
		private int updateCmBillDueDtToCncl(String finDocId ) {
			PreparedStatement dueDtUpdateStmt = null;
			int row= 0;
			try {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_BILL_DUE_DT SET IS_MERCH_BALANCED = 'N', ");
				stringBuilder.append("STATUS_UPD_DTTM = SYSTIMESTAMP WHERE BILL_ID = :billId ");
				dueDtUpdateStmt = createPreparedStatement(stringBuilder.toString(), "");
				dueDtUpdateStmt.bindString("billId", finDocId, "BILL_ID");
				row = dueDtUpdateStmt.executeUpdate();
				logger.info("Rows updated for CM_BILL_DUE_DT - " +row);
				return row;
			} catch (Exception e){
				logger.error("Exception while updating bill Due date", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (dueDtUpdateStmt != null) {
					dueDtUpdateStmt.close();
					dueDtUpdateStmt = null;
				}
			}
		}
	}

	/**
	 * Retrieve CM_PAY_RJCT_STG staging data
	 * @return
	 */
	private List<ThreadWorkUnit> getStagingData(){
		PreparedStatement preparedStatement = null;
		Account_Id accountId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		String acctId = "";
		
		try {
			StringBuilder stringBuilder = new StringBuilder();
			// RIA: Threading on ACCT_ID
			stringBuilder.append("SELECT DISTINCT ACCT_ID FROM CI_BILL WHERE BILL_ID IN ");
			stringBuilder.append("(SELECT MATCH_VAL FROM CM_PAY_RJCT_STG ");
			stringBuilder.append("WHERE BO_STATUS_CD=:selectBoStatus1)");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.bindString("selectBoStatus1", "UPLD", "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);

			for (SQLResultRow resultSet : preparedStatement.list()) {
				acctId = resultSet.getString("ACCT_ID");
				accountId = new Account_Id(acctId);
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(accountId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				//*************************
			}
		} catch(Exception e) {
			logger.error("Exception occurred in getStagingData()" , e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
		return threadWorkUnitList;
	}
	
public static final class PaymentCancelStagingMatchVal_Id implements Id{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String matchVal;
	
		public PaymentCancelStagingMatchVal_Id(){
		}
		
		public PaymentCancelStagingMatchVal_Id(String matchVal) {
			setMatchVal(matchVal);
		}
	
		public boolean isNull() {
			return false;
		}
	
		public void appendContents(StringBuilder arg0) {
			
		}
	
		public String getMatchVal() {
			return matchVal;
		}
	
		public void setMatchVal(String matchVal) {
			this.matchVal = matchVal;
		}
	}

	public static final class PaymentCancelStaging_Id implements Id{
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private String txnHeaderId;
		private String boStatusCd;
		private String currencyCd;
		private BigDecimal tenderAmt;
		private String extReferenceId;
		private String matchVal;
		private String canRsnCd;
	
		public PaymentCancelStaging_Id(){
		}
		
		public PaymentCancelStaging_Id(String txnHeaderId,
				String boStatusCd, String currencyCd,
				BigDecimal tenderAmt, String extReferenceId,
				String matchVal, String canRsnCd) {
			
			setTxnHeaderId(txnHeaderId);
			setBoStatusCd(boStatusCd);
			setCurrencyCd(currencyCd);
			setTenderAmt(tenderAmt);
			setExtReferenceId(extReferenceId);
			setMatchVal(matchVal);
			setCanRsnCd(canRsnCd);
		}
	
		public boolean isNull() {
			return false;
		}
	
		public void appendContents(StringBuilder arg0) {
			
		}
	
		public String getBoStatusCd() {
			return boStatusCd;
		}
	
		public void setBoStatusCd(String boStatusCd) {
			this.boStatusCd = boStatusCd;
		}
	
		public String getCurrencyCd() {
			return currencyCd;
		}
	
		public void setCurrencyCd(String currencyCd) {
			this.currencyCd = currencyCd;
		}
			
	
		public String getExtReferenceId() {
			return extReferenceId;
		}
	
		public void setExtReferenceId(String extReferenceId) {
			this.extReferenceId = extReferenceId;
		}
	
		public String getMatchVal() {
			return matchVal;
		}
	
		public void setMatchVal(String matchVal) {
			this.matchVal = matchVal;
		}
	
		public BigDecimal getTenderAmt() {
			return tenderAmt;
		}
	
		public void setTenderAmt(BigDecimal tenderAmt) {
			this.tenderAmt = tenderAmt;
		}
		public String getTxnHeaderId() {
			return txnHeaderId;
		}
	
		public void setTxnHeaderId(String txnHeaderId) {
			this.txnHeaderId = txnHeaderId;
		}
	
		public String getCanRsnCd() {
			return canRsnCd;
		}
		public void setCanRsnCd(String canRsnCd) {
			this.canRsnCd = canRsnCd;
		}
	}
}
