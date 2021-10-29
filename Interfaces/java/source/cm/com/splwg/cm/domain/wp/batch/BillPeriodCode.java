/*******************************************************************************
* FileName                   : BillPeriodCode.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Mar 24, 2015 
* Version Number             : 0.2
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 24, 2015        ABHISHEK PALIWAL     Implemented all requirements for CD1.
0.2      NA             May 25, 2017        ANKUR JAIN           PAM-12552 Fix 
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import com.ibm.icu.text.SimpleDateFormat;
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
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author paliwala708
 *
 @BatchJob (multiThreaded = false, rerunnable = false,
 *      modules = { "demo"})
 */
public class BillPeriodCode extends BillPeriodCode_Gen {
	public static final Logger logger = LoggerFactory
	.getLogger(BillPeriodCode.class);
	
	
	
	public BillPeriodCode(){
		
	}

	public JobWork getJobWork() {		
		logger.debug("Inside getJobWork() method");
		BillPeriod_Cd billPeriodCd = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		List<BillPeriod_Cd> stagingDataList = getBillPeriodData();
		int rowsForProcessing = stagingDataList.size();
		logger.debug("No of rows selected for processing are - " + rowsForProcessing);
		for (int i=0; i < rowsForProcessing; i++) {
			billPeriodCd = stagingDataList.get(i);
			threadworkUnit = new ThreadWorkUnit();
			threadworkUnit.setPrimaryId(billPeriodCd);
			threadWorkUnitList.add(threadworkUnit);
			threadworkUnit = null;
		}
		stagingDataList = null;
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	public Class<BillPeriodCodeWorker> getThreadWorkerClass() {
		return BillPeriodCodeWorker.class;
	}

//	*********************** getBillPeriodData Method******************************
	
	/**
	 * getBillPeriodData() method retrieves all the elements from CM_BILL_PERIOD_CD table.
	 * 
	 * @return List BillPeriod_Cd
	 */
	private List<BillPeriod_Cd> getBillPeriodData() {
		PreparedStatement preparedStatement = null;
		BillPeriod_Cd billPeriodCd = null;
		List<BillPeriod_Cd> rowsForProcessingList = new ArrayList<BillPeriod_Cd>();
		try {
			preparedStatement = createPreparedStatement(" SELECT TXN_HEADER_ID, TXN_DETAIL_ID, BILL_PERIOD_CD, " +
					" LANGUAGE_CD, DESCR, BILL_DT, CUTOFF_DT " +
					"  FROM CM_BILL_PERIOD_CD " +
					" WHERE BO_STATUS_CD = :selectBoStatus1 " +
					" ORDER BY BILL_PERIOD_CD, TXN_HEADER_ID ","");
			preparedStatement.bindString("selectBoStatus1",
					"UPLD", "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				String transactionHeaderId = resultSet.getString("TXN_HEADER_ID");
				String transactionDetailId = String.valueOf(resultSet.getInteger("TXN_DETAIL_ID"));
				String billPeriodCode = resultSet.getString("BILL_PERIOD_CD");
				String languageCode = resultSet.getString("LANGUAGE_CD");
				String descr = resultSet.getString("DESCR");
				String billDate = String.valueOf(resultSet.getDate("BILL_DT"));
				String cutOffDate = String.valueOf(resultSet.getDate("CUTOFF_DT"));
				billPeriodCd = new BillPeriod_Cd(transactionHeaderId,
						transactionDetailId, billPeriodCode, languageCode,
						descr, billDate, cutOffDate);
				rowsForProcessingList.add(billPeriodCd);
				resultSet = null;
				billPeriodCd = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getBillPeriodData() ",e);//e.printStackTrace();
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
//		Set records that would be processed as PENDING
		try {
			preparedStatement = createPreparedStatement("UPDATE CM_BILL_PERIOD_CD SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP " 
					+ "WHERE BO_STATUS_CD = :selectBoStatus ","");
			preparedStatement.bindString("newBoStatus", "PENDING", "BO_STATUS_CD");
			preparedStatement.bindString("selectBoStatus", "UPLD", "BO_STATUS_CD");
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in getBillPeriodData() ",e);//e.printStackTrace();
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return rowsForProcessingList;
	}

	

	public static class BillPeriodCodeWorker extends BillPeriodCodeWorker_Gen {
		
		@SuppressWarnings("rawtypes")
		private ArrayList<ArrayList> updateBillPeriodStatusList = new ArrayList<ArrayList>();
		private ArrayList<String> erroneousBillPeriodList = null;
		
		public BillPeriodCodeWorker() {
		}
		
		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			super.initializeThreadWork(arg0);
		}
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		
		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * every row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to create / update
		 * bill Periods.
		 */

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			BillPeriod_Cd billPeriodCd = (BillPeriod_Cd) unit.getPrimaryId();
			logger.debug("Inside executeWorkUnit() method for processing of Transaction Header Id - "
					+ billPeriodCd.getTransactionHeaderId());
			String messageCategoryNumber = String.valueOf(CustomMessages.MESSAGE_CATEGORY);
			String transactionHeaderId = CommonUtils.CheckNull(billPeriodCd.getTransactionHeaderId()).trim();
			String transactionDetailId = CommonUtils.CheckNull(billPeriodCd.getTransactionDetailId()).trim();
			SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
			Date billDate = null;
			Date cutOffDate = null;
			String previousCutOffDate="";
			try{
		    billDate = dateFormat.parse(billPeriodCd.getBillDate().trim());
		    cutOffDate = dateFormat.parse(CommonUtils.CheckNull(billPeriodCd.getCutOffDate()).trim());
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit: ",e);//e.printStackTrace();
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			}
			
			if (CommonUtils.CheckNull(billPeriodCd.getBillPeriodCode()).trim().equals("")) {
				return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.BILL_PERIOD_COULD_NOT_BE_DETRMINED),
							getErrorDescription(String.valueOf(CustomMessages.BILL_PERIOD_COULD_NOT_BE_DETRMINED)));
			}
			
			if (CommonUtils.CheckNull(billPeriodCd.getLanguageCode()).trim().equals("")) {
				return logError(transactionHeaderId, transactionDetailId, "ERROR",
						messageCategoryNumber, String.valueOf(CustomMessages.LANG_CODE_COULD_NOT_BE_DETERMINED),
						getErrorDescription(String.valueOf(CustomMessages.LANG_CODE_COULD_NOT_BE_DETERMINED)));
			}
			
			
			if (CommonUtils.CheckNull(billPeriodCd.getDescr()).trim().equals("")) {
				return logError(transactionHeaderId, transactionDetailId, "ERROR",
						messageCategoryNumber, String.valueOf(CustomMessages.DESCRIPTION_COULD_NOT_BE_DETERMINED),
						getErrorDescription(String.valueOf(CustomMessages.DESCRIPTION_COULD_NOT_BE_DETERMINED)));
			}
// 			Bill Period Record Processing
//			*******************		Bill Period Creation / Updation *****************************************
			
				boolean billPeriodToBeUpdated = false;
				boolean billPeriodToBeUpdatedInLangTable = false;
				boolean billPeriodToBeUpdatedInScheduleTable = false;
				PreparedStatement preparedStatement = null;
				List<SQLResultRow> sqlRowList = null;
				try {
					
				if(cutOffDate.compareTo(billDate) < 0) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.BILL_DT_BACKDATED_THAN_CUTOFF_DT),
							getErrorDescription(String.valueOf(CustomMessages.BILL_DT_BACKDATED_THAN_CUTOFF_DT)));
				}
				/*if(billDate.compareTo(currentDate) < 0) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.BILL_DT_BACKDATED_THAN_SYSTEM_DT),
							getErrorDescription(String.valueOf(CustomMessages.BILL_DT_BACKDATED_THAN_SYSTEM_DT)));	
				}
				*/
				preparedStatement = createPreparedStatement(" SELECT * FROM CI_BILL_PERIOD WHERE BILL_PERIOD_CD = :billPeriodCode","");
				preparedStatement.bindString("billPeriodCode", billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
				preparedStatement.setAutoclose(false);
				sqlRowList = preparedStatement.list();
				if (sqlRowList.size() > 0) {
					billPeriodToBeUpdated = true;
				}
	
					preparedStatement.close();
					preparedStatement = null;
	
				sqlRowList = null;
				preparedStatement = createPreparedStatement(" SELECT * FROM CI_BILL_PERIOD_L WHERE BILL_PERIOD_CD = :billPeriodCode " +
						" AND LANGUAGE_CD = :languageCd","");
				preparedStatement.bindString("billPeriodCode", billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
				preparedStatement.bindString("languageCd", billPeriodCd.getLanguageCode(), "LANGUAGE_CD");
				preparedStatement.setAutoclose(false);
				sqlRowList = preparedStatement.list();
				if (sqlRowList.size() > 0) {
					billPeriodToBeUpdatedInLangTable = true;
				}
				
	
					preparedStatement.close();
					preparedStatement = null;
	
				preparedStatement = createPreparedStatement(" SELECT * FROM CM_BILL_PER_SCH WHERE BILL_PERIOD_CD = :billPeriodCode " +
						" AND BILL_DT = to_date(:startDate,'YYYY-MM-DD') ","");
				preparedStatement.bindString("billPeriodCode", billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
				preparedStatement.bindString("startDate", billPeriodCd.getBillDate(), "BILL_DT");
				preparedStatement.setAutoclose(false);
				sqlRowList = preparedStatement.list();
				if (sqlRowList.size() > 0) {
					billPeriodToBeUpdatedInScheduleTable = true;
					previousCutOffDate = String.valueOf(preparedStatement.firstRow().getDate("CUTOFF_DT"));
					
					logger.info("previous date "+previousCutOffDate);
				}
				
	
					preparedStatement.close();
					preparedStatement = null;
	
				
				if(!billPeriodToBeUpdated) { 
					preparedStatement = createPreparedStatement(" INSERT INTO CI_BILL_PERIOD (BILL_PERIOD_CD) " +
					" VALUES (:billPeriodCd) ","");
			preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
			preparedStatement.executeUpdate();
			
	
				preparedStatement.close();
				preparedStatement = null;
	
				}
				if (billPeriodToBeUpdatedInLangTable) {
					
					preparedStatement = createPreparedStatement(" UPDATE CI_BILL_PERIOD_L SET DESCR = :description " +
							" WHERE BILL_PERIOD_CD = :billPeriodCd AND LANGUAGE_CD = :languageCd ","");
					preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
					preparedStatement.bindString("languageCd", billPeriodCd.getLanguageCode(), "LANGUAGE_CD");
					preparedStatement.bindString("description",billPeriodCd.getDescr(), "DESCR");
					preparedStatement.executeUpdate();
					
	
						preparedStatement.close();
						preparedStatement = null;
	
					
				} else {
					preparedStatement = createPreparedStatement(" INSERT INTO CI_BILL_PERIOD_L (BILL_PERIOD_CD,LANGUAGE_CD,DESCR) " +
					" VALUES (:billPeriodCd, :languageCd, :description) ","");
					preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
					preparedStatement.bindString("languageCd", billPeriodCd.getLanguageCode(), "LANGUAGE_CD");
					preparedStatement.bindString("description",billPeriodCd.getDescr(), "DESCR");
					preparedStatement.executeUpdate();
					
	
						preparedStatement.close();
						preparedStatement = null;
	
					
			
				}
				if(billPeriodToBeUpdatedInScheduleTable) {

					preparedStatement = createPreparedStatement(" UPDATE CI_BILL_PER_SCH SET CUTOFF_DT = :endDt, BILL_DT= :endDt " +
							" WHERE BILL_PERIOD_CD = :billPeriodCd AND CUTOFF_DT = to_date(:preCutOffDt,'YYYY-MM-DD') ","");
					preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
					preparedStatement.bindString("preCutOffDt", previousCutOffDate, "CUTOFF_DT");
					preparedStatement.bindString("endDt", billPeriodCd.getCutOffDate(), "CUTOFF_DT");
					preparedStatement.executeUpdate();
					preparedStatement.close();
					preparedStatement = null;
						
					preparedStatement = createPreparedStatement(" UPDATE CM_BILL_PER_SCH SET CUTOFF_DT = :endDt " +
								" WHERE BILL_PERIOD_CD = :billPeriodCd AND BILL_DT = to_date(:startDt,'YYYY-MM-DD') ","");
					preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
					preparedStatement.bindString("startDt", billPeriodCd.getBillDate(), "BILL_DT");
					preparedStatement.bindString("endDt", billPeriodCd.getCutOffDate(), "CUTOFF_DT");
					preparedStatement.executeUpdate();
					preparedStatement.close();
					preparedStatement = null;
	
				} else {
			
			preparedStatement = createPreparedStatement(" INSERT INTO CI_BILL_PER_SCH (BILL_PERIOD_CD,BILL_DT,CUTOFF_DT) " +
					" VALUES (:billPeriodCd, :startDt, :endDt) ","");
			preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
			preparedStatement.bindString("startDt",billPeriodCd.getCutOffDate(), "BILL_DT");
			preparedStatement.bindString("endDt", billPeriodCd.getCutOffDate(), "CUTOFF_DT");
			preparedStatement.executeUpdate();

	
				preparedStatement.close();
				preparedStatement = null;
				
			preparedStatement = createPreparedStatement(" INSERT INTO CM_BILL_PER_SCH (BILL_PERIOD_CD,BILL_DT,CUTOFF_DT) " +
						" VALUES (:billPeriodCd, :startDt, :endDt) ","");
			preparedStatement.bindString("billPeriodCd",	billPeriodCd.getBillPeriodCode(), "BILL_PERIOD_CD");
			preparedStatement.bindString("startDt", billPeriodCd.getBillDate(), "BILL_DT");
			preparedStatement.bindString("endDt", billPeriodCd.getCutOffDate(), "CUTOFF_DT");
			preparedStatement.executeUpdate();

		
					preparedStatement.close();
					preparedStatement = null;
	
				}
				
				logger.debug("Bill Period record Added / Updated for Transaction Header Id- " + billPeriodCd.getTransactionHeaderId());
//				 Update status of row in CM_BILL_PERIOD_CD
				updateStagingStatus(transactionHeaderId, transactionDetailId, "COMPLETED", "0", "0", " ");
				
		} catch (Exception e) {
			//e.printStackTrace();
			logger.error("Exception in executeWorkUnit: ",e);
		} 
		return true;
		}// end of execute work unit
		
		public static String getErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.billPeriodError(messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}
		
		/**
		 * logError() method stores the error information in the List and does rollback of all the database transaction of this unit.
		 * 
		 * @param aTransactionHeaderId
		 * @param aTransactionDetailId
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 * @return
		 */
		private boolean logError(String aTransactionHeaderId, String aTransactionDetailId,
				String aStatus, String aMessageCategory, String aMessageNumber, 
				String aErrorMessage) {
			logger.debug("Inside logError() method");
				erroneousBillPeriodList = new ArrayList<String>();
				erroneousBillPeriodList.add(0, aTransactionHeaderId);
				erroneousBillPeriodList.add(1, aTransactionDetailId);
				erroneousBillPeriodList.add(2, aStatus);
				erroneousBillPeriodList.add(3, aMessageCategory);
				erroneousBillPeriodList.add(4, aMessageNumber);
				erroneousBillPeriodList.add(5, aErrorMessage);
				updateBillPeriodStatusList.add(erroneousBillPeriodList);
				erroneousBillPeriodList = null;
				
				if (aMessageCategory.trim().equals(String.valueOf(CustomMessages.MESSAGE_CATEGORY))) {
					addError(CustomMessageRepository.billPeriodError(aMessageNumber));
				}
		return false;
		}
		
		/**
		 * updateStagingStatus() method updates the CM_BILL_PERIOD_CD table with processing status.
		 * 
		 * @param aTransactionHeaderId
		 * @param aTransactionDetailId
		 * @param aStatus
		 * @param aMessageCategoryNumber
		 * @param aMessageNumber
		 * @param aErrorDescription
		 */
		private void updateStagingStatus(String aTransactionHeaderId,
				String aTransactionDetailId, String aStatus,
				String aMessageCategoryNumber, String aMessageNumber,
				String aErrorDescription) {
				logger.debug("Inside updateStagingStatus() method");
				PreparedStatement preparedStatement = null;
			try {
				if (CommonUtils.CheckNull(aErrorDescription).trim().length() > 250) {
					aErrorDescription = aErrorDescription.substring(0, 250);
				}
				preparedStatement = createPreparedStatement(" UPDATE CM_BILL_PERIOD_CD SET BO_STATUS_CD = :completeStatus, STATUS_UPD_DTTM = SYSTIMESTAMP, " +
				" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription " +	
				" WHERE TXN_HEADER_ID = :headerId AND TXN_DETAIL_ID = :detailId AND BO_STATUS_CD = :selectBoStatus ","");
				preparedStatement.bindString("completeStatus", aStatus, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", aMessageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", aMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", aErrorDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("detailId", aTransactionDetailId, "TXN_DETAIL_ID");
				preparedStatement.bindString("selectBoStatus", "PENDING", "BO_STATUS_CD");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in updateStagingStatus() ",e);//e.printStackTrace();
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}// end method


		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after processing all units.
		 */
		@SuppressWarnings("rawtypes")
		public void finalizeThreadWork() throws ThreadAbortedException,
				RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			// Logic to update Erroneous records
			if (updateBillPeriodStatusList.size() > 0) {
				Iterator updateBillPeriodStatusItr = updateBillPeriodStatusList
						.iterator();
				updateBillPeriodStatusList = null;
				ArrayList rowList = null;
				while (updateBillPeriodStatusItr.hasNext()) {
					rowList = (ArrayList) updateBillPeriodStatusItr.next();
					updateStagingStatus(String.valueOf(rowList.get(0)), String
							.valueOf(rowList.get(1)), String.valueOf(rowList
							.get(2)), String.valueOf(rowList.get(3)), String
							.valueOf(rowList.get(4)), String.valueOf(rowList
							.get(5)));
					rowList = null;
				}
				updateBillPeriodStatusItr = null;
			}
			super.finalizeThreadWork();
		}
		
	} // end of worker class

		
	public static final class BillPeriod_Cd implements Id {
		private static final long serialVersionUID = 1L;
		private String transactionHeaderId;
		private String transactionDetailId;
		private String billPeriodCode;
		private String languageCode;
		private String descr;
		private String billDate;
		private String cutOffDate;

		public BillPeriod_Cd(String transactionHeaderId,
				String transactionDetailId, String billPeriodCode,
				String languageCode, String descr, String billDate,
				String cutOffDate) {
			setTransactionHeaderId(transactionHeaderId);
			setTransactionDetailId(transactionDetailId);
			setBillPeriodCode(billPeriodCode);
			setLanguageCode(languageCode);
			setDescr(descr);
			setBillDate(billDate);
			setCutOffDate(cutOffDate);

		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getBillDate() {
			return billDate;
		}

		public void setBillDate(String billDate) {
			this.billDate = billDate;
		}

		public String getBillPeriodCode() {
			return billPeriodCode;
		}

		public void setBillPeriodCode(String billPeriodCode) {
			this.billPeriodCode = billPeriodCode;
		}

		public String getCutOffDate() {
			return cutOffDate;
		}

		public void setCutOffDate(String cutoffDate) {
			this.cutOffDate = cutoffDate;
		}

		public String getDescr() {
			return descr;
		}

		public void setDescr(String descr) {
			this.descr = descr;
		}

		public String getLanguageCode() {
			return languageCode;
		}

		public void setLanguageCode(String languageCode) {
			this.languageCode = languageCode;
		}

		public String getTransactionDetailId() {
			return transactionDetailId;
		}

		public void setTransactionDetailId(String transactionDetailId) {
			this.transactionDetailId = transactionDetailId;
		}

		public String getTransactionHeaderId() {
			return transactionHeaderId;
		}

		public void setTransactionHeaderId(String transactionHeaderId) {
			this.transactionHeaderId = transactionHeaderId;
		}

	}// end of Id Class

}
