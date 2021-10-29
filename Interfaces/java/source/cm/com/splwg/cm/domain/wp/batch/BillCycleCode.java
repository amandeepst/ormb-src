/*******************************************************************************
 * FileName                   : BillCycleCode.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : June 06, 2014
 * Version Number             : 0.6 
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             06-Jun-2014        Sunaina       Implemented all requirement as required for INT039.
 0.2      NA             30-Jan-2015        Sunaina       Updated Loggers.
 0.3 	  NA			 17-Aug-2015		Sunaina		  Implemented all requirement as required for CD2.
 0.4 	  NA			 13-Oct-2017		Ankur		  Fixed unique constraint error in CM_BILL_CYC_SCH table.
 0.5 	  NA			 19-Jan-2018		Vienna Rom	  3-step billing bill cycle changes
 0.6 	  NA			 09-APR-2018		Ankur	      NAP-25484 remove CM_BILL_CYC_SCH
 0.7 	  NA			 30-APR-2018		Vienna Rom	  NAP-26508 remove Accounting Calendar update
 0.8      NA             07-FEB-2019        Manasi Gupta  NAP-38791 adding validation dates on CM_BILL_CYC_CD
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
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.language.Language_Id;
import com.splwg.ccb.domain.admin.billCycle.BillCycle;
import com.splwg.ccb.domain.admin.billCycle.BillCycleSchedule;
import com.splwg.ccb.domain.admin.billCycle.BillCycleSchedule_Id;
import com.splwg.ccb.domain.admin.billCycle.BillCycle_Id;
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
public class BillCycleCode extends BillCycleCode_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(BillCycleCode.class);

	public BillCycleCode() {
	}

	/**
	 * getJobWork() method selects data for processing by Bill Cycle Code Interface. 
	 * The source of data is selected from CM_BILL_CYC_CD table and then passed to 
	 * the executeWorkUnit for further processing by framework.
	 */
	public JobWork getJobWork() {

		List<ThreadWorkUnit> threadWorkUnitList = getBillCycleData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}


	//	*********************** getBillCycleData Method******************************

	/**
	 * getBillCycleData() method retrieves all the elements from CM_BILL_CYC_CD table.
	 * 
	 * @return List BillCycle_Cd
	 */
	private List<ThreadWorkUnit> getBillCycleData() {
		PreparedStatement preparedStatement = null;
		BillCycle_Cd billCycleCd = null;
		List<BillCycle_Cd> rowsForProcessingList = new ArrayList<BillCycle_Cd>();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		StringBuilder stringBuilder = new StringBuilder();

		String transactionHeaderId = "";
		String transactionDetailId = "";
		String billCycleCode = "";
		String languageCode = "";
		String descr = "";
		com.splwg.base.api.datatypes.Date winStartDate = null;
		com.splwg.base.api.datatypes.Date winEndDate = null;
		com.splwg.base.api.datatypes.Date accountingDate = null;
		com.splwg.base.api.datatypes.Date estimationDate = null;
		com.splwg.base.api.datatypes.Date validStartDate = null;
		com.splwg.base.api.datatypes.Date validEndDate = null;
		String freezeCompleteSw = "";

		try {
			stringBuilder.append(" SELECT TXN_HEADER_ID, TXN_DETAIL_ID, BILL_CYC_CD,LANGUAGE_CD, " );
			stringBuilder.append(" DESCR, WIN_START_DT, WIN_END_DT, ACCOUNTING_DT,EST_DT, " );
			stringBuilder.append(" FREEZE_COMPLETE_SW , VALID_START_DT, VALID_END_DT FROM CM_BILL_CYC_CD " );
			stringBuilder.append(" WHERE BO_STATUS_CD = :selectBoStatus1 " );
			stringBuilder.append(" ORDER BY BILL_CYC_CD, VALID_END_DT ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("selectBoStatus1", "UPLD", "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				transactionHeaderId = resultSet.getString("TXN_HEADER_ID");
				transactionDetailId = String.valueOf(resultSet.getInteger("TXN_DETAIL_ID"));
				billCycleCode = resultSet.getString("BILL_CYC_CD");
				languageCode = resultSet.getString("LANGUAGE_CD");
				descr = resultSet.getString("DESCR");
				winStartDate = resultSet.getDate("WIN_START_DT");
				winEndDate = resultSet.getDate("WIN_END_DT");
				accountingDate = resultSet.getDate("ACCOUNTING_DT");
				estimationDate = resultSet.getDate("EST_DT");
				validStartDate=resultSet.getDate("VALID_START_DT");
				validEndDate=resultSet.getDate("VALID_END_DT");
				freezeCompleteSw = resultSet.getString("FREEZE_COMPLETE_SW");

				
				
				billCycleCd = new BillCycle_Cd(transactionHeaderId,
						transactionDetailId, billCycleCode, languageCode,
						descr, winStartDate, winEndDate, accountingDate,
						estimationDate, validStartDate,validEndDate, freezeCompleteSw);
				
				rowsForProcessingList.add(billCycleCd);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(billCycleCd);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				billCycleCd = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getBillCycleData()", e);
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


	public Class<BillCycleCodeWorker> getThreadWorkerClass() {
		return BillCycleCodeWorker.class;
	}

	public static class BillCycleCodeWorker extends
	BillCycleCodeWorker_Gen {

		private static final String FREEZE_COMP_FLG_DEFAULT = "N";
		private ArrayList<ArrayList<String>> updateBillCycleStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> erroneousBillCycleList = null;
		private static final String messageCategoryNumber = String.valueOf(CustomMessages.MESSAGE_CATEGORY);

		public BillCycleCodeWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
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
		 * bill Cycles.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			BillCycle_Cd billCycleCd = (BillCycle_Cd) unit.getPrimaryId();

			String transactionHeaderId = CommonUtils.CheckNull(billCycleCd.getTransactionHeaderId()).trim();
			String transactionDetailId = CommonUtils.CheckNull(billCycleCd.getTransactionDetailId()).trim();
			Date windowStartDate = billCycleCd.getWinStartDate();
			Date windowEndDate = billCycleCd.getWinEndDate();
			Date estimationDt = billCycleCd.getEstimationDate();
			Date validStartDt=billCycleCd.getValidityStartDate();
			Date validEndDt=billCycleCd.getValidityEndDate();
			
			boolean billCycleToBeUpdated = false;
			boolean billCycleToBeUpdatedInLangTable = false;
			boolean billCycleToBeUpdatedInScheduleTable = false;
			boolean billCycleToBeUpdatedInDivisionTable = false;
			
			boolean billCycleToBeDeletedInScheduleTable = false;
			
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try {
				if (isBlankOrNull(billCycleCd.getBillCycleCode())) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.BILL_CYCLE_COULD_NOT_BE_DETRMINED),
							getErrorDescription(String.valueOf(CustomMessages.BILL_CYCLE_COULD_NOT_BE_DETRMINED)));
				} else if (isBlankOrNull(billCycleCd.getLanguageCode())) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.LANGUAGE_CODE_COULD_NOT_BE_DETERMINED),
							getErrorDescription(String.valueOf(CustomMessages.LANGUAGE_CODE_COULD_NOT_BE_DETERMINED)));
				} else if (isBlankOrNull(billCycleCd.getFreezeCompleteSw())) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.FREEZE_COMPLTE_SW_COULD_NOT_BE_DETERMINED),
							getErrorDescription(String.valueOf(CustomMessages.FREEZE_COMPLTE_SW_COULD_NOT_BE_DETERMINED)));
				} else if (isBlankOrNull(billCycleCd.getDescr())) {
					return logError(transactionHeaderId, transactionDetailId, "ERROR",
							messageCategoryNumber, String.valueOf(CustomMessages.DESCR_COULD_NOT_BE_DETERMINED),
							getErrorDescription(String.valueOf(CustomMessages.DESCR_COULD_NOT_BE_DETERMINED)));
				} else {
					// 			Bill Cycle Record Processing
					//			*******************		Bill Cycle Creation / Updation *****************************************



					if(windowEndDate.compareTo(windowStartDate) < 0) {
						return logError(transactionHeaderId, transactionDetailId, "ERROR",
								messageCategoryNumber, String.valueOf(CustomMessages.WINDOW_END_DT_BACKDATED_THAN_WINDOW_START_DT),
								getErrorDescription(String.valueOf(CustomMessages.WINDOW_END_DT_BACKDATED_THAN_WINDOW_START_DT)));
					}
					if(windowEndDate.compareTo(estimationDt) < 0) {
						return logError(transactionHeaderId, transactionDetailId, "ERROR",
								messageCategoryNumber, String.valueOf(CustomMessages.WINDOW_END_DT_BACKDATED_THAN_ESTIMATION_DT),
								getErrorDescription(String.valueOf(CustomMessages.WINDOW_END_DT_BACKDATED_THAN_ESTIMATION_DT)));
					}
					if(estimationDt.compareTo(windowStartDate) < 0) {
						return logError(transactionHeaderId, transactionDetailId, "ERROR",
								messageCategoryNumber, String.valueOf(CustomMessages.ESTIMATION_DT_BACKDATED_THAN_WINDOW_START_DATE),
								getErrorDescription(String.valueOf(CustomMessages.ESTIMATION_DT_BACKDATED_THAN_WINDOW_START_DATE)));
					}

					BillCycle_Id billCycleId = new BillCycle_Id(billCycleCd.getBillCycleCode().trim());
					BillCycle billCycleCode = billCycleId.getEntity();
					
						if(notNull(billCycleCode)){
							billCycleToBeUpdated = true;
						}
					
					
					
                  	
                	stringBuilder = new StringBuilder(" SELECT 1 FROM CI_BILL_CYC_L WHERE  BILL_CYC_CD = :billCycleCode ");
   					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
   					preparedStatement.setAutoclose(false);
   					preparedStatement.bindId("billCycleCode", billCycleId);
   					
   						if(notNull(preparedStatement.firstRow())) {
   							billCycleToBeUpdatedInLangTable = true;
   						}
   					

   					preparedStatement.close();
   					preparedStatement = null;


   					BillCycleSchedule billCycSch = new BillCycleSchedule_Id(billCycleCode, windowStartDate).getEntity();
   					if(isNull(validEndDt) && notNull(billCycSch)){
   						
   	   						billCycleToBeUpdatedInScheduleTable = true;
   						
   					}
   				
   				   else if(notNull(validEndDt)){
   							billCycleToBeDeletedInScheduleTable=true;
   						}
   					

   					stringBuilder = new StringBuilder(" SELECT 1 FROM CI_CIS_DIV_BICY WHERE BILL_CYC_CD = :billCycleCd ");
   					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
   					preparedStatement.bindId("billCycleCd", billCycleId);
   					preparedStatement.setAutoclose(false);
   					
   					if(notNull(preparedStatement.firstRow())) {
   						billCycleToBeUpdatedInDivisionTable = true;	
   						
   					}
   					
   				
   					preparedStatement.close();
   					preparedStatement = null;

   					if(!billCycleToBeUpdated && isNull(validEndDt)) { 
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BILL_CYC (BILL_CYC_CD) " );
						stringBuilder.append(" VALUES (:billCycleCd) ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;
					}
   					
					
					if (billCycleToBeUpdatedInLangTable) {
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_BILL_CYC_L SET DESCR = :description " );
						stringBuilder.append(" WHERE BILL_CYC_CD = :billCycleCd  AND LANGUAGE_CD = :languageCd ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.bindId("languageCd", new Language_Id(billCycleCd.getLanguageCode().trim()));
						preparedStatement.bindString("description",billCycleCd.getDescr().trim(), "DESCR");
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;
					} 
					
					else if(isNull(validEndDt)){
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_BILL_CYC_L (BILL_CYC_CD,LANGUAGE_CD,DESCR) " );
						stringBuilder.append(" VALUES (:billCycleCd, :languageCd, :description) ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.bindId("languageCd", new Language_Id(billCycleCd.getLanguageCode().trim()));
						preparedStatement.bindString("description",billCycleCd.getDescr().trim(), "DESCR");
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;
					}
					
					if(billCycleToBeUpdatedInScheduleTable) {
						stringBuilder = new StringBuilder();
						stringBuilder.append(" UPDATE CI_BILL_CYC_SCH SET WIN_END_DT = :endDt, " );
						stringBuilder.append(" ACCOUNTING_DT = :accountingDt, EST_DT = :estDt , FREEZE_COMPLETE_SW =  :freezeCompleteFlag " );
						stringBuilder.append(" WHERE  BILL_CYC_CD = :billCycleCd  AND WIN_START_DT = :startDt");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.bindDate("startDt", billCycleCd.getWinStartDate());
						preparedStatement.bindDate("endDt", billCycleCd.getWinEndDate());
						preparedStatement.bindDate("accountingDt", billCycleCd.getWinEndDate());
						preparedStatement.bindDate("estDt", billCycleCd.getEstimationDate());
						preparedStatement.bindString("freezeCompleteFlag", FREEZE_COMP_FLG_DEFAULT, "FREEZE_COMPLETE_SW");
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;
					
					} else if(billCycleToBeDeletedInScheduleTable) {
						stringBuilder = new StringBuilder();
             
   						stringBuilder.append(" DELETE FROM CI_BILL_CYC_SCH" );
						stringBuilder.append(" WHERE BILL_CYC_CD=:billCycleCd AND WIN_START_DT=:startDt AND WIN_END_DT=:endDt");						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.bindDate("startDt", billCycleCd.getWinStartDate());
						preparedStatement.bindDate("endDt", billCycleCd.getWinEndDate());
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;
						
					}
					else {
						stringBuilder = new StringBuilder();

						stringBuilder.append(" INSERT INTO CI_BILL_CYC_SCH (BILL_CYC_CD,WIN_START_DT,WIN_END_DT,ACCOUNTING_DT,EST_DT,FREEZE_COMPLETE_SW) " );
						stringBuilder.append(" VALUES (:billCycleCd, :startDt, :endDt, :accountingDt, :estDt, :freezeCompleteFlag) " );
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.bindDate("startDt", billCycleCd.getWinStartDate());
						preparedStatement.bindDate("endDt", billCycleCd.getWinEndDate());
						preparedStatement.bindDate("accountingDt", billCycleCd.getWinEndDate());
						preparedStatement.bindDate("estDt", billCycleCd.getEstimationDate());
						preparedStatement.bindString("freezeCompleteFlag", FREEZE_COMP_FLG_DEFAULT, "FREEZE_COMPLETE_SW");
						preparedStatement.executeUpdate();


						preparedStatement.close();
						preparedStatement = null;

					}

					if (billCycleToBeUpdatedInDivisionTable) {
						stringBuilder = new StringBuilder();
						stringBuilder.append(" DELETE FROM CI_CIS_DIV_BICY WHERE BILL_CYC_CD = :billCycleCd  ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("billCycleCd", billCycleId);
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;


						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_CIS_DIV_BICY " );
						stringBuilder.append(" (CIS_DIVISION, BILL_CYC_CD, VERSION) " );
						stringBuilder.append(" SELECT DISTINCT DIV.CIS_DIVISION, BCYC.BILL_CYC_CD, 1 AS VERSION FROM " );
						stringBuilder.append(" CI_CIS_DIVISION DIV, CM_BILL_CYC_CD BCYC " );
						stringBuilder.append(" WHERE  TRIM(BCYC.BILL_CYC_CD) = :billCycleCd  ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("billCycleCd",	billCycleCd.getBillCycleCode().trim(), "BILL_CYC_CD");
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;


					} else if(isNull(validEndDt)) {
						stringBuilder = new StringBuilder();
						stringBuilder.append(" INSERT INTO CI_CIS_DIV_BICY " );
						stringBuilder.append(" (CIS_DIVISION, BILL_CYC_CD, VERSION) " );
						stringBuilder.append(" SELECT DISTINCT DIV.CIS_DIVISION, BCYC.BILL_CYC_CD, 1 AS VERSION FROM " );
						stringBuilder.append(" CI_CIS_DIVISION DIV, CM_BILL_CYC_CD BCYC " );
						stringBuilder.append(" WHERE TRIM(BCYC.BILL_CYC_CD) = :billCycleCd ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("billCycleCd",	billCycleCd.getBillCycleCode().trim(), "BILL_CYC_CD");
						preparedStatement.executeUpdate();

						preparedStatement.close();
						preparedStatement = null;

					}
					

					// Update status of row in CM_BILL_CYC_CD
					updateStagingStatus(transactionHeaderId, transactionDetailId, "COMPLETED", "0", "0", " ");

				}
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit: " , e);

			} 

			return true;
		}

		public static String getErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.billCycleError(messageNumber).getMessageText();
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

			erroneousBillCycleList = new ArrayList<String>();
			erroneousBillCycleList.add(0, aTransactionHeaderId);
			erroneousBillCycleList.add(1, aTransactionDetailId);
			erroneousBillCycleList.add(2, aStatus);
			erroneousBillCycleList.add(3, aMessageCategory);
			erroneousBillCycleList.add(4, aMessageNumber);
			erroneousBillCycleList.add(5, aErrorMessage);
			updateBillCycleStatusList.add(erroneousBillCycleList);
			erroneousBillCycleList = null;

			if (aMessageCategory.trim().equals(String.valueOf(CustomMessages.MESSAGE_CATEGORY))) {
				addError(CustomMessageRepository.billCycleError(aMessageNumber));
			}

			return false;
		}
		
		/**
		 * insertIntoCustomTable() method will feed records from custom staging table to custom bill cycle code table for data storage.
		 * 
		 * @param inputTable
		 */
		/*private void insertIntoFiscalCalendar() {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append(" INSERT INTO CI_CAL_PERIOD " );
				stringBuilder.append(" (CALENDAR_ID,FISCAL_YEAR,ACCOUNTING_PERIOD,BEGIN_DT,END_DT,OPEN_FROM_DT,OPEN_TO_DT,VERSION)  " );
				stringBuilder.append(" SELECT 'FI' AS CALENDAR_ID, EXTRACT(YEAR FROM WIN_START_DT) AS FISCAL_YEAR,  " );
				stringBuilder.append(" EXTRACT(MONTH FROM WIN_START_DT) AS ACCOUNTING_PERIOD, " );
				stringBuilder.append(" WIN_START_DT AS BEGIN_DT, WIN_END_DT AS END_DT, " );
				stringBuilder.append(" WIN_START_DT AS OPEN_FROM_DT, WIN_END_DT AS OPEN_TO_DT, " );
				stringBuilder.append(" 1 AS VERSION " );
				stringBuilder.append(" FROM CM_BILL_CYC_CD Y WHERE BILL_CYC_CD='WPMO' AND BO_STATUS_CD ='COMPLETED' " );
				stringBuilder.append(" AND NOT EXISTS(SELECT 1 FROM CI_CAL_PERIOD X WHERE X.ACCOUNTING_PERIOD = EXTRACT(MONTH FROM WIN_START_DT)" );
				stringBuilder.append(" AND X.FISCAL_YEAR = EXTRACT(YEAR FROM Y.WIN_START_DT)) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();

				preparedStatement.close();
				preparedStatement = null;

				stringBuilder = new StringBuilder();
				stringBuilder.append(" Insert into CI_CAL_PERIOD_L " );
				stringBuilder.append(" (CALENDAR_ID,LANGUAGE_CD,FISCAL_YEAR,PERIOD_DESCR,ACCOUNTING_PERIOD,VERSION) " );
				stringBuilder.append(" SELECT 'FI' AS CALENDAR_ID, LANGUAGE_CD, EXTRACT(YEAR FROM WIN_START_DT) AS FISCAL_YEAR, " );
				stringBuilder.append(" (TO_CHAR(WIN_START_DT,'MON') || '-' || TO_CHAR(WIN_START_DT,'yyyy')) AS PERIOD_DESCR, " );
				stringBuilder.append(" EXTRACT(MONTH FROM WIN_START_DT) AS ACCOUNTING_PERIOD, 1 AS VERSION " );
				stringBuilder.append(" FROM CM_BILL_CYC_CD WHERE BILL_CYC_CD='WPMO' AND BO_STATUS_CD ='COMPLETED' " );
				stringBuilder.append(" AND NOT EXISTS(SELECT 1 FROM CI_CAL_PERIOD_L X WHERE X.ACCOUNTING_PERIOD = EXTRACT(MONTH FROM WIN_START_DT)" );
				stringBuilder.append(" AND X.FISCAL_YEAR = EXTRACT(YEAR FROM WIN_START_DT)) ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.executeUpdate();
				
					
			} catch (RuntimeException e) {
				logger.error("Inside insertIntoFiscalCalendar() method Language table entry of BillCycleCode, Error -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		*/

		/**
		 * updateStagingStatus() method updates the CM_BILL_CYC_CD table with processing status.
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
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			try {
				if (CommonUtils.CheckNull(aErrorDescription).trim().length() > 250) {
					aErrorDescription = aErrorDescription.substring(0, 250);
				}
				stringBuilder.append(" UPDATE CM_BILL_CYC_CD SET BO_STATUS_CD = :completeStatus, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
				stringBuilder.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription " );
				stringBuilder.append(" WHERE TXN_HEADER_ID = :headerId AND TXN_DETAIL_ID = :detailId " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("completeStatus", aStatus, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", aMessageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", aMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", aErrorDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("detailId", aTransactionDetailId, "TXN_DETAIL_ID");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in updateStagingStatus()", e);
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
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {

			//Insert into Fiscal Calendar
			//insertIntoFiscalCalendar();

			//Logic to update Erroneous records
			for(ArrayList<String> rowList : updateBillCycleStatusList) {
				updateStagingStatus(String.valueOf(rowList.get(0)), String.valueOf(rowList.get(1)),
						String.valueOf(rowList.get(2)), String.valueOf(rowList.get(3)), 
						String.valueOf(rowList.get(4)), String.valueOf(rowList.get(5)));
				rowList = null;
			}

		}

	} // end of worker class
	public static final class BillCycle_Cd implements Id {

		private static final long serialVersionUID = 1L;

		private String transactionHeaderId;
		private String transactionDetailId;
		private String billCycleCode;
		private String languageCode;
		private String descr;
		private com.splwg.base.api.datatypes.Date winStartDate;
		private com.splwg.base.api.datatypes.Date winEndDate;
		private com.splwg.base.api.datatypes.Date accountingDate;
		private com.splwg.base.api.datatypes.Date estimationDate;
		private com.splwg.base.api.datatypes.Date validStartDate;
		private com.splwg.base.api.datatypes.Date validEndDate;
		private String freezeCompleteSw;

		

		

		public BillCycle_Cd(String transactionHeaderId,
				String transactionDetailId, String billCycleCode,
				String languageCode, String descr, 
				com.splwg.base.api.datatypes.Date winStartDate,
				com.splwg.base.api.datatypes.Date winEndDate, 
				com.splwg.base.api.datatypes.Date accountingDate,
				com.splwg.base.api.datatypes.Date estimationDate,
				com.splwg.base.api.datatypes.Date validStartDate,
				com.splwg.base.api.datatypes.Date validEndDate,
				 String freezeCompleteSw) {
			setTransactionHeaderId(transactionHeaderId);
			setTransactionDetailId(transactionDetailId);
			setBillCycleCode(billCycleCode);
			setLanguageCode(languageCode);
			setDescr(descr);
			setWinStartDate(winStartDate);
			setWinEndDate(winEndDate);
			setAccountingDate(accountingDate);
			setEstimationDate(estimationDate);
			setFreezeCompleteSw(freezeCompleteSw);
			setValidityStartDate(validStartDate);
			setValidityEndDate(validEndDate);
		} 

	
		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getBillCycleCode() {
			return billCycleCode;
		}

		public void setBillCycleCode(String billCycleCode) {
			this.billCycleCode = billCycleCode;
		}

		public String getFreezeCompleteSw() {
			return freezeCompleteSw;
		}

		public void setFreezeCompleteSw(String freezeCompleteSw) {
			this.freezeCompleteSw = freezeCompleteSw;
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

		public String getDescr() {
			return descr;
		}

		public void setDescr(String descr) {
			this.descr = descr;
		}
		public com.splwg.base.api.datatypes.Date getWinStartDate() {
			return winStartDate;
		}

		public void setWinStartDate(com.splwg.base.api.datatypes.Date winStartDate) {
			this.winStartDate = winStartDate;
		}

		public com.splwg.base.api.datatypes.Date getWinEndDate() {
			return winEndDate;
		}

		public void setWinEndDate(com.splwg.base.api.datatypes.Date winEndDate) {
			this.winEndDate = winEndDate;
		}

		public com.splwg.base.api.datatypes.Date getAccountingDate() {
			return accountingDate;
		}

		public void setAccountingDate(com.splwg.base.api.datatypes.Date accountingDate) {
			this.accountingDate = accountingDate;
		}

		public com.splwg.base.api.datatypes.Date getEstimationDate() {
			return estimationDate;
		}

		public void setEstimationDate(com.splwg.base.api.datatypes.Date estimationDate) {
			this.estimationDate = estimationDate;
		}
				
		
		public com.splwg.base.api.datatypes.Date getValidityStartDate() {
			return validStartDate;
			
		}

		private void setValidityStartDate(com.splwg.base.api.datatypes.Date validStartDate) {
			this.validStartDate=validStartDate;
			
		}
		public com.splwg.base.api.datatypes.Date getValidityEndDate() {
			return validEndDate;
		}
		
		private void setValidityEndDate(com.splwg.base.api.datatypes.Date validEndDate) {
			this.validEndDate=validEndDate;
			
		}


	}// end of Id Class

}
