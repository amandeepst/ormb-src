/*******************************************************************************
 * FileName                   : CustomCreditNoteInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jul 7, 2015
 * Version Number             : 0.5
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Jul 07, 2015		Preeti		 Implemented all the requirements for CD2.	
0.2		 NA				Sep 30, 2015        Preeti		 Removal of empty finally blocks.
0.3		 NA				Oct 06, 2015        Preeti		 Included Invoice batch run.
0.4		 NA				Jan 25, 2016        Preeti		 Updated as per 2.4 tables structure.
0.5		 NA				Jan 09, 2017        Preeti		 Batch redesigning as per NAP-10686.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
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
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
   @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"})
 */
public class CustomCreditNoteInterface extends

CustomCreditNoteInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CustomCreditNoteInterface.class);
	private static final CustomCreditNoteInterfaceLookUp customCreditNoteInterfaceLookUp = new CustomCreditNoteInterfaceLookUp();

	// Default constructor
	public CustomCreditNoteInterface() {
	}

	private static final String CreateCreditNoteBusinessService = "C1-CreateCreditNote";

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {		
		customCreditNoteInterfaceLookUp.setLookUpConstants();

		List<ThreadWorkUnit> threadWorkUnitList = getCreditNoteData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// *********************** getCreditNoteData
	// Method******************************

	/**
	 * getCreditNoteData() method
	 * selects distinct set of ALT_BILL_ID from CM_INV_RECALC_STG staging table.
	 * 
	 * @return List AltBill_Id
	 */
	private List<ThreadWorkUnit> getCreditNoteData() {
		PreparedStatement preparedStatement = null;
		CreditNoteProcessingData_Id creditNoteProcessingDataId = null;
		//***********************
		ThreadWorkUnit threadworkUnit = null;
		DateTime systemDateTime = getSystemDateTime();
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		//***********************

		try {			
			preparedStatement = createPreparedStatement("SELECT DISTINCT BILL_ID FROM CM_INV_RECALC_STG "
					+ " WHERE BO_STATUS_CD = :selectBoStatus1 AND TYPE = :type","");
			preparedStatement.bindString("selectBoStatus1",customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
			preparedStatement.bindString("type",customCreditNoteInterfaceLookUp.getType().trim(), "TYPE");

			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				String altBillId = CommonUtils.CheckNull(resultSet.getString("BILL_ID"));
				creditNoteProcessingDataId = new CreditNoteProcessingData_Id(altBillId);
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(creditNoteProcessingDataId);
				threadworkUnit.addSupplementalData("systemDateTime", systemDateTime);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				creditNoteProcessingDataId = null;
				//*************************
			}
		} catch (Exception e) {
			logger.error("Inside catch block of getCreditNoteData() method-", e);
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

	public Class<CustomCreditNoteInterfaceWorker> getThreadWorkerClass() {
		return CustomCreditNoteInterfaceWorker.class;
	}

	public static class CustomCreditNoteInterfaceWorker extends
	CustomCreditNoteInterfaceWorker_Gen {

		private String billId = "";		
		private ArrayList<ArrayList<String>> updateCreditNoteStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> eachCreditNoteStatusList = null;

		// Default constructor
		public CustomCreditNoteInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once per
		 * thread by the framework.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			super.initializeThreadWork(arg0);
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * each row of processing. The selected row for processing is read
		 * (comes as input) and then processed further.
		 */

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			CreditNoteProcessingData_Id creditNoteProcessingDataId = (CreditNoteProcessingData_Id) unit.getPrimaryId();
			List<InboundCreditNoteProcessingData_Id> inboundCreditNoteProcessingDataId = getCreditNoteEventData(creditNoteProcessingDataId);
			DateTime ilmDate= (DateTime) unit.getSupplementallData("systemDateTime");

			for(InboundCreditNoteProcessingData_Id rowList : inboundCreditNoteProcessingDataId) {

				removeSavepoint("Rollback".concat(getParameters().getThreadCount().toString()));					
				setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));//Required to nullify the effect of database transactions in case of error scenario
				try {
					setPendingStatus(rowList);
					boolean validationFlag = true;
					validationFlag = validateInvoice(rowList);
					// If validation has failed, then exit processing
					if (!validationFlag) {								
						return validationFlag;
					}

					// ****************** Credit Note creation ******************

					String returnStatus = createOrUpdateCreditNote(rowList,ilmDate);
					if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {
						String[] returnStatusArray = returnStatus.split("~");
						returnStatusArray[3] = returnStatusArray[3].replace("Text:", "");

						return logError(rowList.getEventId(),
								returnStatusArray[1].trim(), returnStatusArray[2].trim(),
								returnStatusArray[3].trim(), rowList.getAltBillId());
					} else {
						updateStagingTableStatus(
								rowList.getEventId(),
								customCreditNoteInterfaceLookUp.getCompleted().trim(), 
								"0", "0", " ", rowList.getAltBillId());
					}//else
				} catch (Exception e) {
					logger.error("Exception in executeWorkUnit: " + e);
				} 
			}// for loop
			inboundCreditNoteProcessingDataId = null;
			return true;
		}

		/**
		 * getCreditNoteEventData() method selects applicable data rows
		 * from CM_INV_RECALC_STG staging table.
		 * 
		 * @return List InboundCreditNoteProcessingData_Id
		 */
		private List<InboundCreditNoteProcessingData_Id> getCreditNoteEventData(
				CreditNoteProcessingData_Id creditNoteProcessingDataId) {
			PreparedStatement preparedStatement = null;
			InboundCreditNoteProcessingData_Id inboundCreditNoteProcessingDataId = null;
			List<InboundCreditNoteProcessingData_Id> inboundCreditNoteProcessingDataIdList = new ArrayList<InboundCreditNoteProcessingData_Id>();

			try {				
				preparedStatement = createPreparedStatement("SELECT EVENT_ID, EVENT_CD, BILL_ID, "
						+ " REASON_CD FROM CM_INV_RECALC_STG "
						+ " WHERE BO_STATUS_CD =:boStatus AND BILL_ID = :altBillId ","");
				preparedStatement.bindString("boStatus",customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("altBillId",creditNoteProcessingDataId.getAltBillId().trim(),"BILL_ID");
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String eventId = CommonUtils.CheckNull(resultSet.getString("EVENT_ID"));
					String eventCode = CommonUtils.CheckNull(resultSet.getString("EVENT_CD"));
					String altBillId = CommonUtils.CheckNull(resultSet.getString("BILL_ID"));
					String reasonCode = CommonUtils.CheckNull(resultSet.getString("REASON_CD"));

					inboundCreditNoteProcessingDataId = new InboundCreditNoteProcessingData_Id(eventId, eventCode, altBillId, reasonCode);

					inboundCreditNoteProcessingDataIdList.add(inboundCreditNoteProcessingDataId);
					resultSet = null;
					inboundCreditNoteProcessingDataId = null;
				}
			} catch (Exception e) {
				logger.error("Inside catch block of getCreditNoteEventData() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return inboundCreditNoteProcessingDataIdList;
		}

		/**
		 * setPendingStatus sets record being processed into Pending state.
		 * @param aInboundCreditNoteProcessingDataId
		 */
		private void setPendingStatus(InboundCreditNoteProcessingData_Id aInboundCreditNoteProcessingDataId) {
			PreparedStatement preparedStatement = null;
			//Set records that would be processed as PENDING
			//Update only effective dated records for processing.
			try {
				preparedStatement = createPreparedStatement("UPDATE CM_INV_RECALC_STG SET BO_STATUS_CD =:status1, STATUS_UPD_DTTM = SYSTIMESTAMP "
						+ " WHERE BO_STATUS_CD =:status2 and EVENT_ID=:eventId","");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getPending().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("status2", customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("eventId", aInboundCreditNoteProcessingDataId.getEventId().trim(), "EVENT_ID");
				preparedStatement.executeUpdate();			
			} catch (Exception e) {
				logger.error("Inside catch block of setPendingStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */

		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			for(ArrayList<String> rowList : updateCreditNoteStatusList) {
				updateStagingTableStatus(String.valueOf(rowList.get(0)), String.valueOf(rowList.get(1)), String.valueOf(rowList.get(2)), 
						String.valueOf(rowList.get(3)), String.valueOf(rowList.get(4)), String.valueOf(rowList.get(5)));
				rowList = null;				
			}
			updateCreditNoteStatusList = null;
			super.finalizeThreadWork();
		}

		/**
		 * createOrUpdateCreditNote() method 
		 * @param aInboundCreditNoteProcessingDataId
		 * @return
		 * @throws RunAbortedException
		 */
		private String createOrUpdateCreditNote(
				InboundCreditNoteProcessingData_Id aInboundCreditNoteProcessingDataId, DateTime ilmDate) {
			try {

				List<String> sqlList=updatePendingBills(billId);
				createBillAndFtEntries(ilmDate);
				//markBsegFrozen(billId);
				//updateCompletedBills(sqlList);

			} catch (Exception e) {
				logger.error("Inside catch block of createOrUpdateCreditNote() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());

				String errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));

				String errorMessageCategory = errorMessage.substring(
						errorMessage.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));

				if (errorMessage.contains("Text:")
						&& errorMessage.contains("Description:")) {
					errorMessage = errorMessage.substring(errorMessage
							.indexOf("Text:"), errorMessage
							.indexOf("Description:"));
				}
				if (errorMessage.length() > 250) {
					errorMessage = errorMessage.substring(0, 250);
				} else {
					errorMessage = errorMessage.substring(0, errorMessage
							.length());
				}
				return "false" + "~" + errorMessageCategory + "~"
				+ errorMessageNumber + "~" + errorMessage;
			}
			return "true";
		}
		public void commit() {
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement(" COMMIT", "");
				preparedStatement.execute();
			} catch (RuntimeException e) {
				logger.error("Inside commit() method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occurred while committing records"));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		private void createOrUpdateFt(String newBillId) {
			// TODO Auto-generated method stub
			PreparedStatement preparedStatement = null;
			StringBuilder getFTData = new StringBuilder();
			String siblingId= "";
			String saId= "";
			String ftId = "";
			try {
				getFTData.append("SELECT SIBLING_ID,SA_ID  FROM CI_FT WHERE BILL_ID= :billId and FT_TYPE_FLG=:ftTypeflg ");
				preparedStatement = createPreparedStatement(getFTData.toString(), "");
				preparedStatement.bindString("billId", billId, "BILL_ID");
				preparedStatement.bindString("ftTypeflg", "BS", "FT_TYPE_FLG");
				preparedStatement.setAutoclose(false);

				for (SQLResultRow sqlRow : preparedStatement.list()) {
					siblingId = sqlRow.getString("SIBLING_ID");
					saId = sqlRow.getString("SA_ID");
					ftId = generateNewFtId(saId);
					if (ftId.length()>0) {
						insertFtIdInCiFtK(ftId);
						insertFtInCiFt(ftId, siblingId, newBillId);
					}
				}
			}
			catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Queries  for FT data INSERT in createOrUpdateFt() - ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CI_FT OR CI_FT_K- " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside customCreditNote  interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CI_FT ORCI_FT_K - "  + e.toString()));

			} finally {
				closeConnection(preparedStatement);
			}


		}
		private void insertFtInCiFt(String ftId,String siblingId,String newBillId) {
			ArrayList<ArrayList<Object>> paramsList = null;
			paramsList = new ArrayList<>();
			StringBuilder createFtEntry = new StringBuilder();

				createFtEntry.append(" INSERT INTO CI_FT (FT_ID, SIBLING_ID, SA_ID, PARENT_ID, GL_DIVISION, CIS_DIVISION, CURRENCY_CD, ");
				createFtEntry.append(" FT_TYPE_FLG, CUR_AMT, TOT_AMT, CRE_DTTM, FREEZE_SW, FREEZE_USER_ID, FREEZE_DTTM, ARS_DT, CORRECTION_SW,  ");
				createFtEntry.append(" REDUNDANT_SW, NEW_DEBIT_SW, SHOW_ON_BILL_SW, NOT_IN_ARS_SW, BILL_ID, ACCOUNTING_DT, VERSION, XFERRED_OUT_SW,  ");
				createFtEntry.append(" XFER_TO_GL_DT, GL_DISTRIB_STATUS, SCHED_DISTRIB_DT, BAL_CTL_GRP_ID, MATCH_EVT_ID, PRSN_BILL_ID, FXLG_CALC_AMT, "); 
				createFtEntry.append(" SETTLEMENT_ID_NBR, FXLG_CALC_STATUS) ");
				createFtEntry.append(" SELECT :ftId ,SIBLING_ID, SA_ID, PARENT_ID,GL_DIVISION,CIS_DIVISION,CURRENCY_CD,'BX' AS FT_TYPE_FLG ,(CUR_AMT*(-1)),(TOT_AMT*(-1)), ");
				createFtEntry.append(" SYSTIMESTAMP,FREEZE_SW,FREEZE_USER_ID,SYSTIMESTAMP,sysdate,CORRECTION_SW,REDUNDANT_SW,NEW_DEBIT_SW, ");
				createFtEntry.append(" SHOW_ON_BILL_SW,NOT_IN_ARS_SW,:newBillId,sysdate,:version,XFERRED_OUT_SW,XFER_TO_GL_DT,GL_DISTRIB_STATUS, sysdate, ");
				createFtEntry.append(" BAL_CTL_GRP_ID,MATCH_EVT_ID,:newBillId,FXLG_CALC_AMT,SETTLEMENT_ID_NBR,FXLG_CALC_STATUS ");
				createFtEntry.append(" from ci_ft where bill_id= :billId and ft_type_flg=:ftTypeFlg and sibling_id=:siblingId ");

			addParams(paramsList, "ftId", ftId,"FT_ID");
			addParams(paramsList, "siblingId", siblingId,"SIBLING_ID");
			addParams(paramsList, "ftTypeFlg", "BS","FT_TYPE_FLG");
			addParams(paramsList, "newBillId",newBillId ,"BILL_ID");
			addParams(paramsList, "billId",billId ,"BILL_ID");
			addParams(paramsList, "version", "1","VERSION");

			executeQuery(createFtEntry, "Insert into CI_FT (FT_ID,....", paramsList);
		}

		
		private void insertFtIdInCiFtK(String ftId) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder createFtEntry = new StringBuilder();
			paramsList = new ArrayList<>();

			createFtEntry.append(" INSERT INTO CI_FT_K  ");
			createFtEntry.append(" (FT_ID , ENV_ID )");
			createFtEntry.append(" SELECT :ftId, ENV_ID FROM CI_FT_K WHERE ROWNUM=1 ");
			addParams(paramsList, "ftId", ftId,"FT_ID");
			executeQuery(createFtEntry, "Insert into CI_FT_K (FT_ID,....", paramsList);
		}

		private String generateNewFtId(String saId){
			PreparedStatement preparedStatement = null;
			String ftId ="";
			StringBuilder getFtId = new StringBuilder();
			try{
				getFtId.append("SELECT fn_get_ft_id(:saId) as FT_ID FROM dual ");
				preparedStatement = createPreparedStatement(getFtId.toString(), "");
				preparedStatement.bindString("saId", saId,"SA_ID");
				preparedStatement.setAutoclose(false);
				SQLResultRow sqlResultRow = preparedStatement.firstRow();
				if (notNull(sqlResultRow)) {
					ftId=	sqlResultRow.getString("FT_ID");
				}
				
			} catch (RuntimeException e) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("While generating FT_ID - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside generateNewFtId() method, Error -", e);
			} finally {
				closeConnection(preparedStatement);
			}
			return ftId ;
		}
		private void createBillAndFtEntries( DateTime ilmDateTime)
		{
			Account_Id acctId=new Bill_Id(billId).getEntity().getAccount().getId();
			String newBillid="";
			try {
				newBillid = generateNewBillId(acctId);
				insertBillIdInCiBillK(newBillid);
				createBillEntry(ilmDateTime, newBillid);
				createOrUpdateFt(newBillid);
			}
			catch(Exception e){
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution(newBillid+ " While generating BILL_ID  and creating billentriess - " + e.toString()));
			}
		}
		
		private void insertBillIdInCiBillK(String newbillId) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder createBillKEntry = new StringBuilder();
			paramsList = new ArrayList<>();

				createBillKEntry.append(" INSERT INTO CI_BILL_K  ");
				createBillKEntry.append(" (BILL_ID , ENV_ID ) ");
				createBillKEntry.append(" SELECT :billId, ENV_ID FROM CI_BILL_K WHERE ROWNUM=1 ");
			addParams(paramsList, "billId", newbillId,"BILL_ID");
			executeQuery(createBillKEntry, "Insert into CI_BILL_K (BILL_ID,....", paramsList);
		}

		private String generateNewBillId(Account_Id acctId){
			PreparedStatement preparedStatement = null;
			String billId ="";
			StringBuilder fetchBillIdQuery = new StringBuilder();
			try{
				fetchBillIdQuery.append("SELECT fn_get_bill_id(:acctId) as NEW_BILL_ID FROM dual ");
				preparedStatement = createPreparedStatement(fetchBillIdQuery.toString(), "");

				preparedStatement.bindId("acctId", acctId);
				preparedStatement.setAutoclose(false);
				SQLResultRow sqlResultRow = preparedStatement.firstRow();
				if (notNull(sqlResultRow)) {
					billId=	sqlResultRow.getString("NEW_BILL_ID");
				}
				
			} catch (RuntimeException e) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("While generating BILL_ID - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside generateNewBillId() method, Error -", e);
			} finally {
				closeConnection(preparedStatement);
			}
			return billId ;
		}


		private void createBillEntry(DateTime ilmDateTime, String newBillId) {
			// TODO Auto-generated method stub
			ArrayList<ArrayList<Object>> paramsList = null;
			paramsList = new ArrayList<>();
			StringBuilder createBillEntry = new StringBuilder();
			Date billDate = ilmDateTime.getDate();
			Date dueDate = billDate.addDays(1);

			createBillEntry.append(" INSERT INTO ci_bill  ");
			createBillEntry.append(" (BILL_ID , BILL_CYC_CD, WIN_START_DT, ACCT_ID, BILL_STAT_FLG, BILL_DT, ");
			createBillEntry.append(" DUE_DT, CRE_DTTM, COMPLETE_DTTM, LATE_PAY_CHARGE_SW, LATE_PAY_CHARGE_DT,ALLOW_REOPEN_SW, ");
			createBillEntry.append(" VERSION, NEXT_CR_RVW_DT, CR_NOTE_FR_BILL_ID, APAY_CRE_DT, APAY_AMT, ARCHIVE_SW, APAY_STOP_USER_ID, ");
			createBillEntry.append(" APAY_STOP_DTTM, APAY_STOP_AMT, APAY_STOP_CRE_DT, ADHOC_BILL_SW, GRP_REF_VAL, TD_ENTRY_ID, TRIAL_BILL_ID, ");
			createBillEntry.append(" ILM_DT, ILM_ARCH_SW, ALT_BILL_ID) ");
			createBillEntry.append(" SELECT :newBillId , ' ' AS  BILL_CYC_CD, null AS WIN_START_DT, ACCT_ID, :billStatFlg , ");
			createBillEntry.append(" :billDate , :dueDate, :createDate ,:completeDate , LATE_PAY_CHARGE_SW, ");
			createBillEntry.append(" LATE_PAY_CHARGE_DT, ALLOW_REOPEN_SW,:version, NEXT_CR_RVW_DT, :billId , APAY_CRE_DT, ");
			createBillEntry.append(" APAY_AMT, ARCHIVE_SW, APAY_STOP_USER_ID, APAY_STOP_DTTM, APAY_STOP_AMT, APAY_STOP_CRE_DT, ADHOC_BILL_SW, GRP_REF_VAL, ");
			createBillEntry.append(" TD_ENTRY_ID, TRIAL_BILL_ID, :ilmDateTime, ILM_ARCH_SW, ' '  AS ALT_BILL_ID  ");
			createBillEntry.append(" FROM CI_BILL WHERE BILL_ID =:billId ");

			addParams(paramsList, "billStatFlg", "C","BILL_STAT_FLG");
			addParams(paramsList, "ilmDateTime", ilmDateTime,"ILM_DT");
			addParams(paramsList, "createDate", ilmDateTime,"CRE_DTTM");
			addParams(paramsList, "completeDate", ilmDateTime,"COMPLETE_DTTM");
			addParams(paramsList, "billDate", billDate,"BILL_DT");
			addParams(paramsList, "dueDate", dueDate,"DUE_DT");
			addParams(paramsList, "newBillId", newBillId,"BILL_ID");
			addParams(paramsList, "billId", billId,"CR_NOTE_FR_BILL_ID");
			addParams(paramsList, "version", "1","VERSION");

			executeQuery(createBillEntry, "Insert into CI_BILL (BILL_ID,....", paramsList);
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
						} else if ("NullableDateTime".equalsIgnoreCase(objList.get(2) + "")) {
							preparedStatement.bindDateTime(objList.get(0) + "", (DateTime) objList.get(1));
						}
						else if (objList.get(1) instanceof Bill_Id) {
							preparedStatement.bindId(objList.get(0) + "",(Bill_Id) objList.get(1));
						}
					}
				}
				int count = preparedStatement.executeUpdate();
				logger.debug("Rows inserted by query " + message + " are: " + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside custom credit note interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table/s " + message));
			}

			if (preparedStatement != null) {
				closeConnection(preparedStatement);
			}
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
		private void closeConnection(PreparedStatement preparedStatement) {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

/*		private void updateCompletedBills(List<String> sqlList) {
			if(!sqlList.isEmpty() && sqlList.size() > 0){
				for(String billId : sqlList){
					markPending(billId);
				}
			}

		}
*/
		private List<String> updatePendingBills(String billId) {

			StringBuilder sb= new StringBuilder();
			PreparedStatement ps= null;
			Account_Id acctId=new Bill_Id(billId).getEntity().getAccount().getId();
			List<String> billList = new ArrayList<>();
			sb.append("SELECT BILL_ID FROM CI_BILL WHERE ACCT_ID=:acctId and BILL_STAT_FLG =:billStatFlg");
			try{
				ps=createPreparedStatement(sb.toString(),"");
				ps.bindId("acctId", acctId);
				ps.bindLookup("billStatFlg", BillStatusLookup.constants.PENDING);
				ps.setAutoclose(false);
				if(ps.list() != null && ps.list().size() > 0){
					for (SQLResultRow rs : ps.list()) {
						billList.add(rs.getString("BILL_ID"));
						markComplete(rs);
					}
				}

			}
			catch(Exception e){
				logger.error("Error updating Pending Bills : "+e.getMessage());
			}
			finally {
				if(ps != null){
					ps.close();
					ps = null;
				}
			}
			 return billList;
		}


/*		private void markPending(String billId) {
			StringBuilder sb= new StringBuilder();
			PreparedStatement updateCompBill= null;
			sb.append("UPDATE CI_BILL SET BILL_STAT_FLG = :billStatusFlag WHERE BILL_ID=:billId");
			try{
				updateCompBill =createPreparedStatement(sb.toString(),"");
				updateCompBill.bindLookup("billStatusFlag",BillStatusLookup.constants.PENDING);
				updateCompBill.bindString("billId",billId,"BILL_ID");
				updateCompBill.setAutoclose(false);
				updateCompBill.executeUpdate();
			}
			catch(Exception e){
				logger.error("Update Bill Pending with errors :"+e.getMessage());
			}
			finally{
				if(updateCompBill != null){
					updateCompBill.close();
					updateCompBill = null;
				}
			}
		}
*/
		private void markComplete(SQLResultRow rs) {
			StringBuilder sb= new StringBuilder();
			PreparedStatement updatePendBill= null;
			sb.append("UPDATE CI_BILL SET BILL_STAT_FLG = :billStatusFlag WHERE BILL_ID=:billId");
			try{
				updatePendBill =createPreparedStatement(sb.toString(),"");
				updatePendBill.bindLookup("billStatusFlag",BillStatusLookup.constants.COMPLETE);
				updatePendBill.bindString("billId",rs.getString("BILL_ID"),"BILL_ID");
				updatePendBill.setAutoclose(false);
				updatePendBill.executeUpdate();
			}
			catch(Exception e){
				logger.error("Update Bill Completion with errors :"+e.getMessage());
			}
			finally{
				if(updatePendBill != null){
					updatePendBill.close();
					updatePendBill = null;
				}
			}
		}

/*		private void markBsegFrozen(String billId) {

			StringBuilder sb=new StringBuilder();
			PreparedStatement ps=null;
			sb.append("UPDATE CI_BSEG SET BSEG_STAT_FLG=:bsegFlg WHERE BILL_ID=:billId");
			try{
				ps=createPreparedStatement(sb.toString(),"");
				ps.setAutoclose(false);
				ps.bindLookup("bsegFlg", BillSegmentStatusLookup.constants.FROZEN);
				ps.bindString("billId",billId,"BILL_ID");
				ps.executeUpdate();
			}
			catch(Exception e){
				logger.error("Error updating Bill Segment to Frozen : "+e.getMessage());
			}
			finally {
				if(ps != null){
					ps.close();
					ps=null;
				}
			}

		}
*/
		/**
		 * validateInvoice() method Checks whether invoice exists in ORMB.
		 * 
		 * @param aInboundCreditNoteProcessingDataId
		 * @return
		 * @throws RunAbortedException
		 */
		private boolean validateInvoice(
				InboundCreditNoteProcessingData_Id aInboundCreditNoteProcessingDataId) {
			String invoiceNumber = CommonUtils.CheckNull(aInboundCreditNoteProcessingDataId.getAltBillId());
			String validationMessageNumber = null;
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement("SELECT BILL_ID FROM CI_BILL WHERE BILL_ID=:invoiceNumber","");
				preparedStatement.bindString("invoiceNumber", invoiceNumber.trim(),"BILL_ID");
				preparedStatement.setAutoclose(false);
				if (notNull(preparedStatement.firstRow())) {
					billId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("BILL_ID"));
				} else {
					validationMessageNumber = "Invoice Number does not exist in ORMB.";
					logger.info("validationMessageNumber: "+ validationMessageNumber);
					updateStagingTableStatus(aInboundCreditNoteProcessingDataId.getEventId(), customCreditNoteInterfaceLookUp.getError().trim(), "0",
							"0", "Invoice Number does not exist in ORMB.",aInboundCreditNoteProcessingDataId.getAltBillId());
					return false;
				}
			} catch (Exception e) {
				logger.error("Inside catch block of validateInvoice() method-", e);
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
			// In case error occurs, roll back all changes for the current transaction and log error.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}


		/**
		 * logError() method stores the error information in the List and does
		 * roll back all the database transaction of this unit.
		 * @param aEventId
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aMessageDescription
		 * @param aAltBillId
		 * @return
		 */
		private boolean logError(String aEventId, String aMessageCategory,
				String aMessageNumber, String aMessageDescription, String aAltBillId) {
			eachCreditNoteStatusList = new ArrayList<String>();
			eachCreditNoteStatusList.add(0, aEventId);
			eachCreditNoteStatusList.add(1,customCreditNoteInterfaceLookUp.getError().trim()); // lookup value ERROR
			eachCreditNoteStatusList.add(2, aMessageCategory);
			eachCreditNoteStatusList.add(3, aMessageNumber);
			eachCreditNoteStatusList.add(4, aMessageDescription);
			eachCreditNoteStatusList.add(5, aAltBillId);
			updateCreditNoteStatusList.add(eachCreditNoteStatusList);
			eachCreditNoteStatusList = null;
			// Excepted to do roll back
			rollbackToSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));
			addError(CustomMessageRepository.exceptionInExecution(aMessageDescription));			
			return false; // intentionally kept false as roll back has to occur
			// here
		}
		
		/**
		 * updateHierStagingStatus() method updates the CM_INV_RECALC_STG table
		 * with processing status.
		 * @param aEventId
		 * @param processingStatus
		 * @param messageCategoryNumber
		 * @param aMessageNumber
		 * @param aMessageDescription
		 * @param aAltBillId
		 */
		private void updateStagingTableStatus(String aEventId, String processingStatus,
				String messageCategoryNumber, String aMessageNumber,
				String aMessageDescription, String aAltBillId) {
			PreparedStatement preparedStatement = null;
			try {
				if (CommonUtils.CheckNull(aMessageDescription).trim().length() > 250) {
					aMessageDescription = aMessageDescription.substring(0, 250);
				}
				preparedStatement = createPreparedStatement("UPDATE CM_INV_RECALC_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, "
						+ " MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:actualErrorMessageNumber, ERROR_INFO =:errorDescription "
						+ " WHERE EVENT_ID =:eventId","");				
				preparedStatement.bindString("status", processingStatus,"BO_STATUS_CD");
				preparedStatement.bindString("messageCategory",messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("actualErrorMessageNumber",aMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription",aMessageDescription, "ERROR_INFO");
				preparedStatement.bindString("eventId", aEventId,"EVENT_ID");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Inside catch block of updateStagingTableStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}// end method		
	}// end worker

	public static final class CreditNoteProcessingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String altBillId;

		public CreditNoteProcessingData_Id(String altBillId) {
			setAltBillId(altBillId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getAltBillId() {
			return altBillId;
		}

		public void setAltBillId(String altBillId) {
			this.altBillId = altBillId;
		}		
	}

	public static final class InboundCreditNoteProcessingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String eventId;
		private String eventCode;
		private String altBillId;
		private String reasonCode;

		public InboundCreditNoteProcessingData_Id(String eventId,
				String eventCode, String altBillId,	String reasonCode) {
			setEventId(eventId);
			setEventCode(eventCode);
			setAltBillId(altBillId);
			setReasonCode(reasonCode);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}

		public String getEventCode() {
			return eventCode;
		}

		public void setEventCode(String eventCode) {
			this.eventCode = eventCode;
		}

		public String getAltBillId() {
			return altBillId;
		}

		public void setAltBillId(String altBillId) {
			this.altBillId = altBillId;
		}

		public String getReasonCode() {
			return reasonCode;
		}

		public void setReasonCode(String reasonCode) {
			this.reasonCode = reasonCode;
		}
	} // end Id class
}