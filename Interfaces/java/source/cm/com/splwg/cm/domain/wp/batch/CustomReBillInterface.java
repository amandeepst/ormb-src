/*******************************************************************************
 * FileName                   : CustomReBillInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jul 7, 2015
 * Version Number             : 0.5
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Jul 07, 2015	    Preeti		 Implemented all the requirements for CD2.	
0.2		 NA				Sep 30, 2015        Preeti		 Removal of empty finally blocks.
0.3		 NA				Nov 10, 2016        Preeti		 Fix to update temporary table logic.
0.4		 NA				Jan 09, 2017        Preeti		 Batch redesigning as per NAP-10686.
0.5		 NA				Dec 21, 2017        Preeti		 Implementation of Bill generation strategy using application service logic.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.List;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillableChargeStatusLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.api.lookup.UnableToCompleteBillActionLookup;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.BillCompletionInputData;
import com.splwg.ccb.domain.billing.bill.BillGenerationData;
import com.splwg.ccb.domain.customerinfo.account.Account;
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
public class CustomReBillInterface extends
CustomReBillInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CustomReBillInterface.class);
	private static final CustomReBillInterfaceLookUp customReBillInterfaceLookUp = new CustomReBillInterfaceLookUp();

	// Default constructor
	public CustomReBillInterface() {
	}

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {	
		
		customReBillInterfaceLookUp.setLookUpConstants();	
		//delete from temporary table
		deleteFromTmpTable("CM_BILL_TMP");		

		List<ThreadWorkUnit> threadWorkUnitList = getReBillData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}
	
	/**
	 * deleteFromTmpTable() method will delete from the table provided as
	 * input.
	 * 
	 * @param inputPayReqTmpTable
	 */
	private void deleteFromTmpTable(String inputTmpTable) {
		PreparedStatement preparedStatement = null;
		
		try {
			preparedStatement = createPreparedStatement("DELETE FROM " + inputTmpTable,"");
			preparedStatement.execute();
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteFromTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside deleteFromTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("commit","");
			preparedStatement.execute();
		} catch (RuntimeException e) {
			logger.error("Inside deleteFromTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	// *********************** getReBillData
	// Method******************************

	/**
	 * getReBillData() method
	 * selects distinct set of BILL_DT from CI_BILL and CM_INV_RECALC_STG tables.
	 * 
	 * @return List billDate
	 */
	private List<ThreadWorkUnit> getReBillData() {
		
		PreparedStatement preparedStatement = null;
		InboundReBillProcessingData_Id inboundReBillProcessingDataId = null;
		
		//***********************
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		//***********************
		
		Date billDate=null;
		String billCycCode="";
		String acctId="";
		try {			
			preparedStatement = createPreparedStatement("SELECT distinct B.BILL_DT, B.BILL_CYC_CD, B.ACCT_ID" +
					" FROM CM_INV_RECALC_STG A, CI_BILL B" +
					" WHERE trunc(A.STATUS_UPD_DTTM) = trunc(SYSDATE)" +
					" AND A.BILL_ID=B.BILL_ID and A.BO_STATUS_CD=:status1" +
					" ORDER BY B.BILL_DT","");			
			preparedStatement.bindString("status1", customReBillInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				billDate = resultSet.getDate("BILL_DT");
				billCycCode = CommonUtils.CheckNull(resultSet.getString("BILL_CYC_CD"));
				acctId = CommonUtils.CheckNull(resultSet.getString("ACCT_ID"));
				inboundReBillProcessingDataId = new InboundReBillProcessingData_Id(billDate, billCycCode, acctId);				
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(inboundReBillProcessingDataId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				inboundReBillProcessingDataId = null;
				//*************************
			}
		} catch (Exception e) {
			logger.error("Inside catch block of getReBillData() method-", e);
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

	public Class<CustomReBillInterfaceWorker> getThreadWorkerClass() {
		return CustomReBillInterfaceWorker.class;
	}

	public static class CustomReBillInterfaceWorker extends
	CustomReBillInterfaceWorker_Gen {

		// Default constructor
		public CustomReBillInterfaceWorker() {
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
			
			InboundReBillProcessingData_Id InboundReBillProcessingDataId = (InboundReBillProcessingData_Id) unit.getPrimaryId();
			
			removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
			setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));//Required to nullify the effect of database transactions in case of error scenario
			
			try {
				// ****************** Re Bill ******************

				String returnStatus = createOrUpdateReBill(InboundReBillProcessingDataId);
				if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {																		
					logger.info("Recalculation failed for Bill dated -"+InboundReBillProcessingDataId.getBillDate());
					logger.info("Recalculation failed for Bill cycle -"+InboundReBillProcessingDataId.getBillCycCode());
				} else {
					logger.info("Recalculation completed for Bill dated -"+InboundReBillProcessingDataId.getBillDate());
					logger.info("Recalculation completed for Bill cycle -"+InboundReBillProcessingDataId.getBillCycCode());
				}
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit: " + e);
			}		
			InboundReBillProcessingDataId = null;
			return true;
		}

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {		
			
			PreparedStatement preparedStatement = null;
			try {			
				preparedStatement = createPreparedStatement("UPDATE CI_BILL B SET B.BILL_DT=(SELECT A.BILL_DT" +
						" FROM CM_BILL_TMP A WHERE A.BILL_ID=B.BILL_ID)" +
						" WHERE B.BILL_ID IN (SELECT C.BILL_ID FROM CM_BILL_TMP C)","");
				int rowsInserted=preparedStatement.executeUpdate();
				logger.info("Rows Updated in CI_BILL table"+rowsInserted);
			} catch (Exception e) {
				logger.error("Inside catch block of getReBillData() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			try {			
				preparedStatement = createPreparedStatement("UPDATE CI_BILL B SET B.WIN_START_DT=(SELECT A.BILL_DT" +
						" FROM CM_BILL_TMP A WHERE A.BILL_ID=B.BILL_ID)" +
						" WHERE B.BILL_ID IN (SELECT C.BILL_ID FROM CM_BILL_TMP C) AND B.WIN_START_DT IS NOT NULL","");
				int rowsInserted=preparedStatement.executeUpdate();
				logger.info("Rows Updated in CI_BILL table"+rowsInserted);
			} catch (Exception e) {
				logger.error("Inside catch block of getReBillData() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			super.finalizeThreadWork();
		}

		/**
		 * createOrUpdateReBill() method
		 * 
		 * @param aInboundReBillProcessingDataId
		 * @return
		 * @throws RunAbortedException
		 */
		private String createOrUpdateReBill(
				InboundReBillProcessingData_Id aInboundReBillProcessingDataId) {
			
			Date billDate=aInboundReBillProcessingDataId.getBillDate();
			
			PreparedStatement preparedStatement = null;	
			StringBuilder stringBuilder = new StringBuilder();
			try {			
				stringBuilder.append("INSERT INTO CM_BILL_TMP (BILL_ID,BILL_DT) ");
				stringBuilder.append("SELECT A.BILL_ID, A.BILL_DT FROM CI_BILL A ");
				stringBuilder.append("WHERE TO_CHAR(A.BILL_DT,'YYYY-MM-DD') >= :billDate ");
				stringBuilder.append("AND A.ACCT_ID =:acctId AND A.BILL_STAT_FLG='C' ");
				stringBuilder.append("and A.BILL_ID NOT IN (SELECT TMP.BILL_ID FROM CM_BILL_TMP TMP) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.bindString("acctId", aInboundReBillProcessingDataId.getAcctId().trim(), "ACCT_ID");	
				int rowsInserted=preparedStatement.executeUpdate();
				logger.info("Rows inserted into CM_BILL_TMP"+rowsInserted);
			} catch (Exception e) {
				logger.error("Inside catch block of getReBillData() method-", e);
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
				stringBuilder.append("UPDATE CI_BILL B SET B.BILL_DT=(SELECT (C.WIN_START_DT-1) ");
				stringBuilder.append("FROM CM_BILL_CYC_SCH C WHERE C.BILL_CYC_CD=:billCycCode AND TO_CHAR(C.WIN_END_DT,'YYYY-MM-DD') = :billDate) ");
				stringBuilder.append("WHERE exists (SELECT A.BILL_ID FROM CM_BILL_TMP A where A.BILL_ID=B.BILL_ID) AND B.ACCT_ID=:acctId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.bindString("billCycCode", aInboundReBillProcessingDataId.getBillCycCode().trim(), "BILL_CYC_CD");	
				preparedStatement.bindString("acctId", aInboundReBillProcessingDataId.getAcctId().trim(), "ACCT_ID");	
				int rowsInserted=preparedStatement.executeUpdate();
				logger.info("Rows Updated in CI_BILL table"+rowsInserted);
			} catch (Exception e) {
				logger.error("Inside catch block of getReBillData() method-", e);
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
				stringBuilder.append("UPDATE CI_BILL B SET B.WIN_START_DT=(SELECT (C.WIN_START_DT-1) ");
				stringBuilder.append("FROM CM_BILL_CYC_SCH C WHERE C.BILL_CYC_CD=:billCycCode AND TO_CHAR(C.WIN_END_DT,'YYYY-MM-DD') = :billDate) ");
				stringBuilder.append("WHERE exists (SELECT A.BILL_ID FROM CM_BILL_TMP A where A.BILL_ID=B.BILL_ID) AND B.ACCT_ID=:acctId AND B.WIN_START_DT IS NOT NULL ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.bindString("billCycCode", aInboundReBillProcessingDataId.getBillCycCode().trim(), "BILL_CYC_CD");	
				preparedStatement.bindString("acctId", aInboundReBillProcessingDataId.getAcctId().trim(), "ACCT_ID");	
				int rowsInserted=preparedStatement.executeUpdate();
				logger.info("Rows Updated in CI_BILL table"+rowsInserted);
			} catch (Exception e) {
				logger.error("Inside catch block of getReBillData() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}			

			try {
				
				//SQL to find billable service agreements based on cut off date.
				Boolean eligibleToBill=Boolean.FALSE;
				stringBuilder = null;
				stringBuilder = new StringBuilder();

				stringBuilder.append("SELECT DISTINCT A.SA_ID FROM CI_SA A, CI_BILL_CHG B WHERE A.ACCT_ID=:acctId ");
				stringBuilder.append("AND A.SA_STATUS_FLG IN (:s1, :s2, :s3, :s4) ");
				stringBuilder.append("AND A.SA_ID=B.SA_ID AND B.BILLABLE_CHG_STAT=:status AND B.END_DT<=:billDate ");
				stringBuilder.append("AND NOT EXISTS (SELECT 'X' FROM CI_BSEG_CALC BC , CI_BSEG BS WHERE BC.BILLABLE_CHG_ID=B.BILLABLE_CHG_ID AND BC.BSEG_ID=BS.BSEG_ID ");
				stringBuilder.append("AND BS.BSEG_STAT_FLG='50') ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctId", aInboundReBillProcessingDataId.getAcctId().trim(), "ACCT_ID");	
				preparedStatement.bindLookup("s1", ServiceAgreementStatusLookup.constants.ACTIVE);
				preparedStatement.bindLookup("s2", ServiceAgreementStatusLookup.constants.PENDING_STOP);
				preparedStatement.bindLookup("s3", ServiceAgreementStatusLookup.constants.STOPPED);
				preparedStatement.bindLookup("s4", ServiceAgreementStatusLookup.constants.REACTIVATED);
				preparedStatement.bindLookup("status", BillableChargeStatusLookup.constants.BILLABLE);
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.setAutoclose(false);	
				
				if(notNull(preparedStatement.firstRow())){
					eligibleToBill=Boolean.TRUE;
				}
				
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
				
				if (eligibleToBill){
				Bill newBill = null;                
                Account accnt = (Account)(new Account_Id(aInboundReBillProcessingDataId.getAcctId().trim())).getEntity();
               // BillCycle billCycle = (BillCycle)(new BillCycle_Id(aInboundReBillProcessingDataId.getBillCycCode().trim())).getEntity();
                
                  
                BillGenerationData billGenerationData = BillGenerationData.Factory.newInstance();
                billGenerationData.setAccountingDate(getSystemDateTime().getDate().addDays(-1));
                billGenerationData.setCutoffDate(billDate);                
                newBill = accnt.generateAndFreezeBill(billGenerationData);                                      
                
                BillCompletionInputData billCompletionInputData = BillCompletionInputData.Factory.newInstance();
                billCompletionInputData.setBillDate(billDate);
                billCompletionInputData.setShouldAssignSequentialNumber(Bool.FALSE);
                //billCompletionInputData.setWindowStartDate(billDate);
                //billCompletionInputData.setBillCycle(billCycle);
                billCompletionInputData.setUnableToCompleteBillAction(UnableToCompleteBillActionLookup.constants.SHOW_ERROR);                
                newBill.complete(billCompletionInputData); 
                
                String billId= newBill.getId().getIdValue();
                
                //Update Bill cycle code and Win start date on CI_BILL table
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("update ci_bill set win_start_dt=:billDate, bill_cyc_cd=:billCycCode where bill_id=:billId");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.bindString("billCycCode", aInboundReBillProcessingDataId.getBillCycCode().trim(), "BILL_CYC_CD");	
				preparedStatement.bindString("billId", billId.trim(), "BILL_ID");	
				preparedStatement.executeUpdate();
				
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
				//Update Bill cycle code and Win start date on CI_BSEG
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("update ci_bseg set win_start_dt=:billDate, bill_cyc_cd=:billCycCode where bill_id=:billId");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindDate("billDate", billDate);
				preparedStatement.bindString("billCycCode", aInboundReBillProcessingDataId.getBillCycCode().trim(), "BILL_CYC_CD");	
				preparedStatement.bindString("billId", billId.trim(), "BILL_ID");	
				preparedStatement.executeUpdate();
				
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
				}
				
			} catch (Exception e) {
				logger.error("Inside catch block of createOrUpdateReBill() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				String errorMessageNumber = errorMessage.substring(errorMessage.indexOf("Number:") + 8, errorMessage.indexOf("Call Sequence:"));
				String errorMessageCategory = errorMessage.substring(errorMessage.indexOf("Category:") + 10, errorMessage.indexOf("Number"));

				if (errorMessage.contains("Text:") && errorMessage.contains("Description:")) {
					errorMessage = errorMessage.substring(errorMessage.indexOf("Text:"), errorMessage.indexOf("Description:"));
				}
				if (errorMessage.length() > 250) {
					errorMessage = errorMessage.substring(0, 250);
				} else {
					errorMessage = errorMessage.substring(0, errorMessage.length());
				}
				return "false" + "~" + errorMessageCategory + "~" + errorMessageNumber + "~" + errorMessage;
			}				
			
			return "true";
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
	}// end worker class

	public static final class InboundReBillProcessingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private Date billDate;
		private String billCycCode;
		private String acctId;

		public InboundReBillProcessingData_Id(Date billDate, String billCycCode, String acctId) {
			setBillDate(billDate);
			setBillCycCode(billCycCode);
			setAcctId(acctId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public void setBillDate(Date billDate) {
			this.billDate = billDate;
		}

		public Date getBillDate() {
			return billDate;
		}
		public String getBillCycCode() {
			return billCycCode;
		}

		public void setBillCycCode(String billCycCode) {
			this.billCycCode = billCycCode;
		}	
		
		public String getAcctId() {
			return acctId;
		}

		public void setAcctId(String acctId) {
			this.acctId = acctId;
		}
	} // end Id class
}