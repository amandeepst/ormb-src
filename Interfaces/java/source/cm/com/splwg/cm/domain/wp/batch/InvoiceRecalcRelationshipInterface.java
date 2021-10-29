/*******************************************************************************
 * FileName                   : InvoiceRecalcRelationshipInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Dec 05, 2017
 * Version Number             : 0.1
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Dec 05, 2017        Preeti		  Create relationship between bills generated as part of Invoice Recalculation.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
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
public class InvoiceRecalcRelationshipInterface extends
InvoiceRecalcRelationshipInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(InvoiceRecalcRelationshipInterface.class);
	private static final CustomCreditNoteInterfaceLookUp customCreditNoteInterfaceLookUp = new CustomCreditNoteInterfaceLookUp();
	private static final String BILL_ID = "BILL_ID";
	private static final String PARENT_BILL_ID = "parentBillId";


	// Default constructor
	public InvoiceRecalcRelationshipInterface() {
	}	

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		customCreditNoteInterfaceLookUp.setLookUpConstants();				
		
		resetInvRelStgIdSQ();
		List<ThreadWorkUnit> threadWorkUnitList = getInvoiceStagingData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// *********************** getInvoiceStagingData Method******************************

	/**
	 * getInvoiceStagingData() method
	 * selects data from CM_INV_RECALC_STG table.
	 * 
	 * @return threadWorkUnitList
	 */
	private List<ThreadWorkUnit> getInvoiceStagingData() {

		PreparedStatement preparedStatement = null;
		InvoiceStagingData_Id invoiceStagingDataId = null;

		//***********************
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		//***********************

		String billId="";
		String paidInvoiceFlg="";
		String reuseDueDateFlg="";
		String eventId="";
		String type ="";
		String reasonCode="";
		Date processDate=getProcessDateTime().getDate();
		
		processDate=notNull(processDate) ? processDate : getSystemDateTime().getDate();
		Date endDate=processDate.addDays(1);

		try {			
			preparedStatement = createPreparedStatement("SELECT A.BILL_ID," +
					" A.PAID_INVOICE, A.REUSE_DUE_DT, A.EVENT_ID, A.TYPE, A.REASON_CD" +
					" FROM CM_INV_RECALC_STG A" +
					" WHERE A.STATUS_UPD_DTTM >= :processDate AND A.STATUS_UPD_DTTM < :endDate" +
					" AND A.BO_STATUS_CD=:status1 AND A.TYPE=:type AND NOT EXISTS(SELECT 1 FROM" +
					" CM_INV_RELATION_STG WHERE PARENT_BILL_ID=A.BILL_ID)","");
			preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
			preparedStatement.bindString("type", customCreditNoteInterfaceLookUp.getType().trim(), "TYPE");	
			preparedStatement.bindDate("processDate", processDate);
			preparedStatement.bindDate("endDate", endDate);

			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				billId = CommonUtils.CheckNull(resultSet.getString(BILL_ID));
				paidInvoiceFlg = CommonUtils.CheckNull(resultSet.getString("PAID_INVOICE"));
				reuseDueDateFlg = CommonUtils.CheckNull(resultSet.getString("REUSE_DUE_DT"));
				eventId = CommonUtils.CheckNull(resultSet.getString("EVENT_ID"));
				type = CommonUtils.CheckNull(resultSet.getString("TYPE"));
				reasonCode = CommonUtils.CheckNull(resultSet.getString("REASON_CD"));

				invoiceStagingDataId = new InvoiceStagingData_Id(billId,paidInvoiceFlg,reuseDueDateFlg,eventId,type,reasonCode);
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(invoiceStagingDataId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				invoiceStagingDataId = null;
				//*************************
			}
		} catch (Exception e) {
			logger.error("Inside catch block of getInvoiceStagingData() method-", e);
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
	
	@SuppressWarnings("deprecation")
	private void resetInvRelStgIdSQ() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_INV_RELATION_STG_ID}");
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside resetInvRelStgIdSQ() method, Error  -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public Class<InvoiceRecalcRelationshipInterfaceWorker> getThreadWorkerClass() {
		return InvoiceRecalcRelationshipInterfaceWorker.class;
	}

	public static class InvoiceRecalcRelationshipInterfaceWorker extends
	InvoiceRecalcRelationshipInterfaceWorker_Gen {


		// Default constructor
		public InvoiceRecalcRelationshipInterfaceWorker() {
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

			String creditNoteBillId="";
			String newBillId="";		
			PreparedStatement preparedStatement = null;		
			InvoiceStagingData_Id InvoiceStagingDataId = (InvoiceStagingData_Id) unit.getPrimaryId();

			removeSavepoint("Rollback".concat(getParameters().getThreadCount().toString()));					
			setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));//Required to nullify the effect of database transactions in case of error scenario

			//Determine credit note bill id			
			try {
				preparedStatement = createPreparedStatement("SELECT BILL_ID FROM CI_BILL WHERE CR_NOTE_FR_BILL_ID=:originalBillId","");
				preparedStatement.bindString("originalBillId", InvoiceStagingDataId.getBillId().trim(),"CR_NOTE_FR_BILL_ID");
				preparedStatement.setAutoclose(false);
				if (notNull(preparedStatement.firstRow())) {
					creditNoteBillId = CommonUtils.CheckNull(preparedStatement.firstRow().getString(BILL_ID));
				} 
			} catch (Exception e) {
				logger.error("Inside catch block of validateInvoice() method- ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}	

			//Determine new bill id (recalculated invoice to correct original bill)				
			/*try {
				preparedStatement = createPreparedStatement("SELECT B.BILL_ID FROM CI_BILL A, CI_BILL B" +
						" WHERE A.BILL_ID=:originalBillId" +
						" AND A.ACCT_ID=B.ACCT_ID" +
						" AND A.BILL_DT=B.BILL_DT" +
						" AND B.BILL_ID<>A.BILL_ID","");
				preparedStatement.bindString("originalBillId", InvoiceStagingDataId.getBillId().trim(),"BILL_ID");
				preparedStatement.setAutoclose(false);
				if (notNull(preparedStatement.firstRow())) {
					newBillId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("BILL_ID"));
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
			}	*/

			//Insert relationship records for 3 bills into staging table			
			try {					
				insertIntoInvoiceRelationshipTable(InvoiceStagingDataId.getBillId(),creditNoteBillId,
						InvoiceStagingDataId.getEventId(),InvoiceStagingDataId.getPaidInvoiceFlg(),InvoiceStagingDataId.getReuseBillDateFlg(),
						"51",InvoiceStagingDataId.getType(),InvoiceStagingDataId.getReasonCode());

				/*insertIntoInvoiceRelationshipTable(creditNoteBillId,newBillId,
						InvoiceStagingDataId.getEventId(),InvoiceStagingDataId.getPaidInvoiceFlg(),InvoiceStagingDataId.getReuseBillDateFlg(),"52");

				insertIntoInvoiceRelationshipTable(InvoiceStagingDataId.getBillId(),newBillId,
						InvoiceStagingDataId.getEventId(),InvoiceStagingDataId.getPaidInvoiceFlg(),InvoiceStagingDataId.getReuseBillDateFlg(),"53");*/

			} catch (Exception e) {
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} 

			//Update Payment request attributes for the bills associated to invoice recalculation events for the day's run.			
			try {			
				updatePaymentRequestData(InvoiceStagingDataId.getBillId(),newBillId,
						InvoiceStagingDataId.getPaidInvoiceFlg(),InvoiceStagingDataId.getReuseBillDateFlg());

			} catch (Exception e) {
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} 
			return true;
		}

		private void insertIntoInvoiceRelationshipTable(String parentBillId, String childBillId,
				String eventId, String paidInvoiceFlg, String reuseDueDateFlg, String relationshipType, String type,String reasonCode) {

			String taxBsegId = getTaxbillSegment( parentBillId);
			PreparedStatement preparedStatement = null;		
			try {
				preparedStatement = createPreparedStatement("INSERT INTO CM_INV_RELATION_STG" +
						" (PARENT_BILL_ID,CHILD_BILL_ID,RELATIONSHIP_TYPE,EVENT_ID," +
						" PAID_INVOICE,REUSE_DUE_DT,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM, INV_RELATION_STG_ID,TYPE,REASON_CD,TAX_BSEG_ID )" +
						" values (:parentBillId,:childBillId,:relationshipType,:eventId," +
						" :paidInvoiceFlg,:reuseDueDateFlg,sysdate,'Y',null, inv_relation_stg_seq.nextval,:type ,:reasonCode,:taxBsegId)","");
				
				preparedStatement.bindString(PARENT_BILL_ID, parentBillId.trim(),"PARENT_BILL_ID");
				preparedStatement.bindString("childBillId", childBillId.trim(),"CHILD_BILL_ID");
				preparedStatement.bindString("relationshipType", relationshipType.trim(),"RELATIONSHIP_TYPE");
				preparedStatement.bindString("eventId", eventId.trim(),"EVENT_ID");
				preparedStatement.bindString("paidInvoiceFlg", paidInvoiceFlg.trim(),"PAID_INVOICE");
				preparedStatement.bindString("reuseDueDateFlg", reuseDueDateFlg.trim(),"REUSE_DUE_DT");
				preparedStatement.bindString("type", type ,"TYPE");
				preparedStatement.bindString("reasonCode", reasonCode ,"REASON_CD");
				preparedStatement.bindString("taxBsegId", CommonUtils.CheckNull(taxBsegId) ,"TAX_BSEG_ID");

				preparedStatement.executeUpdate();					
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
		}

		private String getTaxbillSegment(String parentBillId) {
			PreparedStatement preparedStatement = null;
			String taxBillSegment= null;

			try {
				preparedStatement = createPreparedStatement(" SELECT BS.BSEG_ID AS TAX_BSEG_ID FROM CI_BSEG BS, CI_BSEG_CAlC CL WHERE "
						+ " BS.BSEG_ID  = CL.BSEG_ID AND CL.RS_CD = :tax AND BS.BILL_ID = :parentBillId ","");

				preparedStatement.bindString(PARENT_BILL_ID, parentBillId.trim(), BILL_ID);
				preparedStatement.bindString("tax", "TAX", "RS_CD");

				preparedStatement.setAutoclose(false);
				if (notNull(preparedStatement.firstRow())) {
					taxBillSegment = CommonUtils.CheckNull(preparedStatement.firstRow().getString("TAX_BSEG_ID"));
				}
			}
			catch (Exception e) {
				logger.error("Inside catch block of getTaxbillSegment() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return taxBillSegment;
		}

		private void updatePaymentRequestData(String parentBillId, String childBillId,
				String paidInvoiceFlg, String reuseDueDateFlg) {

			PreparedStatement preparedStatement = null;		
			try {
				preparedStatement = createPreparedStatement("UPDATE CM_PAY_REQ" +
						" SET CR_NOTE_FR_BILL_ID=:parentBillId,REUSE_DUE_DT=:reuseDueDateFlg," +
						" INV_RECALC_FLG=:paidInvoiceFlg WHERE BILL_ID=:childBillId","");
				preparedStatement.bindString(PARENT_BILL_ID, parentBillId.trim(),"PARENT_BILL_ID");
				preparedStatement.bindString("childBillId", childBillId.trim(),"CHILD_BILL_ID");
				preparedStatement.bindString("paidInvoiceFlg", paidInvoiceFlg.trim(),"PAID_INVOICE");
				preparedStatement.bindString("reuseDueDateFlg", reuseDueDateFlg.trim(),"REUSE_DUE_DT");
				preparedStatement.executeUpdate();					
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
		}

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */

		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {		
			super.finalizeThreadWork();
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

	}// end worker	

	public static final class InvoiceStagingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String billId;
		private String paidInvoiceFlg;
		private String reuseBillDateFlg;
		private String eventId;
		private String type;
		private String reasonCode;

		public InvoiceStagingData_Id(String billId,String paidInvoiceFlg,String reuseBillDateFlg,String eventId, String type,String reasonCode) {
			setBillId(billId);
			setPaidInvoiceFlg(paidInvoiceFlg);
			setReuseBillDateFlg(reuseBillDateFlg);
			setEventId(eventId);
			setType(type);
			setReasonCode(reasonCode);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getBillId() {
			return billId;
		}

		public void setBillId(String billId) {
			this.billId = billId;
		}

		public String getReasonCode() {
			return reasonCode;
		}

		public void setReasonCode(String reasonCode) {
			this.reasonCode = reasonCode;
		}

		public String getType() {
			return type;
		}

		public void setType(String type) {
			this.type = type;
		}

		public String getPaidInvoiceFlg() {
			return paidInvoiceFlg;
		}

		public void setPaidInvoiceFlg(String paidInvoiceFlg) {
			this.paidInvoiceFlg = paidInvoiceFlg;
		}	

		public String getReuseBillDateFlg() {
			return reuseBillDateFlg;
		}

		public void setReuseBillDateFlg(String reuseBillDateFlg) {
			this.reuseBillDateFlg = reuseBillDateFlg;
		}	

		public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}	
	} // end Id class
}