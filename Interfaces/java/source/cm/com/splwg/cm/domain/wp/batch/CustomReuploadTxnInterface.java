/*******************************************************************************
 * FileName                   : CustomReuploadTxnInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jan 9, 2017
 * Version Number             : 0.2
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Jan 09, 2017        Preeti		  Batch redesigning as per NAP-10686.
0.2		 NA				Dec 07, 2017        Preeti		  Add attributes to Payment request data as required for Invoice Recalculation.
0.3		 NAP-24192		Mar 14, 2018		RIA			  CM_TXNUP Interface - Add ILM columns in INSERT SQLs. 
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
   @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"})
 */
public class CustomReuploadTxnInterface extends
CustomReuploadTxnInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CustomReuploadTxnInterface.class);
	private static final CustomCreditNoteInterfaceLookUp customCreditNoteInterfaceLookUp = new CustomCreditNoteInterfaceLookUp();

	// Default constructor
	public CustomReuploadTxnInterface() {
	}	

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {			
		customCreditNoteInterfaceLookUp.setLookUpConstants();

		ThreadWorkUnit threadworkUnit = new ThreadWorkUnit();
		threadworkUnit.setPrimaryId(new BatchControl_Id(getParameters().getBatchControlId().getIdValue()));
		ArrayList<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList.add(threadworkUnit);

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	public Class<CustomReuploadTxnInterfaceWorker> getThreadWorkerClass() {
		return CustomReuploadTxnInterfaceWorker.class;
	}

	public static class CustomReuploadTxnInterfaceWorker extends
	CustomReuploadTxnInterfaceWorker_Gen {

		// Default constructor
		public CustomReuploadTxnInterfaceWorker() {
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

			removeSavepoint("Rollback".concat(getParameters().getThreadCount().toString()));					
			setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));//Required to nullify the effect of database transactions in case of error scenario

			try {					
				//******************************Execute out bound interfaces for Credit Note data***********************//
				executeOutboundBatches();				
				//******************************Delete original charges and upload original transactions back into transaction staging tables*********************//
				updateBillableChgStatus();

			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit: " + e);
			} 
			return true;
		}

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */

		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {		
			super.finalizeThreadWork();
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

		private boolean executeOutboundBatches() {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();		

			//**********************CM_EPRCI/all event charges (Adjustment/Adhoc charge)********************//
			//**********************CM_EVENT_PRICE********************//
			try {
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE ");
				stringBuilder.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - Start Change
//				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT) ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - End Change
				stringBuilder.append("SELECT A.EVENT_ID, A.PRICEITEM_CD, A.PRICE_CATEGORY, (A.CALC_AMT*(-1)), ");
				stringBuilder.append("A.CURRENCY_CD, A.ACCT_TYPE, A.BILL_REFERENCE, A.INVOICEABLE_FLG, ");
				stringBuilder.append("'Y', SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM, A.ACCRUED_DATE, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - Start Change
//				stringBuilder.append("A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT ");	
				stringBuilder.append("A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW  ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - End Change
				stringBuilder.append("FROM CM_EVENT_PRICE A, CM_INV_RECALC_STG B, CM_BILL_ID_MAP C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND C.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND A.BILL_REFERENCE=C.BILL_REFERENCE ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs. - End Change
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_EVENT_PRICE -" + count);

			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			//**********************CM_PYRQI********************//
			//**********************CM_PAY_REQ********************//
			//Only populate for paid invoice flg=Y otherwise payment request should be suppressed and payment confirmation record would be created
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_PAY_REQ (BILL_ID, LINE_ID, BILL_DT, ");
				stringBuilder.append("CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, PER_ID_NBR, PAY_TYPE, BILL_AMT, "); 
				stringBuilder.append("CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ, ");
				stringBuilder.append("FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES, ");
				stringBuilder.append("CREATE_DTTM, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,REUSE_DUE_DT,INV_RECALC_FLG) ");
				stringBuilder.append("SELECT C.BILL_ID, A.LINE_ID, C.BILL_DT, "); 
				stringBuilder.append("C.CR_NOTE_FR_BILL_ID, C.ALT_BILL_ID, A.ACCT_TYPE, "); 
				stringBuilder.append("A.CIS_DIVISION, A.PER_ID_NBR, DECODE(SIGN(A.BILL_AMT-0),-1,'DR','CR'), (A.BILL_AMT*(-1)), A.CURRENCY_CD, ");
				stringBuilder.append("A.IS_IND_FLG, A.SUB_STLMNT_LVL, A.SUB_STLMNT_LVL_REF, A.REL_RSRV_FLG, "); 
				stringBuilder.append("A.REL_WAF_FLG, A.IS_IMD_FIN_ADJ, ");
				stringBuilder.append("A.FIN_ADJ_MAN_NRT, A.FASTEST_ROUTE_INDICATOR, A.CASE_IDENTIFIER, A.PAY_REQ_GRANULARITIES, "); 
				stringBuilder.append("C.COMPLETE_DTTM, SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM,B.REUSE_DUE_DT,B.PAID_INVOICE ");
				stringBuilder.append("FROM CM_PAY_REQ A, CM_INV_RECALC_STG B, CI_BILL C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND A.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				stringBuilder.append("AND B.PAID_INVOICE='Y' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_PAY_REQ- " + count);
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_PAY_CNF_STG (TXN_HEADER_ID,UPLOAD_DTTM,BO_STATUS_CD,STATUS_UPD_DTTM,MESSAGE_CAT_NBR,MESSAGE_NBR, ");
				stringBuilder.append("ERROR_INFO,PAY_DT,EXT_SOURCE_CD,EXT_TRANSMIT_ID,FINANCIAL_DOC_ID,FINANCIAL_DOC_LINE_ID, "); 
				stringBuilder.append("TENDER_AMT,CURRENCY_CD,BANKING_ENTRY_STATUS) ");
				stringBuilder.append("SELECT C.BILL_ID, SYSDATE,'UPLD',NULL,0,0,' ',TRUNC(SYSDATE),A.EXT_SOURCE_CD, ");
				stringBuilder.append("C.BILL_ID,C.BILL_ID,'1',(-1*A.TENDER_AMT),A.CURRENCY_CD,'CREDIT_NOTE' ");
				stringBuilder.append("FROM CM_PAY_CNF_STG A, CM_INV_RECALC_STG B, CI_BILL C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) "); 
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 "); 
				stringBuilder.append("AND A.FINANCIAL_DOC_ID=B.BILL_ID ");
				stringBuilder.append("AND A.FINANCIAL_DOC_ID=C.CR_NOTE_FR_BILL_ID "); 
				stringBuilder.append("AND B.PAID_INVOICE='N' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_PAY_REQ- " + count);
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

			//**********************CM_NTPRC********************//
			//**********************CM_BILL_ID_MAP********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {						
				stringBuilder.append("INSERT INTO CM_BILL_ID_MAP ");
				stringBuilder.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("PER_ID_NBR, CIS_DIVISION, BILL_END_DT, ");
				stringBuilder.append("BILL_AMT, CURRENCY_CD, EVENT_TYPE_ID, EVENT_PROCESS_ID, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, BILL_REFERENCE, ACCT_TYPE) ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, BILL_REFERENCE, ACCT_TYPE,ILM_DT,ILM_ARCH_SW, BILL_MAP_ID) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("SELECT C.BILL_ID, A.BILL_START_DT, C.ALT_BILL_ID, ");
				stringBuilder.append("C.BILL_DT, C.CR_NOTE_FR_BILL_ID, A.PER_ID_NBR, ");
				stringBuilder.append("A.CIS_DIVISION, A.BILL_END_DT, ");
				stringBuilder.append("(A.BILL_AMT*(-1)), A.CURRENCY_CD, A.EVENT_TYPE_ID, A.EVENT_PROCESS_ID, ");
				stringBuilder.append("SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("A.BILL_REFERENCE, A.ACCT_TYPE ");
				stringBuilder.append("A.BILL_REFERENCE, A.ACCT_TYPE, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW, bill_id_map_seq.nextval ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_BILL_ID_MAP A, CM_INV_RECALC_STG B, CI_BILL C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND A.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_BILL_ID_MAP -" + count);
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));

			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			//**********************CM_NON_TRANS_PRICE********************//			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {		
				stringBuilder.append("INSERT INTO CM_NON_TRANS_PRICE ");
				stringBuilder.append("(NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM) ");
				stringBuilder.append("BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("SELECT A.NON_EVENT_ID, A.PER_ID_NBR, A.ACCT_TYPE, A.PRICEITEM_CD, ");
				stringBuilder.append("A.PRICE_CATEGORY, (A.CALC_AMT*(-1)), A.CURRENCY_CD, ");
				stringBuilder.append("A.BILL_REFERENCE, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("A.INVOICEABLE_FLG, SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM ");
				stringBuilder.append("A.INVOICEABLE_FLG, SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_NON_TRANS_PRICE A, CM_INV_RECALC_STG B, CM_BILL_ID_MAP C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND C.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND A.BILL_REFERENCE=C.BILL_REFERENCE ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_NON_TRANS_PRICE for minimum charges-" + count);
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));

			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			//*******************CM_INVDT*******************//
			//**********************CM_INVOICE_DATA********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INVOICE_DATA (BILL_ID,ALT_BILL_ID,BILLING_PARTY_ID, ");
				stringBuilder.append("CIS_DIVISION,ACCT_TYPE,WPBU,BILL_DT,BILL_CYC_CD,WIN_START_DT,WIN_END_DT, "); 
				stringBuilder.append("CURRENCY_CD,CALC_AMT,MERCH_TAX_REG_NBR,WP_TAX_REG_NBR,TAX_AUTHORITY,TAX_TYPE, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("PREVIOUS_AMT,CR_NOTE_FR_BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
				stringBuilder.append("PREVIOUS_AMT,CR_NOTE_FR_BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("SELECT C.BILL_ID, C.ALT_BILL_ID, A.BILLING_PARTY_ID, "); 
				stringBuilder.append("A.CIS_DIVISION, A.ACCT_TYPE, A.WPBU, C.BILL_DT, A.BILL_CYC_CD, A.WIN_START_DT, A.WIN_END_DT, "); 
				stringBuilder.append("A.CURRENCY_CD, (A.CALC_AMT*(-1)), A.MERCH_TAX_REG_NBR, A.WP_TAX_REG_NBR, A.TAX_AUTHORITY, A.TAX_TYPE, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("(A.PREVIOUS_AMT*(-1)), C.CR_NOTE_FR_BILL_ID, SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM ");
				stringBuilder.append("(A.PREVIOUS_AMT*(-1)), C.CR_NOTE_FR_BILL_ID, SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INVOICE_DATA A, CM_INV_RECALC_STG B, CI_BILL C ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND A.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INVOICE_DATA- " + count);
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

			//**********************CM_INVOICE_DATA_LN********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INVOICE_DATA_LN (BILL_ID,BSEG_ID,BILLING_PARTY_ID,PRICE_CATEGORY, "); 
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("CURRENCY_CD,PRICE_CATEGORY_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
				stringBuilder.append("CURRENCY_CD,PRICE_CATEGORY_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.BILLING_PARTY_ID, D.PRICE_CATEGORY, "); 
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("D.CURRENCY_CD, D.PRICE_CATEGORY_DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM ");
				stringBuilder.append("D.CURRENCY_CD, D.PRICE_CATEGORY_DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INV_RECALC_STG B, CI_BILL C, CM_INVOICE_DATA_LN D ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND D.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BILL_ID=C.CR_NOTE_FR_BILL_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INVOICE_DATA_LN- " + count);
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

			//**********************CM_INV_DATA_LN_BCL********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_BCL (BILL_ID,BSEG_ID,BCL_TYPE,BCL_DESCR,CALC_AMT,TAX_STAT, "); 
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("TAX_STAT_DESCR,TAX_RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
				stringBuilder.append("TAX_STAT_DESCR,TAX_RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.BCL_TYPE, D.BCL_DESCR, (D.CALC_AMT*(-1)), D.TAX_STAT, "); 
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("D.TAX_STAT_DESCR, D.TAX_RATE, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM ");
				stringBuilder.append("D.TAX_STAT_DESCR, D.TAX_RATE, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INV_RECALC_STG B, CI_BILL C, CM_INV_DATA_LN_BCL D ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND D.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BILL_ID=C.CR_NOTE_FR_BILL_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INV_DATA_LN_BCL- " + count);
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

			//**********************CM_INV_DATA_LN_SVC_QTY********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY (BILL_ID,BSEG_ID,CURRENCY_CD,SQI_CD,SVC_QTY, "); 
//				stringBuilder.append("SQI_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY (BILL_ID,BSEG_ID,SQI_CD,SVC_QTY, "); 
				stringBuilder.append("SQI_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.CURRENCY_CD, D.SQI_CD, (D.SVC_QTY*(-1)), "); 
//				stringBuilder.append("D.SQI_DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM ");
				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.SQI_CD, (D.SVC_QTY*(-1)), "); 
				stringBuilder.append("D.SQI_DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INV_RECALC_STG B, CI_BILL C, CM_INV_DATA_LN_SVC_QTY D ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND D.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INV_DATA_LN_SVC_QTY- " + count);
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

			//**********************CM_INV_DATA_LN_RATE********************//			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_RATE (BILL_ID,BSEG_ID,RATE_TP,RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
//				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.RATE_TP, D.RATE, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM ");
				stringBuilder.append("INSERT INTO CM_INV_DATA_LN_RATE (BILL_ID,BSEG_ID,RATE_TP,RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				stringBuilder.append("SELECT C.BILL_ID, D.BSEG_ID, D.RATE_TP, D.RATE, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INV_RECALC_STG B, CI_BILL C, CM_INV_DATA_LN_RATE D ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND D.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INV_DATA_LN_RATE- " + count);
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

			//**********************CM_INV_DATA_ADJ********************//
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_ADJ (BILL_ID, ADJ_ID, ADJ_AMT, ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("ADJ_TYPE_CD,CURRENCY_CD,DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM) ");
				stringBuilder.append("ADJ_TYPE_CD,CURRENCY_CD,DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("SELECT C.BILL_ID, D.ADJ_ID, (ADJ_AMT*(-1)), ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
//				stringBuilder.append("D.ADJ_TYPE_CD, D.CURRENCY_CD, D.DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM ");
				stringBuilder.append("D.ADJ_TYPE_CD, D.CURRENCY_CD, D.DESCR, SYSTIMESTAMP, D.EXTRACT_FLG, D.EXTRACT_DTTM, :processDate AS ILM_DT,'N' AS ILM_ARCH_SW ");
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				stringBuilder.append("FROM CM_INV_RECALC_STG B, CI_BILL C, CM_INV_DATA_ADJ D ");
				stringBuilder.append("WHERE TRUNC(B.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND B.BO_STATUS_CD=:status1 ");
				stringBuilder.append("AND D.BILL_ID=B.BILL_ID ");
				stringBuilder.append("AND B.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - Start Change
				preparedStatement.bindDate("processDate", this.getProcessDateTime().getDate());
				//NAP-24192 : RIA: CM_TXNUP Interface - Add ILM columns in INSERT SQLs - End Change
				int count=preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_INV_DATA_ADJ- " + count);
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

		/**
		 * updateBillableChgStatus() method uploads original transactions back and delete original charges so that they are not billed again.
		 */
		private boolean updateBillableChgStatus() {
			int rowsUpdated = 0;
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();			

			//Populate Detail staging table with original transactions
			try {
				stringBuilder.append("INSERT INTO CI_TXN_DETAIL_STG (TXN_DETAIL_ID,TXN_HEADER_ID,TXN_SOURCE_CD, ");
				stringBuilder.append("TXN_REC_TYPE_CD,TXN_DTTM,EXT_TXN_NBR,CUST_REF_NBR,CIS_DIVISION,ACCT_ID,TXN_VOL,TXN_AMT,CURRENCY_CD,MANUAL_SW, ");
				stringBuilder.append("USER_ID,HOW_TO_USE_TXN_FLG,MESSAGE_CAT_NBR,MESSAGE_NBR,UDF_CHAR_1,UDF_CHAR_2,UDF_CHAR_3,UDF_CHAR_4,UDF_CHAR_5, ");
				stringBuilder.append("UDF_CHAR_6,UDF_CHAR_7,UDF_CHAR_8,UDF_CHAR_9,UDF_CHAR_10,UDF_CHAR_11,UDF_CHAR_12,UDF_CHAR_13,UDF_CHAR_14, ");
				stringBuilder.append("UDF_CHAR_15,UDF_NBR_1,UDF_NBR_2,UDF_NBR_3,UDF_NBR_4,UDF_NBR_5,UDF_NBR_6,UDF_NBR_7,UDF_NBR_8,UDF_NBR_9, ");
				stringBuilder.append("UDF_NBR_10,UDF_AMT_1,UDF_CURRENCY_CD_1,UDF_AMT_2,UDF_CURRENCY_CD_2,UDF_AMT_3,UDF_CURRENCY_CD_3,UDF_AMT_4, ");
				stringBuilder.append("UDF_CURRENCY_CD_4,UDF_AMT_5,UDF_CURRENCY_CD_5,UDF_DTTM_1,UDF_DTTM_2,UDF_DTTM_3,UDF_DTTM_4,UDF_DTTM_5, ");
				stringBuilder.append("BUS_OBJ_CD,BO_STATUS_CD,STATUS_UPD_DTTM,version,DO_NOT_AGG_SW,TXN_UPLOAD_DTTM,ACCT_NBR_TYPE_CD,ACCT_NBR, ");
				stringBuilder.append("UDF_CHAR_16,UDF_CHAR_17,UDF_CHAR_18,UDF_CHAR_19,UDF_CHAR_20,UDF_CHAR_21,UDF_CHAR_22,UDF_CHAR_23,UDF_CHAR_24, ");
				stringBuilder.append("UDF_CHAR_25,RULE_CD,MESSAGE_DESC,DISAGG_SW,DISAGG_CNT,PROCESSING_DT,LAST_SYS_PRCS_DT,UDF_CHAR_26,UDF_CHAR_27, ");
				stringBuilder.append("UDF_CHAR_28,UDF_CHAR_29,UDF_CHAR_30,UDF_CHAR_31,UDF_CHAR_32,UDF_CHAR_33,UDF_CHAR_34,UDF_CHAR_35,UDF_CHAR_36, ");
				stringBuilder.append("UDF_CHAR_37,UDF_CHAR_38,UDF_CHAR_39,UDF_CHAR_40,UDF_CHAR_41,UDF_CHAR_42,UDF_CHAR_43,UDF_CHAR_44,UDF_CHAR_45, ");
				stringBuilder.append("UDF_CHAR_46,UDF_CHAR_47,UDF_CHAR_48,UDF_CHAR_49,UDF_CHAR_50,UDF_NBR_11,UDF_NBR_12,UDF_NBR_13,UDF_NBR_14, ");
				stringBuilder.append("UDF_NBR_15,UDF_NBR_16,UDF_NBR_17,UDF_NBR_18,UDF_NBR_19,UDF_NBR_20,UDF_AMT_6,UDF_AMT_7,UDF_AMT_8,UDF_AMT_9, ");
				stringBuilder.append("UDF_AMT_10,UDF_CURRENCY_CD_6,UDF_CURRENCY_CD_7,UDF_CURRENCY_CD_8,UDF_CURRENCY_CD_9,UDF_CURRENCY_CD_10) ");
				stringBuilder.append("SELECT A.TXN_DETAIL_ID,A.TXN_HEADER_ID,A.TXN_SOURCE_CD, ");
				stringBuilder.append("A.TXN_REC_TYPE_CD,A.TXN_DTTM,A.EXT_TXN_NBR,A.CUST_REF_NBR,A.CIS_DIVISION,' ',A.TXN_VOL,A.TXN_AMT,A.CURRENCY_CD,A.MANUAL_SW, ");
				stringBuilder.append("A.USER_ID,A.HOW_TO_USE_TXN_FLG,A.MESSAGE_CAT_NBR,A.MESSAGE_NBR,A.UDF_CHAR_1,A.UDF_CHAR_2,A.UDF_CHAR_3,A.UDF_CHAR_4,A.UDF_CHAR_5, ");
				stringBuilder.append("A.UDF_CHAR_6,A.UDF_CHAR_7,A.UDF_CHAR_8,A.UDF_CHAR_9,A.UDF_CHAR_10,A.UDF_CHAR_11,A.UDF_CHAR_12,A.UDF_CHAR_13,A.UDF_CHAR_14, ");
				stringBuilder.append("A.UDF_CHAR_15,A.UDF_NBR_1,A.UDF_NBR_2,A.UDF_NBR_3,A.UDF_NBR_4,A.UDF_NBR_5,A.UDF_NBR_6,A.UDF_NBR_7,A.UDF_NBR_8,A.UDF_NBR_9, ");
				stringBuilder.append("A.UDF_NBR_10,A.UDF_AMT_1,A.UDF_CURRENCY_CD_1,A.UDF_AMT_2,A.UDF_CURRENCY_CD_2,A.UDF_AMT_3,A.UDF_CURRENCY_CD_3,A.UDF_AMT_4, ");
				stringBuilder.append("A.UDF_CURRENCY_CD_4,A.UDF_AMT_5,A.UDF_CURRENCY_CD_5,A.UDF_DTTM_1,A.UDF_DTTM_2,A.UDF_DTTM_3,A.UDF_DTTM_4,A.UDF_DTTM_5, ");
				stringBuilder.append("A.BUS_OBJ_CD,:boStatus,A.STATUS_UPD_DTTM,A.VERSION,A.DO_NOT_AGG_SW,A.TXN_UPLOAD_DTTM,A.ACCT_NBR_TYPE_CD,A.ACCT_NBR, ");
				stringBuilder.append("A.UDF_CHAR_16,A.UDF_CHAR_17,A.UDF_CHAR_18,A.UDF_CHAR_19,A.UDF_CHAR_20,A.UDF_CHAR_21,A.UDF_CHAR_22,A.UDF_CHAR_23,A.UDF_CHAR_24, ");
				stringBuilder.append("A.UDF_CHAR_25,' ',A.MESSAGE_DESC,A.DISAGG_SW,A.DISAGG_CNT,null,A.LAST_SYS_PRCS_DT,A.UDF_CHAR_26,A.UDF_CHAR_27, ");
				stringBuilder.append("A.UDF_CHAR_28,A.UDF_CHAR_29,A.UDF_CHAR_30,A.UDF_CHAR_31,A.UDF_CHAR_32,A.UDF_CHAR_33,A.UDF_CHAR_34,A.UDF_CHAR_35,A.UDF_CHAR_36, ");
				stringBuilder.append("A.UDF_CHAR_37,A.UDF_CHAR_38,A.UDF_CHAR_39,A.UDF_CHAR_40,A.UDF_CHAR_41,A.UDF_CHAR_42,A.UDF_CHAR_43,A.UDF_CHAR_44,A.UDF_CHAR_45, ");
				stringBuilder.append("A.UDF_CHAR_46,A.UDF_CHAR_47,A.UDF_CHAR_48,A.UDF_CHAR_49,A.UDF_CHAR_50,A.UDF_NBR_11,A.UDF_NBR_12,A.UDF_NBR_13,A.UDF_NBR_14, ");
				stringBuilder.append("A.UDF_NBR_15,A.UDF_NBR_16,A.UDF_NBR_17,A.UDF_NBR_18,A.UDF_NBR_19,A.UDF_NBR_20,A.UDF_AMT_6,A.UDF_AMT_7,A.UDF_AMT_8,A.UDF_AMT_9, ");
				stringBuilder.append("A.UDF_AMT_10,A.UDF_CURRENCY_CD_6,A.UDF_CURRENCY_CD_7,A.UDF_CURRENCY_CD_8,A.UDF_CURRENCY_CD_9,A.UDF_CURRENCY_CD_10 ");
				stringBuilder.append("FROM CI_TXN_DETAIL A WHERE A.TXN_DETAIL_ID IN (SELECT B.TXN_DETAIL_ID FROM CI_TXN_DTL_PRITM B ");
				stringBuilder.append("WHERE B.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");	
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				preparedStatement.bindString("boStatus",customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
				rowsUpdated = preparedStatement.executeUpdate();
				logger.info("Detail Staging table populated with original transactions"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();				

			//Delete existing charges and transaction data for the corresponding bill id				
			try {
				stringBuilder.append("DELETE FROM CI_TXN_DETAIL A WHERE A.TXN_DETAIL_ID ");
				stringBuilder.append("IN (SELECT B.TXN_DETAIL_ID FROM CI_TXN_DTL_PRITM B ");
				stringBuilder.append("WHERE B.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");		
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_DETAIL table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("delete from CI_BILL_CHG C where C.BILLABLE_CHG_ID ");
				stringBuilder.append("IN (select A.BILLABLE_CHG_ID FROM CI_TXN_DTL_PRITM A ");
				stringBuilder.append("WHERE A.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("where D.BSEG_ID in (select E.BSEG_ID from CI_BSEG E ");
				stringBuilder.append("where E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");		
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_BILL_CHG table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("DELETE FROM CI_TXN_CALC_LN_CHAR B WHERE ");
				stringBuilder.append("B.TXN_CALC_ID IN (SELECT C.TXN_CALC_ID FROM CI_TXN_DTL_PRITM C ");
				stringBuilder.append("WHERE C.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");	
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_CALC_LN_CHAR table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("DELETE FROM CI_TXN_CALC_LN B WHERE ");
				stringBuilder.append("B.TXN_CALC_ID IN (SELECT C.TXN_CALC_ID FROM CI_TXN_DTL_PRITM C ");
				stringBuilder.append("WHERE C.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");		
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_CALC_LN table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("DELETE FROM CI_TXN_SQ B WHERE ");
				stringBuilder.append("B.TXN_CALC_ID IN (SELECT C.TXN_CALC_ID FROM CI_TXN_DTL_PRITM C ");
				stringBuilder.append("WHERE C.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");	
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_SQ table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("DELETE FROM CI_TXN_CALC C ");
				stringBuilder.append("WHERE C.TXN_CALC_ID IN (SELECT A.TXN_CALC_ID FROM CI_TXN_DTL_PRITM A ");
				stringBuilder.append("WHERE A.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");		
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_CALC table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("DELETE FROM CI_TXN_DTL_PRITM A WHERE A.TXN_DETAIL_ID ");
				stringBuilder.append("IN (SELECT B.TXN_DETAIL_ID FROM CI_TXN_DTL_PRITM B ");
				stringBuilder.append("WHERE B.BILLABLE_CHG_ID IN (SELECT D.BILLABLE_CHG_ID FROM CI_BSEG_CALC D ");
				stringBuilder.append("WHERE D.BSEG_ID IN (SELECT E.BSEG_ID FROM CI_BSEG E WHERE E.BILL_ID=(SELECT G.BILL_ID FROM CM_INV_RECALC_STG G ");
				stringBuilder.append("WHERE TRUNC(G.STATUS_UPD_DTTM) = TRUNC(SYSDATE) ");
				stringBuilder.append("AND G.BO_STATUS_CD=:status1)))) ");	
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", customCreditNoteInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");	
				rowsUpdated=preparedStatement.executeUpdate();
				logger.info("Data from CI_TXN_DTL_PRITM table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of updateBillableChgStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
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
		} // end method
	}// end worker	
}