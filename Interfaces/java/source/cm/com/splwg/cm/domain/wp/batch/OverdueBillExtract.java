/*******************************************************************************
 * FileName                   : OverdueBillExtract.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 9, 2016
 * Version Number             : 1.0
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             02-Apr-2016         Sunaina       Implemented all requirement as per Debt Design.
 0.2      NA             09-May-2016         Sunaina       Updated as per change in FD NAP-5214, NAP-5220.
 0.3      NA             28-Jul-2016         Sunaina       Updated as per change in PAM-7368.
 0.4      NA             01-Jan-2017         Preeti        Updated to fix unique constraint error/PAM-9814.
 0.5      NA             14-Jun-2017         Ankur         Updated to fix performance issue as per PAM-12861
 0.6 	  NA             28-Jun-2017	     Ankur		   NAP-17306 fix
 0.7 	  NA             26-Dec-2017	     Ankur		   PAM-16422,PAM-16600 & PAM-15217
 0.8 	  NA             16-Jan-2017	     Ankur		   NAP-25171 Fix to run overdue for past dated due_Dt
 0.9 	  NA             09-Apr-2018	     Ankur		   NAP-25484 remove CM_BILL_CYC_SCH        
 1.0	  NA			 11-Sep-2018   		 Vikalp        NAP-30848, NAP-31784 removed temp6 table and join of CI_FT. rerunnable for same business date
 1.1      NA             01-Jan-2019         Vikalp        NAP-36988, Changes to include LINE_ID
 1.2	  NA			 13-Aug-2019		 Somya		   NAP-46657, Changes to insert only one record in CM_INVOICE_BAL table for 1 bill ID & line ID
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
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;


/**
 * @author rainas403
 *
 * @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = processingDate, type = string)
 *            , @BatchJobSoftParameter (name = chunkSize, required = true, type = integer)})
 */
public class OverdueBillExtract extends OverdueBillExtract_Gen {
	public static final Logger logger = LoggerFactory
			.getLogger(OverdueBillExtract.class);
	public static final String OVERDUE_TEMP = "CM_OVERDUE_TEMP";

	public OverdueBillExtract() {
		// OverdueBillExtract
	}

	/**
	 * getJobWork() method selects data for processing by OverdueBillExtract Interface.
	 * The source of data is selected from ORMB base and custom table and then passed to
	 * the executeWorkUnit for further processing by framework.
	 */
	@Override
	public JobWork getJobWork() {

		logger.debug("Inside getJobWork() method");
		return createJobWorkForThreadWorkUnitList(getOverdueBillData());
	}



//	*********************** getOverdueBillData Method******************************

	/**
	 * getOverdueBillData() method retrieves all the elements from CM_BILL_DUE_DT  table.
	 *
	 * @return List ThreadWorkUnit
	 */

	private List<ThreadWorkUnit> getOverdueBillData() {
		logger.debug("Inside getOverdueBillData() method");
		PreparedStatement preparedStatement = null;
		OverDueBills_Id overDueBillId = null;
		BigInteger overDueLowId;
		BigInteger overDueHighId;
		int chunkSize = getParameters().getChunkSize().intValue();
		String processingDate=getParameters().getProcessingDate();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		StringBuilder stringBuilder = new StringBuilder();
		Date invBalMaxUploadDt=null;

		resetInvBalSQ();
		/**
		 This will retrieve distinct BILL_IDs from CM_BILL_DUE_DT table where due date is one day less than given processing date and  Bill is not closed.
		 We are selecting lower and higher over due bill id from distinct bill_ids on the basis of given chunksize.
		 These lower and higher over due bill ids will be part of single thread work unit.We are using chunk size so that we can fix maximum no of records to be processed by single query in one go.
		 */

		invBalMaxUploadDt = findMaxDate();

		truncateFromOverdueTmpTable(OVERDUE_TEMP);
		insertTempBill(processingDate,invBalMaxUploadDt);

		stringBuilder=null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append(" WITH TBL AS (SELECT TEMP.ROW_ID FROM CM_OVERDUE_TEMP TEMP ORDER BY 1) ");
			stringBuilder.append(" SELECT THREAD_NUM, MIN(ROW_ID) AS OVER_DUE_LOW_ID, ");
			stringBuilder.append(" MAX(ROW_ID) AS OVER_DUE_HIGH_ID FROM (SELECT ROW_ID, ");
			stringBuilder.append(" CEIL((ROWNUM)/:CHUNKSIZE) AS THREAD_NUM FROM TBL) ");
			stringBuilder.append(" GROUP BY THREAD_NUM ORDER BY 1 ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindBigInteger("CHUNKSIZE", new BigInteger(String.valueOf(chunkSize)));
			preparedStatement.setAutoclose(false);

			for (SQLResultRow sqlRow : preparedStatement.list()) {

				overDueLowId  = sqlRow.getInteger("OVER_DUE_LOW_ID");
				overDueHighId = sqlRow.getInteger("OVER_DUE_HIGH_ID");
				overDueBillId = new OverDueBills_Id(overDueLowId, overDueHighId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(overDueBillId);
				threadworkUnit.addSupplementalData("maxDate",invBalMaxUploadDt);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				overDueBillId = null;
			}

		}
		catch (Exception e) {
			logger.error("Inside getOverdueBillData() method of OverDueBillExtract, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		}
		finally{
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		logger.info("Number of records selected for processing "+threadWorkUnitList.size());
		return threadWorkUnitList;
	}

	private void truncateFromOverdueTmpTable(String inputTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("TRUNCATE TABLE "+ inputTable,"");
			preparedStatement.execute();
			logger.info("Temp tables " +inputTable +"deleted");
		}
		catch (ThreadAbortedException e) {
			logger.error("Inside truncateFromOverdueTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside truncateFromOverdueTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	private void insertTempBill(String processingDate,
			Date invBalMaxUploadDt) {

		StringBuilder stringBuilder = new StringBuilder();
		PreparedStatement preparedStatement = null;

		try {
			stringBuilder.append("INSERT INTO CM_OVERDUE_TEMP ");
			stringBuilder.append("SELECT BILL_ID ,ROWNUM FROM ");
			stringBuilder.append("(SELECT DISTINCT DUE.BILL_ID FROM CM_BILL_DUE_DT DUE ");
			if(isNull(processingDate))
			{
				stringBuilder.append(" WHERE (TRUNC(DUE.DUE_DT )= TRUNC(SYSDATE-1) ");
				stringBuilder.append(" OR (TRUNC(DUE.UPLOAD_DTTM) >= :invBalUploadDt ");
				stringBuilder.append( " AND TRUNC(DUE.DUE_DT) < TRUNC(SYSDATE-1))) ");
			}
			else
			{
				stringBuilder.append(" WHERE (TRUNC(DUE.DUE_DT)=TRUNC(TO_DATE('"+processingDate+"','YYYY-MM-DD')-1) ");
				stringBuilder.append(" OR (TRUNC(DUE.UPLOAD_DTTM) >= :invBalUploadDt ");
				stringBuilder.append(" AND TRUNC(DUE.DUE_DT) < TRUNC(TO_DATE('"+processingDate+"','YYYY-MM-DD')-1))) ");
			}
			stringBuilder.append(" AND DUE.IS_MERCH_BALANCED='N' AND NOT EXISTS (SELECT 1 FROM CM_INVOICE_BAL ");
			//NAP-46657
			stringBuilder.append(" WHERE DUE.BILL_ID = FINANCIAL_DOC_ID AND DUE.LINE_ID = LINE_ID)) ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.setAutoclose(false);
			preparedStatement.bindDate("invBalUploadDt", invBalMaxUploadDt );
			preparedStatement.executeUpdate();

		}

		catch (Exception e) {
			logger.error("Inside insertTempBill() method of OverDueBillExtract, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		}

		finally{
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	private Date findMaxDate() {
		StringBuilder stringBuilder = new StringBuilder();
		PreparedStatement preparedStatement = null;
		Date maxDate = getSystemDateTime().getDate().addDays(-1);

		try {
			stringBuilder.append("SELECT MAX(UPLOAD_DTTM) as UPLD_DT FROM CM_INVOICE_BAL");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.setAutoclose(false);
			SQLResultRow invBalRow = preparedStatement.firstRow();
			if(invBalRow!=null){
				maxDate=invBalRow.getDate("UPLD_DT");
			}
		}


		catch (Exception e) {
			logger.error("Inside findMaxDate() method of OverDueBillExtract, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		}

		finally{
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return maxDate;
	}

	@SuppressWarnings("deprecation")
	private void resetInvBalSQ() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_INVOICE_BAL_ID}");
			preparedStatement.execute();

		} catch (RuntimeException e) {
			logger.error("Inside resetInvRelStgIdSQ() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}


	public Class<OverdueBillExtractWorker> getThreadWorkerClass() {
		return OverdueBillExtractWorker.class;
	}

	public static class OverdueBillExtractWorker extends
			OverdueBillExtractWorker_Gen {

		public OverdueBillExtractWorker() {
			// OverdueBillExtractWorker
		}


		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		@Override
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork() method");
			super.initializeThreadWork(arg0);
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
		 * every row of processing.
		 */
		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			logger.info("Inside executeWorkUnit() for thread number - " + getBatchThreadNumber());
			BigInteger overDueLowId;
			BigInteger overDueHighId;
			PreparedStatement preparedStatement = null;
			OverDueBills_Id overdueBillId = (OverDueBills_Id) unit.getPrimaryId();
			Date invBalanceMaxDate= (Date) unit.getSupplementallData("maxDate");
			overDueLowId = overdueBillId.getOverDueLowId();
			overDueHighId = overdueBillId.getOverDueHighId();
			String processingDate=getParameters().getProcessingDate();

			logger.info("Row Id 1 = " + overDueLowId);
			logger.info("Row Id 2 = " + overDueHighId);
			StringBuilder stringBuilder = new StringBuilder();


			/**
			 Inserting data into global temp table 1 from CM_BILL_DUE_DT table having latest record for given banking entry event id on the basis of given range
			 and same condition which per applied in getJobWork method.
			 As Global temp tables locks data specific to session.Therefore using Temp1 table to store range based data which will help in further processing.
			 */
			try {
				stringBuilder.append(" INSERT INTO CM_OVERDUE_BILL_DTL ");
				stringBuilder.append(" (BILL_ID,LINE_ID,BANK_ENTRY_EVENT_ID,DUE_DT,ALT_BILL_ID,BILL_DT,WIN_START_DT,ACCT_NBR,IS_MERCH_BALANCED)  ");
				stringBuilder.append(" SELECT A.BILL_ID,");
				stringBuilder.append(" A.LINE_ID,");
				stringBuilder.append(" MAX(A.BANK_ENTRY_EVENT_ID) AS BANK_ENTRY_EVENT_ID,");
				stringBuilder.append(" MIN(A.DUE_DT) AS DUE_DT,");
				stringBuilder.append(" B.ALT_BILL_ID,");
				stringBuilder.append(" B.BILL_DT,");
				stringBuilder.append(" B.WIN_START_DT,");
				stringBuilder.append(" B.ACCT_TYPE,");
				stringBuilder.append(" A.IS_MERCH_BALANCED");
				stringBuilder.append(" FROM CM_BILL_DUE_DT A,CM_OVERDUE_TEMP TEMP,");
				stringBuilder.append(" CM_INVOICE_DATA B ");
				stringBuilder.append(" WHERE 1=1");
				if(isNull(processingDate))
				{
					stringBuilder.append(" AND (TRUNC(A.DUE_DT )=TRUNC(SYSDATE-1) ");
					stringBuilder.append(" OR (TRUNC(A.UPLOAD_DTTM) >= :invBalUploadDt ");
					stringBuilder.append(" AND TRUNC(A.DUE_DT) < TRUNC(SYSDATE-1))) ");
				}
				else
				{
					stringBuilder.append(" AND (TRUNC(A.DUE_DT)=TRUNC(TO_DATE('"+processingDate+"','YYYY-MM-DD')-1) ");
					stringBuilder.append(" OR (TRUNC(A.UPLOAD_DTTM) >= :invBalUploadDt ");
					stringBuilder.append(" AND TRUNC(A.DUE_DT) < TRUNC(TO_DATE('"+processingDate+"','YYYY-MM-DD')-1))) ");
				}
				stringBuilder.append(" AND A.IS_MERCH_BALANCED='N' ");
				stringBuilder.append(" AND NOT EXISTS ( ");
				stringBuilder.append(" SELECT 1 FROM CM_INVOICE_BAL E ");
				stringBuilder.append(" WHERE A.BILL_ID=E.FINANCIAL_DOC_ID ");
				stringBuilder.append(" AND A.LINE_ID = E.LINE_ID) ");
				stringBuilder.append(" AND A.BILL_ID=B.BILL_ID ");
				stringBuilder.append(" AND A.BILL_ID=TEMP.BILL_ID ");
				stringBuilder.append(" AND TEMP.ROW_ID BETWEEN :overDueLowId AND :overDueHighId ");
				stringBuilder.append(" GROUP BY A.BILL_ID,");
				stringBuilder.append(" A.LINE_ID,");
				stringBuilder.append(" B.ALT_BILL_ID,");
				stringBuilder.append(" B.BILL_DT,");
				stringBuilder.append(" B.WIN_START_DT,");
				stringBuilder.append(" B.ACCT_TYPE,");
				stringBuilder.append(" A.IS_MERCH_BALANCED");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("overDueLowId", overDueLowId);
				preparedStatement.bindBigInteger("overDueHighId", overDueHighId);
				preparedStatement.bindDate("invBalUploadDt",invBalanceMaxDate);
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch(Exception e) {
				logger.error("Exception in executeWorkUnit() : While inserting into CM_OVERDUE_BILL_DTL - ",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			/**
			 Selecting required data from CI_BILL which exists in Temp 1 table and inserting it into global temp table 2.
			 Using Case statement to take care of adhoc bills where bill_cyc_cd and win_start_dt doesn't exist.This will avoid an update statement.
			 */
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_OVERDUE_PAY_DTL ");
				stringBuilder.append(" (BANK_ENTRY_EVENT_ID,BILL_ID,BILL_DT,LINE_AMT,UNPAID_AMT,CURRENCY_CD, ");
				stringBuilder.append(" IS_MERCH_BALANCED,DUE_DT,ALT_BILL_ID,WIN_START_DT,ACCT_NBR,LINE_ID,OVERPAID) ");
				stringBuilder.append(" SELECT B.BANK_ENTRY_EVENT_ID,B.BILL_ID,B.BILL_DT,A.LINE_AMT,A.UNPAID_AMT,A.CURRENCY_CD, ");
				stringBuilder.append(" B.IS_MERCH_BALANCED,B.DUE_DT,B.ALT_BILL_ID,B.WIN_START_DT,B.ACCT_NBR,B.LINE_ID,A.OVERPAID ");
				stringBuilder.append(" FROM CM_BILL_PAYMENT_DTL A, ");
				stringBuilder.append(" CM_OVERDUE_BILL_DTL B ");
				stringBuilder.append(" WHERE B.BILL_ID=A.BILL_ID ");
				stringBuilder.append(" AND A.LINE_ID=B.LINE_ID ");
				stringBuilder.append(" AND A.PAY_DTL_ID= ( ");
				stringBuilder.append(" SELECT MAX(PAY_DTL_ID) ");
				stringBuilder.append(" FROM CM_BILL_PAYMENT_DTL ");
				stringBuilder.append(" WHERE BILL_ID=B.BILL_ID ");
				stringBuilder.append(" AND LINE_ID=B.LINE_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch(Exception e) {
				logger.error("Exception in executeWorkUnit() : While inserting into CM_OVERDUE_PAY_DTL - ",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			/**
			 Retrieve all the data resulting after joining TEMP1 and TEMP6 table and inserting it into target table CM_INVOICE_BAL.
			 We are not using range value here because Temp1 & Temp6 both are global temp tables and hence will have session specific data.
			 */
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" INSERT INTO CM_INVOICE_BAL ");
				stringBuilder.append(" (BANK_ENTRY_EVENT_ID, FINANCIAL_DOC_ID, LINE_ID, BILL_DT,INVOICEAMT, UNPAIDINVBAL, ");
				stringBuilder.append(" INVCURR, IS_MERCH_BALANCED, DUE_DT,  ALT_BILL_ID, WIN_START_DT, WIN_END_DT, ");
				stringBuilder.append(" UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ACCT_TYPE, INVOICE_BAL_ID ) ");
				stringBuilder.append(" SELECT A.BANK_ENTRY_EVENT_ID, A.BILL_ID AS FINANCIAL_DOC_ID, A.LINE_ID, ");
				stringBuilder.append(" A.BILL_DT, A.LINE_AMT AS INVOICEAMT, A.UNPAID_AMT AS UNPAIDINVBAL, ");
				stringBuilder.append(" A.CURRENCY_CD AS INVCURR, 'N' AS IS_MERCH_BALANCED, ");
				stringBuilder.append(" A.DUE_DT, A.ALT_BILL_ID, A.WIN_START_DT, Null, ");
				stringBuilder.append(" SYSTIMESTAMP, 'Y' AS EXTRACT_FLG, '' AS EXTRACT_DTTM,A.ACCT_NBR, invoice_bal_seq.nextval  ");
				stringBuilder.append(" FROM CM_OVERDUE_PAY_DTL A WHERE A.UNPAID_AMT <> 0 ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch(Exception e) {
				logger.error("Exception in executeWorkUnit() : While inserting into CM_INVOICE_BAL - ",e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			/**
			 Using commit in order to delete all the session specific data from global temp tables.This is to avoid unique constraint error.
			 */
			try {
				preparedStatement = createPreparedStatement("commit","");
				preparedStatement.execute();
			} catch (RuntimeException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}


			return true;
		} // end of execute work unit

		public static String getErrorDescription(String messageNumber) {
			String errorInfo = "";
			errorInfo = CustomMessageRepository.billCycleError(messageNumber).getMessageText();

			if (errorInfo.contains("Text:") && errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException,RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}

	} // end of worker class

	public static final class OverDueBills_Id implements Id {

		private static final long serialVersionUID = 1L;

		private BigInteger overDueLowId;

		private BigInteger overDueHighId;


		public OverDueBills_Id(BigInteger overDueLowId, BigInteger overDueHighId) {
			setOverDueLowId(overDueLowId);
			setOverDueHighId(overDueHighId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
			// appendContents
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public BigInteger getOverDueLowId() {
			return overDueLowId;
		}

		public void setOverDueLowId(BigInteger overDueLowId) {
			this.overDueLowId = overDueLowId;
		}

		public BigInteger getOverDueHighId() {
			return overDueHighId;
		}

		public void setOverDueHighId(BigInteger overDueHighId) {
			this.overDueHighId = overDueHighId;
		}



	}// end of Id Class

}
