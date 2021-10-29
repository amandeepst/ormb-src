/*******************************************************************************
 * FileName                   : PaymentsRequestInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015 
 * Version Number             : 3.8
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 24, 2015        Preeti Tiwari        Implemented all requirements for CD1. 
0.2      NA             Apr 24, 2015        Abhishek Paliwal     Txn source parameter introduced
0.3      NA             Jun 11, 2015        Preeti Tiwari        Fix for Defect PAM-2012. 
0.4      NA             Jul 06, 2015        Preeti Tiwari        Implemented all requirements for CD2 (Payment granularity).
0.5      NA             Jul 22, 2015        Preeti Tiwari        Implemented logic to append data into out bound tables instead of truncating them.
0.6      NA             Aug 19, 2015        Preeti Tiwari        Updated to handle over payment scenarios for Payment Confirmation.
0.7		 NA				Sep 30, 2015		Preeti Tiwari        Updated code as per LDM changes.
0.8		 NA				Feb 29, 2016		Sunaina Raina		 Updated for performance.
0.9		 NA				Apr 22, 2016		Preeti Tiwari		 Code review changes.
1.0		 NA				Apr 26, 2016		Sunaina Raina		 Updated for defect fixes PAM-5536, PAM-5662.
1.1      NA             Jun 11, 2016        Preeti Tiwari        Hopper 9 CIT fix.
1.2      NA             Jul 08, 2016        Preeti Tiwari        SIT fix for duplicate payments.
1.3      NA             Sep 14, 2016        Preeti Tiwari        Non Zero Balance Migration-Suppress bills/Production issue fix for adjustments.
1.4      NA             Sep 26, 2016        Preeti Tiwari        Production issue fix for adjustments.
1.5      NA             Nov 09, 2016        Preeti Tiwari        Insert into bill due date table for over payments.
1.6      NA             Jan 09, 2017        Preeti Tiwari        Removed credit note logic.
1.7      NA             Feb 23, 2017        Ankur Jain           Implemented amendment of group by in case ind_flag as 'Y' 
1.8      NA             Mar 02, 2017        Ankur Jain           Implemented global temp table changes(removed commit from  in between of executeWorkUnit )
1.9 	 NA             Mar 10, 2017        Ankur Jain           GTT bug fix:added one more table CM_PAY_BILL_MAP to copy data from CM_PAY_REQ_TMP
2.0      NA             Apr 13, 2017        Ankur Jain           PAM-12101: Implemented performance review points.
2.1      NA             May 09, 2017        Preeti Tiwari        PAM-12572- Only case identifier and payment narrative to be defaulted if Individual flag is N.
2.2      NA             May 22, 2017        Ankur Jain		     PAM-12834 Gather statics implementation
2.3      NA             May 25, 2017        Ankur Jain           Fixed PAM-12281
2.4      NA             Jun 20, 2017        Ankur Jain           NAP-16883 fix
2.5      NA             Jul 31, 2017        Preeti Tiwari        NAP-16981 Implementation: Granularity extraction as per new delimiters
2.6      NA             Aug 10, 2017        Preeti Tiwari        PAM-13698 Charge back case identifier
2.7      NA             Sep 14, 2017        Ankur Jain           PAM-13709 Fix
2.8      NA             Sep 21, 2017        Preeti Tiwari        Aggregate charging payment requests into single line
2.9 	 NA             Dec 26, 2017	    Ankur Jain		     PAM-16600 Fix
3.0 	 NA             Jan 30, 2018	    Preeti Tiwari        NAP-21723/NAP-22185: Utilize TXN/FT/BCHG Mappings.
3.1 	 NA             Apr 18, 2018	    Ankur Jain           NAP-25782 Bill amount issue fix & PAM-17628 Overpayment issue fix

3.2		 NA				Apr 20, 2018		Kaustubh K			 ILM changes and bill characteristics for BILL_ID 
3.3	 	 NA			 	Jun 07, 2018		RIA		  			 Prepared Statement close 
3.4	 	 NA			 	Jul 09, 2018		RIA		  			 NAP-27444 Aggregation of BILL_AMT by IS_IND_FLG
3.5      NA             Jul 13, 2018        Vikalp               NAP-29992 Rerunnable batch with billDate soft parameter
3.6      NA             Aug 27, 2018        RIA                  NAP-30741 Accounting changs to account for negative funding separately.
3.7      NA             Sep 12, 2018        RIA                  NAP-33280.
3.8      NA             Sep 19, 2018        Somya                NAP-33272 Insert REQUEST & OVERPAID entries for bills having overpay adjusment.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (modules = { "demo"}, softParameters = { @BatchJobSoftParameter (name = billDate, type = date) 
 * 			, @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)
 *          , @BatchJobSoftParameter (name = txnSourceCode, type = string)})
 */

public class PaymentsRequestInterface extends PaymentsRequestInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(PaymentsRequestInterface.class);

	private static final PaymentsRequestInterfaceLookUp paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp();

	public static final String SYSTEM_DATE = "sysdt";
	public static final String ILM_DATE = "ILM_DT";


	@Override
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork()");
		// To truncate error tables and temporary tables

		deleteFromPayReqTmpTable(paymentsRequestInterfaceLookUp.getCmPayReqErr());
		resetPayDtlId();
		logger.debug("Rows deleted from temporary and error tables");

		List<ThreadWorkUnit> threadWorkUnitList = getPayData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.debug("No of rows selected for processing are - " + rowsForProcessing);

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * deleteFromPayReqTmpTable() method will delete from the table provided as
	 * input.
	 * 
	 * @param inputPayReqTmpTable
	 */
	private void deleteFromPayReqTmpTable(String inputPayReqTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("DELETE FROM " + inputPayReqTmpTable, "");
			preparedStatement.execute();

		} catch (ThreadAbortedException e) {
			logger.error("Inside getPayData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getPayData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	@SuppressWarnings("deprecation")
	private void resetPayDtlId() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_PAY_DTL_ID }");
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside resetSourceKeySQ() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	// *********************** getPayData Method******************************

	/**
	 * getPayData() method selects Bill IDs for processing by this Interface.
	 * 
	 * @return List Per_Id_Nbr_Id
	 */
	private List<ThreadWorkUnit> getPayData() {
		logger.debug("Inside getPayData() method");

		PreparedStatement preparedStatement = null;
		DateTime maxCreateDttm = null;
		DateTime ilmDateTime = getSystemDateTime();
		int chunkSize = getParameters().getCustomThreadCount().intValue();
		Date billDate = getParameters().getBillDate();
		int threadCount = getParameters().getThreadCount().intValue();
		String lowPayBillId = "";
		String highPayBillId = "";
		PayBillIdData payBillIdData = null;
		StringBuilder stringBuilder = new StringBuilder();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();

		// fetching maxCreateDttm
		maxCreateDttm = getMaxCreateDttm();

		// fetching chunSize
		chunkSize = getChunkSize(billDate, chunkSize, threadCount, maxCreateDttm);

		try {
			preparedStatement = null;
			stringBuilder.append(" WITH TBL AS (SELECT BILL.BILL_ID");
			stringBuilder.append(" FROM CI_BILL BILL, CI_ACCT ACC, CI_ACCT_NBR ACCN");
			stringBuilder.append(" WHERE ACC.ACCT_ID = BILL.ACCT_ID");
			stringBuilder.append(" AND ACCN.ACCT_ID = ACC.ACCT_ID");
			stringBuilder.append(" AND ACCN.ACCT_NBR_TYPE_CD = :acctNbrTypeCd");
			stringBuilder.append(" AND BILL.BILL_STAT_FLG = 'C' ");
			stringBuilder.append(" AND trim(BILL.CR_NOTE_FR_BILL_ID) IS NULL");
			if (billDate == null) {
				stringBuilder.append(" AND BILL.COMPLETE_DTTM > :maxCreateDttm");
			} else {
				stringBuilder.append(" AND BILL.BILL_DT = :billDate");
			}
			stringBuilder.append(" AND NOT EXISTS (SELECT 1 FROM  CM_PAY_REQ WHERE");
			stringBuilder.append(" BILL.BILL_ID = BILL_ID) ORDER BY BILL_ID) ");
			stringBuilder.append(" SELECT THREAD_NUM, MIN(BILL_ID) AS LOW_BILL_ID,");
			stringBuilder.append(" MAX(BILL_ID) AS HIGH_BILL_ID FROM (SELECT BILL_ID,");
			stringBuilder.append(" CEIL((ROWNUM)/:CHUNKSIZE) AS THREAD_NUM FROM TBL)");
			stringBuilder.append(" GROUP BY THREAD_NUM ORDER BY 1");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.bindString("acctNbrTypeCd", paymentsRequestInterfaceLookUp.getAcctNbrTypeCd().trim(),
					"ACCT_TYPE");
			if (billDate == null) {
				preparedStatement.bindDateTime("maxCreateDttm", maxCreateDttm);
			} else {
				preparedStatement.bindDate("billDate", billDate);
			}
			preparedStatement.bindBigInteger("CHUNKSIZE", new BigInteger(String.valueOf(chunkSize)));
			preparedStatement.setAutoclose(false);

			for (SQLResultRow sqlRow : preparedStatement.list()) {
				lowPayBillId = sqlRow.getString("LOW_BILL_ID");
				highPayBillId = sqlRow.getString("HIGH_BILL_ID");
				payBillIdData = new PayBillIdData(lowPayBillId, highPayBillId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(payBillIdData);
				threadworkUnit.addSupplementalData("billDate", billDate);
				threadworkUnit.addSupplementalData("maxCreateDttm", maxCreateDttm);
				threadworkUnit.addSupplementalData("ilmDateTime", ilmDateTime);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				payBillIdData = null;
			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getPayData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getPayData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return threadWorkUnitList;
	}

	public DateTime getMaxCreateDttm() {
		PreparedStatement preparedStatement = null;
		DateTime maxCreateDttm = null;
		try {
			preparedStatement = createPreparedStatement(
					"SELECT MAX(CM.CREATE_DTTM) AS CREATE_DTTM FROM CM_PAY_REQ CM WHERE CM.CR_NOTE_FR_BILL_ID= ' '",
					"");
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> rows = preparedStatement.list();
			if (!rows.isEmpty()) {
				maxCreateDttm = rows.get(0).getDateTime("CREATE_DTTM") == null
						? new DateTime(new Date(1950, 1, 1), new Time(0, 0, 0))
						: rows.get(0).getDateTime("CREATE_DTTM");
			}
			logger.debug("The maximum Created date time is = " + maxCreateDttm);

		} catch (RuntimeException e) {
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Calling gather stats, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return maxCreateDttm;

	}

	public int getChunkSize(Date billDate, int chunkSize, int threadCount, DateTime maxCreateDttm) {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		BigInteger maxRecordcount = BigInteger.valueOf(0);
		int countPerThread;
		try {
			stringBuilder.append("SELECT COUNT(BILL.BILL_ID) AS COUNT ");
			stringBuilder.append(" FROM CI_BILL BILL");
			stringBuilder.append(" WHERE ");
			stringBuilder.append(" BILL.BILL_STAT_FLG = 'C' ");
			stringBuilder.append(" AND trim(BILL.CR_NOTE_FR_BILL_ID) IS NULL");
			if (billDate == null) {
				stringBuilder.append(" AND BILL.COMPLETE_DTTM >:maxCreateDttm ");
			} else {
				stringBuilder.append(" AND BILL.BILL_DT =:billDate ");
			}
			stringBuilder.append(" AND NOT EXISTS (SELECT 1 FROM  CM_PAY_REQ");
			stringBuilder.append(" WHERE BILL.BILL_ID = BILL_ID)");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			if (billDate == null)
				preparedStatement.bindDateTime("maxCreateDttm", maxCreateDttm);
			else
				preparedStatement.bindDate("billDate", billDate);
			preparedStatement.setAutoclose(false);
			SQLResultRow sqlRow = preparedStatement.firstRow();
			maxRecordcount = sqlRow.getInteger("COUNT");
			countPerThread = (int) Math.ceil(maxRecordcount.doubleValue() / threadCount);
			if (countPerThread > chunkSize) {
				chunkSize = countPerThread;
			}

		} catch (RuntimeException e) {
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Calling gather count, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return chunkSize;
	}

	public Class<PaymentsRequestInterfaceWorker> getThreadWorkerClass() {
		return PaymentsRequestInterfaceWorker.class;
	}

	public static class PaymentsRequestInterfaceWorker extends PaymentsRequestInterfaceWorker_Gen {

		String errMsg;
		public final String lineId="1";
		public final String  creditPayType ="CR";
		public final String mpgType = "MPG";
		public PaymentsRequestInterfaceWorker() {
			// constructore
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by the
		 * framework per thread.
		 */
		@Override
		public void initializeThreadWork(boolean arg0) throws ThreadAbortedException, RunAbortedException {
			errMsg = null;
			logger.debug("Inside initializeThreadWork() method for batch thread number: " + getBatchThreadNumber());
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
		 * extracting payments request details from ORMB. It validates the extracted
		 * data and populates the target tables accordingly.
		 */
		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() for thread number - " + getBatchThreadNumber());
			logger.debug("group by amendment inside executeWorkUnit");// have to remove
			String lowPayBillId = "";
			String highPayBillId = "";
			InboundPaymentsLookUp payConfirmationLookup = new InboundPaymentsLookUp();
			DateTime maxCreateDttm = (DateTime) unit.getSupplementallData("maxCreateDttm");
			Date billDate = (Date) unit.getSupplementallData("billDate");
			StringBuilder stringBuilder = null;
			ArrayList<ArrayList<Object>> paramsList = null;
			PayBillIdData payBillIdData = (PayBillIdData) unit.getPrimaryId();
			lowPayBillId = payBillIdData.getLowPayBillId();
			highPayBillId = payBillIdData.getHighPayBillId();
			DateTime ilmDateTime = (DateTime) unit.getSupplementallData("ilmDateTime");


			logger.debug("billId1 = " + lowPayBillId);
			logger.debug("billId2 = " + highPayBillId);
			

			// Added currency
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_BILL_PER (BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM, PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO,CURRENCY_CD)");
			stringBuilder.append(
					" SELECT BILL.BILL_ID, TRIM(BILL.ACCT_ID) AS ACCT_ID, BILL.BILL_DT, BILL.DUE_DT, BILL.CR_NOTE_FR_BILL_ID, BILL.ALT_BILL_ID, :sysTime , TRIM(B.PER_ID_NBR), BILL.ADHOC_BILL_SW, TRIM(ACCN.ACCT_NBR) AS ACCT_TYPE, TRIM(ACC.CIS_DIVISION) AS CIS_DIVISION, ");
			stringBuilder.append(" :initialStatus AS STATUS_CD, ' ' AS ERROR_INFO,ACC.CURRENCY_CD");
			stringBuilder.append(" FROM CI_BILL BILL, CI_ACCT ACC, CI_ACCT_NBR ACCN, CI_PER_ID B, CI_ACCT_PER X");
			stringBuilder.append(" WHERE ACC.ACCT_ID = BILL.ACCT_ID");
			stringBuilder.append(" AND ACCN.ACCT_ID  = ACC.ACCT_ID");
			stringBuilder.append("  AND ACCN.ACCT_NBR_TYPE_CD = :acctNbrTypeCd");
			stringBuilder.append(" AND B.PER_ID = X.PER_ID");
			stringBuilder.append("  AND X.ACCT_ID = BILL.ACCT_ID");
			stringBuilder.append(" AND B.ID_TYPE_CD = :extPartyId");
			stringBuilder.append(" AND TRIM(BILL.BILL_STAT_FLG) = 'C'");
			stringBuilder.append(" AND TRIM(BILL.CR_NOTE_FR_BILL_ID) IS NULL");
			if (billDate == null) {
				stringBuilder.append(" AND (BILL.COMPLETE_DTTM > :maxCreateDttm ");
			} else {
				stringBuilder.append(" AND(BILL.BILL_DT =  :billDate");
			}
			stringBuilder.append(" AND NOT EXISTS (SELECT 1 FROM  CM_PAY_REQ ");
			stringBuilder.append(" WHERE ");
			// stringBuilder.append(" BILL_DT = :billDate AND ");
			stringBuilder.append(" BILL.BILL_ID = BILL_ID))");
			stringBuilder.append(" AND BILL_ID between :lowPayBillId AND :highPayBillId");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			addParams(paramsList, "acctNbrTypeCd", paymentsRequestInterfaceLookUp.getAcctNbrTypeCd().trim(),
					"ACCT_TYPE");
			addParams(paramsList, "extPartyId", paymentsRequestInterfaceLookUp.getExtPartyId().trim(), "ID_TYPE_CD");
			if (billDate == null) {
				addParams(paramsList, "maxCreateDttm", maxCreateDttm, "COMPLETE_DTTM");
			} else {
				addParams(paramsList, "billDate", billDate, "BILL_DT");
			}
			addParams(paramsList, "lowPayBillId", lowPayBillId, "BILL_ID");
			addParams(paramsList, "highPayBillId", highPayBillId, "BILL_ID");
			addParams(paramsList, "sysTime", ilmDateTime, "CREATE_DTTM");


			executeQuery(stringBuilder, "Insert into cm_pay_req_bill_per (bill_id,....", paramsList);

			// Used char value for bill amount
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_BILL_AMT");
			stringBuilder.append(
					" (BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM, PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO, BILL_AMT, PAY_TYPE, CURRENCY_CD)");
			stringBuilder.append(
					" SELECT X.BILL_ID, X.ACCT_ID, X.BILL_DT, X.BILL_DUE_DT, X.CR_NOTE_FR_BILL_ID, X.ALT_BILL_ID, X.CREATE_DTTM, X.PER_ID_NBR, X.IS_IMD_FIN_ADJ, X.ACCT_TYPE, X.CIS_DIVISION, X.STATUS_CD, X.ERROR_INFO,");
			stringBuilder.append(
					" A.SRCH_CHAR_VAL AS BILL_AMT, DECODE(SIGN(A.SRCH_CHAR_VAL-0), -1, 'CR', 1, 'DR', 0, 'NA', NULL) AS PAY_TYPE, X.CURRENCY_CD");
			stringBuilder.append(" FROM CI_BILL_CHAR A, CM_PAY_REQ_BILL_PER X");
			stringBuilder
					.append(" WHERE A.BILL_ID=X.BILL_ID AND A.CHAR_TYPE_CD='BILL_AMT' AND X.STATUS_CD=:initialStatus");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_bill_amt...", paramsList);

			// Added this SQL for bill segments
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_LINE_AMT");
			stringBuilder.append(
					"  (BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM, PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO, BILL_AMT, PAY_TYPE, CURRENCY_CD,CUR_AMT, LINE_ID, PARENT_ID, ADHOC_CHAR_VAL)");
			stringBuilder.append(
					" SELECT X.BILL_ID, X.ACCT_ID, X.BILL_DT, X.BILL_DUE_DT, X.CR_NOTE_FR_BILL_ID, X.ALT_BILL_ID, X.CREATE_DTTM, X.PER_ID_NBR, X.IS_IMD_FIN_ADJ, X.ACCT_TYPE, X.CIS_DIVISION,");
			stringBuilder.append(" 'I' AS STATUS_CD,");
			stringBuilder.append(
					" X.ERROR_INFO, X.BILL_AMT, X.PAY_TYPE, X.CURRENCY_CD, A.CALC_AMT, B.BSEG_ID, B.BILL_ID, null");
			stringBuilder.append(" FROM CI_BSEG B, CI_BSEG_CALC A, CM_PAY_REQ_BILL_AMT X");
			stringBuilder.append(" WHERE X.BILL_ID=B.BILL_ID AND X.STATUS_CD=:initialStatus");
			stringBuilder.append(" AND B.BSEG_ID=A.BSEG_ID");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_line_amt(...", paramsList);

			// Using this for adjustment lines ONLY
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_LINE_AMT");
			stringBuilder.append(
					"  (BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM, PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO, BILL_AMT, PAY_TYPE, CURRENCY_CD,CUR_AMT, LINE_ID, PARENT_ID, ADHOC_CHAR_VAL)");
			stringBuilder.append(
					" SELECT X.BILL_ID, X.ACCT_ID, X.BILL_DT, X.BILL_DUE_DT, X.CR_NOTE_FR_BILL_ID, X.ALT_BILL_ID, X.CREATE_DTTM, X.PER_ID_NBR, X.IS_IMD_FIN_ADJ, X.ACCT_TYPE, X.CIS_DIVISION,");
			stringBuilder.append(
					" (CASE WHEN X.BILL_AMT>0 AND B.ADJ_AMT>0 AND X.ACCT_TYPE='FUND' AND B.ADJ_TYPE_CD='MOVRPAYF' AND ADJ.ADJ_ID IS NOT NULL THEN 'A'");
			stringBuilder.append(
					" WHEN X.BILL_AMT<0 AND B.ADJ_AMT<0 AND X.ACCT_TYPE='CHRG' AND B.ADJ_TYPE_CD='MOVRPAYC' AND ADJ.ADJ_ID IS NOT NULL THEN 'A'");
			stringBuilder.append(
					" WHEN X.BILL_AMT>0 AND B.ADJ_AMT>0 AND X.ACCT_TYPE='CHBK' AND B.ADJ_TYPE_CD='MOVRPYCB' AND ADJ.ADJ_ID IS NOT NULL THEN 'A'");
			stringBuilder.append(" ELSE 'I' END)  AS STATUS_CD,");
			stringBuilder.append(
					" X.ERROR_INFO, X.BILL_AMT, X.PAY_TYPE, X.CURRENCY_CD, B.ADJ_AMT, B.ADJ_ID, B.ADJ_TYPE_CD, ADJ.ADHOC_CHAR_VAL");
			stringBuilder.append(" FROM CI_ADJ_CHAR A, CI_ADJ B, CM_PAY_REQ_BILL_AMT X, CI_ADJ_CHAR ADJ");
			stringBuilder.append(
					" WHERE A.SRCH_CHAR_VAL=X.BILL_ID AND A.CHAR_TYPE_CD='BILL_ID' AND X.STATUS_CD=:initialStatus");
			stringBuilder.append(" AND A.ADJ_ID=B.ADJ_ID AND B.ADJ_ID=ADJ.ADJ_ID(+)");
			stringBuilder.append(" AND ADJ.CHAR_TYPE_CD(+)='PAYID'");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_line_amt(...", paramsList);

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" UPDATE CM_PAY_REQ_LINE_AMT SET STATUS_CD='A' WHERE BILL_ID IN(SELECT DISTINCT BILL_ID FROM CM_PAY_REQ_LINE_AMT WHERE STATUS_CD='A')");
			executeQuery(stringBuilder, "Update cm_pay_req_line_amt set status_cd...", paramsList);

			// Added mapping table
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			// ADDED COLUMN SETT_LEVEL_GRANULARITY TO FETCH FROM CM_TXN_ATTRIBUTES_MAP -
			// NAP-30741
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_BILL_TXN_INFO (BILL_ID, LINE_ID, ACCT_TYPE, UDF_CHAR_11, UDF_CHAR_12, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(
					" SELECT BSG.BILL_ID, BSGC.BSEG_ID, TMP3.ACCT_TYPE, TXNP.UDF_CHAR_11, TXNP.UDF_CHAR_12, TXNP.SETT_LEVEL_GRANULARITY");
			stringBuilder.append(
					" FROM CM_TXN_ATTRIBUTES_MAP TXNP, CI_BSEG_CALC BSGC, CI_BSEG BSG, CM_PAY_REQ_BILL_AMT TMP3");
			stringBuilder.append(" WHERE TXNP.BILLABLE_CHG_ID = BSGC.BILLABLE_CHG_ID");
			stringBuilder.append(" AND BSGC.BSEG_ID = BSG.BSEG_ID");
			stringBuilder.append(" AND TMP3.BILL_ID = BSG.BILL_ID");
			stringBuilder.append(" AND TMP3.ACCT_TYPE IN ('FUND','CHBK')");
			stringBuilder.append(" AND TMP3.STATUS_CD = :initialStatus");
			stringBuilder.append(" AND BSGC.BILLABLE_CHG_ID! =' '");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_bill_txn_info...", paramsList);

			// Removed distinct and trim
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			// ADDED COLUMN SETT_LEVEL_GRANULARITY TO FETCH FROM CM_PAY_REQ_BILL_TXN_INFO -
			// NAP-30741
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_TXN_GRANULARITY");
			stringBuilder.append(
					" (BILL_ID, LINE_ID, ACCT_TYPE, UDF_CHAR_11, UDF_CHAR_12, UDF_CHAR_11_KEY, UDF_CHAR_11_VAL, UDF_CHAR_12_KEY, UDF_CHAR_12_VAL, STATUS_CD, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(" SELECT BILL_ID, LINE_ID, ACCT_TYPE, UDF_CHAR_11, UDF_CHAR_12,");
			stringBuilder.append(" REGEXP_SUBSTR(trim(udf_char_11), '[^|]+', 1, 1) AS UDF_CHAR_11_KEY,");
			stringBuilder.append(" REGEXP_SUBSTR(trim(udf_char_11), '[^|]+', 1, 2) AS UDF_CHAR_11_VAl,");
			stringBuilder.append(" REGEXP_SUBSTR(trim(COLUMN_VALUE), '[^|]+', 1, 1) AS UDF_CHAR_12_KEY,");
			stringBuilder.append(
					" REGEXP_SUBSTR(trim(COLUMN_VALUE), '[^|]+', 1, 2) AS UDF_CHAR_12_VAL,:initialStatus, SETT_LEVEL_GRANULARITY");
			stringBuilder.append(" FROM (SELECT * FROM CM_PAY_REQ_BILL_TXN_INFO),");
			stringBuilder.append(" XMLTABLE(('\"'|| REPLACE(UDF_CHAR_12, ',', '\",\"')|| '\"'))");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_txn_granularity...", paramsList);
			
			// Removed distinct and trim
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_TXN_GRANULARITY");
			stringBuilder.append(
					" (BILL_ID, LINE_ID, ACCT_TYPE, UDF_CHAR_11, UDF_CHAR_12, UDF_CHAR_11_KEY, UDF_CHAR_11_VAL, UDF_CHAR_12_KEY, UDF_CHAR_12_VAL, STATUS_CD, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(" SELECT A.BILL_ID, A.LINE_ID, A.ACCT_TYPE, A.UDF_CHAR_11, A.UDF_CHAR_12,");
			stringBuilder.append(" REGEXP_SUBSTR(A.UDF_CHAR_11,'[^|]+',1,1) AS UDF_CHAR_11_KEY,");
			stringBuilder.append(" REGEXP_SUBSTR(A.UDF_CHAR_11,'[^|]+',1,2) AS UDF_CHAR_11_VAl,");
			stringBuilder.append(
					" NULL AS UDF_CHAR_12_KEY, NULL AS UDF_CHAR_12_VAL, :initialStatus, SETT_LEVEL_GRANULARITY");
			stringBuilder.append(" FROM CM_PAY_REQ_BILL_TXN_INFO A");
			stringBuilder.append(" WHERE UDF_CHAR_12 IS NULL AND UDF_CHAR_11 IS NOT NULL");
			stringBuilder.append(" AND A.ACCT_TYPE ='FUND'");
			addParams(paramsList, "initialStatus", paymentsRequestInterfaceLookUp.getInitialStatus().trim(),
					"STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_txn_granularity ", paramsList);
			
			// Used Mapping table and removed distinct
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_BCHG_INFO(BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM,"
							+ " PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO, BILL_AMT, PAY_TYPE, CURRENCY_CD,"
							+ "CUR_AMT, LINE_ID, PARENT_ID, ADHOC_CHAR_VAL,");
			stringBuilder.append(
					" IS_IND_FLG, FIN_ADJ_MAN_NRT, REL_RSRV_FLG, REL_WAF_FLG, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, SUB_STLMNT_LVL, "
							+ "SUB_STLMNT_LVL_REF, PAY_REQ_GRANULARITIES,GRANULARITY_HASH) ");
			stringBuilder.append(
					" SELECT DISTINCT A.BILL_ID,A.ACCT_ID, A.BILL_DT, A.BILL_DUE_DT, A.CR_NOTE_FR_BILL_ID, A.ALT_BILL_ID, A.CREATE_DTTM, "
							+ "A.PER_ID_NBR, NVL(W.ADHOC_SW,'N') AS IS_IMD_FIN_ADJ, A.ACCT_TYPE, A.CIS_DIVISION, A.STATUS_CD, A.ERROR_INFO, A.BILL_AMT,"
							+ " A.PAY_TYPE, A.CURRENCY_CD, A.CUR_AMT, A.LINE_ID, A.PARENT_ID, A.ADHOC_CHAR_VAL,");
			stringBuilder.append(" NVL(W.IS_IND_FLG,'N') AS IS_IND_FLG,");
			stringBuilder.append(
					" DECODE(NVL(W.IS_IND_FLG,'N'),'Y',NVL(W.PAY_NARRATIVE,'N'),'N') AS FIN_ADJ_MAN_NRT, NVL(W.REL_RESERVE_FLG,'N') AS REL_RSRV_FLG,"
							+ " NVL(W.REL_WAF_FLG,'N') AS REL_WAF_FLG,");
			stringBuilder.append(" NVL(W.FAST_PAY_VAL,'N') AS FASTEST_ROUTE_INDICATOR,");
			stringBuilder.append(
					" DECODE(NVL(W.REL_RESERVE_FLG,'N'),'Y', NVL(W.CASE_IDENTIFIER,'N'), DECODE(NVL(W.REL_WAF_FLG,'N'), 'Y',"
							+ "NVL(W.CASE_IDENTIFIER,'N'),'N')) AS CASE_IDENTIFIER,");
			stringBuilder.append(
					" Z.UDF_CHAR_11_KEY AS SUB_STLMNT_LVL, Z.UDF_CHAR_11_VAL AS SUB_STLMNT_LVL_REF, '' AS PAY_REQ_GRANULARITIES, "
							+ "W.GRANULARITY_HASH");
			stringBuilder.append(
					" FROM CM_PAY_REQ_LINE_AMT A, CI_BSEG_CALC Y, CM_BCHG_ATTRIBUTES_MAP W, CM_PAY_REQ_TXN_GRANULARITY Z");
			stringBuilder.append(" WHERE A.ACCT_TYPE!='CHRG'");
			stringBuilder.append(" AND Z.BILL_ID(+)=A.BILL_ID AND Z.LINE_ID(+)=A.LINE_ID");
			stringBuilder.append(" AND W.BILLABLE_CHG_ID(+)=Y.BILLABLE_CHG_ID");
			stringBuilder.append(" AND Y.BSEG_ID(+)=A.LINE_ID");
			executeQuery(stringBuilder, "Insert into cm_pay_req_bchg_info(bill_id...", paramsList);
			

			// Removed distinct
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_BCHG_INFO");
			stringBuilder.append(
					" (BILL_ID, ACCT_ID, BILL_DT, BILL_DUE_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, CREATE_DTTM, PER_ID_NBR, IS_IMD_FIN_ADJ, ACCT_TYPE, CIS_DIVISION, STATUS_CD, ERROR_INFO, BILL_AMT, PAY_TYPE, CURRENCY_CD,CUR_AMT, LINE_ID, PARENT_ID, ADHOC_CHAR_VAL,");
			stringBuilder.append(
					" IS_IND_FLG, FIN_ADJ_MAN_NRT, REL_RSRV_FLG, REL_WAF_FLG, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, PAY_REQ_GRANULARITIES)");
			stringBuilder.append(
					"SELECT  A.BILL_ID,A.ACCT_ID, A.BILL_DT, A.BILL_DUE_DT, A.CR_NOTE_FR_BILL_ID, A.ALT_BILL_ID, A.CREATE_DTTM, A.PER_ID_NBR, A.IS_IMD_FIN_ADJ, A.ACCT_TYPE, A.CIS_DIVISION, A.STATUS_CD, A.ERROR_INFO, A.BILL_AMT, A.PAY_TYPE, A.CURRENCY_CD, A.CUR_AMT, A.LINE_ID, A.PARENT_ID, A.ADHOC_CHAR_VAL, ");
			stringBuilder.append("nvl(IS_IND_FLG,'N') as IS_IND_FLG, ");
			stringBuilder.append("'N' as FIN_ADJ_MAN_NRT, ");
			stringBuilder.append("'N' as REL_RSRV_FLG, 'N' as REL_WAF_FLG, ");
			stringBuilder.append("'N' as FASTEST_ROUTE_INDICATOR, ");
			stringBuilder.append("'N' as CASE_IDENTIFIER, ");
			stringBuilder.append("null AS SUB_STLMNT_LVL, ");
			stringBuilder.append("null AS SUB_STLMNT_LVL_REF, ");
			stringBuilder.append("'' AS PAY_REQ_GRANULARITIES ");
			stringBuilder.append("FROM CM_PAY_REQ_LINE_AMT A, CI_BSEG_CALC Y, CM_BCHG_ATTRIBUTES_MAP W ");
			stringBuilder.append(
					"WHERE A.ACCT_TYPE='CHRG' AND A.LINE_ID = Y.BSEG_ID(+) AND Y.BILLABLE_CHG_ID =W.BILLABLE_CHG_ID(+) ");
			executeQuery(stringBuilder, "Insert into cm_pay_req_bchg_info...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" UPDATE CM_PAY_REQ_BCHG_INFO A");
			stringBuilder.append(
					" SET A.PAY_REQ_GRANULARITIES= ((SELECT LISTAGG((B.UDF_CHAR_12_KEY),'|') within GROUP (ORDER BY B.UDF_CHAR_12_KEY)");
			stringBuilder
					.append(" FROM CM_PAY_REQ_TXN_GRANULARITY B WHERE A.BILL_ID=B.BILL_ID AND A.LINE_ID  =B.LINE_ID))");
			executeQuery(stringBuilder, "Update cm_pay_req_bchg_info a...", paramsList);

			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_FUND_INFO  (BILL_ID, LINE_1, LINE_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, PER_ID_NBR, PAY_TYPE, BILL_AMT, CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF,");
			stringBuilder.append(
					" REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ, FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES, "
							+ "CREATE_DTTM, UDF_CHAR_11, UDF_CHAR_12,SETT_LEVEL_GRANULARITY) ");
			stringBuilder.append(
					" SELECT TMP2.BILL_ID, ROWNUM LINE_1,TMP2.LINE_ID, TMP2.BILL_DT, TMP2.CR_NOTE_FR_BILL_ID, TMP2.ALT_BILL_ID, TMP2.ACCT_TYPE,");
			stringBuilder.append(
					" TMP2.CIS_DIVISION, TMP2.PER_ID_NBR, TMP2.PAY_TYPE, TMP2.CUR_AMT, TMP2.CURRENCY_CD, TMP2.IS_IND_FLG, TMP2.SUB_STLMNT_LVL, TMP2.SUB_STLMNT_LVL_REF, TMP2.REL_RSRV_FLG,");
			stringBuilder.append(
					" TMP2.REL_WAF_FLG, TMP2.IS_IMD_FIN_ADJ, TMP2.FIN_ADJ_MAN_NRT, TMP2.FASTEST_ROUTE_INDICATOR, TMP2.CASE_IDENTIFIER, TMP2.PAY_REQ_GRANULARITIES,");
			stringBuilder.append(" TMP2.CREATE_DTTM, T.UDF_CHAR_11, T.UDF_CHAR_12, ");
			stringBuilder.append("CASE WHEN TMP2.GRANULARITY_HASH = 0 or  TMP2.GRANULARITY_HASH is null ");
			stringBuilder.append("THEN CONCAT(TO_CHAR(TMP2.BILL_DT,'YYMMDD'),TMP2.ACCT_ID) ");
			stringBuilder.append("ELSE CONCAT(CONCAT(TO_CHAR(TMP2.BILL_DT,'YYMMDD'),TMP2.ACCT_ID),TMP2.GRANULARITY_HASH) "
							+ " END AS SETT_LEVEL_GRANULARITY ");

			stringBuilder.append(
					" from CM_PAY_REQ_BCHG_INFO TMP2,(select distinct t.bill_id,t.line_id,t.udf_Char_11,t.udf_char_12 from CM_PAY_REQ_TXN_GRANULARITY t)  T");
			stringBuilder.append(" WHERE TMP2.IS_IND_FLG ='N'");
			stringBuilder.append(" AND TMP2.ACCT_TYPE IN ('FUND','CHBK')");
			stringBuilder.append(" AND TMP2.PAY_TYPE!='NA'");
			stringBuilder.append(" AND TMP2.BILL_AMT<>0");
			stringBuilder.append(" AND TMP2.bill_id=T.BILL_id(+)");
			stringBuilder.append(" AND TMP2.LINE_ID=T.LINE_ID(+) AND TMP2.STATUS_CD='I'");
			executeQuery(stringBuilder, "Insert into cm_pay_req_fund_info  (bill_id...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();

			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_TMP (BILL_ID, LINE_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, PER_ID_NBR, BILL_AMT,");
			stringBuilder.append(
					" CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES, GRANULARITIES_TYPE, REFERENCE_VAL,");
			stringBuilder.append(
					" CREATE_DTTM, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, UDF_CHAR_11, UDF_CHAR_12, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(
					" SELECT HI.BILL_ID, min(HI.LINE_1), HI.BILL_DT, HI.CR_NOTE_FR_BILL_ID, HI.ALT_BILL_ID, HI.ACCT_TYPE, HI.CIS_DIVISION, HI.PER_ID_NBR, SUM(HI.BILL_AMT) AS BILL_AMT, HI.CURRENCY_CD,");
			stringBuilder.append(
					" HI.IS_IND_FLG, HI.SUB_STLMNT_LVL, HI.SUB_STLMNT_LVL_REF, HI.REL_RSRV_FLG, HI.REL_WAF_FLG, HI.IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" HI.FIN_ADJ_MAN_NRT, HI.FASTEST_ROUTE_INDICATOR, HI.CASE_IDENTIFIER, HI.PAY_REQ_GRANULARITIES,");
			stringBuilder
					.append(" DECODE(TRIM(HI.PAY_REQ_GRANULARITIES),NULL,'',TMP1.UDF_CHAR_12_KEY) AS UDF_CHAR_12_KEY,");
			stringBuilder
					.append(" DECODE(TRIM(HI.PAY_REQ_GRANULARITIES),NULL,'',TMP1.UDF_CHAR_12_VAL) AS UDF_CHAR_12_VAL,");
			stringBuilder.append(
					" HI.CREATE_DTTM, HI.CREATE_DTTM, :yesStatus, NULL, HI.UDF_CHAR_11, HI.UDF_CHAR_12,  "
							+ "nvl(TMP1.SETT_LEVEL_GRANULARITY,HI.SETT_LEVEL_GRANULARITY) ");
			stringBuilder.append(" from CM_PAY_REQ_TXN_GRANULARITY TMP1, CM_PAY_REQ_FUND_INFO HI");
			stringBuilder.append(" WHERE TMP1.BILL_ID(+) = HI.BILL_ID and TMP1.LINE_ID(+) = HI.LINE_ID");
			stringBuilder.append(
					" group by HI.BILL_ID, HI.BILL_DT, HI.CR_NOTE_FR_BILL_ID, HI.ALT_BILL_ID, HI.ACCT_TYPE, HI.CIS_DIVISION, HI.PER_ID_NBR,");
			stringBuilder.append(
					" HI.CURRENCY_CD, HI.IS_IND_FLG, HI.SUB_STLMNT_LVL, HI.SUB_STLMNT_LVL_REF, HI.REL_RSRV_FLG, HI.REL_WAF_FLG,");
			stringBuilder.append(
					" HI.IS_IMD_FIN_ADJ, HI.FIN_ADJ_MAN_NRT, HI.FASTEST_ROUTE_INDICATOR, HI.CASE_IDENTIFIER, HI.PAY_REQ_GRANULARITIES,");
			stringBuilder.append(
					" DECODE(TRIM(HI.PAY_REQ_GRANULARITIES),NULL,'',TMP1.UDF_CHAR_12_KEY), DECODE(TRIM(HI.PAY_REQ_GRANULARITIES),");
			stringBuilder.append(
					" NULL,'',TMP1.UDF_CHAR_12_VAL), HI.CREATE_DTTM, SYSTIMESTAMP, :yesStatus, NULL, HI.UDF_CHAR_11, HI.UDF_CHAR_12,  "
							+ "nvl(TMP1.SETT_LEVEL_GRANULARITY,HI.SETT_LEVEL_GRANULARITY) ");
			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_tmp (bill_id...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_TMP (BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, PER_ID_NBR, BILL_AMT,");
			stringBuilder.append(
					" CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES, GRANULARITIES_TYPE, REFERENCE_VAL,");
			stringBuilder.append(
					" CREATE_DTTM, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, UDF_CHAR_11, UDF_CHAR_12, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(
					" SELECT D.BILL_ID, D.BILL_DT, D.CR_NOTE_FR_BILL_ID, D.ALT_BILL_ID, D.ACCT_TYPE, D.CIS_DIVISION, D.PER_ID_NBR, SUM(D.CUR_AMT), D.CURRENCY_CD,");
			stringBuilder.append(
					" D.IS_IND_FLG, NULL AS SUB_STLMNT_LVL, NULL AS SUB_STLMNT_LVL_REF, D.REL_RSRV_FLG, D.REL_WAF_FLG, D.IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" D.FIN_ADJ_MAN_NRT, D.FASTEST_ROUTE_INDICATOR, D.CASE_IDENTIFIER, NULL AS PAY_REQ_GRANULARITIES,NULL,NULL,");
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder.append(" D.CREATE_DTTM, D.CREATE_DTTM, :yesStatus, NULL,D.SUB_STLMNT_LVL,null,0");
			stringBuilder.append(" FROM CM_PAY_REQ_BCHG_INFO D");
			stringBuilder.append(" WHERE (D.ACCT_TYPE <>'FUND' AND D.ACCT_TYPE<>'CHBK')");
			stringBuilder.append(" and D.PAY_TYPE!='NA'");
			stringBuilder.append(" AND D.IS_IND_FLG='N' AND D.STATUS_CD='I'");
			stringBuilder.append(
					" GROUP BY D.BILL_ID, D.BILL_DT, D.CR_NOTE_FR_BILL_ID, D.ALT_BILL_ID, D.ACCT_TYPE, D.CIS_DIVISION, D.PER_ID_NBR, D.CURRENCY_CD,");
			stringBuilder.append(
					" D.IS_IND_FLG, NULL, NULL, D.REL_RSRV_FLG, D.REL_WAF_FLG, D.IS_IMD_FIN_ADJ, D.FIN_ADJ_MAN_NRT, D.FASTEST_ROUTE_INDICATOR,");
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder.append(
					" D.CASE_IDENTIFIER, NULL, NULL, NULL, D.CREATE_DTTM, SYSTIMESTAMP, 'Y', NULL, D.SUB_STLMNT_LVL, null,0");
			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_tmp (bill_id...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ_TMP (BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, "
							+ "PER_ID_NBR, BILL_AMT,");
			stringBuilder.append(
					" CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES, GRANULARITIES_TYPE, REFERENCE_VAL,");
			stringBuilder.append(
					" CREATE_DTTM, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, UDF_CHAR_11, UDF_CHAR_12, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(
					" SELECT D.BILL_ID, D.BILL_DT, D.CR_NOTE_FR_BILL_ID, D.ALT_BILL_ID, D.ACCT_TYPE, D.CIS_DIVISION, D.PER_ID_NBR, "
							+ "SUM(D.CUR_AMT), D.CURRENCY_CD,");
			stringBuilder.append(
					" D.IS_IND_FLG, NULL AS SUB_STLMNT_LVL, NULL AS SUB_STLMNT_LVL_REF, D.REL_RSRV_FLG, D.REL_WAF_FLG, D.IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" D.FIN_ADJ_MAN_NRT, D.FASTEST_ROUTE_INDICATOR, D.CASE_IDENTIFIER, NULL AS PAY_REQ_GRANULARITIES,NULL,NULL,");
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder
					.append(" D.CREATE_DTTM, D.CREATE_DTTM, :yesStatus, NULL,D.SUB_STLMNT_LVL,null,");
			stringBuilder.append("CASE WHEN D.GRANULARITY_HASH = 0 or  D.GRANULARITY_HASH is null ");
			stringBuilder.append("THEN CONCAT(TO_CHAR(D.BILL_DT,'YYMMDD'),D.ACCT_ID) ");
			stringBuilder.append(
					"ELSE CONCAT(CONCAT(TO_CHAR(D.BILL_DT,'YYMMDD'),D.ACCT_ID),D.GRANULARITY_HASH)  END AS SETT_LEVEL_GRANULARITY ");
			stringBuilder.append(" FROM CM_PAY_REQ_BCHG_INFO D");
			stringBuilder.append(" WHERE D.PAY_TYPE!='NA'");
			stringBuilder.append(" AND D.IS_IND_FLG='Y' AND D.STATUS_CD='I'");
			stringBuilder.append(
					" GROUP BY D.BILL_ID, D.BILL_DT, D.CR_NOTE_FR_BILL_ID, D.ALT_BILL_ID, D.ACCT_TYPE, D.CIS_DIVISION, D.PER_ID_NBR, "
							+ "D.CURRENCY_CD,");
			stringBuilder.append(
					" D.IS_IND_FLG, NULL, NULL, D.REL_RSRV_FLG, D.REL_WAF_FLG, D.IS_IMD_FIN_ADJ, D.FIN_ADJ_MAN_NRT, D.FASTEST_ROUTE_INDICATOR,");
			// ADDED COLUMN SETT_LEVEL_GRANULARITY FROM CM_PAY_REQ_TXN_GRANULARITY BUT
			// POPULATED AS 0 - NAP-30741
			stringBuilder.append(
					" D.CASE_IDENTIFIER, NULL, NULL, NULL, D.CREATE_DTTM, SYSTIMESTAMP, 'Y', NULL, D.SUB_STLMNT_LVL, null, ");
			stringBuilder.append("CASE WHEN D.GRANULARITY_HASH = 0 or  D.GRANULARITY_HASH is null ");
			stringBuilder.append("THEN CONCAT(TO_CHAR(D.BILL_DT,'YYMMDD'),D.ACCT_ID) ");
			stringBuilder.append(
					"ELSE CONCAT(CONCAT(TO_CHAR(D.BILL_DT,'YYMMDD'),D.ACCT_ID),D.GRANULARITY_HASH)  END ");

			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_tmp (bill_id,...", paramsList);			
			
			 stringBuilder = new StringBuilder();
			 paramsList = new ArrayList<>(); 
			 stringBuilder.append(" delete from CM_PAY_REQ_TMP WHERE BILL_AMT=0");
			 executeQuery(stringBuilder, "delete from CM_PAY_REQ_TMP WHERE BILL_AMT=0...", paramsList);
			  
			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" delete from CM_PAY_REQ_TMP t WHERE exists (select 1 from cm_bill_payment_dtl where bill_id=t.bill_id and status_cd= :overpaid) ");
			addParams(paramsList, "overpaid", "OVERPAID", "status_cd");
			executeQuery(stringBuilder, "delete from CM_PAY_REQ_TMP WHERE exists...", paramsList);

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" DELETE FROM CM_PAY_REQ_TMP WHERE BILL_ID IN ( ");
			stringBuilder.append(" SELECT A.BILL_ID FROM CM_PAY_REQ_TMP A, CI_BSEG B, CI_BSEG_CALC C,");
			stringBuilder.append(" CI_BILL_CHG D, CI_PRICEITEM_CHAR E, CI_BILL_CHG_CHAR F, CM_BCHG_ATTRIBUTES_MAP STG");
			stringBuilder.append(
					" WHERE A.BILL_ID=B.BILL_ID AND B.BSEG_ID = C.BSEG_ID AND C.BILLABLE_CHG_ID =D.BILLABLE_CHG_ID");
			stringBuilder.append(
					" AND D.PRICEITEM_CD= E.PRICEITEM_CD AND E.CHAR_TYPE_CD ='NON_ZERO' AND E.ADHOC_CHAR_VAL='Y'");
			stringBuilder.append(
					" AND D.BILLABLE_CHG_ID = F.BILLABLE_CHG_ID AND F.CHAR_TYPE_CD='NON_ZERO' AND STG.BILLABLE_CHG_ID=D.BILLABLE_CHG_ID)");
			executeQuery(stringBuilder, "delete from CM_PAY_REQ_TMP WHERE exists...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" UPDATE CM_PAY_REQ_TMP SET PAY_TYPE=CASE WHEN BILL_AMT<=0 THEN 'CR' WHEN BILL_AMT>0 THEN 'DR' END");
			executeQuery(stringBuilder, "Update cm_pay_req_tmp set pay_type=case when bill_amt...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" UPDATE CM_PAY_REQ_TMP SET LINE_ID=ROWNUM WHERE LINE_ID IS NULL");
			executeQuery(stringBuilder, "Update cm_pay_req_tmp set line_id...", paramsList);

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" UPDATE CM_PAY_REQ_TMP TMP SET MPG_TYPE='MPG2' WHERE EXISTS (SELECT 1 FROM CM_MPG2_LIST WHERE PER_ID_NBR = TMP.PER_ID_NBR ");
			stringBuilder.append(" AND VALID_FROM <= :billDate AND (VALID_TO >= :billDate OR VALID_TO IS NULL))");
			if (billDate == null) {
				addParams(paramsList, "billDate", getSystemDateTime().getDate().addDays(-1), "BILL_DT");
			} else {
				addParams(paramsList, "billDate", billDate, "BILL_DT");
			}
			executeQuery(stringBuilder, "Update cm_pay_req_tmp set mpg_type...", paramsList);			
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_BILL_SETT_MAP (BILL_ID, LINE_ID, SETT_LEVEL_GRANULARITY, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, MPG_TYPE)");
			// ADDED SETT_LEVEL_GRANULARITY, FETCHED FROM CM_PAY_REQ_TMP
			stringBuilder.append(
					" SELECT DISTINCT TMP.BILL_ID, TMP.LINE_ID, ( CASE WHEN TMP.SETT_LEVEL_GRANULARITY IS NULL THEN SUBSTR(CONCAT(concat(TO_CHAR(TMP2.BILL_DT,'YYMMDD'), ");
			stringBuilder.append(
					" TMP2.ACCT_ID),ORA_HASH(CONCAT(TRIM(TMP.UDF_CHAR_11),TRIM(TMP.UDF_CHAR_12)))),1,30) WHEN TMP.SETT_LEVEL_GRANULARITY IS NOT NULL THEN TMP.SETT_LEVEL_GRANULARITY END), ");
			stringBuilder.append(" TMP2.CREATE_DTTM, :yesStatus, NULL, TMP.MPG_TYPE");
			stringBuilder.append(" FROM CM_PAY_REQ_TMP TMP, CM_PAY_REQ_BILL_PER TMP2");
			stringBuilder.append(" WHERE TMP.BILL_ID=TMP2.BILL_ID");

			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_bill_sett_map (bill_id...", paramsList);

			// REQUEST entry to be created in CM_BILL_PAYMENT table for all bills
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_BILL_PAYMENT_DTL (PAY_DTL_ID, UPLOAD_DTTM, PAY_DT, BILL_ID,LINE_ID,LINE_AMT,PAY_TYPE,PREV_UNPAID_AMT,PAY_AMT, UNPAID_AMT,       ");
			stringBuilder.append(
					" CURRENCY_CD,STATUS_CD, EXT_TRANSMIT_ID, STATUS_UPD_DTTM, ILM_DT, OVERPAID, RECORD_STAT, OVERPAYMENT_RELEASE) " );
			stringBuilder.append("SELECT PAY_DTL_ID_SQ.NEXTVAL, PAY.CREATE_DTTM, ");
			stringBuilder.append(
					" PAY.CREATE_DTTM , PAY.BILL_ID BILL_ID, PAY.LINE_ID AS LINE_ID, PAY.BILL_AMT, PAY.PAY_TYPE PAY_TYPE, PAY.BILL_AMT PREV_UNPAID_AMT,              ");
			stringBuilder.append(
					" 0 AS TENDER_AMT, PAY.BILL_AMT UNPAID_AMT, PAY.CURRENCY_CD CURRENCY_CD, 'REQUEST' STATUS, ' ' EXT_TRANSMIT_ID, :systime, :sysdt,'' AS OVERPAID, ");
			stringBuilder.append(
					" :pending, ' ' FROM ( SELECT DISTINCT PR.CREATE_DTTM CREATE_DTTM , PR.BILL_ID BILL_ID, PR.LINE_ID LINE_ID, " );
			stringBuilder.append(" PR.BILL_AMT as BILL_AMT, PR.PAY_TYPE PAY_TYPE, ");
			stringBuilder.append(
					" PR.CURRENCY_CD CURRENCY_CD FROM CM_PAY_REQ_TMP PR WHERE NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL ");
			stringBuilder.append(" WHERE BILL_ID =PR.BILL_ID)) PAY ");
			addParams(paramsList, "pending", payConfirmationLookup.getPending(), "RECORD_STAT");
			addParams(paramsList, "systime", getSystemDateTime(), "STATUS_UPD_DTTM");
			addParams(paramsList, SYSTEM_DATE, getSystemDateTime().getDate(), ILM_DATE);
			executeQuery(stringBuilder, "Insert into cm_bill_payment_dtl (bill_id, line_id...", paramsList);


			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(
					" INSERT INTO CM_PAY_REQ (BILL_ID, LINE_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ALT_BILL_ID, ACCT_TYPE, CIS_DIVISION, PER_ID_NBR, PAY_TYPE, BILL_AMT,");
			stringBuilder.append(
					" CURRENCY_CD, IS_IND_FLG, SUB_STLMNT_LVL, SUB_STLMNT_LVL_REF, REL_RSRV_FLG, REL_WAF_FLG, IS_IMD_FIN_ADJ,");
			stringBuilder.append(" FIN_ADJ_MAN_NRT, FASTEST_ROUTE_INDICATOR, CASE_IDENTIFIER, PAY_REQ_GRANULARITIES,");
			stringBuilder.append(" CREATE_DTTM, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, MPG_TYPE, ILM_DT, ILM_ARCH_SW, ");
			stringBuilder.append(" PAY_REQ_ID, PAY_DETAIL_ID, SETT_LEVEL_GRANULARITY)");
			stringBuilder.append(" SELECT distinct A.BILL_ID, A.LINE_ID, A.BILL_DT,");
			stringBuilder.append(" A.CR_NOTE_FR_BILL_ID, A.ALT_BILL_ID, A.ACCT_TYPE,");
			stringBuilder.append(" A.CIS_DIVISION, A.PER_ID_NBR, A.PAY_TYPE, A.BILL_AMT, A.CURRENCY_CD,");
			stringBuilder.append(" A.IS_IND_FLG, TRIM(A.SUB_STLMNT_LVL), A.SUB_STLMNT_LVL_REF, A.REL_RSRV_FLG,");
			stringBuilder.append(" A.REL_WAF_FLG, A.IS_IMD_FIN_ADJ,");
			stringBuilder.append(
					" A.FIN_ADJ_MAN_NRT, A.FASTEST_ROUTE_INDICATOR, A.CASE_IDENTIFIER, A.PAY_REQ_GRANULARITIES,");
			stringBuilder.append(
					" A.CREATE_DTTM, A.CREATE_DTTM, A.EXTRACT_FLG, A.EXTRACT_DTTM, A.MPG_TYPE, A.CREATE_DTTM, :yesStatus, " );
			stringBuilder.append("CONCAT(A.BILL_ID, A.LINE_ID),");
			stringBuilder.append(" (NVL((SELECT MAX(PAY_DTL_ID) FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID = A.BILL_ID AND ");
			stringBuilder.append(" LINE_ID = A.LINE_ID GROUP BY BILL_ID, LINE_ID),0)), ");
			stringBuilder.append(" (SELECT SETT_LEVEL_GRANULARITY FROM CM_BILL_SETT_MAP WHERE BILL_ID = A.BILL_ID ");
			stringBuilder.append(" AND LINE_ID = A.LINE_ID)");
			stringBuilder.append(
					" FROM CM_PAY_REQ_TMP A WHERE NOT EXISTS (SELECT 1 FROM CM_PAY_REQ WHERE BILL_ID =A.BILL_ID) ");
			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "ILM_ARCH_SW");
			executeQuery(stringBuilder, "Insert into cm_pay_req (bill_id, line_id...", paramsList);

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_GRANULARITIES (BILL_ID, LINE_ID, GRANULARITIES_TYPE,");
			stringBuilder.append(" REFERENCE_VAL, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, MPG_TYPE, PAY_DETAIL_ID)");
			stringBuilder.append(" SELECT distinct A.BILL_ID, A.LINE_ID, A.GRANULARITIES_TYPE, A.REFERENCE_VAL, ");
			stringBuilder.append(" A.CREATE_DTTM, :yesStatus, NULL, A.MPG_TYPE, ");
			stringBuilder.append(" (NVL((SELECT MAX(PAY_DTL_ID) FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID = A.BILL_ID AND ");
			stringBuilder.append(" LINE_ID = A.LINE_ID GROUP BY BILL_ID, LINE_ID),0)) ");
			stringBuilder.append(" FROM CM_PAY_REQ_TMP A");
			stringBuilder.append(" WHERE A.ACCT_TYPE IN ('FUND','CHBK') AND A.GRANULARITIES_TYPE IS NOT NULL");
			stringBuilder.append(
					" AND A.REFERENCE_VAL IS NOT NULL AND NOT EXISTS (SELECT 1 FROM CM_PAY_REQ_GRANULARITIES WHERE BILL_ID =A.BILL_ID)");
			addParams(paramsList, "yesStatus", paymentsRequestInterfaceLookUp.getYesStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_granularities (bill_id...", paramsList);
			
			
			// RIA: Set char CM_ISGRN on bill (For granularity)
			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder
					.append(" INSERT INTO CI_BILL_CHAR (BILL_ID, CHAR_TYPE_CD, SEQ_NUM, CHAR_VAL, SRCH_CHAR_VAL) ");
			stringBuilder.append(" SELECT BILL_ID, 'CM_ISGRN', '1', 'Y', 'Y' ");
			stringBuilder.append(" FROM CM_PAY_REQ_TMP ");
			stringBuilder.append(" GROUP BY BILL_ID HAVING COUNT(BILL_ID)>1 ");
			executeQuery(stringBuilder, "Insert CM_ISGRN char type into CI_BILL_CHAR", paramsList);

			stringBuilder = new StringBuilder();
			paramsList = new ArrayList<>();
			stringBuilder.append(" INSERT INTO CM_PAY_REQ_ERR (BILL_ID,");
			stringBuilder.append(" MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO)");
			stringBuilder.append(" SELECT BILL_ID, :msgCat, MESSAGE_NBR, ERROR_INFO");
			stringBuilder.append(" FROM CM_PAY_REQ_BILL_PER");
			stringBuilder.append(" WHERE STATUS_CD=:errorStatus");
			addParams(paramsList, "msgCat", paymentsRequestInterfaceLookUp.getMsgCat().trim(), "MESSAGE_CAT_NBR");
			addParams(paramsList, "errorStatus", paymentsRequestInterfaceLookUp.getErrorStatus().trim(), "STATUS_COD");
			executeQuery(stringBuilder, "Insert into cm_pay_req_err (bill_id...", paramsList);
			

			insertZeroAmtPayReq();
			insertZeroAmtPayReqInPaymentDetailTable( payConfirmationLookup);
			createPayDtlSnapshotEntries();


			stringBuilder = new StringBuilder("commit");
			paramsList = new ArrayList<>();
			executeQuery(stringBuilder, "commit...", paramsList);

			return true;
		}

		private void insertZeroAmtPayReqInPaymentDetailTable(InboundPaymentsLookUp payConfirmationLookup) {

			// REQUEST entry to be created in CM_BILL_PAYMENT table for zero amount  bills
			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();

			stringBuilder.append(" INSERT INTO CM_BILL_PAYMENT_DTL (PAY_DTL_ID, UPLOAD_DTTM, PAY_DT, BILL_ID, LINE_ID,");
			stringBuilder.append(" LINE_AMT, PAY_TYPE, PREV_UNPAID_AMT, PAY_AMT, UNPAID_AMT, CURRENCY_CD, ");
			stringBuilder.append(" STATUS_CD, EXT_TRANSMIT_ID, STATUS_UPD_DTTM, ILM_DT, OVERPAID, RECORD_STAT, OVERPAYMENT_RELEASE) ");
			stringBuilder.append(" SELECT PAY_DTL_ID_SQ.NEXTVAL, PAY.CREATE_DTTM,  PAY.CREATE_DTTM PAY_DT, PAY.BILL_ID, PAY.LINE_ID, ");
			stringBuilder.append(" PAY.BILL_AMT, PAY.PAY_TYPE , PAY.BILL_AMT PREV_UNPAID_AMT, 0 AS PAY_AMT, PAY.BILL_AMT UNPAID_AMT, ");
			stringBuilder.append(" PAY.CURRENCY_CD , 'REQUEST' STATUS, ' ' EXT_TRANSMIT_ID, :systime, :sysdt,'' AS OVERPAID,:pending, ' ' ");
			stringBuilder.append(" FROM ( SELECT DISTINCT PR.CREATE_DTTM CREATE_DTTM , PR.BILL_ID BILL_ID, PR.LINE_ID LINE_ID, ");
			stringBuilder.append(" PR.BILL_AMT as BILL_AMT, PR.PAY_TYPE PAY_TYPE,PR.CURRENCY_CD CURRENCY_CD FROM CM_PAY_REQ PR WHERE ");
			stringBuilder.append(" BILL_AMT='0' AND NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID =PR.BILL_ID)  ) PAY ");

			addParams(paramsList, "pending", payConfirmationLookup.getPending(), "RECORD_STAT");
			addParams(paramsList, "systime", getSystemDateTime(), "STATUS_UPD_DTTM");
			addParams(paramsList, SYSTEM_DATE, getSystemDateTime().getDate(), ILM_DATE);
			executeQuery(stringBuilder, "Insert into cm_bill_payment_dtl (bill_id, line_id...", paramsList);

		}

		private void createPayDtlSnapshotEntries(){
			
		  StringBuilder createSnapshotEntries = new StringBuilder();
		  ArrayList<ArrayList<Object>>	paramsList = new ArrayList<>();

		  createSnapshotEntries.append(" INSERT INTO CM_BILL_PAYMENT_DTL_SNAPSHOT ");
		  createSnapshotEntries.append(" (BILL_BALANCE_ID ,LATEST_PAY_DTL_ID ,LATEST_UPLOAD_DTTM ,PAY_DT ,BILL_DT,PARTY_ID , ");
		  createSnapshotEntries.append(" LCP_DESCRIPTION ,LCP,ACCT_TYPE ,ACCOUNT_DESCRIPTION ,EXT_TRANSMIT_ID ,BILL_ID ,ALT_BILL_ID , ");
		  createSnapshotEntries.append(" LINE_ID ,LINE_AMT ,PREV_UNPAID_AMT ,LATEST_PAY_AMT ,UNPAID_AMT ,BILL_AMOUNT, BILL_BALANCE , ");
		  createSnapshotEntries.append(" CURRENCY_CD ,LATEST_STATUS ,PAY_TYPE ,ILM_DT ,ILM_ARCH_SW ,OVERPAID ,RECORD_STAT , ");
		  createSnapshotEntries.append(" STATUS_UPD_DTTM ,MESSAGE_CAT_NBR,MESSAGE_NBR,ERROR_INFO, ");
		  createSnapshotEntries.append("EXT_SOURCE_CD,CREDIT_NOTE_ID, OVERPAYMENT_RELEASE) ");
		  createSnapshotEntries.append(" SELECT BILL_BALANCE_ID_SEQ.NEXTVAL, PAY.PAY_DTL_ID, PAY.UPLOAD_DTTM, PAY.PAY_DT , ");
		  createSnapshotEntries.append(" AMT.BILL_DT,AMT.PER_ID_NBR ,L.DESCR,trim(DCHAR.CHAR_VAL),AMT.ACCT_TYPE, ");
		  createSnapshotEntries.append(" CASE WHEN AMT.ACCT_TYPE = 'CHRG' then 'Charging' ");
		  createSnapshotEntries.append(" when AMT.ACCT_TYPE = 'FUND' then 'Funding' ");
		  createSnapshotEntries.append(" when AMT.ACCT_TYPE = 'CHBK' then 'Chargebacks' end AS ACCOUNT_DESCRIPTION, ");
		  createSnapshotEntries.append(" PAY.EXT_TRANSMIT_ID,PAY.BILL_ID , AMT.ALT_BILL_ID, PAY.LINE_ID, PAY.LINE_AMT, ");
		  createSnapshotEntries.append(" PAY.PREV_UNPAID_AMT , PAY.PAY_AMT, PAY.UNPAID_AMT , nvl(AMT.BILL_AMT, PAY.LINE_AMT) ");
		  createSnapshotEntries.append(" AS BILL_AMOUNT, ");
		  createSnapshotEntries.append(" UNPAID_AMT AS BILL_BALANCE, PAY.CURRENCY_CD , PAY.STATUS_CD, PAY.PAY_TYPE PAY_TYPE, ");
		  createSnapshotEntries.append(" PAY.ILM_DT, PAY.ILM_ARCH_SW,PAY.OVERPAID,PAY.RECORD_STAT,PAY.STATUS_UPD_DTTM, ");
		  createSnapshotEntries.append(" PAY.MESSAGE_CAT_NBR , PAY.MESSAGE_NBR,PAY.ERROR_INFO, PAY.EXT_SOURCE_CD,PAY.CREDIT_NOTE_ID, ' ' ");
		  createSnapshotEntries.append(" FROM CI_CIS_DIVISION_L L, CI_CIS_DIV_CHAR DCHAR, CM_PAY_REQ_BILL_AMT AMT, ");
		  createSnapshotEntries.append(" CM_BILL_PAYMENT_DTL PAY WHERE AMT.BILL_ID = PAY.BILL_ID  AND PAY.ILM_DT = :sysdt ");
		  createSnapshotEntries.append(" AND L.CIS_DIVISION = AMT.CIS_DIVISION ");
		  createSnapshotEntries.append(" AND L.CIS_DIVISION = DCHAR.CIS_DIVISION AND DCHAR.CHAR_TYPE_CD= 'BOLE    ' ");
			createSnapshotEntries.append(" AND NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL_SNAPSHOT WHERE BILL_ID= PAY.BILL_ID)");


			addParams(paramsList, SYSTEM_DATE, getSystemDateTime().getDate(), ILM_DATE);
		  executeQuery(createSnapshotEntries, "Insert into cm_bill_payment_dtl_snapshot (bill_id, line_id...", paramsList);

		}

		private void insertZeroAmtPayReq() {

			StringBuilder zeroPayRequestBuilder = new StringBuilder();
			ArrayList<ArrayList<Object>>	paramsList = new ArrayList<>();
			zeroPayRequestBuilder.append(" INSERT INTO CM_PAY_REQ                                                                        ");
			zeroPayRequestBuilder.append(" (BILL_ID,PER_ID_NBR,CIS_DIVISION,LINE_ID,ALT_BILL_ID,BILL_DT,ACCT_TYPE,                       ");
			zeroPayRequestBuilder.append(" PAY_TYPE,BILL_AMT,CURRENCY_CD,IS_IND_FLG,SUB_STLMNT_LVL,SUB_STLMNT_LVL_REF,                   ");
			zeroPayRequestBuilder.append(" CR_NOTE_FR_BILL_ID,IS_IMD_FIN_ADJ,FIN_ADJ_MAN_NRT,REL_RSRV_FLG,REL_WAF_FLG,                   ");
			zeroPayRequestBuilder.append(" FASTEST_ROUTE_INDICATOR,CASE_IDENTIFIER,PAY_REQ_GRANULARITIES,CREATE_DTTM,                    ");
			zeroPayRequestBuilder.append(" UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,REUSE_DUE_DT,INV_RECALC_FLG,ILM_DT,                      ");
			zeroPayRequestBuilder.append(" ILM_ARCH_SW,PAY_REQ_ID,MPG_TYPE, PAY_DETAIL_ID)                                                  ");
			zeroPayRequestBuilder.append(" (SELECT PER.BILL_ID,PER.PER_ID_NBR AS PER_ID_NBR,PER.CIS_DIVISION,                            ");
			zeroPayRequestBuilder.append(" :lineId AS LINE_ID,PER.ALT_BILL_ID,PER.BILL_DT,PER.ACCT_TYPE,                                 ");
			zeroPayRequestBuilder.append(" :creditPayType AS PAY_TYPE,BCHAR.ADHOC_CHAR_VAL AS BILL_AMT,PER.CURRENCY_CD,                  ");
			zeroPayRequestBuilder.append(" 'N' AS IS_IND_FLG,NULL AS SUB_STLMNT_LVL,NULL AS SUB_STLMNT_LVL_REF,PER.CR_NOTE_FR_BILL_ID,   ");
			zeroPayRequestBuilder.append(" 'N' AS IS_IMD_FIN_ADJ,'N' AS FIN_ADJ_MAN_NRT,'N' AS REL_RSRV_FLG,'N' AS REL_WAF_FLG,          ");
			zeroPayRequestBuilder.append(" 'N' AS FASTEST_ROUTE_INDICATOR,'N' AS CASE_IDENTIFIER,NULL AS PAY_REQ_GRANULARITIES,          ");
			zeroPayRequestBuilder.append(" PER.CREATE_DTTM,PER.CREATE_DTTM AS UPLOAD_DTTM,'Y' AS EXTRACT_FLG,NULL AS EXTRACT_DTTM,       ");
			zeroPayRequestBuilder.append(" NULL AS REUSE_DUE_DT,NULL AS INV_RECALC_FLG,PER.CREATE_DTTM AS ILM_DT,'Y' AS ILM_ARCH_SW,     ");
			zeroPayRequestBuilder.append(" CONCAT(PER.BILL_ID, :lineId) as PAY_REQ_ID,:mpgType AS MPG_TYPE,                              ");
			zeroPayRequestBuilder.append(" (NVL((SELECT MAX(PAY_DTL_ID) FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID = PER.BILL_ID                ");
			zeroPayRequestBuilder.append( " GROUP BY BILL_ID),0))                                        ");
			zeroPayRequestBuilder.append(" FROM CM_PAY_REQ_BILL_PER PER,CI_BILL_CHAR BCHAR                                               ");
			zeroPayRequestBuilder.append(" WHERE PER.BILL_ID = BCHAR.BILL_ID                                                             ");
			zeroPayRequestBuilder.append(" AND BCHAR.CHAR_TYPE_CD = 'BILL_AMT'                                                           ");
			zeroPayRequestBuilder.append(" AND BCHAR.ADHOC_CHAR_VAL = '0')                                                               ");

			addParams(paramsList, "mpgType", mpgType, "MPG_TYPE");
			addParams(paramsList, "lineId", lineId, "LINE_ID");
			addParams(paramsList, "creditPayType", creditPayType, "PAY_TYPE");
			executeQuery(zeroPayRequestBuilder, "Insert into cm_pay_req (bill_id, per_id_nbr...", paramsList);
			
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
					}
				}
				int count = preparedStatement.executeUpdate();
				logger.debug("Rows inserted by query " + message + " are: " + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table/s"));
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
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

		public static String getPayReqErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.getPayReqErrorMessage(messageNumber).getMessageText();
			if (errorInfo.contains("Text:") && errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"), errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after
		 * processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}
	}

	public static final class PayBillIdData implements Id {

		private static final long serialVersionUID = 1L;
		private String lowPayBillId;
		private String highPayBillId;

		public PayBillIdData(String lowPayBillId, String highPayBillId) {
			setLowPayBillId(lowPayBillId);
			setHighPayBillId(highPayBillId);
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

		public String getHighPayBillId() {
			return highPayBillId;
		}

		public void setHighPayBillId(String highPayBillId) {
			this.highPayBillId = highPayBillId;
		}

		public String getLowPayBillId() {
			return lowPayBillId;
		}

		public void setLowPayBillId(String lowPayBillId) {
			this.lowPayBillId = lowPayBillId;
		}
	}
}
