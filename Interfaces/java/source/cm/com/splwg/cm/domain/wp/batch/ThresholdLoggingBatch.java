/*******************************************************************************
* FileName                   : ThresholdLoggingBatch.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Jun 15, 2015 
* Version Number             : 1.0
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Jun 15, 2015        Preeti Tiwari        Implemented all requirements for RFM1.
0.2      NA             Sep 11, 2017        Preeti Tiwari        Implemented batch failure requirement.
0.3      NA             Nov 01, 2017        Preeti Tiwari        Parameterized batch job for batch type and threshold percentage.
0.4      NA             Mar 14, 2018        Ankur Jain        	 PAM-15904 performance issue fix
0.5      NA             May 01, 2018        Ankur                NAP-26526 & NAP-26537 changed to complete_dttm   
0.6		 NAP-24110		May 23, 2018		Nitika Sharma		 Included ILM_ARCH_SW and ILM_DT    
0.7		 NAP-31860		Aug 24, 2018		Prerna Mehta		 Implemented batch failure requirement for category=INVDT	  
0.8      NA             Sep 13, 2018        Vienna Rom			 Running Totals fix          
0.9		 NAP-37531		Dec 13, 2018		Somya Sikka			 Added a date parameter for invoice data threshold check   
1.0		 NAP-37776		Dec 19, 2018		Somya Sikka			 Added a new category to check invoice exception count from CM_INV_DATA_EXCP table
1.1		 NAP-40127		Feb 05, 2019		Somya Sikka			 Added transaction map category to check missing billable charge count 
1.2		 NAP-42154		Mar 12, 2019		RIA			         Added RUN_TOT check to verify Running Totals calculation
1.3		 NAP-44963		May 22, 2019		Somya Sikka			 Added a check for TFM1 category
1.4      NAP-48808      Jul 17, 2019		RIA					 Change the threshold check in TFM1 and TFM2.
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.domain.common.message.MessageCategory_Id;
import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.base.domain.common.message.ServerMessageFactory;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.common.ServerMessage;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billDate, type = date)
 *            , @BatchJobSoftParameter (name = txnDt, type = date)
 *            , @BatchJobSoftParameter (name = batchType, required = true, type = string)
 *            , @BatchJobSoftParameter (name = threshold, type = string)
 *            , @BatchJobSoftParameter (name = category, type = string)})
 */

public class ThresholdLoggingBatch extends ThresholdLoggingBatch_Gen {

	public static final Logger logger = LoggerFactory.getLogger(ThresholdLoggingBatch.class);

	@Override
	public JobWork getJobWork() {
		// getParameters().getThreadCount()
		ThreadWorkUnit threadworkUnit = new ThreadWorkUnit();
		ArrayList<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadworkUnit.setPrimaryId(new BatchControl_Id(getParameters().getBatchControlId().getIdValue()));
		threadWorkUnitList.add(threadworkUnit);

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	public Class<ThresholdLoggingBatchWorker> getThreadWorkerClass() {
		return ThresholdLoggingBatchWorker.class;
	}

	public static class ThresholdLoggingBatchWorker extends ThresholdLoggingBatchWorker_Gen {

		private ArrayList<ArrayList<String>> loggingList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> loggingParameter = null;
		private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();
		private InboundPaymentsLookUp inboundPaymentsLookup = new InboundPaymentsLookUp();

		public ThresholdLoggingBatchWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by the
		 * framework per thread.
		 */
		@Override
		public void initializeThreadWork(boolean arg0) throws ThreadAbortedException, RunAbortedException {
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
		 * extracting error details from ORMB. It validates the extracted data and
		 * populates the target tables accordingly.
		 */
		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {

			String batchType = CommonUtils.CheckNull(getParameters().getBatchType()).trim();
			String criticalThreshold = CommonUtils.CheckNull(getParameters().getThreshold()).trim();
			String category = CommonUtils.CheckNull(getParameters().getCategory()).trim();

			logger.debug("Threshold Monitoring Batch: batchType-" + batchType + ", criticalThreshold-"
					+ criticalThreshold + ", category-" + category + ".");

			if (batchType.equalsIgnoreCase("W")) {
				loggingIntoStaging();
			} else if (batchType.equalsIgnoreCase("C")) {
				checkForCriticalThreshold();
			}

			return true;
		}

		private void getLoggingParameters() {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			try {
				stringBuilder.append(
						"SELECT TABLE_NAME, BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, THRESHOLD_COUNT FROM CM_THRESHOLD_LOG ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					loggingParameter = new ArrayList<String>();
					loggingParameter.add(0, resultSet.getString("TABLE_NAME"));
					loggingParameter.add(1, resultSet.getString("BATCH_CD"));
					loggingParameter.add(2, resultSet.getInteger("MESSAGE_CAT_NBR").toString());
					loggingParameter.add(3, resultSet.getInteger("MESSAGE_NBR").toString());
					loggingParameter.add(4, resultSet.getInteger("THRESHOLD_COUNT").toString());
					loggingList.add(loggingParameter);
					loggingParameter = null;
				}
			} catch (Exception e) {
				logger.error("Inside getLoggingParameters() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		private void loggingIntoStaging() {

			String tableName = "";
			String batchCode = "";
			String messageCatNbr = "";
			String messageNbr = "";
			String threshold = "";

			try {

				getLoggingParameters();

				// Call for-loop to run through the list derived above
				if (loggingList.size() > 0) {
					Iterator<ArrayList<String>> loggingListItr = loggingList.iterator();
					loggingList = null;
					ArrayList<String> rowList = null;
					while (loggingListItr.hasNext()) {

						rowList = (ArrayList<String>) loggingListItr.next();

						tableName = String.valueOf(rowList.get(0));
						batchCode = String.valueOf(rowList.get(1));
						messageCatNbr = String.valueOf(rowList.get(2));
						messageNbr = String.valueOf(rowList.get(3));
						threshold = String.valueOf(rowList.get(4));
						rowList = null;

						fetchErrorRec(tableName, batchCode, messageCatNbr, messageNbr, threshold);

					} // while loop
					loggingListItr = null;
				} // if list not empty
			} catch (Exception e) {
				logger.error("Inside getLoggingParameters() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			}
		}

		private void fetchErrorRec(String tableName, String batchCode, String messageCatNbr, String messageNbr,
				String threshold) {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			int count = 0;

			try {
				if ((CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CI_BSEG_EXCP"))) {

					stringBuilder.append(
							"insert into CM_THRESHOLD_STG (BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, THRESHOLD_COUNT, REC_NUM, UPLOAD_DTTM, ILM_DT, ILM_ARCH_SW) ");
					stringBuilder.append(
							"select T1.BATCH_CD,T1.MESSAGE_CAT_NBR,T1.MESSAGE_NBR,T1.EXP_MSG, T1.THRESHOLD_COUNT,rownum,SYSTIMESTAMP,TRUNC(SYSDATE),'Y' ");
					stringBuilder.append("from ");
					stringBuilder.append(
							"(select :batchCode AS BATCH_CD, a.MESSAGE_CAT_NBR,a.MESSAGE_NBR,a.EXP_MSG,COUNT(*) AS THRESHOLD_COUNT ");
					stringBuilder.append("from CI_BSEG_EXCP a where a.MESSAGE_CAT_NBR=:messageCatNbr ");
					stringBuilder.append("and a.MESSAGE_NBR=:messageNbr ");
					stringBuilder.append("and trunc(A.CRE_DTTM) = trunc(SYSDATE) ");
					stringBuilder.append("group by :batchCode, a.MESSAGE_CAT_NBR, a.MESSAGE_NBR, a.EXP_MSG ");
					stringBuilder.append("HAVING COUNT(*)>=:threshold) T1 ");

				} else if ((CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CI_TXN_HEADER"))) {

					stringBuilder.append(
							"insert into CM_THRESHOLD_STG (BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, THRESHOLD_COUNT, REC_NUM, UPLOAD_DTTM, ILM_DT, ILM_ARCH_SW) ");
					stringBuilder.append(
							"select T1.BATCH_CD,T1.MESSAGE_CAT_NBR,T1.MESSAGE_NBR,T1.MESSAGE_TEXT, T1.THRESHOLD_COUNT,rownum,SYSTIMESTAMP,TRUNC(SYSDATE),'Y' ");
					stringBuilder.append("from ");
					stringBuilder.append(
							"(select :batchCode AS BATCH_CD, a.MESSAGE_CAT_NBR,a.MESSAGE_NBR,C.MESSAGE_TEXT,COUNT(*) AS THRESHOLD_COUNT ");
					stringBuilder.append("from CI_TXN_HEADER a, CI_MSG_L C where a.MESSAGE_CAT_NBR=:messageCatNbr ");
					stringBuilder.append(
							"and a.MESSAGE_NBR=:messageNbr AND A.MESSAGE_CAT_NBR=C.MESSAGE_CAT_NBR and a.MESSAGE_NBR=C.MESSAGE_NBR ");
					stringBuilder.append("and trunc(A.UPLOAD_DTTM) = trunc(SYSDATE) ");
					stringBuilder.append("group by :batchCode, a.MESSAGE_CAT_NBR, a.MESSAGE_NBR, C.MESSAGE_TEXT ");
					stringBuilder.append("HAVING COUNT(*)>=:threshold) T1 ");

				} else if ((CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CI_TXN_DETAIL"))) {

					stringBuilder.append(
							"insert into CM_THRESHOLD_STG (BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, THRESHOLD_COUNT, REC_NUM, UPLOAD_DTTM, ILM_DT, ILM_ARCH_SW) ");
					stringBuilder.append(
							"select T1.BATCH_CD,T1.MESSAGE_CAT_NBR,T1.MESSAGE_NBR,T1.MESSAGE_TEXT, T1.THRESHOLD_COUNT,rownum,SYSTIMESTAMP,TRUNC(SYSDATE),'Y' ");
					stringBuilder.append("from ");
					stringBuilder.append(
							"(select :batchCode AS BATCH_CD, a.MESSAGE_CAT_NBR,a.MESSAGE_NBR,C.MESSAGE_TEXT,COUNT(*) AS THRESHOLD_COUNT ");
					stringBuilder.append("from CI_TXN_DETAIL a, CI_MSG_L C where a.MESSAGE_CAT_NBR=:messageCatNbr ");
					stringBuilder.append(
							"and a.MESSAGE_NBR=:messageNbr AND A.MESSAGE_CAT_NBR=C.MESSAGE_CAT_NBR and a.MESSAGE_NBR=C.MESSAGE_NBR ");
					stringBuilder.append("and trunc(A.CURR_SYS_PRCS_DT) = trunc(SYSDATE) ");
					stringBuilder.append("group by :batchCode, a.MESSAGE_CAT_NBR, a.MESSAGE_NBR, C.MESSAGE_TEXT ");
					stringBuilder.append("HAVING COUNT(*)>=:threshold) T1 ");

				} else if ((CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CM_PRICE_TYPE_ERR"))
						|| (CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CM_EVENT_PRICE_ERR"))
						|| (CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CM_PAY_REQ_ERR"))
						|| (CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CM_FT_GL_ASL_ERR"))
						|| (CommonUtils.CheckNull(tableName).trim().equalsIgnoreCase("CM_INV_DATA_ERR"))) {

					stringBuilder.append(
							"insert into CM_THRESHOLD_STG (BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, THRESHOLD_COUNT, REC_NUM, UPLOAD_DTTM, ILM_DT, ILM_ARCH_SW) ");
					stringBuilder.append(
							"select T1.BATCH_CD,T1.MESSAGE_CAT_NBR,T1.MESSAGE_NBR,T1.ERROR_INFO, T1.THRESHOLD_COUNT,rownum,SYSTIMESTAMP,TRUNC(SYSDATE),'Y' ");
					stringBuilder.append("from ");
					stringBuilder.append(
							"(select :batchCode AS BATCH_CD, a.MESSAGE_CAT_NBR,a.MESSAGE_NBR,a.ERROR_INFO,COUNT(*) AS THRESHOLD_COUNT ");
					stringBuilder.append("from " + tableName + " a where a.MESSAGE_CAT_NBR=:messageCatNbr ");
					stringBuilder.append("and a.MESSAGE_NBR=:messageNbr ");
					stringBuilder.append("group by :batchCode, a.MESSAGE_CAT_NBR, a.MESSAGE_NBR, a.ERROR_INFO ");
					stringBuilder.append("HAVING COUNT(*)>=:threshold) T1 ");

				} else {
					stringBuilder.append(
							"insert into CM_THRESHOLD_STG (BATCH_CD, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, THRESHOLD_COUNT, REC_NUM, UPLOAD_DTTM, ILM_DT, ILM_ARCH_SW) ");
					stringBuilder.append(
							"select :batchCode AS BATCH_CD,T1.MESSAGE_CAT_NBR,T1.MESSAGE_NBR,T1.ERROR_INFO, T1.THRESHOLD_COUNT,rownum,SYSTIMESTAMP,TRUNC(SYSDATE),'Y' ");
					stringBuilder.append("from ");
					stringBuilder.append(
							"(select a.MESSAGE_CAT_NBR,a.MESSAGE_NBR,a.ERROR_INFO,COUNT(*) AS THRESHOLD_COUNT ");
					stringBuilder.append("from " + tableName + " a where a.MESSAGE_CAT_NBR=:messageCatNbr ");
					stringBuilder.append("and a.MESSAGE_NBR=:messageNbr ");
					stringBuilder.append("and trunc(A.STATUS_UPD_DTTM) = trunc(SYSDATE) ");
					stringBuilder.append("group by a.MESSAGE_CAT_NBR, a.MESSAGE_NBR, a.ERROR_INFO ");
					stringBuilder.append("HAVING COUNT(*)>=:threshold) T1 ");
				}

				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("batchCode", batchCode.trim(), "BATCH_CD");
				preparedStatement.bindString("messageCatNbr", messageCatNbr.trim(), "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNbr", messageNbr.trim(), "MESSAGE_NBR");
				preparedStatement.bindString("threshold", threshold.trim(), "THRESHOLD_COUNT");

				count = preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Inside fetchErrorRec() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			// Print message into Logs
			if (count > 0) {
				MessageParameters params = new MessageParameters();
				ServerMessage sMessage = ServerMessageFactory.Factory.newInstance().createMessage(
						new MessageCategory_Id(new BigInteger(String.valueOf(messageCatNbr))),
						Integer.valueOf(messageNbr), params);
				logger.fatal("Threshold has been reached for the Batch Job-" + batchCode + ". Please check table-"
						+ tableName + "" + sMessage);
			}
		}

		public Date setWinMinDate() {
			PreparedStatement preparedStatement = null;
			Date winMinDate = null;
			preparedStatement = createPreparedStatement("SELECT MIN(win_start_dt) AS WIN_START_DT FROM ci_bill a  "
					+ " WHERE EXISTS (SELECT 1 FROM ci_bill_cyc_sch b "
					+ " WHERE :batchDt BETWEEN b.win_start_dt AND b.win_end_dt "
					+ " AND a.bill_cyc_cd  = b.bill_cyc_cd " + " AND a.win_start_dt = b.win_start_dt)  "
					+ " AND bill_stat_flg  ='P' AND A.bill_cyc_cd IS NOT NULL ", "");
			preparedStatement.bindDate("batchDt", getProcessDateTime().getDate());

			preparedStatement.setAutoclose(false);
			SQLResultRow row = preparedStatement.firstRow();
			if (notNull(row)) {
				winMinDate = row.getDate("WIN_START_DT");
			}

			if (notNull(preparedStatement)) {
				preparedStatement.close();
				preparedStatement = null;
			}
			return winMinDate;

		}

		private void checkForCriticalThreshold() {

			Date batchDate = getParameters().getBillDate();
			Date sysDt = getSystemDateTime().getDate();
			Date billDate = notNull(batchDate) ? batchDate : sysDt;
			Date bcCreDate = notNull(batchDate) ? batchDate : sysDt.addDays(-1);
			String criticalThreshold = CommonUtils.CheckNull(getParameters().getThreshold()).trim();
			String category = CommonUtils.CheckNull(getParameters().getCategory()).trim();
			Date txnDttm = notNull(getParameters().getTxnDt()) ? getParameters().getTxnDt() : sysDt.addDays(-1);
			BigInteger runningTotalBillErrorCountTfm = BigInteger.ZERO;
			BigInteger runningTotalBillErrorCountCu = BigInteger.ZERO;
			Date winMinDate = null;
			PreparedStatement preparedStatement = null;

			try {
				// checking valid number using BigDecimal constructor
				new BigDecimal(criticalThreshold);
			} catch (NumberFormatException e) {
				criticalThreshold = "0";
			}

			checkRecordInError(category, criticalThreshold);
			checkRecordInError2(category, criticalThreshold);
			checkRecordInError3(category, billDate);
			checkRecordInError4(category, criticalThreshold, billDate, bcCreDate);
			checkRecordInError5(category, txnDttm);
			checkRecordInError6(category);

			// NAP-42154- Running totals check
			if (category.equalsIgnoreCase("RUN_TOT")) {
				
				winMinDate = setWinMinDate();

				SQLResultRow checkRunningTotalsRowTfm = checkRunningTotalsTfm(winMinDate);
				if (notNull(checkRunningTotalsRowTfm)) {
					runningTotalBillErrorCountTfm = checkRunningTotalsRowTfm.getInteger("ERROR_COUNT_TFM");
				}

				SQLResultRow checkRunningTotalsRowCu = checkRunningTotalsCu(winMinDate);
				if (notNull(checkRunningTotalsRowCu)) {
					runningTotalBillErrorCountCu = checkRunningTotalsRowCu.getInteger("ERROR_COUNT_CUSTOM");
				}

			}
			// NAP-42154- Running totals check
			if (runningTotalBillErrorCountTfm.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(runningTotalBillErrorCountTfm
						+ " Bills have wrong Running Totals Calculation- " + category + "."));
			}

			if (runningTotalBillErrorCountCu.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(runningTotalBillErrorCountCu
						+ " Bills have wrong Running Totals Calculation- " + category + "."));
			}

		}

		private void checkRecordInError(String category, String criticalThreshold) {
			PreparedStatement preparedStatement = null;
			BigDecimal errorCount = BigDecimal.ZERO;
			BigDecimal headerCount = BigDecimal.ZERO;
			BigDecimal totalCount = BigDecimal.ZERO;
			BigDecimal threshold = new BigDecimal(criticalThreshold);

			if (category.equalsIgnoreCase("TFM1")) {
				preparedStatement = createPreparedStatement(
						"SELECT COUNT(A.TXN_HEADER_ID) AS ERROR_COUNT,"
								+ " (COUNT(B.TXN_HEADER_ID)*(:criticalThreshold)) AS TOTAL_COUNT"
								+ " FROM (SELECT * FROM CI_TXN_HEADER A WHERE MESSAGE_NBR !=0) A , CI_TXN_HEADER B"
								+ " WHERE A.TXN_HEADER_ID(+)=B.TXN_HEADER_ID" + " AND trunc(B.UPLOAD_DTTM) = :sysDt",
						"");
				preparedStatement.bindString("criticalThreshold", criticalThreshold, "BATCH_CD");
				preparedStatement.bindDate("sysDt", getSystemDateTime().getDate());
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					errorCount = row.getBigDecimal("ERROR_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
					headerCount = errorCount;
					totalCount = row.getBigDecimal("TOTAL_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
				}

			} else if (category.equalsIgnoreCase("TFM2")) {
				preparedStatement = createPreparedStatement(
						"select (COUNT(TXN_DETAIL_ID)*(:criticalThreshold)) AS TOTAL_COUNT from CI_TXN_DETAIL_STG where LAST_SYS_PRCS_DT >= trunc(SYSDATE) " +
								"AND TXN_UPLOAD_DTTM >= trunc(SYSDATE)",
						"");
				preparedStatement.bindString("criticalThreshold", criticalThreshold, "BATCH_CD");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					totalCount = row.getBigDecimal("TOTAL_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
				}

				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}

				row = null;

				preparedStatement = createPreparedStatement(
						"select COUNT(stg.TXN_DETAIL_ID) AS ERROR_COUNT from CI_TXN_DETAIL_STG stg where stg.LAST_SYS_PRCS_DT >= trunc(SYSDATE) " +
								"and stg.MESSAGE_CAT_NBR !=0 AND stg.TXN_UPLOAD_DTTM >= trunc(SYSDATE)",
						"");
				preparedStatement.setAutoclose(false);
				row = preparedStatement.firstRow();
				if (notNull(row)) {
					errorCount = row.getBigDecimal("ERROR_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
				}
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			if (errorCount.compareTo(threshold) > 0) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Critical Threshold has been reached: Category-"
								+ category + "." + " " + errorCount + " transactions are in error"));
			}

			if (headerCount.compareTo(BigDecimal.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						" Category-" + category + "." + " " + headerCount + " transactions are in error"));
			}

		}

		private void checkRecordInError2(String category, String criticalThreshold) {
			PreparedStatement preparedStatement = null;
			BigDecimal paymentAmt = BigDecimal.ZERO;
			BigDecimal billAmt = BigDecimal.ZERO;
			BigDecimal difference = BigDecimal.ZERO;

			if (category.equalsIgnoreCase("PAYMENT")) {
				preparedStatement = createPreparedStatement(
						" WITH FT_AMT AS " + " (SELECT SUM(A.CUR_AMT) AS BILL FROM CI_FT A, CI_BILL B  "
								+ " WHERE TRUNC(B.COMPLETE_DTTM)=TRUNC(SYSDATE) AND A.BILL_ID = B.BILL_ID  "
								+ " AND NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL WHERE STATUS_CD = 'OVERPAID' "
								+ " AND BILL_ID = B.BILL_ID)), " + " PAY_AMT AS "
								+ " (SELECT SUM(C.BILL_AMT) AS PAYMENT_REQUEST FROM CM_PAY_REQ C "
								+ " WHERE TRUNC(C.UPLOAD_DTTM)=TRUNC(SYSDATE) " + " GROUP BY TRUNC(C.UPLOAD_DTTM)) "
								+ " SELECT T1.PAYMENT_REQUEST, T2.BILL, T1.PAYMENT_REQUEST-T2.BILL AS DIFFERENCE"
								+ " FROM PAY_AMT T1, FT_AMT T2",
						"");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					paymentAmt = row.getBigDecimal("PAYMENT_REQUEST").setScale(2, BigDecimal.ROUND_HALF_UP);
					billAmt = row.getBigDecimal("BILL").setScale(2, BigDecimal.ROUND_HALF_UP);
					difference = row.getBigDecimal("DIFFERENCE").setScale(2, BigDecimal.ROUND_HALF_UP);
				}

			} else if (category.equalsIgnoreCase("INVOICE")) {
				preparedStatement = createPreparedStatement(" SELECT (SUM(INVOICE_AMT) + SUM(ADJ_AMT))  AS INVOICE_AMT , "
						+ " SUM(PAYMENT_REQUEST) AS PAYMENT_REQUEST, "
						+ " (SUM(INVOICE_AMT) + SUM(ADJ_AMT))- SUM(PAYMENT_REQUEST) AS DIFFERENCE FROM "
						+ " (SELECT inv.CALC_AMT AS INVOICE_AMT, "
						+ " (SELECT SUM(A.BILL_AMT) FROM CM_PAY_REQ a "
						+ " WHERE a.bill_id = inv.bill_id GROUP BY a.bill_id) AS PAYMENT_REQUEST, "
						+ " nvl((SELECT adj.adj_amt FROM CM_INV_DATA_ADJ adj "
						+ " WHERE adj.bill_id=inv.bill_id AND adj_type_cd in ('MOVRPAYC')),0) as ADJ_AMT "
						+ " FROM CM_INVOICE_DATA inv WHERE inv.ILM_DT >= TRUNC(SYSDATE)) ", "");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					paymentAmt = row.getBigDecimal("INVOICE_AMT").setScale(2, BigDecimal.ROUND_HALF_UP);
					billAmt = row.getBigDecimal("PAYMENT_REQUEST").setScale(2, BigDecimal.ROUND_HALF_UP);
					difference = row.getBigDecimal("DIFFERENCE").setScale(2, BigDecimal.ROUND_HALF_UP);
				}
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			if (difference.abs().compareTo(new BigDecimal(criticalThreshold)) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						"Critical Threshold has been reached: Category-" + category + ". Pay Req/Invoice Amount: "
								+ paymentAmt + ", Bill/Pay Req: " + billAmt + ", Difference: " + difference + "."));
			}

		}

		private void checkRecordInError4(String category, String criticalThreshold, Date billDate, Date bcCreDate) {
			PreparedStatement preparedStatement = null;
			BigInteger excpCnt = BigInteger.ZERO;
			BigInteger missingBchgCnt = BigInteger.ZERO;
			BigDecimal errorCount = BigDecimal.ZERO;
			BigDecimal totalCount = BigDecimal.ZERO;

			if (category.equalsIgnoreCase("BILLING")) {
				preparedStatement = createPreparedStatement("SELECT COUNT(A.BSEG_ID) as ERROR_COUNT,"
						+ " (COUNT(B.BSEG_ID)*(:criticalThreshold)) as TOTAL_COUNT" + " FROM CI_BSEG_EXCP A, CI_BSEG B"
						+ " WHERE A.BSEG_ID(+)=B.BSEG_ID" + " AND trunc(B.CRE_DTTM) = trunc(SYSDATE)", "");
				preparedStatement.bindString("criticalThreshold", criticalThreshold, "BATCH_CD");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					errorCount = row.getBigDecimal("ERROR_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
					totalCount = row.getBigDecimal("TOTAL_COUNT").setScale(2, BigDecimal.ROUND_HALF_UP);
				}

			}
			// NAP-37776
			else if ("INVEXCP".equalsIgnoreCase(category)) {
				preparedStatement = createPreparedStatement(
						"select count(1) EXCP_CNT from CM_INV_DATA_EXCP where bill_dt = :billDt", "ExceptionCountSql");
				preparedStatement.bindDate("billDt", billDate.addDays(-1));
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					excpCnt = row.getInteger("EXCP_CNT");
				}
			}
			// NAP-40127
			else if ("TXMAP".equalsIgnoreCase(category)) {
				preparedStatement = createPreparedStatement(
						"SELECT COUNT(1) BCHG_CNT FROM ci_bill_chg WHERE cre_dt=:inputSysdate"
								+ " AND feed_source_flg = :tfm AND billable_chg_stat=:active AND billable_chg_id NOT IN "
								+ " (SELECT billable_chg_id FROM cm_txn_attributes_map)",
						"missingBillableChgCountSql");
				preparedStatement.bindDate("inputSysdate", bcCreDate);
				preparedStatement.bindString("tfm", "TFM ", "feed_source_flg");
				preparedStatement.bindString("active", inboundPaymentsLookup.getInitPevtStatus().trim(),
						"billable_chg_stat");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					missingBchgCnt = row.getInteger("BCHG_CNT");
				}
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			if (errorCount.compareTo(totalCount) > 0) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Critical Threshold has been reached: Category-"
								+ category + "." + " " + errorCount + " transactions are in error"));
			}

			// NAP-37776 - Job should fail if exception count in CM_INV_DATA_EXCP table has
			// exceeded critical threshold
			if (excpCnt.compareTo(new BigInteger(criticalThreshold)) > 0) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Critical Threshold has been reached: Category-"
								+ category + ". Exception Count: " + excpCnt + "."));
			}

			// NAP-40127- Job should fail if billable charge doesn't exist in
			// CM_TXN_ATTRIBUTES_MAP
			if (missingBchgCnt.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						missingBchgCnt + " billable charges are missing from CM_TXN_ATTRIBUTES_MAP table: Category-"
								+ category + "."));
			}
		}

		private void checkRecordInError3(String category, Date billDate) {
			PreparedStatement preparedStatement = null;
			BigInteger missingBillCnt = BigInteger.ZERO;
			BigInteger missingTaxLineCnt = BigInteger.ZERO;

			if (category.equalsIgnoreCase("INVDT")) {
				// NAP-37531
				preparedStatement = createPreparedStatement(
						"SELECT count(distinct BILL.BILL_ID) as BILL_CNT FROM CI_BILL BILL "
								+ " WHERE BILL.BILL_STAT_FLG = :billStatusFlag AND BILL.BILL_DT = :billDate AND NOT EXISTS "
								+ " (SELECT 1 FROM CM_INVOICE_DATA INVDT WHERE INVDT.BILL_ID = BILL.BILL_ID)",
						"MissingBillCntSql");
				preparedStatement.bindDate("billDate", billDate.addDays(-1));
				preparedStatement.bindLookup("billStatusFlag", BillStatusLookup.constants.COMPLETE);
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();				
				if (notNull(row)) {
					missingBillCnt = row.getInteger("BILL_CNT");
				}
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
				row = null;

				preparedStatement = createPreparedStatement(
						  "SELECT COUNT(1) AS TAX_COUNT "
						  + " FROM CM_INV_DATA_EXCP A, "
						  + " CM_INV_DATA_ERR B "
						+ " WHERE A.BILL_ID = B.BILL_ID "
						+ " AND A.BILL_DT = :billDt "
						+ " AND MESSAGE_NBR = '4305' ", "MissingTaxLineSql");
				preparedStatement.bindDate("billDt", billDate.addDays(-1));
				preparedStatement.setAutoclose(false);
				row = preparedStatement.firstRow();				
				if (notNull(row)) {
					missingTaxLineCnt = row.getInteger("TAX_COUNT");
				}
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			// Job should fail if a completed bill ID exists with no record in invoice data
			// & tax line is missing on any bill of charging accounts
			if (missingBillCnt.compareTo(BigInteger.ZERO) > 0 && missingTaxLineCnt.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(missingBillCnt + " bills are not captured for invoice detail and "
								+ missingTaxLineCnt + " bills have missing tax lines : Category-" + category + "."));
			}

			// Job should fail if a completed bill ID exists with no record in invoice data
			if (missingBillCnt.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						missingBillCnt + " bills are not captured for invoice detail: Category- " + category + "."));
			}

			// Job should fail if tax line is missing on any bill of charging accounts
			if (missingTaxLineCnt.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						missingTaxLineCnt + " bills have missing tax lines : Category-" + category + "."));
			}
		}

		// Job should fail if any funding or chargeBack or Charge events mssing in
		// CM_EVENT_PRICE
		public void jobFailOnMissingInfo(BigInteger missingFundChargeBackEvents,
				BigDecimal missingFundChargeBackEventstxnAmt, BigInteger missingChargeEvents,
				BigDecimal missingChagreEventTxnAmt, String category) {
			if (missingFundChargeBackEvents.compareTo(BigInteger.ZERO) > 0
					&& missingChargeEvents.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(missingFundChargeBackEvents
						+ " Fund and ChargeBack Events with TXN_AMT - " + missingFundChargeBackEventstxnAmt + " and "
						+ missingChargeEvents + "  Charge Events with TXN_AMT - " + missingChagreEventTxnAmt
						+ " missing from CM_EVENT_PRICE : Category-" + category + "."));
			}

			// Job should fail if any funding or chargeBack events mssing in CM_EVENT_PRICE
			if (missingFundChargeBackEvents.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(missingFundChargeBackEvents
						+ " Fund and ChargeBack Events  with TXN_AMT - " + missingFundChargeBackEventstxnAmt
						+ " missing from CM_EVENT_PRICE : Category- " + category + "."));
			}

			// Job should fail if any Charge events mssing in CM_EVENT_PRICE
			if (missingChargeEvents.compareTo(BigInteger.ZERO) > 0) {
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(
						missingChargeEvents + " Charge Events with TXN_AMT - " + missingChagreEventTxnAmt
								+ " missing from CM_EVENT_PRICE : Category-" + category + "."));
			}
		}

		private void checkRecordInError5(String category, Date txnDttm) {
			PreparedStatement preparedStatement = null;
			BigInteger missingFundChargeBackEvents = BigInteger.ZERO;
			BigDecimal missingFundChargeBackEventstxnAmt = BigDecimal.ZERO;
			BigInteger missingChargeEvents = BigInteger.ZERO;
			BigDecimal missingChagreEventTxnAmt = BigDecimal.ZERO;

			if (category.toUpperCase() == "EVTPRC") {

				StringBuilder sb1 = new StringBuilder();
				sb1.append(" SELECT count(1) AS EVT_CNT, ");
				sb1.append(" sum(txn_amt) AS TXN_AMT ");
				sb1.append(" FROM ci_txn_detail_stg a ");
				sb1.append(" WHERE txn_dttm      >= :txnDttm ");
				sb1.append(" AND bo_status_cd     = 'COMP' ");
				sb1.append(" AND txn_rec_type_cd IN ( ");
				sb1.append(" 'CHRG_NAPR_DCCRB_MMAN', ");
				sb1.append(" 'CHDCCMANUAL', ");
				sb1.append(" 'CHRG_PRM_AQR_FND', ");
				sb1.append(" 'FDISPUTE_RESPONSE', ");
				sb1.append(" 'FUNDINGREJECTIONS', ");
				sb1.append(" 'FUNDINGREVERSALS', ");
				sb1.append(" 'CQPR_FUND') ");
				sb1.append(" AND NOT EXISTS ");
				sb1.append(" (SELECT 1 ");
				sb1.append(" FROM cm_event_price b ");
				sb1.append(" WHERE a.ext_txn_nbr = b.event_id ");
				sb1.append(" AND b.acct_type     IN ('FUND', 'CHBK') ");
				sb1.append(" AND ilm_dt         >= :txnDttm) ");

				preparedStatement = createPreparedStatement(sb1.toString(), "missingFundChargeBackEvents");
				preparedStatement.bindDate("txnDttm", txnDttm);
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					missingFundChargeBackEvents = row.getInteger("EVT_CNT");
					missingFundChargeBackEventstxnAmt = row.getBigDecimal("TXN_AMT");
				}
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
				row = null;

				StringBuilder sb2 = new StringBuilder();
				sb2.append(" SELECT count(1) AS EVT_CNT, ");
				sb2.append(" sum(txn_amt) AS TXN_AMT ");
				sb2.append(" FROM ci_txn_detail_stg a ");
				sb2.append(" WHERE txn_dttm      >= :txnDttm ");
				sb2.append(" AND bo_status_cd     = 'COMP' ");
				sb2.append(" AND (txn_rec_type_cd NOT IN  ( ");
				sb2.append(" 'FDISPUTE_RESPONSE', ");
				sb2.append(" 'FUNDINGREJECTIONS', ");
				sb2.append(" 'FUNDINGREVERSALS', ");
				sb2.append(" 'CQPR_FUND', ");
				sb2.append(" 'CQPRREP', ");
				sb2.append(" 'AUTHINTEGRITY') ");
				sb2.append(" OR txn_rec_type_cd = 'AUTHINTEGRITY' ");
				sb2.append(" AND udf_char_10 = 'ICPP') ");
				sb2.append(" AND NOT EXISTS ");
				sb2.append(" (SELECT 1 ");
				sb2.append(" FROM cm_event_price b ");
				sb2.append(" WHERE a.ext_txn_nbr = b.event_id ");
				sb2.append(" AND b.acct_type     = 'CHRG' ");
				sb2.append(" AND ilm_dt         >= :txnDttm)  ");
				preparedStatement = createPreparedStatement(sb2.toString(), "missingChargeEvents");
				preparedStatement.bindDate("txnDttm", txnDttm);
				preparedStatement.setAutoclose(false);
				row = preparedStatement.firstRow();
				if (notNull(row)) {
					missingChargeEvents = row.getInteger("EVT_CNT");
					missingChagreEventTxnAmt = row.getBigDecimal("TXN_AMT");
				}
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

		
			jobFailOnMissingInfo(missingFundChargeBackEvents, missingFundChargeBackEventstxnAmt, missingChargeEvents,
					missingChagreEventTxnAmt, category);
		}
		
		private StringBuilder prepareQuery() {
			StringBuilder sb = new StringBuilder();
			sb.append(" WITH yday_bal AS ");
			sb.append(" (SELECT DISTINCT per_id_nbr ");
			sb.append(" ||'_' ");
			sb.append(" ||acct_nbr ");
			sb.append(" ||'_' ");
			sb.append(" ||currency_cd AS acct_nbr, ");
			sb.append(" cis_division, acctbal ");
			sb.append(" FROM cm_merch_ledger_acct ");
			sb.append(" WHERE ilm_dt  = TRUNC(sysdate - 1) AND acct_nbr IN ('FUND', 'CHRG', 'CHBK')), ");
			sb.append(" today_bal AS ");
			sb.append("   (SELECT DISTINCT per_id_nbr ");
			sb.append(" ||'_' ");
			sb.append(" ||acct_nbr ");
			sb.append(" ||'_' ");
			sb.append(" ||currency_cd AS acct_nbr, ");
			sb.append(" cis_division,  acctbal ");
			sb.append(" FROM cm_merch_ledger_acct ");
			sb.append(" WHERE ilm_dt = TRUNC(sysdate)), ");
			sb.append(" bill_amt AS ");
			sb.append(" (SELECT c.acct_nbr, d.cis_division, ");
			sb.append(" SUM(b.adhoc_char_val) AS bill_amt ");
			sb.append(" FROM ci_bill a, ");
			sb.append(" ci_bill_char b, ci_acct_nbr c, ci_acct d ");
			sb.append(" WHERE a.bill_id= b.bill_id ");
			sb.append(" AND a.acct_id  =c.acct_id ");
			sb.append(" AND c.acct_id  = d.acct_id ");
			sb.append(" AND c.acct_nbr_type_cd = 'C1_F_ANO' ");
			sb.append(" AND b.char_type_cd = 'BILL_AMT' ");
			sb.append(" AND complete_dttm BETWEEN TRUNC(sysdate) AND TRUNC(sysdate+1) ");
			sb.append(" AND a.ilm_dt >= sysdate -36 ");
			sb.append(" GROUP BY c.acct_nbr, d.cis_division ), ");
			sb.append(" billed_op AS ");
			sb.append(" (SELECT c.acct_nbr, b.cis_division, ");
			sb.append(" SUM(adj_amt) AS overpayment_amt ");
			sb.append(" FROM ci_adj a, ");
			sb.append(" ci_sa b, ci_acct_nbr c, ");
			sb.append(" ci_adj_char d, ci_bill e ");
			sb.append(" WHERE a.sa_id = b.sa_id ");
			sb.append(" AND b.acct_id = c.acct_id ");
			sb.append(" AND a.adj_id  = d.adj_id ");
			sb.append(" AND e.bill_id = d.srch_char_val ");
			sb.append(" AND e.complete_dttm BETWEEN TRUNC(sysdate) AND TRUNC(sysdate+1) ");
			sb.append(" AND c.acct_nbr_type_cd = 'C1_F_ANO' ");
			sb.append(" AND adj_type_cd   IN ('MOVRPAYC', 'MOVRPAYF', 'MOVRPYCB') ");
			sb.append(" GROUP BY c.acct_nbr, b.cis_division), ");
			sb.append(" payment_amt AS");
			sb.append(" (SELECT acct_nbr, d.cis_division, ");
			sb.append(" SUM(pay_amt) AS pay_amt ");
			sb.append(" FROM cm_bill_payment_dtl a, ");
			sb.append(" ci_bill b, ci_acct_nbr c, ci_acct d ");
			sb.append(" WHERE a.bill_id = b.bill_id ");
			sb.append(" AND b.acct_id = c.acct_id ");
			sb.append(" AND c.acct_id = d.acct_id ");
			sb.append(" AND c.acct_nbr_type_cd = 'C1_F_ANO' ");
			sb.append(" AND a.ilm_dt = TRUNC(sysdate) AND status_cd <> 'OVERPAID' ");
			sb.append(" GROUP BY acct_nbr, cis_division), waf_res_amt AS ");
			sb.append(" (SELECT c.acct_nbr, b.cis_division, ");
			sb.append(" SUM(adj_amt) AS waf_res_amt ");
			sb.append(" FROM ci_adj a, ci_sa b, ci_acct_nbr c ");
			sb.append(" WHERE a.sa_id = b.sa_id ");
			sb.append(" AND b.acct_id = c.acct_id ");
			sb.append(" AND c.acct_nbr_type_cd = 'C1_F_ANO' ");
			sb.append(
					" AND adj_type_cd IN ('WAFBLD', 'WAFUTIL', 'WAFREL', 'DYNUTIL', 'STATUTIL', 'ANEGPROC', 'WAFMREL', 'STATMREL', 'DYNMREL', 'STATBLD') ");
			sb.append(" AND b.sa_type_cd IN ('WAF', 'DRES', 'SRES') ");
			sb.append(" AND ilm_Dt = TRUNC(sysdate) ");
			sb.append(" GROUP BY c.acct_nbr, b.cis_division) ");
			sb.append(" SELECT ybal.acct_nbr, ybal.cis_division, ");
			sb.append(" ybal.acctbal AS yday_bal, ");
			sb.append(" NVL(bill.bill_amt,0) AS bill_amt, ");
			sb.append(" NVL(pay.pay_amt,0)   AS pay_amt, ");
			sb.append(" NVL(waf_res.waf_res_amt,0)   AS waf_res_amt, ");
			sb.append(" NVL(bop.overpayment_amt,0)   AS billed_op, ");
			sb.append(" tbal.acctbal AS today_bal, ");
			sb.append(
					" ybal.acctbal + NVL(bill.bill_amt,0)+ NVL(pay.pay_amt,0) + NVL(waf_res.waf_res_amt,0) - NVL(bop.overpayment_amt,0) - tbal.acctbal AS diff ");
			sb.append(" FROM yday_bal ybal, ");
			sb.append(" today_bal tbal, bill_amt bill, ");
			sb.append(" payment_amt pay, waf_res_amt waf_res, billed_op bop ");
			sb.append(" WHERE ybal.acct_nbr = tbal.acct_nbr ");
			sb.append(" AND ybal.cis_division = tbal.cis_division ");
			sb.append(" AND ybal.acct_nbr = bill.acct_nbr (+) ");
			sb.append(" AND ybal.cis_division = bill.cis_division (+) ");
			sb.append(" AND ybal.acct_nbr = pay.acct_nbr (+) ");
			sb.append(" AND ybal.cis_division = pay.cis_division (+) ");
			sb.append(" AND ybal.acct_nbr = waf_res.acct_nbr (+) ");
			sb.append(" AND ybal.cis_division = waf_res.cis_division (+) ");
			sb.append(" AND ybal.acct_nbr = bop.acct_nbr (+) ");
			sb.append(" AND ybal.cis_division = bop.cis_division (+) ");
			sb.append(
					" AND ybal.acctbal + NVL(bill.bill_amt,0)+ NVL(pay.pay_amt,0)+ NVL(waf_res.waf_res_amt,0) - NVL(bop.overpayment_amt,0) <> tbal.acctbal ");
			return sb;
		}

		private void forErrorLogging(PreparedStatement preparedStatement) {
			String acctNbr = null;
			String cisDivision = null;
			BigDecimal difference = BigDecimal.ZERO;
			BigDecimal yDayBal;
			BigDecimal tDayBal;
			BigDecimal billAmt;
			BigDecimal payAmt;
			BigDecimal wafResAmt;
			BigDecimal billedOverPayAmt;
			
			if (!preparedStatement.list().isEmpty()) {
				for (SQLResultRow row : preparedStatement.list()) {
					acctNbr = row.getString("ACCT_NBR");
					cisDivision = row.getString("CIS_DIVISION");
					yDayBal = row.getBigDecimal("YDAY_BAL");
					tDayBal = row.getBigDecimal("TODAY_BAL");
					billAmt = row.getBigDecimal("BILL_AMT");
					payAmt = row.getBigDecimal("PAY_AMT");
					wafResAmt = row.getBigDecimal("WAF_RES_AMT");
					billedOverPayAmt = row.getBigDecimal("BILLED_OP");
					difference = row.getBigDecimal("DIFF");

					if (difference.abs().compareTo(BigDecimal.ZERO) > 0) {
						logger.error("Balance Substantiation failed for Merchant - " + acctNbr + ". Details: Div - "
								+ cisDivision + " Yesterday's Balance - " + yDayBal + " Today's Balance - "
								+ tDayBal + " Difference - " + difference + " Today's Bill Amount - " + billAmt
								+ " Today's Paymnet Amount - " + payAmt + " Today's WAF Res Amount - " + wafResAmt
								+ " Bill Over Payment - " + billedOverPayAmt);
					}
				}
			}

		}
		private void checkRecordInError6(String category) {
			PreparedStatement preparedStatement = null;
			BigDecimal difference = BigDecimal.ZERO;

			if (category.toUpperCase() == "EODBAL") {
				StringBuilder sb = new StringBuilder();
				sb = prepareQuery();
				preparedStatement = createPreparedStatement(sb.toString(), "");
				preparedStatement.setAutoclose(false);
				
				forErrorLogging(preparedStatement);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

			if (difference.abs().compareTo(BigDecimal.ZERO) > 0) {
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Critical Threshold has been reached: Category-"
								+ category + ". Balance Substantiation failed for listed Merchants in error log."));
			}

		}

		private SQLResultRow checkRunningTotalsTfm(Date winMinDate) {
			PreparedStatement preparedStatement = null;
			StringBuilder sb = null;
			SQLResultRow row = null;
			try {
				sb = new StringBuilder();
				sb.append("SELECT COUNT(*) as ERROR_COUNT_TFM ");
				sb.append("FROM ( ");
				sb.append("SELECT D.BILL_ID, D.SRCH_CHAR_VAL,  D.RUNNING_TOT_AMT, SUM(B.CALC_AMT) AS BSEG_AMT ");
				sb.append(" FROM CM_TXN_ATTRIBUTES_MAP A, CI_BSEG_CALC B, CI_BSEG C, ");
				sb.append(" (SELECT BILL_ID,  SRCH_CHAR_VAL, SUM(CHAR_VAL_FK2) AS RUNNING_TOT_AMT ");
				sb.append("  FROM CI_BILL_CHAR  WHERE CHAR_TYPE_CD = 'RUN_TOT' AND BILL_ID   IN  ");
				sb.append(" (SELECT BILL_ID    FROM CI_BILL    WHERE BILL_STAT_FLG = 'P' ");
				sb.append(" AND WIN_START_DT   >= :startDate    )   GROUP BY BILL_ID, SRCH_CHAR_VAL) D ");
				sb.append(" WHERE A.BILLABLE_CHG_ID = B.BILLABLE_CHG_ID ");
				sb.append(" AND B.BSEG_ID = C.BSEG_ID AND C.BILL_ID  = D.BILL_ID ");
				sb.append(" AND A.CHILD_PRODUCT     = D.SRCH_CHAR_VAL");
				sb.append(" AND C.WIN_START_DT      >= :startDate");
				sb.append(
						" GROUP BY D.BILL_ID,  D.SRCH_CHAR_VAL, D.RUNNING_TOT_AMT HAVING D.RUNNING_TOT_AMT <> SUM(B.CALC_AMT)) ");

				preparedStatement = createPreparedStatement(sb.toString(), "");
				preparedStatement.bindDate("startDate", winMinDate);
				preparedStatement.setAutoclose(false);
				row = preparedStatement.firstRow();

			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return row;
		}

		private SQLResultRow checkRunningTotalsCu(Date winMinDate) {
			PreparedStatement preparedStatement = null;
			StringBuilder sb = null;
			SQLResultRow row = null;
			try {

				sb = new StringBuilder();
				sb.append("SELECT COUNT(*) as ERROR_COUNT_CUSTOM ");
				sb.append("FROM ( ");
				sb.append("SELECT D.BILL_ID, D.SRCH_CHAR_VAL,  D.RUNNING_TOT_AMT, SUM(B.CALC_AMT) AS BSEG_AMT ");
				sb.append(" FROM CI_BILL_CHG A, CI_BSEG_CALC B, CI_BSEG C, ");
				sb.append(" (SELECT BILL_ID,  SRCH_CHAR_VAL, SUM(CHAR_VAL_FK2) AS RUNNING_TOT_AMT ");
				sb.append("  FROM CI_BILL_CHAR  WHERE CHAR_TYPE_CD = 'RUN_TOT' AND BILL_ID   IN  ");
				sb.append(" (SELECT BILL_ID    FROM CI_BILL    WHERE BILL_STAT_FLG = 'P' ");
				sb.append(" AND WIN_START_DT   >= :startDate    )   GROUP BY BILL_ID, SRCH_CHAR_VAL) D ");
				sb.append(" WHERE A.BILLABLE_CHG_ID = B.BILLABLE_CHG_ID ");
				sb.append(" AND B.BSEG_ID = C.BSEG_ID AND C.BILL_ID  = D.BILL_ID ");
				sb.append(" AND A.PRICEITEM_CD     = D.SRCH_CHAR_VAL");
				sb.append(" AND C.WIN_START_DT      >= :startDate");
				sb.append(
						" GROUP BY D.BILL_ID,  D.SRCH_CHAR_VAL, D.RUNNING_TOT_AMT HAVING D.RUNNING_TOT_AMT <> SUM(B.CALC_AMT)) ");

				preparedStatement = createPreparedStatement(sb.toString(), "");
				preparedStatement.bindDate("startDate", winMinDate);
				preparedStatement.setAutoclose(false);
				row = preparedStatement.firstRow();

			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return row;
		}

		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after
		 * processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			super.finalizeThreadWork();
		}
	}
}