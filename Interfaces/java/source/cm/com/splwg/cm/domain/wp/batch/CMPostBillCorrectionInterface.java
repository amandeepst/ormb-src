package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
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
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.cm.domain.wp.batch.InvoiceRecalcRelationshipInterface.InvoiceStagingData_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import java.util.Map;

/**
 * @author tutejaa105
 *
@BatchJob (modules = { "demo"})
 */
public class CMPostBillCorrectionInterface extends
		CMPostBillCorrectionInterface_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(CMPostBillCorrectionInterface.class);
	private static final CustomCreditNoteInterfaceLookUp customCreditNoteInterfaceLookUp = new CustomCreditNoteInterfaceLookUp();
	private static final String TAX_BILL_SEGMENT_ID = "taxBillSegmentId";
	public static final String OVERPAID = "OVERPAID";
	public static final String EXT_SOURCE_CD = "EXT_SOURCE_CD";
	public static final String UNPAID_AMT = "UNPAID_AMT";
	public static final String PREV_UNPAID_AMT = "PREV_UNPAID_AMT";
	public static final String LINE_AMT = "LINE_AMT";
	public static final String PAY_AMT = "PAY_AMT";
	public static final String PAY_TYPE = "PAY_TYPE";
	public static final String EXT_TRANSMIT_ID = "EXT_TRANSMIT_ID";
	public static final String LINE_ID = "LINE_ID";
	public static final String PAY_DTL_ID = "PAY_DTL_ID";






	@Override
	public JobWork getJobWork() {
		
		List<ThreadWorkUnit> billList= fetchInvRelationBills();
		resetSourceKeySQ();
		resetBillMapIdSq();
		resetPayDtlId();

		return createJobWorkForThreadWorkUnitList(billList);
		
	}

	private void resetPayDtlId() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_PAY_DTL_ID } ","");
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

	private void resetBillMapIdSq() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_SOURCE_KEY} ","");
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

	private void resetSourceKeySQ() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_BILL_MAP_ID} ","");
			preparedStatement.execute();

		} catch (RuntimeException e) {
			logger.error("Inside resetBillMapIdSq() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	private List<ThreadWorkUnit> fetchInvRelationBills() {
		
		Date processDate=getProcessDateTime().getDate();
		processDate=notNull(processDate) ? processDate : getSystemDateTime().getDate();
		Date endDate=processDate.addDays(1);		
		StringBuilder query=new StringBuilder();
		PreparedStatement preparedStatement=null;
		String parentBillId;
		String childBillId;
		String taxBillSegmentId;
		ThreadWorkUnit threadworkUnit;
		List<ThreadWorkUnit> threadWorkUnitList=new ArrayList<>();

		try{
			query.append("SELECT STG.PARENT_BILL_ID,STG.CHILD_BILL_ID,STG.TAX_BSEG_ID FROM CM_INV_RELATION_STG STG WHERE STG.UPLOAD_DTTM >= "
					+ ":processDate ");
			query.append("AND STG.UPLOAD_DTTM < :endDate AND NOT EXISTS (SELECT 1 FROM CM_INVOICE_DATA WHERE BILL_ID=STG.CHILD_BILL_ID) ");
			preparedStatement =createPreparedStatement(query.toString(), "");
			preparedStatement.bindDate("processDate",processDate);
			preparedStatement.bindDate("endDate",endDate);
			for (SQLResultRow resultSet : preparedStatement.list()) {

				parentBillId = CommonUtils.CheckNull(resultSet.getString("PARENT_BILL_ID"));
				childBillId = CommonUtils.CheckNull(resultSet.getString("CHILD_BILL_ID"));
				taxBillSegmentId = CommonUtils.CheckNull(resultSet.getString("TAX_BSEG_ID"));
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(new Bill_Id(parentBillId));
				threadworkUnit.addSupplementalData("childBillId",childBillId);
				threadworkUnit.addSupplementalData(TAX_BILL_SEGMENT_ID,taxBillSegmentId);

				threadworkUnit.addSupplementalData("systemDateTime",getSystemDateTime());
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				//*************************
			}
		} catch (Exception e) {
			logger.error("Inside catch block of fetchInvRelationBills() method-", e);
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
		

	public Class<CMPostBillCorrectionInterfaceWorker> getThreadWorkerClass() {
		return CMPostBillCorrectionInterfaceWorker.class;
	}

	public static class CMPostBillCorrectionInterfaceWorker extends
			CMPostBillCorrectionInterfaceWorker_Gen {



		/**
		 * initializeThreadWork() method contains logic that is invoked once per
		 * thread by the framework.
		 */
		@Override
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			super.initializeThreadWork(arg0);
			customCreditNoteInterfaceLookUp.setLookUpConstants();
		}

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			Bill_Id parentBillId= (Bill_Id) unit.getPrimaryId();
			Bill_Id childBillId= new Bill_Id((String) unit.getSupplementallData("childBillId"));
			String taxBillSegmentId = (String) unit.getSupplementallData(TAX_BILL_SEGMENT_ID);
			DateTime ilmDate= (DateTime) unit.getSupplementallData("systemDateTime");

			//Create Event Price Reversals for Cancellations.
			createEventPriceReversals(parentBillId,ilmDate);

			//Create Bill Id Map Records
			createBillIdMapReversals(parentBillId,ilmDate);

			//create Non Trans Price Reverals-
			createNonTransPriceReversals(parentBillId,ilmDate);

			//Create Invoice Cancellations-
			createInvoiceDataCancellations(parentBillId,childBillId,taxBillSegmentId,ilmDate);

			//Create Bill Payment Details Records For Cancellations to balance FT's in Accounting.
			createBillPaymentCancellations(parentBillId,childBillId,ilmDate);
			updateBillPaymentDetailSnapshot(parentBillId,childBillId,ilmDate);


			return true;
		}

		private void updateBillPaymentDetailSnapshot(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {

			Map<String, String> payDetailValues = fetchDataFromBillPaymentDetail(parentBillId);
			StringBuilder updateSnapshotTableEntries = new StringBuilder();
			ArrayList<ArrayList<Object>> paramsList = null;
			paramsList = new ArrayList<>();

			updateSnapshotTableEntries.append(" UPDATE CM_BILL_PAYMENT_DTL_SNAPSHOT SET LATEST_PAY_DTL_ID=:payDetailId, ");
			updateSnapshotTableEntries.append(" LATEST_UPLOAD_DTTM= SYSTIMESTAMP, PAY_DT=:payDate, EXT_TRANSMIT_ID=:extTransmitId, ");
			updateSnapshotTableEntries.append(" PAY_TYPE= :payType, LINE_AMT=:lineAmt, ");
			updateSnapshotTableEntries.append(" PREV_UNPAID_AMT=:prevUnpaidAmt, LATEST_PAY_AMT=:payAmt, UNPAID_AMT=:unpaidAmt, ");
			updateSnapshotTableEntries.append(" BILL_BALANCE= CASE WHEN :lineAmt < 0 AND :unpaidAmt > 0 THEN 0 ");
			updateSnapshotTableEntries.append(" WHEN :lineAmt > 0 AND :unpaidAmt < 0 THEN 0 ");
			updateSnapshotTableEntries.append(" ELSE :unpaidAmt END,LATEST_STATUS =:cancelled,EXT_SOURCE_CD=:extSrcCd,  ");
			updateSnapshotTableEntries.append(" OVERPAID= :overpayFlg, RECORD_STAT=:recStatus, STATUS_UPD_DTTM = SYSTIMESTAMP,  ");
			updateSnapshotTableEntries.append(" CREDIT_NOTE_ID=:childBillId WHERE BILL_ID =:parentBillId AND LINE_ID= :lineId");

			addParams(paramsList, "payDetailId", payDetailValues.get(PAY_DTL_ID), "LATEST_PAY_DTL_ID");
			addParams(paramsList, "lineId", payDetailValues.get(LINE_ID), LINE_ID);
			addParams(paramsList, "extTransmitId", payDetailValues.get(EXT_TRANSMIT_ID), EXT_TRANSMIT_ID);
			addParams(paramsList, "payType", payDetailValues.get(PAY_TYPE), PAY_TYPE);
			addParams(paramsList, "lineAmt", new BigDecimal(payDetailValues.get(LINE_AMT)), LINE_AMT);
			addParams(paramsList, "prevUnpaidAmt", new BigDecimal(payDetailValues.get(PREV_UNPAID_AMT)), PREV_UNPAID_AMT);
			addParams(paramsList, "payAmt", new BigDecimal(payDetailValues.get(PAY_AMT)), "LATEST_PAY_AMT");
			addParams(paramsList, "unpaidAmt", new BigDecimal(payDetailValues.get(UNPAID_AMT)), UNPAID_AMT);
			addParams(paramsList, "extSrcCd", payDetailValues.get(EXT_SOURCE_CD), EXT_SOURCE_CD);
			addParams(paramsList, "overpayFlg", payDetailValues.get(OVERPAID), OVERPAID);
			addParams(paramsList, "parentBillId", parentBillId, "BILL_ID");
			addParams(paramsList, "childBillId", childBillId, "BILL_ID");
			addParams(paramsList, "payDate", ilmDate.getDate(), "PAY_DT");
			addParams(paramsList, "cancelled", "CANCELLED", "STATUS_CD");
			addParams(paramsList, "recStatus", "PENDING", "RECORD_STAT");

			executeQuery(updateSnapshotTableEntries, "UPDATE CM_BILL_PAYMENT_DTL_SNAPSHOT ...", paramsList);

		}

		private Map<String, String> fetchDataFromBillPaymentDetail(Bill_Id parentBillId) {
			PreparedStatement preparedStatement = null;
			StringBuilder fetchRecords = new StringBuilder();
			Map<String, String> payDetail = new HashMap<>();
			try {
				fetchRecords.append(" SELECT PAY_DTL_ID, LINE_ID, EXT_TRANSMIT_ID,PAY_TYPE,LINE_AMT, ");
				fetchRecords.append(" PREV_UNPAID_AMT,PAY_AMT,UNPAID_AMT,EXT_SOURCE_CD,OVERPAID ");
				fetchRecords.append(" FROM CM_BILL_PAYMENT_DTL PT ");
				fetchRecords.append(" WHERE BILL_ID=:billID  ");
				fetchRecords.append(" AND PAY_DTL_ID=(SELECT MAX(PAY_DTL_ID) FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID=PT.BILL_ID) ");

				preparedStatement = createPreparedStatement(fetchRecords.toString(), "");
				preparedStatement.bindId("billID", parentBillId);
				preparedStatement.setAutoclose(false);
				SQLResultRow sqlResultRow = preparedStatement.firstRow();
				if (notNull(sqlResultRow)) {
					payDetail.put(PAY_DTL_ID, CommonUtils.CheckNull(sqlResultRow.getString(PAY_DTL_ID)));
					payDetail.put(LINE_ID, CommonUtils.CheckNull(sqlResultRow.getString(LINE_ID)));
					payDetail.put(EXT_TRANSMIT_ID, CommonUtils.CheckNull(sqlResultRow.getString(EXT_TRANSMIT_ID)));
					payDetail.put(PAY_TYPE, CommonUtils.CheckNull(sqlResultRow.getString(PAY_TYPE)));
					payDetail.put(LINE_AMT, CommonUtils.CheckNull(sqlResultRow.getBigDecimal(LINE_AMT).toString()));
					payDetail.put(PREV_UNPAID_AMT, CommonUtils.CheckNull(sqlResultRow.getBigDecimal(PREV_UNPAID_AMT).toString()));
					payDetail.put(PAY_AMT, CommonUtils.CheckNull(sqlResultRow.getBigDecimal(PAY_AMT).toString()));
					payDetail.put(UNPAID_AMT, CommonUtils.CheckNull(sqlResultRow.getBigDecimal(UNPAID_AMT).toString()));
					payDetail.put(EXT_SOURCE_CD, CommonUtils.CheckNull(sqlResultRow.getString(EXT_SOURCE_CD)));
					payDetail.put(OVERPAID, CommonUtils.CheckNull(sqlResultRow.getString(OVERPAID)));
				}

			} catch (Exception e) {
				logger.error("Inside catch block of fetchDataFromBillPaymentDetail() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return payDetail;
		}


		private void createBillPaymentCancellations(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder billPayQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			billPayQuery.append("INSERT INTO CM_BILL_PAYMENT_DTL (PAY_DTL_ID,UPLOAD_DTTM,PAY_DT,BILL_ID,LINE_ID, ");
			billPayQuery.append("LINE_AMT,PREV_UNPAID_AMT,PAY_AMT,UNPAID_AMT,CURRENCY_CD,STATUS_CD,PAY_TYPE,ILM_DT, ");
			billPayQuery.append("ILM_ARCH_SW,OVERPAID,RECORD_STAT,STATUS_UPD_DTTM,CREDIT_NOTE_ID) ");
			billPayQuery.append("SELECT PAY_DTL_ID_SQ.NEXTVAL,:ilmDate,:processDate,PT.BILL_ID,PT.LINE_ID, ");
			billPayQuery.append("PT.LINE_AMT,PT.UNPAID_AMT AS PREV_UNPAID_AMT,(-1 * PT.LINE_AMT) AS PAY_AMT, ");
			billPayQuery.append("(PT.UNPAID_AMT + (-1 * PT.LINE_AMT)) AS UNPAID_AMT,PT.CURRENCY_CD,:cancelled,PT.PAY_TYPE,:processDate,'Y', ");
			billPayQuery.append("(CASE WHEN (PT.UNPAID_AMT + (-1 * PT.LINE_AMT)) <> '0' THEN :overPaidFlg ELSE null END) AS OVERPAID, ");
			billPayQuery.append(":status,:ilmDate,:childBillId FROM CM_BILL_PAYMENT_DTL PT WHERE PT.BILL_ID=:parentBillId ");
			billPayQuery.append("AND PAY_DTL_ID=(SELECT MAX(PAY_DTL_ID) FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID=PT.BILL_ID) ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");
			addParams(paramsList, "processDate", ilmDate.getDate(),"PAY_DT");
			addParams(paramsList,"cancelled","CANCELLED","STATUS_CD");
			addParams(paramsList,"status","PENDING","RECORD_STAT");
			addParams(paramsList,"overPaidFlg","Y",OVERPAID);

			executeQuery(billPayQuery, "Insert into CM_BILL_PAYMENT_DTL (BILL_ID,....", paramsList);


		}

		private void createInvoiceDataCancellations(Bill_Id parentBillId, Bill_Id childBillId, String taxBillSegmentId, DateTime ilmDate) {

			markInvoiceData(parentBillId,taxBillSegmentId,ilmDate);
			markInvoiceDataLine(parentBillId,childBillId,ilmDate);
			markInvoiceDataBCL(parentBillId,childBillId,ilmDate);
			markInvoiceDataLineSVCQty(parentBillId,childBillId,ilmDate);
			markInvoiceDataLineRate(parentBillId,childBillId,ilmDate);
			markInvoiceDataAdj(parentBillId,childBillId,ilmDate);
			markInvoiceDataTax(parentBillId,childBillId,ilmDate);

		}

		private void markInvoiceDataTax(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {

			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataTaxQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataTaxQuery.append(" INSERT INTO CM_INV_DATA_TAX ");
			invoiceDataTaxQuery.append(" (BILL_ID, CALC_AMT, BASE_AMT ,TAX_STAT, TAX_STAT_DESCR, ");
			invoiceDataTaxQuery.append(" TAX_RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, REVERSE_CHRG_SW) ");
			invoiceDataTaxQuery.append(" SELECT :childBillId, ((-1)*D.CALC_AMT), ((-1)*D.BASE_AMT), ");
			invoiceDataTaxQuery.append(" D.TAX_STAT, D.TAX_STAT_DESCR, D.TAX_RATE, ");
			invoiceDataTaxQuery.append(" :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT, 'Y' AS ILM_ARCH_SW, D.REVERSE_CHRG_SW ");
			invoiceDataTaxQuery.append(" FROM CM_INV_DATA_TAX D WHERE D.BILL_ID =:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataTaxQuery, "Insert into CM_INV_DATA_TAX (BILL_ID,....", paramsList);
		}

		private void markInvoiceDataAdj(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataAdjQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataAdjQuery.append("INSERT INTO CM_INV_DATA_ADJ (BILL_ID, ADJ_ID, ADJ_AMT, ");
			invoiceDataAdjQuery.append("ADJ_TYPE_CD,CURRENCY_CD,DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
			invoiceDataAdjQuery.append("SELECT :childBillId, D.ADJ_ID, (ADJ_AMT*(-1)), ");
			invoiceDataAdjQuery.append("D.ADJ_TYPE_CD, D.CURRENCY_CD, D.DESCR, :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW ");
			invoiceDataAdjQuery.append("FROM CM_INV_DATA_ADJ D ");
			invoiceDataAdjQuery.append("WHERE D.BILL_ID=:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataAdjQuery, "Insert into CM_INV_DATA_ADJ (BILL_ID,....", paramsList);

		}

		private void markInvoiceDataLineRate(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataRateQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataRateQuery.append("INSERT INTO CM_INV_DATA_LN_RATE (BILL_ID,BSEG_ID,RATE_TP,RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW,PRICE_ASGN_ID) ");
			invoiceDataRateQuery.append("SELECT :childBillId, D.BSEG_ID, D.RATE_TP, D.RATE, :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW,D.PRICE_ASGN_ID ");
			invoiceDataRateQuery.append("FROM CM_INV_DATA_LN_RATE D ");
			invoiceDataRateQuery.append("WHERE D.BILL_ID=:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataRateQuery, "Insert into CM_INV_DATA_LN_RATE (BILL_ID,....", paramsList);
		}

		private void markInvoiceDataLineSVCQty(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataSVCQtyQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataSVCQtyQuery.append("INSERT INTO CM_INV_DATA_LN_SVC_QTY (BILL_ID,BSEG_ID,SQI_CD,SVC_QTY, ");
			invoiceDataSVCQtyQuery.append("SQI_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
			invoiceDataSVCQtyQuery.append("SELECT :childBillId, D.BSEG_ID, D.SQI_CD, D.SVC_QTY, ");
			invoiceDataSVCQtyQuery.append("D.SQI_DESCR, :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW ");
			invoiceDataSVCQtyQuery.append("FROM CM_INV_DATA_LN_SVC_QTY D ");
			invoiceDataSVCQtyQuery.append("WHERE D.BILL_ID=:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataSVCQtyQuery, "Insert into CM_INV_DATA_LN_SVC_QTY (BILL_ID,....", paramsList);
		}

		private void markInvoiceDataBCL(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {

			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataBCLQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataBCLQuery.append("INSERT INTO CM_INV_DATA_LN_BCL (BILL_ID,BSEG_ID,BCL_TYPE,BCL_DESCR,CALC_AMT,TAX_STAT, ");
			invoiceDataBCLQuery.append("TAX_STAT_DESCR,TAX_RATE,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
			invoiceDataBCLQuery.append("SELECT :childBillId, D.BSEG_ID, D.BCL_TYPE, D.BCL_DESCR, (D.CALC_AMT*(-1)), D.TAX_STAT, ");
			invoiceDataBCLQuery.append("D.TAX_STAT_DESCR, D.TAX_RATE, :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW ");
			invoiceDataBCLQuery.append("FROM CM_INV_DATA_LN_BCL D ");
			invoiceDataBCLQuery.append("WHERE D.BILL_ID=:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataBCLQuery, "Insert into CM_INV_DATA_LN_BCL (BILL_ID,....", paramsList);

		}

		private void markInvoiceDataLine(Bill_Id parentBillId, Bill_Id childBillId, DateTime ilmDate) {

			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataLineQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataLineQuery.append("INSERT INTO CM_INVOICE_DATA_LN (BILL_ID,BSEG_ID,BILLING_PARTY_ID,PRICE_CATEGORY, ");
			invoiceDataLineQuery.append("CURRENCY_CD,PRICE_CATEGORY_DESCR,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW,UDF_CHAR_25) ");
			invoiceDataLineQuery.append("SELECT :childBillId, D.BSEG_ID, D.BILLING_PARTY_ID, D.PRICE_CATEGORY, ");
			invoiceDataLineQuery.append("D.CURRENCY_CD, D.PRICE_CATEGORY_DESCR, :ilmDate, D.EXTRACT_FLG, D.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW, ");
			invoiceDataLineQuery.append("D.UDF_CHAR_25 FROM CM_INVOICE_DATA_LN D ");
			invoiceDataLineQuery.append("WHERE D.BILL_ID=:parentBillId ");

			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "childBillId", childBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");

			executeQuery(invoiceDataLineQuery, "Insert into CM_INVOICE_DATA_LN (BILL_ID,....", paramsList);
		}

		private void markInvoiceData(Bill_Id parentBillId,String taxBillSegmentId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder invoiceDataQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			invoiceDataQuery.append("INSERT INTO CM_INVOICE_DATA (BILL_ID,ALT_BILL_ID,BILLING_PARTY_ID, ");
			invoiceDataQuery.append("CIS_DIVISION,ACCT_TYPE,WPBU,BILL_DT,BILL_CYC_CD,WIN_START_DT,WIN_END_DT, ");
			invoiceDataQuery.append("CURRENCY_CD,CALC_AMT,MERCH_TAX_REG_NBR,WP_TAX_REG_NBR,TAX_AUTHORITY,TAX_TYPE, ");
			invoiceDataQuery.append("PREVIOUS_AMT,CR_NOTE_FR_BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW, ");
			invoiceDataQuery.append("TAX_BSEG_ID,TAX_RGME) ");
			invoiceDataQuery.append("SELECT C.BILL_ID, C.ALT_BILL_ID, A.BILLING_PARTY_ID, ");
			invoiceDataQuery.append("A.CIS_DIVISION, A.ACCT_TYPE, A.WPBU, C.BILL_DT, A.BILL_CYC_CD, A.WIN_START_DT, A.WIN_END_DT, ");
			invoiceDataQuery.append("A.CURRENCY_CD, (A.CALC_AMT*(-1)), A.MERCH_TAX_REG_NBR, A.WP_TAX_REG_NBR, A.TAX_AUTHORITY, A.TAX_TYPE, ");
			invoiceDataQuery.append("(A.PREVIOUS_AMT*(-1)), C.CR_NOTE_FR_BILL_ID, :ilmDate, A.EXTRACT_FLG, A.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW, ");
			invoiceDataQuery.append(":taxBillSegmentId,A.TAX_RGME ");
			invoiceDataQuery.append("FROM CM_INVOICE_DATA A, CI_BILL C ");
			invoiceDataQuery.append("WHERE A.BILL_ID=:parentBillId ");
			invoiceDataQuery.append("AND A.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, TAX_BILL_SEGMENT_ID, taxBillSegmentId,"TAX_BSEG_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");
			executeQuery(invoiceDataQuery, "Insert into CM_INVOICE_DATA (BILL_ID,....", paramsList);

		}

		private void createNonTransPriceReversals(Bill_Id parentBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder nonTransQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			nonTransQuery.append("INSERT INTO CM_NON_TRANS_PRICE ");
			nonTransQuery.append("(NON_EVENT_ID, PER_ID_NBR, ACCT_TYPE, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
			nonTransQuery.append("BILL_REFERENCE, INVOICEABLE_FLG, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW, ");
			nonTransQuery.append("SOURCE_KEY,SOURCE_TYPE,SOURCE_ID,CREDIT_NOTE_FLG) ");
			nonTransQuery.append("SELECT A.NON_EVENT_ID, A.PER_ID_NBR, A.ACCT_TYPE, A.PRICEITEM_CD, ");
			nonTransQuery.append("A.PRICE_CATEGORY, (A.CALC_AMT*(-1)), A.CURRENCY_CD, ");
			nonTransQuery.append("CONCAT('CR+',A.BILL_REFERENCE) AS BILL_REFERENCE, ");
			nonTransQuery.append("A.INVOICEABLE_FLG, :ilmDate, A.EXTRACT_FLG, A.EXTRACT_DTTM, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW, ");
			nonTransQuery.append("CM_SOURCE_KEY_SQ.NEXTVAL,A.SOURCE_TYPE,A.SOURCE_ID,'Y' ");
			nonTransQuery.append("FROM CM_NON_TRANS_PRICE A, CM_BILL_ID_MAP C ");
			nonTransQuery.append("WHERE C.BILL_ID=:parentBillId ");
			nonTransQuery.append("AND A.BILL_REFERENCE=C.BILL_REFERENCE ");
			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");
			executeQuery(nonTransQuery, "Insert into CM_NON_TRANS_PRICE (NON_EVENT_ID,....", paramsList);

		}

		private void createBillIdMapReversals(Bill_Id parentBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder billIdMapQuery=new StringBuilder();
			paramsList = new ArrayList<>();

			billIdMapQuery.append("INSERT INTO CM_BILL_ID_MAP ");
			billIdMapQuery.append("(BILL_ID, BILL_START_DT, ALT_BILL_ID, BILL_DT, CR_NOTE_FR_BILL_ID, ");
			billIdMapQuery.append("PER_ID_NBR, CIS_DIVISION, BILL_END_DT, ");
			billIdMapQuery.append("BILL_AMT, CURRENCY_CD, EVENT_TYPE_ID, EVENT_PROCESS_ID, ");
			billIdMapQuery.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, BILL_REFERENCE, ACCT_TYPE,ILM_DT,ILM_ARCH_SW, BILL_MAP_ID) ");
			billIdMapQuery.append("SELECT C.BILL_ID, C.BILL_DT AS BILL_START_DT, C.ALT_BILL_ID, ");
			billIdMapQuery.append("C.BILL_DT, C.CR_NOTE_FR_BILL_ID, A.PER_ID_NBR, ");
			billIdMapQuery.append("A.CIS_DIVISION, C.BILL_DT AS BILL_END_DT, ");
			billIdMapQuery.append("(A.BILL_AMT*(-1)), A.CURRENCY_CD, A.EVENT_TYPE_ID, A.EVENT_PROCESS_ID, ");
			billIdMapQuery.append(":ilmDate, A.EXTRACT_FLG, A.EXTRACT_DTTM, ");
			billIdMapQuery.append("CONCAT('CR+',A.BILL_REFERENCE) AS BILL_REFERENCE, A.ACCT_TYPE, :ilmDate AS ILM_DT,'Y' AS ILM_ARCH_SW, bill_id_map_seq.nextval ");
			billIdMapQuery.append("FROM CM_BILL_ID_MAP A, CI_BILL C ");
			billIdMapQuery.append("WHERE A.BILL_ID=:parentBillId ");
			billIdMapQuery.append("AND A.BILL_ID=C.CR_NOTE_FR_BILL_ID ");
			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");
			executeQuery(billIdMapQuery, "Insert into CM_BILL_ID_MAP (BILL_ID,....", paramsList);
		}



		private void createEventPriceReversals(Bill_Id parentBillId, DateTime ilmDate) {
			ArrayList<ArrayList<Object>> paramsList = null;
			StringBuilder eventPriceQuery=new StringBuilder();


			paramsList = new ArrayList<>();

			eventPriceQuery.append("INSERT INTO CM_EVT_PRICE ");
			eventPriceQuery.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
			eventPriceQuery.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
			eventPriceQuery.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT,ILM_DT,ILM_ARCH_SW, ");
			eventPriceQuery.append("GRANULARITY_REFERENCE,ORG_TXN_ID,SOURCE_KEY,SUMMARY_ID) ");
			eventPriceQuery.append("SELECT A.EVENT_ID, A.PRICEITEM_CD, A.PRICE_CATEGORY, (A.CALC_AMT*(-1)), ");
			eventPriceQuery.append("A.CURRENCY_CD, A.ACCT_TYPE, CONCAT('CR+',A.BILL_REFERENCE) AS BILL_REFERENCE, A.INVOICEABLE_FLG, ");
			eventPriceQuery.append("'Y', :ilmDate, A.EXTRACT_FLG, A.EXTRACT_DTTM, A.ACCRUED_DATE, ");
			eventPriceQuery.append("A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT, :ilmDate AS ILM_DT,'N' AS ILM_ARCH_SW, ");
			eventPriceQuery.append("A.GRANULARITY_REFERENCE,A.ORG_TXN_ID,CM_SOURCE_KEY_SQ.NEXTVAL,A.SUMMARY_ID ");
			eventPriceQuery.append("FROM CM_EVENT_PRICE A, CM_BILL_ID_MAP C ");
			eventPriceQuery.append("WHERE C.BILL_ID=:parentBillId ");
			eventPriceQuery.append("AND A.BILL_REFERENCE=C.BILL_REFERENCE ");
			addParams(paramsList, "ilmDate", ilmDate,"ILM_DT");
			addParams(paramsList, "parentBillId", parentBillId,"BILL_ID");


			executeQuery(eventPriceQuery, "Insert into CM_EVT_PRICE (event_id,....", paramsList);
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
				logger.error("Inside post bill Correction data interface method, Error -", e);
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


		protected final void removeSavepoint(String savePointName)
		{
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
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


	}

}
