/*******************************************************************************
* FileName                   : CmFtMapping_Impl.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Jan 23, 2018 
* Version Number             : 0.1
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Jan 23, 2018        Preeti Tiwari        Initial version.
0.2 	 NA				Nov 12, 2018		Somya Sikka			 Added logic to insert REQUEST & OVERPAID entries in CM_BILL_PAYMENT_DTL table for bills having overpay adjustments 
0.3		 NA				Mar 04, 2019		Somya Sikka			 Overpay Check amended- Prod Incident Issue
*******************************************************************************/
package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.AdjustmentStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.customerClass.CustomerClassPostBillCompletionAlgorithmSpot;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.cm.domain.wp.batch.InboundPaymentsLookUp;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import java.util.Map;

/**
 * @author Preeti
 *
@AlgorithmComponent ()
 */
public class CmFtMapping_Impl extends
CmFtMapping_Gen implements
        CustomerClassPostBillCompletionAlgorithmSpot {
	
	private static final Logger logger = LoggerFactory.getLogger(CmFtMapping_Impl.class);
	private static final String OVERPAID = "OVERPAID";
	private static final String STATUS = "status";
	private static final String PAY_DETAIL_ID = "PAY_DETAIL_ID";
	private static final String PAY_TYPE = "PAY_TYPE";
	private static final String CURRENCY_CODE = "CURRENCY_CODE";
	private static final String STATUS_CODE = "STATUS_CODE";
	private Bill bill;
	private Bill_Id billId;	
	private InboundPaymentsLookUp payConfirmationLookup = null;
	public void invoke() {			
		
		billId=bill.getId();
		BigDecimal billAmount=BigDecimal.ZERO;
		
		if(payConfirmationLookup == null) {
			
			payConfirmationLookup = new InboundPaymentsLookUp();
		}
					
		List<String> siblingIdList=new ArrayList<String>();
		String ftTypeFlg="";
		
		StringBuilder sb = new StringBuilder();
		sb.append("from FinancialTransaction ft where ft.billId=:billId ");
		Query<QueryResultRow> query= createQuery(sb.toString(),"");
		query.bindId("billId",billId);
		query.addResult("currentAmount","ft.currentAmount");
		query.addResult("siblingId","ft.siblingId");
		query.addResult("financialTransactionType","ft.financialTransactionType");
		
		for (QueryResultRow resultRow : query.list()) {			
			billAmount=billAmount.add(resultRow.getMoney("currentAmount").getAmount());
			ftTypeFlg=CommonUtils.CheckNull(resultRow.getLookup("financialTransactionType", FinancialTransactionTypeLookup.class).toString());

			if (ftTypeFlg.equalsIgnoreCase("AD") ||	ftTypeFlg.equalsIgnoreCase("AX")){
				siblingIdList.add(resultRow.getString("siblingId"));
			}
		}		
		
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			
			stringBuilder = new StringBuilder();
			stringBuilder.append("INSERT INTO CI_BILL_CHAR " );
			stringBuilder.append("(BILL_ID,CHAR_TYPE_CD,SEQ_NUM,VERSION,CHAR_VAL, " );
			stringBuilder.append("ADHOC_CHAR_VAL,CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4, " );
			stringBuilder.append("CHAR_VAL_FK5,SRCH_CHAR_VAL) " );
			stringBuilder.append("VALUES (:billId, :charTypeCode,:seq,1,' ', " );
			stringBuilder.append(":billAmount,' ',' ',' ',' ',' ', :billAmount) ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindBigDecimal("billAmount", billAmount);
			preparedStatement.bindString("charTypeCode", "BILL_AMT", "CHAR_TYPE_CD");
			preparedStatement.bindBigInteger("seq", BigInteger.ONE);
			preparedStatement.bindString("billId", billId.getIdValue(), "BILL_ID");
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in invoke -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}		
		
		
		if (!siblingIdList.isEmpty()){
			for (int i = 0; i < siblingIdList.size(); i++) {				
				try {					
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CI_ADJ_CHAR " );
					stringBuilder.append("(ADJ_ID,CHAR_TYPE_CD,SEQ_NUM,VERSION,CHAR_VAL, " );
					stringBuilder.append("ADHOC_CHAR_VAL,CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4, " );
					stringBuilder.append("CHAR_VAL_FK5,SRCH_CHAR_VAL) " );
					stringBuilder.append("VALUES (:adjId, :charTypeCode,:seq,1,' ', " );
					stringBuilder.append(":billId,' ',' ',' ',' ',' ', :billId) ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("adjId", siblingIdList.get(i).trim(), "ADJ_ID");
					preparedStatement.bindString("charTypeCode", "BILL_ID", "CHAR_TYPE_CD");
					preparedStatement.bindBigInteger("seq", new BigInteger("2"));
					preparedStatement.bindString("billId", billId.getIdValue(), "SRCH_CHAR_VAL");
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Exception while inserting in CI_ADJ_CHAR", e);
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
					}
				}
			}
		}	
		checkOverpayment(billAmount);
	}
	
	/**
	 *This method will create REQUEST & OVERPAID entries in CM_BILL_PAYMENT_DTL table if OVERPAY adjustment is available for that bill 
	 * @param billAmount
	 */
	private void checkOverpayment(BigDecimal billAmount) {
		BigDecimal adjAmt = BigDecimal.ZERO;
		adjAmt = isAdjCreated();
		
		//Overpay Check amended- Prod Incident Issue
		//(bill_AMT>= 0 and A.ADJ_AMT< 0 AND bill_AMT<=ABS(A.ADJ_AMT) or (bill_AMT<= 0 and A.ADJ_AMT> 0 and ABS(bill_AMT)>A.ADJ_AMT)
		

/*		if (billAmount.compareTo(BigDecimal.ZERO)<=0 && adjAmt.compareTo(BigDecimal.ZERO)!=0)
		{*/
			BigDecimal billAmt = billAmount.subtract(adjAmt);
		if((billAmt.compareTo(BigDecimal.ZERO) >=0 && adjAmt.compareTo(BigDecimal.ZERO) < 0 && billAmt.compareTo(adjAmt.abs()) <=0) 
				|| (billAmt.compareTo(BigDecimal.ZERO) <=0 && adjAmt.compareTo(BigDecimal.ZERO) > 0 && adjAmt.compareTo(billAmt.abs()) >= 0))
		{
			String currencyCd = bill.getAccount().getCurrency().getId().getIdValue();
			String payType = (billAmt.compareTo(BigDecimal.ZERO)>=0) ? "DR" :"CR";
			String overpayFlg = (billAmount.compareTo(BigDecimal.ZERO) < 0) ? "Y" :null;
			String  statusCode ="REQUEST";
			Map<String,String>payDetailInfo = new HashMap<>();
			String payDetailId = getPayDetailId();
			payDetailInfo.put(PAY_DETAIL_ID,payDetailId);
			payDetailInfo.put(PAY_TYPE,payType);
			payDetailInfo.put(CURRENCY_CODE,currencyCd);
			payDetailInfo.put(STATUS_CODE,statusCode);

			insertPayDetail(payDetailInfo,billAmt, billAmt, BigDecimal.ZERO, billAmt,  null);

			payDetailId = getPayDetailId();
			payDetailInfo.put(PAY_DETAIL_ID,payDetailId);
			payDetailInfo.put(STATUS_CODE,OVERPAID);

			insertPayDetail(payDetailInfo,billAmt, billAmt, adjAmt, billAmount, overpayFlg);
			insertInPayDetailSnapshot( payDetailInfo,billAmt, billAmt, adjAmt, billAmount, overpayFlg);
		}
	}

	private String getPayDetailId() {
		PreparedStatement preparedStatement = null;
		String payDetailId = null;
		try {
			preparedStatement = createPreparedStatement(
					"SELECT PAY_DTL_ID_SQ.NEXTVAL as PAY_DETAIL_ID FROM DUAL", "");
			preparedStatement.setAutoclose(false);
			SQLResultRow sqlResultRow = preparedStatement.firstRow();
			if (notNull(sqlResultRow)) {
				payDetailId = sqlResultRow.getString(PAY_DETAIL_ID);
			}
		} catch (RuntimeException e) {
			logger.error("Inside getPayDetailId() method, Error -", e);
			throw new RunAbortedException(
					CustomMessageRepository.exceptionInExecution("While fetching PAY_DTL_ID - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside getPayDetailId() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		return payDetailId;
	}

	private void insertInPayDetailSnapshot(Map<String,String>payDetailInfo, BigDecimal billAmt, BigDecimal prevUnpaidAmt,
			BigDecimal tenderAmt, BigDecimal unpaidAmt, String overpayFlg) {
		PreparedStatement stmt = null;
		StringBuilder createSnapshotEntries = new StringBuilder();

		String  payDetailId = payDetailInfo.get(PAY_DETAIL_ID);
		String  payType = payDetailInfo.get(PAY_TYPE);
		String currencyCd = payDetailInfo.get(CURRENCY_CODE);
		String status = payDetailInfo.get(STATUS_CODE);
		try {
			createSnapshotEntries.append(" INSERT INTO CM_BILL_PAYMENT_DTL_SNAPSHOT  ");
			createSnapshotEntries.append(" (BILL_BALANCE_ID ,LATEST_PAY_DTL_ID ,LATEST_UPLOAD_DTTM ,PAY_DT ,BILL_DT,PARTY_ID , ");
			createSnapshotEntries.append(" LCP_DESCRIPTION ,LCP,ACCT_TYPE ,ACCOUNT_DESCRIPTION ,EXT_TRANSMIT_ID ,BILL_ID ,ALT_BILL_ID ,");
			createSnapshotEntries.append(" LINE_ID ,LINE_AMT ,PREV_UNPAID_AMT ,LATEST_PAY_AMT ,UNPAID_AMT ,BILL_AMOUNT, BILL_BALANCE , ");
			createSnapshotEntries.append(" CURRENCY_CD ,LATEST_STATUS ,PAY_TYPE ,ILM_DT  ,OVERPAID ,RECORD_STAT ,STATUS_UPD_DTTM ) ");
			createSnapshotEntries.append(" SELECT BILL_BALANCE_ID_SEQ.NEXTVAL,:payDetailId, :sysDttm, :payDate,TMP.BILL_DT,TMP.PARTY_ID, ");
			createSnapshotEntries.append(" TMP.LCP_DESCRIPTION,TMP.LCP,TMP.ACCT_TYPE, ");
			createSnapshotEntries.append(" CASE WHEN TMP.ACCT_TYPE = 'CHRG' THEN 'Charging'  ");
			createSnapshotEntries.append(" WHEN TMP.ACCT_TYPE = 'FUND' THEN 'Funding' ");
			createSnapshotEntries.append(" WHEN TMP.ACCT_TYPE = 'CHBK' THEN 'Chargebacks' ");
			createSnapshotEntries.append(" END AS ACCOUNT_DESCRIPTION, ");
			createSnapshotEntries.append(" :eventId,:billId, TMP.ALT_BILL_ID,:lineId, :lineAmt, :prevUnpaidAmt, :tenderAmt, :unpaidAmt, ");
			createSnapshotEntries.append(" nvl(TMP.BILL_AMT, :lineAmt), ");
			createSnapshotEntries.append(" CASE WHEN :lineAmt < 0 AND :unpaidAmt > 0 THEN 0 ");
			createSnapshotEntries.append(" WHEN :lineAmt > 0 AND :unpaidAmt < 0 THEN 0 ");
			createSnapshotEntries.append(" ELSE :unpaidAmt END AS BILL_BALANCE,");
			createSnapshotEntries.append(" :currencyCd,TRIM(:status),  ");
			createSnapshotEntries.append(" :payType,:sysDt ,:overpayFlg, :pending,  :sysDttm ");
			createSnapshotEntries.append(" FROM (SELECT DISTINCT BILL.BILL_ID, BILL.BILL_DT,BILL.ALT_BILL_ID, BCHAR.SRCH_CHAR_VAL AS BILL_AMT, ");
			createSnapshotEntries.append(" REFDATA.PARTY_ID,REFDATA.ACCT_TYPE,REFDATA.LCP, DIVL.DESCR AS LCP_DESCRIPTION ");
			createSnapshotEntries.append(" FROM CI_BILL BILL, VW_MERCH_ACCT_REF_DATA_RMB REFDATA, CI_CIS_DIVISION_L DIVL, ");
			createSnapshotEntries.append(" CI_CIS_DIV_CHAR DIVCHAR, CI_BILL_CHAR BCHAR ");
			createSnapshotEntries.append(" WHERE BILL.ACCT_ID = REFDATA.ACCT_ID ");
			createSnapshotEntries.append(" AND DIVCHAR.CHAR_VAL = REFDATA.LCP ");
			createSnapshotEntries.append(" AND DIVCHAR.CIS_DIVISION = DIVL.CIS_DIVISION ");
			createSnapshotEntries.append(" AND BILL.BILL_ID = BCHAR.BILL_ID ");
			createSnapshotEntries.append(" AND BCHAR.CHAR_TYPE_CD='BILL_AMT' ");
			createSnapshotEntries.append(" AND BILL.BILL_ID=:billId) TMP ");
			stmt = createPreparedStatement(createSnapshotEntries.toString(), "");

			stmt.bindDate("payDate", getSystemDateTime().getDate());
			stmt.bindId("billId", billId);
			stmt.bindDateTime("sysDttm", getSystemDateTime());
			stmt.bindString("lineId", "1", "LINE_ID");
			stmt.bindString("payDetailId", payDetailId, "LATEST_PAY_DTL_ID");
			stmt.bindBigDecimal("lineAmt", billAmt);
			stmt.bindString("payType", payType, PAY_TYPE);
			stmt.bindBigDecimal("prevUnpaidAmt", prevUnpaidAmt);
			stmt.bindBigDecimal("tenderAmt", tenderAmt);
			stmt.bindBigDecimal("unpaidAmt", unpaidAmt);
			stmt.bindString("currencyCd", currencyCd, "CURRENCY_CD");
			stmt.bindString(STATUS, status, "LATEST_STATUS");
			stmt.bindString("eventId", " ", "EXT_TRANSMIT_ID");
			stmt.bindString("overpayFlg", overpayFlg, OVERPAID);
			stmt.bindDate("sysDt", getSystemDateTime().getDate());
			stmt.bindString("pending", payConfirmationLookup.getPending(), "RECORD_STAT");
			stmt.executeUpdate();
		} catch (
				Exception e) {
			logger.error("Exception in insertInPayDetailSnapshot() method - ", e);
		} finally {
			if (stmt != null) {
				stmt.close();
			}
		}

	}

	private void insertPayDetail( Map<String,String>payDetailInfo, BigDecimal billAmt, BigDecimal prevUnpaidAmt,
			BigDecimal tenderAmt,
			BigDecimal unpaidAmt, String overpayFlg) {

		PreparedStatement stmt = null;
		StringBuilder sb = new StringBuilder();

		String  payDetailId = payDetailInfo.get(PAY_DETAIL_ID);
		String  payType = payDetailInfo.get(PAY_TYPE);
		String currencyCd = payDetailInfo.get(CURRENCY_CODE);
		String status = payDetailInfo.get(STATUS_CODE);
		try {
			sb.append(" INSERT INTO CM_BILL_PAYMENT_DTL (PAY_DTL_ID, UPLOAD_DTTM, PAY_DT, BILL_ID,LINE_ID,LINE_AMT,PAY_TYPE,PREV_UNPAID_AMT,"
					+ "PAY_AMT,UNPAID_AMT,CURRENCY_CD,");
			sb.append(" STATUS_CD, EXT_TRANSMIT_ID, STATUS_UPD_DTTM, ILM_DT, OVERPAID, RECORD_STAT) ");
			sb.append(" VALUES(:payDetailId, :sysDttm, :payDate, :billId, :lineId, :lineAmt, :payType, :prevUnpaidAmt, :tenderAmt, :unpaidAmt, "
					+ ":currencyCd, ");
			sb.append(" TRIM(:status), :eventId, :sysDttm, :sysDt, :overpayFlg, :pending) ");
			stmt = createPreparedStatement(sb.toString(), "");
			stmt.bindDate("payDate", getSystemDateTime().getDate());
			stmt.bindId("billId", billId);
			stmt.bindDateTime("sysDttm", getSystemDateTime());
			stmt.bindString("payDetailId", payDetailId, "PAY_DTL_ID");
			stmt.bindString("lineId", "1", "LINE_ID");
			stmt.bindBigDecimal("lineAmt", billAmt);
			stmt.bindString("payType", payType, PAY_TYPE);
			stmt.bindBigDecimal("prevUnpaidAmt", prevUnpaidAmt);
			stmt.bindBigDecimal("tenderAmt", tenderAmt);
			stmt.bindBigDecimal("unpaidAmt", unpaidAmt);
			stmt.bindString("currencyCd", currencyCd, "CURRENCY_CD");
			stmt.bindString(STATUS, status, "STATUS_CD");
			stmt.bindString("eventId", " ", "EXT_TRANSMIT_ID");
			stmt.bindString("overpayFlg", overpayFlg, OVERPAID);
			stmt.bindDate("sysDt", getSystemDateTime().getDate());
			stmt.bindString("pending", payConfirmationLookup.getPending(), "RECORD_STAT");
			stmt.executeUpdate();
		}catch (Exception e){
			logger.error("Exception occured in insertPayDetail() -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));

		}
		finally {
			if(stmt!=null)
			stmt.close();
		}
	}
	
	/**
	 * This method will check if OVERPAY adjustment is available for the given bill Id
	 * @return SUM(ADJ_AMT)
	 */
	private BigDecimal isAdjCreated() {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();	
		
		try {
			
			/********** Extracting Adjustment Details *************/
			stringBuilder.append("SELECT SUM(A.ADJ_AMT) ADJ_AMT " );
			stringBuilder.append(" FROM CI_ADJ A, CI_ADJ_CHAR C WHERE A.ADJ_ID = C.ADJ_ID " );
			stringBuilder.append(" AND C.CHAR_TYPE_CD = :charTypeCd " );
			stringBuilder.append(" AND C.SRCH_CHAR_VAL = :billId " );
			stringBuilder.append(" AND A.ADJ_TYPE_CD in (:chrg,:fund,:chbk) AND A.ADJ_STATUS_FLG=:status GROUP BY C.SRCH_CHAR_VAL");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindId("billId", billId);
			preparedStatement.bindString("chrg", payConfirmationLookup.getOverpayChrg(), "ADJ_TYPE_CD");
			preparedStatement.bindString("fund", payConfirmationLookup.getOverpayFund(), "ADJ_TYPE_CD");
			preparedStatement.bindString("chbk", payConfirmationLookup.getOverpayChbk(), "ADJ_TYPE_CD");
			preparedStatement.bindLookup(STATUS, AdjustmentStatusLookup.constants.FROZEN);
			preparedStatement.bindString("charTypeCd", "BILL_ID", "CHAR_TYPE_CD");
			preparedStatement.setAutoclose(false);

			SQLResultRow adjAmt = preparedStatement.firstRow();
			if(notNull(adjAmt)){
				return adjAmt.getBigDecimal("ADJ_AMT");
			} 
			
		} catch (Exception e) {
			logger.error("Exception occured in createPaymentStaging() ", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return BigDecimal.ZERO;	
	}
	
	public void setBill(Bill arg0) {
		bill=arg0;		
	}
}