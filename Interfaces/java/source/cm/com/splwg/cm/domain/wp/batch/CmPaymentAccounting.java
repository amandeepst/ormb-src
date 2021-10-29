/*******************************************************************************
 * FileName                   : CmPaymentAccounting.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Aug 02, 2018
 * Version Number             : 0.2
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             02-Aug-2018         Amandeep          Implemented all requirement as per Design.
 0.2      NAP-37745      12-Dec-2018         Somya Sikka       Added logic to stamp correct sibling id for AD/AX FT entries.
 0.3	  NA			 09-Jan-2019		 Somya Sikka	   Transfer Adjustment will be created to handle residual overpaid amount
 0.4      US480270       16-Jan-2021         Vikalp            Over payment changes implementations
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.security.user.User_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.api.lookup.GlDistributionStatusLookup;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.generalLedgerDivision.GeneralLedgerDivision_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 *
 @BatchJob (multiThreaded = true, rerunnable = true,
  *     modules = {"demo"},
  *     softParameters = { @BatchJobSoftParameter (name = batchReRunDate, type = date)})
 */
public class CmPaymentAccounting extends CmPaymentAccounting_Gen {

	/**
	 * @author tutejaa105
	 *
	 */

	public static final Logger logger = LoggerFactory.getLogger(CmPaymentAccounting.class);

	private InboundPaymentsLookUp payConfirmationLookup = new InboundPaymentsLookUp();
	public static final String OVERPAID="OVERPAID";
	public static final String PAYTYPE = "PAY_TYPE";
	public static final String CURRENCYCD = "CURRENCY_CD";
	public static final String CISDIVISION = "CIS_DIVISION";
	public static final String SIBLINGID = "SIBLING_ID";
	public static final String SIBLINGID2 = "siblingId";
	public static final String PAYDTLID= "payDtlId";
	public static final String ACCTTYPE = "ACCT_TYPE";
	public static final String BILLID = "BILL_ID";
	public static final String BILLID2 = "billId";
	public static final String PAYTYPE2 = "payType";

	@Override
	public JobWork getJobWork() {

		List<ThreadWorkUnit> threadWorkUnitList =new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList=getPayDetailId();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

		// TODO Auto-generated method stub

	}

	private List<ThreadWorkUnit> getPayDetailId() {
		// TODO Auto-generated method stub
		PreparedStatement ps=null;
		StringBuilder sb=new StringBuilder();
		Date batchReRunDate=getParameters().getBatchReRunDate();
		BigDecimal payDetailId=null;
		Payment_Dtl_Id payDtlId;
		ThreadWorkUnit twu;
		List<ThreadWorkUnit> threadWorkUnitList=new ArrayList<ThreadWorkUnit>();
		BigInteger envId= InstallationHelper.getEnvironmentId();
		String billId;
		String lineId;

		try{
			sb.append("SELECT DISTINCT DT.BILL_ID,DT.LINE_ID FROM CM_BILL_PAYMENT_DTL DT WHERE DT.STATUS_CD <> 'REQUEST' AND DT.RECORD_STAT=:pend ");
			sb.append(" AND ");

			if(notNull(batchReRunDate)){
				sb.append("DT.PAY_DT=:parmDate");
			}
			else{
				sb.append("ILM_DT >= :date1 AND ILM_DT < :date2");
			}

			ps=createPreparedStatement(sb.toString(), "");

			if(notNull(batchReRunDate)){
				ps.bindDate("parmDate", batchReRunDate);
			}
			else{
				ps.bindDate("date1", getSystemDateTime().getDate());
				ps.bindDate("date2", getSystemDateTime().getDate().addDays(1));

			}

			ps.bindString("pend", payConfirmationLookup.getPending(), "RECORD_STAT");

			for(SQLResultRow rs:ps.list()){
				billId=rs.getString(BILLID);
				lineId=rs.getString("LINE_ID");

				payDtlId=new Payment_Dtl_Id(billId,lineId);
				twu=new ThreadWorkUnit();
				twu.setPrimaryId(payDtlId);
				twu.addSupplementalData("envId", envId);
				threadWorkUnitList.add(twu);
			}
		}

		catch(Exception e){
			logger.error("Exception occurred in getStagingData()" , e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		}
		finally{
			if(ps != null){
				ps.close();
				ps=null;
			}
		}
		return threadWorkUnitList;
	}


	public Class<CmPaymentAccountingWorker> getThreadWorkerClass() {
		return CmPaymentAccountingWorker.class;
	}

	public static class CmPaymentAccountingWorker extends
			CmPaymentAccountingWorker_Gen {

		private InboundPaymentsLookUp payConfirmationLookup = new InboundPaymentsLookUp();

		String saId = "";
		String cisDivision = "";
		BigInteger envId = null;
		private ArrayList<ArrayList<String>> updateCustomerStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> eachCustomerStatusList = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public String updateCreatePayStatus(BigDecimal payAmt,String status,PayDetail_Id payDtl,String overpayment) {

			String  createPayStatus= "false";
			String overpayRelease = payDtl.overPaymentRelease;
			if(payAmt.compareTo(BigDecimal.ZERO) ==0 ){
				createPayStatus="true";
			}
			else {
				if (status.equals("REJECTED") || "WRITE_OFF_REV".equals(status)) {
					createPayStatus =createFT(payDtl,FinancialTransactionTypeLookup
							.constants.PAY_CANCELLATION,payDtl.getPayAmt(), overpayRelease);
				} else if (status.equals(OVERPAID)) {
					createPayStatus = payOverpaidFT(payDtl);
				}

				else if ("CANCELLED".equals(status) && overpayment == null) {
					createPayStatus = "true";
				} else {
					createPayStatus = payConfirmationFT(payDtl);
				}
			}

			return 	createPayStatus;

		}
		public void functionToupdateCmBillDueDtTbl(PayDetail_Id payDtl,BigDecimal unPaidAmt) {

			if(unPaidAmt.compareTo(BigDecimal.ZERO) ==0){
				/********** Setting merchant as balanced in Due date table *************/

				updateCmBillDueDtTbl(payDtl,"Y");
			}
			else{
				updateCmBillDueDtTbl(payDtl,"N");
			}
		}

		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			Payment_Dtl_Id payDtlId=(Payment_Dtl_Id) unit.getPrimaryId();
			envId = (BigInteger) unit.getSupplementallData("envId");
			PayDetail_Id payDtl =null;
			String billId=payDtlId.getBillId();
			String lineId=payDtlId.getLineId();
			BigDecimal lineAmt=null;
			String payType=null;
			BigDecimal prevUnpaidAmt=null;
			BigDecimal payAmt=null;
			BigDecimal unPaidAmt=null;
			String status=null;
			String eventId=null;
			BigDecimal payDetailId=null;
			Date payDt=null;
			String currencyCode=null;
			String overpayment=null;
			String extSourceCd=null;
			String creditNoteId;
			String overPaymentRelease=null;
			Date batchReRunDate=getParameters().getBatchReRunDate();


			//Selecting Records for Payment Detail Table
			List<SQLResultRow> list= fetchPayDetailRecords(billId,lineId,batchReRunDate);

			for(SQLResultRow rs : list){

				payDetailId=rs.getBigDecimal("PAY_DTL_ID");
				lineAmt=rs.getBigDecimal("LINE_AMT");
				payType=rs.getString(PAYTYPE);
				prevUnpaidAmt=rs.getBigDecimal("PREV_UNPAID_AMT");
				payAmt=rs.getBigDecimal("PAY_AMT");
				unPaidAmt=rs.getBigDecimal("UNPAID_AMT");
				status=rs.getString("STATUS_CD").trim();
				eventId=rs.getString("EXT_TRANSMIT_ID");
				payDt=rs.getDate("PAY_DT");
				currencyCode=rs.getString(CURRENCYCD);
				extSourceCd =rs.getString("EXT_SOURCE_CD");
				overpayment=rs.getString(OVERPAID);
				creditNoteId=rs.getString("CREDIT_NOTE_ID");
				overPaymentRelease = rs.getString("OVERPAYMENT_RELEASE");

				payDtl = new PayDetail_Id(billId, payDetailId, lineId, status, payType, eventId,
						currencyCode, overpayment, prevUnpaidAmt, payAmt, unPaidAmt, lineAmt,
						payDt,extSourceCd,creditNoteId, overPaymentRelease);

				removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
				setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));

				boolean extractBillDetail=false;

				extractBillDetail = extractBillDetails(billId,lineId);

				if(!extractBillDetail) {
					return logError(billId, lineId,
							payConfirmationLookup.getError(), String.valueOf(CustomMessages.MESSAGE_CATEGORY),
							String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS),
							getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS)));
				}


				String createPayStatus = updateCreatePayStatus(payAmt,status,payDtl,overpayment);
				functionToupdateCmBillDueDtTbl(payDtl, unPaidAmt);
				if (CommonUtils.CheckNull(createPayStatus).trim().startsWith("false")) {
					String[] returnStatusArray = createPayStatus.split("~");
					if(returnStatusArray[1].contains("Text:")){
						returnStatusArray[1] = returnStatusArray[1].replace("Text:", "");
					}

					return logError(
							billId, lineId,
							payConfirmationLookup.getError(),
							returnStatusArray[2].trim(), returnStatusArray[3].trim(),
							returnStatusArray[1].trim());
				} else {
					updateBillPayDtl(lineId,
							billId,
							payConfirmationLookup.getCompleted(), "0", "0"," ");
					updateBillPaymentDtlSnapshotEntries(payDtl,
							payConfirmationLookup.getCompleted(), "0", "0"," ");
				}
			}
			return true;
		}

		public List<SQLResultRow> fetchPayDetailRecords(String billId, String lineId, Date batchReRunDate) {
			StringBuilder sb=new StringBuilder();
			PreparedStatement ps=null;
			List<SQLResultRow> sqlList= new ArrayList<>();

			sb.append(" SELECT PAY_DTL_ID,LINE_AMT,PAY_TYPE,PREV_UNPAID_AMT,PAY_AMT,UNPAID_AMT,");
			sb.append("	OVERPAID,STATUS_CD,EXT_TRANSMIT_ID,PAY_DT,CURRENCY_CD,EXT_SOURCE_CD,CREDIT_NOTE_ID, OVERPAYMENT_RELEASE ");
			sb.append(" FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID=trim(:billId) AND LINE_ID=:lineId AND STATUS_CD <> 'REQUEST' AND RECORD_STAT=:pend ");
			sb.append(" AND ");

			if(notNull(batchReRunDate)){
				sb.append(" PAY_DT=:parmDate" );
			}
			else{
				sb.append("ILM_DT >= :date1 AND ILM_DT < :date2");
			}
			sb.append(" ORDER BY PAY_DTL_ID");

			try {
				ps = createPreparedStatement(sb.toString(), "");

				ps.bindString(BILLID2, billId,BILLID);
				ps.bindString("lineId", lineId, "LINE_ID");

				if (notNull(batchReRunDate)) {
					ps.bindDate("parmDate", batchReRunDate);
				} else {
					ps.bindDate("date1", getSystemDateTime().getDate());
					ps.bindDate("date2", getSystemDateTime().getDate().addDays(1));
				}
				ps.bindString("pend", payConfirmationLookup.getPending(), "RECORD_STAT");
				sqlList = ps.list();
			}

			catch(Exception e){
				logger.error("Exception in execute Work Unit"+e);
			}
			finally{
				if(ps!=null){
					ps.close();
					ps=null;
				}
			}

			return sqlList;
		}

		/**
		 * @param payDtl
		 * @return
		 */

		private String payOverpaidFT(PayDetail_Id payDtl) {
			// TODO Auto-generated method stub

			BigDecimal unpaidAmt=payDtl.getUnPaidAmt();
			String status = null;
			if(unpaidAmt.compareTo(BigDecimal.ZERO) != 0){

				status = createPayReq(payDtl.getBillId(),payDtl.getLineId(),unpaidAmt,
						payDtl.getPayDetailId());
				if(CommonUtils.CheckNull(status).trim().startsWith("false")){
					return status;
				}
			}
			else{
				status="true";
			}

			return status;
		}


		/**
		 * @param errorMessage
		 * @return
		 */
		public Map<String, String> errorList(String errorMessage) {
			Map<String, String> errorMap = new HashMap<String, String>();
			String errorMessageNumber = "";
			String errorMessageCategory = "";
			if (errorMessage.contains("Number:")) {
				errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));
				errorMap.put("Number", errorMessageNumber);
			}
			if (errorMessage.contains("Category:")) {
				errorMessageCategory = errorMessage.substring(errorMessage
						.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));
				errorMap.put("Category", errorMessageCategory);
			}
			if (errorMessage.contains("Text:")
					&& errorMessage.contains("Description:")) {
				errorMessage = errorMessage
						.substring(errorMessage.indexOf("Text:"), errorMessage
								.indexOf("Description:"));
			}
			if (errorMessage.length() > 250) {
				errorMessage = errorMessage.substring(0, 250);
				errorMap.put("Text", errorMessage);
			} else {
				errorMessage = errorMessage.substring(0, errorMessage.length());
				errorMap.put("Text", errorMessage);
			}
			return errorMap;
		}

		//Validating And Creating New FTs for PS and Creating Adjustments
		/**
		 * @param payDtl
		 * @return
		 */
		private String payConfirmationFT(PayDetail_Id payDtl) {
			String status;

			String overPayflag=CommonUtils.CheckNull(payDtl.getOverpayment());
			String overPayRelease = payDtl.overPaymentRelease;
			if(overPayflag.equals("Y")){
				status = overPayConfirmation(payDtl);
			}
			else if("Y".equals(overPayRelease) && payDtl.getUnPaidAmt().compareTo(BigDecimal.ZERO)!=0){
				status = createPayReq(payDtl.getBillId(),payDtl.getLineId(),payDtl.getUnPaidAmt(),
						payDtl.getPayDetailId());
				if ("true".equals(status)){
					status=createFT(payDtl,FinancialTransactionTypeLookup.constants.PAY_SEGMENT,
							payDtl.getPayAmt(), overPayRelease);
				}
			}

			else{
				status=createFT(payDtl,FinancialTransactionTypeLookup.constants.PAY_SEGMENT,
						payDtl.getPayAmt(), overPayRelease);
			}

			return status;
		}

		private String overPayConfirmation(PayDetail_Id payDtl){
			String status;
			String overPayRelease = payDtl.overPaymentRelease;

			if(payDtl.getCreditNoteId() != null){
				status = createPayReq(payDtl.getBillId(),payDtl.getLineId(),payDtl.getUnPaidAmt(),
						payDtl.getPayDetailId());
			}
			else{
				status=createFT(payDtl,FinancialTransactionTypeLookup.constants.PAY_SEGMENT,
						payDtl.getPayAmt(), overPayRelease);
				if(CommonUtils.CheckNull(status).trim().startsWith("false")){
					return status;
				}
				status = createPayReq(payDtl.getBillId(),payDtl.getLineId(),payDtl.getUnPaidAmt(),
						payDtl.getPayDetailId());
			}
			return status;
		}

		private String createPayReq(String billId, String lineId, BigDecimal unpaidAmt,
									BigDecimal payDtlId) {

			String overpayReleaseExists = overpayReleaseExists(billId, lineId);
			String status;
			if ("false".equals(overpayReleaseExists)) {
				SQLResultRow sql;
				PreparedStatement ps = null;
				sql = getPayReqData(billId, lineId);
				try {
					StringBuilder sb = new StringBuilder();
					sb.append("Insert into CM_PAY_REQ_OVERPAY (BILL_ID,PER_ID_NBR,CIS_DIVISION,LINE_ID,ALT_BILL_ID,BILL_DT,ACCT_TYPE,PAY_TYPE,BILL_AMT,");
					sb.append("CURRENCY_CD,IS_IND_FLG,SUB_STLMNT_LVL,SUB_STLMNT_LVL_REF,CR_NOTE_FR_BILL_ID,IS_IMD_FIN_ADJ,FIN_ADJ_MAN_NRT,REL_RSRV_FLG,");
					sb.append("REL_WAF_FLG,FASTEST_ROUTE_INDICATOR,CASE_IDENTIFIER,PAY_REQ_GRANULARITIES,CREATE_DTTM,UPLOAD_DTTM,");
					sb.append("EXTRACT_FLG,EXTRACT_DTTM,REUSE_DUE_DT,INV_RECALC_FLG,ILM_DT,ILM_ARCH_SW,PAY_REQ_ID, ");
					sb.append(" MPG_TYPE, OVERPAYMENT_FLG, PAY_DETAIL_ID) ");
					sb.append("values (trim(:billId), :perIdNbr, :cisDivision, :lineId, :altBillId, :billDt, :acctType, :payType, :billAmt, ");
					sb.append(":currencyCd, :isIndFlg, :subStlmntLvl, :substlmntLvlRef, :crNotFrBillId, :isImdFinAdj, :finAdjManNrt, :relRsrvFlg, ");
					sb.append(":relWafFlg, :FastestRouteIndicator, :caseIdentifier, :payReqGranularity, :createDttm, :uploadDttm, ");
					sb.append("trim(:extractFlg), :extract_dttm, :reuseDueDt, :invRecalcFlg, :ilmDt, :ilmArchSw, ");
					sb.append(":payReqId, :mpgType, :overPaymentFlg, :payDtlId)");
					ps = createPreparedStatement(sb.toString(), "");
					ps.bindString(BILLID2, sql.getString(BILLID), BILLID);
					ps.bindString("perIdNbr", sql.getString("PER_ID_NBR"), "PER_ID_NBR");
					ps.bindString("cisDivision", sql.getString(CISDIVISION), CISDIVISION);
					ps.bindString("lineId", sql.getString("LINE_ID"), "LINE_ID");
					ps.bindString("altBillId", sql.getString("ALT_BILL_ID"), "ALT_BILL_ID");
					ps.bindDate("billDt", sql.getDate("BILL_DT"));
					ps.bindString("acctType", sql.getString(ACCTTYPE), ACCTTYPE);
					if(unpaidAmt.compareTo(BigDecimal.ZERO)>0) {
						ps.bindString(PAYTYPE2, "DR", PAYTYPE);
					}
					else {
						ps.bindString(PAYTYPE2, "CR", PAYTYPE);
					}
					ps.bindBigDecimal("billAmt", unpaidAmt);
					ps.bindString("currencyCd", sql.getString(CURRENCYCD), CURRENCYCD);
					ps.bindString("isIndFlg", sql.getString("IS_IND_FLG"), "IS_IND_FLG");
					ps.bindString("subStlmntLvl", null, "SUB_STLMNT_LVL");
					ps.bindString("substlmntLvlRef", null, "SUB_STLMNT_LVL_REF");
					ps.bindString("crNotFrBillId", sql.getString("CR_NOTE_FR_BILL_ID"), "CR_NOTE_FR_BILL_ID");
					ps.bindString("isImdFinAdj", sql.getString("IS_IMD_FIN_ADJ"), "IS_IMD_FIN_ADJ");
					ps.bindString("finAdjManNrt", sql.getString("FIN_ADJ_MAN_NRT"), "FIN_ADJ_MAN_NRT");
					ps.bindString("relRsrvFlg", sql.getString("REL_RSRV_FLG"), "REL_RSRV_FLG");
					ps.bindString("relWafFlg", sql.getString("REL_WAF_FLG"), "REL_WAF_FLG");
					ps.bindString("FastestRouteIndicator", sql.getString("FASTEST_ROUTE_INDICATOR"), "FASTEST_ROUTE_INDICATOR");
					ps.bindString("caseIdentifier", sql.getString("CASE_IDENTIFIER"), "CASE_IDENTIFIER");
					ps.bindString("payReqGranularity", null, "PAY_REQ_GRANULARITIES");
					ps.bindDateTime("createDttm", getSystemDateTime());
					ps.bindDateTime("uploadDttm", getSystemDateTime());
					ps.bindString("extractFlg", sql.getString("EXTRACT_FLG"), "EXTRACT_FLG");
					ps.bindDate("extract_dttm", null);
					ps.bindString("reuseDueDt", null, "REUSE_DUE_DT");
					ps.bindString("invRecalcFlg", null, "INV_RECALC_FLG");
					ps.bindDateTime("ilmDt", getSystemDateTime());
					ps.bindString("ilmArchSw", sql.getString("ILM_ARCH_SW"), "ILM_ARCH_SW");
					ps.bindString("payReqId", null, "PAY_REQ_ID");
					ps.bindString("mpgType", sql.getString("MPG_TYPE"), "MPG_TYPE");
					ps.bindString("overPaymentFlg", "Y", "OVERPAYMENT_FLG");
					ps.bindBigDecimal(PAYDTLID, payDtlId);
					ps.setAutoclose(false);
					ps.executeUpdate();
				} catch (Exception e) {
					logger.error("Inside catch block of creating Pay Request method-", e);
					return "false";
				} finally {
					if (ps != null) {
						ps.close();
						ps = null;
					}
				}
			}
			else {
				status = updatePayReq(billId, lineId, payDtlId, unpaidAmt);
				return status;
			}
			return "true";
		}

		private String overpayReleaseExists(String billId, String lineId){

			PreparedStatement preparedStatement = null;
			StringBuilder sb = new StringBuilder();
			SQLResultRow sqlResultRow;
			try {
				sb.append(" SELECT 1 COUNT ");
				sb.append(" FROM CM_PAY_REQ_OVERPAY WHERE BILL_ID = :billId and LINE_ID = :lineId ");
				sb.append(" AND OVERPAYMENT_FLG = 'Y' AND ILM_DT >= TRUNC(SYSDATE) ");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString(BILLID2, billId, BILLID);
				preparedStatement.bindString("lineId", lineId, "LINE_ID");
				preparedStatement.setAutoclose(false);
				sqlResultRow = preparedStatement.firstRow();
				if(sqlResultRow != null){
					return "true";
				}
			}
			catch(Exception e){
				logger.error(" Inside catch  block of updating overpayReleaseExists method-", e);
				return "false" ;
			}
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return  "false";
		}

		private String updatePayReq(String billId, String lineId, BigDecimal payDtlId, BigDecimal unpaidAmt){

			PreparedStatement preparedStatement = null;
			StringBuilder sb = new StringBuilder();
			try {
				sb.append(" UPDATE CM_PAY_REQ_OVERPAY SET BILL_AMT = :unpaidAmt, PAY_DETAIL_ID = :payDtlId  ");
				sb.append(" WHERE BILL_ID = :billId and LINE_ID = :lineId ");
				sb.append(" AND OVERPAYMENT_FLG = 'Y' AND ILM_DT >= TRUNC(SYSDATE) ");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString(BILLID2, billId, BILLID);
				preparedStatement.bindString("lineId", lineId, "LINE_ID");
				preparedStatement.bindBigDecimal("unpaidAmt", unpaidAmt);
				preparedStatement.bindBigDecimal(PAYDTLID, payDtlId);
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			}
			catch(Exception e){
				logger.error(" Inside catch  block of updating Pay Request method-", e);
				return "false" ;
			}
			finally{
				if(preparedStatement != null){
					preparedStatement.close();
				}
			}
			return "true";
		}

		private SQLResultRow getPayReqData(String billId, String lineId){

			PreparedStatement preparedStatement = null ;
			StringBuilder sb = new StringBuilder();
			SQLResultRow sqlResultRow = null;

			sb.append("select snp.BILL_ID, snp.party_id PER_ID_NBR, substr(trim(snp.lcp), -5) CIS_DIVISION, LINE_ID, snp.ALT_BILL_ID, ");
			sb.append(" snp.BILL_DT, snp.ACCT_TYPE,PAY_TYPE, 0 BILL_AMT, ");
			sb.append("	snp.CURRENCY_CD, 'N' IS_IND_FLG, null SUB_STLMNT_LVL, null SUB_STLMNT_LVL_REF, ");
			sb.append("' ' CR_NOTE_FR_BILL_ID, 'Y' IS_IMD_FIN_ADJ, ");
			sb.append(" 'OVERPAYMENT'|| TO_CHAR(SYSDATE,'DDMM') FIN_ADJ_MAN_NRT, ");
			sb.append(" 'N' REL_RSRV_FLG, 'N' REL_WAF_FLG, 'N' FASTEST_ROUTE_INDICATOR, 'N' CASE_IDENTIFIER, null PAY_REQ_GRANULARITIES, ");
			sb.append(" sysdate CREATE_DTTM, ");
			sb.append("sysdate UPLOAD_DTTM, 'Y' EXTRACT_FLG,null EXTRACT_DTTM, null REUSE_DUE_DT,null INV_RECALC_FLG,sysdate ILM_DT, ");
			sb.append(" 'Y' ILM_ARCH_SW, null PAY_REQ_ID, 'MPG' MPG_TYPE, 'Y' OVERPAYMENT_FLG, LATEST_PAY_DTL_ID ");
			sb.append("from cm_bill_payment_dtl_snapshot snp where BILL_ID = trim(:billId) and LINE_ID = :lineId  order by LATEST_PAY_DTL_ID DESC");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindString(BILLID2, billId, BILLID);
			preparedStatement.bindString("lineId", lineId, "LINE_ID");
			preparedStatement.setAutoclose(false);
			sqlResultRow = preparedStatement.firstRow();
			return  sqlResultRow;
		}

		/**
		 * @param payDtl
		 * @param ftFlag
		 * @param payAmt
		 * @return
		 */
		private String createFT(PayDetail_Id payDtl,
								FinancialTransactionTypeLookup ftFlag, BigDecimal payAmt, String overpayRelease) {

			String returnStatus="false";
			String billId=payDtl.getBillId();
			int num1 = 100;
			int num2 = 800;
			int num3 = 100000000;
			int num4 = 800000000;
			SecureRandom random = new SecureRandom();
			String ftId = String.valueOf(random.nextInt(num2) + num1)
					.concat(String.valueOf(random.nextInt(num4) + num3));
			logger.debug("****************BILL ID***************************************"+billId);

			//Added logic to check for Existence for New FT Id creation

			while (checkFTExists(ftId))
			{
				random = new SecureRandom();
				ftId = String.valueOf(random.nextInt(num2) + num1)
						.concat(String.valueOf(random.nextInt(num4) + num3));			}

			returnStatus=createFTEntries(ftId,ftFlag,payDtl,payAmt,billId, overpayRelease);

			return returnStatus;
		}

		//Validating And Creating New FTs

		/**
		 * @param ftId
		 * @param ftFlag
		 * @param payDtl
		 * @param payAmt
		 * @param billingId
		 * @return
		 */
		private String createFTEntries(String ftId,
									   FinancialTransactionTypeLookup ftFlag, PayDetail_Id payDtl,
									   BigDecimal payAmt, String billingId, String overpayRelease) {

			StringBuilder sb=new StringBuilder();
			PreparedStatement ps=null;

			try{
				sb.append("INSERT INTO CI_FT VALUES(:ftId,:siblingId,:saId,:parentId,:glDiv,:cisDiv,:currCd,:ftType,:curAmt,:totAmt,SYSTIMESTAMP,:freezeSw,:userId,:freezeDttm,:arsDt,:corrSw,:redSw,");
				sb.append(":newSw,:showSw,:notArSw,trim(:billId),:accdDt,'1','N','', ");
				sb.append(":glStatus,:glDistDt,'0',:overpayRelease ,:prsnBillId,'0',:settNbr,' ')");
				ps=createPreparedStatement(sb.toString(), "");
				// NAP-37745
				ps.bindString(SIBLINGID2, " ", SIBLINGID);
				ps.bindString("parentId", payDtl.getExtSourceCd(), "PARENT_ID");
				ps.bindString("ftId", ftId, "FT_ID");
				ps.bindString("saId", saId,"SA_ID");
				ps.bindId("glDiv",new GeneralLedgerDivision_Id(payConfirmationLookup.getGlDivision()));
				ps.bindId("cisDiv", new CisDivision_Id(cisDivision));
				ps.bindId("currCd", new Currency_Id(CommonUtils.convertNullToEmptyString(payDtl.getCurrencyCode())));
				ps.bindLookup("ftType", ftFlag);
				ps.bindMoney("curAmt",new Money(payAmt, new Currency_Id(CommonUtils.convertNullToEmptyString(payDtl.getCurrencyCode()))));
				ps.bindMoney("totAmt",new Money(payAmt, new Currency_Id(CommonUtils.convertNullToEmptyString(payDtl.getCurrencyCode()))));
				ps.bindBoolean("freezeSw", Bool.TRUE);
				ps.bindId("userId", new User_Id(payConfirmationLookup.getUserId()));
				ps.bindDateTime("freezeDttm", getSystemDateTime());
				ps.bindDate("arsDt",getSystemDateTime().getDate());
				ps.bindBoolean("corrSw", Bool.FALSE);
				ps.bindBoolean("redSw", Bool.FALSE);
				ps.bindBoolean("newSw", Bool.FALSE);
				ps.bindBoolean("showSw", Bool.TRUE);
				ps.bindBoolean("notArSw", Bool.FALSE);
				ps.bindString(BILLID2, billingId,BILLID);
				ps.bindDate("accdDt", payDtl.getPayDate());
				ps.bindLookup("glStatus", GlDistributionStatusLookup.constants.GENERATED);
				ps.bindDate("glDistDt",getSystemDateTime().getDate());
				ps.bindString("overpayRelease", overpayRelease, "MATCH_EVT_ID");
				ps.bindString("prsnBillId", billingId,BILLID);
				ps.bindString("settNbr", payDtl.getPayDetailId().toString(), "SETTLEMENT_ID_NBR");
				ps.executeUpdate();

				insertFTKey(ftId);

			}
			catch(Exception e){
				logger.error("Inside catch block of creating FT's method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);

				return "false" + "~" + errorMsg.get("Text") + "~"
						+ errorMsg.get("Category") + "~"
						+ errorMsg.get("Number");
			}
			finally{
				if(ps != null){
					ps.close();
					ps=null;
				}
			}
			return "true";

		}

		private boolean checkFTExists(String ftId) {

			BigInteger count=BigInteger.ZERO;
			StringBuilder sb=new StringBuilder();
			sb.append("SELECT COUNT(*) COUNT_FT FROM CI_FT WHERE FT_ID=:ftId");
			PreparedStatement ps=null;
			ps=createPreparedStatement(sb.toString(),"");
			ps.bindString("ftId",ftId,"FT_ID");
			ps.setAutoclose(false);
			SQLResultRow rs=ps.firstRow();
			count=rs.getInteger("COUNT_FT");

			if (notNull(ps)) {
				ps.close();
				ps = null;
			}

			logger.debug(" checkFTExists() method :: END");

			if(count.compareTo(BigInteger.ZERO) == 0){
				return false;
			}

			return true;
		}

		private void insertFTKey(String ftId) {
			// TODO Auto-generated method stub

			PreparedStatement ps=null;
			StringBuilder sb=new StringBuilder();

			try{
				sb.append("INSERT INTO CI_FT_K VALUES(:ftId,:envId)");
				ps=createPreparedStatement(sb.toString(),"");
				ps.bindString("ftId", ftId, "FT_ID");
				ps.bindBigInteger("envId",envId);
				ps.executeUpdate();

			}
			catch(Exception e){
				logger.error("Error in Inserting in CI_FT_K  :"+e);

			}
			finally{
				if(ps!=null){
					ps.close();
					ps=null;
				}
			}

		}

		// Extracting Account Details for SA_ID, CIS_DIVISION

		private boolean extractBillDetails (String billId, String lineId){
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {

				/********** Extracting account Details *************/
				stringBuilder.append("SELECT ACCT.SUBACCOUNTID, INV.CIS_DIVISION ");
				stringBuilder.append("FROM CM_INVOICE_DATA INV, CBE_CUE_OWNER.VWM_BCU_ACCOUNT ACCT ");
				stringBuilder.append("WHERE INV.BILLING_PARTY_ID = ACCT.PARTYID ");
				stringBuilder.append("AND TRIM(ACCT.SUBACCOUNTTYPE) = INV.ACCT_TYPE ");
				stringBuilder.append("AND INV.CIS_DIVISION = ACCT.LEGALCOUNTERPARTY ");
				stringBuilder.append("AND INV.CURRENCY_CD = ACCT.CURRENCYCODE ");
				stringBuilder.append("AND INV.BILL_ID = trim(:billId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString(BILLID2, billId, BILLID);
				preparedStatement.setAutoclose(false);
				if (!notNull(preparedStatement.firstRow()))
				{
					return false;
				}
				else {
					cisDivision = CommonUtils.CheckNull(preparedStatement.firstRow().getString(CISDIVISION));
					saId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("SUBACCOUNTID"));
				}
			}
                catch(Exception e)
				{
					logger.error("Exception occured in createPaymentStaging() ", e);
					return logError(billId, lineId,
					payConfirmationLookup.getError(), String.valueOf(CustomMessages.MESSAGE_CATEGORY),
							String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS),
							getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS)));
				}
		 finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return true;
		}


		private void updateBillPaymentDtlSnapshotEntries(PayDetail_Id payDtl,
														 String aStatus, String aMessageCategory, String aMessageNumber, String aErrorMessage) {

			PreparedStatement stmt = null;
			StringBuilder updateRequestEntries = new StringBuilder();
			try {
				updateRequestEntries.append(" UPDATE CM_BILL_PAYMENT_DTL_SNAPSHOT SET LATEST_PAY_DTL_ID=:payDetailId, ");
				updateRequestEntries.append(" LATEST_UPLOAD_DTTM= SYSTIMESTAMP, PAY_DT=:payDate, EXT_TRANSMIT_ID=trim(:eventId), ");
				updateRequestEntries.append(" PAY_TYPE= :payType, LINE_AMT=:lineAmt, ");
				updateRequestEntries.append(" PREV_UNPAID_AMT=:prevUnpaidAmt, LATEST_PAY_AMT=:payAmt, UNPAID_AMT=:unpaidAmt, ");
				updateRequestEntries.append(" BILL_BALANCE= :unpaidAmt, ");
				updateRequestEntries.append(" LATEST_STATUS =:status,EXT_SOURCE_CD=:extSrcCd,  ");
				updateRequestEntries.append(" OVERPAID= :overpayFlg, RECORD_STAT=:recStatus, STATUS_UPD_DTTM = SYSTIMESTAMP,  ");
				updateRequestEntries.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription, ");
				updateRequestEntries.append(" CREDIT_NOTE_ID=:creditNoteId, OVERPAYMENT_RELEASE = :overPayRelease ");
				updateRequestEntries.append(" WHERE BILL_ID =trim(:billId) AND LINE_ID=:lineId ");

				stmt = createPreparedStatement(updateRequestEntries.toString(), "");
				stmt.bindBigDecimal("payDetailId", payDtl.getPayDetailId());
				stmt.bindDate("payDate", payDtl.getPayDate());
				stmt.bindString("eventId", payDtl.getEventId(), "EXT_TRANSMIT_ID");
				stmt.bindString(PAYTYPE2, payDtl.getPayType(), PAYTYPE);
				stmt.bindBigDecimal("lineAmt", payDtl.getLineAmt());
				stmt.bindBigDecimal("prevUnpaidAmt", payDtl.getPrevUnpaidAmt());
				stmt.bindBigDecimal("payAmt", payDtl.getPayAmt());
				stmt.bindBigDecimal("unpaidAmt", payDtl.getUnPaidAmt());
				stmt.bindString("status", payDtl.getStatus().trim(), "STATUS_CD");
				stmt.bindString("extSrcCd", payDtl.getExtSourceCd(), "EXT_SOURCE_CD");
				stmt.bindString("overpayFlg", payDtl.getOverpayment(), OVERPAID);
				stmt.bindString("recStatus", CommonUtils.CheckNull(aStatus.trim()), "RECORD_STAT");
				stmt.bindBigInteger("messageCategory", new BigInteger(aMessageCategory));
				stmt.bindBigInteger("messageNumber", new BigInteger(aMessageNumber));
				stmt.bindString("errorDescription", CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
				stmt.bindString("creditNoteId", payDtl.getCreditNoteId(), "CREDIT_NOTE_ID");
				stmt.bindString(BILLID2, payDtl.getBillId(), BILLID);
				stmt.bindString("lineId", payDtl.getLineId().trim(), "LINE_ID");
				stmt.bindString("overPayRelease", payDtl.getOverPaymentRelease(),"OVERPAYMENT_RELEASE");

				stmt.executeUpdate();
			}
			catch(Exception e){
				logger.error("Exception in execute Work Unit"+e);
			}
			finally{
				if(stmt != null){
					stmt.close();
					stmt = null;
				}
			}

		}


		/**
		 * updateBillPayDtl() method will update the CM_BILL_PAYMENT_DTL
		 * table with the processing status
		 *
		 * @param lineId
		 * @param finDocId
		 * @param aStatus
		 */
		private void updateBillPayDtl(String lineId, String finDocId, String aStatus, String aMessageCategory, String aMessageNumber, String aErrorMessage) {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			if (aErrorMessage.length() > 255) {
				aErrorMessage = aErrorMessage.substring(0, 249);
			}

			try {
				stringBuilder.append("UPDATE CM_BILL_PAYMENT_DTL SET RECORD_STAT=:recStatus, STATUS_UPD_DTTM = SYSTIMESTAMP, MESSAGE_CAT_NBR =:messageCategory, ");
				stringBuilder.append("  MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription WHERE BILL_ID = trim(:finDocId) ");
				stringBuilder.append("AND LINE_ID=:lineId AND RECORD_STAT=:pend ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("recStatus", CommonUtils.CheckNull(aStatus.trim()), "RECORD_STAT");
				preparedStatement.bindString("pend", payConfirmationLookup.getPending(), "RECORD_STAT");
				preparedStatement.bindBigInteger("messageCategory", new BigInteger(aMessageCategory));
				preparedStatement.bindBigInteger("messageNumber", new BigInteger(aMessageNumber));
				preparedStatement.bindString("errorDescription", CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
				preparedStatement.bindString("finDocId", finDocId, BILLID);
				preparedStatement.bindString("lineId", lineId, "LINE_ID");

				preparedStatement.executeUpdate();

			} catch (Exception e) {
				logger.error("Error in updateBillPayDtl");
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
		 *
		 * @param finDocId
		 * @param lineId
		 * @param aStatus
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 * @return
		 */
		private boolean logError(String finDocId,String lineId, String aStatus,
								 String aMessageCategory, String aMessageNumber,String aErrorMessage) {

			eachCustomerStatusList = new ArrayList<String>();
			eachCustomerStatusList.add(0, finDocId);
			eachCustomerStatusList.add(1, lineId);
			eachCustomerStatusList.add(2, payConfirmationLookup.getError());
			eachCustomerStatusList.add(3, aMessageCategory);
			eachCustomerStatusList.add(4, aMessageNumber);
			eachCustomerStatusList.add(5, aErrorMessage);
			updateCustomerStatusList.add(eachCustomerStatusList);
			eachCustomerStatusList = null;


			//Excepted to do rollback
			rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
			if (aMessageCategory.trim().equals(String.valueOf(CustomMessages.MESSAGE_CATEGORY))) {
				addError(CustomMessageRepository.billCycleError(aMessageNumber));
			}

			return false; // intentionally kept false as rollback has to occur
			// here
		}

		/**
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 *
		 * @return errorInfo
		 */
		public static String getErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.merchantError(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		/**
		 * updateCmBillDueDtTbl() method will set Merchant Balanced flag 
		 * for a particular bill as 'Y' if the bill is settled.
		 *
		 * @param payDtl
		 * @param merchFlag
		 */
		private int updateCmBillDueDtTbl(PayDetail_Id payDtl, String merchFlag) {

			PreparedStatement dueDtUpdateStmt = null;
			StringBuilder stringBuilder = new StringBuilder();
			int row= 0;

			try {
				stringBuilder.append("UPDATE CM_BILL_DUE_DT SET IS_MERCH_BALANCED = :merchFlag, " );
				stringBuilder.append(" STATUS_UPD_DTTM = SYSTIMESTAMP WHERE BILL_ID = trim(:billId) AND LINE_ID = :lineId");
				dueDtUpdateStmt = createPreparedStatement(stringBuilder.toString(), "");
				dueDtUpdateStmt.bindString(BILLID2, payDtl.getBillId(), BILLID);
				dueDtUpdateStmt.bindString("lineId", payDtl.getLineId(), "LINE_ID");
				dueDtUpdateStmt.bindString("merchFlag", merchFlag, "IS_MERCH_BALANCED");

				row = dueDtUpdateStmt.executeUpdate();
				return row;
			} catch (Exception e){
				logger.error("Exception while updating bill Due date", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (dueDtUpdateStmt != null) {
					dueDtUpdateStmt.close();
					dueDtUpdateStmt = null;
				}
			}
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

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException,
				RunAbortedException {

			//			Logic to update erroneous records
			if (updateCustomerStatusList.size() > 0) {
				Iterator<ArrayList<String>> updatePayCnfStatusItr = updateCustomerStatusList.iterator();
				updateCustomerStatusList = null;
				ArrayList<String> rowList = null;
				while (updatePayCnfStatusItr.hasNext()) {
					rowList = (ArrayList<String>) updatePayCnfStatusItr.next();
					updateBillPayDtl(String.valueOf(rowList.get(1)),
							String.valueOf(rowList.get(0)), String.valueOf(rowList.get(2)), String.valueOf(rowList.get(3)),
							String.valueOf(rowList.get(4)),String.valueOf(rowList.get(5)));
					rowList = null;
				}
				updatePayCnfStatusItr = null;
			}
			payConfirmationLookup = null;
			super.finalizeThreadWork();
		}

	} //End of Worker Class



	public static final class Payment_Dtl_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String billId;
		private String lineId;


		public Payment_Dtl_Id(String billId, String lineId) {
			setBillId(billId);
			setLineId(lineId);
		}


		public static long getSerialversionuid() {
			return serialVersionUID;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getLineId() {
			return lineId;
		}

		public void setLineId(String lineId) {
			this.lineId = lineId;
		}

		public String getBillId() {
			return billId;
		}

		public void setBillId(String billId) {
			this.billId = billId;
		}
	}

	public static final class PayDetail_Id implements Id {

		/**
		 *
		 */
		private static final long serialVersionUID = 1L;
		private String billId;
		private BigDecimal payDetailId;
		private BigDecimal lineAmt;

		private String lineId;
		private String status;
		private String payType;
		private String eventId;
		private String currencyCode;
		private String overpayment;
		private BigDecimal prevUnpaidAmt;
		private String extSourceCd;

		private BigDecimal payAmt;
		private BigDecimal unPaidAmt;
		private Date payDate;
		private String creditNoteId;
		private String overPaymentRelease;


		public PayDetail_Id(String billId, BigDecimal payDetailId, String lineId,
							String status, String payType, String eventId,
							String currencyCode, String overpayment,
							BigDecimal prevUnpaidAmt, BigDecimal payAmt,
							BigDecimal unPaidAmt, BigDecimal lineAmt, Date payDate,
							String extSourceCd,String creditNoteId, String overPaymentRelease) {

			setBillId(billId);
			setCurrencyCode(currencyCode);
			setEventId(eventId);
			setLineId(lineId);
			setOverpayment(overpayment);
			setPayAmt(payAmt);
			setPayDetailId(payDetailId);
			setPayType(payType);
			setPrevUnpaidAmt(prevUnpaidAmt);
			setStatus(status);
			setUnPaidAmt(unPaidAmt);
			setLineAmt(lineAmt);
			setPayDate(payDate);
			setExtSourceCd(extSourceCd);
			setCreditNoteId(creditNoteId);
			setOverPaymentRelease(overPaymentRelease);

		}

		public String getCreditNoteId() {
			return creditNoteId;
		}

		public void setCreditNoteId(String creditNoteId) {
			this.creditNoteId = creditNoteId;
		}

		/**
		 * @return the extSourceCd
		 */
		public String getExtSourceCd() {
			return extSourceCd;
		}
		/**
		 * @param extSourceCd the extSourceCd to set
		 */
		public void setExtSourceCd(String extSourceCd) {
			this.extSourceCd = extSourceCd;
		}
		/**
		 * @return the lineAmt
		 */
		public BigDecimal getLineAmt() {
			return lineAmt;
		}
		/**
		 * @param lineAmt the lineAmt to set
		 */
		public void setLineAmt(BigDecimal lineAmt) {
			this.lineAmt = lineAmt;
		}
		/**
		 * @return the billId
		 */
		public String getBillId() {
			return billId;
		}
		/**
		 * @param billId the billId to set
		 */
		public void setBillId(String billId) {
			this.billId = billId;
		}
		/**
		 * @return the payDetailId
		 */
		public BigDecimal getPayDetailId() {
			return payDetailId;
		}
		/**
		 * @param payDetailId the payDetailId to set
		 */
		public void setPayDetailId(BigDecimal payDetailId) {
			this.payDetailId = payDetailId;
		}

		/**
		 * @return the payDate
		 */
		public Date getPayDate() {
			return payDate;
		}
		/**
		 * @param payDate the payDate to set
		 */
		public void setPayDate(Date payDate) {
			this.payDate = payDate;
		}

		/**
		 * @return the lineId
		 */
		public String getLineId() {
			return lineId;
		}
		/**
		 * @param lineId the lineId to set
		 */
		public void setLineId(String lineId) {
			this.lineId = lineId;
		}
		/**
		 * @return the status
		 */
		public String getStatus() {
			return status;
		}
		/**
		 * @param status the status to set
		 */
		public void setStatus(String status) {
			this.status = status;
		}
		/**
		 * @return the payType
		 */
		public String getPayType() {
			return payType;
		}
		/**
		 * @param payType the payType to set
		 */
		public void setPayType(String payType) {
			this.payType = payType;
		}
		/**
		 * @return the eventId
		 */
		public String getEventId() {
			return eventId;
		}
		/**
		 * @param eventId the eventId to set
		 */
		public void setEventId(String eventId) {
			this.eventId = eventId;
		}
		/**
		 * @return the currencyCode
		 */
		public String getCurrencyCode() {
			return currencyCode;
		}
		/**
		 * @param currencyCode the currencyCode to set
		 */
		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}
		/**
		 * @return the overPaymentRelease
		 */
		public String getOverPaymentRelease() {
			return overPaymentRelease;
		}
		/**
		 * @param overPaymentRelease the overpayment to set
		 */
		public void setOverPaymentRelease(String overPaymentRelease) {
			this.overPaymentRelease = overPaymentRelease;
		}
		/**
		 * @return the prevUnpaidAmt
		 */
		public BigDecimal getPrevUnpaidAmt() {
			return prevUnpaidAmt;
		}
		/**
		 * @param prevUnpaidAmt the prevUnpaidAmt to set
		 */
		public void setPrevUnpaidAmt(BigDecimal prevUnpaidAmt) {
			this.prevUnpaidAmt = prevUnpaidAmt;
		}
		/**
		 * @return the payAmt
		 */
		public BigDecimal getPayAmt() {
			return payAmt;
		}
		/**
		 * @param payAmt the payAmt to set
		 */
		public void setPayAmt(BigDecimal payAmt) {
			this.payAmt = payAmt;
		}
		/**
		 * @return the unPaidAmt
		 */
		public BigDecimal getUnPaidAmt() {
			return unPaidAmt;
		}
		/**
		 * @param unPaidAmt the unPaidAmt to set
		 */
		public void setUnPaidAmt(BigDecimal unPaidAmt) {
			this.unPaidAmt = unPaidAmt;
		}
		/**
		 * @return the overpayment
		 */
		public String getOverpayment() {
			return overpayment;
		}
		/**
		 * @param overpayment the overpayment to set
		 */
		public void setOverpayment(String overpayment) {
			this.overpayment = overpayment;
		}
		@Override
		public void appendContents(StringBuilder arg0) {
			// TODO Auto-generated method stub

		}
		@Override
		public boolean isNull() {
			// TODO Auto-generated method stub
			return false;
		}


	}

}
