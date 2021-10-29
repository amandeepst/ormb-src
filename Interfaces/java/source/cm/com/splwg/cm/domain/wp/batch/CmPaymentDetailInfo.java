package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
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
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 * 
 * @BatchJob (multiThreaded = true, rerunnable = true, modules = {"demo"},
 *           softParameters = { @BatchJobSoftParameter (name = batchReRunDate,
 *           type = date)})
 */
public class CmPaymentDetailInfo extends CmPaymentDetailInfo_Gen {

	private InboundPaymentsLookUp payConfirmationLookup = null;
	public static final String FINANCIAL_DOC_ID = "FINANCIAL_DOC_ID";
	public static final String FINANCIAL_DOC_LINE_ID = "FINANCIAL_DOC_LINE_ID";
	public static final String BILL_ID = "billId";
	public static final String LINE_ID = "lineId";
	public static final String RELEASED = "RELEASED";
	public static final String REJECTED = "REJECTED";

	public static final Logger logger = LoggerFactory
			.getLogger(CmPaymentDetailInfo.class);

	public JobWork getJobWork() {
		payConfirmationLookup = new InboundPaymentsLookUp();
		resetPayDtlId();

		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		threadWorkUnitList = getFinanacialDocId();
		payConfirmationLookup = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// Get Distinct Financial Doc ID from Payment Staging Tables under UPLD Status
	private List<ThreadWorkUnit> getFinanacialDocId() {		

		Financial_Doc_Id finDocId = null;
		Date batchReRunDate = getParameters().getBatchReRunDate();	

		List<ThreadWorkUnit> list = new ArrayList<>();
		PreparedStatement ps = null;
		StringBuilder sb = null;
		String financialDocId = "";
		String lineId = "";
		ThreadWorkUnit twu = null;	

		try {
			sb = new StringBuilder();
			//To pick request entries on which payment has been applied
			sb.append(" SELECT DISTINCT D.BILL_ID BILL_ID,D.LINE_ID LINE_ID ");
			sb.append(" FROM CM_PAY_STG S, CM_BILL_PAYMENT_DTL D        ");
			sb.append(" WHERE D.BILL_ID =S.FINANCIAL_DOC_ID AND S.FINANCIAL_DOC_LINE_ID =D.LINE_ID AND S.BO_STATUS_CD=:boStatusCd AND PAY_DTL_ID =(SELECT MAX(PAY_DTL_ID) ");
			sb.append(" FROM CM_BILL_PAYMENT_DTL WHERE D.BILL_ID =BILL_ID AND LINE_ID =D.LINE_ID)                             ");
			sb.append(" AND NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID=D.BILL_ID AND LINE_ID=D.LINE_ID AND EXT_TRANSMIT_ID=S.EXT_TRANSMIT_ID)            ");
			if(notNull(batchReRunDate)) {
				sb.append(" AND TRUNC(S.PAY_DT) =:payDate ");
			}

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindString("boStatusCd", payConfirmationLookup.getUpload().trim(), "BO_STATUS_CD");

			if(notNull(batchReRunDate))
			{
				ps.bindDate("payDate", batchReRunDate);
			}

			for (SQLResultRow rs : ps.list()) {

				financialDocId = rs.getString("BILL_ID");
				lineId = rs.getString("LINE_ID");
				finDocId = new Financial_Doc_Id(financialDocId,lineId);
				twu = new ThreadWorkUnit();
				twu.setPrimaryId(finDocId);
				list.add(twu);
				twu = null;
				rs = null;
				finDocId = null;						
			}

		} catch (Exception e) {
			logger.error("Exception occurred in getStagingData()", e);
			throw new RunAbortedException(
					CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

		return list;
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

	public Class<CmPaymentDetailInfoWorker> getThreadWorkerClass() {
		return CmPaymentDetailInfoWorker.class;
	}

	public static class CmPaymentDetailInfoWorker extends
	CmPaymentDetailInfoWorker_Gen {

		private InboundPaymentsLookUp payConfirmationLookup = null;
		String billId = "";
		String lineId = "";
		Date payDate = null;
		BigDecimal billAmt = null;
		BigDecimal tenderAmt = null;
		String status = "";
		String payType = "";
		String extTrnsmtId = "";
		String overpayFlg = null;
		String currencyCd = null;
		BigDecimal unpaidAmt = null;


		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {

			if (payConfirmationLookup == null) {
				payConfirmationLookup = new InboundPaymentsLookUp();
			}
		}

		public ThreadExecutionStrategy createExecutionStrategy() {

			return new StandardCommitStrategy(this);
		}

		/**
		 * ExecuteWorkUnit() method contains logic that is invoked by every unit
		 * of thread
		 */

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			Financial_Doc_Id finDocId = (Financial_Doc_Id) unit.getPrimaryId();
			billId = "";
			lineId = "";
			overpayFlg="";
			String recStatus = null;
			String statusCd = null;
			BigDecimal tenderAmtVal = BigDecimal.ZERO;
			String extTrnsmtIdVal = null;
			String extSrcCd = null;
			BigDecimal prevUnpaidAmt = null;
			List<String> billIds = new ArrayList<>();
			try {

				List<SQLResultRow> payStg = fetchRecords(finDocId);
				billId = finDocId.getFinDocId();
				lineId = finDocId.getLineId();

				for (SQLResultRow rs : payStg) {

					payDate = rs.getDate("PAY_DT");
					billAmt = rs.getBigDecimal("LINE_AMT");
					unpaidAmt = rs.getBigDecimal("UNPAID_AMT");
					payType = rs.getString("PAY_TYPE");
					currencyCd = rs.getString("CURRENCY_CD");
					extTrnsmtId = rs.getString("EXT_TRANSMIT_ID");
					status = rs.getString("STATUS");
					extSrcCd = rs.getString("EXT_SOURCE_CD");

					if (REJECTED.equalsIgnoreCase(status) || "WRITE_OFF_REV".equalsIgnoreCase(status)) {
						tenderAmt = rs.getBigDecimal("TENDER_AMT");
					} else {
						tenderAmt = rs.getBigDecimal("TENDER_AMT").negate();
					}

					if(isNull(prevUnpaidAmt)){
						prevUnpaidAmt = unpaidAmt;
					}

					unpaidAmt = prevUnpaidAmt.add(tenderAmt);
					checkOverPayment(status, prevUnpaidAmt, billAmt, unpaidAmt);
					logger.debug("Bill Id: "+ billId+ "line Id: "+lineId+ " Pay Date: " +payDate+ "Bill amt: " +billAmt+ "prev unpaid: " +prevUnpaidAmt+ "tender amt: " +tenderAmtVal+ "unpaid amt: " +unpaidAmt+ "status: " +statusCd+ "event Id: " +extTrnsmtIdVal+ "over pay flag: " +overpayFlg);
					recStatus = insertPayDetail(billId, lineId, payDate, billAmt, payType, prevUnpaidAmt, tenderAmt, unpaidAmt, currencyCd, status, extTrnsmtId, extSrcCd, overpayFlg);

					if (CommonUtils.CheckNull(recStatus).trim().startsWith("false")) {
						String[] returnStatusArray = recStatus.split("~");							
						markError(billId, lineId, returnStatusArray[2].trim(), returnStatusArray[3].trim(), returnStatusArray[1].trim());	
					}
					else {
						billIds.add(billId);
					}
					prevUnpaidAmt = unpaidAmt;
				}
				if(!billIds.isEmpty())
				{
					updateRecordStatus(billIds);
				}				

			} catch (Exception e) {
				logger.error("Error in Loading records in Bill Payment Detail" + e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<>();
				errorMsg = errorList(errorMessage);

				markError(billId, lineId, errorMsg.get("Category"), errorMsg.get("Number"), errorMsg.get("Text").replace("Text:", ""));	

			} 

			return true;
		}


		/**
		 * @param errorMessage
		 * @return
		 */
		public Map<String, String> errorList(String errorMessage) {
			Map<String, String> errorMap = new HashMap<>();
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

		public void markError(String billId, String lineId, String catNbr, String msgNbr, String errMsg) {

			StringBuilder stmt = new StringBuilder();
			PreparedStatement pStmt = null	;			

			try {
				stmt.append( "UPDATE CM_PAY_STG SET BO_STATUS_CD=:stat, STATUS_UPD_DTTM = SYSTIMESTAMP, MESSAGE_CAT_NBR=:catNbr, MESSAGE_NBR=:msgNbr, ERROR_INFO =:errMsg ") 
				.append(" WHERE FINANCIAL_DOC_ID =:billId AND FINANCIAL_DOC_LINE_ID=:lineId AND BO_STATUS_CD=:upld");

				pStmt = createPreparedStatement(stmt.toString(), "markRecordsInError");
				pStmt.bindString("catNbr",catNbr, "MESSAGE_CAT_NBR");
				pStmt.bindString("msgNbr",msgNbr, "MESSAGE_NBR");
				pStmt.bindString("errMsg",errMsg, "ERROR_INFO");
				pStmt.bindString(BILL_ID,billId, FINANCIAL_DOC_ID);
				pStmt.bindString(LINE_ID,lineId, FINANCIAL_DOC_LINE_ID);
				pStmt.bindString("upld",payConfirmationLookup.getUpload(), "BO_STATUS_CD");
				pStmt.bindString("stat", payConfirmationLookup.getError(), "BO_STATUS_CD");
				pStmt.execute();
			}
			catch(Exception e){
				logger.error("Exception in markError method "+e);									
			}
			finally{
				if(notNull(pStmt)){
					pStmt.close();
					pStmt = null;
				}
			}
		}

		/**
		 * Fetch records based on Bill id and Line Id
		 * @param financialDocId
		 * @return
		 */
		private List<SQLResultRow> fetchRecords(Financial_Doc_Id financialDocId){
			logger.debug("CmGLAccountConstruction_Impl :: fetchRecords() method :: START");
			
			billId = financialDocId.getFinDocId();
			lineId = financialDocId.getLineId();

			StringBuilder sb = new StringBuilder();
			//To pick request entries on which payment has been applied
			sb.append(" SELECT S.PAY_DT PAY_DT, D.LINE_AMT LINE_AMT,D.UNPAID_AMT, S.TENDER_AMT TENDER_AMT,D.PAY_TYPE PAY_TYPE, D.CURRENCY_CD CURRENCY_CD, ");
			sb.append(" S.BANKING_ENTRY_STATUS STATUS, S.EXT_TRANSMIT_ID EXT_TRANSMIT_ID, S.EXT_SOURCE_CD EXT_SOURCE_CD FROM CM_PAY_STG S, CM_BILL_PAYMENT_DTL D        ");
			sb.append(" WHERE D.BILL_ID =S.FINANCIAL_DOC_ID AND S.FINANCIAL_DOC_LINE_ID =D.LINE_ID AND S.FINANCIAL_DOC_ID = :billId AND S.FINANCIAL_DOC_LINE_ID = :lineId AND ");
			sb.append(" PAY_DTL_ID =(SELECT MAX(PAY_DTL_ID) ");
			sb.append(" FROM CM_BILL_PAYMENT_DTL WHERE D.BILL_ID =BILL_ID AND LINE_ID =D.LINE_ID)                             ");
			sb.append(" AND NOT EXISTS (SELECT 1 FROM CM_BILL_PAYMENT_DTL WHERE BILL_ID=D.BILL_ID AND LINE_ID=D.LINE_ID ");
			sb.append(" AND trim(EXT_TRANSMIT_ID)=trim(S.EXT_TRANSMIT_ID)) ORDER BY S.ILM_DT  ");

			PreparedStatement ps = null;
			ps = createPreparedStatement(sb.toString(),"");
			ps.setAutoclose(false);
			ps.bindString(BILL_ID, billId, FINANCIAL_DOC_ID);
			ps.bindString(LINE_ID, lineId, FINANCIAL_DOC_LINE_ID);

			List<SQLResultRow> sqlRow = ps.list();


			if(ps != null){
				ps.close();
				ps = null;
			}
			logger.debug("CmGLAccountConstruction_Impl :: fetchRecords() method :: END");
			return sqlRow;
		}


		public void updateRecordStatus(List<String> billIds) {

			StringBuilder builder = new StringBuilder();
			StringBuilder stmt = new StringBuilder();
			PreparedStatement pStmt = null;

			for(String bill_Id : billIds) {
				builder.append("'").append(bill_Id).append("',");
			}

			stmt.append( "UPDATE CM_PAY_STG SET BO_STATUS_CD=:comp, STATUS_UPD_DTTM = SYSTIMESTAMP WHERE FINANCIAL_DOC_ID IN ( ") 
			.append( builder.deleteCharAt( builder.length() -1 )) 
			.append(" ) AND BO_STATUS_CD=:upld ");

			pStmt = createPreparedStatement(stmt.toString(), "updateRecordStatus");
			pStmt.bindString("upld",payConfirmationLookup.getUpload(), "BO_STATUS_CD");
			pStmt.bindString("comp", payConfirmationLookup.getCompleted(), "BO_STATUS_CD");
			pStmt.execute();

		}


		public void checkOverPayment(String status, BigDecimal prevUnpaidAmt, BigDecimal billAmt, BigDecimal unpaidAmt) {
			if (REJECTED.equalsIgnoreCase(status) || "WRITE_OFF_REV".equalsIgnoreCase(status)) {
				if (prevUnpaidAmt.compareTo(BigDecimal.ZERO) != 0 && billAmt.multiply(prevUnpaidAmt).compareTo(BigDecimal.ZERO) < 0) {
					overpayFlg = "Y";
				} else {
					overpayFlg = "";
				}
			} else if ((!"REQUEST".equalsIgnoreCase(status)) && unpaidAmt.compareTo(BigDecimal.ZERO) != 0 && billAmt.multiply(unpaidAmt).compareTo(BigDecimal.ZERO) < 0) {
				overpayFlg = "Y";
			} else {
				overpayFlg = "";
			}
		}

		// Inserting into Payment Detail Table
		private String insertPayDetail(String billId, String lineId, Date payDate, BigDecimal billAmt, String payType, BigDecimal prevUnpaidAmt, BigDecimal tenderAmt,
				BigDecimal unpaidAmt, String currencyCd, String status, String extTrnsmtId, String extSrcCd, String overpayFlg) {
			// TODO Auto-generated method stub
			boolean amtSignage = checkSinage(billAmt, tenderAmt);
			PreparedStatement stmt = null;
			StringBuilder sb = new StringBuilder();
			try {
				sb.append(" INSERT INTO CM_BILL_PAYMENT_DTL (PAY_DTL_ID, UPLOAD_DTTM, PAY_DT, BILL_ID,LINE_ID,LINE_AMT,PAY_TYPE,PREV_UNPAID_AMT,PAY_AMT, ");
				sb.append(" UNPAID_AMT,CURRENCY_CD,STATUS_CD, EXT_TRANSMIT_ID, STATUS_UPD_DTTM, EXT_SOURCE_CD, ");
				sb.append(" ILM_DT, OVERPAID, RECORD_STAT, OVERPAYMENT_RELEASE) ");
				sb.append(" VALUES(PAY_DTL_ID_SQ.NEXTVAL, :sysDttm, :payDate, trim(:billId), :lineId, ");
				sb.append(" :lineAmt, :payType, :prevUnpaidAmt, :tenderAmt, ");
				sb.append(" :unpaidAmt, :currencyCd, :status, trim(:eventId), :sysDttm, :extSrcCd, :sysDt, :overpayFlg, :pending, "
						+ ":overPaymentRelease) ");
				stmt = createPreparedStatement(sb.toString(), "");
				stmt.bindDate("payDate", payDate);
				stmt.bindString(BILL_ID, billId, "BILL_ID");
				stmt.bindDateTime("sysDttm", getSystemDateTime());
				stmt.bindString(LINE_ID, lineId.trim(), "LINE_ID");
				stmt.bindBigDecimal("lineAmt", billAmt);
				stmt.bindString("payType", payType, "PAY_TYPE");
				stmt.bindBigDecimal("prevUnpaidAmt", prevUnpaidAmt);
				stmt.bindBigDecimal("tenderAmt", tenderAmt);
				stmt.bindBigDecimal("unpaidAmt", unpaidAmt); 
				stmt.bindString("currencyCd", currencyCd, "CURRENCY_CD");
				stmt.bindString("status", status.trim(), "STATUS_CD");
				stmt.bindString("eventId", extTrnsmtId, "EXT_TRANSMIT_ID");
				stmt.bindString("extSrcCd", extSrcCd, "EXT_SOURCE_CD");
				stmt.bindString("overpayFlg", overpayFlg, "OVERPAID");
				stmt.bindDate("sysDt", getSystemDateTime().getDate());
				stmt.bindString("pending", payConfirmationLookup.getPending(), "RECORD_STAT");
				if ((status.trim().equals(RELEASED) && amtSignage) || (status.trim().equals(REJECTED) && !amtSignage)){
					stmt.bindString("overPaymentRelease", "Y", "OVERPAYMENT_RELEASE");
				}
				else {
					stmt.bindString("overPaymentRelease", " ", "OVERPAYMENT_RELEASE");
				}

				stmt.executeUpdate();
			}
			catch(Exception e){
				logger.error("Exception in execute Work Unit"+e);				
				return "false" + "~" + 
				getErrorDescription(String.valueOf(CustomMessageRepository.errorInsertingInTable("CM_BILL_PAYMENT_DTL")))+ "~"
				+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
				+ CustomMessages.ERROR_INSERTING_IN_TABLE;

			}
			finally{
				if(stmt !=null){
					stmt.close();
					stmt = null;
				}
			}
			return "true";
		}

		public boolean checkSinage(BigDecimal billAmt, BigDecimal tenderAmt){
			if (billAmt.compareTo(BigDecimal.ZERO) != 0 && tenderAmt.compareTo(BigDecimal.ZERO) != 0
					&& billAmt.multiply(tenderAmt).compareTo(BigDecimal.ZERO) > 0) {
				return  true;
				}
			else if (billAmt.compareTo(BigDecimal.ZERO) != 0 && tenderAmt.compareTo(BigDecimal.ZERO) != 0
					&& billAmt.multiply(tenderAmt).compareTo(BigDecimal.ZERO) < 0) {
				return false;
				}
			return false;
		}

	}

		/**
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 *
		 * @return errorInfo
		 */
  public static String getErrorDescription(String messageNumber) {
  	String errorInfo = " ";

  	errorInfo = CustomMessageRepository.merchantError(messageNumber).getMessageText();
	  if (errorInfo.contains("Text:") && errorInfo.contains("Description:")) {
	  	errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),	errorInfo.indexOf("Description:"));
		  }
		return errorInfo;
		}

  public static final class Financial_Doc_Id implements Id {

		public Financial_Doc_Id(String finDocId,
				String lineId) {
			this.finDocId = finDocId;
			this.lineId = lineId;
		}

		private static final long serialVersionUID = 1L;

		private String finDocId;
		private String lineId;

		public String getFinDocId() {
			return finDocId;
		}	

		public String getLineId() {
			return lineId;
		}

		public static long getSerialversionuid() {
			return serialVersionUID;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
			// appendContents
		}

	}

}
