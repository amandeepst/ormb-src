/*******************************************************************************
 * FileName                   : InboundPayments.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Aug 17, 2015
 * Version Number             : 0.8
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Aug 17, 2015		Sunaina		  Implemented all the requirements for CD2.
0.2		 NA				Oct 20, 2016		Preeti		  Production issue fix for Duplicate charging payments.
0.3		 NA				Nov 09, 2016		Preeti		  Update to handle Granularity Payments.
0.4		 NA				Jan 01, 2017		Preeti		  PAM-10445 Match event id and 0 payment segment issue fix.
0.5		 NA				Feb 20, 2017		Vienna		  Amendment for using adjustment to pay bill.
0.6		 NA				Mar 15, 2017		Preeti		  PAM-11520 Signage issue fix.
0.7		 NA				Aug 31, 2017		Ankur		  PAM-14246 Performance issue fix
0.8		 NA				Nov 06, 2017		Ankur		  PAM-15775 Performance issue fix          
0.9		 NA				Apr 20, 2018		Kaustubh K	  ILM changes and use bill characteristics to find bill amount           
********************************************************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.ListFilter;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.lookup.CharacteristicTypeLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.security.user.User_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.DepositControlStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.api.lookup.GlDistributionStatusLookup;
import com.splwg.ccb.api.lookup.MatchEventStatusLookup;
import com.splwg.ccb.api.lookup.PaymentStatusLookup;
import com.splwg.ccb.api.lookup.TenderControlStagingStatusLookup;
import com.splwg.ccb.api.lookup.TenderSourceTypeLookup;
import com.splwg.ccb.api.lookup.TenderStatusLookup;
import com.splwg.ccb.domain.adjustment.adjustment.CreateFrozenAdjustmentData;
import com.splwg.ccb.domain.admin.adjustmentType.AdjustmentType_Id;
import com.splwg.ccb.domain.admin.bank.BankAccount_Id;
import com.splwg.ccb.domain.admin.bank.Bank_Id;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.generalLedgerDivision.GeneralLedgerDivision_Id;
import com.splwg.ccb.domain.admin.matchType.MatchType_Id;
import com.splwg.ccb.domain.admin.tenderSource.TenderSource_Id;
import com.splwg.ccb.domain.admin.tenderType.TenderType_Id;
import com.splwg.ccb.domain.billing.bill.BillCharacteristic;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_DTO;
import com.splwg.ccb.domain.financial.matchEvent.MatchEvent_DTO;
import com.splwg.ccb.domain.financial.matchEvent.MatchEvent_Id;
import com.splwg.ccb.domain.payment.depositControl.DepositControl_DTO;
import com.splwg.ccb.domain.payment.depositControl.DepositControl_Id;
import com.splwg.ccb.domain.payment.depositControl.TenderDeposit_DTO;
import com.splwg.ccb.domain.payment.payment.PaymentSegment_DTO;
import com.splwg.ccb.domain.payment.payment.Payment_DTO;
import com.splwg.ccb.domain.payment.payment.Payment_Id;
import com.splwg.ccb.domain.payment.paymentEvent.PaymentEvent_DTO;
import com.splwg.ccb.domain.payment.paymentEvent.PaymentEvent_Id;
import com.splwg.ccb.domain.payment.paymentEvent.PaymentTender_DTO;
import com.splwg.ccb.domain.payment.tenderControl.TenderControl_DTO;
import com.splwg.ccb.domain.payment.tenderControl.TenderControl_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author rainas403
 *
 *  @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"})
 */
public class InboundPayments extends InboundPayments_Gen {

	private InboundPaymentsLookUp payConfirmationLookup = null;

	public static final Logger logger = LoggerFactory.getLogger(InboundPayments.class);

	public JobWork getJobWork() {
		
		//Initialize Lookup that stores various constants used by this interface.
		payConfirmationLookup = new InboundPaymentsLookUp();	

		//Updates the bill ids in error for invalid bill details
		validatePayConfirmation();
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList = getFinanicalDocIdData();
		payConfirmationLookup = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	//	*********************** validatePayConfirmation Method******************************

	/**
	 * validatePayConfirmation() method validates Financial Document Details input from CM_PAY_CNF_STG staging table.
	 * 
	 * @return 
	 */
	private void validatePayConfirmation() {
		
		PreparedStatement preparedStatement = null;
		String errorInfo=""; 
		StringBuilder stringBuilder = new StringBuilder();

		try {
			errorInfo = CustomMessageRepository.merchantError(
					String.valueOf(CustomMessages.PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS)).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			stringBuilder.append("UPDATE CM_PAY_CNF_STG CNF " );
			stringBuilder.append(" SET BO_STATUS_CD = :errorStatus, MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr," );
			stringBuilder.append(" ERROR_INFO = :errorInfo  WHERE BO_STATUS_CD = :uploadStatus " );
			stringBuilder.append(" AND NOT EXISTS (SELECT BILL.BILL_ID FROM CI_BILL BILL " );
			stringBuilder.append(" WHERE BILL.BILL_ID = CNF.FINANCIAL_DOC_ID) " );
			
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("uploadStatus", payConfirmationLookup.getUpload(), "BO_STATUS_CD");
			preparedStatement.bindString("errorStatus", payConfirmationLookup.getError(), "BO_STATUS_CD");
			preparedStatement.bindString("errorInfo", errorInfo, "ERROR_INFO");
			preparedStatement.bindString("msgCatNbr", String.valueOf(CustomMessages.MESSAGE_CATEGORY), "MESSAGE_CAT_NBR");
			preparedStatement.bindString("msgNbr", String.valueOf(CustomMessages.PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS), "MESSAGE_NBR");
			preparedStatement.executeUpdate();
			
		} catch (Exception e) {
			logger.error("Exception in validatePayConfirmation" , e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

	}


	//	*********************** getFinanicalDocIdData Method******************************

	/**
	 * getFinanicalDocIdData() method selects distinct Financial Document Ids 
	 * from CM_PAY_CNF_STG staging table in the order of the bill dates.
	 * 
	 * @return List Fin_Doc_Id
	 */
	private List<ThreadWorkUnit> getFinanicalDocIdData() {
		
		PreparedStatement preparedStatement = null;
		Fin_Doc_Id finDocId = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		ThreadWorkUnit threadworkUnit = null;
		StringBuilder stringBuilder = new StringBuilder();
		String financialDocId = "";
		
		try {
			stringBuilder.append("SELECT DISTINCT FINANCIAL_DOC_ID FROM CM_PAY_CNF_STG CNF " );
			stringBuilder.append("WHERE CNF.BO_STATUS_CD = :selectBoStatus1 " );
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("selectBoStatus1", payConfirmationLookup.getUpload(), "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);
			//HashSet<SQLResultRow> listHashSet=new HashSet<SQLResultRow>(preparedStatement.list()); this can be used to remove distinct
			for (SQLResultRow resultSet : preparedStatement.list()) {
				financialDocId = resultSet.getString("FINANCIAL_DOC_ID");
				finDocId = new Fin_Doc_Id(financialDocId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(finDocId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				finDocId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getFinanicalDocIdData" , e);
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



	public Class<InboundPaymentsWorker> getThreadWorkerClass() {
		return InboundPaymentsWorker.class;
	}

	public static class InboundPaymentsWorker extends
	InboundPaymentsWorker_Gen {

		private InboundPaymentsLookUp payConfirmationLookup = null;

		BigInteger seqNo = BigInteger.ZERO;
		String acctId = "";
		String acctNbr = "";
		String saId = "";
		String tenderType = "";
		String cisDivision = "";
		String matchEventId = "";
		ArrayList <List<String>> contractDetails = null;

		private ArrayList<ArrayList<String>> updateCustomerStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> eachCustomerStatusList = null;

		//		Default constructor
		public InboundPaymentsWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			
			if(payConfirmationLookup == null) {
				payConfirmationLookup = new InboundPaymentsLookUp();
			}
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			
			return new StandardCommitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * every row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to confirm payments or adjustments.
		 */

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			
			seqNo = BigInteger.ZERO;
			String createPayStatus = "";
			try {

				Fin_Doc_Id finDocId = (Fin_Doc_Id) unit.getPrimaryId();
				String skipRowsFlag = "false"; //This flag will be used to determine whether the rows should be skipped or not
				//as per the status of earlier rows processed with same Financial Document Id.
				
				// RIA: Read CM_ISGRN char from CI_BILL_CHAR
				boolean granularPayment = checkIfGranular(finDocId);

				List<PayConfirmation_Id> payCnfId = getPayConfirmationStgData(finDocId);
				for (int finDocCount=0; finDocCount<payCnfId.size(); finDocCount++) {

					boolean extractBillDetail = false;
					PayConfirmation_Id payCnf = payCnfId.get(finDocCount);
					removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
					setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));

					extractBillDetail = extractBillDetails(payCnf);
					if(!extractBillDetail) {
						return false;	
					}


					/********** Creating payments *************/
					createPayStatus = createPayments(payCnf,seqNo.add(BigInteger.ONE),granularPayment);

					if (CommonUtils.CheckNull(createPayStatus).trim().startsWith("false")) {
						String[] returnStatusArray = createPayStatus.split("~");
						if(returnStatusArray[1].contains("Text:")){
							returnStatusArray[1] = returnStatusArray[1].replace("Text:", "");
						}
						skipRowsFlag = "true";
						return logError(payCnf.getTxnHeaderId(), 
								payCnf.getBillId(), 
								payConfirmationLookup.getError(), 
								returnStatusArray[2].trim(), returnStatusArray[3].trim(), 
								returnStatusArray[1].trim(),skipRowsFlag);
					} else {
						updatePayCnfStaging(payCnf.getTxnHeaderId(),
								payCnf.getBillId(), 
								payConfirmationLookup.getCompleted(), 
								"0", "0", " ", skipRowsFlag, "Y");
					}
				}
			} catch(Exception e) {
				logger.error("Exception in executeWorkUnit()", e);
			}
			return true;
		}

		//		*********************** comparePayments Method******************************

		/**
		 * Check Bill Granularity
		 * @param finDocId
		 * @return
		 */
		private boolean checkIfGranular(Fin_Doc_Id finDocId) {
			boolean granularPayment = false;
			String strGranularPayment = null;
			Bill_Id billId = new Bill_Id(finDocId.getFinDocId().trim());
			CharacteristicType_Id charTypeId = new CharacteristicType_Id("CM_ISGRN");
			CharacteristicType charType = charTypeId.getEntity();
			ListFilter<BillCharacteristic> billCharFilter = billId.getEntity().getCharacteristics().createFilter(" WHERE this.id.characteristicType =:billCharType ", "CM_ISGRN");
			billCharFilter.bindId("billCharType", charTypeId);
			if(notNull(billCharFilter) && notNull(charType)) {
				BillCharacteristic billChar = (BillCharacteristic)billCharFilter.firstRow();
				if(notNull(billChar)) {
					CharacteristicTypeLookup charTypeLookUp = charType.getCharacteristicType();
					if ((charTypeLookUp.isAdhocValue()) || (charTypeLookUp.isFileLocationValue())) {
						strGranularPayment = billChar.getAdhocCharacteristicValue();
					} else if (charTypeLookUp.isPredefinedValue()) {
						strGranularPayment = billChar.getCharacteristicValue();
					} else if (charTypeLookUp.isForeignKeyValue()) {
						strGranularPayment = billChar.getCharacteristicValueForeignKey1();
					} 
				}
			}
			if(!isBlankOrNull(strGranularPayment)) {
				granularPayment = "Y".equalsIgnoreCase(strGranularPayment.trim());
			}
			return granularPayment;
		}

		/**
		 * createPayments() method creates payment for each input row
		 * @param granularPayment2 
		 * 
		 * @return 
		 */

		private String createPayments(PayConfirmation_Id payCnfId, BigInteger payUpldSeqNo, boolean granularPayment) {

			/************* Initiating DTOs ******************/
			PreparedStatement preparedStatement = null;
			DepositControl_DTO depCntl = new DepositControl_DTO();
			TenderControl_DTO tndrCntl = new TenderControl_DTO();
			TenderDeposit_DTO tndrDep = new TenderDeposit_DTO();
			PaymentEvent_DTO payEvnt = new PaymentEvent_DTO();
			PaymentTender_DTO payTndr = new PaymentTender_DTO(); 
			Payment_DTO pay = new Payment_DTO();
			PaymentSegment_DTO paySeg = new PaymentSegment_DTO();
			MatchEvent_DTO matchEvent = new MatchEvent_DTO();
			FinancialTransaction_DTO ft = new FinancialTransaction_DTO();

			/************* Initializing variables ******************/

			BigDecimal billAmt = BigDecimal.ZERO;
			BigDecimal payDueAmt = BigDecimal.ZERO; //payDueAmt is if anything is due after current payment being processed.
			BigDecimal payAmt = BigDecimal.ZERO; //Payment Amount to be processed.
			BigDecimal adjustmentAmt = BigDecimal.ZERO;
			BigDecimal amtForPayCreation = BigDecimal.ZERO;
			int payDueAmtZeroComparison = 0;
			
			Boolean isPayDueAmtMore = false;
			Boolean isPayDueAmtZero = false;

			String bankId = "";
			String tenderSource = "";
			String bankAcctKey = "";

			String adjustmentId = "";
			String adjustmentType = "";
			ServiceAgreement saIdEntity = null;
			StringBuilder stringBuilder = new StringBuilder();
			String returnCreatePaySegFTAndFtGl = "";

			try {
				
				seqNo = seqNo.add(BigInteger.ONE);
				payAmt = payAmt.add(new BigDecimal(payCnfId.getAmt())); 
				billAmt = billAmt.add(extractUnpaidBillAmt(payCnfId));

				/*** Comparing Payment Amount with Bill amount to determine if the adjustment needs to be created after this payment ***/	
				
				//************************Granularity Payments-consider as partial payment and balanced for the last entry*******************//				
//				stringBuilder.append("SELECT BILL_ID FROM CM_PAY_REQ " );
//				stringBuilder.append("WHERE BILL_ID=:billId");
//
//				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
//				preparedStatement.bindString("billId", payCnfId.getBillId().trim(), "BILL_ID");
//				preparedStatement.setAutoclose(false);
//				if(preparedStatement.list().size()>1){
//					granularPayment = true;
//				}
//				preparedStatement.close();
				//************************Granularity Payments-consider as partial payment and balanced for the last entry*******************//		

				//************************COMPARISON LOGIC-START*******************//
				if((payAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) == 0) {
					return "false" + "~" + 
							"Payment amount is 0" + "~"
							+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
							+ CustomMessages.PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS;
				}

				Boolean adjustmentFlag=false;
				if((billAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) == 0) {
					adjustmentFlag=true;
				}else{
					if(!granularPayment){
						if (((payAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) > 0)
								&& ((billAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) < 0)) {
							return "false" + "~" + 
									"Bill Amount and Payment amount have different signage" + "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS;
						}
						if (((payAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) < 0)
								&& ((billAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) > 0)) {
							return "false" + "~" + 
									"Bill Amount and Payment amount have different signage" + "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS;
						}


						//Payment amount and Bill amount have same sign
						Boolean changeSignFlag=false;
						if (((payAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) < 0)
								&& ((billAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) < 0)) {
							changeSignFlag=true;
							payAmt=payAmt.negate();
							billAmt=billAmt.negate();
						}
						payDueAmt = (billAmt).subtract(payAmt);
						payDueAmtZeroComparison = payDueAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2));
						if(payDueAmtZeroComparison == 0) {
							
							isPayDueAmtZero = true;
							if(changeSignFlag){
								payAmt=payAmt.negate();
								billAmt=billAmt.negate();
							}
							amtForPayCreation = billAmt;
						}else if (payDueAmtZeroComparison < 0) {
							
							isPayDueAmtMore = true;
							if(changeSignFlag){
								payAmt=payAmt.negate();
								billAmt=billAmt.negate();
							}
							amtForPayCreation = billAmt;
						}else if (payDueAmtZeroComparison > 0) {
							
							if(changeSignFlag){
								payAmt=payAmt.negate();
								billAmt=billAmt.negate();
							}
							amtForPayCreation = payAmt;
						}
					}else{
						//granularity-start
						payDueAmt = (billAmt).subtract(payAmt);
						payDueAmtZeroComparison = payDueAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2));
						if(payDueAmtZeroComparison == 0) {
							
							isPayDueAmtZero = true;
							amtForPayCreation = billAmt;
						}else {
													
							amtForPayCreation = payAmt;
						}
						//granularity-end
					}
				}//else not true adjustment flag
				
				//************************COMPARISON LOGIC-END*******************//			

				
				
				//Start-If the bill amount/unpaid amount is not zero
				if (!adjustmentFlag) {
					
					if(contractDetails.size()==0){
						return "false" + "~" + getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_NO_SA_ID)) + "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ String.valueOf(CustomMessages.PAY_CNF_NO_SA_ID);

					}
					
					/********** Deposit Control Set Up *************/

					
					depCntl.setTenderSourceType(TenderSourceTypeLookup.constants.AD_HOC);
					depCntl.setDepositControlStatus(DepositControlStatusLookup.constants.OPEN);
					depCntl.setCreationDateTime(getSystemDateTime());
					depCntl.setUserId(new User_Id(payConfirmationLookup.getUserId()));
					depCntl.setCurrencyId(new Currency_Id(payCnfId.getCurrencyCode()));
					depCntl.setBalancedUserId(new User_Id(payConfirmationLookup.getUserId()));
					depCntl.setBalancedDateTime(getSystemDateTime());
					depCntl.setEndingBalance(new Money(amtForPayCreation,new Currency_Id(payCnfId.getCurrencyCode())));
					depCntl.newEntity();
					
					DepositControl_Id depositControlId=depCntl.getId();
					
					
					/********** Tender Control Set Up and Tender Deposit Control Set Up *************/
				
					String payExtSrcId = payCnfId.getExtSourceId().trim();
					String payCurrency = payCnfId.getCurrencyCode().trim();
					Currency_Id currId = new Currency_Id(payCurrency);
					
		
					if ((payExtSrcId.length() <= AdjustmentType_Id.FIELD_SIZE) && notNull(new AdjustmentType_Id(payExtSrcId).getEntity())) {
						
						tenderSource = payConfirmationLookup.getTenderSource();
						bankAcctKey = payConfirmationLookup.getBankAcctKey();
						bankId = payConfirmationLookup.getBankId();
					} else {
							
						// Tender Source Query
						stringBuilder = new StringBuilder();
						stringBuilder.append(" SELECT TNDR_SOURCE_CD FROM CI_TNDR_SRCE " );
						stringBuilder.append("  WHERE EXT_SOURCE_ID = :source AND CURRENCY_CD = :currencyCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("source", payExtSrcId, "EXT_SOURCE_ID");
						preparedStatement.bindString("currencyCode", payCurrency, "CURRENCY_CD");
						preparedStatement.setAutoclose(false);
						if(preparedStatement.list().size()==1){
							tenderSource = preparedStatement.firstRow().getString("TNDR_SOURCE_CD").trim();
						} else {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_BANK_ACCT_DETAILS))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_BANK_ACCT_DETAILS;
						}
						preparedStatement.close();
						
						// Bank Details Query
						stringBuilder = new StringBuilder();
						stringBuilder.append(" SELECT BANK_CD, BANK_ACCT_KEY FROM CI_BANK_ACCOUNT " );
						stringBuilder.append(" WHERE ACCOUNT_NBR = :acctNbr AND CURRENCY_CD = :currencyCode ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("acctNbr", payExtSrcId, "DST_ID");//ACCOUNT_NBR
						preparedStatement.bindString("currencyCode", payCurrency, "CURRENCY_CD");
						preparedStatement.setAutoclose(false);
						if(preparedStatement.list().size()==1){
							bankAcctKey = preparedStatement.firstRow().getString("BANK_ACCT_KEY").trim();
							bankId = preparedStatement.firstRow().getString("BANK_CD").trim();
						} else {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_BANK_ACCT_DETAILS))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_BANK_ACCT_DETAILS;
						}
						preparedStatement.close();
					}
					
					if((!notBlank(tenderSource)) || (!notBlank(bankId)) || (!notBlank(bankAcctKey))) {
						
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_BANK_ACCT_DETAILS))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_BANK_ACCT_DETAILS;	
					}
					
					
					tndrCntl.setCurrencyId(currId);
					tndrCntl.setTenderSourceId(new TenderSource_Id(tenderSource));
					tndrCntl.setTenderControlStagingStatus(TenderControlStagingStatusLookup.constants.OPEN);
					tndrCntl.setUserId(new User_Id(payConfirmationLookup.getUserId()));
					tndrCntl.setCreationDateTime(getSystemDateTime());
					tndrCntl.setDepositControlId(depositControlId);
					tndrCntl.setBalancedUserId(new User_Id(payConfirmationLookup.getUserId()));
					tndrCntl.setBalancedDateTime(getSystemDateTime());
					tndrCntl.setStartingBalance(Money.ZERO);
					tndrCntl.setEndingBalance(new Money(amtForPayCreation,currId));
					tndrCntl.setIsAllUsers(Bool.FALSE);
					tndrCntl.setExternalTransmissionId(payCnfId.getExtRefId());
					tndrCntl.newEntity();
					TenderControl_Id tenderControlId = tndrCntl.getId();
					
					tndrDep.setBankAccountId(new BankAccount_Id(new Bank_Id (bankId),bankAcctKey)); 
					tndrDep.setCurrencyId(currId);
					tndrDep.setDepositAmount(new Money(amtForPayCreation,currId));
					tndrDep.setDepositControlId(depositControlId);
					tndrDep.newEntity();
					
					MatchEvent_Id matchEventIdObject= null; 
					if(!notBlank(matchEventId)){
						/********** Match Event Set Up *************/
						
						matchEvent.setAccountId(new Account_Id(acctId));
						matchEvent.setMatchEventStatus(MatchEventStatusLookup.constants.OPEN);
						matchEvent.setIsDisputed(Bool.FALSE);
						matchEvent.setCreatedDate(payCnfId.getPayDate());
						matchEvent.setIsArchived(Bool.FALSE);
						matchEvent.newEntity();
						matchEventIdObject  = matchEvent.getId();
						
					}

					/********** Payment Event Set Up *************/
					payEvnt.setCreationDateTime(getSystemDateTime());
					payEvnt.setPaymentDate(payCnfId.getPayDate());
					payEvnt.setExtraTCopybookFieldValue("ACCT_ID_T", acctId); //Set as required for internal processing.
					payEvnt.newEntity();
					PaymentEvent_Id paymentEventId = payEvnt.getId();
					

					/********** Payment Upload Staging Set Up *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CI_PEVT_DTL_ST " );
					stringBuilder.append(" (EXT_SOURCE_ID,EXT_TRANSMIT_ID,PEVT_DTL_SEQ,PEVT_STG_ST_FLG,DST_RULE_CD,DST_RULE_VALUE," );
					stringBuilder.append(" CURRENCY_CD,TENDER_AMT,ACCOUNTING_DT,TENDER_TYPE_CD,CHECK_NBR,MICR_ID,CUST_ID,NAME1," );
					stringBuilder.append(" EXT_REFERENCE_ID,MATCH_TYPE_CD,MATCH_VAL,TNDR_CTL_ID,ACCT_ID,PAY_EVENT_ID,PEVT_PROCESS_ID," );
					stringBuilder.append(" VERSION,APAY_SRC_CD,EXT_ACCT_ID,EXPIRE_DT,ENTITY_NAME) " );
					stringBuilder.append(" VALUES (:extSourceId, :extTransmitId, :pevtDtlSeq, :pevtStgStFlg, :dstRuleCode, :dstRuleVal, " );
					stringBuilder.append(" :currencyCode, :tenderAmt, :accountingDt,:tenderTypeCode,' ',' ',' ',' ', " );
					stringBuilder.append(" :extRefId, :matchType, :matchValBill,:tenderCtlId, :accountId,:payEventId,:payEventprocessId, " );
					stringBuilder.append(" 1,' ',' ',NULL,' ')");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("extSourceId", payCnfId.getExtSourceId(), "EXT_SOURCE_ID");
					preparedStatement.bindString("extTransmitId", payCnfId.getExtRefId(), "EXT_TRANSMIT_ID");
					preparedStatement.bindString("pevtDtlSeq", String.valueOf(seqNo),"PEVT_DTL_SEQ");
					preparedStatement.bindString("pevtStgStFlg", payConfirmationLookup.getInitPevtStatus(), "PEVT_STG_ST_FLG");
					preparedStatement.bindString("dstRuleCode", payConfirmationLookup.getDstRuleCode(), "DST_RULE_CD");
					preparedStatement.bindString("dstRuleVal", saId, "DST_RULE_VALUE");
					preparedStatement.bindString("currencyCode", payCnfId.getCurrencyCode(), "CURRENCY_CD");
					preparedStatement.bindBigDecimal("tenderAmt", new BigDecimal(payCnfId.getAmt()));
					preparedStatement.bindDate("accountingDt", payCnfId.getPayDate());
					preparedStatement.bindString("tenderTypeCode", tenderType, "TENDER_TYPE_CD");
					preparedStatement.bindString("extRefId", payCnfId.getExtRefId(), "EXT_REFERENCE_ID");
					preparedStatement.bindString("matchType", "BILL-ID", "MATCH_TYPE_CD");
					preparedStatement.bindString("matchValBill", payCnfId.getBillId(), "MATCH_VAL");
					preparedStatement.bindString("accountId", acctId, "ACCT_ID");
					preparedStatement.bindId("payEventId", paymentEventId);
					preparedStatement.bindId("tenderCtlId", tenderControlId);
					preparedStatement.bindString("payEventprocessId", acctId, "PEVT_PROCESS_ID");
					int row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
						getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_PAY_UPLD_NOT_UPDATED))+ "~"
						+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
						+ CustomMessages.PAY_CNF_PAY_UPLD_NOT_UPDATED;
					}
					preparedStatement.close();
					
					
					/********** Payment Event Distribution Detail Set Up *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CI_PEVT_DST_DTL " );
					stringBuilder.append(" (PAY_EVENT_ID,CHAR_TYPE_CD,SEQ_NUM,DST_RULE_CD,AMOUNT,CHAR_VAL,ADHOC_CHAR_VAL, " );
					stringBuilder.append(" CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4,CHAR_VAL_FK5,SRCH_CHAR_VAL,VERSION) " );
					stringBuilder.append(" VALUES (:payEvntId,:saTypeCode,:seqNo,:dstRuleCode,:amt,' ',:saIdChar,' ',' ',' ',' ',' ',:searchSaId,1) ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindId("payEvntId", paymentEventId);
					preparedStatement.bindString("saTypeCode", payConfirmationLookup.getDstRuleCode(), "CHAR_TYPE_CD");
					preparedStatement.bindBigInteger("seqNo", seqNo);
					preparedStatement.bindString("dstRuleCode", payConfirmationLookup.getDstRuleCode(), "DST_RULE_CD");
					preparedStatement.bindBigDecimal("amt", amtForPayCreation);
					preparedStatement.bindString("saIdChar", saId, "ADHOC_CHAR_VAL");
					preparedStatement.bindString("searchSaId", saId, "SRCH_CHAR_VAL");
					row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_PEVT_DST_DTL_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_PEVT_DST_DTL_NOT_UPDATED;
					}
					preparedStatement.close();

					
					/********** Payment Tender Set Up *************/
					payTndr.setCurrencyId(new Currency_Id(payCnfId.getCurrencyCode()));
					payTndr.setPayorAccountId(new Account_Id(acctId)); 
					payTndr.setTenderTypeId(new TenderType_Id(tenderType));
					payTndr.setTenderControlId(tenderControlId);
					payTndr.setIsIncludedInTenderControlBalance(Bool.TRUE);
					payTndr.setTenderAmount(new Money(amtForPayCreation,new Currency_Id(payCnfId.getCurrencyCode())));
					payTndr.setTenderStatus(TenderStatusLookup.constants.VALID);
					payTndr.setExternalReferenceId(payCnfId.getExtRefId());
					payTndr.setPaymentEventId(paymentEventId);
					payTndr.newEntity();
					

					/********** Payment Set Up *************/
					pay.setPaymentEventId(paymentEventId);
					pay.setCurrencyId(new Currency_Id(payCnfId.getCurrencyCode()));
					pay.setAccountId(new Account_Id(acctId)); 
					pay.setPaymentStatus(PaymentStatusLookup.constants.FREEZABLE);
					pay.setPaymentAmount(new Money(amtForPayCreation, new Currency_Id(payCnfId.getCurrencyCode())));
					pay.setMatchTypeId(new MatchType_Id(payConfirmationLookup.getMatchType()));
					pay.setMatchValue(payCnfId.getBillId()); 
					pay.newEntity();
					Payment_Id paymentId = pay.getId();
					
		
					//Start-Over Payment or Balanced Payment scenario
					if(isPayDueAmtMore || isPayDueAmtZero) {
						/********** Two Payment Segments to be created for payment amount if there are more than 1 contract billed ************/
						BigDecimal paySegAmt = BigDecimal.ZERO;
						BigDecimal amtInSa = BigDecimal.ZERO;
						String paySaId= "";
						int seq = 0;

						/********** One Payment Segment to be created for recurring amount ************/

						for (int i =0; i<contractDetails.size(); i++){
							seq=1;
							paySaId =contractDetails.get(i).get(0).toString();
							amtInSa = new BigDecimal(contractDetails.get(i).get(2).toString());
							paySegAmt = amtInSa;
							returnCreatePaySegFTAndFtGl=createPaySegFTAndFtGl(paySeg,payCnfId,paySegAmt,paymentId ,paySaId,ft,seq,matchEventIdObject,i);
							if(returnCreatePaySegFTAndFtGl.startsWith("false")){
								return returnCreatePaySegFTAndFtGl;
							}
						}
						//End-Over Payment or Balanced Payment scenario
					} else {
						//Start-Under payment scenario
						
						/********** Two Payment Segments to be created for payment amount if there are more than 1 contract billed ************/
						BigDecimal paySegAmt = BigDecimal.ZERO;
						BigDecimal amtInSa = BigDecimal.ZERO;
						String paySaId="";
						String contractType="";

						/********** One Payment Segment to be created for recurring amount ************/
						//For loop for each contract
						for (int i =0; i<contractDetails.size(); i++){
							if (!granularPayment){

							int seq = 1;
							Boolean paySegSignFlag=false;
							paySaId =contractDetails.get(i).get(0).toString();
							amtInSa = new BigDecimal(contractDetails.get(i).get(2).toString());

							//Only for contracts having balances as same sign as in payment
							if ((amtInSa.signum()>0 && payAmt.signum()>0) || (amtInSa.signum()<0 && payAmt.signum()<0)){  
								//change to positive
								if (payAmt.signum()<0 && paySegAmt.signum()<0){
									paySegSignFlag=true;
									payAmt=payAmt.negate();
									paySegAmt=paySegAmt.negate();									
								}
								//to determine new payment amount after multiple payments
								payAmt = payAmt.subtract(paySegAmt);
								if((payAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))) == 0) {
									break;
								}
								//Back to original sign
								if (paySegSignFlag){
									payAmt=payAmt.negate();
									paySegAmt=paySegAmt.negate();	
								}	
								
								//determine payment segment amount
								if((amtInSa.compareTo(payAmt) < 0) && (amtInSa.signum()>0 && payAmt.signum()>0)){
									paySegAmt = amtInSa;
								} else if ((amtInSa.compareTo(payAmt) > 0) && (amtInSa.signum()<0 && payAmt.signum()<0)){
									paySegAmt = amtInSa;
								} else {
									paySegAmt = payAmt;
								}
								
								
								if((paySegAmt.setScale(2).compareTo(BigDecimal.ZERO.setScale(2))!=0)){
								
									returnCreatePaySegFTAndFtGl=createPaySegFTAndFtGl(paySeg,payCnfId,paySegAmt,paymentId ,paySaId,ft,seq,matchEventIdObject,i);
									if(returnCreatePaySegFTAndFtGl.startsWith("false")){
										return returnCreatePaySegFTAndFtGl;
										}
									}//pay seg not 0
								}//if signum
							}else{
								//Granularity
								
								contractType=contractDetails.get(i).get(1).toString().trim();
								if(contractType.equals("FUND")||contractType.equals("CHRG")||contractType.equals("CRWD")||contractType.equals("CHBK")){
								int seq = 1;
								paySaId =contractDetails.get(i).get(0).toString();
								amtInSa = new BigDecimal(contractDetails.get(i).get(2).toString());
								paySegAmt = payAmt;
								
								returnCreatePaySegFTAndFtGl=createPaySegFTAndFtGl(paySeg,payCnfId,paySegAmt,paymentId ,paySaId,ft,seq,matchEventIdObject,i);
								if(returnCreatePaySegFTAndFtGl.startsWith("false")){
									return returnCreatePaySegFTAndFtGl;
									}
								}
								//Granularity
								}//ELSE GRANULARITY
						}//for loop							
						//End-Under payment scenario
					}
					
					//Start-Adjustment creation for over payment scenario*************************************************************
					if(isPayDueAmtMore){
						Boolean adjSignFlag=false;
						if (payAmt.signum()<0 && billAmt.signum()<0){
							adjSignFlag=true;
							payAmt=payAmt.negate();
							billAmt=billAmt.negate();									
						}
						//to determine adjustment amount for over payment
						adjustmentAmt = payAmt.subtract(billAmt);
						//Back to original sign
						if (adjSignFlag){
							payAmt=payAmt.negate();
							billAmt=billAmt.negate();	
							adjustmentAmt=adjustmentAmt.negate();
						}						
					
					adjustmentType = getAdjustmentType();

					/********** Adjustment amount for extra payment amount ************/
					saIdEntity = new ServiceAgreement_Id(saId).getEntity();					
					CreateFrozenAdjustmentData frozenAdjustment = CreateFrozenAdjustmentData.Factory.newInstance();
					frozenAdjustment.setServiceAgreement(saIdEntity); 
					frozenAdjustment.setAdjustmentType(new AdjustmentType_Id(adjustmentType).getEntity());
					frozenAdjustment.setAdjustmentAmount(new Money(adjustmentAmt.negate(), new Currency_Id(payCnfId.getCurrencyCode())));
					frozenAdjustment.setAdjustmentDate(payCnfId.getPayDate());
					frozenAdjustment.setAllowZeroAmount(Bool.FALSE);
					saIdEntity.createFrozenAdjustment(frozenAdjustment);
					adjustmentId = frozenAdjustment.getAdjustment().getId().getIdValue();


					/********** Adjustment characteristic for extra payment amount identification ************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CI_ADJ_CHAR " );
					stringBuilder.append(" (ADJ_ID,CHAR_TYPE_CD,SEQ_NUM,VERSION,CHAR_VAL," );
					stringBuilder.append(" ADHOC_CHAR_VAL,CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4," );
					stringBuilder.append(" CHAR_VAL_FK5,SRCH_CHAR_VAL) " );
					stringBuilder.append(" VALUES (:adjId, :payIdChar,:seq,1,'                '," );
					stringBuilder.append(" :payId,' ',' ',' ',' ',' ', :payId) ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("adjId", adjustmentId, "ADJ_ID");
					preparedStatement.bindString("payIdChar", payConfirmationLookup.getPayIdChar(), "CHAR_TYPE_CD");
					preparedStatement.bindBigInteger("seq", seqNo);
					preparedStatement.bindString("payId", paymentId.getIdValue(), "ADHOC_CHAR_VAL");
					row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_ADJ_CHAR_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_ADJ_CHAR_NOT_UPDATED;
					}
					preparedStatement.close();	
				}//if over payment
					//End-Adjustment creation for over payment scenario*********************************************************
					
					//Start-Common logic for all payment scenarios
					/********** Tender Ending Balance Set Up *************/
					
					if(notNull(tenderControlId)) {
						
						stringBuilder = new StringBuilder();
						stringBuilder.append("Insert into CI_TNDR_END_BAL " );
						stringBuilder.append(" (TNDR_CTL_ID,TENDER_TYPE_CD,ENDING_AMT) " );
						stringBuilder.append(" values (:tenderCtlId, :tenderTypeCode, :amt)");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindId("tenderCtlId", tenderControlId);
						preparedStatement.bindString("tenderTypeCode", tenderType, "TENDER_TYPE_CD");
						preparedStatement.bindBigDecimal("amt", amtForPayCreation);
						row = preparedStatement.executeUpdate();
						
						if (row==0) {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_TNDR_END_BAL_NOT_UPDATED))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_TNDR_END_BAL_NOT_UPDATED;
						}
						preparedStatement.close();
					}

					/********** Linking Match Event Id to Bill Id only in case it was not previously linked *************/
					if(!notBlank(matchEventId)){
						stringBuilder = new StringBuilder();
						stringBuilder.append("UPDATE CI_FT SET MATCH_EVT_ID = :matchEvtId WHERE BILL_ID = :billId");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("billId", payCnfId.getBillId(), "BILL_ID");
						preparedStatement.bindId("matchEvtId", matchEventIdObject);
						row = preparedStatement.executeUpdate();
						
						if (row==0) {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_BILL_MATCH_EVT_NOT_UPDATED))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_BILL_MATCH_EVT_NOT_UPDATED;
						}
						preparedStatement.close();
					}
					//End-Common logic for all payment scenarios
					
					//Start-Update status to Balanced in case of Over payment and Balanced payment
					if(isPayDueAmtZero || isPayDueAmtMore){
						
						//If any match event was created for the current bill id being processed previously, 
						// then we will link the current payment amount with previous open match event
						if(!notBlank(matchEventId)){
							stringBuilder = new StringBuilder();
							stringBuilder.append("UPDATE CI_MATCH_EVT SET MEVT_STATUS_FLG='B' WHERE MATCH_EVT_ID = :matchEventId");
							preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
							preparedStatement.bindId("matchEventId", matchEventIdObject);
							row = preparedStatement.executeUpdate();
							
							if (row==0) {
								return "false" + "~" + 
										getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED))+ "~"
										+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
										+ CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED;
							}
							preparedStatement.close();

						} else {

							/********** Setting match event as balanced *************/
							stringBuilder = new StringBuilder();
							stringBuilder.append("UPDATE CI_MATCH_EVT SET MEVT_STATUS_FLG= 'B' WHERE MATCH_EVT_ID = :matchEvtId");
							preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
							preparedStatement.bindString("matchEvtId", matchEventId, "MATCH_EVT_ID");
							row = preparedStatement.executeUpdate();
							
							if (row==0) {
								return "false" + "~" + 
										getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED))+ "~"
										+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
										+ CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED;
							}
							preparedStatement.close();
						}
						
						/********** Setting merchant as balanced in Due date table *************/
						int dueDtRowUpdated = updateCmBillDueDtTbl(payCnfId.getBillId());
						if (payCnfId.getBankingEntryStatus().trim().equalsIgnoreCase("RELEASED")){
							if (dueDtRowUpdated==0) {
								return "false" + "~" + 
										getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_DUE_DT))+ "~"
										+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
										+ CustomMessages.PAY_CNF_DUE_DT;
							}
						}
						//End-Update status to Balanced in case of Over payment and Balanced payment
					}//if over or balanced
					
					//Start-Common logic for all payment scenarios-1

					/********** Setting tender control as balanced *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CI_TNDR_CTL SET TNDR_CTL_ST_FLG= :tenderStatusFlg WHERE TNDR_CTL_ID = :tndrCtlId");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("tenderStatusFlg", "30", "TNDR_CTL_ST_FLG");
					preparedStatement.bindId("tndrCtlId", tenderControlId);
					row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_TNDR_CTL_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_TNDR_CTL_NOT_UPDATED;
					}
					preparedStatement.close();

					/********** Setting deposit control as balanced *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CI_DEP_CTL SET DEP_CTL_STATUS_FLG = :depStatusFlg WHERE DEP_CTL_ID = :depCtlId");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("depStatusFlg", "30", "DEP_CTL_STATUS_FLG");
					preparedStatement.bindId("depCtlId", depositControlId);
					row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_DEP_CTL_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_DEP_CTL_NOT_UPDATED;
					}
					preparedStatement.close();

					/********** Setting payment status as Frozen *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CI_PAY SET PAY_STATUS_FLG= :payStatusFlg WHERE PAY_ID = :payId");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("payStatusFlg", "50", "PAY_STATUS_FLG");
					preparedStatement.bindId("payId", paymentId);
					row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_PAY_FROZEN_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_PAY_FROZEN_NOT_UPDATED;
					}
					preparedStatement.close();

					/********** Payment Upload Staging Set Up *************/
					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CI_PEVT_DTL_ST set PEVT_STG_ST_FLG = :payEventStgFlag " );
					stringBuilder.append(" WHERE EXT_SOURCE_ID = :extSourceId AND EXT_TRANSMIT_ID=:extTransmitId AND PEVT_DTL_SEQ = :seqNum");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("payEventStgFlag", "40", "PEVT_STG_ST_FLG");
					preparedStatement.bindString("extSourceId", payCnfId.getExtSourceId(), "EXT_SOURCE_ID");
					preparedStatement.bindString("extTransmitId", payCnfId.getExtRefId(), "EXT_TRANSMIT_ID");
					preparedStatement.bindString("seqNum", String.valueOf(payUpldSeqNo), "PEVT_DTL_SEQ");
					row = preparedStatement.executeUpdate();
					
					if(row == 0){
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_PAY_UPLD_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_PAY_UPLD_NOT_UPDATED;
					}
					preparedStatement.close();
					
					//End-Common logic for all payment scenarios-1					
					//End-If the bill amount/unpaid amount is not zero
				} else {	
					//Start-If the bill amount/unpaid amount is zero
					adjustmentType = getAdjustmentType();

					/********** Adjustment amount for extra payment amount ************/
					
					saIdEntity = new ServiceAgreement_Id(saId).getEntity();
					adjustmentAmt = payAmt;
					CreateFrozenAdjustmentData frozenAdjustment = CreateFrozenAdjustmentData.Factory.newInstance();
					frozenAdjustment.setServiceAgreement(saIdEntity);
					frozenAdjustment.setAdjustmentType(new AdjustmentType_Id(adjustmentType).getEntity());
					frozenAdjustment.setAdjustmentAmount(new Money(adjustmentAmt.negate(), new Currency_Id(payCnfId.getCurrencyCode())));
					frozenAdjustment.setAdjustmentDate(payCnfId.getPayDate());
					frozenAdjustment.setAllowZeroAmount(Bool.FALSE);
					saIdEntity.createFrozenAdjustment(frozenAdjustment);
					adjustmentId = frozenAdjustment.getAdjustment().getId().getIdValue();
					

					/********** Adjustment characteristic for extra payment amount identification ************/
					
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CI_ADJ_CHAR " );
					stringBuilder.append(" (ADJ_ID,CHAR_TYPE_CD,SEQ_NUM,VERSION,CHAR_VAL," );
					stringBuilder.append(" ADHOC_CHAR_VAL,CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4," );
					stringBuilder.append(" CHAR_VAL_FK5,SRCH_CHAR_VAL) " );
					stringBuilder.append(" VALUES (:adjId, :payIdChar,:seq,1,'                '," );
					stringBuilder.append(" :payId,' ',' ',' ',' ',' ', :payId) ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("adjId", adjustmentId, "ADJ_ID");
					preparedStatement.bindString("payIdChar", payConfirmationLookup.getPayIdChar(), "CHAR_TYPE_CD");
					preparedStatement.bindBigInteger("seq", seqNo);
					preparedStatement.bindString("payId", adjustmentId, "ADHOC_CHAR_VAL");
					int row = preparedStatement.executeUpdate();
					
					if (row==0) {
						return "false" + "~" + 
								getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_ADJ_CHAR_NOT_UPDATED))+ "~"
								+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
								+ CustomMessages.PAY_CNF_ADJ_CHAR_NOT_UPDATED;
					}
					preparedStatement.close();

					if(!notBlank(matchEventId)){
						/********** Match Event Set Up *************/
						matchEvent.setAccountId(new Account_Id(acctId));
						matchEvent.setMatchEventStatus(MatchEventStatusLookup.constants.BALANCED);
						matchEvent.setIsDisputed(Bool.FALSE);
						matchEvent.setCreatedDate(payCnfId.getPayDate());
						matchEvent.setIsArchived(Bool.FALSE);
						matchEvent.newEntity();
						MatchEvent_Id matchEventIdObject = matchEvent.getId();
						

						stringBuilder = new StringBuilder();
						stringBuilder.append("UPDATE CI_FT SET MATCH_EVT_ID = :matchEvtId WHERE BILL_ID = :billId");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("billId", payCnfId.getBillId(), "BILL_ID");
						preparedStatement.bindId("matchEvtId", matchEventIdObject);
						row = preparedStatement.executeUpdate();
						
						if (row==0) {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_BILL_MATCH_EVT_NOT_UPDATED))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_BILL_MATCH_EVT_NOT_UPDATED;
						}
						preparedStatement.close();

					} else {

						/********** Setting match event as balanced *************/
						stringBuilder = new StringBuilder();
						stringBuilder.append("UPDATE CI_MATCH_EVT SET MEVT_STATUS_FLG= 'B' WHERE MATCH_EVT_ID = :matchEvtId");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("matchEvtId", matchEventId, "MATCH_EVT_ID");
						row = preparedStatement.executeUpdate();
						
						if (row==0) {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED;
						}
						preparedStatement.close();

					}
					/********** Setting merchant as balanced in Due date table *************/
					int dueDtRowUpdated = updateCmBillDueDtTbl(payCnfId.getBillId());
					if (payCnfId.getBankingEntryStatus().trim().equalsIgnoreCase("RELEASED")){
						if (dueDtRowUpdated==0) {
							return "false" + "~" + 
									getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_DUE_DT))+ "~"
									+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
									+ CustomMessages.PAY_CNF_DUE_DT;
						}
					}
					//End-If the bill amount/unpaid amount is zero
				}//else if bill amount zero-adjustment flag
				
			} catch (Exception e) {
				logger.error("Inside catch block of createPayments() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}

			return "true";
		}
		
		
		public String createPaySegFTAndFtGl(PaymentSegment_DTO paySeg,PayConfirmation_Id payCnfId,BigDecimal paySegAmt,
				Payment_Id paymentId ,String paySaId,FinancialTransaction_DTO ft,int seq,MatchEvent_Id matchEventIdObject,int i)
		{
			String siblingId1 = "";
			String ft1 = "";
			String returnCreateFtGlStatus = "";
			
			/********** Pay Segment for balanced bill amount ************/
			String returnCreatePayStatus = createPaySeg(paySeg, payCnfId.getCurrencyCode(), paySegAmt, paymentId, paySaId);
			if(returnCreatePayStatus.startsWith("false")){
				return returnCreatePayStatus;
			} else {
				siblingId1 = returnCreatePayStatus;
			}

			/********** One FT to be created for current payment segment ************/
			if( notNull(paymentId) && notBlank(siblingId1) ){
				
				//If any match event was created for the current bill id being processed previously, 
				// then we will link the current payment amount with previous open match event
				if(notBlank(matchEventId)){
					MatchEvent_Id prevOpenMatchEvtId = new MatchEvent_Id(matchEventId);
					String returnCreateFTStatus = createFT(ft, siblingId1, paySegAmt.negate(), 
							paymentId, prevOpenMatchEvtId, payCnfId, paySaId);

					if(returnCreateFTStatus.startsWith("false")){
						return returnCreateFTStatus;
					} else {
						ft1 = returnCreateFTStatus;
					}
				} else {

					String returnCreateFTStatus = createFT(ft, siblingId1, paySegAmt.negate(), 
							paymentId, matchEventIdObject, payCnfId, paySaId);
					if(returnCreateFTStatus.startsWith("false")){
						return returnCreateFTStatus;
					} else {
						ft1 = returnCreateFTStatus;
					}
				}
			}

			
			/********** FT GL Set Up for FT created with current payment segment amount *************/
			/********** FT GL Set Up for current FT created for debt distribution id *************/
			String debtDstId = getDstIdDetails(contractDetails.get(i).get(1).toString());
			returnCreateFtGlStatus = createFtGl(ft1, new BigInteger(String.valueOf(seq)), 
					paySegAmt.negate(), debtDstId.trim(), Bool.TRUE);
			if(returnCreateFtGlStatus.startsWith("false")){
				return returnCreateFtGlStatus;
			}

			/********** FT GL Set Up for current FT created for control distribution id *************/
			
			returnCreateFtGlStatus = createFtGl(ft1, new BigInteger(String.valueOf(seq+1)), 
					paySegAmt, payCnfId.getExtSourceId().trim(), Bool.FALSE);
			if(returnCreateFtGlStatus.startsWith("false")){
				return returnCreateFtGlStatus;
			}
			
			return "true";
		}


		/**
		 * getDstIdDetails() method extracts distribution id for current FT GL being created.
		 * 
		 * @return String
		 */

		private String getDstIdDetails (String saTypeCode) {
			
			PreparedStatement preparedStatement = null;
			String dstId = "";
			StringBuilder stringBuilder = new StringBuilder();

			try {
				stringBuilder.append(" SELECT DST_ID FROM CI_SA_TYPE WHERE SA_TYPE_CD = :saTypeCode ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("saTypeCode", saTypeCode, "SA_TYPE_CD");
				preparedStatement.setAutoclose(false);
				if(notNull(preparedStatement.firstRow())){
					dstId = preparedStatement.firstRow().getString("DST_ID");		
				} else {
					
					return "false" + "~" + getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_NO_DST_ID)) + "~"
					+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
					+ String.valueOf(CustomMessages.PAY_CNF_NO_DST_ID);
				}
			} catch (Exception e) {
				logger.error("Exception occurred in getDstIdDetails() ", e);
				return "false" + "~" + 
				getErrorDescription(String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION))+ "~"
				+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
				+ CustomMessages.RUN_TIME_ERROR_IN_EXECUTION;
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return dstId;
		}


		/**
		 * getAdjustmentType() method extracts adjustment type for the current SA_ID being processed.
		 * 
		 * @return String
		 */

		private String getAdjustmentType () {
		
			String adjType = "";
			String saType = "";
			try {
				saType = acctNbr.trim();
				if(saType.equals(payConfirmationLookup.getChrg())) {
					adjType = payConfirmationLookup.getOverpayChrg();
				} else if(saType.equals(payConfirmationLookup.getFund())) {
					adjType = payConfirmationLookup.getOverpayFund();
				} else if(saType.equals(payConfirmationLookup.getChbk())) {
					adjType = payConfirmationLookup.getOverpayChbk();
				}
			} catch (Exception e) {
				logger.error("Exception occurred in getAdjustmentType() ", e);
				return "false" + "~" + 
				getErrorDescription(String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION))+ "~"
				+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
				+ CustomMessages.RUN_TIME_ERROR_IN_EXECUTION;
			} 		
			return adjType;
		}


		/**
		 * getPayConfirmationStgData() method selects data from CM_PAY_CNF_STG staging table.
		 * 
		 * @return List PayConfirmation_Id
		 */
		private List<PayConfirmation_Id> getPayConfirmationStgData (Fin_Doc_Id finDocId) {
			
			PreparedStatement preparedStatement = null;
			PayConfirmation_Id payConfirmationId = null;
			List<PayConfirmation_Id> rowsForProcessingList = new ArrayList<PayConfirmation_Id>();
			StringBuilder stringBuilder = new StringBuilder();

			String txnHeaderId = "";
			com.splwg.base.api.datatypes.Date payDate =  null;
			String extSourceId = "";
			String extRefId = "";
			String billId = "";
			String bsegId = "";
			String amt = "";
			String currencyCode = "";
			String bankingEntryStatus = "";

			try {
				stringBuilder.append("SELECT TXN_HEADER_ID, PAY_DT, EXT_SOURCE_CD, EXT_TRANSMIT_ID, " );
				stringBuilder.append(" FINANCIAL_DOC_ID, FINANCIAL_DOC_LINE_ID, TENDER_AMT, CURRENCY_CD, BANKING_ENTRY_STATUS FROM CM_PAY_CNF_STG " );
				stringBuilder.append(" WHERE BO_STATUS_CD = :selectBoStatus1 AND FINANCIAL_DOC_ID = :finDocId ORDER BY TXN_HEADER_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString() ,"");
				preparedStatement.bindString("selectBoStatus1", payConfirmationLookup.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindString("finDocId", finDocId.getFinDocId(), "FINANCIAL_DOC_ID");
				preparedStatement.setAutoclose(false);

				for (SQLResultRow resultSet : preparedStatement.list()) {
					txnHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID"));
					payDate =  resultSet.getDate("PAY_DT");
					extSourceId = CommonUtils.CheckNull(resultSet.getString("EXT_SOURCE_CD"));
					extRefId = CommonUtils.CheckNull(resultSet.getString("EXT_TRANSMIT_ID"));
					billId = CommonUtils.CheckNull(resultSet.getString("FINANCIAL_DOC_ID"));
					bsegId = CommonUtils.CheckNull(resultSet.getString("FINANCIAL_DOC_LINE_ID"));
					amt = CommonUtils.CheckNull(String.valueOf(resultSet.getBigDecimal("TENDER_AMT")));
					currencyCode = CommonUtils.CheckNull(resultSet.getString("CURRENCY_CD")).trim();
					bankingEntryStatus = CommonUtils.CheckNull(resultSet.getString("BANKING_ENTRY_STATUS")).trim();

					payConfirmationId = new PayConfirmation_Id(
							txnHeaderId, payDate, extSourceId, extRefId,
							billId, bsegId, amt, currencyCode, bankingEntryStatus);

					rowsForProcessingList.add(payConfirmationId);
					resultSet = null;
					payConfirmationId = null;
				}
			} catch (Exception e) {
				logger.error("Exception occurred in getPayConfirmationStgData()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			return rowsForProcessingList;
		}


		/**
		 * extractBillAmt() method compares input from CM_PAY_CNF_STG staging table with Bill Amount.
		 * 
		 * @return 
		 */
		private BigDecimal extractUnpaidBillAmt(PayConfirmation_Id payCnfId) {
			
			PreparedStatement preparedStatement = null;
			BigDecimal billAmt = BigDecimal.ZERO;
			StringBuilder stringBuilder = new StringBuilder();
			ArrayList<String> saIdList = null;
			contractDetails = new ArrayList<List<String>>();
			
			//Start-logic to update base match event id as SPACE--acctId
			try {				
				stringBuilder.append("UPDATE CI_FT FT1 SET FT1.MATCH_EVT_ID=' ' WHERE FT1.BILL_ID=:billId " );
				stringBuilder.append("AND NOT EXISTS (SELECT 1 FROM CI_FT FT2 " );
				stringBuilder.append("WHERE FT2.FT_TYPE_FLG IN ('PS','PX') AND FT1.MATCH_EVT_ID=FT2.MATCH_EVT_ID) AND FT1.MATCH_EVT_ID!=' ' " );
				preparedStatement = createPreparedStatement(stringBuilder.toString() ,"");
				preparedStatement.bindString("billId", payCnfId.getBillId(), "BILL_ID");
				preparedStatement.executeUpdate();

			} catch (Exception e) {
				logger.error("Exception in extractBillAmt" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			//End-logic to update base match event id as null

			
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT  SA.SA_ID, SA.SA_TYPE_CD,FT.MATCH_EVT_ID, SUM(FT.CUR_AMT) AS AMT ");
				stringBuilder.append("FROM CI_SA SA, CI_FT FT ");
				stringBuilder.append("WHERE SA.SA_ID = FT.SA_ID and FT.FREEZE_SW = 'Y' ");
				stringBuilder.append("AND FT.BILL_ID = :billId and FT.CUR_AMT!=0 ");
				stringBuilder.append("GROUP BY SA.SA_ID, SA.SA_TYPE_CD,FT.MATCH_EVT_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("billId", payCnfId.getBillId(), "BILL_ID");
				preparedStatement.setAutoclose(false);

				for (SQLResultRow resultSet : preparedStatement.list()) {
					if(((new BigDecimal(resultSet.getString("AMT")).setScale(2)).compareTo(BigDecimal.ZERO.setScale(2))!=0)){
					saIdList = new ArrayList<String>();
					saIdList.add(resultSet.getString("SA_ID"));
					saIdList.add(resultSet.getString("SA_TYPE_CD"));
					billAmt = billAmt.add(resultSet.getBigDecimal("AMT"));
					matchEventId = resultSet.getString("MATCH_EVT_ID");
					saIdList.add(resultSet.getString("AMT"));
					contractDetails.add(saIdList);
					saIdList = null;
					}
				}		

			} catch (Exception e) {
				logger.error("Exception occurred in getting Contract Details ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			if(notBlank(matchEventId)){
				billAmt = BigDecimal.ZERO;
				contractDetails = new ArrayList<List<String>>();
				try {
					stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT  SA.SA_ID, SA.SA_TYPE_CD,FT.MATCH_EVT_ID, SUM(FT.CUR_AMT) AS AMT ");
					stringBuilder.append("FROM CI_SA SA, CI_FT FT ");
					stringBuilder.append("WHERE SA.SA_ID = FT.SA_ID and FT.FREEZE_SW = 'Y' ");
					stringBuilder.append("AND FT.MATCH_EVT_ID = :matchEvtId and FT.CUR_AMT!=0 ");
					stringBuilder.append("GROUP BY SA.SA_ID, SA.SA_TYPE_CD,FT.MATCH_EVT_ID ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("matchEvtId", matchEventId, "MATCH_EVT_ID");
					preparedStatement.setAutoclose(false);

					for (SQLResultRow resultSet : preparedStatement.list()) {
						if(((new BigDecimal(resultSet.getString("AMT")).setScale(2)).compareTo(BigDecimal.ZERO.setScale(2))!=0)){
						saIdList = new ArrayList<String>();
						saIdList.add(resultSet.getString("SA_ID"));
						saIdList.add(resultSet.getString("SA_TYPE_CD"));
						billAmt = billAmt.add(resultSet.getBigDecimal("AMT"));
						matchEventId = resultSet.getString("MATCH_EVT_ID");
						saIdList.add(resultSet.getString("AMT"));
						contractDetails.add(saIdList);
						saIdList = null;
						}
					}		

				} catch (Exception e) {
					logger.error("Exception occurred in getting Contract Details ", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}				
			}

			return billAmt;
		}


		/**
		 * createPaySeg() method creates Payment Segments based on input data
		 * 
		 * @return 
		 */

		private String createPaySeg(PaymentSegment_DTO paySeg, String currencyCode, BigDecimal amt, Payment_Id paymentId, String saId) {
			
			String paySegId = "";
			try {

				/********** Pay Segment for balanced bill amount ************/
				paySeg.setCurrencyId(new Currency_Id(currencyCode));
				paySeg.setPaymentId(paymentId);
				paySeg.setServiceAgreementId(new ServiceAgreement_Id(saId)); 
				paySeg.setPaySegmentAmount(new Money(amt,new Currency_Id(currencyCode)));
				paySeg.newEntity();
				paySegId = paySeg.getId().getIdValue();
				
			} catch (Exception e) {
				logger.error("Exception in payment segment ",e );
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}

			return paySegId;
		}


		/**
		 * createFT() method creates FTs based on input data
		 * 
		 * @return 
		 */

		private String createFT(FinancialTransaction_DTO ft, String siblingId1, 
				BigDecimal amt, Payment_Id payId, MatchEvent_Id matchEventId, 
				PayConfirmation_Id payCnfId, String saId ) {
			
			String ft1 = "";
			try {
				
				ft.setSiblingId(siblingId1);
				ft.setServiceAgreementId(new ServiceAgreement_Id(saId));
				ft.setParentId(payId.getIdValue());
				ft.setDivisionId(new CisDivision_Id(cisDivision));
				ft.setGlDivisionId(new GeneralLedgerDivision_Id(payConfirmationLookup.getGlDivision()));
				ft.setCurrencyId(new Currency_Id(payCnfId.getCurrencyCode()));
				ft.setFinancialTransactionType(FinancialTransactionTypeLookup.constants.PAY_SEGMENT);
				ft.setCurrentAmount(new Money(amt, new Currency_Id(payCnfId.getCurrencyCode())));
				ft.setPayoffAmount(new Money(amt, new Currency_Id(payCnfId.getCurrencyCode())));
				ft.setCreationDateTime(getSystemDateTime());
				ft.setIsFrozen(Bool.TRUE);
				ft.setFrozenByUserId(new User_Id(payConfirmationLookup.getUserId()));
				ft.setFreezeDateTime(getSystemDateTime());
				ft.setArrearsDate(payCnfId.getPayDate());
				ft.setIsCorrection(Bool.FALSE);
				ft.setIsRedundant(Bool.FALSE);
				ft.setIsNewCharge(Bool.FALSE);
				ft.setShouldShowOnBill(Bool.TRUE);
				ft.setIsNotInArrears(Bool.FALSE);
				ft.setAccountingDate(payCnfId.getPayDate());
				ft.setGlDistributionStatus(GlDistributionStatusLookup.constants.GENERATED);
				ft.setScheduledDistributionDate(payCnfId.getPayDate());
				ft.setMatchEventId(matchEventId);
				ft.newEntity();
				ft1 = ft.getId().getIdValue();
				
			} catch (Exception e) {
				logger.error("Exception in createFT() ",e );
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			}

			return ft1;
		}


		/**
		 * createFtGl() method creates FT GLs based on input data
		 * 
		 * @return 
		 */

		private String createFtGl(String ftId, BigInteger seqNum, BigDecimal amt, String dstId, Bool totalAmtSw) {
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			/********** FT GL Set Up for FT created with payment tender amount *************/
			try {
				/********** FT GL Set Up for current FT created for debt distribution id *************/
				stringBuilder.append("Insert into CI_FT_GL " );
				stringBuilder.append(" (FT_ID,GL_SEQ_NBR,DST_ID,CHAR_TYPE_CD,AMOUNT,CHAR_VAL,TOT_AMT_SW, GL_ACCT) " );
				stringBuilder.append(" values (:ftId, :seqNo, TRIM(:dstId), '  ', :amt,' ', :totalAmtSw , ' ')");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("dstId", dstId.trim(), "DST_ID");
				preparedStatement.bindString("ftId", ftId, "FT_ID");
				preparedStatement.bindBigInteger("seqNo", seqNum);
				preparedStatement.bindBigDecimal("amt", amt);
				preparedStatement.bindBoolean("totalAmtSw", totalAmtSw);
				int row = preparedStatement.executeUpdate();
				
				if (row==0) {
					return "false" + "~" + 
							getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_FT_GL_NOT_UPDATED))+ "~"
							+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
							+ CustomMessages.PAY_CNF_FT_GL_NOT_UPDATED;
				}

			} catch (Exception e) {
				logger.error("Exception in ci_ft_gl insertion ", e );
				return "false" + "~" + 
				getErrorDescription(String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION))+ "~"
				+ String.valueOf(CustomMessages.MESSAGE_CATEGORY) + "~"
				+ CustomMessages.RUN_TIME_ERROR_IN_EXECUTION;
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return "true";
		}

		
		private boolean extractBillDetails (PayConfirmation_Id payCnfId){
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				/********** Extracting account Details *************/
				stringBuilder.append("SELECT BILL.ACCT_ID, ACCTNBR.ACCT_NBR, ACCT.CIS_DIVISION " );
				stringBuilder.append(" FROM CI_BILL BILL, CI_ACCT_NBR ACCTNBR, CI_ACCT ACCT WHERE ACCT.ACCT_ID = BILL.ACCT_ID " );
				stringBuilder.append(" AND BILL.ACCT_ID = ACCTNBR.ACCT_ID " );
				stringBuilder.append(" AND BILL.BILL_ID = :billId " );
				stringBuilder.append(" AND ACCTNBR.ACCT_NBR_TYPE_CD= :accountType");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("billId", payCnfId.getBillId(), "BILL_ID");
				preparedStatement.bindString("accountType", payConfirmationLookup.getAccountType(), "ACCT_NBR_TYPE_CD");
				preparedStatement.setAutoclose(false);

				if(notNull(preparedStatement.firstRow())){
					acctId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("ACCT_ID"));
					acctNbr = CommonUtils.CheckNull(preparedStatement.firstRow().getString("ACCT_NBR"));
					cisDivision = CommonUtils.CheckNull(preparedStatement.firstRow().getString("CIS_DIVISION"));
				} else {
					
					return logError(payCnfId.getTxnHeaderId(), payCnfId.getBillId(), 
							payConfirmationLookup.getError(), String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
							String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS), 
							getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_INVALID_ACCT_DETAILS)), 
							"true") ;
				}

				//Merchant is receiving money in all the cases except from Charging so Pay for other types
				if(acctNbr.trim().equals("CHRG")) {
					tenderType = payConfirmationLookup.getRcptTenderType();
				} else {
					tenderType = payConfirmationLookup.getPayTenderType();
				}

				
			} catch (Exception e) {
				logger.error("Exception occured in createPaymentStaging() ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			try {
				
				/********** Extracting contract Details *************/
				stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT SA.SA_ID FROM CI_SA SA ");
				stringBuilder.append("WHERE SA.ACCT_ID = :acctId "); 
				stringBuilder.append("AND SA.SA_TYPE_CD=:acctNbr ");
				stringBuilder.append("AND EXISTS (SELECT 1 FROM CI_SA_CHAR SACR WHERE SA.SA_ID=SACR.SA_ID "); 
				stringBuilder.append("AND SACR.CHAR_TYPE_CD='SA_ID') ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctId", acctId, "ACCT_ID");
				preparedStatement.bindString("acctNbr", acctNbr, "SA_TYPE_CD");
				preparedStatement.setAutoclose(false);
				
				if(notNull(preparedStatement.firstRow())){
					saId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("SA_ID"));
				} else {
					
					return logError(payCnfId.getTxnHeaderId(), payCnfId.getBillId(), 
							payConfirmationLookup.getError(), String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
							String.valueOf(CustomMessages.PAY_CNF_NO_SA_ID), 
							getErrorDescription(String.valueOf(CustomMessages.PAY_CNF_NO_SA_ID)), 
							"true") ;
				}
				
			} catch (Exception e) {
				logger.error("Exception occurred in createPaymentStaging() ", e);
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
		 * updatePayCnfStaging() method will update the CM_PAY_CNF_STG
		 * staging table with the processing status
		 * 
		 * @param aTransactionHeaderId
		 * @param finDocId
		 * @param aStatus
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 * @param skipRemainingRows
		 */
		private void updatePayCnfStaging(String aTransactionHeaderId, String finDocId, String aStatus,
				String aMessageCategory, String aMessageNumber,String aErrorMessage,
				String skipRemainingRows, String ilmSw) {
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			if (aErrorMessage.length() > 255) {
				aErrorMessage = aErrorMessage.substring(0, 249);
			}

			try {
				stringBuilder.append("UPDATE CM_PAY_CNF_STG SET BO_STATUS_CD =:status1, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription, ");
				// RIA: Set ILM_DT = processDate and ILM_ARCH_SW = 'Y' for BO_STATUS_CD = 'COMP'
				stringBuilder.append(" ILM_DT =:ilmDate, ILM_ARCH_SW =:ilmSw ");
				stringBuilder.append(" WHERE TXN_HEADER_ID =:headerId AND FINANCIAL_DOC_ID = :finDocId " );
				stringBuilder.append(" AND BO_STATUS_CD = :status2 ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status1", CommonUtils.CheckNull(aStatus.trim()), "BO_STATUS_CD");
				preparedStatement.bindString("status2", payConfirmationLookup.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindBigInteger("messageCategory", new BigInteger(aMessageCategory));
				preparedStatement.bindBigInteger("messageNumber", new BigInteger(aMessageNumber));
				preparedStatement.bindString("errorDescription", CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
				preparedStatement.bindDate("ilmDate", getProcessDateTime().getDate());
				preparedStatement.bindString("ilmSw", ilmSw, "ILM_ARCH_SW");
				preparedStatement.bindString("headerId", CommonUtils.CheckNull(aTransactionHeaderId), "TXN_HEADER_ID");
				preparedStatement.bindString("finDocId", finDocId, "FINANCIAL_DOC_ID");
				preparedStatement.executeUpdate();

			} catch (Exception e) {
				logger.error("Error in updatePayCnfStaging");
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			//			This logic is required to update those rows, with corresponding error messages, which were skipped for processing through execute work unit
			//since one row with same parent and child merchant combination already has got failed
			if (CommonUtils.CheckNull(skipRemainingRows)
					.trim().startsWith("true")){
				
				try {
					aMessageCategory ="0";
					aMessageNumber="0";
					aErrorMessage = "Row couldn't be processed: One row is already in error for same Financial Document Id.";

					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CM_PAY_CNF_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
					stringBuilder.append(" MESSAGE_CAT_NBR = :messageCategory, MESSAGE_NBR = :messageNumber, ERROR_INFO = :errorDescription " );
					stringBuilder.append(" WHERE (TXN_HEADER_ID <>:headerId) " );
					stringBuilder.append(" AND FINANCIAL_DOC_ID = :finDocId " );
					stringBuilder.append(" AND BO_STATUS_CD =:status1 ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"" );
					preparedStatement.bindString("status1", payConfirmationLookup.getUpload().trim(), "BO_STATUS_CD");
					preparedStatement.bindString("status", payConfirmationLookup.getUpload().trim(), "BO_STATUS_CD");
					preparedStatement.bindString("messageCategory", aMessageCategory, "MESSAGE_CAT_NBR");
					preparedStatement.bindString("messageNumber", aMessageNumber, "MESSAGE_NBR");
					preparedStatement.bindString("errorDescription", aErrorMessage, "ERROR_INFO");
					preparedStatement.bindString("headerId", aTransactionHeaderId.trim(), "TXN_HEADER_ID");
					preparedStatement.bindString("finDocId", finDocId, "FINANCIAL_DOC_ID");
					preparedStatement.executeUpdate();

				} catch (Exception e){
					logger.error("Exception while updating other rows for same PER_ID_NBR", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}
		}

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

		/**
		 * logError() method stores the error information in the List and does rollback of all the database transaction of this unit.
		 * 
		 * @param aTransactionHeaderId
		 * @param finDocId
		 * @param aStatus
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 * @param skipRemainingRows
		 * @return
		 */

		private boolean logError(String aTransactionHeaderId, String finDocId, String aStatus,
				String aMessageCategory, String aMessageNumber,String aErrorMessage,
				String skipRemainingRows) {
			
			eachCustomerStatusList = new ArrayList<String>();
			eachCustomerStatusList.add(0, aTransactionHeaderId);
			eachCustomerStatusList.add(1, finDocId);
			eachCustomerStatusList.add(2, payConfirmationLookup.getError());
			eachCustomerStatusList.add(3, aMessageCategory);
			eachCustomerStatusList.add(4, aMessageNumber);
			eachCustomerStatusList.add(5, aErrorMessage);
			eachCustomerStatusList.add(6, skipRemainingRows);
			updateCustomerStatusList.add(eachCustomerStatusList);
			eachCustomerStatusList = null;

			//			Excepted to do rollback
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
		 * @param finDocId
		 */
		private int updateCmBillDueDtTbl(String finDocId ) {
			
			PreparedStatement dueDtUpdateStmt = null;
			StringBuilder stringBuilder = new StringBuilder();
			int row= 0;

			try {
				stringBuilder.append("UPDATE CM_BILL_DUE_DT SET IS_MERCH_BALANCED = 'Y', " );
				stringBuilder.append(" STATUS_UPD_DTTM = SYSTIMESTAMP WHERE BILL_ID = :billId ");
				dueDtUpdateStmt = createPreparedStatement(stringBuilder.toString(), "");
				dueDtUpdateStmt.bindString("billId", finDocId, "BILL_ID");
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
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			
			//			Logic to update erroneous records
			if (updateCustomerStatusList.size() > 0) {
				Iterator<ArrayList<String>> updatePayCnfStatusItr = updateCustomerStatusList.iterator();
				updateCustomerStatusList = null;
				ArrayList<String> rowList = null;
				while (updatePayCnfStatusItr.hasNext()) {
					rowList = (ArrayList<String>) updatePayCnfStatusItr.next();
					updatePayCnfStaging(String.valueOf(rowList.get(0)),
							String.valueOf(rowList.get(1)), String
							.valueOf(rowList.get(2)), String
							.valueOf(rowList.get(3)), String
							.valueOf(rowList.get(4)), String
							.valueOf(rowList.get(5)), String
							.valueOf(rowList.get(6)), "N");
					rowList = null;
				}
				updatePayCnfStatusItr = null;
			}
			payConfirmationLookup = null;
			super.finalizeThreadWork();
		}

	} //End of Worker Class

	public static final class Fin_Doc_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String finDocId;
		

		public Fin_Doc_Id(String finDocId) {
			setFinDocId(finDocId);
			}

		public static long getSerialversionuid() {
			return serialVersionUID;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getFinDocId() {
			return finDocId;
		}

		public void setFinDocId(String finDocId) {
			this.finDocId = finDocId;
		}

	}

	public static final class PayConfirmation_Id implements Id {

		private static final long serialVersionUID = 1L;
		private String txnHeaderId;
		private com.splwg.base.api.datatypes.Date payDate;
		private String extSourceId;
		private String extRefId;
		private String billId;
		private String bsegId;
		private String amt;
		private String currencyCode;
		private String bankingEntryStatus;

		public PayConfirmation_Id(String txnHeaderId, 
				com.splwg.base.api.datatypes.Date payDate, 
				String extSourceId, String extRefId,
				String billId, String bsegId, String amt, String currencyCode, String bankingEntryStatus) {
			setTxnHeaderId(txnHeaderId);
			setPayDate(payDate);
			setExtSourceId(extSourceId);
			setExtRefId(extRefId);
			setBillId(billId);
			setBsegId(bsegId);
			setAmt(amt);
			setCurrencyCode(currencyCode);
			setBankingEntryStatus(bankingEntryStatus);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getTxnHeaderId() {
			return txnHeaderId;
		}

		public void setTxnHeaderId(String txnHeaderId) {
			this.txnHeaderId = txnHeaderId;
		}
		
		public String getBankingEntryStatus() {
			return bankingEntryStatus;
		}

		public void setBankingEntryStatus(String bankingEntryStatus) {
			this.bankingEntryStatus = bankingEntryStatus;
		}

		public com.splwg.base.api.datatypes.Date getPayDate() {
			return payDate;
		}

		public void setPayDate(com.splwg.base.api.datatypes.Date payDate) {
			this.payDate = payDate;
		}

		public String getExtSourceId() {
			return extSourceId;
		}

		public void setExtSourceId(String extSourceId) {
			this.extSourceId = extSourceId;
		}

		public String getExtRefId() {
			return extRefId;
		}

		public void setExtRefId(String extRefId) {
			this.extRefId = extRefId;
		}

		public String getBillId() {
			return billId;
		}

		public void setBillId(String billId) {
			this.billId = billId;
		}

		public String getBsegId() {
			return bsegId;
		}

		public void setBsegId(String bsegId) {
			this.bsegId = bsegId;
		}

		public String getAmt() {
			return amt;
		}

		public void setAmt(String amt) {
			this.amt = amt;
		}

		public String getCurrencyCode() {
			return currencyCode;
		}

		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}

		public static long getSerialversionuid() {
			return serialVersionUID;
		}

	}
}