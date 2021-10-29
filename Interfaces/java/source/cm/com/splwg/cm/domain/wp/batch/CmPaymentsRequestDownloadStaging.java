/**************************************************************************
 *
 * PROGRAM DESCRIPTION:
 * 
 * This batch process reads from ORMB payment request tables and custom 
 * staging tables, and populates the outbound staging tables that is 
 * interfaced to an External System. This process uses all Bill / 
 * Batch Process records associated with its batch control that are marked 
 * with a supplied run number.  If a run number is not supplied, the process 
 * uses all Bill / Batch Process records marked with the current run number.
 * 
 *
 **************************************************************************
 *
 * CHANGE HISTORY:
 *
 * Date:        by:      Reason:
 * YYYY-MM-DD   IN       Reason text.
 * 
 * 2018-01-03	AVilla	 Initial Version. RIA_Payment Request Interface_TDD-v0.3
 **************************************************************************/


package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

import com.splwg.base.api.Query;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.WorkUnitResult;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.currency.Currency;
import com.splwg.base.domain.common.generalProcess.GeneralProcess;
import com.splwg.base.domain.common.generalProcess.GeneralProcess_Id;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.adjustmentType.AdjustmentType_Id;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.common.StringUtilities;


/**
 * @author AVilla
 *
 * @BatchJob (multiThreaded = true, rerunnable = true, 
 *     modules = {},
 *     softParameters = { @BatchJobSoftParameter (name = transactionSrcCd, type = string)
 *      , @BatchJobSoftParameter (name = acctNumberTypeCd, required = true, type = string)})
 */


public class CmPaymentsRequestDownloadStaging extends
		CmPaymentsRequestDownloadStaging_Gen {
	
	// Soft Parameter
	private static String transactionSrcCd;
	private static String acctNumberTypeCd;
	
	// Work Variables
	public static PaymentsRequestInterfaceLookUp paymentsRequestInterfaceLookUp = null;
	
	public void validateSoftParameters(boolean isNewRun) {
		
		transactionSrcCd = getParameters().getTransactionSrcCd();
		acctNumberTypeCd = getParameters().getAcctNumberTypeCd();
		
		if (isBlankOrNull(acctNumberTypeCd)){
			addError(CustomMessageRepository.batchParameterMissing(
					"acctNumberTypeCd",  
					this.getBatchControlId().getIdValue().trim()));
		}
		
		transactionSrcCd = isNull(transactionSrcCd) ? "" : transactionSrcCd.trim();
	
	}
		
	
	public Class<CmPaymentsRequestDownloadStagingWorker> getThreadWorkerClass() {
		return CmPaymentsRequestDownloadStagingWorker.class;
	}
		
	public static class CmPaymentsRequestDownloadStagingWorker extends
			CmPaymentsRequestDownloadStagingWorker_Gen {
		
		//Constants
		private static final String CR_FT_PAY_TYPE = "CR";
		private static final String DR_FT_PAY_TYPE = "DR";
		private static final String NA_FT_PAY_TYPE = "NA";
		private static final String FUND_ACCT_TYPE = "FUND";
		private static final String CHRG_ACCT_TYPE = "CHRG";
		private static final String CHBK_ACCT_TYPE = "CHBK";
				
		// Work variables
		private Bill_Id billId;
		private Bill bill;
		private DateTime processDateTime;
		private Account billAcctId;
		private Date billDt;
		private Date billDueDt;
		private String billCorrectionNoteFrBillId;
		private String billAlternateBillId;
		private DateTime billCreDttm;
		private CisDivision_Id accountPerCisDivision;
		private String accountPerAcctTypeStr;
		private String accountPerPerIdNbr;
		private Money financialTransactionBillAmount;
		private Currency financialTransactionCurrencyCd;
		private String financialTransactionPaytype;
		private String transactionPerAcctTypeStr;
		private String transactionTxnSourceCd;
		private String transactionUdfChar11;
		private String transactionUdfChar12;
		private String transactionUdfChar11Key;
		private String transactionUdfChar12Key;
		private String transactionUdfChar11Value;
		private String transactionUdfChar12Value;
		private String transactionUdfChar12ListAggStr;
		private Money settlementBillAmt;
		private String settlementCurrencyCd; 
		private String settlementIndFlg;
		private String settlementImmFinAdjFlg;
		private String settlementFinAdjManNrt;
		private String settlementRelReserveFlg; 
		private String settlementPayNarrative;
		private String settlementRelWafFlg;
		private String settlementFastPayVal; 
		private String settlementCaseIdentifier;
		private String settlementSubLevel;
		private String settlementReferenceSubLevel;
		private String settlementPayRequestGranularities;
		private String settlementLevelGranularities;
	
			
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new ThreadIterationStrategy(this);
		}
		
        @SuppressWarnings("rawtypes")
        protected QueryIterator getQueryIteratorForThread(StringId lowId, StringId highId) {
			
        	
        	PreparedStatement statement = null;
        	        	
        	String sql = " SELECT A.PK_VALUE1, A.GEN_PROC_ID " +
					 " FROM F1_GEN_PROC A " +
					 " WHERE A.MAINT_OBJ_CD = 'BILL' " +
					 "   AND A.BATCH_CD = :batchControl " +
					 "   AND A.PK_VALUE1 BETWEEN :lowId AND :highId " +
					 "   AND A.BATCH_NBR = :currentBatchRun " +
					 "	 AND NOT EXISTS (SELECT B.BILL_ID FROM CM_PAY_REQ B " +
					 "						WHERE B.BILL_ID = A.PK_VALUE1) " +
					 "	 AND NOT EXISTS (SELECT C.TXN_HEADER_ID FROM CM_PAY_CNF_STG C " +
					 "						WHERE C.TXN_HEADER_ID = A.PK_VALUE1) " +
					 "	 AND NOT EXISTS (SELECT D.BILL_ID FROM CM_BILL_DUE_DT D " +
					 "						WHERE D.BILL_ID = A.PK_VALUE1) " +
					 " ORDER BY A.PK_VALUE1 ";
       
		try{
		
			statement = createPreparedStatement(sql,"");
			statement.setAutoclose(false);
			statement.bindId("lowId", lowId); 
			statement.bindId("highId", highId);
			statement.bindId("batchControl", this.getBatchControlId());
			statement.bindBigInteger("currentBatchRun", this.getBatchNumber());
			
			
		}finally{			
			if(notNull(statement)){
				statement.close();
			}
		}
 			
			return statement.iterate();	
        }
        
        /**
		 * This customized throwable class will act as a 
		 * placeholder to skip the bill processing when triggered
		 * in a catch clause
		 */
        @SuppressWarnings("serial")
		private class ProceedToNextBill extends Throwable{
        	
        }

		/**
		 * This method generates a unit of work.
		 */
		protected ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
			ThreadWorkUnit unit = new ThreadWorkUnit(row.getId
					("GEN_PROC_ID", GeneralProcess.class));
			unit.addSupplementalData("pkValue1", row.getString("PK_VALUE1"));
			return unit;
		}
        
		/**
		 * An implemented method that is executed at the start of each thread run for initialization.
		 * @param initializationSuccessful boolean indicating successful processing
		 * @throws ThreadAbortedException Thread Aborted Exception
         * @throws RunAbortedException Run Aborted Exception
		 */
		public void initializeThreadWork(
				boolean initializationPreviouslySuccessful)
				throws ThreadAbortedException, RunAbortedException {
		
			transactionSrcCd = getParameters().getTransactionSrcCd();
			
			acctNumberTypeCd = getParameters().getAcctNumberTypeCd();
			
			transactionSrcCd = isNull(transactionSrcCd) ? "" : transactionSrcCd.trim();
			
			paymentsRequestInterfaceLookUp = null;
			paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp();
			paymentsRequestInterfaceLookUp.setLookUpConstants();
			
			startResultRowQueryIteratorForThread(GeneralProcess_Id.class);

		}
		
		/**
		 * This method processes every record retrieved by the Query Iterator.
		 */
		public WorkUnitResult executeWorkUnitDetailedResult(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			try{
				
				// Initialize variable
				initializeVariables();
				
				// Get bill Id				
				billId = new Bill_Id(unit.getSupplementallData("pkValue1").toString().trim());
							
				//Main Process
				
				//Retrieve Records for Bill & AcctPer Info
				retrieveAndValidateBillAcctPerInfo(billId);
				
				//Retrieve and parse Transaction Detail Info
				parseTransactionDetailInfo(billId);
							
				//Retrieve Records for FT Info
				retrieveAndValidateFTInfo(billId);
				
				//Retrieve and process Settlement Info
				processSettlementInfo(billId);
				
				//Process Overpayment
				processOverpayment(billId, accountPerAcctTypeStr);
				
				//Check Non Zero Balance Migration Bill
				checkNonZeroMigrationBillExists(billId);
				
			}catch(ProceedToNextBill ptnb){
				return new WorkUnitResult(true);
			}
			
			WorkUnitResult results = new WorkUnitResult(true);
			results.setUnitsProcessed(1);
			return results;
		}
		
		/*
		 * This method initializes all the variables that will be used in the process
		 */
		private void initializeVariables(){
			
			billId = null;
			bill = null;
			processDateTime = this.getProcessDateTime();
			billAcctId = null;
			billCorrectionNoteFrBillId = "";
			billAlternateBillId = "";
			accountPerCisDivision = null;
			accountPerAcctTypeStr = "";
			accountPerPerIdNbr = " ";
			financialTransactionBillAmount = Money.ZERO;
			financialTransactionCurrencyCd = null;
			financialTransactionPaytype = "";
			transactionTxnSourceCd = "";
			transactionUdfChar11 = "";
			transactionUdfChar12 = "";
			transactionPerAcctTypeStr = "";
			transactionUdfChar11Key = null;
			transactionUdfChar12Key = null;
			transactionUdfChar11Value = null;
			transactionUdfChar12Value = null;
			transactionUdfChar12ListAggStr = " ";
			settlementCurrencyCd = "";
			settlementIndFlg = " ";
			settlementImmFinAdjFlg =  ""; 
			settlementFinAdjManNrt = " ";
			settlementRelReserveFlg = " ";
			settlementRelWafFlg = " ";
			settlementPayNarrative = " ";
			settlementFastPayVal = " ";
			settlementCaseIdentifier = " ";
			settlementSubLevel = " ";
			settlementReferenceSubLevel = " ";
			settlementPayRequestGranularities = " ";
			settlementLevelGranularities = " ";			
		}

		/*
		 * This method inserts message information in custom table CM_PAY_REY_ERR
		 */
		private void insertErrMessage(Bill_Id billId, BigInteger msgCatNbr, BigInteger msgNbr, String errInfo){
			
			PreparedStatement statement = null;
			
			String sql = " INSERT INTO CM_PAY_REQ_ERR (BILL_ID," +
						" MESSAGE_CAT_NBR," +
						" MESSAGE_NBR," +
						" ERROR_INFO)" +
						" VALUES " +
						" (:billId," +
						" :msgCatNbr," +
						" :msgNbr," +
						" :errInfo)";
			
			try{
				
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindBigInteger("msgCatNbr", msgCatNbr);
				statement.bindBigInteger("msgNbr", msgNbr);
				statement.bindString("errInfo", errInfo, null);
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_PAY_REQ_ERR"));
			}finally{
			
				if(notNull(statement)){
					statement.close();
				}	
			}	
		}
		
		/*
		 * This method retrieve the transaction detail information
		 */
		private List<SQLResultRow> retrieveTransactionDetailInformation(Bill_Id billId){
					
			PreparedStatement statement = null;
			List<SQLResultRow> sqlList = null;
			
			String sql = " SELECT T.TXN_SOURCE_CD, T.ACCT_NBR AS ACCOUNT_PER_ACCT_TYPE_STR, T.UDF_CHAR_11, T.UDF_CHAR_12 FROM ( " +
					"   SELECT TXN.TXN_SOURCE_CD, ACNBR.ACCT_NBR, TXN.UDF_CHAR_11, " +
					"   (CASE WHEN TXN.UDF_CHAR_12 IS NULL OR TXN.UDF_CHAR_12 = ' ' THEN ' ' " +
					"   ELSE TXN.UDF_CHAR_12 " +
					"   END) AS UDF_CHAR_12 " +
					"   FROM CI_BSEG BSEG, " +
					"     CI_BSEG_CALC BSCALC, " +
					"     CI_TXN_DTL_PRITM TXNDTL ,  " +
					"     CI_TXN_DETAIL TXN, CI_ACCT_NBR ACNBR " +
					"   WHERE BSEG.BILL_ID = :billId " +
					"   AND BSEG.BSEG_ID = BSCALC.BSEG_ID " +
					"   AND BSCALC.BILLABLE_CHG_ID = TXNDTL.BILLABLE_CHG_ID " +
					"   AND TXNDTL.TXN_DETAIL_ID = TXN.TXN_DETAIL_ID " +
					"   AND ACNBR.ACCT_ID = TXNDTL.ACCT_ID " +
					"   AND BSEG.BSEG_STAT_FLG = :bsegStatFlg) T " +
					" GROUP BY T.TXN_SOURCE_CD, T.ACCT_NBR, T.UDF_CHAR_11, T.UDF_CHAR_12 ";
			
			try{
				
				statement = createPreparedStatement(sql, "");
				statement.bindId("billId", billId);
				statement.bindLookup("bsegStatFlg", BillSegmentStatusLookup.constants.FROZEN);
				statement.setAutoclose(false);
			
				sqlList = statement.list();

			}finally{
				if(notNull(statement)){
					statement.close();
				}
			}
			
			return sqlList;
			
		}
		
		/*
		 * This method parse/process the records retrieved 
		 * in retrieveTransactionDetailInformation()
		 */
		private void parseTransactionDetailInfo(Bill_Id billId) 
				throws ProceedToNextBill{
			
			Iterator<SQLResultRow> iterator = retrieveTransactionDetailInformation(billId).iterator();
			List<String> udfChar12KeyList = new ArrayList<String>();
			int i = 1;
			
				while(iterator.hasNext()){
					
					QueryResultRow qRow = iterator.next();
			
					transactionPerAcctTypeStr = qRow.getString("ACCOUNT_PER_ACCT_TYPE_STR");
					transactionTxnSourceCd = qRow.getString("TXN_SOURCE_CD");
					transactionUdfChar11 = qRow.getString("UDF_CHAR_11");
					transactionUdfChar12 = qRow.getString("UDF_CHAR_12");
									
					checkBillTransactionSrcCd(transactionTxnSourceCd);
														
					if(transactionPerAcctTypeStr.equals(FUND_ACCT_TYPE) &&
							!isBlankOrNull(transactionUdfChar12)){
						
						HashMap<String, String> hMap1 = getMapPair(transactionUdfChar11);
						HashMap<String, String> hMap2 = getMapPair(transactionUdfChar12);
						
						transactionUdfChar11Key = hMap1.keySet().iterator().next();
						transactionUdfChar11Value = hMap1.get(transactionUdfChar11Key);
						transactionUdfChar12Key = hMap2.keySet().iterator().next();
						transactionUdfChar12Value = hMap2.get(transactionUdfChar12Key);	
						udfChar12KeyList.add(transactionUdfChar12Key);
						
						if(!isBlankOrNull(transactionUdfChar12Key) 
								&& !isBlankOrNull(transactionUdfChar12Value)){
							
							if(transactionUdfChar12Key.trim().equals("TT")){
								transactionUdfChar12Value = 
										transactionUdfChar12Value.concat("-JAN-00");
							}
							
							insertIntoCmPayReqGranularities(billId, BigInteger.valueOf(i++), 
									processDateTime, transactionUdfChar12Key, 
									transactionUdfChar12Value);
						}
						
					}else if((transactionPerAcctTypeStr.equals(FUND_ACCT_TYPE) || 
							transactionPerAcctTypeStr.equals(CHBK_ACCT_TYPE)) && 
							isBlankOrNull(transactionUdfChar12)){
						
						HashMap<String, String> hMap1 = getMapPair(transactionUdfChar11);

						transactionUdfChar11Key = hMap1.keySet().iterator().next();
						transactionUdfChar11Value = hMap1.get(transactionUdfChar11Key);
						transactionUdfChar12Key = null;
						transactionUdfChar12Value = null;	
					}	
				}	
				
				if(!udfChar12KeyList.isEmpty() && !udfChar12KeyList.contains(null)){
					Collections.sort(udfChar12KeyList);
					transactionUdfChar12ListAggStr = udfChar12KeyList.toString()
							.replace("[", "")
							.replace("]", "")
							.replace(", ", "|");
				}
		}
		
		/*
		 * This method split/tokenize the string based on its delimiter
		 * and put in in a hash map (HashMap<String, String>)
		 */
		private HashMap<String, String> getMapPair(String transactionUdfChar)
		{
			String mapKey = "";
			String mapValue = "";
			
			StringTokenizer tokenizer = 
					new StringTokenizer(transactionUdfChar, "|");
			
		    mapKey = tokenizer.nextToken();
		    		    
		    if(tokenizer.hasMoreTokens()){
		    	mapValue = tokenizer.nextToken();
    		}
		    
		    HashMap<String,String> hm = new HashMap<String,String>();
		    
		    hm.put(mapKey, mapValue);
		    
		    return hm;
		}
		
		/*
		 * This methods checks if the 'transaction source cd'
		 * batch parameter input is equal to the retrieved transaction source cd 
		 */
		private void checkBillTransactionSrcCd(String transactionTxnSourceCd) 
				throws ProceedToNextBill{

			if(!isBlankOrNull(transactionSrcCd)){
				if(!transactionSrcCd.equals(transactionTxnSourceCd)){
					throw new ProceedToNextBill();
				}
			}
		}
		
		/*
		 * This method retrieves the bill and account person information
		 */
		private void retrieveAndValidateBillAcctPerInfo(Bill_Id billId) throws ProceedToNextBill {
						
			paymentsRequestInterfaceLookUp = new PaymentsRequestInterfaceLookUp();
			paymentsRequestInterfaceLookUp.setLookUpConstants();
			
			Query<QueryResultRow> query = createQuery( 
							" FROM " +
							"	Bill bill, " +
							"	Account acct, " +
							"	AccountNumber acctNbr, " +
							"	AccountPerson acctPer, " +
							"	PersonId perId " +
							" WHERE " +
							"	bill.id = :billId " +
							"	AND bill.account = acct.id " +
							"	AND acct.id = acctNbr.id.account.id " +
							"	AND acctNbr.id.accountIdentifierType = '" + acctNumberTypeCd.trim() + "' " + 
							"	AND acct.id = acctPer.id.account.id " +
							"	AND acctPer.id.person.id = perId.id.person.id " +
							"	AND acctPer.isMainCustomer = 'Y' " +
							"	AND perId.id.idType = '" + paymentsRequestInterfaceLookUp.getExtPartyId().trim() + "' ", "");					
					
			query.bindId("billId", billId);		
			query.addResult("cisDivision", "acct.divisionId");
			query.addResult("acctType", "acctNbr.accountNumber");
			query.addResult("perIdNbr", "perId.personIdNumber");
			query.addResult("acctId", "bill.account");
			query.addResult("billId", "bill.id");
			query.addResult("billDt", "bill.billDate");
			query.addResult("dueDt", "bill.dueDate");
			query.addResult("crNoteFrBillId", "bill.crNoteFromBillId");
			query.addResult("altBillId", "bill.seqBillNumber");
			query.addResult("creDttm", "bill.creationDateTime");

			QueryResultRow qRow = query.firstRow();
			
			if(notNull(qRow)){
				
				billAcctId = qRow.getEntity("acctId", Account.class);
				billDt = qRow.getDate("billDt");
				billDueDt = qRow.getDate("dueDt");
				billCorrectionNoteFrBillId = qRow.getString("crNoteFrBillId");
				billAlternateBillId = qRow.getString("altBillId");
				billCreDttm = qRow.getDateTime("creDttm");
				accountPerCisDivision = (CisDivision_Id) qRow.getId("cisDivision", CisDivision.class);
				accountPerAcctTypeStr = qRow.getString("acctType");
				accountPerPerIdNbr = qRow.getString("perIdNbr");
								
				if(isNull(billDt)){
					
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingBillDate().getCategory(), 
							CustomMessageRepository.missingBillDate().getNumber(),
							CustomMessageRepository.missingBillDate().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingBillDate());
					
					//Skip bill from processing
					throw new ProceedToNextBill();
					
				}
				
				if(isNull(accountPerCisDivision)){
					
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingDivision().getCategory(), 
							CustomMessageRepository.missingDivision().getNumber(),
							CustomMessageRepository.missingDivision().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingDivision());
					
					//Skip bill from processing
					throw new ProceedToNextBill();
				}
				
				if(isBlankOrNull(accountPerAcctTypeStr)){
					
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingAcctType().getCategory(), 
							CustomMessageRepository.missingAcctType().getNumber(),
							CustomMessageRepository.missingAcctType().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingAcctType());
					
					//Skip bill from processing
					throw new ProceedToNextBill();
				}
				
				if(isBlankOrNull(accountPerPerIdNbr)){
					
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingExternalPartyId().getCategory(), 
							CustomMessageRepository.missingExternalPartyId().getNumber(),
							CustomMessageRepository.missingExternalPartyId().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingExternalPartyId());
					
					//Skip bill from processing
					throw new ProceedToNextBill();	
				}	
			}else{
				throw new ProceedToNextBill();
			}
		}
		
		/*
		 * This method retrieve and valdate the bill FT amount and the currency cd
		 */
		private void retrieveAndValidateFTInfo(Bill_Id billId) throws ProceedToNextBill {
			
			Query<Money> query = createQuery(" FROM " +
									  " FinancialTransaction ft" +
									  " WHERE ft.billId = :billId ", "");
			
			query.bindId("billId", billId);
			query.addResult("currAmt", "SUM(ft.currentAmount)");
			
			Money qRow = query.firstRow();
			
			if(notNull(qRow)){
				
				bill = billId.getEntity();
				
				if(notNull(bill)){
					financialTransactionCurrencyCd = bill.getAccount().getCurrency();
					financialTransactionBillAmount = qRow;
				}

				if(isNull(financialTransactionBillAmount)){
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingAmount().getCategory(),
							CustomMessageRepository.missingAmount().getNumber(), 
							CustomMessageRepository.missingAmount().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingAmount());
					
					//Skip bill from processing
					throw new ProceedToNextBill();
				}
				
				if(isNull(financialTransactionCurrencyCd)){
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingCurrencyCd().getCategory(),
							CustomMessageRepository.missingCurrencyCd().getNumber(), 
							CustomMessageRepository.missingCurrencyCd().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingCurrencyCd());
					
					//Skip bill from processing
					throw new ProceedToNextBill();
				}
				
				if(financialTransactionBillAmount.isLessThan(Money.ZERO)){
					financialTransactionPaytype = CR_FT_PAY_TYPE;
				}else if(financialTransactionBillAmount.isGreaterThan(Money.ZERO)){
					financialTransactionPaytype = DR_FT_PAY_TYPE;
				}else if(financialTransactionBillAmount.isEqualTo(Money.ZERO)){
					financialTransactionPaytype = NA_FT_PAY_TYPE;
				}else{
					financialTransactionPaytype = null;
					//Insert record
					insertErrMessage(billId, CustomMessageRepository.missingPayType().getCategory(),
							CustomMessageRepository.missingPayType().getNumber(), 
							CustomMessageRepository.missingPayType().getMessageText().trim());
					
					//Log message in run tree
					logError(CustomMessageRepository.missingPayType());
					//logError(message);
					
					//Skip bill from processing
					throw new ProceedToNextBill();	
				}	
			}
		}
		
		/*
		 * This method retrieve the settlement information
		 */
		private List<SQLResultRow> retrieveSettlementInfo(Bill_Id billId){
			
			PreparedStatement statement = null;
			List<SQLResultRow> sqlList = null;
						
			String sql = " SELECT DISTINCT A.BILL_ID, " +
						"       SUM(A.CUR_AMT),  " +
						"       A.CURRENCY_CD,  " +
						"       W.IS_IND_FLG,  " +
						"       W.ADHOC_SW as IS_IMD_FIN_ADJ,  " +
						"		 W.PAY_NARRATIVE,  " +
						"       W.REL_RESERVE_FLG,  " +
						"       W.REL_WAF_FLG,  " +
						"       W.FAST_PAY_VAL,  " +
						"		 W.CASE_IDENTIFIER " +
						"  FROM CI_FT A,  " +
						"       CI_BSEG_CALC Y,   " +
						"       CM_BCHG_STG W   " +
						"  WHERE A.BILL_ID = :billId  " + 
						"    AND Y.BSEG_ID(+) = A.SIBLING_ID  " +
						"    AND Y.HEADER_SEQ(+) = '1'   " +
						"    AND W.BILLABLE_CHG_ID(+) = Y.BILLABLE_CHG_ID  " +
						"    AND ((W.BILLABLE_CHG_ID IS NULL)  " +
						"    OR (W.STATUS_UPD_DTTM = (SELECT MAX(Z.STATUS_UPD_DTTM)  " +
						"                                FROM CM_BCHG_STG Z  " +
						"                               WHERE Z.BILLABLE_CHG_ID = W.BILLABLE_CHG_ID))) " +
						"    GROUP BY A.BILL_ID, A.CURRENCY_CD,  " +
						"       W.IS_IND_FLG,  " +
						"       W.ADHOC_SW,  " +
						"	   W.PAY_NARRATIVE,  " +
						"       W.REL_RESERVE_FLG,  " +
						"       W.REL_WAF_FLG,  " +
						"       W.FAST_PAY_VAL,  " +
						"	   W.CASE_IDENTIFIER ";
						 

			
			try{
				
				statement = createPreparedStatement(sql,"");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				sqlList = statement.list();
					
			}finally{
				if(notNull(statement)){
					statement.close();
				}
			}
				
			return sqlList;
		}
		
		/*
		 * This method process the record retrieved in retrieveSettlementInfo()
		 * if the account type is not 'CHRG'
		 */
		private void processSettlementInfo(Bill_Id billId) throws ProceedToNextBill{
		
			Iterator<SQLResultRow> iterator = retrieveSettlementInfo(billId).iterator();
			
				settlementBillAmt = Money.ZERO;
				int i = 1;
				int j = 1;
								
					while(iterator.hasNext()){
						
						QueryResultRow qRow = iterator.next();

						if(accountPerAcctTypeStr.equals(CHRG_ACCT_TYPE)){
				
							settlementBillAmt = financialTransactionBillAmount;
							settlementCurrencyCd = financialTransactionCurrencyCd.getId().getIdValue();
							settlementIndFlg = "N";
							
							if(billId.getEntity().getIsAdhocBill().isTrue()){
								settlementImmFinAdjFlg = "Y";
							}else{
								settlementImmFinAdjFlg = "N";
							}

							settlementFinAdjManNrt = "N";
							settlementRelReserveFlg = "N";
							settlementRelWafFlg = "N";
							settlementFastPayVal = "N";
							settlementCaseIdentifier = "N";
							settlementSubLevel = null;
							settlementReferenceSubLevel = null;
							settlementPayRequestGranularities = transactionUdfChar12ListAggStr;
							
										
						}else if(!accountPerAcctTypeStr.equals(CHRG_ACCT_TYPE)){
					
								settlementBillAmt = qRow.getMoney("SUM(A.CUR_AMT)");
								settlementCurrencyCd = qRow.getString("CURRENCY_CD");
							
								settlementIndFlg = qRow.getString("IS_IND_FLG");
								if(isBlankOrNull(settlementIndFlg)){settlementIndFlg = "N";}
							
								settlementImmFinAdjFlg = qRow.getString("IS_IMD_FIN_ADJ");
								if(isBlankOrNull(settlementImmFinAdjFlg)){settlementImmFinAdjFlg = "N";}
							
								settlementPayNarrative = qRow.getString("PAY_NARRATIVE");
								if(settlementIndFlg.equals("Y")){
									settlementFinAdjManNrt = settlementPayNarrative;
								}else{
									settlementFinAdjManNrt = "N";
								}
							
								settlementRelReserveFlg = qRow.getString("REL_RESERVE_FLG");
								if(isBlankOrNull(settlementRelReserveFlg)){settlementRelReserveFlg = "N";}
							
								settlementRelWafFlg = qRow.getString("REL_WAF_FLG");
								if(isBlankOrNull(settlementRelWafFlg)){settlementRelWafFlg = "N";}
							
								settlementFastPayVal = qRow.getString("FAST_PAY_VAL");
								if(isBlankOrNull(settlementFastPayVal)){settlementFastPayVal = "N";}
							
								if(settlementRelReserveFlg.equals("Y")){
									settlementCaseIdentifier = qRow.getString("CASE_IDENTIFIER");
								}else if(settlementRelWafFlg.equals("Y")){
									settlementCaseIdentifier = qRow.getString("CASE_IDENTIFIER");
								}else{
									settlementCaseIdentifier = "N";
								}
							
								settlementSubLevel = transactionUdfChar11Key;
								settlementReferenceSubLevel = transactionUdfChar11Value;
								settlementPayRequestGranularities = transactionUdfChar12ListAggStr;
								
							}	
						
						//Create Bill Settlement Map
						createBillSettlementMap(billId, BigInteger.valueOf(j++) ,processDateTime.getDate());
						
						if(accountPerAcctTypeStr.equals(FUND_ACCT_TYPE) && settlementIndFlg.equals("N") 
								&& !financialTransactionPaytype.equals(NA_FT_PAY_TYPE) 
								&& !settlementBillAmt.isEqualTo(Money.ZERO)
								&& !financialTransactionBillAmount.isEqualTo(Money.ZERO)
								){
								
							//Set the pay type
							if(settlementBillAmt.isLessThan(Money.ZERO)){
								financialTransactionPaytype = CR_FT_PAY_TYPE;
							}else if(settlementBillAmt.isGreaterThan(Money.ZERO)){
								financialTransactionPaytype = DR_FT_PAY_TYPE;
							}
							
							//Create Payment Request for Funding Payment							
							createFundingPaymentRequest(billId, BigInteger.valueOf(i++),billDt, 
								billCorrectionNoteFrBillId, billAlternateBillId, 
								accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
								financialTransactionPaytype, settlementBillAmt, settlementCurrencyCd, 
								settlementSubLevel, settlementReferenceSubLevel, 
								settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
								settlementFinAdjManNrt, settlementFastPayVal, settlementCaseIdentifier, 
								settlementPayRequestGranularities, billCreDttm, processDateTime);
							
						}else if(!accountPerAcctTypeStr.equals(FUND_ACCT_TYPE)
								&& settlementIndFlg.equals("N")
								&& !financialTransactionPaytype.equals(NA_FT_PAY_TYPE)
								&& !settlementBillAmt.isEqualTo(Money.ZERO)){
							
							//Set the pay type
							if(settlementBillAmt.isLessThan(Money.ZERO)){
								financialTransactionPaytype = CR_FT_PAY_TYPE;
							}else if(settlementBillAmt.isGreaterThan(Money.ZERO)){
								financialTransactionPaytype = DR_FT_PAY_TYPE;
							}
							
							//Create Payment Request for Non-Funding Payment
							createNonFundingPaymentRequest(billId, BigInteger.valueOf(i++),billDt, 
								billCorrectionNoteFrBillId, billAlternateBillId, 
								accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
								financialTransactionPaytype, settlementBillAmt, settlementCurrencyCd,  
								settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
								settlementFinAdjManNrt, settlementFastPayVal, settlementCaseIdentifier, 
								billCreDttm, processDateTime);
							
						}else if(!financialTransactionPaytype.equals(NA_FT_PAY_TYPE)
								&& settlementIndFlg.equals("Y")
								&& !settlementBillAmt.isEqualTo(Money.ZERO)){
							
							//Set the pay type
							if(settlementBillAmt.isLessThan(Money.ZERO)){
								financialTransactionPaytype = CR_FT_PAY_TYPE;
							}else if(settlementBillAmt.isGreaterThan(Money.ZERO)){
								financialTransactionPaytype = DR_FT_PAY_TYPE;
							}
							
							//Create Payment Request for Individual Payment
							createIndividualPaymentRequest(billId, BigInteger.valueOf(i++), billDt, 
								billCorrectionNoteFrBillId, billAlternateBillId, 
								accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
								financialTransactionPaytype, settlementBillAmt, settlementCurrencyCd,  
								settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
								settlementFinAdjManNrt, settlementFastPayVal, settlementCaseIdentifier, 
								billCreDttm, processDateTime);
							
						}else{
							//Log message in run tree
							logError(CustomMessageRepository.unableToCreatePaymentRequest(billId.getIdValue().trim()));
							
							//Skip bill from processing
							throw new ProceedToNextBill();
						}
					}					
		}
		
		/*
		 * This method process the overpayment base on the account type & FT bill amount
		 */
		private void processOverpayment(Bill_Id billId, String accountPerAcctTypeStr) throws ProceedToNextBill{
			
			if(accountPerAcctTypeStr.equals(FUND_ACCT_TYPE) && 
					financialTransactionBillAmount.isGreaterThan(Money.ZERO)){
				
				AdjustmentType_Id adjTypeId = new AdjustmentType_Id("MOVRPAYF");
				if(isOverpaymentAdjCharExists(billId, adjTypeId).isTrue()){	
					insertMultipleEntriesIntoCmPayCnfStaging(billId, processDateTime, getDSTId(billId), 
							 financialTransactionBillAmount, financialTransactionCurrencyCd.getId().getIdValue());
					
					throw new ProceedToNextBill();
				}
				
			}else if(accountPerAcctTypeStr.equals(CHRG_ACCT_TYPE) && 
					financialTransactionBillAmount.isLessThan(Money.ZERO)){
				
				AdjustmentType_Id adjTypeId = new AdjustmentType_Id("MOVRPAYC");
				if(isOverpaymentAdjCharExists(billId, adjTypeId).isTrue()){
				
					insertMultipleEntriesIntoCmPayCnfStaging(billId, processDateTime, getDSTId(billId), 
							 financialTransactionBillAmount, financialTransactionCurrencyCd.getId().getIdValue());	
					
					throw new ProceedToNextBill();
				}
	
			}else if(accountPerAcctTypeStr.equals(CHBK_ACCT_TYPE) 
					&& financialTransactionBillAmount.isGreaterThan(Money.ZERO)){
					
				AdjustmentType_Id adjTypeId = new AdjustmentType_Id("MOVRPYCB");
				if(isOverpaymentAdjCharExists(billId, adjTypeId).isTrue()){
				
					insertMultipleEntriesIntoCmPayCnfStaging(billId, processDateTime, getDSTId(billId), 
							 financialTransactionBillAmount, financialTransactionCurrencyCd.getId().getIdValue());	
					
					throw new ProceedToNextBill();
				}		
			}
		}
		
		/*
		 * This method will insert 2 entries in custom table CM_PAY_CNF_STG
		 */
		private void insertMultipleEntriesIntoCmPayCnfStaging(Bill_Id billId, DateTime processDateTime, String dstId, 
				Money financialTransactionBillAmount, String financialTransactionCurrencyCd){
			
			PreparedStatement statement = null;
			
			String sql =  " INSERT ALL " +
						" 	 INTO CM_PAY_CNF_STG (TXN_HEADER_ID,UPLOAD_DTTM,BO_STATUS_CD,STATUS_UPD_DTTM, " +
						" 	 MESSAGE_CAT_NBR,MESSAGE_NBR,ERROR_INFO,  " +
						" 	 PAY_DT,EXT_SOURCE_CD,EXT_TRANSMIT_ID,FINANCIAL_DOC_ID, " +
						" 	 FINANCIAL_DOC_LINE_ID,TENDER_AMT,CURRENCY_CD,BANKING_ENTRY_STATUS)  " +
						"      VALUES (:billId + 1,:processDateTime,'UPLD',null,0,0,' ',:processDateTime, " +
						" 			:dstId,:billId + 1,:billId,' ',:ftAmount,:currencyCd,'OVERPAID') " +
						" 	INTO CM_PAY_CNF_STG (TXN_HEADER_ID,UPLOAD_DTTM,BO_STATUS_CD,STATUS_UPD_DTTM, " +
						" 	MESSAGE_CAT_NBR,MESSAGE_NBR,ERROR_INFO,  " +
						" 	PAY_DT,EXT_SOURCE_CD,EXT_TRANSMIT_ID,FINANCIAL_DOC_ID, " +
						" 	FINANCIAL_DOC_LINE_ID,TENDER_AMT,CURRENCY_CD,BANKING_ENTRY_STATUS) " +
						"     VALUES (:billId + 2,:processDateTime,'UPLD',null,0,0,' ',:processDateTime, " +
						" 			:dstId,:billId + 2,:billId,' ',:ftAmount,:currencyCd,'OVERPAID') " +
						"  SELECT * FROM DUAL ";

			try{
			
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindDateTime("processDateTime", processDateTime);
				statement.bindString("dstId",dstId,null);
				statement.bindBigDecimal("ftAmount", financialTransactionBillAmount.getAmount());
				statement.bindString("currencyCd",financialTransactionCurrencyCd,null);
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_PAY_CNF_STG"));
			}finally{
				statement.close();
			}
		}
		
		/*
		 * This method will get the DST ID
		 */
		private String getDSTId(Bill_Id billId){
			
			String dstId = " ";
			PreparedStatement ps = null;
			
			String sql = " SELECT DISTINCT D.DST_ID " +
						"   FROM CI_FT A,  " +
						"        CI_ADJ_CHAR B,  " +
						"        CI_FT C,  " +
						"        CI_FT_GL D  " +
						"  WHERE A.BILL_ID = :billId " +
						"    AND A.FT_TYPE_FLG = :adjTypeFtFlg " +
						"    AND A.SIBLING_ID = B.ADJ_ID  " +
						"    AND B.CHAR_TYPE_CD = 'PAYID'  " +
						"    AND (B.ADHOC_CHAR_VAL = C.PARENT_ID  " +
						"      OR B.ADHOC_CHAR_VAL = C.SIBLING_ID)  " +
						"    AND C.FT_ID = D.FT_ID  " +
						"    AND D.GL_SEQ_NBR = 2 ";
			
			try{
				ps = createPreparedStatement(sql, "");
				ps.setAutoclose(false);
				ps.bindId("billId", billId);
				ps.bindLookup("adjTypeFtFlg", FinancialTransactionTypeLookup.constants.ADJUSTMENT);
				
				if(ps.list().size() > 0){
					
					SQLResultRow qRow = ps.firstRow();
					
					dstId = qRow.getString("DST_ID");	
				}
				
			}finally{
				if(notNull(ps)){
					ps.close();
				}
			}

			return dstId;
		}
		
		/*
		 * This methods checks if there is overpayment adjustment 
		 * characteristic exists for the bill
		 */
		private Bool isOverpaymentAdjCharExists(Bill_Id billId, AdjustmentType_Id adjTypeId){
						
			Bool overpayAdjCharSw = Bool.FALSE;
			
			Query query = createQuery(" FROM " +
										" 	FinancialTransaction ft, " +
										" 	AdjustmentCharacteristic adjChar, " +
										" WHERE " +
										" 	ft.billId = :billId " +
										" 	AND TRIM(ft.parentId) = :adjTypeCd " +
										" 	AND ft.financialTransactionType = :adjTypeFtFlg " +
										" 	AND ft.siblingId = adjChar.id.adjustment.id " +
										" 	AND adjChar.id.characteristicType = 'PAYID' ", "");
			
			query.bindId("billId", billId);
			query.bindId("adjTypeCd", adjTypeId);
			query.bindLookup("adjTypeFtFlg", FinancialTransactionTypeLookup.constants.ADJUSTMENT);
			
			if(query.list().size() > 0){
				overpayAdjCharSw = Bool.TRUE;
			}

			return overpayAdjCharSw;		
		}
		
		/*
		 * This method check if there is a non migration bill
		 */
		private void checkNonZeroMigrationBillExists(Bill_Id billId) throws ProceedToNextBill{
			
			PreparedStatement ps = null;
			
			String sql = " SELECT A.BILL_ID " +
						"   FROM CI_BILL A " +
						" WHERE EXISTS " +
						" (SELECT B.BSEG_ID" +
						"   FROM CI_BSEG B, " +
						"        CI_BSEG_CALC C, " +
						"        CI_BILL_CHG D, " +
						"        CI_PRICEITEM_CHAR E," + 
						"        CI_BILL_CHG_CHAR F " +
						"  WHERE B.BILL_ID = :billId " +
						"    AND A.BILL_ID = B.BILL_ID " +
						"    AND B.BSEG_ID = C.BSEG_ID " +
						"    AND C.HEADER_SEQ = 1 " +
						"    AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID " +
						"    AND D.PRICEITEM_CD = E.PRICEITEM_CD " + 
						"    AND E.CHAR_TYPE_CD = 'NON_ZERO' " +
						"    AND E.ADHOC_CHAR_VAL = 'Y' " +
						"    and D.billable_chg_id = F.BILLABLE_CHG_ID " +
						"    AND F.CHAR_TYPE_CD = 'NON_ZERO') ";
			
			try{
				
				ps = createPreparedStatement(sql, "");
				ps.setAutoclose(false);
				ps.bindId("billId", billId);
				
				if(ps.list().size() > 0){
					//insert record
					insertIntoCmBillDueDate(billId, 
							processDateTime.getDate(), 
							processDateTime);
					
					//proceed to next bill
					throw new ProceedToNextBill();
				}
				
			}finally{
				if(notNull(ps)){
					ps.close();
				}
			}
		}
		
		/*
		 * This method insert a record in custom table CM_BILL_DUE_DT
		 */
		private void insertIntoCmBillDueDate(Bill_Id billId, Date processDate, DateTime processDateTime){
			
			PreparedStatement statement = null;
			
			String sql = " INSERT " +
						 "    INTO CM_BILL_DUE_DT (BANK_ENTRY_EVENT_ID,BILL_ID,DUE_DT," +
						 "		IS_MERCH_BALANCED,UPLOAD_DTTM,STATUS_UPD_DTTM,PAY_DT,LINE_ID,BANKING_ENTRY_STATUS)" + 
						 "        VALUES (:billId,:billId,null,'N',:processDateTime, " +
						 "		:processDateTime,:processDate,'1','RELEASED')";
						 
			try{
			
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindDate("processDate", processDate);
				statement.bindDateTime("processDateTime", processDateTime);
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_BILL_DUE_DT"));
			}finally{
				statement.close();
			}
		}
		
		/*
		 * This method will create a bill settlement map record, 
		 * it inserts a record in custom table CM_BILL_SETT_MAP
		 */
		private void createBillSettlementMap(Bill_Id billId, 
				BigInteger rowNum ,Date processDateTime){
			
			PreparedStatement statement = null;
			
			String udfChar11Hash = "";
			String udfChar12Hash = "";
			String udfCharComb = "";
			
			String sql = " INSERT " +
						 "    INTO CM_BILL_SETT_MAP (BILL_ID,LINE_ID,SETT_LEVEL_GRANULARITY,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM)" + 
						 "        VALUES (:billId,:settlementLineId,:settlementLevelGran ,:processDateTime,'Y',null)";
					
			try{
				
				udfChar11Hash = String.valueOf(transactionUdfChar11.trim().hashCode());
				udfChar12Hash = String.valueOf(transactionUdfChar12.trim().hashCode());
				udfCharComb = udfChar11Hash.concat(udfChar12Hash);
								
				if(notNull(billAcctId) && notNull(billDt)){
					settlementLevelGranularities = 
							StringUtilities.padRight(billDt.toString(new DateFormat("YYMMDD")).
									concat(billAcctId.getId().getIdValue().
											concat(udfCharComb)), 30).replaceAll("[^a-zA-Z0-9]","");
				}
								
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindBigInteger("settlementLineId", rowNum);
				statement.bindDate("processDateTime", processDateTime);
				statement.bindString("settlementLevelGran", settlementLevelGranularities, null);
					
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_BILL_SETT_MAP"));
			}finally{
				if(notNull(statement)){
					statement.close();
				}
				
			}
		}
		
		/*
		 * This methods will create a funding payment request base on the account type, 
		 * settlement individual flag, FT pay type, settlement bill amount, and FT bill amount
		 */
		private void createFundingPaymentRequest(Bill_Id billId, BigInteger rowNum, Date billDt, 
				String billCorrectionNoteFrBillId, String billAlternateBillId, 
				String accountPerAcctTypeStr, CisDivision_Id accountPerCisDivision, String accountPerPerIdNbr, 
				String financialTransactionPaytype, Money settlementBillAmt,String settlementCurrencyCd, 
				String settlementSubLevel, String settlementReferenceSubLevel, 
				String settlementRelReserveFlg, String settlementRelWafFlg, String settlementImmFinAdjFlg, 
				String settlementFinAdjManNrt, String settlementFastPayVal, String settlementCaseIdentifier, 
				String settlementPayRequestGranularities, DateTime billCreDttm, DateTime processDateTime) 
				throws ProceedToNextBill{
						
			insertIntoCmPayReq(billId, rowNum, billDt, 
				 billCorrectionNoteFrBillId, billAlternateBillId, 
					 accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
					 financialTransactionPaytype, settlementBillAmt,settlementCurrencyCd, 
					 "N", settlementSubLevel, settlementReferenceSubLevel, 
					 settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
					 settlementFinAdjManNrt, settlementFastPayVal, settlementCaseIdentifier, 
					 settlementPayRequestGranularities, billCreDttm, processDateTime);
		}
		
		/*
		 * This method create a non funding payment request based on the account type,
		 *  settlement individual flag, FT pay type, and settlement bill amount
		 */
		private void createNonFundingPaymentRequest(Bill_Id billId, BigInteger rowNum, Date billDt, 
				String billCorrectionNoteFrBillId, String billAlternateBillId, 
				String accountPerAcctTypeStr, CisDivision_Id accountPerCisDivision, String accountPerPerIdNbr, 
				String financialTransactionPaytype, Money settlementBillAmt,String settlementCurrencyCd,  
				String settlementRelReserveFlg, String settlementRelWafFlg, String settlementImmFinAdjFlg, 
				String settlementFinAdjManNrt, String settlementFastPayVal, String settlementCaseIdentifier, 
				DateTime billCreDttm, DateTime processDateTime) throws ProceedToNextBill{
		
			String caseIdentifier = " ";
			
			//Set Case identifier
			if(isBlankOrNull(settlementSubLevel)){
				caseIdentifier = settlementCaseIdentifier;
				if(isBlankOrNull(caseIdentifier)){
					caseIdentifier = " ";
				}
				
			}else{
				caseIdentifier = settlementReferenceSubLevel;
				if(isBlankOrNull(caseIdentifier)){
					caseIdentifier = " ";
				}
			}
							
			insertIntoCmPayReq(billId, rowNum, billDt, 
					 billCorrectionNoteFrBillId, billAlternateBillId, 
 					 accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
 					 financialTransactionPaytype, settlementBillAmt, settlementCurrencyCd, 
 					 "N", null, null, 
 					 settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
 					 settlementFinAdjManNrt, settlementFastPayVal, caseIdentifier, 
 					 null, billCreDttm, processDateTime);
		}
		
		/*
		 * This method creates an individual payment request based on FT pay type,
		 *  settlement individual flag, and settlement bill amount
		 */
		private void createIndividualPaymentRequest(Bill_Id billId, BigInteger rowNum, Date billDt, 
				String billCorrectionNoteFrBillId, String billAlternateBillId, 
				String accountPerAcctTypeStr, CisDivision_Id accountPerCisDivision, String accountPerPerIdNbr, 
				String financialTransactionPaytype, Money settlementBillAmt,String settlementCurrencyCd,  
				String settlementRelReserveFlg, String settlementRelWafFlg, String settlementImmFinAdjFlg, 
				String settlementFinAdjManNrt, String settlementFastPayVal, String settlementCaseIdentifier, 
				DateTime billCreDttm, DateTime processDateTime) throws ProceedToNextBill{
		
			insertIntoCmPayReq(billId, rowNum, billDt, 
				billCorrectionNoteFrBillId, billAlternateBillId, 
	 			accountPerAcctTypeStr, accountPerCisDivision, accountPerPerIdNbr, 
	 			financialTransactionPaytype, settlementBillAmt, settlementCurrencyCd, 
	 			"Y", null, null, 
	 			settlementRelReserveFlg, settlementRelWafFlg, settlementImmFinAdjFlg, 
	 			settlementFinAdjManNrt, settlementFastPayVal, settlementCaseIdentifier, 
	 			null, billCreDttm, processDateTime);
		}
		
		/*
		 * This method insert a record in a custom table CM_PAY_REQ_GRANULARITIES
		 */
		private void insertIntoCmPayReqGranularities(Bill_Id billId, BigInteger rowNum, 
				DateTime processDateTime, String udfChar12Key, 
				String udfChar12Value){
			
			PreparedStatement statement = null;
						
			String sql = " INSERT " +
						 "    INTO CM_PAY_REQ_GRANULARITIES (BILL_ID,LINE_ID,GRANULARITIES_TYPE,REFERENCE_VAL, " +
						 " 							UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM)" + 
						 "        VALUES (:billId,:lineId,:granularitiesType,:referenceValue, " +
						 " :processDateTime,'Y',null)";

			try{
			
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindString("granularitiesType", udfChar12Key, null);
				statement.bindString("referenceValue", udfChar12Value, null);
				statement.bindDate("processDateTime", processDateTime.getDate());
				statement.bindBigInteger("lineId", rowNum);
				
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_PAY_REQ_GRANULARITIES"));
			}finally{
				if(notNull(statement)){
					statement.close();
				}
				
			}	
		}
		
		/*
		 * This method insert a record in a custom table CM_PAY_REQ
		 */
		private void insertIntoCmPayReq(Bill_Id billId, BigInteger rowNum, Date billDt, 
				String billCorrectionNoteFrBillId, String billAlternateBillId, 
				String accountPerAcctTypeStr, CisDivision_Id accountPerCisDivision, String accountPerPerIdNbr, 
				String financialTransactionPaytype, Money settlementBillAmt,String settlementCurrencyCd, 
				String settlementIndFlg, String settlementSubLevel, String settlementReferenceSubLevel, 
				String settlementRelReserveFlg, String settlementRelWafFlg, String settlementImmFinAdjFlg, 
				String settlementFinAdjManNrt, String settlementFastPayVal, String settlementCaseIdentifier, 
				String settlementPayRequestGranularities, DateTime billCreDttm, DateTime processDateTime){
			
			PreparedStatement statement = null;
								
			String sql = " INSERT " +
						 "    INTO CM_PAY_REQ (BILL_ID,LINE_ID,BILL_DT,CR_NOTE_FR_BILL_ID,ALT_BILL_ID, " +
						 "			ACCT_TYPE,CIS_DIVISION,PER_ID_NBR,PAY_TYPE,BILL_AMT,CURRENCY_CD, " +
						 "			IS_IND_FLG,SUB_STLMNT_LVL,SUB_STLMNT_LVL_REF,REL_RSRV_FLG, " +
						 "			REL_WAF_FLG,IS_IMD_FIN_ADJ,FIN_ADJ_MAN_NRT,FASTEST_ROUTE_INDICATOR, " +
						 "			CASE_IDENTIFIER,PAY_REQ_GRANULARITIES,CREATE_DTTM,UPLOAD_DTTM, " +
						 "			EXTRACT_FLG,EXTRACT_DTTM) " + 
						 "    VALUES (:billId,:rowNum,:billDt,:billCorrectionNoteFrBillId, " +
						 " 			:billAlternateBillId,:accountPerAcctTypeStr,:accountPerCisDivision, " +
						 " 			:accountPerPerIdNbr,:financialTransactionPaytype,:settlementBillAmt, " +
						 " 			:settlementCurrencyCd,:settlementIndFlg,:settlementSubLevel, " +
						 " 			:settlementReferenceSubLevel,:settlementRelReserveFlg,:settlementRelWafFlg, " +
						 " 			:settlementImmFinAdjFlg,:settlementFinAdjManNrt,:settlementFastPayVal, " +
						 " 			:settlementCaseIdentifier,:settlementPayRequestGranularities,:billCreDttm, " +
						 " 			:processDateTime,'Y',null)";
						 	 
			try{
				
				statement = createPreparedStatement(sql, "");
				statement.setAutoclose(false);
				statement.bindId("billId", billId);
				statement.bindBigInteger("rowNum", rowNum);
				statement.bindDate("billDt", billDt);
				statement.bindString("billCorrectionNoteFrBillId", billCorrectionNoteFrBillId, null);
				statement.bindString("billAlternateBillId", billAlternateBillId, null);
				statement.bindString("accountPerAcctTypeStr", accountPerAcctTypeStr, null);
				statement.bindId("accountPerCisDivision", accountPerCisDivision);
				statement.bindString("accountPerPerIdNbr", accountPerPerIdNbr, null);
				statement.bindString("financialTransactionPaytype", financialTransactionPaytype, null);
				statement.bindMoney("settlementBillAmt", settlementBillAmt);
				statement.bindString("settlementCurrencyCd", settlementCurrencyCd, null);
				statement.bindString("settlementIndFlg", settlementIndFlg, null);
				statement.bindString("settlementSubLevel", settlementSubLevel, null);
				statement.bindString("settlementReferenceSubLevel", settlementReferenceSubLevel, null);
				statement.bindString("settlementRelReserveFlg", settlementRelReserveFlg, null);
				statement.bindString("settlementRelWafFlg", settlementRelWafFlg, null);
				statement.bindString("settlementImmFinAdjFlg", settlementImmFinAdjFlg, null);
				statement.bindString("settlementFinAdjManNrt", settlementFinAdjManNrt, null);
				statement.bindString("settlementFastPayVal", settlementFastPayVal, null);
				statement.bindString("settlementCaseIdentifier", settlementCaseIdentifier, null);
				statement.bindString("settlementPayRequestGranularities", settlementPayRequestGranularities, null);
				statement.bindDateTime("billCreDttm", billCreDttm);
				statement.bindDate("processDateTime", processDateTime.getDate());
				
				statement.execute();
				
			}catch (ApplicationError e) {
				addError(CustomMessageRepository.errorInsertingInTable("CM_PAY_REQ"));
			}finally{
				statement.close();
			}
		}
		
		public void finalizeThreadWork() throws ThreadAbortedException,RunAbortedException {
			
		}	
	}
}
