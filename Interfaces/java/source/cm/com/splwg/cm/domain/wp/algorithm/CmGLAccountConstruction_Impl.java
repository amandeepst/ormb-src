/***********************************************************************************
 * FileName                   : CmGLAccountConstruction_Impl.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May	21, 2018
 * Version Number             : 0.4
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1		 NA				May	21, 2018		Kaustubh Kale		 Initial Version (PAM-16440)
0.2 	 NA				Jun 14, 2018		Kaustubh Kale		 Changes for batch rerun
0.3		 NA				Jul 03, 2018		Kaustubh Kale		 Performance improvements
0.4		 NA				Aug 16, 2018		Swapnil Gupta		 Update GL Account For Negative FT (NAP- 30741)
0.5		 NA			    Aug 21, 2018		Kaustubh Kale		 Added ACCT_NBR extra column (NAP-31893)
0.6      NA             Oct 26, 2018        Swapnil Gupta        L0 Product Mapping change(NAP-33635)
0.7		 NA				Oct 29, 2018		Kaustubh Kale		 Payment to accounting reconciliation (NAP-35019)
 ***********************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import com.splwg.base.api.Query;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.lookup.CharacteristicTypeLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.message.MessageCategory_Id;
import com.splwg.base.domain.common.message.Message_Id;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCodeGlAccountConstructionAlgorithmSpot;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode_Id;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.billing.trialBilling.TrialFinancialTransaction;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.customerinfo.account.AccountNumber;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.person.PersonCharacteristic;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransactionGeneralLedger_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Kaustubh
 *
@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = taxDistributionCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = taxGLAccountCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = overrideDistributionCodeCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (name = idType, required = true, type = string)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = counterPartyCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = intercompanyCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (name = intercompanyCharValue, required = true, type = string)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = businessUnitCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = schemeCharType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = accountNumberType, name = fundAccountNumberType, required = true, type = entity)
 *            , @AlgorithmSoftParameter (name = fundAccountNumber, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = fundGLAccount, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = payType, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = negGlAccount, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = dstCdForDebtFund, required = true, type = string)})
 */
public class CmGLAccountConstruction_Impl extends CmGLAccountConstruction_Gen implements
GeneralLedgerDistributionCodeGlAccountConstructionAlgorithmSpot {

	private static final Logger logger = LoggerFactory.getLogger(CmGLAccountConstruction_Impl.class);
	private static final String NO = "N";
	private static final String NOT_APPLICABLE = "NA";
	private static final String NOT_SLASH_APPLICABLE = "N/A";
	private static final FinancialTransactionTypeLookup FTTYPE_BS = FinancialTransactionTypeLookup.constants.BILL_SEGMENT;
	private static final FinancialTransactionTypeLookup FTTYPE_BX = FinancialTransactionTypeLookup.constants.BILL_CANCELLATION;
	private static final FinancialTransactionTypeLookup FTTYPE_AD = FinancialTransactionTypeLookup.constants.ADJUSTMENT;
	private static final FinancialTransactionTypeLookup FTTYPE_AX = FinancialTransactionTypeLookup.constants.ADJUSTMENT_CANCELLATION;
	private static final FinancialTransactionTypeLookup FTTYPE_PS = FinancialTransactionTypeLookup.constants.PAY_SEGMENT;
	private static final FinancialTransactionTypeLookup FTTYPE_PX = FinancialTransactionTypeLookup.constants.PAY_CANCELLATION;

	private static final BillStatusLookup BILL_COMPLETE = BillStatusLookup.constants.COMPLETE;
	private static final BigInteger MESSAGE_CATEGORY = new BigInteger("92000");
	private static final BigInteger ACCT_DT_MISSING_MSG_NBR = new BigInteger("1401");
	private static final BigInteger SCHEME_MISSING_MSG_NBR = new BigInteger("1402");
	private static final BigInteger BUSINESS_UNIT_MISSING_MSG_NBR = new BigInteger("1403");
	private static final String WAFMREL ="WAFMREL";
	private static final String STATMREL ="STATMREL";
	private static final String DYNMREL ="DYNMREL";
	private static final String DELIMITER ="~";
	
	private static ArrayList<String> ftGlTmpArray = new ArrayList<String>();
	private static HashMap<String, SQLResultRow> ftGlMapHashMap = new HashMap<String, SQLResultRow>();
	private static HashMap<String, String> intercompanyCharHashMap = new HashMap<String, String>();
	private static HashMap<String, String> counterpartyCharHashMap = new HashMap<String, String>();
	private static HashMap<String, String> schemeCharHashMap = new HashMap<String, String>();
	private static HashMap<String, String> overrideDistCharHashMap = new HashMap<String, String>();
	private static HashMap<String,String> overrideAdjCharHashMap =new HashMap<>();

	private String glAccount = " ";
	private FinancialTransaction financialTransaction;
	private GeneralLedgerDistributionCode glDistributionCode;
	private BigInteger glSequenceNumber;

	private FinancialTransactionTypeLookup ftType;
	private BillSegment_Id bsegId;
	private FinancialTransaction_Id ftId;
	private Currency_Id ftCurrency;
	private Date ftAccountingDate;
	private Money ftGlAmount;
	private String ftDivision;
	private GeneralLedgerDistributionCode_Id glDistributionCodeId;
	private String intercompanyCharVal;
	private String counterPartyCharval;
	private String overrideDistributionCodeCharVal;
	private String schemeCharVal;
	private Bill_Id billId;
	private String ftParentId;

	public void invoke() {
		logger.debug("CmGLAccountConstruction_Impl :: invoke() method :: START");

		// Get Soft parameters
		String taxDistributionCode = getTaxDistributionCode();
		CharacteristicType taxGLAccountCharType = getTaxGLAccountCharType();
		CharacteristicType overrideDistributionCodeCharType = getOverrideDistributionCodeCharType();
		String perIdNbr = getIdType();
		CharacteristicType counterPartyCharType = getCounterPartyCharType();
		CharacteristicType intercompanyCharType = getIntercompanyCharType();
		String intercompanyCharValue = getIntercompanyCharValue();
		CharacteristicType businessUnitCharType = getBusinessUnitCharType();
		CharacteristicType schemeCharType = getSchemeCharType();
		AccountNumberType fundAccountNumberType = getFundAccountNumberType();
		String fundAccountNumber = getFundAccountNumber();
		String fundGLAccount = getFundGLAccount();
		String negGlAccount = getNegGlAccount();
		String payType = getPayType();
		String dstCdForDebtFund = getDstCdForDebtFund();

		String intercompany = " ";
		String costCentre = " ";
		String businessUnit = " ";
		String scheme = " ";
		String schemeUdfChar13 = "";
		String fundCurrency = " ";
		String binSettleCurrency = " ";
		
		// Load ftGlTmpArray and ftGlMapHashMap
		if(ftGlTmpArray.isEmpty()) {
			loadFtGlTmpArray();
		}
		if(ftGlMapHashMap.isEmpty()) {
			loadFtGlMapHashMap();			
		}
		
		if(overrideAdjCharHashMap.isEmpty()){
			loadOverrideAdjCharHashMap();
		}

		ftType = financialTransaction.getDTO().getFinancialTransactionType();
		if(ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX))
			bsegId = financialTransaction.fetchSiblingBillSegment().getId();
		ftId = financialTransaction.getId();
		ftCurrency = financialTransaction.getCurrency().getId();
		ftAccountingDate = financialTransaction.getAccountingDate();
		FinancialTransactionGeneralLedger_Id ftGlId = new FinancialTransactionGeneralLedger_Id(financialTransaction, glSequenceNumber);
		ftGlAmount = ftGlId.getEntity().getAmount();
		ftDivision = financialTransaction.getDivision().getId().getIdValue().trim();
		glDistributionCodeId = glDistributionCode.getId();
		billId = financialTransaction.getBillId();
		ftParentId=financialTransaction.getParentId();

		ServiceAgreement servAggr = financialTransaction.getServiceAgreement();
		Account account = servAggr.getAccount();
		Person person = getAccountMainPerson(account);
		// NAP-31893 : Add ACCT_NBR extra column
		String acctNbr = getAccountNumber(account);
		BillableCharge billChg = getBillableCharge(bsegId);
		PriceItem priceItem = notNull(billChg) ? billChg.getPriceItemCodeId().getEntity() : null;
		
		// Fetch child_product from CM_TXN_ATTRIBUTES_MAP (for scheme)
		if(notNull(billChg)) {
			schemeUdfChar13 = fetchSchemeFromTxnAttributesMap(billChg.getId());
		}
		
		// Fetch division characteristic values
		if(intercompanyCharHashMap.isEmpty() || counterpartyCharHashMap.isEmpty()) {
			fetchDivisionChars(counterPartyCharType, intercompanyCharType);
		}
		
		String divIntercompanyCharKey = ftDivision.concat(intercompanyCharType.getId().getIdValue().trim());
		String divCounterpartyCharKey = ftDivision.concat(counterPartyCharType.getId().getIdValue().trim());
		intercompanyCharVal = intercompanyCharHashMap.get(divIntercompanyCharKey);
		counterPartyCharval = counterpartyCharHashMap.get(divCounterpartyCharKey);
		
		// Fetch Price item characteristic values
		String piOverrideDistCharKey = null;
		String piSchemeCharKey = null;
		if(notNull(priceItem)) {
			String strPriceitem = priceItem.getId().getIdValue().trim();
			if(overrideDistCharHashMap.isEmpty() || schemeCharHashMap.isEmpty()) {
				fetchPriceItemChars(overrideDistributionCodeCharType, schemeCharType);
			}
			
			piOverrideDistCharKey = strPriceitem.concat(overrideDistributionCodeCharType.getId().getIdValue().trim());
			piSchemeCharKey = strPriceitem.concat(schemeCharType.getId().getIdValue().trim());
			overrideDistributionCodeCharVal = overrideDistCharHashMap.get(piOverrideDistCharKey);
			if(isBlankOrNull(schemeUdfChar13))
				schemeCharVal = schemeCharHashMap.get(piSchemeCharKey);
			else{
				schemeCharVal = schemeCharHashMap.get(schemeUdfChar13.concat(schemeCharType.getId().getIdValue().trim()));
			}
				
		}

		// Set GL Account of the Financial Transaction
		boolean doesDistCdExist = ftGlTmpArray.contains(glDistributionCodeId.getIdValue());
//		// NAP-31545
		if((ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX)) && BILL_COMPLETE.equals(billId.getEntity().getBillStatus())) {
//		if(ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX)) {
			if(taxDistributionCode.equalsIgnoreCase(glDistributionCodeId.getIdValue().trim())) {
				glAccount = fetchTaxGLAccountChar(bsegId, taxGLAccountCharType);
			}
			else if(doesDistCdExist) {
				// RIA: NAP- 30741
				glAccount = glDistributionCodeId.getIdValue().trim();
				if(glAccount.equals(dstCdForDebtFund)){
					glAccount = updateGlAccountForNegativeFT(glAccount, billId, fundAccountNumber, payType, negGlAccount, bsegId);
				}
			}
			else {
				if(!doesDistCdExist) {
					glAccount = isBlankOrNull(overrideDistributionCodeCharVal) ? " " : overrideDistributionCodeCharVal;
				}
				if(isBlankOrNull(glAccount)) {
					glAccount = fetchOverrideDistCharFromBillSeg(overrideDistributionCodeCharType);
				}
			}
		}
		else if(ftType.equals(FTTYPE_AD) || ftType.equals(FTTYPE_AX) || ftType.equals(FTTYPE_PS) || ftType.equals(FTTYPE_PX)) {
			glAccount = glDistributionCodeId.getIdValue().trim();
			// NAP-31545
			if(ftType.equals(FTTYPE_AD) || ftType.equals(FTTYPE_AX)) {
				if((financialTransaction.getShouldShowOnBill().isTrue() && (notNull(billId) && !isBlankOrNull(billId.getIdValue())))
						|| (financialTransaction.getShouldShowOnBill().isFalse())) 
				{
					 String key = glDistributionCodeId.getIdValue().trim().concat(DELIMITER).concat(ftDivision.toString().trim()).concat(DELIMITER)
							 .concat(ftCurrency.getTrimmedValue());
					if(glDistributionCodeId.getIdValue().trim().equals(ftParentId.trim()) && (overrideAdjCharHashMap.containsKey(key)))
					{
						//Fetch from map
						glAccount = overrideAdjCharHashMap.get(key).trim();
					}
					else 
					{
					   glAccount = glDistributionCodeId.getIdValue().trim();
					}
				} 
			}
			else if(ftType.equals(FTTYPE_PS) || ftType.equals(FTTYPE_PX)) {
				if(acctNbr.equals(fundAccountNumber) && glDistributionCodeId.getIdValue().trim().equals(dstCdForDebtFund) ){
					glAccount = checkForPayType(glDistributionCodeId.getIdValue().trim(),billId,negGlAccount, payType, ftId);
				}
				else {
					glAccount = glDistributionCodeId.getIdValue().trim();
				}
			}
		}

		SQLResultRow ftGlMapResult = ftGlMapHashMap.get(glAccount.trim());
		boolean doesGLAcctExist = false;
		String intercompanyFlag = " ";
		String schemeFlag = " ";
		if(notNull(ftGlMapResult)) {
			doesGLAcctExist = true;
			intercompanyFlag = ftGlMapResult.getString("INTERCOMPANY_FLG").trim();
			schemeFlag = ftGlMapResult.getString("SCHEME_FLG").trim();
		}
		
		// Set cost centre
		costCentre = NOT_APPLICABLE;

		// Set counter party
		String counterParty = isBlankOrNull(counterPartyCharval) ? " " : counterPartyCharval; 

		// Retrieve Intercompany
		if(!perIdNbr.equalsIgnoreCase(counterParty) || !intercompanyCharValue.equalsIgnoreCase(intercompanyCharVal)) {
			intercompany = NOT_APPLICABLE;
		}
		else if(perIdNbr.equalsIgnoreCase(counterParty) && intercompanyCharValue.equalsIgnoreCase(intercompanyCharVal)) {
			if(doesGLAcctExist && NO.equalsIgnoreCase(intercompanyFlag)){
				intercompany = NOT_APPLICABLE;	
			} else {
				intercompany = counterParty;
			}
		}

		// Retrieved Business Unit of Person
		businessUnit = fetchBusinessUnitChar(person, businessUnitCharType);

		// Retrieve Scheme
		if(notNull(priceItem)) {
			if(doesGLAcctExist && NO.equalsIgnoreCase(schemeFlag)) {
				scheme = NOT_APPLICABLE;
			} else {
				scheme = isBlankOrNull(schemeCharVal) ? " " : schemeCharVal;
			}
		}
		else if(ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX)) {
			if(doesGLAcctExist && NO.equalsIgnoreCase(schemeFlag)) {
				scheme = NOT_APPLICABLE;
			} else {
				scheme = NOT_SLASH_APPLICABLE;
			}
		}
		else {
			scheme = NOT_APPLICABLE;
		}
		
		// Set Fund Currency
		fundCurrency = getFundCurrency(account, fundAccountNumberType, fundAccountNumber, fundGLAccount);

		// Set BIN Settle Currency
		binSettleCurrency = getBinSettleCurrency(fundCurrency, billChg);

		boolean isFTError = checkIfFtIsInError(ftAccountingDate, scheme, businessUnit);
		if(!isFTError && !isBlankOrNull(glAccount)) {
			insertIntoFtGlAslStaging(counterParty, businessUnit, costCentre, intercompany, scheme, acctNbr);
			if(!isBlankOrNull(binSettleCurrency))
				insertIntoFtGlFxStaging(fundCurrency, binSettleCurrency, counterParty, businessUnit);
		}

		logger.debug("CmGLAccountConstruction_Impl :: invoke() method :: END");
	}


	/************************************************* Check/Load Data Methods *************************************************/

	/**
	 * Load FT GL TMP array which holds all data for the table CM_FT_GL_TMP
	 */
	private void loadFtGlTmpArray() {
		logger.debug("CmGLAccountConstruction_Impl :: loadFtGlTmpArray() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT DST_ID FROM CM_FT_GL_TMP ");
		sb.append(" WHERE DIST_TYPE = 'REG' ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.setAutoclose(false);
		List<SQLResultRow> resultList = ps.list();

		if(notNull(resultList) && resultList.size()>0) {
			for(SQLResultRow result:resultList) {
				String dstId = result.getString("DST_ID");
				if(!ftGlTmpArray.contains(dstId))
					ftGlTmpArray.add(dstId);	
			}
		}
		
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: loadFtGlTmpArray() method :: END");
	}

	/**
	 * Load override AJ Map hash map which holds data for the table CI_ADJ_TY_CHAR
	 */
	private void loadOverrideAdjCharHashMap() 
	{
		logger.debug("CmGLAccountConstruction_Impl :: loadOverrideAdjCharHashMap() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT ADJ_TYPE_CD, CHAR_TYPE_CD, ADHOC_CHAR_VAL, CHAR_VAL_FK1, CHAR_VAL_FK2 FROM CI_ADJ_TY_CHAR WHERE CHAR_TYPE_CD ='OV_DSTCD' ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.setAutoclose(false);
		List<SQLResultRow> resultList = ps.list();
		
		if(notNull(resultList) && resultList.size()>0) {
			for(SQLResultRow result:resultList) {
				String adjTypeCharKey = result.getString("ADJ_TYPE_CD").trim().concat(DELIMITER).concat(result.getString("CHAR_VAL_FK1").trim())
						.concat(DELIMITER).concat(result.getString("CHAR_VAL_FK2").trim());	
				String adhocCharVal = result.getString("ADHOC_CHAR_VAL");
				if(!overrideAdjCharHashMap.containsKey(adjTypeCharKey)) {
					overrideAdjCharHashMap.put(adjTypeCharKey, adhocCharVal);
				}
			}
		}

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: loadFtGlMapHashMap() method :: END");
	}
	
	/**
	 * Load FT GL TMP hash map which holds all data for the table CM_FT_GL_MAP
	 */
	private void loadFtGlMapHashMap() {
		logger.debug("CmGLAccountConstruction_Impl :: loadFtGlMapHashMap() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT DST_ID, INTERCOMPANY_FLG, SCHEME_FLG FROM CM_FT_GL_MAP ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.setAutoclose(false);
		List<SQLResultRow> resultList = ps.list();
		
		if(notNull(resultList) && resultList.size()>0) {
			for(SQLResultRow result:resultList) {
				String dstId = result.getString("DST_ID");
				if(!ftGlMapHashMap.containsKey(dstId)) {
					ftGlMapHashMap.put(dstId, result);
				}
			}
		}

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: loadFtGlMapHashMap() method :: END");
	}
	/**
	 * Determine if the Financial Transaction is in Error 
	 * If yes, then insert error record in CM_FT_GL_ASL_ERR table
	 * Else, return true
	 * @param businessUnit 
	 * @param scheme 
	 * @param accountingDate 
	 * @return true: If FT is in error
	 * 		  false: otherwise
	 */
	private boolean checkIfFtIsInError(Date accountingDate, String scheme, String businessUnit) {
		logger.debug("CmGLAccountConstruction_Impl :: checkIfFtIsInError() method :: START");
		boolean isFtInError = false;
		Message_Id messageId = null;
		if(isNull(accountingDate)) {
			isFtInError = true;
			messageId = new Message_Id(new MessageCategory_Id(MESSAGE_CATEGORY), ACCT_DT_MISSING_MSG_NBR);
		} 
		else if(isBlankOrNull(scheme)) {
			isFtInError = true;
			messageId = new Message_Id(new MessageCategory_Id(MESSAGE_CATEGORY), SCHEME_MISSING_MSG_NBR);
		} 
		else if(isBlankOrNull(businessUnit)) {
			isFtInError = true;
			messageId = new Message_Id(new MessageCategory_Id(MESSAGE_CATEGORY), BUSINESS_UNIT_MISSING_MSG_NBR);
		}
		if(isFtInError) {
			insertIntoFtGlAslError(messageId);
			glAccount = " ";
		}
		logger.debug("CmGLAccountConstruction_Impl :: checkIfFtIsInError() method :: END");
		return isFtInError;
	}

	/************************************************* Get Data Methods *************************************************/

	/**
	 * Get Main customer of the account
	 * @param account
	 * @return mainPerson
	 */
	private Person getAccountMainPerson(Account account) {
		logger.debug("CmGLAccountConstruction_Impl :: getAccountMainPerson() method :: START");
		Bool isMainCust = Bool.FALSE;
		Person per = null;
		Iterator<AccountPerson> acctPerIterator = account.getPersons().iterator();
		while(acctPerIterator.hasNext()) {
			AccountPerson acctper = acctPerIterator.next();
			isMainCust = acctper.getIsMainCustomer();
			if(isMainCust.isTrue()) {
				per = acctper.fetchIdPerson();
				break;
			}
		}
		logger.debug("CmGLAccountConstruction_Impl :: getAccountMainPerson() method :: END");
		return per;
	}
	
	/**
	 * Get account number for ACCT_NBR_TYPE_CD = 'ACCTTYPE'
	 * @param account
	 * @return acctNbr
	 */
	private String getAccountNumber(Account account) {
		logger.debug("CmGLAccountConstruction_Impl :: getAccountNumber() method :: START");
		String acctNbr = null;
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT ACCT_NBR FROM CI_ACCT_NBR WHERE ACCT_ID =:acctId AND ACCT_NBR_TYPE_CD ='ACCTTYPE' ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("acctId", account.getId());
		ps.setAutoclose(false);
		SQLResultRow result = ps.firstRow();
		
		if(notNull(result)) {
			acctNbr = result.getString("ACCT_NBR").trim();
		}

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: getAccountNumber() method :: END");
		return acctNbr;
	}

	/**
	 * Get Fund currency
	 * If GL Account = Fund GL Account (Algorithm Parameter)
	 * and Account Number Type = Fund Account Number Type (Algorithm Parameter) 
	 * and Account Number = Fund Account Number (Algorithm Parameter) 
	 * then Fund currency = Account Currency
	 * else Fund currency = BLANK
	 * @param account
	 * @param fundAccountNumberType
	 * @param fundAccountNumber
	 * @param fundGLAccount
	 * @return fundCurrency
	 */
	private String getFundCurrency(Account account, AccountNumberType fundAccountNumberType, String fundAccountNumber, String fundGLAccount) {
		logger.debug("CmGLAccountConstruction_Impl :: getFundCurrency() method :: START");
		String fundCurrency = " ";
		if(fundGLAccount.equalsIgnoreCase(glAccount)) {
			Iterator<AccountNumber> acctNbrIterator = account.getAccountNumber().iterator();
			while(acctNbrIterator.hasNext()) {
				AccountNumber acctNbr = acctNbrIterator.next();
				String strAcctNbr = acctNbr.getAccountNumber().trim();
				AccountNumberType acctNbrType = acctNbr.getId().getAccountIdentifierType();
				if(fundAccountNumber.equalsIgnoreCase(strAcctNbr) && fundAccountNumberType.equals(acctNbrType)) {
					fundCurrency = account.getCurrency().getId().getIdValue().trim();
					break;
				}
			}
		} 
		logger.debug("CmGLAccountConstruction_Impl :: getFundCurrency() method :: END");
		return fundCurrency;
	}

	/**
	 * Get BIN Settle Currency
	 * @param fundCurrency 
	 * @param billChg 
	 * @return binSettleCurrency
	 */
	private String getBinSettleCurrency(String fundCurrency, BillableCharge billChg) {
		logger.debug("CmGLAccountConstruction_Impl :: getBinSettleCurrency() method :: START");
		String binSettleCurrency = " ";
		if(!isBlankOrNull(fundCurrency) && notNull(billChg)) {
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT UDF_CHAR_15 FROM CM_TXN_ATTRIBUTES_MAP ");
			sb.append(" WHERE BILLABLE_CHG_ID =:billChgId ");

			ps = createPreparedStatement(sb.toString(),"");
			ps.bindId("billChgId", billChg.getId());
			ps.setAutoclose(false);

			SQLResultRow result = ps.firstRow();
			if(notNull(result)) {
				binSettleCurrency = result.getString("UDF_CHAR_15");
				if(isBlankOrNull(binSettleCurrency) || fundCurrency.equalsIgnoreCase(binSettleCurrency)) {
					binSettleCurrency = " ";
				}
			}
			if (notNull(ps)) {
				ps.close();
				ps = null;
			}
		}
		logger.debug("CmGLAccountConstruction_Impl :: getBinSettleCurrency() method :: END");
		return binSettleCurrency;
	}

	/**
	 * Get Billable Charge entity corresponding to Bill Segment
	 * @param bsegId
	 * @return BillableCharge
	 */
	private BillableCharge getBillableCharge(BillSegment_Id bsegId) {
		logger.debug("CmGLAccountConstruction_Impl :: getBillableCharge() method :: START");
		BillableCharge bchgEntity = null;

		if(notNull(bsegId)) {
			StringBuilder getBchgHQL = new StringBuilder();
			getBchgHQL.append(" FROM BillSegmentCalculationHeader bscl, BillableCharge bchg ");
			getBchgHQL.append(" WHERE bscl.billableChargeId = bchg.id ");
			getBchgHQL.append(" AND bscl.id.billSegment.id =:bsegId ");

			Query<BillableCharge_Id> query = createQuery(getBchgHQL.toString(),"");
			query.bindId("bsegId", bsegId);
			query.addResult("billChgId", "bchg.id");

			BillableCharge_Id result = query.firstRow();
			if(notNull(result)) {
				bchgEntity = result.getEntity();
			}
			getBchgHQL.setLength(0);
			getBchgHQL =null;	
		}
		logger.debug("CmGLAccountConstruction_Impl :: getBillableCharge() method :: END");
		return bchgEntity;
	}

	/***************************************** Fetch Characteristic Value Methods *****************************************/

	/**
	 * Fetch Tax GL Account Bill Segment Calculation Line Char
	 * @param bsegId
	 * @param taxGLAccountCharType
	 * @return taxGLAcctBillSegCalcLineCharVal
	 */
	private String fetchTaxGLAccountChar(BillSegment_Id bsegId, CharacteristicType taxGLAccountCharType) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchTaxGLAccountChar() method :: START");
		String taxGLAcctBillSegCalcLineCharVal = " ";
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT CHAR_VAL FROM CI_BSEG_CL_CHAR ");
		sb.append(" WHERE BSEG_ID =:bsegId ");
		sb.append(" AND CHAR_TYPE_CD =:charTypeCd ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("bsegId", bsegId);
		ps.bindId("charTypeCd", taxGLAccountCharType.getId());
		ps.setAutoclose(false);

		SQLResultRow result = ps.firstRow();
		if(notNull(result)) {
			String strTaxGLAcctBillSegCalcLineCharVal = result.getString("CHAR_VAL");
			if(!isBlankOrNull(strTaxGLAcctBillSegCalcLineCharVal)) {
				taxGLAcctBillSegCalcLineCharVal = strTaxGLAcctBillSegCalcLineCharVal.trim();
			}
		}
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: fetchTaxGLAccountChar() method :: END");
		return taxGLAcctBillSegCalcLineCharVal;
	}

	/**
	 * Fetch division characteristics
	 * 1. Counter Party Char Value
	 * 2. Intercompany Char Value
	 * @param counterPartyCharType
	 * @param intercompanyCharType
	 */
	private void fetchDivisionChars(CharacteristicType counterPartyCharType, CharacteristicType intercompanyCharType) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchDivisionChars() method :: START");
		String charValue = null;
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT CIS_DIVISION, CHAR_TYPE_CD, CHAR_VAL, CHAR_VAL_FK1, ADHOC_CHAR_VAL ");
		sb.append(" FROM CI_CIS_DIV_CHAR ");
		sb.append(" WHERE CHAR_TYPE_CD IN (:counterPartyCharType, :intercompanyCharType) ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("counterPartyCharType", counterPartyCharType.getId());
		ps.bindId("intercompanyCharType", intercompanyCharType.getId());
		ps.setAutoclose(false);

		List<SQLResultRow> resultList = ps.list();
		if(notNull(resultList) && resultList.size() > 0) {
			for(SQLResultRow result:resultList) {
				String strCisDivId = result.getString("CIS_DIVISION").trim();
				String strCharType = result.getString("CHAR_TYPE_CD").trim();
				CharacteristicType_Id charTypeId = new CharacteristicType_Id(strCharType);
				String divChar = strCisDivId.concat(strCharType);
				
				CharacteristicType charType = charTypeId.getEntity();
				CharacteristicTypeLookup charTypeLookUp = charType.getCharacteristicType();
				if(charTypeLookUp.isAdhocValue() || charTypeLookUp.isFileLocationValue()) {
					charValue = result.getString("ADHOC_CHAR_VAL").trim();
				} else if(charTypeLookUp.isPredefinedValue()) {
					charValue = result.getString("CHAR_VAL").trim();
				} else if(charTypeLookUp.isForeignKeyValue()) {
					charValue = result.getString("CHAR_VAL_FK1").trim();
				}
				if(charType.equals(intercompanyCharType) && !intercompanyCharHashMap.containsKey(divChar)) {
					intercompanyCharHashMap.put(divChar, charValue);
				} else if(charType.equals(counterPartyCharType) && !counterpartyCharHashMap.containsKey(divChar)) {
					counterpartyCharHashMap.put(divChar, charValue);
				}
			}
		}
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: fetchDivisionChars() method :: END");
	}

	/**
	 * Fetch Price Item characteristics
	 * 1. Override Distribution Code Char Value
	 * 2. Scheme Char Value
	 * @param priceItem
	 * @param overrideDistributionCodeCharType
	 * @param schemeCharType
	 */
	private void fetchPriceItemChars(CharacteristicType overrideDistributionCodeCharType, CharacteristicType schemeCharType) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchPriceItemChars() method :: START");
		String charValue = null;
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT PRICEITEM_CD, CHAR_TYPE_CD, CHAR_VAL, CHAR_VAL_FK1, ADHOC_CHAR_VAL ");
		sb.append(" FROM CI_PRICEITEM_CHAR ");
		sb.append(" WHERE CHAR_TYPE_CD IN (:overrideDistributionCodeCharType, :schemeCharType) ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("overrideDistributionCodeCharType", overrideDistributionCodeCharType.getId());
		ps.bindId("schemeCharType", schemeCharType.getId());
		ps.setAutoclose(false);

		List<SQLResultRow> resultList = ps.list();
		if(notNull(resultList) && resultList.size() > 0) {
			for(SQLResultRow result:resultList) {
				String strPriceitemCd = result.getString("PRICEITEM_CD").trim();
				String strCharType = result.getString("CHAR_TYPE_CD").trim();
				CharacteristicType_Id charTypeId = new CharacteristicType_Id(strCharType);
				String priceitemChar = strPriceitemCd.concat(strCharType);
				
				CharacteristicType charType = charTypeId.getEntity();
				CharacteristicTypeLookup charTypeLookUp = charType.getCharacteristicType();
				if(charTypeLookUp.isAdhocValue() || charTypeLookUp.isFileLocationValue()) {
					charValue = result.getString("ADHOC_CHAR_VAL").trim();
				} else if(charTypeLookUp.isPredefinedValue()) {
					charValue = result.getString("CHAR_VAL").trim();
				} else if(charTypeLookUp.isForeignKeyValue()) {
					charValue = result.getString("CHAR_VAL_FK1").trim();
				}
				if(charType.equals(overrideDistributionCodeCharType) && !overrideDistCharHashMap.containsKey(priceitemChar)) {
					overrideDistCharHashMap.put(priceitemChar, charValue);
				} else if(charType.equals(schemeCharType) && !schemeCharHashMap.containsKey(priceitemChar)) {
					schemeCharHashMap.put(priceitemChar, charValue);
				}
			}
		}
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: fetchPriceItemChars() method :: END");
	}

	/**
	 * Fetch Person's Business Unit Char Value
	 * @param person
	 * @param businessUnitCharType
	 * @return businessUnitCharVal
	 */
	private String fetchBusinessUnitChar(Person person, CharacteristicType businessUnitCharType) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchBusinessUnitChar() method :: START");
		String businessUnitCharVal = " ";
		CharacteristicType charType = businessUnitCharType;
		PersonCharacteristic personChar = person.getEffectiveCharacteristic(charType);
		if(notNull(personChar)){
			CharacteristicTypeLookup charTypeLookUp = charType.getCharacteristicType();
			if(charTypeLookUp.isAdhocValue() || charTypeLookUp.isFileLocationValue()) {
				businessUnitCharVal = personChar.getAdhocCharacteristicValue().trim();
			}else if(charTypeLookUp.isPredefinedValue()) {
				businessUnitCharVal = personChar.getCharacteristicValue().trim();
			}else if(charTypeLookUp.isForeignKeyValue()){
				businessUnitCharVal = personChar.getCharacteristicValueForeignKey1().trim();
			}
		}
		logger.debug("CmGLAccountConstruction_Impl :: fetchBusinessUnitChar() method :: END");
		return businessUnitCharVal;
	}
	
	/**
	 * Fetch Override distribution char value from priceitem (Fetch priceitem from CI_BSEG_EXT using Bill Segment ID)
	 * @param overrideDistributionCodeCharType
	 * @return overriderDistCharVal
	 */
	private String fetchOverrideDistCharFromBillSeg(CharacteristicType overrideDistributionCodeCharType) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchOverrideDistCharFromBillSeg() method :: START");
		String overriderDistCharVal = " ";
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT PRICEITEM_CD FROM CI_BSEG_EXT ");
		sb.append(" WHERE BSEG_ID =:bsegId ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("bsegId", bsegId);
		ps.setAutoclose(false);

		SQLResultRow result = ps.firstRow();
		if(notNull(result)) {
			String strPriceitemCd = result.getString("PRICEITEM_CD").trim();
			String overriderDistCharKey = strPriceitemCd.concat(overrideDistributionCodeCharType.getId().getIdValue().trim());
			overriderDistCharVal = overrideDistCharHashMap.get(overriderDistCharKey);
		}
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: fetchOverrideDistCharFromBillSeg() method :: END");
		return overriderDistCharVal;
	}
	
	/**
	 * Fetch UDF_CHAR_13 from CM_TXN_ATTRIBUTES_MAP (for scheme)
	 * @param billChgId
	 * @return udf_char_13
	 */
	private String fetchSchemeFromTxnAttributesMap(BillableCharge_Id billChgId) {
		logger.debug("CmGLAccountConstruction_Impl :: fetchSchemeFromTxnAttributesMap() method :: START");
		
		String udfChar13 = null;
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" SELECT CHILD_PRODUCT FROM CM_TXN_ATTRIBUTES_MAP ");
		sb.append(" WHERE BILLABLE_CHG_ID =:billChgId ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("billChgId", billChgId);
		ps.setAutoclose(false);

		SQLResultRow result = ps.firstRow();
		if(notNull(result)) {	
			udfChar13 = result.getString("CHILD_PRODUCT");
			if(!isBlankOrNull(udfChar13)) {
				udfChar13 = udfChar13.trim();
			}
		}
		
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		
		logger.debug("CmGLAccountConstruction_Impl :: fetchSchemeFromTxnAttributesMap() method :: END");
		return udfChar13;
	}

	/************************************************* Insert Data Methods *************************************************/

	/**
	 * Insert Data into CM_FT_GL_ASL_STG
	 * @param counterParty
	 * @param businessUnit
	 * @param costCentre
	 * @param intercompany
	 * @param scheme
	 * @param acctNbr 
	 * @param acctNbr 
	 */
	private void insertIntoFtGlAslStaging(String counterParty, String businessUnit, String costCentre, String intercompany, String scheme, String acctNbr) {
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslStaging() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" INSERT INTO CM_FT_GL_ASL_STG ");
		sb.append(" (FT_ID, GL_SEQ_NBR, GL_ACCT, AMOUNT, ACCOUNTING_DT, CURRENCY_CD, COUNTERPARTY, ");
		sb.append(" BUSINESS_UNIT, COST_CENTRE, INTERCOMPANY, SCHEME, FT_TYPE_FLG, ACCT_NBR) ");
		sb.append(" VALUES ");
		sb.append(" (:ftId, :glSeqNbr, :glAccount, :amount, :accountingDate, :currencyCd, :counterparty, ");
		sb.append(" :businessUnit, :costCentre, :intercompany, :scheme, :ftTypeFlg, :acctNbr) ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("ftId", ftId);
		ps.bindBigInteger("glSeqNbr", glSequenceNumber);
		ps.bindString("glAccount", glAccount, "GL_ACCT");
		ps.bindMoney("amount", ftGlAmount);
		ps.bindDate("accountingDate", ftAccountingDate);
		ps.bindId("currencyCd", ftCurrency);
		ps.bindString("counterparty", counterParty, "COUNTERPARTY");
		ps.bindString("businessUnit", businessUnit, "");
		ps.bindString("costCentre", costCentre, "COST_CENTRE");
		ps.bindString("intercompany", intercompany, "INTERCOMPANY");
		ps.bindString("scheme", scheme, "SCHEME");
		// NAP-35019: Payment to accounting reconciliation
		ps.bindLookup("ftTypeFlg", ftType);
//		if(ftType.equals(FTTYPE_PS) || ftType.equals(FTTYPE_PX))
//			ps.bindLookup("ftTypeFlg", FTTYPE_PS);
//		else
//			ps.bindLookup("ftTypeFlg", FTTYPE_BS);
		// NAP-31893 : Add ACCT_NBR extra column
		ps.bindString("acctNbr", acctNbr, "ACCT_NBR");
		ps.executeUpdate();

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslStaging() method :: END");
	}
	
	private String checkForPayType(String glAcct, Bill_Id billId, String negGlAccount, String payType, FinancialTransaction_Id ftId){
		logger.debug("CmGLAccountConstruction_Impl :: checkForPayType() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;
		String glAccount = glAcct;
		
		sb.append(" SELECT A.BILL_ID FROM CM_BILL_PAYMENT_DTL A , CI_FT B ");
		sb.append(" WHERE A.PAY_DTL_ID = B.SETTLEMENT_ID_NBR ");
		sb.append(" AND B.FT_ID = :ftId ");
		sb.append(" AND A.BILL_ID = :billId ");
		sb.append(" AND A.PAY_TYPE = :payType ");
		
		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("billId", billId);
		ps.bindId("ftId", ftId);
		ps.bindString("payType",payType, "PAY_TYPE");
		
		ps.setAutoclose(false);
		SQLResultRow result = ps.firstRow();
		
		if(notNull(result)){
			glAccount = negGlAccount;	
		}//end-if
		
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: checkForPayType() method :: END");
		return glAccount;
	}
	
	/**
	 * 
	 * @param glAccount2
	 * @param billId
	 * @param fundAccountNumber
	 * @param payType
	 * @param negGlAccount
	 * @return
	 */
	private String updateGlAccountForNegativeFT(String glAcct, Bill_Id billId, String fundAccountNumber, String payType, String negGlAccount, 
			BillSegment_Id bsegId){
		logger.debug("CmGLAccountConstruction_Impl :: updateGlAccountForNegativeFT() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;
		String glAccount = glAcct;
		
		sb.append(" SELECT D.BSEG_ID AS BSEG_ID FROM CM_PAY_REQ A, CM_BILL_SETT_MAP B, CM_TXN_ATTRIBUTES_MAP C, CI_BSEG_CALC D  ");
		sb.append(" WHERE A.BILL_ID = B.BILL_ID AND A.LINE_ID = B.LINE_ID AND B.SETT_LEVEL_GRANULARITY = C.SETT_LEVEL_GRANULARITY AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID ");
		sb.append(" AND A.PAY_TYPE = :payType ");
		sb.append(" AND A.ACCT_TYPE = :fund ");
		sb.append(" AND A.BILL_ID = :billId ");
		sb.append(" AND D.BSEG_ID = :bsegId ");
		
		ps = createPreparedStatement(sb.toString(),"");
		ps.bindString("payType", payType, "PAY_TYPE");
		ps.bindString("fund",fundAccountNumber, "ACCT_TYPE");
		ps.bindId("billId", billId);
		ps.bindId("bsegId", bsegId);
		
		ps.setAutoclose(false);
		SQLResultRow result = ps.firstRow();
		
		if(notNull(result)){
			glAccount = negGlAccount;	
		}//end-if
		
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: updateGlAccountForNegativeFT() method :: END");
		return glAccount;
	}

	/**
	 * Insert Data into CM_FT_GL_FX_STG
	 * @param businessUnit 
	 * @param counterParty 
	 * @param binSettleCurrency 
	 * @param fundCurrency 
	 */
	private void insertIntoFtGlFxStaging(String fundCurrency, String binSettleCurrency, String counterParty, String businessUnit) {
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlFxStaging() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" INSERT INTO CM_FT_GL_FX_STG ");
		sb.append(" (FT_ID, GL_SEQ_NBR, GL_ACCT, FUND_CURRENCY, BIN_SETTLE_CURRENCY, ACCOUNTING_DT, AMOUNT, COUNTERPARTY, BUSINESS_UNIT) ");
		sb.append(" VALUES ");
		sb.append(" (:ftId, :glSeqNbr, :glAccount, :fundCurrency, :binSettleCurrency, :accountingDate, :amount, :counterParty, :businessUnit) ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("ftId", ftId);
		ps.bindBigInteger("glSeqNbr", glSequenceNumber);
		ps.bindString("glAccount", glAccount, "GL_ACCT");
		ps.bindString("fundCurrency", fundCurrency, "FUND_CURRENCY");
		ps.bindString("binSettleCurrency", binSettleCurrency, "BIN_SETTLE_CURRENCY");
		ps.bindDate("accountingDate", ftAccountingDate);
		ps.bindMoney("amount", ftGlAmount);
		ps.bindString("counterParty", counterParty, "COUNTERPARTY");
		ps.bindString("businessUnit", businessUnit, "");
		ps.executeUpdate();

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlFxStaging() method :: END");
	}

	/**
	 * Insert Error Data into CM_FT_GL_ASL_ERR
	 * @param messageId
	 */
	private void insertIntoFtGlAslError(Message_Id messageId) {
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslError() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement ps = null;

		sb.append(" INSERT INTO CM_FT_GL_ASL_ERR ");
		sb.append(" (FT_ID, GL_ACCT, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, AMOUNT, CURRENCY_CD, GL_SEQ_NBR) ");
		sb.append(" VALUES ");
		sb.append(" (:ftId, :glAcct, :msgCatNbr, :msgNbr, :errorInfo, :amount, :currencyCd, :glSeqNbr) ");

		ps = createPreparedStatement(sb.toString(),"");
		ps.bindId("ftId", ftId);
		if(isBlankOrNull(glAccount))
			ps.bindString("glAcct", " ", "GL_ACCT");
		else 
			ps.bindString("glAcct", glAccount, "GL_ACCT");
		ps.bindId("msgCatNbr", messageId.getMessageCategoryId());
		ps.bindBigInteger("msgNbr", messageId.getMessageNumber());
		ps.bindString("errorInfo", messageId.getEntity().fetchLanguageEntity().getMessageText(), "ERROR_INFO");
		ps.bindMoney("amount", ftGlAmount);
		ps.bindId("currencyCd", ftCurrency);
		ps.bindBigInteger("glSeqNbr", glSequenceNumber);
		ps.executeUpdate();

		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslError() method :: END");
	}
	
	public String getGlAccount() {
		return glAccount;
	}

	public void setFinancialTransaction(FinancialTransaction arg0) {
		financialTransaction = arg0;
	}

	public void setGlDistribution(GeneralLedgerDistributionCode arg0) {
		glDistributionCode = arg0;
	}

	public void setGlSequenceNumber(BigInteger arg0) {
		glSequenceNumber = arg0;
	}

	public void setTrialFinancialTransaction(TrialFinancialTransaction arg0) {

	}

}
