package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

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
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.lookup.CharacteristicTypeLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.message.MessageCategory_Id;
import com.splwg.base.domain.common.message.Message_Id;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode_Id;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransactionGeneralLedger_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Lenovo
 *
 @BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (entityName = characteristicType, name = taxGLAccountCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = overrideDistributionCodeCharTy, required = true, type = entity)
 *            , @BatchJobSoftParameter (name = idType, required = true, type = string)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = counterPartyCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = intercompanyCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (name = intercompanyCharValue, required = true, type = string)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = businessUnitCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (entityName = characteristicType, name = schemeCharType, required = true, type = entity)
 *            , @BatchJobSoftParameter (entityName = accountNumberType, name = fundAccountNumberType, required = true, type = entity)
 *            , @BatchJobSoftParameter (name = fundAccountNumber, required = true, type = string)
 *            , @BatchJobSoftParameter (name = fundGLAccount, required = true, type = string)
 *            , @BatchJobSoftParameter (name = payType, required = true, type = string)
 *            , @BatchJobSoftParameter (name = negGlAccount, required = true, type = string)
 *            , @BatchJobSoftParameter (name = dstCdForDebtFund, required = true, type = string)
 *            , @BatchJobSoftParameter (name = taxDistributionCode, required = true, type = string)
 *            , @BatchJobSoftParameter (name = division, type = string)
 *            , @BatchJobSoftParameter (name = noOfRetries, type = integer)})
 */
public class CmAssignGlAccount extends CmAssignGlAccount_Gen {

	public static final Logger logger = LoggerFactory
			.getLogger(CmAssignGlAccount.class);

	public Class<CmAssignGlAccountWorker> getThreadWorkerClass() {
		return CmAssignGlAccountWorker.class;
	}

	public static class CmAssignGlAccountWorker extends
			CmAssignGlAccountWorker_Gen {

		FinancialTransactionTypeLookup ftType;
		FinancialTransaction_Id ftId;
		Currency_Id ftCurrency;
		Date ftAccountingDate;
		Money ftGlAmount;
		String ftDivision;
		GeneralLedgerDistributionCode_Id glDistributionCodeId;
		String intercompanyCharVal;
		String counterPartyCharval;
		String overrideDistributionCodeCharVal;
		String schemeCharVal;
		Bill_Id billId;
		FinancialTransaction financialTransaction;
		GeneralLedgerDistributionCode glDistributionCode;
		BigInteger glSequenceNumber;
		String glAccount;
		String ftParentId;

		String taxDistributionCode;
		BigInteger maxRetries;
		CharacteristicType taxGLAccountCharType;
		CharacteristicType overrideDistributionCodeCharType;
		String perIdNbr;
		CharacteristicType counterPartyCharType;
		CharacteristicType intercompanyCharType;
		String intercompanyCharValue;
		CharacteristicType businessUnitCharType;
		CharacteristicType schemeCharType;
		AccountNumberType fundAccountNumberType;
		String fundAccountNumber;
		String fundGLAccount;
		String negGlAccount;
		String payType;
		String dstCdForDebtFund;
		BatchControl_Id batchControlId;
		BigInteger batchNbr;

		private static HashSet<String> ftGlHashSet = new HashSet<>();
		private static HashMap<String, SQLResultRow> ftGlMapHashMap = new HashMap<>();
		private static HashMap<String, String> intercompanyCharHashMap = new HashMap<>();
		private static HashMap<String, String> counterpartyCharHashMap = new HashMap<>();
		private static HashMap<String, String> schemeCharHashMap = new HashMap<>();
		private static HashMap<String, String> overrideDistCharHashMap = new HashMap<>();
		private static HashMap<String, String> overrideAdjCharHashMap = new HashMap<>();
		private static final String NO = "N";
		private static final String DELIMITER = "~";
		private static final String NOT_APPLICABLE = "NA";
		private static final String NOT_SLASH_APPLICABLE = "N/A";
		private static final FinancialTransactionTypeLookup FTTYPE_BS = FinancialTransactionTypeLookup.constants.BILL_SEGMENT;
		private static final FinancialTransactionTypeLookup FTTYPE_BX = FinancialTransactionTypeLookup.constants.BILL_CANCELLATION;
		private static final FinancialTransactionTypeLookup FTTYPE_AD = FinancialTransactionTypeLookup.constants.ADJUSTMENT;
		private static final FinancialTransactionTypeLookup FTTYPE_AX = FinancialTransactionTypeLookup.constants.ADJUSTMENT_CANCELLATION;
		private static final FinancialTransactionTypeLookup FTTYPE_PS = FinancialTransactionTypeLookup.constants.PAY_SEGMENT;
		private static final FinancialTransactionTypeLookup FTTYPE_PX = FinancialTransactionTypeLookup.constants.PAY_CANCELLATION;
		private static final BillStatusLookup BILL_COMPLETE = BillStatusLookup.constants.COMPLETE;
		private static final BigInteger MESSAGE_CATEGORY = BigInteger
				.valueOf(92000);
		private static final BigInteger ACCT_DT_MISSING_MSG_NBR = BigInteger
				.valueOf(1401);
		private static final BigInteger SCHEME_MISSING_MSG_NBR = BigInteger
				.valueOf(1402);
		private static final BigInteger BUSINESS_UNIT_MISSING_MSG_NBR = BigInteger
				.valueOf(1403);
		private static final BigInteger MAX_RETRIES = BigInteger
				.valueOf(5);
		
		private static final String BATCH_CODE="CM_GLASN";

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */

		public ThreadExecutionStrategy createExecutionStrategy() {

			return new ThreadIterationStrategy(this);
		}

		@SuppressWarnings("rawtypes")
		protected QueryIterator getQueryIteratorForThread(StringId lowId,
				StringId highId) {

			PreparedStatement statement = null;
			BigInteger maxRetries=isNull(getParameters().getNoOfRetries())?MAX_RETRIES:getParameters().getNoOfRetries();

			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT B.FT_ID AS FT_ID, B.DST_ID AS DST_ID , B.GL_SEQ_NBR AS GL_SEQ_NBR FROM CI_FTTEMP A, CI_FT_GL B ");
			sb.append(" WHERE A.FT_ID = B.FT_ID AND B.GL_ACCT = ' ' AND B.FT_ID BETWEEN :lowId AND :highId ");
			if (!isBlankOrNull(getParameters().getDivision())) {
				sb.append(" AND ((:division <> ' ' AND CIS_DIVISION = :division) OR :division  = ' ') ");
			}
			
			sb.append(" AND NOT EXISTS(SELECT 1 FROM CM_FT_GL_ASL_ERR WHERE FT_ID=A.FT_ID GROUP BY FT_ID, GL_SEQ_NBR HAVING COUNT(*) >= :maxRetries) ");

			try {
				statement = createPreparedStatement(sb.toString(), "");
				statement.setAutoclose(false);
				statement.bindId("lowId", lowId);
				statement.bindId("highId", highId);
				statement.bindBigInteger("maxRetries", maxRetries);

			} finally {
				if (statement != null) {
					statement.close();
				}
			}

			return statement.iterate();
		}

		/**
		 * This method generates a unit of work.
		 */
		protected ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
			ThreadWorkUnit unit = new ThreadWorkUnit(row.getId("FT_ID",
					FinancialTransaction.class));
			unit.addSupplementalData("dstId", row.getString("DST_ID"));
			unit.addSupplementalData("gLSeqNbr", row.getInteger("GL_SEQ_NBR"));
			return unit;
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork() method for batch thread number: "
					+ getBatchThreadNumber());

			taxDistributionCode = getParameters().getTaxDistributionCode();
			taxGLAccountCharType = getParameters().getTaxGLAccountCharType();
			overrideDistributionCodeCharType = getParameters().getOverrideDistributionCodeCharTy();
			maxRetries=getParameters().getNoOfRetries();
			perIdNbr = getParameters().getIdType();
			counterPartyCharType = getParameters().getCounterPartyCharType();
			intercompanyCharType = getParameters().getIntercompanyCharType();
			intercompanyCharValue = getParameters().getIntercompanyCharValue();
			businessUnitCharType = getParameters().getBusinessUnitCharType();
			schemeCharType = getParameters().getSchemeCharType();
			fundAccountNumberType = getParameters().getFundAccountNumberType();
			fundAccountNumber = getParameters().getFundAccountNumber();
			fundGLAccount = getParameters().getFundGLAccount();
			negGlAccount = getParameters().getNegGlAccount();
			payType = getParameters().getPayType();
			dstCdForDebtFund = getParameters().getDstCdForDebtFund();
			batchControlId = new BatchControl_Id(BATCH_CODE);
			batchNbr = batchControlId.getEntity().getNextBatchNumber();
			//noOfRetries=getParameters().get

			if (ftGlHashSet.isEmpty()) {
				loadFtGlHashSet();
			}
			if (ftGlMapHashMap.isEmpty()) {
				loadFtGlMapHashMap();
			}

			if (overrideAdjCharHashMap.isEmpty()) {
				loadOverrideAdjCharHashMap();
			}

			// Fetch division characteristic values
			if (intercompanyCharHashMap.isEmpty()
					|| counterpartyCharHashMap.isEmpty()) {
				fetchDivisionChars(counterPartyCharType, intercompanyCharType);
			}

			if (overrideDistCharHashMap.isEmpty()
					|| schemeCharHashMap.isEmpty()) {
				fetchPriceItemChars(overrideDistributionCodeCharType,
						schemeCharType);
			}

			startResultRowQueryIteratorForThread(FinancialTransaction_Id.class);
		}

		/**
		 * Load FT GL TMP array which holds all data for the table CM_FT_GL_TMP
		 */
		private void loadFtGlHashSet() {
			logger.debug("CmGLAccountConstruction_Impl :: loadFtGlTmpArray() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT DST_ID FROM CM_FT_GL_TMP ");
			sb.append(" WHERE DIST_TYPE = 'REG' ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.setAutoclose(false);
			List<SQLResultRow> resultList = ps.list();

			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {
					String dstId = result.getString("DST_ID");
					if (!ftGlHashSet.contains(dstId))
						ftGlHashSet.add(dstId);
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: loadFtGlTmpArray() method :: END");
		}

		/**
		 * Load override AJ Map hash map which holds data for the table
		 * CI_ADJ_TY_CHAR
		 */
		private void loadOverrideAdjCharHashMap() {
			logger.debug("CmGLAccountConstruction_Impl :: loadOverrideAdjCharHashMap() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT ADJ_TYPE_CD, CHAR_TYPE_CD, ADHOC_CHAR_VAL, CHAR_VAL_FK1, CHAR_VAL_FK2 FROM CI_ADJ_TY_CHAR WHERE CHAR_TYPE_CD ='OV_DSTCD' ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.setAutoclose(false);
			List<SQLResultRow> resultList = ps.list();

			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {
					String adjTypeCharKey = result.getString("ADJ_TYPE_CD")
							.trim().concat(DELIMITER)
							.concat(result.getString("CHAR_VAL_FK1").trim())
							.concat(DELIMITER)
							.concat(result.getString("CHAR_VAL_FK2").trim());
					String adhocCharVal = result.getString("ADHOC_CHAR_VAL");
					if (!overrideAdjCharHashMap.containsKey(adjTypeCharKey)) {
						overrideAdjCharHashMap
								.put(adjTypeCharKey, adhocCharVal);
					}
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: loadOverrideAdjCharHashMap() method :: END");
		}

		/**
		 * Load FT GL TMP hash map which holds all data for the table
		 * CM_FT_GL_MAP
		 */
		private void loadFtGlMapHashMap() {
			logger.debug("CmGLAccountConstruction_Impl :: loadFtGlMapHashMap() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT DST_ID, INTERCOMPANY_FLG, SCHEME_FLG FROM CM_FT_GL_MAP ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.setAutoclose(false);
			List<SQLResultRow> resultList = ps.list();

			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {
					String dstId = result.getString("DST_ID");
					if (!ftGlMapHashMap.containsKey(dstId)) {
						ftGlMapHashMap.put(dstId, result);
					}
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: loadFtGlMapHashMap() method :: END");
		}

		/**
		 * Fetch division characteristics 1. Counter Party Char Value 2.
		 * Intercompany Char Value
		 * 
		 * @param counterPartyCharType
		 * @param intercompanyCharType
		 */
		private void fetchDivisionChars(
				CharacteristicType counterPartyCharType,
				CharacteristicType intercompanyCharType) {
			logger.debug("CmGLAccountConstruction_Impl :: fetchDivisionChars() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT CIS_DIVISION, CHAR_TYPE_CD, CHAR_VAL, CHAR_VAL_FK1, ADHOC_CHAR_VAL ");
			sb.append(" FROM CI_CIS_DIV_CHAR ");
			sb.append(" WHERE CHAR_TYPE_CD IN (:counterPartyCharType, :intercompanyCharType) ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("counterPartyCharType", counterPartyCharType.getId());
			ps.bindId("intercompanyCharType", intercompanyCharType.getId());
			ps.setAutoclose(false);

			List<SQLResultRow> resultList = ps.list();
			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {
					loadCharHasgMap(result);
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: fetchDivisionChars() method :: END");
		}

		/**
		 * @param result
		 */
		public void loadCharHasgMap(SQLResultRow result) {

			String charValue = null;
			String strCisDivId = result.getString("CIS_DIVISION").trim();
			String strCharType = result.getString("CHAR_TYPE_CD").trim();
			CharacteristicType_Id charTypeId = new CharacteristicType_Id(
					strCharType);
			String divChar = strCisDivId.concat(strCharType);

			CharacteristicType charType = charTypeId.getEntity();
			CharacteristicTypeLookup charTypeLookUp = charType
					.getCharacteristicType();
			if (charTypeLookUp.isAdhocValue()
					|| charTypeLookUp.isFileLocationValue()) {
				charValue = result.getString("ADHOC_CHAR_VAL").trim();
			} else if (charTypeLookUp.isPredefinedValue()) {
				charValue = result.getString("CHAR_VAL").trim();
			} else if (charTypeLookUp.isForeignKeyValue()) {
				charValue = result.getString("CHAR_VAL_FK1").trim();
			}
			if (charType.equals(intercompanyCharType)
					&& !intercompanyCharHashMap.containsKey(divChar)) {
				intercompanyCharHashMap.put(divChar, charValue);
			} else if (charType.equals(counterPartyCharType)
					&& !counterpartyCharHashMap.containsKey(divChar)) {
				counterpartyCharHashMap.put(divChar, charValue);
			}
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * extracting payments request details from ORMB. It validates the
		 * extracted data and populates the target tables accordingly.
		 */
		public WorkUnitResult executeWorkUnitDetailedResult(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			String intercompany = "";
			String costCentre = "";
			String businessUnit = "";
			String scheme = "";
			String schemeUdfChar13 = "";
			String fundCurrency = "";
			String binSettleCurrency = "";
			BillSegment_Id bsegId;
			
			ftId = (FinancialTransaction_Id) unit.getPrimaryId();
			String dstId = unit.getSupplementallData("dstId").toString();

			glSequenceNumber = new BigInteger(unit.getSupplementallData(
					"gLSeqNbr").toString());
			glDistributionCodeId = new GeneralLedgerDistributionCode_Id(dstId);
			financialTransaction = ftId.getEntity();

			ftType = financialTransaction.getDTO()
					.getFinancialTransactionType();
			bsegId = fetchBsegId(financialTransaction, ftType);
			ftId = financialTransaction.getId();
			ftCurrency = financialTransaction.getCurrency().getId();
			ftAccountingDate = financialTransaction.getAccountingDate();
			FinancialTransactionGeneralLedger_Id ftGlId = new FinancialTransactionGeneralLedger_Id(
					financialTransaction, glSequenceNumber);
			ftGlAmount = ftGlId.getEntity().getAmount();
			ftDivision = financialTransaction.getDivision().getId()
					.getIdValue().trim();
			billId = financialTransaction.getBillId();
			ftParentId = financialTransaction.getParentId();

			String divIntercompanyCharKey = ftDivision
					.concat(intercompanyCharType.getId().getIdValue().trim());
			String divCounterpartyCharKey = ftDivision
					.concat(counterPartyCharType.getId().getIdValue().trim());
			intercompanyCharVal = intercompanyCharHashMap
					.get(divIntercompanyCharKey);
			counterPartyCharval = counterpartyCharHashMap
					.get(divCounterpartyCharKey);

			ServiceAgreement servAggr = financialTransaction
					.getServiceAgreement();
			Account account = servAggr.getAccount();
			Person person = getAccountMainPerson(account);
			// NAP-31893 : Add ACCT_NBR extra column
			String acctNbr = getAccountNumber(account);
			BillableCharge billChg = getBillableCharge(bsegId);
			PriceItem priceItem = (billChg != null) ? billChg
					.getPriceItemCodeId().getEntity() : null;

			// Fetch child_product from CM_TXN_ATTRIBUTES_MAP (for scheme)
			schemeUdfChar13 = fetchSchemeFromTxnAttributesMap(billChg);

			// Fetch Price item characteristic values
			schemeCharVal = fetchSchemeCharVal(priceItem, schemeCharType,
					schemeUdfChar13);
			overrideDistributionCodeCharVal = fetchOverrideDistributionCodeCharVal(
					priceItem, overrideDistributionCodeCharType);
			// Set GL Account of the Financial Transaction
			boolean doesDistCdExist = ftGlHashSet.contains(glDistributionCodeId
					.getIdValue());

			// // NAP-31545
			if ((ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX))
					&& BILL_COMPLETE.equals(billId.getEntity().getBillStatus())) {
				if (taxDistributionCode.equalsIgnoreCase(glDistributionCodeId
						.getIdValue().trim())) {
					glAccount = fetchTaxGLAccountChar(bsegId,
							taxGLAccountCharType);
				} else if (doesDistCdExist) {
					// RIA: NAP- 30741
					glAccount = glDistributionCodeId.getIdValue().trim();

					glAccount = distCdExist(glAccount, billId,
							fundAccountNumber, payType, negGlAccount, bsegId,
							dstCdForDebtFund);
				} else {
				glAccount = distCdNotExist(doesDistCdExist,
						overrideDistributionCodeCharVal,
						overrideDistributionCodeCharType, bsegId);
				}
			} else if (ftType.equals(FTTYPE_AD) || ftType.equals(FTTYPE_AX)) {
				glAccount = getGlAccountADAX(financialTransaction,
						glDistributionCodeId, billId, ftDivision, ftCurrency,
						ftParentId);

			} else if (ftType.equals(FTTYPE_PS) || ftType.equals(FTTYPE_PX)) {
				glAccount = getGlAccountPSPX(fundAccountNumber,
						glDistributionCodeId, dstCdForDebtFund, billId,
						negGlAccount, payType, acctNbr, ftId);
			}

			SQLResultRow ftGlMapResult = ftGlMapHashMap.get(glAccount.trim());
			boolean doesGLAcctExist = false;
			String intercompanyFlag = "";
			String schemeFlag = "";
			doesGLAcctExist = fetchDoesGLAcctExist(ftGlMapResult);
			intercompanyFlag = fetchIntercompanyFlag(ftGlMapResult);
			schemeFlag = fetchSchemeFlag(ftGlMapResult);

			// Set cost centre
			costCentre = NOT_APPLICABLE;

			// Set counter party
			String counterParty = isBlankOrNull(counterPartyCharval) ? " "
					: counterPartyCharval;

			// Retrieve Intercompany
			intercompany = retrieveIntercompany(perIdNbr, counterParty,
					intercompanyCharVal, intercompanyCharValue,
					doesGLAcctExist, intercompanyFlag);

			// Retrieved Business Unit of Person
			businessUnit = fetchBusinessUnitChar(person, businessUnitCharType);

			// Retrieve Scheme
			scheme = getSchemeString(priceItem, doesGLAcctExist, schemeFlag,
					schemeCharVal, ftType);

			// Set Fund Currency
			fundCurrency = getFundCurrency(account, fundAccountNumberType,
					fundAccountNumber, fundGLAccount);

			// Set BIN Settle Currency
			binSettleCurrency = getBinSettleCurrency(fundCurrency, billChg);

			boolean isFTError = checkIfFtIsInError(ftAccountingDate, scheme,
					businessUnit);
			if (!isFTError && !isBlankOrNull(glAccount)) {
				insertIntoFtGlAslStaging(counterParty, businessUnit,
						costCentre, intercompany, scheme, acctNbr);
				updateGlAccount(ftId, glAccount);
				insertIntoFtGlFxStaging(fundCurrency, binSettleCurrency,
						counterParty, businessUnit);
			}

			logger.debug("CmGLAccountConstruction_Impl :: invoke() method :: END");

			WorkUnitResult results = new WorkUnitResult(true);
			results.setUnitsProcessed(1);
			return results;
		}

		/**
		 * @param financialTransaction
		 * @param ftType
		 * @return
		 */
		public BillSegment_Id fetchBsegId(
				FinancialTransaction financialTransaction,
				FinancialTransactionTypeLookup ftType) {
			BillSegment_Id bsegId = null;
			if (ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX)) {
				bsegId = financialTransaction.fetchSiblingBillSegment().getId();
			}
			return bsegId;
		}

		/**
		 * @param ftGlMapResult
		 * @return
		 */
		public String fetchIntercompanyFlag(SQLResultRow ftGlMapResult) {
			String intercompanyFlag = " ";

			if (notNull(ftGlMapResult)) {
				intercompanyFlag = ftGlMapResult.getString("INTERCOMPANY_FLG")
						.trim();
			}
			return intercompanyFlag;
		}

		/**
		 * @param ftGlMapResult
		 * @return
		 */
		public String fetchSchemeFlag(SQLResultRow ftGlMapResult) {
			String schemeFlag = " ";

			if (notNull(ftGlMapResult)) {
				schemeFlag = ftGlMapResult.getString("SCHEME_FLG").trim();
			}
			return schemeFlag;
		}

		/**
		 * @param ftGlMapResult
		 * @return
		 */
		public boolean fetchDoesGLAcctExist(SQLResultRow ftGlMapResult) {
			boolean doesGLAcctExist = false;

			if (notNull(ftGlMapResult)) {
				doesGLAcctExist = true;
			}
			return doesGLAcctExist;
		}

		/**
		 * @param priceItem
		 * @param schemeCharType
		 * @param schemeUdfChar13
		 * @return
		 */
		public String fetchSchemeCharVal(PriceItem priceItem,
				CharacteristicType schemeCharType, String schemeUdfChar13) {

			String schemeChrVal = " ";
			String piSchemeCharKey = null;

			if (priceItem != null) {
				String strPriceitem = priceItem.getId().getIdValue().trim();
				piSchemeCharKey = strPriceitem.concat(schemeCharType.getId()
						.getIdValue().trim());
				if (isBlankOrNull(schemeUdfChar13))
					schemeChrVal = schemeCharHashMap.get(piSchemeCharKey);
				else {
					schemeChrVal = schemeCharHashMap
							.get(schemeUdfChar13.concat(schemeCharType.getId()
									.getIdValue().trim()));
				}
			}
			return schemeChrVal;
		}

		/**
		 * @param priceItem
		 * @param overrideDistributionCodeCharType
		 * @return
		 */
		public String fetchOverrideDistributionCodeCharVal(PriceItem priceItem,
				CharacteristicType overrideDistributionCodeCharType) {

			String piOverrideDistCharKey = null;
			String overrideDistributionCodeCharVal1 = null;

			if (priceItem != null) {
				String strPriceitem = priceItem.getId().getIdValue().trim();

				piOverrideDistCharKey = strPriceitem
						.concat(overrideDistributionCodeCharType.getId()
								.getIdValue().trim());
				overrideDistributionCodeCharVal1 = overrideDistCharHashMap
						.get(piOverrideDistCharKey);

			}
			return overrideDistributionCodeCharVal1;
		}

		/**
		 * @param glAcct
		 * @param billId
		 * @param fundAccountNumber
		 * @param payType
		 * @param negGlAccount
		 * @param bsegId
		 * @param dstCdForDebtFund
		 * @return
		 */
		public String distCdExist(String glAcct, Bill_Id billId,
				String fundAccountNumber, String payType, String negGlAccount,
				BillSegment_Id bsegId, String dstCdForDebtFund) {

			if (glAcct.equals(dstCdForDebtFund)) {
				glAcct = updateGlAccountForNegativeFT(glAcct, billId,
						fundAccountNumber, payType, negGlAccount, bsegId);
			}
			return glAcct;
		}

		/**
		 * @param doesDistCdExist
		 * @param overrideDistributionCodeCharVal
		 * @param overrideDistributionCodeCharType
		 * @return
		 */
		public String distCdNotExist(boolean doesDistCdExist,
				String overrideDistributionCodeCharVal,
				CharacteristicType overrideDistributionCodeCharType, BillSegment_Id bsegId) {
			String glAccountDistCdN = " ";

			if (!doesDistCdExist) {
				glAccountDistCdN = isBlankOrNull(overrideDistributionCodeCharVal) ? " "
						: overrideDistributionCodeCharVal;
			}
			if (isBlankOrNull(glAccountDistCdN)) {
				glAccountDistCdN = fetchOverrideDistCharFromBillSeg(overrideDistributionCodeCharType, bsegId);
			}

			return glAccountDistCdN;
		}

		/**
		 * @param fundAccountNumber
		 * @param glDistributionCodeId
		 * @param dstCdForDebtFund
		 * @param billId
		 * @param negGlAccount
		 * @param payType
		 * @param acctNbr
		 * @param ftId
		 * @return
		 */
		public String getGlAccountPSPX(String fundAccountNumber,
				GeneralLedgerDistributionCode_Id glDistributionCodeId,
				String dstCdForDebtFund, Bill_Id billId, String negGlAccount,
				String payType, String acctNbr, FinancialTransaction_Id ftId) {
			String glAccountPSPX = glDistributionCodeId.getIdValue().trim();

			if (acctNbr.equals(fundAccountNumber)
					&& glDistributionCodeId.getIdValue().trim()
							.equals(dstCdForDebtFund)) {
				glAccountPSPX = checkForPayType(glDistributionCodeId
						.getIdValue().trim(), billId, negGlAccount, payType,
						ftId);
			} else {
				glAccountPSPX = glDistributionCodeId.getIdValue().trim();
			}
			return glAccountPSPX;
		}

		/**
		 * @param financialTransaction
		 * @param glDistributionCodeId
		 * @param billId
		 * @param ftDivision
		 * @param ftCurrency
		 * @param ftParentId
		 * @return
		 */
		public String getGlAccountADAX(
				FinancialTransaction financialTransaction,
				GeneralLedgerDistributionCode_Id glDistributionCodeId,
				Bill_Id billId, String ftDivision, Currency_Id ftCurrency,
				String ftParentId) {
			String glAccountADAX = glDistributionCodeId.getIdValue().trim();

			if ((financialTransaction.getShouldShowOnBill().isTrue() && (notNull(billId) && !isBlankOrNull(billId
					.getIdValue())))
					|| (financialTransaction.getShouldShowOnBill().isFalse())) {
				String key = glDistributionCodeId.getIdValue().trim()
						.concat(DELIMITER).concat(ftDivision.trim())
						.concat(DELIMITER).concat(ftCurrency.getTrimmedValue());
				if (glDistributionCodeId.getIdValue().trim()
						.equals(ftParentId.trim())
						&& (overrideAdjCharHashMap.containsKey(key))) {
					// Fetch from map
					glAccountADAX = overrideAdjCharHashMap.get(key).trim();
				} else {
					glAccountADAX = glDistributionCodeId.getIdValue().trim();
				}
			}
			return glAccountADAX;
		}

		/**
		 * @param perIdNbr
		 * @param counterParty
		 * @param intercompanyCharVal
		 * @param intercompanyCharValue
		 * @param doesGLAcctExist
		 * @param intercompanyFlag
		 * @return
		 */
		public String retrieveIntercompany(String perIdNbr,
				String counterParty, String intercompanyCharVal,
				String intercompanyCharValue, boolean doesGLAcctExist,
				String intercompanyFlag) {

			String intercompany = " ";
			if (!perIdNbr.equalsIgnoreCase(counterParty)
					|| !intercompanyCharValue
							.equalsIgnoreCase(intercompanyCharVal)) {
				intercompany = NOT_APPLICABLE;
			} else if (perIdNbr.equalsIgnoreCase(counterParty)
					&& intercompanyCharValue
							.equalsIgnoreCase(intercompanyCharVal)) {
				if (doesGLAcctExist && NO.equalsIgnoreCase(intercompanyFlag)) {
					intercompany = NOT_APPLICABLE;
				} else {
					intercompany = counterParty;
				}
			}
			return intercompany;
		}

		/**
		 * Set Scheme String
		 * 
		 * @param priceItem
		 * @param doesGLAcctExist
		 * @param schemeFlag
		 * @param ftType
		 * @return
		 */
		public String getSchemeString(PriceItem priceItem,
				boolean doesGLAcctExist, String schemeFlag,
				String schemeCharVal, FinancialTransactionTypeLookup ftType) {
			logger.debug("CmGLAccountConstruction_Impl :: getSchemeString() method :: START");
			String scheme = "";

			if (notNull(priceItem)) {
				if (doesGLAcctExist && NO.equalsIgnoreCase(schemeFlag)) {
					scheme = NOT_APPLICABLE;
				} else {
					scheme = isBlankOrNull(schemeCharVal) ? " " : schemeCharVal;
				}
			} else if (ftType.equals(FTTYPE_BS) || ftType.equals(FTTYPE_BX)) {
				if (doesGLAcctExist && NO.equalsIgnoreCase(schemeFlag)) {
					scheme = NOT_APPLICABLE;
				} else {
					scheme = NOT_SLASH_APPLICABLE;
				}
			} else {
				scheme = NOT_APPLICABLE;
			}
			return scheme;
		}

		/**
		 * Get Accounting Error Description
		 * 
		 * @param messageNumber
		 * @return
		 */
		public static String getAccountingErrorDescription(String messageNumber) {
			String errorInfo = "";
			errorInfo = CustomMessageRepository.getPayReqErrorMessage(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		/**
		 * Fetch Price Item characteristics 1. Override Distribution Code Char
		 * Value 2. Scheme Char Value
		 * 
		 * @param priceItem
		 * @param overrideDistributionCodeCharType
		 * @param schemeCharType
		 */
		private void fetchPriceItemChars(
				CharacteristicType overrideDistributionCodeCharType,
				CharacteristicType schemeCharType) {
			logger.debug("CmGLAccountConstruction_Impl :: fetchPriceItemChars() method :: START");

			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT PRICEITEM_CD, CHAR_TYPE_CD, CHAR_VAL, CHAR_VAL_FK1, ADHOC_CHAR_VAL ");
			sb.append(" FROM CI_PRICEITEM_CHAR ");
			sb.append(" WHERE CHAR_TYPE_CD IN (:overrideDistributionCodeCharType, :schemeCharType) ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("overrideDistributionCodeCharType",
					overrideDistributionCodeCharType.getId());
			ps.bindId("schemeCharType", schemeCharType.getId());
			ps.setAutoclose(false);

			List<SQLResultRow> resultList = ps.list();
			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {
					loadDistHashMap(result);
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: fetchPriceItemChars() method :: END");
		}

		public void loadDistHashMap(SQLResultRow result) {

			String charValue = null;
			String strPriceitemCd = result.getString("PRICEITEM_CD").trim();
			String strCharType = result.getString("CHAR_TYPE_CD").trim();
			CharacteristicType_Id charTypeId = new CharacteristicType_Id(
					strCharType);
			String priceitemChar = strPriceitemCd.concat(strCharType);

			CharacteristicType charType = charTypeId.getEntity();
			CharacteristicTypeLookup charTypeLookUp = charType
					.getCharacteristicType();
			if (charTypeLookUp.isAdhocValue()
					|| charTypeLookUp.isFileLocationValue()) {
				charValue = result.getString("ADHOC_CHAR_VAL").trim();
			} else if (charTypeLookUp.isPredefinedValue()) {
				charValue = result.getString("CHAR_VAL").trim();
			} else if (charTypeLookUp.isForeignKeyValue()) {
				charValue = result.getString("CHAR_VAL_FK1").trim();
			}
			if (charType.equals(overrideDistributionCodeCharType)
					&& !overrideDistCharHashMap.containsKey(priceitemChar)) {
				overrideDistCharHashMap.put(priceitemChar, charValue);
			} else if (charType.equals(schemeCharType)
					&& !schemeCharHashMap.containsKey(priceitemChar)) {
				schemeCharHashMap.put(priceitemChar, charValue);
			}
		}

		/**
		 * Get Main customer of the account
		 * 
		 * @param account
		 * @return mainPerson
		 */
		private Person getAccountMainPerson(Account account) {
			logger.debug("CmGLAccountConstruction_Impl :: getAccountMainPerson() method :: START");
			Bool isMainCust;
			Person per = null;
			Iterator<AccountPerson> acctPerIterator = account.getPersons()
					.iterator();
			while (acctPerIterator.hasNext()) {
				AccountPerson acctper = acctPerIterator.next();
				isMainCust = acctper.getIsMainCustomer();
				if (isMainCust.isTrue()) {
					per = acctper.fetchIdPerson();
					break;
				}
			}
			logger.debug("CmGLAccountConstruction_Impl :: getAccountMainPerson() method :: END");
			return per;
		}

		/**
		 * Get account number for ACCT_NBR_TYPE_CD = 'ACCTTYPE'
		 * 
		 * @param account
		 * @return acctNbr
		 */
		private String getAccountNumber(Account account) {
			logger.debug("CmGLAccountConstruction_Impl :: getAccountNumber() method :: START");
			String acctNbr = null;
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT ACCT_NBR FROM CI_ACCT_NBR WHERE ACCT_ID =:acctId AND ACCT_NBR_TYPE_CD ='ACCTTYPE' ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("acctId", account.getId());
			ps.setAutoclose(false);
			SQLResultRow result = ps.firstRow();

			if (notNull(result)) {
				acctNbr = result.getString("ACCT_NBR").trim();
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: getAccountNumber() method :: END");
			return acctNbr;
		}

		/**
		 * Get Billable Charge entity corresponding to Bill Segment
		 * 
		 * @param bsegId
		 * @return BillableCharge
		 */
		private BillableCharge getBillableCharge(BillSegment_Id bsegId) {
			logger.debug("CmGLAccountConstruction_Impl :: getBillableCharge() method :: START");
			BillableCharge bchgEntity = null;

			if (notNull(bsegId)) {
				StringBuilder getBchgHQL = new StringBuilder();
				getBchgHQL
						.append(" FROM BillSegmentCalculationHeader bscl, BillableCharge bchg ");
				getBchgHQL.append(" WHERE bscl.billableChargeId = bchg.id ");
				getBchgHQL.append(" AND bscl.id.billSegment.id =:bsegId ");

				Query<BillableCharge_Id> query = createQuery(
						getBchgHQL.toString(), "");
				query.bindId("bsegId", bsegId);
				query.addResult("billChgId", "bchg.id");

				BillableCharge_Id result = query.firstRow();
				if (notNull(result)) {
					bchgEntity = result.getEntity();
				}
				getBchgHQL.setLength(0);
			}
			logger.debug("CmGLAccountConstruction_Impl :: getBillableCharge() method :: END");
			return bchgEntity;
		}

		/**
		 * Fetch UDF_CHAR_13 from CM_TXN_ATTRIBUTES_MAP (for scheme)
		 * 
		 * @param billChgId
		 * @return udf_char_13
		 */
		private String fetchSchemeFromTxnAttributesMap(BillableCharge billChg) {
			logger.debug("CmGLAccountConstruction_Impl :: fetchSchemeFromTxnAttributesMap() method :: START");

			String childProduct = null;
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			if (billChg != null) {
				sb.append(" SELECT CHILD_PRODUCT FROM CM_TXN_ATTRIBUTES_MAP ");
				sb.append(" WHERE BILLABLE_CHG_ID =:billChgId ");

				ps = createPreparedStatement(sb.toString(), "");
				ps.bindId("billChgId", billChg.getId());
				ps.setAutoclose(false);

				SQLResultRow result = ps.firstRow();
				if (notNull(result)) {
					childProduct = result.getString("CHILD_PRODUCT");
					if (!isBlankOrNull(childProduct)) {
						childProduct = childProduct.trim();
					}
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: fetchSchemeFromTxnAttributesMap() method :: END");
			return childProduct;
		}

		/**
		 * Fetch Tax GL Account Bill Segment Calculation Line Char
		 * 
		 * @param bsegId
		 * @param taxGLAccountCharType
		 * @return taxGLAcctBillSegCalcLineCharVal
		 */
		private String fetchTaxGLAccountChar(BillSegment_Id bsegId,
				CharacteristicType taxGLAccountCharType) {
			logger.debug("CmGLAccountConstruction_Impl :: fetchTaxGLAccountChar() method :: START");
			String taxGLAcctBillSegCalcLineCharVal = " ";
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT CHAR_VAL FROM CI_BSEG_CL_CHAR ");
			sb.append(" WHERE BSEG_ID =:bsegId ");
			sb.append(" AND CHAR_TYPE_CD =:charTypeCd ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("bsegId", bsegId);
			ps.bindId("charTypeCd", taxGLAccountCharType.getId());
			ps.setAutoclose(false);

			SQLResultRow result = ps.firstRow();
			if (notNull(result)) {
				String strTaxGLAcctBillSegCalcLineCharVal = result
						.getString("CHAR_VAL");
				if (!isBlankOrNull(strTaxGLAcctBillSegCalcLineCharVal)) {
					taxGLAcctBillSegCalcLineCharVal = strTaxGLAcctBillSegCalcLineCharVal
							.trim();
				}
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: fetchTaxGLAccountChar() method :: END");
			return taxGLAcctBillSegCalcLineCharVal;
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
		private String updateGlAccountForNegativeFT(String glAcct,
				Bill_Id billId, String fundAccountNumber, String payType,
				String negGlAccount, BillSegment_Id bsegId) {
			logger.debug("CmGLAccountConstruction_Impl :: updateGlAccountForNegativeFT() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;
			String glAccount2 = glAcct;

			sb.append(" SELECT D.BSEG_ID AS BSEG_ID FROM CM_PAY_REQ A, CM_BILL_SETT_MAP B, CM_TXN_ATTRIBUTES_MAP C, CI_BSEG_CALC D  ");
			sb.append(" WHERE A.BILL_ID = B.BILL_ID AND A.LINE_ID = B.LINE_ID AND B.SETT_LEVEL_GRANULARITY = C.SETT_LEVEL_GRANULARITY AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID ");
			sb.append(" AND A.PAY_TYPE = :payType ");
			sb.append(" AND A.ACCT_TYPE = :fund ");
			sb.append(" AND A.BILL_ID = :billId ");
			sb.append(" AND D.BSEG_ID = :bsegId ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindString("payType", payType, "PAY_TYPE");
			ps.bindString("fund", fundAccountNumber, "ACCT_TYPE");
			ps.bindId("billId", billId);
			ps.bindId("bsegId", bsegId);

			ps.setAutoclose(false);
			SQLResultRow result = ps.firstRow();

			if (notNull(result)) {
				glAccount2 = negGlAccount;
			}// end-if
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: updateGlAccountForNegativeFT() method :: END");
			return glAccount2;
		}

		/**
		 * Fetch Override distribution char value from priceitem (Fetch
		 * priceitem from CI_BSEG_EXT using Bill Segment ID)
		 * 
		 * @param overrideDistributionCodeCharType
		 * @return overriderDistCharVal
		 */
		private String fetchOverrideDistCharFromBillSeg(
				CharacteristicType overrideDistributionCodeCharType, BillSegment_Id bsegId) {
			
			logger.debug("CmGLAccountConstruction_Impl :: fetchOverrideDistCharFromBillSeg() method :: START");
			String overriderDistCharVal = " ";
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" SELECT PRICEITEM_CD FROM CI_BSEG_EXT ");
			sb.append(" WHERE BSEG_ID =:bsegId ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("bsegId", bsegId);
			ps.setAutoclose(false);

			SQLResultRow result = ps.firstRow();
			if (notNull(result)) {
				String strPriceitemCd = result.getString("PRICEITEM_CD");
				if (notNull(strPriceitemCd)) {
					strPriceitemCd = strPriceitemCd.trim();
					String overriderDistCharKey = strPriceitemCd
							.concat(overrideDistributionCodeCharType.getId()
									.getIdValue().trim());
					overriderDistCharVal = overrideDistCharHashMap
							.get(overriderDistCharKey);
				}

			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: fetchOverrideDistCharFromBillSeg() method :: END");
			return overriderDistCharVal;
		}

		/*
		 * 
		 */
		private String checkForPayType(String glAcct, Bill_Id billId,
				String negGlAccount, String payType,
				FinancialTransaction_Id ftId) {
			logger.debug("CmGLAccountConstruction_Impl :: checkForPayType() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;
			String glAccount1 = glAcct;

			sb.append(" SELECT A.BILL_ID FROM CM_BILL_PAYMENT_DTL A , CI_FT B ");
			sb.append(" WHERE A.PAY_DTL_ID = B.SETTLEMENT_ID_NBR ");
			sb.append(" AND B.FT_ID = :ftId ");
			sb.append(" AND A.BILL_ID = :billId ");
			sb.append(" AND A.PAY_TYPE = :payType ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("billId", billId);
			ps.bindId("ftId", ftId);
			ps.bindString("payType", payType, "PAY_TYPE");

			ps.setAutoclose(false);
			SQLResultRow result = ps.firstRow();

			if (notNull(result)) {
				glAccount1 = negGlAccount;
			}// end-if
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: checkForPayType() method :: END");
			return glAccount1;
		}

		/**
		 * Fetch Person's Business Unit Char Value
		 * 
		 * @param person
		 * @param businessUnitCharType
		 * @return businessUnitCharVal
		 */
		private String fetchBusinessUnitChar(Person person,
				CharacteristicType businessUnitCharType) {
			logger.debug("CmGLAccountConstruction_Impl :: fetchBusinessUnitChar() method :: START");
			String businessUnitCharVal = " ";
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			if (person != null) {
				sb.append(" SELECT ADHOC_CHAR_VAL FROM CI_PER_CHAR WHERE PER_ID = :perId ");
				sb.append(" AND CHAR_TYPE_CD = :charTypeCd ");

				ps = createPreparedStatement(sb.toString(), "");
				ps.bindId("perId", person.getId());
				ps.bindId("charTypeCd", businessUnitCharType.getId());

				ps.setAutoclose(false);
				SQLResultRow result = ps.firstRow();

				if (result != null) {
					businessUnitCharVal = result.getString("ADHOC_CHAR_VAL");
					if (notNull(businessUnitCharVal)) {
						businessUnitCharVal = businessUnitCharVal.trim();
					}
				}
				closePreparedStatement(ps);
			}
			logger.debug("CmGLAccountConstruction_Impl :: fetchBusinessUnitChar() method :: END");
			return businessUnitCharVal;
		}

		/**
		 * Get Fund currency If GL Account = Fund GL Account (Algorithm
		 * Parameter) and Account Number Type = Fund Account Number Type
		 * (Algorithm Parameter) and Account Number = Fund Account Number
		 * (Algorithm Parameter) then Fund currency = Account Currency else Fund
		 * currency = BLANK
		 * 
		 * @param account
		 * @param fundAccountNumberType
		 * @param fundAccountNumber
		 * @param fundGLAccount
		 * @return fundCurrency
		 */
		private String getFundCurrency(Account account,
				AccountNumberType fundAccountNumberType,
				String fundAccountNumber, String fundGLAccount) {
			logger.debug("CmGLAccountConstruction_Impl :: getFundCurrency() method :: START");
			String fundCurrency = " ";
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;
			if (fundGLAccount.equalsIgnoreCase(glAccount)) {

				sb.append(" SELECT B.CURRENCY_CD AS CURRENCY_CD FROM CI_ACCT_NBR A, CI_ACCT B ");
				sb.append(" WHERE A.ACCT_ID = B.ACCT_ID ");
				sb.append(" AND A.ACCT_NBR_TYPE_CD = :acctType ");
				sb.append(" AND A.ACCT_NBR = :fund ");
				sb.append(" AND A.ACCT_ID = :acctId ");

				ps = createPreparedStatement(sb.toString(), "");
				ps.bindString("acctType", fundAccountNumberType.getId()
						.getTrimmedValue(), "ACCT_NBR_TYPE_CD");
				ps.bindString("fund", fundAccountNumber, "ACCT_TYPE");
				ps.bindId("acctId", account.getId());

				ps.setAutoclose(false);
				SQLResultRow result = ps.firstRow();

				if (notNull(result)) {
					fundCurrency = result.getString("CURRENCY_CD");
					if (notNull(fundCurrency)) {
						fundCurrency = fundCurrency.trim();
					}
				}
				closePreparedStatement(ps);
			}
			logger.debug("CmGLAccountConstruction_Impl :: getFundCurrency() method :: END");
			return fundCurrency;
		}

		/**
		 * Get BIN Settle Currency
		 * 
		 * @param fundCurrency
		 * @param billChg
		 * @return binSettleCurrency
		 */
		private String getBinSettleCurrency(String fundCurrency,
				BillableCharge billChg) {
			logger.debug("CmGLAccountConstruction_Impl :: getBinSettleCurrency() method :: START");
			String binSettleCurrency = " ";
			if (!isBlankOrNull(fundCurrency) && billChg != null) {
				StringBuilder sb = new StringBuilder();
				PreparedStatement ps = null;

				sb.append(" SELECT UDF_CHAR_15 FROM CM_TXN_ATTRIBUTES_MAP ");
				sb.append(" WHERE BILLABLE_CHG_ID =:billChgId ");

				ps = createPreparedStatement(sb.toString(), "");
				ps.bindId("billChgId", billChg.getId());
				ps.setAutoclose(false);

				SQLResultRow result = ps.firstRow();
				if (notNull(result)) {
					binSettleCurrency = result.getString("UDF_CHAR_15");
					if (isBlankOrNull(binSettleCurrency)
							|| fundCurrency.equalsIgnoreCase(binSettleCurrency)) {
						binSettleCurrency = " ";
					}
				}
				closePreparedStatement(ps);
			}
			logger.debug("CmGLAccountConstruction_Impl :: getBinSettleCurrency() method :: END");
			return binSettleCurrency;
		}

		/**
		 * Insert Data into CM_FT_GL_ASL_STG
		 * 
		 * @param counterParty
		 * @param businessUnit
		 * @param costCentre
		 * @param intercompany
		 * @param scheme
		 * @param acctNbr
		 * @param acctNbr
		 */
		private void insertIntoFtGlAslStaging(String counterParty,
				String businessUnit, String costCentre, String intercompany,
				String scheme, String acctNbr) {
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslStaging() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" INSERT INTO CM_FT_GL_ASL_STG ");
			sb.append(" (FT_ID, GL_SEQ_NBR, GL_ACCT, AMOUNT, ACCOUNTING_DT, CURRENCY_CD, COUNTERPARTY, ");
			sb.append(" BUSINESS_UNIT, COST_CENTRE, INTERCOMPANY, SCHEME, FT_TYPE_FLG, ACCT_NBR) VALUES ");
			sb.append(" (:ftId, :glSeqNbr, :glAccount, :amount, :accountingDate, :currencyCd, :counterparty, ");
			sb.append(" :businessUnit, :costCentre, :intercompany, :scheme, :ftTypeFlg, :acctNbr) ");

			ps = createPreparedStatement(sb.toString(), "");
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
			// NAP-31893 : Add ACCT_NBR extra column
			ps.bindString("acctNbr", acctNbr, "ACCT_NBR");
			ps.executeUpdate();

			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslStaging() method :: END");
		}

		/**
		 * Insert Data into CM_FT_GL_FX_STG
		 * 
		 * @param businessUnit
		 * @param counterParty
		 * @param binSettleCurrency
		 * @param fundCurrency
		 */
		private void insertIntoFtGlFxStaging(String fundCurrency,
				String binSettleCurrency, String counterParty,
				String businessUnit) {
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlFxStaging() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			if (!isBlankOrNull(binSettleCurrency)) {
				sb.append(" INSERT INTO CM_FT_GL_FX_STG ");
				sb.append(" (FT_ID, GL_SEQ_NBR, GL_ACCT, FUND_CURRENCY, BIN_SETTLE_CURRENCY, ACCOUNTING_DT, AMOUNT, COUNTERPARTY, BUSINESS_UNIT) ");
				sb.append(" VALUES ");
				sb.append(" (:ftId, :glSeqNbr, :glAccount, :fundCurrency, :binSettleCurrency, :accountingDate, :amount, :counterParty, :businessUnit) ");

				ps = createPreparedStatement(sb.toString(), "");
				ps.bindId("ftId", ftId);
				ps.bindBigInteger("glSeqNbr", glSequenceNumber);
				ps.bindString("glAccount", glAccount, "GL_ACCT");
				ps.bindString("fundCurrency", fundCurrency, "FUND_CURRENCY");
				ps.bindString("binSettleCurrency", binSettleCurrency,
						"BIN_SETTLE_CURRENCY");
				ps.bindDate("accountingDate", ftAccountingDate);
				ps.bindMoney("amount", ftGlAmount);
				ps.bindString("counterParty", counterParty, "COUNTERPARTY");
				ps.bindString("businessUnit", businessUnit, "");
				ps.executeUpdate();
			}
			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlFxStaging() method :: END");
		}

		private void updateGlAccount(FinancialTransaction_Id ftId,
				String glAccount) {
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" UPDATE CI_FT_GL ");
			sb.append(" SET GL_ACCT = :glAccount, VALIDATE_SW = :validateSwitch  ");
			sb.append(" WHERE FT_ID = :ftId AND GL_SEQ_NBR = :glSeqNumber ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("ftId", ftId);
			ps.bindBigInteger("glSeqNumber", glSequenceNumber);
			ps.bindString("glAccount", glAccount, "GL_ACCT");
			ps.bindString("validateSwitch", "N", "VALIDATE_SW");
			ps.executeUpdate();

			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlFxStaging() method :: END");
		}

		/**
		 * Determine if the Financial Transaction is in Error If yes, then
		 * insert error record in CM_FT_GL_ASL_ERR table Else, return true
		 * 
		 * @param businessUnit
		 * @param scheme
		 * @param accountingDate
		 * @return true: If FT is in error false: otherwise
		 */
		private boolean checkIfFtIsInError(Date accountingDate, String scheme,
				String businessUnit) {
			logger.debug("CmGLAccountConstruction_Impl :: checkIfFtIsInError() method :: START");
			boolean isFtInError = false;
			Message_Id messageId = null;
			if (isNull(accountingDate)) {
				isFtInError = true;
				messageId = new Message_Id(new MessageCategory_Id(
						MESSAGE_CATEGORY), ACCT_DT_MISSING_MSG_NBR);
			} else if (isBlankOrNull(scheme)) {
				isFtInError = true;
				messageId = new Message_Id(new MessageCategory_Id(
						MESSAGE_CATEGORY), SCHEME_MISSING_MSG_NBR);
			} else if (isBlankOrNull(businessUnit)) {
				isFtInError = true;
				messageId = new Message_Id(new MessageCategory_Id(
						MESSAGE_CATEGORY), BUSINESS_UNIT_MISSING_MSG_NBR);
			}
			if (isFtInError) {
				logger.error("FT's marked in Error due to Error Info-  " +messageId.getEntity().fetchLanguageEntity().getMessageText() + " for FT : "+ftId);
				insertIntoFtGlAslError(messageId);
				glAccount = " ";
			}
			logger.debug("CmGLAccountConstruction_Impl :: checkIfFtIsInError() method :: END");
			return isFtInError;
		}

		/**
		 * Insert Error Data into CM_FT_GL_ASL_ERR
		 * 
		 * @param messageId
		 */
		private void insertIntoFtGlAslError(Message_Id messageId) {
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslError() method :: START");
			StringBuilder sb = new StringBuilder();
			PreparedStatement ps = null;

			sb.append(" INSERT INTO CM_FT_GL_ASL_ERR ");
			sb.append(" (FT_ID, GL_ACCT, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO, AMOUNT, CURRENCY_CD, GL_SEQ_NBR, BATCH_CODE, BATCH_NBR) ");
			sb.append(" VALUES ");
			sb.append(" (:ftId, :glAcct, :msgCatNbr, :msgNbr, :errorInfo, :amount, :currencyCd, :glSeqNbr,:batchCode, :batchNbr) ");

			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("ftId", ftId);
			if (isBlankOrNull(glAccount))
				ps.bindString("glAcct", " ", "GL_ACCT");
			else
				ps.bindString("glAcct", glAccount, "GL_ACCT");
			ps.bindId("msgCatNbr", messageId.getMessageCategoryId());
			ps.bindBigInteger("msgNbr", messageId.getMessageNumber());
			ps.bindString("errorInfo", messageId.getEntity()
					.fetchLanguageEntity().getMessageText(), "ERROR_INFO");
			ps.bindMoney("amount", ftGlAmount);
			ps.bindId("currencyCd", ftCurrency);
			ps.bindBigInteger("glSeqNbr", glSequenceNumber);
			ps.bindId("batchCode", batchControlId);
			ps.bindBigInteger("batchNbr", batchNbr);
			ps.executeUpdate();

			closePreparedStatement(ps);
			logger.debug("CmGLAccountConstruction_Impl :: insertIntoFtGlAslError() method :: END");
		}

		private void closePreparedStatement(PreparedStatement ps) {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}

		/**
		 * finalizeThreadWork() is execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
				RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}

	}

	public static final class AccountingIdData implements Id {

		private static final long serialVersionUID = 1L;

		private String glAcct;

		public AccountingIdData(String glAcct) {
			setGlAcct(glAcct);
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

		public String getGlAcct() {
			return glAcct;
		}

		public void setGlAcct(String glAcct) {
			this.glAcct = glAcct;
		}
	}

}
