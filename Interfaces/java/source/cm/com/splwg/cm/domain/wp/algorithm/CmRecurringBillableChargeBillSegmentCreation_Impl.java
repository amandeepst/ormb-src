/*******************************************************************************
* FileName                   : CmRecurringBillableChargeBillSegmentCreation_Impl.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Oct 04, 2017 
* Version Number             : 0.2
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Oct 04, 2017        Vienna Rom           Cloned from base (last updated Jul 12, 2017) with added Bill After Date logic  
0.2      NA             May 28, 2018        Vienna Rom           Closed prepared statements
0.3      NA             Jun 10, 2018       RIA				     Changes for ORMB Upgrade 2.6.0.1.0        
*******************************************************************************/
package com.splwg.cm.domain.wp.algorithm;


import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.lookup.CharacteristicTypeLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.StandardMessages;
import com.splwg.base.domain.common.algorithm.Algorithm;
import com.splwg.base.domain.common.algorithm.AlgorithmComponentCache;
import com.splwg.base.domain.common.algorithm.Algorithm_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationOption;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationOption_Id;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfiguration_Id;
import com.splwg.base.domain.common.language.Language;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillSegmentActionLookup;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.BillableChargeStatusLookup;
import com.splwg.ccb.api.lookup.CharacteristicEntityLookup;
import com.splwg.ccb.api.lookup.DivAlgEntityFlagLookup;
import com.splwg.ccb.api.lookup.ExitIterationLoopLookup;
import com.splwg.ccb.api.lookup.ExternalSystemTypeLookup;
import com.splwg.ccb.api.lookup.IntervalBillingStartDayOptionLookup;
import com.splwg.ccb.api.lookup.RecurringFlgLookup;
import com.splwg.ccb.api.lookup.SkipSaReasonLookup;
import com.splwg.ccb.cobol.CobolConstants;
import com.splwg.ccb.domain.admin.billCycle.BillCycle;
import com.splwg.ccb.domain.admin.billCycle.BillCycleSchedule_Id;
import com.splwg.ccb.domain.admin.billCycle.BillCycle_Id;
import com.splwg.ccb.domain.admin.billPeriod.BillPeriod_Id;
import com.splwg.ccb.domain.admin.billSegmentType.BillSegmentTypeBillSegmentCreationAlgorithmSpot;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.payplan.policyinvoicefrequency.PolicyInvoiceFrequency;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType;
import com.splwg.ccb.domain.admin.timeOfUse.TimeOfUse_Id;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristicData;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic_Id;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineData;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeaderData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeaderData_Impl;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentGeneratorData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentItemData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentReadData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentRegisterRead;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentRegisterRead_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentRegisterRead_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantity;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantityData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantity_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantity_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.billing.billSegment.InvoiceDataVO;
import com.splwg.ccb.domain.billing.billSegment.MessageRepository;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge;
import com.splwg.ccb.domain.billing.billableCharge.BillableChargeLine;
import com.splwg.ccb.domain.billing.billableCharge.BillableChargeLineCharacteristic;
import com.splwg.ccb.domain.billing.billableCharge.BillableChargeRead;
import com.splwg.ccb.domain.billing.billableCharge.BillableChargeServiceQuantity;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.billing.trialBilling.TrialBill_Id;
import com.splwg.ccb.domain.common.algorithm.AlgorithmMessageRepository;
import com.splwg.ccb.domain.common.characteristic.CharacteristicData;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementRateScheduleData;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.pricing.exchangeRate.CurrencyConversionAlgorithmSpot;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentSearchAlgorithmSpot;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.ccb.domain.pricing.priceparm.PriceParamUtils;
import com.splwg.ccb.domain.rate.ApplyRateData;
import com.splwg.ccb.domain.rate.RateApplicationProcessor;
import com.splwg.ccb.domain.rate.RateApplicationProcessorData;
import com.splwg.ccb.domain.rate.rateSchedule.RateSchedule;
import com.splwg.ccb.domain.rate.rateSchedule.RateSchedule_Id;
import com.splwg.ccb.domain.util.CommonGenericMethod;
import com.splwg.ccb.domain.util.Constants;
import com.splwg.ccb.domain.util.exception.ConfigException;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 *
 *@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = characteristicTypeForAccount, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = characteristicTypeForPerson, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = characteristicTypeForPriceList, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = characteristicTypeForPriceAssignId, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = characteristicTypeForTrialBillId, type = string)})
 *
 *
 */

public class CmRecurringBillableChargeBillSegmentCreation_Impl
        extends CmRecurringBillableChargeBillSegmentCreation_Gen
        implements BillSegmentTypeBillSegmentCreationAlgorithmSpot {

    private static final Logger logger = LoggerFactory.getLogger(CmRecurringBillableChargeBillSegmentCreation_Impl.class);

    //Static Fields  -----------------------------------------------------------------------------------------------------------
    private static final String ACCT_ID = "ACCT_ID";
    //private static final String BILL = "BILL";
    private static final String BILL_ID = "BILL_ID";
    //private static final String BILL_CYC_SCHEDULE = "BILL CYCLE SCHEDULE";
    private static final String CUTOFF_DT = "CUTOFF_DT";
    private static final String ITERATION_CNT = "ITERATION_CNT";
    private static final String SA_ID = "SA_ID";
    private static final String BILL_CYC_CD = "BILL_CYC_CD";
    private static final String CURRENCY_CD = "CURRENCY_CD";
    private static final String ALG_CD = "ALG_CD";
    private static final String WIN_START_DT = "WIN_START_DT";
    private static final String WIN_END_DT = "WIN_END_DT";
    private static final String ACCOUNTING_DT = "ACCOUNTING_DT";
    private static final String CIS_DIVISION = "CIS_DIVISION";
    private static final String TARGET_SA_CHAR_CD = "TARGET_SA_CHAR_CD";
    private static final String SA_TYPE_CD = "SA_TYPE_CD";
    private static final String CI_TRL_BILL = "CI_TRL_BILL";
    private static final String CI_BILL = "CI_BILL";
    private static final String RECON_FEATURE_CONFIG_NAME = "C1_INSRECON";
    private static final ConcurrentHashMap<String, List<BillableChargeWrapperObject>> BILLABLE_CHARGE_HASHMAP = new ConcurrentHashMap<String, List<BillableChargeWrapperObject>>();

    //Soft Parameters  ------------------------------------------------------------------------------------------------------
    private String charTypeAccount;
    private String charTypePerson;
    private String charTypePriceList;
    private String charTypePriceAsgnId;
    //added for trial bill
    private String charTypeTrialBill;

    private boolean prorationRequiredStartDate = false;
    private boolean prorationRequiredEndDate = false;

    //For Logging
    private boolean isDebugLogEnabled = false;

    //characteristic data variables passed to rating engine

    private String billSegmentPricingAccount;
    private String billSegmentPricingPerson;
    private String billSegmentPricingPriceList;
    private String billSegmentPricingPriceAsgnId;

    private String divisionCharType = "";
    private String saTypeCharType = "";
    private String chargeTypeCharType = "";
    private String billableChargeCharType = "";
    private String prorationStartDateCharType = "";
    private String prorationEndDateCharType = "";
    private String refContract = null;

    //----Parameters which store get previous/get next and lead days

    private int billLeadDays;

    private Bool mustRetainReads;
    private Bool mustRetainItems;
    private Bool mustRetainServiceQuantities;
    private ServiceAgreement serviceAgreement;
    private BillSegmentActionLookup billSegmentAction;
    private Date consumptionStartDate;
    private Date consumptionEndDate;
    private Date prorationStartDate;
    private Date prorationEndDate;
    private Date serviceAgreementStartDate;
    private Date serviceAgreementEndDate;
    private IntervalBillingStartDayOptionLookup intervalBillingStartDayOption;
    private Time intervalBillingCutoffTime;
    private Money recurringChargeAmount;
    private Money proratedRecurringChargeAmount;
    private Bool mustSkipServiceAgreement = Bool.FALSE;
    private Bool mustSkipIteration = Bool.FALSE;
    private SkipSaReasonLookup skipSaReason;
    private Bool mustRestartGeneration = Bool.FALSE;
    private ExitIterationLoopLookup exitLoopCheck = ExitIterationLoopLookup.constants.YES;
    private BillSegmentData billSegmentData;
    private List<BillSegmentReadData> billSegmentReadData;
    private BillSegmentGeneratorData billSegmentGeneratorData;
    private List<BillSegmentItemData> billSegmentItemData;
    private List<BillSegmentServiceQuantityData> billSegmentServiceQuantityData;
    private List<BillSegmentCalculationHeaderData> billSegmentCalculationHeaderData;

    //Work Fields     ------------------------------------------------------------------------------------------------------
    private BillableCharge billableCharge = null;
    private BigInteger billSegmentCalculationHeaderCount;
    private BigInteger billSegmentCalculationLineCount;
    private BigInteger billSegmentCalculationLineCharacteristicCount;
    private Bool billableChargeLinesExist = Bool.FALSE;
    private Bool billableChargeServiceQuantitiesExist = Bool.FALSE;
    private Bool mustExitIterationLoop = Bool.FALSE;
    private ServiceAgreementType saType;
    private InvoiceDataVO invoiceDataVO;
    private Bool isInvoiceAcct = Bool.FALSE;
    private Bool isMemberIGAccount = Bool.FALSE;
    private String pricingCurrencyCd;
    private ServiceAgreement refServiceAgreement;
    private BillableChargeStatusLookup billChrgStaus;

    private List<BillableChargeWrapperObject> listBillableCharge = new ArrayList<BillableChargeWrapperObject>();

    private Date billSegmentStartDate;
    private Date billSegmentEndDate;
    /*
     * Start Delete - 2017-09-22 - VRom
     */
    //private Date nextBillSegmentEndDate;
    /*
     * End Delete - 2017-09-22 - VRom
     */

    // Methods ------------------------------------------------------------------------------------------------------

    public void setMustRetainReads(Bool mustRetainReads) {
        this.mustRetainReads = mustRetainReads;
    }

    public void setMustRetainItems(Bool mustRetainItems) {
        this.mustRetainItems = mustRetainItems;
    }

    public void setMustRetainServiceQuantities(Bool mustRetainServiceQuantities) {
        this.mustRetainServiceQuantities = mustRetainServiceQuantities;
    }

    public void setServiceAgreement(ServiceAgreement serviceAgreement) {
        this.serviceAgreement = serviceAgreement;
    }

    public void setBillSegmentAction(BillSegmentActionLookup billSegmentAction) {
        this.billSegmentAction = billSegmentAction;
    }

    public void setConsumptionStartDate(Date consumptionStartDate) {
        this.consumptionStartDate = consumptionStartDate;
    }

    public Date getConsumptionStartDate() {
        return consumptionStartDate;
    }

    public void setConsumptionEndDate(Date consumptionEndDate) {
        this.consumptionEndDate = consumptionEndDate;
    }

    public Date getConsumptionEndDate() {
        return consumptionEndDate;
    }

    public void setServiceAgreementStartDate(Date serviceAgreementStartDate) {
        this.serviceAgreementStartDate = serviceAgreementStartDate;
    }

    public Date getServiceAgreementStartDate() {
        return serviceAgreementStartDate;
    }

    public void setServiceAgreementEndDate(Date serviceAgreementEndDate) {
        this.serviceAgreementEndDate = serviceAgreementEndDate;
    }

    public Date getServiceAgreementEndDate() {
        return serviceAgreementEndDate;
    }

    public void setIntervalBillingStartDayOption(IntervalBillingStartDayOptionLookup intervalBillingStartDayOption) {
        this.intervalBillingStartDayOption = intervalBillingStartDayOption;
    }

    public IntervalBillingStartDayOptionLookup getIntervalBillingStartDayOption() {
        return intervalBillingStartDayOption;
    }

    public void setIntervalBilllingCutoffTime(Time intervalBillingCutOffTime) {
        intervalBillingCutoffTime = intervalBillingCutOffTime;
    }

    public Time getIntervalBillingCutoffTime() {
        return intervalBillingCutoffTime;
    }

    public Money getRecurringChargeAmount() {
        return recurringChargeAmount;
    }

    public Money getProratedRecurringChargeAmount() {
        return proratedRecurringChargeAmount;
    }

    public Bool getMustSkipServiceAgreement() {
        return mustSkipServiceAgreement;
    }

    public SkipSaReasonLookup getSkipSaReason() {
        return skipSaReason;
    }

    public Bool getMustRestartGeneration() {
        return mustRestartGeneration;
    }

    public ExitIterationLoopLookup getExitLoopCheck() {
        return exitLoopCheck;
    }

    public void setBillSegmentData(BillSegmentData billSegmentData) {
        this.billSegmentData = billSegmentData;
    }

    public BillSegmentData getBillSegmentData() {
        return billSegmentData;
    }

    public void setBillSegmentReadData(List<BillSegmentReadData> billSegmentReadData) {
        this.billSegmentReadData = billSegmentReadData;
    }

    public List<BillSegmentReadData> getBillSegmentReadData() {
        return billSegmentReadData;
    }

    public void setBillSegmentGeneratorData(BillSegmentGeneratorData billSegmentGeneratorData) {
        this.billSegmentGeneratorData = billSegmentGeneratorData;
    }

    public BillSegmentGeneratorData getBillSegmentGeneratorData() {
        return billSegmentGeneratorData;
    }

    public void setBillSegmentItemData(List<BillSegmentItemData> billSegmentItemData) {
        this.billSegmentItemData = billSegmentItemData;
    }

    public List<BillSegmentItemData> getBillSegmentItemData() {
        return billSegmentItemData;
    }

    public void setBillSegmentServiceQuantityData(List<BillSegmentServiceQuantityData> billSegmentServiceQuantityData) {
        this.billSegmentServiceQuantityData = billSegmentServiceQuantityData;
    }

    public List<BillSegmentServiceQuantityData> getBillSegmentServiceQuantityData() {
        return billSegmentServiceQuantityData;
    }

    public void setBillSegmentCalculationHeaderData(
            List<BillSegmentCalculationHeaderData> billSegmentCalculationHeaderData) {
        this.billSegmentCalculationHeaderData = billSegmentCalculationHeaderData;
    }

    public List<BillSegmentCalculationHeaderData> getBillSegmentCalculationHeaderData() {
        return billSegmentCalculationHeaderData;
    }

    /*
     * Methods setBillSegment() & setPreviousFrozenBillSegment() are not used in the algo
     * For CM/backward compatibility
     */
    public void setBillSegment(BillSegment billSegment) {
    }

    public void setPreviousFrozenBillSegment(BillSegment billSegment) {
    }

    public void invoke() {
    	
    	logger.info("CUSTOM BSEG CREATION ALG");


        // If debug is enabled, used for logging details.
        FrameworkSession frameworkSession = (FrameworkSession) SessionHolder.getSession();
        isDebugLogEnabled = frameworkSession.getRequestContext().traceStandardOut();

        if (isDebugLogEnabled) {
            logger.info("Calling Recurring Algo...");
            logger.info("BillSegmentGeneratorData contents at Entering invoke() : ");
            logger.info("Acct      : " + billSegmentGeneratorData.getAccount());
            logger.info("SA        : " + billSegmentGeneratorData.getServiceAgreement());
            logger.info("BChg      : " + billSegmentGeneratorData.getBillableCharge());
            logger.info("Itr Cnt   : " + billSegmentGeneratorData.getIterationCount());
            logger.info("CutOffDt  : " + billSegmentGeneratorData.getCutoffDate());
            logger.info("WinStrtDt : " + billSegmentGeneratorData.getWindowStartDate());
        }

        if (billSegmentAction.isDelete())
            return;

        readAndValidateSoftParameters();

        validateInput();
        readAccountDetails();
        try {

            createBillSegment();

        }
        // If an error occured from cobol eg. applying rate
        // mark the charge as processed and skip it in next iteration.
        catch (ApplicationError e) {
            List<BillableChargeWrapperObject> bcList = BILLABLE_CHARGE_HASHMAP.get(billSegmentGeneratorData
                    .getServiceAgreement().getId().getIdValue());
            if (bcList != null) {
                for (BillableChargeWrapperObject bcwo : bcList) {
                    if (bcwo.getBchargeId().getIdValue().equals(billableCharge.getId().getIdValue()))
                        bcwo.setIsProcessed(true);
                }
            }

            throw e;
        }

        // handleSplitBilling();
        clearData();

        if (isDebugLogEnabled) {
            logger.info("BillSegmentGeneratorData contents at Exiting invoke(): ");
            logger.info("Acct      : " + billSegmentGeneratorData.getAccount());
            logger.info("SA        : " + billSegmentGeneratorData.getServiceAgreement());
            logger.info("BChg      : " + billSegmentGeneratorData.getBillableCharge());
            logger.info("Itr Cnt   : " + billSegmentGeneratorData.getIterationCount());
            logger.info("CutOffDt  : " + billSegmentGeneratorData.getCutoffDate());
            logger.info("WinStrtDt : " + billSegmentGeneratorData.getWindowStartDate());
        }
    }

    /**
     * This method checks for any split billing algorithm on bill segment post processing on contract type and executes it
     */
    // calling removed for performance issues
    /*private void handleSplitBilling() {

        logger.debug("Checking for split  billing ");
        ServiceAgreementType saType = null;
        if (notNull(refContract))
            saType = refServiceAgreement.getServiceAgreementType();
        else
            saType = serviceAgreement.getServiceAgreementType();

        ServiceAgreementTypeAlgorithms saTypAlgos = saType.getAlgorithms();
        for (ServiceAgreementTypeAlgorithm saTypeAlgo : saTypAlgos) {
            Algorithm saTypeAlgorithm = saTypeAlgo.getAlgorithm();
            if (saTypeAlgorithm.getAlgorithmType().getAlgorithmEntity().compareTo(
                    ServiceAgreementTypeAlgorithmLookup.constants.BILL_SEGMENT_POST_PROCESSING) == 0) {

                BillSegmentPostProcessingAlgorithmSpot postProcessing = saTypeAlgorithm
                        .getAlgorithmComponent(BillSegmentPostProcessingAlgorithmSpot.class);

                postProcessing.setCalcHeader(billSegmentCalculationHeaderData);

                postProcessing.setBillId(billSegmentGeneratorData.getBill().getId());

                postProcessing.setBillSegmentStartDate(billSegmentStartDate);
                if (billableCharge != null) {
                    postProcessing.setBillableChargeId(billableCharge.getId());
                }
                if (notNull(refContract)) {
                    postProcessing.setServiceAgreementId(refServiceAgreement.getId());
                } else {
                    postProcessing.setServiceAgreementId(billSegmentGeneratorData.getServiceAgreement().getId());
                }
                postProcessing.invoke();

                billSegmentCalculationHeaderData = postProcessing.getCalcHeader();
            }
        }
        logger.debug("Completed split  billing check");
    }*/

    /**
     * Reads the account details
     * Checks if the account is master or member account and sets the flag
     * if member account is being processed, the contract is skipped
     * leads days which is used for getPrevious /getNext billing
     *
     */
    private void readAccountDetails() {

        logger.debug("Reading bill lead days on account");

        Account account = billSegmentGeneratorData.getServiceAgreement().getAccount();
        String charTypeCd = getIGACharType().trim();

        BigInteger billDays = account.getBillLeadDays();
        refContract = checkReferenceContract(billSegmentGeneratorData.getServiceAgreement(), charTypeCd);
        if (refContract != null)
            refServiceAgreement = new ServiceAgreement_Id(refContract).getEntity(); // set in class field to use later

        billLeadDays = billDays.intValue();

        if (!isNull(billSegmentGeneratorData) &&
                isNull(billSegmentGeneratorData.getBillableCharge()) || billSegmentGeneratorData.getIsRebill()
                .isTrue()) {
            //    Check for Member or Master Account, set the flag
        	StringBuilder query = null;
        	query = new StringBuilder();
        	PreparedStatement fileStatement = null;
        	try {

        		query.append(" SELECT COUNT(1) AS COUNTREF FROM CI_SA_CHAR A, CI_SA B ");
        		query.append(" WHERE A.CHAR_TYPE_CD = :charTypeCd ");
        		query.append(" AND B.SA_STATUS_FLG IN ('20', '30', '40', '50') ");
        		query.append(" AND A.SRCH_CHAR_VAL IN ");
        		query
        		.append(" (SELECT SA_ID FROM CI_SA C WHERE C.ACCT_ID = :acctId AND C.SA_STATUS_FLG IN ('20', '30', '40', '50')) ");
        		query.append(" AND B.SA_ID = A.SA_ID  ");
        		query
        		.append(" AND A.EFFDT = (SELECT MAX(X.EFFDT) FROM CI_SA_CHAR X WHERE X.SA_ID = A.SA_ID AND X.CHAR_TYPE_CD = A.CHAR_TYPE_CD ");
        		query
        		.append(" AND X.EFFDT <= :cutoffDate) AND B.START_DT < = :cutoffDate AND ((B.END_DT IS NULL) OR (B.END_DT >= :cutoffDate)) ");

        		fileStatement = createPreparedStatement(query.toString(), "getIsMemberAccount");
        		fileStatement.bindString("acctId", account.getId().getTrimmedValue(), ACCT_ID);
        		fileStatement.bindString("charTypeCd", charTypeCd, "CHAR_TYPE_CD");
        		fileStatement.bindDate("cutoffDate", billSegmentGeneratorData.getCutoffDate());
        		fileStatement.setAutoclose(false);

        		List<SQLResultRow> result = fileStatement.list();

        		if (result != null && !result.isEmpty()) {
        			String countRef = result.get(0).getString("COUNTREF");
        			if (!countRef.equalsIgnoreCase("0")) // Count 0 denotes Master Account
        				isMemberIGAccount = Bool.TRUE;
        		}

        		if (isMemberIGAccount == Bool.TRUE) {
        			billSegmentGeneratorData.setCanSkipSa(Bool.TRUE);
        			mustSkipServiceAgreement = Bool.TRUE;
        			logger.debug("Member Account found.. Skipping Contract...");
        		}

        	} 
        	finally {
        		if(fileStatement != null) {
        			fileStatement.close();
        		}
        	}
        }

        logger.debug("Completed reading bill lead days on account");
    }

    /**
     * Returns Reference SA ID for an Invoice Group
     *
     */
    private String checkReferenceContract(ServiceAgreement contractID, String saCharTypeRef) {
        StringBuilder igaQuery = null;
        String saId = null;
        PreparedStatement fileStatement = null;
        try {
            Date systemDate = SessionHolder.getSession().getProcessDateTime().getDate();

            if (notNull(saCharTypeRef)) {
                igaQuery = new StringBuilder(" SELECT B.SRCH_CHAR_VAL AS CONTRACT_ID from CI_SA_CHAR B, CI_SA A ");
                igaQuery.append(" WHERE B.SA_ID =:serviceAgreement AND B.CHAR_TYPE_CD =:charType ");
                igaQuery.append(" AND B.SA_ID = A.SA_ID AND A.SA_STATUS_FLG IN ('20', '30', '40', '50') ");
                igaQuery.append(" AND B.EFFDT = (SELECT MAX(X.EFFDT) FROM CI_SA_CHAR X WHERE X.SA_ID = B.SA_ID ");
                igaQuery.append(" AND X.CHAR_TYPE_CD = B.CHAR_TYPE_CD AND X.EFFDT <=:currDate) ");
                igaQuery.append(" AND ((A.END_DT IS NULL) OR (A.END_DT >=:endDate))");

                fileStatement = createPreparedStatement(igaQuery.toString(), "getSaID");
                fileStatement.bindString("serviceAgreement", contractID.getId().getIdValue(), "SA_ID");
                fileStatement.bindDate("endDate", systemDate);
                fileStatement.bindDate("currDate", systemDate);
                fileStatement.bindString("charType", saCharTypeRef, "CHAR_TYPE_CD");
                fileStatement.setAutoclose(false);
                final List<? extends QueryResultRow> queryResults = fileStatement.list();

                for (QueryResultRow row : queryResults) {
                    saId = row.getString("CONTRACT_ID");
                }

                if (isDebugLogEnabled)
                    logger.info("reference SA ID :--" + saId);
            }
        }

        finally {
            if (null != fileStatement)
                fileStatement.close();
            fileStatement = null;
        }
        return saId;
    }

    /**
     * Get characteristic type from feature config for IGA setup
     */
    private String getIGACharType() throws ConfigException {
        String saCharType = null;
        try {
            saCharType = CommonGenericMethod.getFeatureConfigValue("C1_INVGRPINF", "SAFK");
        } catch (ConfigException e) {
            logger.info("SA FK Reference Invoicing Group is not configured.");
            throw e;
        }
        return saCharType;
    }

    /**
     * Validates inputs for bill segment creation algo
     */
    private void validateInput() {
        logger.debug("Validating inputs of bill segment creation algo");
        validateIterationCount();
        validateServiceAgreement();
        validateCutoffDate();
        //validateEligibility();
        readAndValidateCharTypesFromFeatureConfig();
    }

    /**
     * Creates bill segments for billable charges having service quantity
     */
    private void generateChargesForBillableChargeServiceQuantities(RateSchedule priceAssignRateSchedule) {
        if (mustRetainServiceQuantities.isFalse()) {
            List<QueryResultRow> billableChargeServiceQuantities = fetchBillableChargeServiceQuantities();
            if (!billableChargeServiceQuantities.isEmpty()) billableChargeServiceQuantitiesExist = Bool.TRUE;
            for (QueryResultRow row : billableChargeServiceQuantities) {
                generateChargeForBillableChargeServiceQuantity((BillableChargeServiceQuantity) row.get("bcsq"));
            }
        }

        if (saType.getIsRateRequired().isTrue() && !billSegmentServiceQuantityData.isEmpty()) {
            generateChargesForBillableChargeServiceQuantitiesUsingRate(priceAssignRateSchedule);
        } else if (notNull(billableCharge.getPriceItemCodeId()) && !billSegmentServiceQuantityData.isEmpty()) {
            generateChargesForBillableChargeServiceQuantitiesUsingRate(priceAssignRateSchedule);
        }
    }

    /**
     * This method reads and validates soft parameters of the algorithm
     */
    private void readAndValidateSoftParameters() {

        logger.debug("Reading and validating soft parameters on algo");

        charTypeAccount = getCharacteristicTypeForAccount();

        validateAdhocAndFKCharTypes(charTypeAccount);

        charTypePerson = getCharacteristicTypeForPerson();

        validateAdhocAndFKCharTypes(charTypePerson);

        charTypePriceList = getCharacteristicTypeForPriceList();

        validateAdhocAndFKCharTypes(charTypePriceList);

        charTypePriceAsgnId = getCharacteristicTypeForPriceAssignId();

        validateAdhocAndFKCharTypes(charTypePriceAsgnId);

        // adding char type for trial bill
        charTypeTrialBill = getCharacteristicTypeForTrialBillId();

    }

    /**
     * @param charTypeStr
     * Validate Adhoc char &FK types
     *
     */
    private void validateAdhocAndFKCharTypes(String charTypeStr) {
        if (charTypeStr == null) {
            addError(MessageRepository.invalidCharTypes());
        }

        CharacteristicType_Id charType = new CharacteristicType_Id(charTypeStr.trim());

        CharacteristicType charTypeEntity = charType.getEntity();

        if (charTypeEntity == null) {
            addError(MessageRepository.invalidCharTypes());
        }

        if (charTypeEntity.getCharacteristicType().compareTo(CharacteristicTypeLookup.constants.ADHOC_VALUE) != 0) {
            addError(MessageRepository.invalidCharTypes());
        }

    }

    /**
     * validate iteration count
     */
    private void validateIterationCount() {
        if (billSegmentGeneratorData == null) addError(StandardMessages.fieldMissing(ITERATION_CNT));

        BigInteger iterationCount = billSegmentGeneratorData.getIterationCount();
        if (iterationCount == BigInteger.ZERO || iterationCount == null)
            addError(StandardMessages.fieldMissing(ITERATION_CNT));
    }

    /**
     * Validates service agreement details within algo inputs
     */
    private void validateServiceAgreement() {
        if (billSegmentGeneratorData.getServiceAgreement() == null) addError(StandardMessages.fieldMissing(SA_ID));

        saType = billSegmentGeneratorData.getServiceAgreement().getServiceAgreementType();
        if (saType == null) addError(StandardMessages.fieldMissing(SA_TYPE_CD));

        if (billSegmentGeneratorData.getAccount() == null) addError(StandardMessages.fieldMissing(ACCT_ID));

    }

    /**
     * Validates presence of cutoffdate
     */
    private void validateCutoffDate() {
        if (billSegmentGeneratorData.getCutoffDate() == null) addError(StandardMessages.fieldMissing(CUTOFF_DT));
    }

    /*
     * calling removed for performance issues
     */
    /*private void validateEligibility() {
        determineBillSegmentGenerationMode();

    }

    private String determineBillSegmentGenerationMode() {
        return SessionHolder.getSession().isOnlineConnection() ? BILL : (billSegmentData.getBillSegmentDto()
                .fetchBillCycleSchedule() != null ? BILL_CYC_SCHEDULE : BILL);
    }*/

    /**
     * This method first checks eligibility of a billable charge
     * if satisfied, it created the bill segment
     * by processing pass through lines and service quantities
     * It also calls crawling algo for pricing info.
     */
    private void createBillSegment() {

        /*
         * Check if the Billable charge is to be skipped.
         * Case: if the query for fetching invoice account details returns no result
         * usually the case, when target contract type is not set
         * Then the Billable Charge is to be Skipped.
         * If the Member Account is being Billed so segments should go to Master Account
         * Then Contract is skipped.
         */
        if (billSegmentGeneratorData.getSkipBillChgSw() == Bool.TRUE
                || billSegmentGeneratorData.getCanSkipSa() == Bool.TRUE) {
            return;
        }

        initializeInputData();
        initializeBillSegmentData();

        if (billSegmentGeneratorData.getSkipBillChgSw() == Bool.TRUE) {
            return;
        }

        initializeBillSegmentCollections();
        //24763058 - ODB : ADHOC CHARGES BILL SEGMENT NOT GENERATED - start
        billableCharge = determineBillableChargeToBill();
        RecurringFlgLookup recurFlg = null;
        if (notNull(billableCharge)) {
            recurFlg = billableCharge.getRecurringFlg();
        }

        if (billSegmentGeneratorData.getIsOffCycleBill().isTrue() &&
                (notNull(recurFlg) && !recurFlg.isBlankLookupValue())) {
            logger.debug("Adhoc billable charge..Recurring... Exiting..");
            exitLoopCheck = ExitIterationLoopLookup.constants.YES;
            mustSkipServiceAgreement = Bool.TRUE;
            skipSaReason = SkipSaReasonLookup.constants.NO_BILLABLE_CHARGES;
            return;
        }
        //24763058 - ODB : ADHOC CHARGES BILL SEGMENT NOT GENERATED - end

        RateSchedule priceAsgnRateSchedule = null;

        if (billableCharge != null) {

            if (isDebugLogEnabled) {
                logger.info("Inside createBillSegment() : ");
                logger.info("Acct      : " + billSegmentGeneratorData.getAccount());
                logger.info("SA        : " + billSegmentGeneratorData.getServiceAgreement());
                logger.info("BChg      : " + billSegmentGeneratorData.getBillableCharge());
                logger.info("Itr Cnt   : " + billSegmentGeneratorData.getIterationCount());
            }
            PriceItem_Id priceItemId = billableCharge.getPriceItemCodeId();

            BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();

            // Pass Through charge lines processing
            if (priceItemId == null) {
                if (billableCharge != null) {
                    bsegDTO.setStartDate(billSegmentStartDate);
                    bsegDTO.setEndDate(billSegmentEndDate);
                    consumptionStartDate = billableCharge.getStartDate();
                    consumptionEndDate = billableCharge.getEndDate();
                }

                if (mustSkipServiceAgreement.isFalse()) {
                    if (mustExitIterationLoop.isFalse())
                        exitLoopCheck = ExitIterationLoopLookup.constants.NO;

                    if (billableCharge != null && billSegmentEndDate != null && billSegmentStartDate != null) {
                        if (billSegmentEndDate.compareTo(billSegmentStartDate) >= 0) {
                            generateBillableChargeBillSegment(null);
                        }
                    }
                }
            }
            // Charges SQ processing
            else {
                //-------- Crawling Algo----------

                // if priceAssign Id is not present in billable charge, call crawling algo
                PriceAsgn_Id prcAsgnId = billableCharge.getPriceAsgnId();
                if (prcAsgnId == null) {

                    Algorithm_Id crawlingId = null;
                    CisDivision_Id divisionId = null;
                    if (refServiceAgreement != null) {
                        divisionId = refServiceAgreement.getServiceAgreementType().getId().getDivisionId();
                    } else {
                        divisionId = serviceAgreement.getServiceAgreementType().getId().getDivisionId();
                    }

                    if (isInvoiceAcct == Bool.FALSE) {
                        Query<Algorithm> divAlgoQuery = createQuery("from CisDivAlg alg where alg.id.division =:divCd and  alg.id.divAlgEntityFlag=:divAlgo ","");

                        divAlgoQuery.bindLookup("divAlgo",
                                com.splwg.ccb.api.lookup.AlgorithmEntityLookup.constants.PRICE_ASSIGNMENT_ALGORITHM);
                        divAlgoQuery.bindId("divCd", divisionId);

                        divAlgoQuery.addResult("algorithmCode", "alg.algorithm");

                        Algorithm crawlingAlgo = (Algorithm) divAlgoQuery.firstRow();
                        crawlingId = crawlingAlgo.getId();
                    }

                    else if (isInvoiceAcct == Bool.TRUE && invoiceDataVO != null) {
                        crawlingId = new Algorithm_Id(invoiceDataVO.getAlgCd());
                    }

                    if (crawlingId != null) {
                        boolean multiParmEnabled;

                        /*
                         * Start Change - 2017-09-22 - VRom
                         */
                        //Algorithm algorithm = crawlingId.getEntity();
                        
                        //PriceAssignmentSearchAlgorithmSpot crawlingAlgo = algorithm
                        //        .getAlgorithmComponent(PriceAssignmentSearchAlgorithmSpot.class);
                        PriceAssignmentSearchAlgorithmSpot crawlingAlgo = AlgorithmComponentCache.getAlgorithmComponent(
                        		crawlingId, PriceAssignmentSearchAlgorithmSpot.class);
                        /*
                         * End Change - 2017-09-22 - VRom
                         */
                        
                        if (refServiceAgreement != null) {
                            crawlingAlgo.setServiceAgreementId(refServiceAgreement.getId().getIdValue());
                        } else {
                            crawlingAlgo.setServiceAgreementId(serviceAgreement.getId().getIdValue());
                        }

                        crawlingAlgo.setPriceItemCode(priceItemId.getIdValue());
                        crawlingAlgo.setProcessDate(billSegmentEndDate);
                        crawlingAlgo.setPriceAssignmentType(Constants.PRICE_ASSIGNMENT_TYPES);
                        multiParmEnabled = Boolean.parseBoolean(CommonGenericMethod.getFeatureConfigValue(
                                "C1_PPARM_FLG", "MPPE").trim());

                        if (isDebugLogEnabled)
                            logger.info("multiParmEnabled :-- " + multiParmEnabled);

                        if (multiParmEnabled) {

                            if (isDebugLogEnabled)
                                logger.info("billableCharge.getPriceItemParmGroupId() :- "
                                        + billableCharge.getPriceItemParmGroupId());

                            crawlingAlgo.setPriceParams(getParamMap(billableCharge.getPriceItemParmGroupId()));
                            crawlingAlgo.setTimeOfUse(null);
                        } else {
                            crawlingAlgo.setPriceParams(null);
                            if (notNull(billableCharge.getTimeOfUseId())) {
                                if (isDebugLogEnabled)
                                    logger.info("billableCharge.getTimeOfUseId().getIdValue() :- "
                                            + billableCharge.getTimeOfUseId().getIdValue());
                                crawlingAlgo.setTimeOfUse(billableCharge.getTimeOfUseId().getIdValue());
                            }
                        }
                        crawlingAlgo.invoke();
                        billSegmentPricingPriceAsgnId = crawlingAlgo.getPriceAssignmentId();
                        billSegmentPricingAccount = crawlingAlgo.getAccountId();
                        billSegmentPricingPerson = crawlingAlgo.getPersonId();
                        billSegmentPricingPriceList = crawlingAlgo.getPriceListId();
                        pricingCurrencyCd = crawlingAlgo.getCurrencyCode();

                        billSegmentGeneratorData.setPriceItemCd(crawlingAlgo.getPriceItemCode());
                        billSegmentGeneratorData.setTouCd(crawlingAlgo.getTimeOfUse());
                        billSegmentGeneratorData.setPriceAsgnId(billSegmentPricingPriceAsgnId);

                        String rsCode = crawlingAlgo.getRateScheduleCode();
                        priceAsgnRateSchedule = new RateSchedule_Id(rsCode).getEntity();
                    }
                }
                // else-if priceAssignId is already present in billable charge, populate the fields
                else {
                    billSegmentPricingPriceAsgnId = prcAsgnId.getIdValue();
                    billSegmentPricingAccount = billableCharge.getPaAccountId();
                    billSegmentPricingPerson = billableCharge.getPaPersonId();
                    billSegmentPricingPriceList = billableCharge.getPaPriceListId();

                    PriceAsgn prcAsgn = prcAsgnId.getEntity();

                    pricingCurrencyCd = prcAsgn.getPriceCurrencyCode();
                    billSegmentGeneratorData.setPriceItemCd(prcAsgn.getPriceItemCodeId().getIdValue());
                    TimeOfUse_Id touId = prcAsgn.getTimeOfUseId();
                    if (touId != null)
                        billSegmentGeneratorData.setTouCd(prcAsgn.getTimeOfUseId().getIdValue());
                    billSegmentGeneratorData.setPriceAsgnId(billSegmentPricingPriceAsgnId);
                    priceAsgnRateSchedule = prcAsgn.getRateScheduleId().getEntity();
                }

                bsegDTO.setStartDate(billSegmentStartDate);
                bsegDTO.setEndDate(billSegmentEndDate);
                consumptionStartDate = billableCharge.getStartDate();
                consumptionEndDate = billableCharge.getEndDate();

                if (mustSkipServiceAgreement.isFalse()) {
                    if (mustExitIterationLoop.isFalse()) exitLoopCheck = ExitIterationLoopLookup.constants.NO;
                    if (billableCharge != null) {
                        if (billSegmentEndDate.compareTo(billSegmentStartDate) >= 0) {
                            generateBillableChargeBillSegment(priceAsgnRateSchedule);
                        }
                    }
                }
            }
        }

        if (mustSkipIteration.isTrue()) {
            exitLoopCheck = ExitIterationLoopLookup.constants.YES;
        }

        if (exitLoopCheck.isYes())
            BILLABLE_CHARGE_HASHMAP.remove(billSegmentGeneratorData.getServiceAgreement().getId().getIdValue());
    }

    private Map<String, String> getParamMap(BigInteger ppgId) {
        String parmStr = null;
        if (ppgId.longValue() != 1) {
        	PreparedStatement query = null;
        	try {
        		query = createPreparedStatement(
        				"select PARM_STR from CI_PRICEITEM_PARM_GRP_K where PRICEITEM_PARM_GRP_ID = :ppgId ",
        				"get Parm Group Details");

        		query.bindBigInteger("ppgId", ppgId);
        		query.setAutoclose(false);
        		List<SQLResultRow> list = query.list();
        		if (!list.isEmpty()) {
        			parmStr = list.get(0).getString("PARM_STR");

        			if (isDebugLogEnabled)
        				logger.info("parmStr:-- " + parmStr);
        		}
        	}
        	finally {
        		if(query != null) {
        			query.close();
        		}
        	}
        }

        return PriceParamUtils.parseParams(parmStr);
    }

    /*
     * Initialize all inputs and lists to null
     */
    private void initializeInputData() {
        if (billSegmentData == null) {
            billSegmentData = BillSegmentData.Factory.newInstance();
            billSegmentData.setBillSegmentDto((BillSegment_DTO) createDTO(BillSegment.class));
        } else if (billSegmentData.getBillSegmentDto() == null)
            billSegmentData.setBillSegmentDto((BillSegment_DTO) createDTO(BillSegment.class));
        if (billSegmentReadData == null) billSegmentReadData = new ArrayList<BillSegmentReadData>();
        if (billSegmentItemData == null) billSegmentItemData = new ArrayList<BillSegmentItemData>();
        if (billSegmentServiceQuantityData == null)
            billSegmentServiceQuantityData = new ArrayList<BillSegmentServiceQuantityData>();
        if (billSegmentGeneratorData.getIsRebill() == null) billSegmentGeneratorData.setIsRebill(Bool.FALSE);
    }

    private void reverseInitializeInputData() {
        billSegmentData = null;
        billSegmentReadData = null;
        billSegmentItemData = null;
        billSegmentServiceQuantityData = null;

    }

    /*
     * Initialize fields required for a Bill Segment
     * Populate data for Regular Billing, Invoice Billing and Trial Billing
     */
    private void initializeBillSegmentData() {
        if (billSegmentGeneratorData.getIterationCount().intValue() > 1) {
            BillSegment_DTO billSegmentDto = billSegmentData.getBillSegmentDto();
            billSegmentDto.setId(BillSegment_Id.NULL);
            BillSegment masterBillSegment = billSegmentGeneratorData.getMasterBillSegment();
            billSegmentDto.setMasterBillSegmentId(masterBillSegment != null ? masterBillSegment.getId() : null);
            Bill bill = billSegmentGeneratorData.getBill();
            billSegmentDto.setBillId(bill != null ? bill.getId() : null);
            billSegmentDto.setStartDate(billSegmentGeneratorData.getCutoffDate());
            billSegmentDto.setEndDate(billSegmentGeneratorData.getCutoffDate());
            billSegmentDto.setIsEstimate(Bool.FALSE);
            billSegmentDto.setIsClosingBillSegment(Bool.FALSE);
            billSegmentDto.setServiceAgreementId(billSegmentGeneratorData.getServiceAgreement().getId());
            billSegmentDto.setIsItemOverride(Bool.FALSE);
            billSegmentDto.setHasServiceQuantityOverride(Bool.FALSE);
            billSegmentDto.setReadSequenceId(null);
            billSegmentDto.setBillSegmentStatus(null);
            if (billSegmentDto.getCreationDateTime() == null) billSegmentDto.setCreationDateTime(getSystemDateTime());
            billSegmentDto.setStatusChangeDateTime(null);
            billSegmentDto.setCancelReasonId(null);
        }

        /*
         * For Regular Billing:
         *      - for Usage Account Bill Id is present in Cobol CopyBook
         *      - for Invoice Account Bill Id is needed to be fetched from CI_BILL table by sql query.
         *      - Invoice Account Details are also needed to be fetched by query.
         * For Trial Billing:
         *      - for Usage Account Bill Id is present in Cobol CopyBook
         *      - for Invoice Account Bill Id is needed to be fetched from CI_TRL_BILL table  by sql query.
         *      - Invoice Account Details are also needed to be fetched by query.
         */
        BillSegment_DTO billSegmentDto = billSegmentData.getBillSegmentDto();
        String invAcctId = billSegmentGeneratorData.getInvBillingAcctId();
        String saId = billSegmentGeneratorData.getServiceAgreement() != null ? billSegmentGeneratorData
                .getServiceAgreement().getId().getIdValue() : null;
                Date winStartDate = billSegmentGeneratorData.getWindowStartDate();
                Date winEndDate = billSegmentGeneratorData.getWindowEndDate();
                String targetSaCd = billSegmentGeneratorData.getTargetSaCharCd();

                Account acct = billSegmentGeneratorData.getAccount();
                String acctId = null;
                if (acct != null && acct.getId() != null)
                    acctId = acct.getId().getIdValue();

                if (!isBlankOrNull(invAcctId) && !invAcctId.equalsIgnoreCase(acctId)) // Invoice Account Processing
                    isInvoiceAcct = Bool.TRUE;

                if (isBlankOrNull(billSegmentGeneratorData.getTrialBillId())) { // Regular Billing
                    if (isInvoiceAcct == Bool.TRUE) { // Invoice Account
                        Bill_Id billId = fetchBillIdforInvoice(invAcctId, false);
                        billSegmentDto.setBillId(billId != null ? billId : null);

                        fetchInvoiceAccountDetails(invAcctId, saId, winStartDate, winEndDate, targetSaCd);

                    } else { // Usage Account
                        Bill bill = billSegmentGeneratorData.getBill();
                        Bill_Id billId = bill.getId();
                        billSegmentDto.setBillId(billId != null ? billId : null);
                    }
                }

                else { // Trial Billing
                    if (isInvoiceAcct == Bool.TRUE) { // Invoice Account
                        Bill_Id billId = fetchBillIdforInvoice(invAcctId, true);
                        billSegmentData.setTrialBillId(billId.getIdValue());

                        fetchInvoiceAccountDetails(invAcctId, saId, winStartDate, winEndDate, targetSaCd);

                    } else { // Usage Account
                        String trialBillId = billSegmentGeneratorData.getTrialBillId();
                        billSegmentData.setTrialBillId(trialBillId);
                    }
                }

    }

    /*
     * This method fetches Bill Id in Pending State for corresponding account number
     * from Bill or Trial Bill table.
     */
    private Bill_Id fetchBillIdforInvoice(String invAcctId, Boolean trialFlag) {
        String tableName;
        Bill_Id billId = null;

        if (trialFlag == true) {
        	tableName = CI_TRL_BILL;
        }
        else {
        	tableName = CI_BILL;
        }

        StringBuilder query = new StringBuilder();
        PreparedStatement fileStatement = null;
        try {
        	query.append(" SELECT BILL.BILL_ID from  ");
        	query.append(tableName);
        	query.append(" BILL where BILL.ACCT_ID = :acctId AND BILL.BILL_STAT_FLG = 'P' ");
        	fileStatement = createPreparedStatement(query.toString(), "getBillID");

        	fileStatement.bindString("acctId", invAcctId, ACCT_ID);
        	fileStatement.setAutoclose(false);

        	List<SQLResultRow> result = fileStatement.list();
        	String bId = null;

        	if (result != null && !result.isEmpty()) {
        		bId = result.get(0).getString(BILL_ID);
        		billId = new Bill_Id(bId);
        	}
        }
        finally {
        	if(fileStatement != null) {
        		fileStatement.close();
        	}
        }

        return billId;
    }

    /*
     * If Invoice account billing,
     * need to fetch details of invoice account from DB
     * and later sent to the cobol copybooks
     */
    private void fetchInvoiceAccountDetails(String invAcctId, String saId, Date winStartDate, Date winEndDate,
            String targetSaCd) {

        boolean billGenFlg = false;
        BillCycle bCycle = billSegmentGeneratorData.getBillCycle();
        String bCycCd = null;
        if (bCycle != null) {
            bCycCd = bCycle.getId().getTrimmedValue();
        }
        // if batch flow and bill cycle is present
        if (!isOnlineConnection() && !isBlankOrNull(bCycCd)) {
            billGenFlg = true;
        }
        
        StringBuilder query = new StringBuilder();
        PreparedStatement fileStatement = null;
        try {
        	query.append(" SELECT SA.SA_ID, SA.SA_TYPE_CD, SA.CIS_DIVISION, ACCT.CURRENCY_CD, ACCT.BILL_CYC_CD, BCYSCH.WIN_START_DT, BCYSCH.WIN_END_DT, BCYSCH.ACCOUNTING_DT, A.ALG_CD ");
        	query.append(" FROM CI_ACCT ACCT, CI_SA SA, CI_BILL_CYC_SCH BCYSCH, CI_CIS_DIV_ALG A ");
        	query.append(" WHERE ACCT.ACCT_ID = :acctId   AND SA.ACCT_ID = ACCT.ACCT_ID   AND TRIM(SA.SA_TYPE_CD) IN ");
        	query.append(" (SELECT TRIM(UPPER(C.ADHOC_CHAR_VAL)) FROM CI_SA B, CI_SA_TYPE_CHAR C WHERE B.SA_ID = :saId AND B.SA_TYPE_CD = C.SA_TYPE_CD AND trim(C.CHAR_TYPE_CD) = :targetSaCd) ");
        	query.append(" AND SA.SA_STATUS_FLG IN ('20', '30', '40', '50') AND ACCT.BILL_CYC_CD = BCYSCH.BILL_CYC_CD  ");

        	if (!billGenFlg) {
        		query.append(" (+) ");
        	}

        	if (!billGenFlg) {
        		if (winStartDate != null && winEndDate != null)
        			query.append(" AND WIN_START_DT <= :startDate AND WIN_END_DT >= :endDate");
        	} else {
        		query.append(" AND WIN_START_DT <= :startDate AND WIN_END_DT >= :endDate");
        	}
        	query.append(" AND A.CIS_DIVISION = ACCT.CIS_DIVISION AND A.DIV_ALG_ENTITY_FLG = 'PRAS' AND ROWNUM = 1  ");

        	fileStatement = createPreparedStatement(query.toString(), "getInvoiceAcctDetails");
        	fileStatement.bindId("acctId", new Account_Id(invAcctId));
        	fileStatement.bindId("saId", new ServiceAgreement_Id(saId));
        	if (!billGenFlg) {
        		if (winStartDate != null && winEndDate != null) {
        			fileStatement.bindDate("startDate", winStartDate);
        			fileStatement.bindDate("endDate", winEndDate);
        		}
        	} else {
        		fileStatement.bindDate("startDate", winStartDate);
        		fileStatement.bindDate("endDate", winEndDate);
        	}
        	fileStatement.bindString("targetSaCd", targetSaCd.trim(), TARGET_SA_CHAR_CD);
        	fileStatement.setAutoclose(false);

        	List<SQLResultRow> result = fileStatement.list();
        	if (result != null && !result.isEmpty()) {
        		SQLResultRow record = result.get(0);
        		String invSaId = record.getString(SA_ID);
        		String invSaTypeCd = record.getString(SA_TYPE_CD);
        		String billCycleCd = record.getString(BILL_CYC_CD);
        		String invCurrencyCd = record.getString(CURRENCY_CD);
        		String algCd = record.getString(ALG_CD);
        		Date winStartDt = record.getDate(WIN_START_DT);
        		Date winEndDt = record.getDate(WIN_END_DT);
        		Date accountingDt = record.getDate(ACCOUNTING_DT);
        		String cisDivision = record.getString(CIS_DIVISION);

        		BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();

        		bsegDTO.setServiceAgreementId(new ServiceAgreement_Id(invSaId));
        		BillCycle_Id billCycleId = new BillCycle_Id(billCycleCd);
        		//BillCycle billCycle = billCycleId.getEntity();
        		bsegDTO.setBillCycleScheduleId(new BillCycleSchedule_Id(billCycleId, winStartDt));

        		invoiceDataVO = new InvoiceDataVO();
        		invoiceDataVO.setAlgCd(algCd);
        		invoiceDataVO.setAccountingDt(accountingDt);
        		invoiceDataVO.setCisDivision(cisDivision);
        		invoiceDataVO.setCurrencyCd(invCurrencyCd);
        		invoiceDataVO.setSaTypeCd(invSaTypeCd);
        		invoiceDataVO.setWinEndDt(winEndDt);
        		invoiceDataVO.setWinStartDt(winStartDt);

        	} 
        	else {
        		billSegmentGeneratorData.setSkipBillChgSw(Bool.TRUE);
        		logger.debug("No result from Query..");
        	}
        }
        finally {
        	if(fileStatement!=null) {
        		fileStatement.close();
        	}
        }
    }

    private void initializeBillSegmentCollections() {
        mustRetainReads = mustRetainReads == null ? Bool.FALSE : mustRetainReads;
        mustRetainItems = mustRetainItems == null ? Bool.FALSE : mustRetainItems;
        mustRetainServiceQuantities = mustRetainServiceQuantities == null ? Bool.FALSE : mustRetainServiceQuantities;

        billSegmentCalculationHeaderData = new ArrayList<BillSegmentCalculationHeaderData>();
        billSegmentCalculationHeaderCount = BigInteger.ZERO;
        billSegmentCalculationLineCount = BigInteger.ZERO;
        billSegmentCalculationLineCharacteristicCount = BigInteger.ZERO;

        if (mustRetainReads == Bool.FALSE) billSegmentReadData = new ArrayList<BillSegmentReadData>();
        if (mustRetainItems == Bool.FALSE) billSegmentItemData = new ArrayList<BillSegmentItemData>();
        if (mustRetainServiceQuantities == Bool.FALSE)
            billSegmentServiceQuantityData = new ArrayList<BillSegmentServiceQuantityData>();
    }

    /**
     * this method determines billable charge to bill
     * @return
     */
    private BillableCharge determineBillableChargeToBill() {

        BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();
        BillSegment_Id cancelBsegId = bsegDTO.getCancelBillSegmentId();

        if ((billSegmentGeneratorData.getIsRebill().isTrue() && billSegmentGeneratorData.getCancelBillSegmentId() != null)
                || (notNull(bsegDTO.getId()) && notNull(cancelBsegId))) {

            String billPeriod = null;

            if (notNull(billSegmentGeneratorData.getCancelBillSegmentId())) {
                billSegmentStartDate = billSegmentGeneratorData.getCancelBillSegmentId().getStartDate();
                billSegmentEndDate = billSegmentGeneratorData.getCancelBillSegmentId().getEndDate();
            } else if (notNull(cancelBsegId)) {
                BillSegment cancelBseg = cancelBsegId.getEntity();
                billSegmentStartDate = cancelBseg.getStartDate();
                billSegmentEndDate = cancelBseg.getEndDate();
            }

            if (serviceAgreementEndDate != null && serviceAgreementEndDate.compareTo(billSegmentEndDate) <= 0) {
                billSegmentEndDate = serviceAgreementEndDate;
            }

            BillableCharge bCharge = determineRebillBillableChargeToBill();

            Date bChargeEndDate = bCharge.getEndDate();

            if (bChargeEndDate != null && bChargeEndDate.compareTo(billSegmentEndDate) <= 0) {
                billSegmentEndDate = bChargeEndDate;
            }
            if (billSegmentStartDate.compareTo(billSegmentEndDate) > 0) {
                reverseInitializeInputData();
                return null;
            }

            BillPeriod_Id bpId = bCharge.getBillPeriodId();
            if (null != bpId && null != bpId.getIdValue()) {
                billPeriod = bCharge.getBillPeriodId().getIdValue();
            }

            if (null != bCharge.getId()) {
                listBillableCharge.add(new BillableChargeWrapperObject(bCharge.getId(), true, billPeriod));
            }
            populateLastEndDateforBillableCharge();
            processListOfBillableCharge(listBillableCharge);

            return bCharge;

        }

        /*  Billable charge Id can come from online flow also.
         *   cases:
         *    - call from Bill Api based on GENERATE_BILL_CHG_SEGMENT
         *    - call from invoice account online billing
         *   hence useless condition removed
        /*        if (billSegmentGeneratorData.getBillableCharge() != null && SessionHolder.getSession().isOnlineConnection()) {
            mustExitIterationLoop = Bool.TRUE;
            return determineNewBillableChargeToBill();
        }*/

        // If charge is non recurring and call is through C1-BLGEN batch, set exitIteration=Y
        // cant do this for online connection and Billing batch as next call will not come and whole SA is skipped.

        //24763058 - ODB : ADHOC CHARGES BILL SEGMENT NOT GENERATED - start

        if (isBlankOrNull(billSegmentGeneratorData.getRecurringFlag())

                && !isNull(billSegmentGeneratorData) && !isNull(billSegmentGeneratorData.getBillableCharge())) {
            if (isNull(billSegmentGeneratorData.getBillableCharge().getRecurringFlg())) {

                mustExitIterationLoop = Bool.TRUE;
            }
        }

        //24763058 - ODB : ADHOC CHARGES BILL SEGMENT NOT GENERATED - end

        return determineNewBillableChargeToBill();
    }

    /**
     * This method determines which billable charges are to be rebilled
     * @return
     */
    private BillableCharge determineRebillBillableChargeToBill() {
        mustExitIterationLoop = Bool.TRUE;

        Query<BillableCharge_Id> query = createQuery(
                " from BillSegmentCalculationHeader as bsch where bsch.id.billSegment.id = :billSegment "
                        + " and @trim(bsch.billableChargeId) is not null ", "");
        query.bindEntity("billSegment", (billSegmentGeneratorData.getIsRebill().isTrue()) ? billSegmentGeneratorData
                .getCancelBillSegmentId() : billSegmentData.getBillSegmentDto().fetchCancelBillSegment());
        query.addResult("billableChargeId", "bsch.billableChargeId");
        query.selectDistinct(true);

        BigInteger count = BigInteger.ZERO;
        BillableCharge rebillBillableCharge = null;

        for (BillableCharge_Id billableChgId : query.list()) {
            count = count.add(BigInteger.ONE);
            if (count.intValue() > 1) break;
            rebillBillableCharge = billableChgId.getEntity();
        }
        if (count.intValue() != 1)
            addError(MessageRepository.billSegmentHasMultipleBillableCharges(billSegmentGeneratorData.getIsRebill()
                    .isTrue() ? billSegmentGeneratorData.getCancelBillSegmentId().getId().getTrimmedValue()
                            : billSegmentData.getBillSegmentDto().getCancelBillSegmentId().getTrimmedValue()));

        return rebillBillableCharge;
    }

    /*
     * This method fetches billable charges
     * from copybook or from DB
     * an sets the necessary flags.
     */
    private BillableCharge determineNewBillableChargeToBill() {

        BillableCharge newBillableCharge = fetchNewBillableChargeToBill();

        if (refContract != null && newBillableCharge == null
                && billSegmentGeneratorData.getIterationCount() != BigInteger.ONE) {
            exitLoopCheck = ExitIterationLoopLookup.constants.YES;
            mustSkipServiceAgreement = Bool.TRUE;
            skipSaReason = SkipSaReasonLookup.constants.NO_BILLABLE_CHARGES;
            return null;
        }
        if (newBillableCharge == null) {
            exitLoopCheck = ExitIterationLoopLookup.constants.YES;
            mustSkipServiceAgreement = Bool.TRUE;
            skipSaReason = SkipSaReasonLookup.constants.NO_BILLABLE_CHARGES;
        }

        //24763058 - ODB : ADHOC CHARGES BILL SEGMENT NOT GENERATED - commented below code as not able to generate multiple BS in a bill

        // If charge is non recurring and call is through C1-BLGEN batch, set exitIteration=Y
        // cant do this for online connection and Billing batch as next call will not come and whole SA is skipped.
        /* if (newBillableCharge != null
                 && (isNull(newBillableCharge.getRecurringFlg()) || isBlankOrNull(newBillableCharge.getRecurringFlg()
                         .trimmedValue()))) {
             mustExitIterationLoop = Bool.TRUE;
         }
         */
        return newBillableCharge;
    }

    /**
     * This method processes complete billing end date of billable charge
     * @param billPeriod_loc
     * @return
     */
    private Date processBillableChargeCompleteBillingEndDatesBillPeriod(String billPeriodVal) {

        Date cutoffDate = consumptionStartDate;
        Date bSegEndDate = null;
        Date upDatedCutoffDate = null;

        upDatedCutoffDate = cutoffDate.addDays(billLeadDays);

        StringBuilder query = new StringBuilder();
        PreparedStatement billableChargeStmt = null;
        try {
        	query.append("select k.cutoff_dt  from ci_bill_per_sch k ");
        	query.append(" where k.bill_period_cd = :billPeriodId ");
        	query.append(" and k.bill_dt = (select max(t.bill_dt)   from ci_bill_per_sch t ");
        	query.append(" where t.bill_period_cd = :billPeriodId ");
        	query.append("and t.bill_dt <= :upDatedCutoffDate ) order by k.cutoff_dt ");

        	billableChargeStmt = createPreparedStatement(query.toString(),
        			"Selects Maximum Segment End Date for a Bill Period");
        	billableChargeStmt.bindString("billPeriodId", billPeriodVal, "BILL_PERIOD_CD");
        	billableChargeStmt.bindDate("upDatedCutoffDate", upDatedCutoffDate);
        	billableChargeStmt.setAutoclose(false);

        	List<SQLResultRow> result = billableChargeStmt.list();
        	bSegEndDate = result.get(0).getDate("CUTOFF_DT");
        }
        finally {
        	if(billableChargeStmt != null) {
        		billableChargeStmt.close();
        	}
        }

        return bSegEndDate;

    }

    /**
     * @param startDate
     * @param endDate
     * @return
     *
     * This method returns the next billable charge to be processed. This method fetches list of recurring billable charges with their bill segment end dates and
     * list of non recurring billable charges. It returns the billable charge which has the invoice bill segment end date less than the billsegment end date. This way
     * all the billable charges will be processed and method will return null after it has gone through all the billable charges
     *
     * If the billable charge are present in the copybook, in case of 3 Step Charge based Billing batches.
     * It checks its eligibility and return the input billable charge without fetching charges from the DB.
     *
     * If the billable charge is not present in the copybook, in case of Billing Batch or Online bill generation.
     * It first fetches Regular billable charges from the DB one by one and generate segment for them.
     * After all regular charges are billed, it fetches all the recurring charges and cache them in a ConcurrentHashMap
     * to be used for further invocations of the algo and avoid the DB hits.
     */
    private BillableCharge fetchNewBillableChargeToBill() {

        StringBuilder bcQuery = null;
        String saId = billSegmentGeneratorData.getServiceAgreement().getId().getIdValue();

        Query<QueryResultRow> query = null;

        // AshishS - Performance changes

        if (billSegmentGeneratorData.getIterationCount().equals(new BigInteger("1")))
            BILLABLE_CHARGE_HASHMAP.remove(saId);

        // 3 Step Charge based batches
        // If Billable charge is passed through LSG copybook as Input
        if (billSegmentGeneratorData.getBillableCharge() != null) {
            processInputBillableCharge();
        }

        // Online or Billing Batch flow, SA based
        else {
            //  ------- Non Recurring start ------

            // Adhoc Charges Query
            if (billSegmentGeneratorData.getIsOffCycleBill().isTrue()) {
                bcQuery = new StringBuilder(" from BillableCharge as bc ");
                bcQuery.append(" where bc.serviceAgreement.id  = :serviceAgreement ");
                bcQuery
                .append(" and ((bc.startDate <= :cutoffDate and bc.billAfter is null) or (bc.billAfter<=:cutoffDate)) ");
                bcQuery.append(" and bc.billableChargeStatus = :billableChargeStatus ");
                bcQuery
                .append(" and (bc.recurringFlg =' ' or bc.recurringFlg is null) and  not exists (select bch.id.billSegment.id");
                bcQuery.append(" from BillSegmentCalculationHeader as bch, BillSegment as bs ");
                bcQuery.append(" where bc.serviceAgreement.id = bs.serviceAgreement.id");
                bcQuery.append(" and bch.id.billSegment.id = bs.id");
                bcQuery.append(" and bch.billableChargeId = bc.id");
                bcQuery.append(" and bs.billSegmentStatus <> :billSegmentStatus) ");
                bcQuery.append(" and bc.isAdhocBill = 'Y' ");

            }
            // Regular Charge Query
            else {
                bcQuery = new StringBuilder(" from BillableCharge as bc ");
                bcQuery.append(" where bc.serviceAgreement.id  = :serviceAgreement ");
                bcQuery
                .append(" and ((bc.startDate <= :cutoffDate and bc.billAfter is null) or (bc.billAfter<=:cutoffDate)) ");
                bcQuery.append(" and bc.billableChargeStatus = :billableChargeStatus ");
                bcQuery
                .append(" and (bc.recurringFlg =' ' or bc.recurringFlg is null) and  not exists (select bch.id.billSegment.id");
                bcQuery.append(" from BillSegmentCalculationHeader as bch, BillSegment as bs ");
                bcQuery.append(" where bc.serviceAgreement.id = bs.serviceAgreement.id");
                bcQuery.append(" and bch.id.billSegment.id = bs.id");
                bcQuery.append(" and bch.billableChargeId = bc.id");
                bcQuery.append(" and bs.billSegmentStatus <> :billSegmentStatus) and bc.isAdhocBill <> 'Y'");
            }

            if (isDebugLogEnabled)
                logger.info("Non Recurring billable charge query :- " + bcQuery.toString());

            query = createQuery(bcQuery.toString(), "NonRecurring");

            if (refServiceAgreement != null)
                query.bindEntity("serviceAgreement", refServiceAgreement);
            else
                query.bindEntity("serviceAgreement", billSegmentGeneratorData.getServiceAgreement());

            query.bindDate("cutoffDate", billSegmentGeneratorData.getCutoffDate().addDays(billLeadDays));
            query.bindLookup("billableChargeStatus", BillableChargeStatusLookup.constants.BILLABLE);
            query.bindLookup("billSegmentStatus", BillSegmentStatusLookup.constants.CANCELED);

            if (billSegmentGeneratorData.getBillableCharge() != null)
                query.bindId("billChgId", billSegmentGeneratorData.getBillableCharge().getId());

            query.addResult("bChargeId", "bc.id");
            query.addResult("billPeriodId", "bc.billPeriodId");

            query.setMaxResults(1);
            List<QueryResultRow> result = query.list();

            populateBillableChargeList(result);
            populateLastEndDateforBillableCharge();

            // -------- Non Recurring End ---------

            // All Non-Rec charges processing complete till here.

            // ------ Recurring charges Start -----
            if (result == null || result.size() == 0) {
                if (!BILLABLE_CHARGE_HASHMAP.containsKey(saId)) {

                    fetchRecurringBillableCharges(saId);
                }
                return processListOfBillableCharge(BILLABLE_CHARGE_HASHMAP.get(saId));
            }
        }

        return processListOfBillableCharge(listBillableCharge);

    }

    /**
     * This method process the Billable Charge Id which is
     * given as Input from the cobol copybook.
     * It is used for 3 step charge based batches
     * and populate billable charge list and also set
     * segment end date for last generated bill segment.
     *
     */

    private void processInputBillableCharge() {
        if (isDebugLogEnabled)
            logger.info("Billable charge from copybook: " + billSegmentGeneratorData.getBillableCharge());
        /*
         * Performance fix:
         *  - In the object billSegmentGeneratorData, complete billable charge object is present,
         *    so no need to query and fetch the object again.
         */
        /*
                StringBuilder billChgQuery = null;
                Query<QueryResultRow> query = null;

                billChgQuery = new StringBuilder(" from BillableCharge as bc where bc.id = :bChargeId ");
                query = createQuery(billChgQuery.toString(), "Recurring");
                query.bindId("bChargeId", billSegmentGeneratorData.getBillableCharge().getId());

                List<QueryResultRow> list = null;

                query.addResult("bChargeId", "bc.id");
                query.addResult("billPeriodId", "bc.billPeriodId");
                list = query.list();

                populateBillableChargeList(list);
         */

        BillableCharge_Id bChargeId = billSegmentGeneratorData.getBillableCharge().getId();
        BillPeriod_Id billPeriodVal = billSegmentGeneratorData.getBillableCharge().getBillPeriodId();
        String billPeriod = null;

        if (billPeriodVal != null)
            billPeriod = billPeriodVal.getTrimmedValue();

        BillableChargeWrapperObject billableChargeWrapperObject = new BillableChargeWrapperObject(
                bChargeId, true, billPeriod);

        listBillableCharge.add(billableChargeWrapperObject);

        populateLastEndDateforBillableCharge();

    }

    /**
     * This method fetches all the recurring billable charges present
     * on the processing contract and caches them into a Concurrent HashMap.
     * Thereby reducing the redundant DB calls each time a segment has to be processed.
     *
     * @param saId
     */

    /*
     * Updated for Bug 26388682 - NO BILL SEGMENTS EXIST FOR THIS BILL ERROR MESSAGE ON BILL GENERATION.
     * For  Contract Start Date 3rd Jan 2017, Cutoff Date 31st Jan 2017
     *
     */
    private void fetchRecurringBillableCharges(String saId) {

    	StringBuilder billChgQuery = new StringBuilder();
    	PreparedStatement preparedStatement = null;
    	try {
    		billChgQuery.append("Select BCHGS.Billable_Chg_Id, MAX(bsegs.end_dt) BSEG_Max_End_Dt, BCHGS.BILL_PERIOD_CD ");
    		billChgQuery.append("  FROM (SELECT BC.BILLABLE_CHG_ID, BC.BILL_PERIOD_CD ");
    		billChgQuery.append("          from CI_BILL_CHG bc ");
    		billChgQuery.append("         where bc.SA_ID = :saId ");
    		billChgQuery.append("           and bc.BILLABLE_CHG_STAT = :bcStatus ");
    		billChgQuery.append("           and bc.RECURRING_FLG != ' ' ");
    		billChgQuery.append("           and bc.RECURRING_FLG is not null ");
    		billChgQuery.append("           AND (bc.RECURRING_FLG != 'BP' AND ");

    		/*
    		 * Start Change - 2017-09-22 - VRom
    		 */
    		//billChgQuery.append("               bc.START_DT <= :cutoffDate OR ");
    		billChgQuery.append("               ((bc.START_DT <= :cutoffDate and bc.BILL_AFTER_DT is null) or (bc.BILL_AFTER_DT<=:cutoffDate)) OR ");
    		/*
    		 * End Change - 2017-09-22 - VRom
    		 */

    		billChgQuery.append("               (bc.RECURRING_FLG = 'BP' AND ");

    		/*
    		 * Start Add - 2017-09-22 - VRom
    		 */
    		billChgQuery.append("               (bc.BILL_AFTER_DT is null OR bc.BILL_AFTER_DT<=:cutoffDate) AND ");
    		/*
    		 * End Add - 2017-09-22 - VRom
    		 */

    		billChgQuery.append("               bc.START_DT <= ");
    		billChgQuery.append("               (SELECT bp.CUTOFF_DT ");
    		billChgQuery.append("                    FROM CI_BILL_PER_SCH bp ");
    		billChgQuery.append("                   WHERE bp.BILL_PERIOD_CD = bc.BILL_PERIOD_CD ");
    		billChgQuery.append("                     AND bp.BILL_DT = ");
    		billChgQuery.append("                         (SELECT MAX(bp2.BILL_DT) ");
    		billChgQuery.append("                            FROM CI_BILL_PER_SCH bp2 ");
    		billChgQuery.append("                           WHERE bp.BILL_PERIOD_CD = bp2.BILL_PERIOD_CD ");

    		//  billChgQuery.append("                              bp2.bill_dt        >= :saStartDt ");

    		billChgQuery
    		.append("                             AND  ( ( bp2.BILL_DT <= bp2.cutoff_dt and bp2.cutoff_dt >= :saStartDt ) ");
    		billChgQuery
    		.append("                             or ( bp2.BILL_DT > bp2.cutoff_dt and bp2.BILL_DT  >= :saStartDt) ) ");
    		billChgQuery.append("                                AND bp2.BILL_DT <= :cutoffDate ))))) BCHGS ");

    		billChgQuery.append("  left outer join (Select bsegCalcHdr.BILLABLE_CHG_ID, bseg.end_dt ");
    		billChgQuery.append("                     from CI_BSEG_CALC bsegCalcHdr, CI_BSEG bseg ");
    		billChgQuery.append("                    where bsegCalcHdr.BSEG_ID = bseg.BSEG_ID ");
    		billChgQuery.append("                      and bseg.BSEG_STAT_FLG <> :bsStatus1 ");
    		billChgQuery.append("                      and bseg.BSEG_STAT_FLG <> :bsStatus2) BSEGS ");
    		billChgQuery.append("  on BCHGS.BILLABLE_CHG_ID = BSEGS.BILLABLE_CHG_ID ");
    		billChgQuery.append("  GROUP BY BCHGS.Billable_Chg_Id, BCHGS.BILL_PERIOD_CD ");
    		billChgQuery.append("  order by BCHGS.Billable_Chg_Id");

    		if (isDebugLogEnabled)
    			logger.info("Recurring billable charge query :- " + billChgQuery.toString());

    		preparedStatement = createPreparedStatement(billChgQuery.toString(), "Recurring");

    		if (refContract != null) {
    			preparedStatement.bindString("saId", refContract, "serviceAgreement");
    			preparedStatement.bindDate("saStartDt", refServiceAgreement.getStartDate());
    		} else {
    			preparedStatement.bindString("saId", saId, "serviceAgreement");
    			preparedStatement.bindDate("saStartDt", billSegmentGeneratorData.getServiceAgreement()
    					.getStartDate());
    		}
    		preparedStatement
    		.bindDate("cutoffDate", billSegmentGeneratorData.getCutoffDate().addDays(billLeadDays));
    		preparedStatement.bindLookup("bcStatus", BillableChargeStatusLookup.constants.BILLABLE);
    		preparedStatement.bindLookup("bsStatus1", BillSegmentStatusLookup.constants.CANCELED);
    		preparedStatement.bindLookup("bsStatus2", BillSegmentStatusLookup.constants.PENDING_CANCEL);
    		preparedStatement.bindDate("saStartDt", billSegmentGeneratorData.getServiceAgreement()
    				.getStartDate());

    		preparedStatement.setAutoclose(false);
    		List<SQLResultRow> resultList = preparedStatement.list();
    		Iterator<SQLResultRow> itr = resultList.iterator();

    		SQLResultRow resultRow = null;
    		String bcId;
    		String bpCd;
    		Date bsegMaxEndDt;
    		BillableChargeWrapperObject bcWrapperObj;
    		List<BillableChargeWrapperObject> recBillableCharges = new ArrayList<BillableChargeWrapperObject>();

    		while (itr.hasNext()) {
    			resultRow = (SQLResultRow) itr.next();
    			bcId = resultRow.getString("BILLABLE_CHG_ID");
    			bpCd = resultRow.getString("BILL_PERIOD_CD");
    			bsegMaxEndDt = resultRow.getDate("BSEG_MAX_END_DT");

    			bcWrapperObj = new BillableChargeWrapperObject(new BillableCharge_Id(bcId), true, bpCd);
    			bcWrapperObj.setBsegEndDate(bsegMaxEndDt);

    			recBillableCharges.add(bcWrapperObj);
    		}

    		BILLABLE_CHARGE_HASHMAP.put(saId, recBillableCharges);

    	}
    	finally {
    		if(preparedStatement != null) {
    			preparedStatement.close();
    		}
    	}

        
    }

    /**
     * This method populates the list of billable charge wrapper objects based on the
     * data fetched from SQL query execution
     * @param result
     */
    private void populateBillableChargeList(List<QueryResultRow> result) {
        if (null != result) {

            for (int i = 0; i < result.size(); i++) {

                if (null != result.get(i)) {

                    String billPeriodVal = null;
                    BillableCharge_Id bChargeId = (BillableCharge_Id) result.get(i).get("bChargeId");
                    BillPeriod_Id billPeriodId = (BillPeriod_Id) result.get(i).get("billPeriodId");

                    if (null != billPeriodId) {
                        billPeriodVal = billPeriodId.getIdValue().trim();
                    }

                    BillableChargeWrapperObject billableChargeWrapperObject = new BillableChargeWrapperObject(
                            bChargeId, true, billPeriodVal);
                    //billableChargeWrapperObject.setBillPeriod(billPeriodVal);

                    listBillableCharge.add(billableChargeWrapperObject);

                }

            }

        }
    }

    /**
     * This method populates the last end date for a particular bill segment
     */
    private void populateLastEndDateforBillableCharge() {
        String trialBillId = billSegmentGeneratorData.getTrialBillId();
        List<QueryResultRow> queryList = null;

        for (BillableChargeWrapperObject bchwo : listBillableCharge) {

            BillableCharge_Id bChargeId = bchwo.getBchargeId();

            if (isDebugLogEnabled)
                logger.info("Billable charge ID:- " + bChargeId.getIdValue());

            StringBuilder query = null;

            query = new StringBuilder(" from BillSegmentCalculationHeader as bsegCalcHdr,BillSegment as bseg ");
            query.append(" where bsegCalcHdr.billableChargeId  = :bcId  and bsegCalcHdr.id.billSegment=bseg.id and ");
            query
            .append(" bseg.billSegmentStatus <> :billSegmentStatus and bseg.billSegmentStatus <> :billSegmentStatus2 ");
            Query<QueryResultRow> query2 = createQuery(query.toString(), "");

            query2.bindId("bcId", bChargeId);
            query2.bindLookup("billSegmentStatus", BillSegmentStatusLookup.constants.CANCELED);
            query2.bindLookup("billSegmentStatus2", BillSegmentStatusLookup.constants.PENDING_CANCEL);
            query2.addResult("enddate", "max(bseg.endDate)");
            query2.addResult("bChargeId", "bsegCalcHdr.billableChargeId");
            query2.groupBy("bChargeId");
            queryList = query2.list();
            if (queryList != null && queryList.size() != 0) {

                for (QueryResultRow resultRow : queryList) {
                    Date date = resultRow.getDate("enddate");
                    bchwo.setBsegEndDate(date);
                }
            }

            if (trialBillId != null && !trialBillId.isEmpty()) {
                // check for bill segments present in trial bill segment calculation

                query = new StringBuilder(
                        " from TrialBillSegmentCalculationHeader as trialBsegCalcHdr,TrialBillSegment as trlBseg ");
                query
                .append(" where trialBsegCalcHdr.billableChargeId  = :bcId  and trialBsegCalcHdr.id.billSegment=trlBseg.id and ");
                query
                .append(" trlBseg.billSegmentStatus <> :billSegmentStatus and trlBseg.billSegmentStatus <> :billSegmentStatus2 and trlBseg.billId= :trialBillId ");
                Query<QueryResultRow> query3 = createQuery(query.toString(), "");

                query3.bindId("bcId", bChargeId);
                query3.bindLookup("billSegmentStatus", BillSegmentStatusLookup.constants.CANCELED);
                query3.bindLookup("billSegmentStatus2", BillSegmentStatusLookup.constants.PENDING_CANCEL);
                query3.bindId("trialBillId", new TrialBill_Id(trialBillId));
                query3.addResult("enddate", "max(trlBseg.endDate)");
                query3.addResult("bChargeId", "trialBsegCalcHdr.billableChargeId");
                query3.groupBy("bChargeId");
                List<QueryResultRow> queryListForTrial = query3.list();

                if (queryList != null && queryList.size() != 0) {
                    if (queryListForTrial != null && queryListForTrial.size() != 0) {
                        queryList.addAll(queryListForTrial);

                        for (QueryResultRow resultRow : queryList) {
                            Date date = resultRow.getDate("enddate");
                            bchwo.setBsegEndDate(date);
                        }
                    }

                } else {
                    if (queryListForTrial != null && queryListForTrial.size() != 0) {
                        for (QueryResultRow resultRow : queryListForTrial) {
                            Date date = resultRow.getDate("enddate");
                            bchwo.setBsegEndDate(date);
                        }
                    }
                }

            }

        }
    }

    /**
     * @returns
     *
     * This method processes list of billable charges and sets start date/end date and if proration is required or not
     * It checks the eligibility of a charge whether it has to be processed for this date range or not.
     * It also checks eligibility of a charge based on its recurring flag - Bill Period or Frequency.
     * It returns a billable charge to be processed and returns null if no charge is left to be proecssed on the contract.
     *
     * Updated for Bug :  Updated for- Proration Start date should be Bill Period Start Not a contract/Billsegment Start Date.
     * @author vwakade
     *
     */
    private BillableCharge processListOfBillableCharge(List<BillableChargeWrapperObject> billableCharges) {

        for (BillableChargeWrapperObject bchwo : billableCharges) {

            if (bchwo.getIsProcessed() == true)
                continue;

            BillableCharge bCharge = bchwo.getBchargeId().getEntity();
            RecurringFlgLookup recurFlg = bCharge.getRecurringFlg();

            billChrgStaus = bchwo.getBchargeId().getEntity().getBillableChargeStatus();
            if (billChrgStaus.equals(BillableChargeStatusLookup.constants.CANCELED)) {
                return null;
            }

            //returns a non recurring billable charge
            if (recurFlg == null || recurFlg.isBlankLookupValue()) {

                //set start date and end date

                billSegmentStartDate = bCharge.getStartDate();
                billSegmentEndDate = bCharge.getEndDate();

                return bCharge;
            }
            if (recurFlg.compareTo(RecurringFlgLookup.constants.BILL_PERIOD) == 0) {

                String billPeriodId = bchwo.getBillPeriod();

                Date finalbillSegmentEndDate = processBillableChargeCompleteBillingEndDatesBillPeriod(billPeriodId);
                if (finalbillSegmentEndDate.compareTo(bCharge.getEndDate()) > 0) {
                    finalbillSegmentEndDate = bCharge.getEndDate();
                }

                Date lastbillSegmentEndDate = bchwo.getBsegEndDate();
                prorationRequiredStartDate = true;
                if (lastbillSegmentEndDate == null) {
                    //set start date and set end date

                    if (bCharge.getStartDate().compareTo(serviceAgreementStartDate) > 0) {

                        //If BS start date is > SA start date then proration start date should be SA start date.
                        billSegmentStartDate = bCharge.getStartDate();
                        //  prorationStartDate = serviceAgreementStartDate;

                    } else {

                        //If BS start date is equal to SA start date then proration start date should be BS start date.
                        billSegmentStartDate = serviceAgreementStartDate;
                        // prorationStartDate = billSegmentStartDate;
                    }
                    
                    /*
                     * Start Add - 2017-09-22 - VRom
                     */
                    //Use Bill After Date as start date if specified
                    if(notNull(bCharge.getBillAfter()) && bCharge.getBillAfter().compareTo(billSegmentStartDate) > 0) {
                    	billSegmentStartDate = bCharge.getBillAfter();
                    }
                    /*
                     * End Add - 2017-09-22 - VRom
                     */

                    //Updated for- Proration Start date should be Bill Period Start Not a contract/Billsegment Start Date. Start
                    StringBuilder bPQueryString = new StringBuilder();
                    bPQueryString.append("SELECT  MAX(BILL_DT) AS MAX_BILL_DT FROM CI_BILL_PER_SCH bp ");
                    bPQueryString.append("WHERE bp.BILL_PERIOD_CD= :billPeriodId ");
                    bPQueryString.append("AND bp.BILL_DT   <= :billSegmentStartDate ");
                    PreparedStatement retrieveMaxBpDate = null;
                    try {

                        retrieveMaxBpDate = createPreparedStatement(bPQueryString.toString(),
                                "retrieve Max Bill Period");
                        retrieveMaxBpDate.bindId("billPeriodId", new BillPeriod_Id(bchwo.getBillPeriod()));
                        retrieveMaxBpDate.bindDate("billSegmentStartDate", billSegmentStartDate);
                        retrieveMaxBpDate.setAutoclose(false);
                        List<SQLResultRow> resultList = retrieveMaxBpDate.list();
                        int size = resultList.size();
                        if (size > 0) {
                            SQLResultRow resultData = resultList.get(0);
                            prorationStartDate = resultData.getDate("MAX_BILL_DT");
                        }

                    } finally
                    {
                        if (retrieveMaxBpDate != null)
                            retrieveMaxBpDate.close();
                    }
                    //end

                } else if (lastbillSegmentEndDate.compareTo(finalbillSegmentEndDate) < 0) {

                    billSegmentStartDate = lastbillSegmentEndDate.addDays(1);
                    // prorationStartDate = billSegmentStartDate; //no need ProrationEnd Date will consider for Last bill segment.

                } else {
                    // if all the bill segments are genareted for a billable charge skip to the next billable charge
                    bchwo.setIsProcessed(true);
                    continue;
                }

                Date scheduleEndDate = null;

                StringBuilder queryString = new StringBuilder();
                queryString.append("SELECT CUTOFF_DT FROM CI_BILL_PER_SCH ");
                queryString.append("WHERE BILL_PERIOD_CD =:billPeriodId ");
                queryString.append("AND BILL_DT <= :cutOffDt ");
                queryString.append("AND CUTOFF_DT >= :startDt ");
                queryString.append(" order by CUTOFF_DT ");

                PreparedStatement retrieveBillPeriodSchedule = null;
                try {
                    retrieveBillPeriodSchedule = createPreparedStatement(queryString.toString(),
                            "retrieveBillPeriodSchedule");

                    retrieveBillPeriodSchedule.bindId("billPeriodId", new BillPeriod_Id(bchwo.getBillPeriod()));
                    retrieveBillPeriodSchedule.bindDate("cutOffDt", billSegmentGeneratorData.getCutoffDate());
                    if (bchwo.getBsegEndDate() != null)
                        retrieveBillPeriodSchedule.bindDate("startDt", bchwo.getBsegEndDate());
                    else
                        retrieveBillPeriodSchedule.bindDate("startDt", billSegmentGeneratorData.getServiceAgreement()
                                .getStartDate());

                    retrieveBillPeriodSchedule.setAutoclose(false);

                    List<SQLResultRow> billPeriodList = retrieveBillPeriodSchedule.list();

                    for (SQLResultRow row : billPeriodList) {

                        if (prorationRequiredStartDate && scheduleEndDate != null) {
                            prorationStartDate = scheduleEndDate.addDays(1);
                        }

                        scheduleEndDate = row.getDate("CUTOFF_DT");

                        if (scheduleEndDate != null) {

                            if ((scheduleEndDate.compareTo(finalbillSegmentEndDate) <= 0 || finalbillSegmentEndDate
                                    .compareTo(bCharge.getEndDate()) == 0)
                                    && scheduleEndDate.compareTo(billSegmentStartDate) >= 0) {
                                billSegmentEndDate = scheduleEndDate;

                                if (billSegmentEndDate.compareTo(bCharge.getEndDate()) >= 0) {
                                    prorationEndDate = billSegmentEndDate;
                                    billSegmentEndDate = bCharge.getEndDate();
                                    prorationRequiredEndDate = true;

                                }

                                //If contract end date >= BS end date i.e. last BS

                                else if (serviceAgreementEndDate != null
                                        && billSegmentEndDate.compareTo(serviceAgreementEndDate) >= 0) {
                                    prorationEndDate = billSegmentEndDate;
                                    billSegmentEndDate = serviceAgreementEndDate;
                                    prorationRequiredEndDate = true;
                                }
                                if (billSegmentEndDate.compareTo(finalbillSegmentEndDate) == 0
                                        && (!isNull(billSegmentGeneratorData) && !isNull(billSegmentGeneratorData
                                                .getBillableCharge()))) {
                                    mustSkipIteration = Bool.TRUE;
                                }

                                if (billSegmentEndDate.compareTo(billSegmentStartDate) > -1) {
                                    bchwo.setBsegEndDate(billSegmentEndDate);
                                    return bCharge;
                                }
                            }
                        }
                    }
                } finally {
                    if (retrieveBillPeriodSchedule != null)
                        retrieveBillPeriodSchedule.close();
                }
            }

            else if (recurFlg.compareTo(RecurringFlgLookup.constants.FREQUENCY) == 0) {

                PolicyInvoiceFrequency policyFreq = bCharge.fetchPolInvcFreqCd();
                BigInteger days = policyFreq.getDailyFreq();
                BigInteger months = policyFreq.getMnthlyFreq();
                Date billSegEndDate = bchwo.getBsegEndDate();
                //would happen for rebilling  on contract termination and when lot of bills
                //                if (billSegEndDate != null && billSegEndDate.compareTo(bCharge.getEndDate()) > 0) {
                //                    billSegEndDate = null;
                //                }

                if (billSegEndDate != null && billSegEndDate.compareTo(bCharge.getEndDate()) > 0) {
                    return null;
                }

                Date cutoffDate = consumptionStartDate;
                Date upDatedCutoffDate = cutoffDate.addDays(billLeadDays);

                if (billSegEndDate == null || upDatedCutoffDate.compareTo(billSegEndDate) > 0) {
                    if (billSegEndDate == null) {

                        if (bCharge.getStartDate().compareTo(serviceAgreementStartDate) > 0) {
                            billSegmentStartDate = bCharge.getStartDate();
                        } else {
                            billSegmentStartDate = serviceAgreementStartDate;
                        }
                        prorationRequiredStartDate = true;
                        prorationStartDate = billSegmentStartDate;
                    } else {
                        billSegmentStartDate = billSegEndDate.addDays(1);
                    }
                    billSegmentEndDate = billSegmentStartDate;
                    /*
                     * Start Delete - 2017-09-22 - VRom
                     */
                    //nextBillSegmentEndDate = billSegmentEndDate;
                    /*
                     * End Delete - 2017-09-22 - VRom
                     */

                    if (days.intValue() > 0) {
                        billSegmentEndDate = billSegmentEndDate.addDays(days.intValue()).addDays(-1);
                        /*
                         * Start Delete - 2017-09-22 - VRom
                         */
                        //nextBillSegmentEndDate = billSegmentEndDate.addDays(days.intValue()).addDays(-1);
                        /*
                         * End Delete - 2017-09-22 - VRom
                         */
                    }
                    if (months.intValue() > 0) {
                        billSegmentEndDate = billSegmentEndDate.addMonths(months.intValue()).addDays(-1);
                        /*
                         * Start Delete - 2017-09-22 - VRom
                         */
                        //nextBillSegmentEndDate = billSegmentEndDate.addMonths(months.intValue()).addDays(-1);
                        /*
                         * End Delete - 2017-09-22 - VRom
                         */
                    }

                    // for freq, segment of cutoff-date month will not be generated. unlike  BP
                    // if C1-BLGEN batch, mark exitIteration=Y, next call will come for next Bchg
                    if (!isNull(billSegmentGeneratorData) && !isNull(billSegmentGeneratorData
                            .getBillableCharge())) {
                        // Removing cutoff date check - bug    25208300 - START
                        //if (billSegmentEndDate.compareTo(upDatedCutoffDate) > 0)
                        //    continue;

                        //if (nextBillSegmentEndDate.compareTo(upDatedCutoffDate) > 0)
                        //    mustSkipIteration = Bool.TRUE;
                    }

                    // else if Online or Billing Batch call, can NOT mark exitIteration=Y as call will not come again.
                    // simply bypass this charge, if its a segment of  cutoff-date month.
                    //else if (billSegmentEndDate.compareTo(upDatedCutoffDate) > 0) {
                    //    bchwo.setIsProcessed(true);
                    //    continue;
                    //}

                    // Removing cutoff date check - bug    25208300 - END

                    if (billSegmentEndDate.compareTo(bCharge.getEndDate()) > 0) {
                        prorationEndDate = billSegmentEndDate;
                        billSegmentEndDate = bCharge.getEndDate();
                        prorationRequiredEndDate = true;

                    } else if (serviceAgreementEndDate != null
                            && billSegmentEndDate.compareTo(serviceAgreementEndDate) >= 0) {
                        prorationEndDate = billSegmentEndDate;
                        billSegmentEndDate = serviceAgreementEndDate;
                        prorationRequiredEndDate = true;

                    }
                    if (billSegmentEndDate.compareTo(billSegmentStartDate) >= 0) {
                        bchwo.setBsegEndDate(billSegmentEndDate);
                        return bCharge;
                    }

                }
            }
        }

        return null;

    }

    /**
     * Generate bill segments for billable charges.
     * First process the pass thru charges ie:Billable Charge Lines and then process the SQIs on billable charges
     */
    private void generateBillableChargeBillSegment(RateSchedule priceAssignRateSchedule) {
        generateChargesForBillableChargeLines();
        if (mustRetainReads.isFalse()) generateChargesForBillableChargeReads();
        generateChargesForBillableChargeServiceQuantities(priceAssignRateSchedule);
        checkForBillSegmentCalculationHeader();
        if (billableChargeLinesExist.isFalse() && billSegmentServiceQuantityData.isEmpty())
            addError(MessageRepository.noLinesOrSqOnBillableCharge(billableCharge.getId().getTrimmedValue()));

        if (saType.getIsRateRequired().isFalse() && billableChargeLinesExist.isFalse()
                && billableCharge.getPriceItemCodeId() == null)
            addError(MessageRepository.noLinesOnBillableChargeAndRateNotAllowedOnSaType(billableCharge.getId()
                    .getTrimmedValue(), saType.getId().getDivision().getId().getTrimmedValue(), saType.getId()
                    .getSaType()));
    }

    /*
     * Process pass through lines in billable charge
     */
    private void generateChargesForBillableChargeLines() {
        List<QueryResultRow> billableChargeLines = fetchBillableChargeLines();
        if (!billableChargeLines.isEmpty()) billableChargeLinesExist = Bool.TRUE;
        for (QueryResultRow row : billableChargeLines) {
            generateChargeForBillableChargeLine((BillableChargeLine) row.get("bcl"));
        }
        BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();
        consumptionStartDate = bsegDTO.getStartDate();
        consumptionEndDate = bsegDTO.getEndDate();
    }

    /**
     * @return
     *
     * This method fetches billable charge lines for a particular billable charge
     */
    private List<QueryResultRow> fetchBillableChargeLines() {
        StringBuilder sbQuery = new StringBuilder("from BillableChargeLine as bcl ");
        sbQuery.append(" where bcl.id.billableCharge.id  = :billableCharge  ");
        Query<QueryResultRow> query = createQuery(sbQuery.toString(), "");
        query.bindEntity("billableCharge", billableCharge);
        query.addResult("bcl", "bcl");
        query.addResult("sequence", "bcl.id.lineSequence");
        query.orderBy("sequence");
        return query.list();
    }

    /**
     * @param billableChargeLines
     *
     * This method generate charges for pass through lines
     */
    private void generateChargeForBillableChargeLine(BillableChargeLine billableChargeLine) {
        if (billSegmentCalculationLineCount.intValue() >= CobolConstants.CI_CONST_BI_MAX_CALC_LINE_COLL) {
            checkForBillSegmentCalculationHeader();
            addError(MessageRepository.calcLineCountLimitReached());
        }
        billSegmentCalculationLineCount = billSegmentCalculationLineCount.add(BigInteger.ONE);
        if (billSegmentCalculationLineCount == BigInteger.ONE)
            createBillSegmentCalculationHeader(billableChargeLine, null);
        BigDecimal newAmount = createBillSegmentCalculationLine(billableChargeLine);
        if (billableChargeLine.getIsMemoOnly().isFalse()) {
            if (billableChargeLine.getDistributionCodeId() == null) {
                checkForBillSegmentCalculationHeader();
                addError(MessageRepository.distributionIdMissing());
            }
            //addChargeAmountToHeader(billableChargeLine.getChargeAmount());
            /*
             * Start Change - 2017-09-22 - VRom
             */
            //Money newChargeAmount = new Money(newAmount);
            Money newChargeAmount = new Money(newAmount, billableChargeLine.getCurrencyId());
            /*
             * End Change - 2017-09-22 - VRom
             */
            addChargeAmountToHeader(newChargeAmount);
            copyBillableChargeLineCharacteristics(billableChargeLine);

        }

        addBillableChargeLineCharacteristics(billableChargeLine);
    }

    /**
     * This method generates charges for billable charge reads
     */
    private void generateChargesForBillableChargeReads() {
        List<QueryResultRow> billableChargeReads = fetchBillableChargeReads();
        for (QueryResultRow row : billableChargeReads) {
            BillableChargeRead billableChargeRead = (BillableChargeRead) row.get("bcr");
            if (billableChargeRead == null) continue;
            createBillSegmentReadData(billableChargeRead);
        }
    }

    /**
     * @return
     */
    private List<QueryResultRow> fetchBillableChargeReads() {
        Query<QueryResultRow> query = createQuery(" from BillableChargeRead as bcr where bcr.id.billableCharge.id = :billableCharge ", "");
        query.bindEntity("billableCharge", billableCharge);
        query.addResult("bcr", "bcr");
        query.addResult("sp", "bcr.id.servicePointIdString");
        query.addResult("sequence", "bcr.id.sequence");
        query.orderBy("sp");
        query.orderBy("sequence");
        return query.list();
    }

    /**
     * @param billableChargeRead
     */
    private void createBillSegmentReadData(BillableChargeRead billableChargeRead) {

        BillSegmentRegisterRead_DTO billSegmentRegisterReadDto = createDTO(BillSegmentRegisterRead.class);
        if (billableChargeRead.getId() != null)
            billSegmentRegisterReadDto.setId(new BillSegmentRegisterRead_Id(BillSegment_Id.NULL, billableChargeRead
                    .getId().getServicePointIdString(), billableChargeRead.getId().getSequence()));
        billSegmentRegisterReadDto.setRegisterConstant(billableChargeRead.getRegisterConstant());
        billSegmentRegisterReadDto.setHowToUseRead(billableChargeRead.getHowToUseRead());
        billSegmentRegisterReadDto.setUsePercent(billableChargeRead.getUsePercent());
        billSegmentRegisterReadDto.setHowToUse(billableChargeRead.getHowToUse());
        billSegmentRegisterReadDto.setMeasuresPeakQuantity(billableChargeRead.getMeasuresPeakQuantity());
        billSegmentRegisterReadDto.setUnitOfMeasureId(billableChargeRead.getUnitOfMeasureId());
        billSegmentRegisterReadDto.setTimeOfUseId(billableChargeRead.getTimeOfUseId());
        billSegmentRegisterReadDto.setServiceQuantityIdentifierId(billableChargeRead.getServiceQuantityIdentifierId());
        billSegmentRegisterReadDto.setStartRegisterReadId(billableChargeRead.getStartRegisterReadId());
        billSegmentRegisterReadDto.setStartReadDateTime(billableChargeRead.getStartReadDateTime());
        billSegmentRegisterReadDto.setStartRegisterReading(billableChargeRead.getStartRegisterReading());
        billSegmentRegisterReadDto.setEndRegisterReadingId(billableChargeRead.getEndRegisterReadingId());
        billSegmentRegisterReadDto.setEndReadDateTime(billableChargeRead.getEndReadDateTime());
        billSegmentRegisterReadDto.setEndRegisterReading(billableChargeRead.getEndRegisterReading());
        billSegmentRegisterReadDto.setMeasuredQuantity(billableChargeRead.getMeasuredQuantity());
        billSegmentRegisterReadDto.setFinalUnitOfMeasureId(billableChargeRead.getFinalUnitOfMeasureId());
        billSegmentRegisterReadDto.setFinalTimeOfUseId(billableChargeRead.getFinalTimeOfUseId());
        billSegmentRegisterReadDto.setRegisterQuantity(billableChargeRead.getRegisterQuantity());
        billSegmentRegisterReadDto.setFinalServiceQuantityId(billableChargeRead.getFinalServiceQuantityIdentifierId());

        BillSegmentReadData billSegmentReadRecord = BillSegmentReadData.Factory.newInstance();
        billSegmentReadRecord.setBillSegmentReadDto(billSegmentRegisterReadDto);
        billSegmentReadData.add(billSegmentReadRecord);
    }

    /**
     * @return
     */
    private List<QueryResultRow> fetchBillableChargeServiceQuantities() {
        Query<QueryResultRow> query = createQuery(" from BillableChargeServiceQuantity as bcsq where bcsq.id.billableCharge.id = :billableCharge", "");
        query.bindEntity("billableCharge", billableCharge);
        query.addResult("bcsq", "bcsq");
        query.addResult("sequence", "bcsq.id.sequence");
        query.orderBy("sequence");
        return query.list();
    }

    private void generateChargeForBillableChargeServiceQuantity(
            BillableChargeServiceQuantity billableChargeServiceQuantity) {
        BillSegmentServiceQuantity_DTO billSegmentServiceQuantityDto = createDTO(BillSegmentServiceQuantity.class);
        billSegmentServiceQuantityDto.setId(new BillSegmentServiceQuantity_Id(BillSegment_Id.NULL,
                billableChargeServiceQuantity.fetchUnitOfMeasure() == null ? null : billableChargeServiceQuantity
                        .getUnitOfMeasureId().getTrimmedValue(),
                        billableChargeServiceQuantity.fetchTimeOfUse() == null ? null : billableChargeServiceQuantity
                                .getTimeOfUseId().getTrimmedValue(), billableChargeServiceQuantity
                                .fetchServiceQuantityIdentifier() == null ? null : billableChargeServiceQuantity
                                        .getServiceQuantityIdentifierId().getTrimmedValue()));
        billSegmentServiceQuantityDto
        .setInitialServiceQuantity(billableChargeServiceQuantity.getDailyServiceQuantity());
        billSegmentServiceQuantityDto.setBillableServiceQuantity(billableChargeServiceQuantity
                .getDailyServiceQuantity());

        BillSegmentServiceQuantityData billSegmentServiceQuantityRecord = BillSegmentServiceQuantityData.Factory
                .newInstance();
        billSegmentServiceQuantityRecord.setBillSegmentServiceQuantityDto(billSegmentServiceQuantityDto);
        billSegmentServiceQuantityData.add(billSegmentServiceQuantityRecord);
    }

    /**
     * @param priceAsignmentRateSchedule
     * Handle the SQs in billable charge and generate billsegments based on rate
     *
     */
    private void generateChargesForBillableChargeServiceQuantitiesUsingRate(RateSchedule priceAsignmentRateSchedule) {

        ApplyRateData applyRateData = prepareApplyRateData(priceAsignmentRateSchedule);
        RateApplicationProcessorData rateApplicationProcessorData = RateApplicationProcessorData.Factory.newInstance();
        rateApplicationProcessorData.setApplyRateData(applyRateData);
        rateApplicationProcessorData.setCalculationHeaderDataList(billSegmentCalculationHeaderData);
        rateApplicationProcessorData.setItemDataList(billSegmentItemData);
        rateApplicationProcessorData.setReadDataList(billSegmentReadData);
        rateApplicationProcessorData.setServiceQuantityDataList(billSegmentServiceQuantityData);
        rateApplicationProcessorData.setCharacteristicDataList(buildCharDataList());

        //and set characteristics for pricing
        applyRate(rateApplicationProcessorData);

    }

    /**
     * @return
     *
     * Build characteristic data list which is passed to rate application
     *
     */
    private List<CharacteristicData> buildCharDataList() {
        List<CharacteristicData> charDataList = new ArrayList<CharacteristicData>();
        CharacteristicData person = CharacteristicData.Factory.newInstance();
        person.setCharacteristicType(new CharacteristicType_Id(charTypePerson).getEntity());
        person.setCharacteristicValue(billSegmentPricingPerson);
        charDataList.add(person);

        CharacteristicData account = CharacteristicData.Factory.newInstance();
        account.setCharacteristicType(new CharacteristicType_Id(charTypeAccount).getEntity());
        account.setCharacteristicValue(billSegmentPricingAccount);
        charDataList.add(account);

        CharacteristicData priceList = CharacteristicData.Factory.newInstance();
        priceList.setCharacteristicType(new CharacteristicType_Id(charTypePriceList).getEntity());
        priceList.setCharacteristicValue(billSegmentPricingPriceList);
        charDataList.add(priceList);

        CharacteristicData priceAsgnId = CharacteristicData.Factory.newInstance();
        priceAsgnId.setCharacteristicType(new CharacteristicType_Id(charTypePriceAsgnId).getEntity());
        priceAsgnId.setCharacteristicValue(billSegmentPricingPriceAsgnId);
        charDataList.add(priceAsgnId);

        CharacteristicData chargeType = CharacteristicData.Factory.newInstance();
        chargeType.setCharacteristicType(new CharacteristicType_Id(chargeTypeCharType).getEntity());
        if (billableCharge.getChrgTypCdId() != null) {
            chargeType.setCharacteristicValue(billableCharge.getChrgTypCdId().getIdValue());
            charDataList.add(chargeType);
        }

        CharacteristicData cisDivision = CharacteristicData.Factory.newInstance();
        cisDivision.setCharacteristicType(new CharacteristicType_Id(divisionCharType).getEntity());
        cisDivision.setCharacteristicValue(serviceAgreement.getServiceAgreementType().fetchIdDivision().getId()
                .getIdValue());
        charDataList.add(cisDivision);

        CharacteristicData saType = CharacteristicData.Factory.newInstance();
        saType.setCharacteristicType(new CharacteristicType_Id(saTypeCharType).getEntity());
        saType.setCharacteristicValue(serviceAgreement.getServiceAgreementType().fetchIdSaType());
        charDataList.add(saType);

        String trialBillId = billSegmentGeneratorData.getTrialBillId();

        if (charTypeTrialBill != null && charTypeTrialBill.length() != 0) {
            CharacteristicData trialBill = CharacteristicData.Factory.newInstance();
            trialBill.setCharacteristicType(new CharacteristicType_Id(charTypeTrialBill).getEntity());
            trialBill.setCharacteristicValue(trialBillId);
            charDataList.add(trialBill);
        }

        if (billableCharge != null) {
            CharacteristicData billableChargeObj = CharacteristicData.Factory.newInstance();
            billableChargeObj.setCharacteristicType(new CharacteristicType_Id(billableChargeCharType).getEntity());
            billableChargeObj.setCharacteristicValue(billableCharge.getId().getIdValue());
            charDataList.add(billableChargeObj);
        }
        if (prorationRequiredStartDate && prorationStartDate != null) {
            CharacteristicData psDate = CharacteristicData.Factory.newInstance();
            psDate.setCharacteristicType(new CharacteristicType_Id(prorationStartDateCharType).getEntity());
            psDate.setCharacteristicValue(prorationStartDate.toString(new DateFormat("yyyy-MM-dd")));
            charDataList.add(psDate);
        }

        if (prorationRequiredEndDate && prorationEndDate != null) {
            CharacteristicData peDate = CharacteristicData.Factory.newInstance();
            peDate.setCharacteristicType(new CharacteristicType_Id(prorationEndDateCharType).getEntity());
            peDate.setCharacteristicValue(prorationEndDate.toString(new DateFormat("yyyy-MM-dd")));
            charDataList.add(peDate);

        }

        return charDataList;
    }

    /**
     * @param pricingRateSchedule
     * @return ApplyRateData
     * This method populates the ApplyRateData object which is used by rate application processor data
     *
     */
    private ApplyRateData prepareApplyRateData(RateSchedule pricingRateSchedule) {

        BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();
        ServiceAgreementRateScheduleData rateScheduleInformation = getRateScheduleInformation();
        ApplyRateData applyRateData = ApplyRateData.Factory.newInstance();
        applyRateData.setServiceAgreement(bsegDTO.fetchServiceAgreement());
        applyRateData.setMasterServiceAgreement(billSegmentGeneratorData.getMasterServiceAgreement());
        applyRateData.setMasterBillSegment(billSegmentGeneratorData.getMasterBillSegment());
        //applyRateData.setBill(billSegmentGeneratorData.getBill());
        //Start Change RIA
        applyRateData.setBillId(billSegmentGeneratorData.getBill().getId().getTrimmedValue());
        //End Change RIA
        applyRateData.setBillSegmentCreationDateTime(bsegDTO.getCreationDateTime());
        applyRateData.setBillSegmentPeriodStart(bsegDTO.getStartDate());
        applyRateData.setBillSegmentPeriodEnd(bsegDTO.getEndDate());
        applyRateData
        .setCharacteristicPremise(bsegDTO.fetchPremise());
        applyRateData.setAccountingDate(determineAccountingDateForRateApplication());
        applyRateData.setConsumptionPeriodStart(consumptionStartDate);
        applyRateData.setConsumptionPeriodEnd(consumptionEndDate);
        /*
         * Start Delete - 2017-09-22 - VRom
         */
        //applyRateData.setBillableChargeId(billableCharge.getId().getTrimmedValue());
        /*
         * End Delete - 2017-09-22 - VRom
         */
        applyRateData.setSaPeriodStartDate(serviceAgreementStartDate);
        applyRateData.setSaPeriodEndDate(serviceAgreementEndDate);
        applyRateData.setStartDayOption(intervalBillingStartDayOption);
        applyRateData.setIntervalBillingSaCutoffTime(intervalBillingCutoffTime);
        applyRateData.setShouldRetainReadCollection(mustRetainReads);
        applyRateData.setShouldRetainItemCollection(mustRetainItems);
        applyRateData.setShouldRetainSqCollection(billableChargeServiceQuantitiesExist.isTrue() ? Bool.TRUE
                : mustRetainServiceQuantities);
        applyRateData.setPriceAssignId(billSegmentGeneratorData.getPriceAsgnId());
        // Prod eligibility on rate comp was not working, So ProductCd sent to Rate engine (BUG23577501)
        applyRateData.setPriceItemCd(billSegmentGeneratorData.getPriceItemCd());
        applyRateData.setShouldRetainSqRules(mustRetainServiceQuantities.not());
        applyRateData.setIsThirdParty(null);
        applyRateData.setLanguage(determineLanguageForRateApplication());
        if (pricingRateSchedule == null) {
            ServiceAgreementType rateSchSaType = rateScheduleInformation.getSaType();
            applyRateData.setCisDivision(rateSchSaType != null ? rateSchSaType.getId().getDivision() : null);
            applyRateData.setServiceAgreementType(rateSchSaType);
            applyRateData.setRevenueClass(rateSchSaType != null ? rateSchSaType.getRevenueClassId().getEntity() : null);
            applyRateData.setRateSchedule(rateScheduleInformation.getRateSchedule());
        } else {

            applyRateData.setRateSchedule(pricingRateSchedule);
        }

        if (pricingCurrencyCd != null)
            applyRateData.setPricingCurrencyCd(pricingCurrencyCd);

        if (isInvoiceAcct == Bool.TRUE && invoiceDataVO.getCurrencyCd() != null)
            applyRateData.setInvoiceCurrencyCd(invoiceDataVO.getCurrencyCd());
        else
            applyRateData
            .setInvoiceCurrencyCd(billSegmentGeneratorData.getAccount().getCurrency().getId().getIdValue());

        if (!isBlankOrNull(billSegmentData.getTrialBillId()))
            applyRateData.setTrialBillId(billSegmentData.getTrialBillId());

        else if (bsegDTO.fetchBill() != null)
            //applyRateData.setBill(bsegDTO.fetchBill());
        	//Start Change RIA
        	applyRateData.setBillId(bsegDTO.fetchBill().getId().getTrimmedValue());
        	//End Change RIA

        return applyRateData;
    }

    private ServiceAgreementRateScheduleData getRateScheduleInformation() {
        ServiceAgreementRateScheduleData serviceAgreementRateScheduleData = ServiceAgreementRateScheduleData.Factory
                .newInstance();
        serviceAgreementRateScheduleData.setAccountingDate(billSegmentGeneratorData.getAccountingDate());
        serviceAgreementRateScheduleData.setStartDate(consumptionStartDate);
        serviceAgreementRateScheduleData.setEndDate(consumptionEndDate);
        if (refServiceAgreement != null) {
            return refServiceAgreement.retrieveRateSchedule(serviceAgreementRateScheduleData);
        }
        return billSegmentGeneratorData.getServiceAgreement().retrieveRateSchedule(serviceAgreementRateScheduleData);
    }

    private Date determineAccountingDateForRateApplication() {
        BillSegment cancelBseg = billSegmentData.getBillSegmentDto().fetchCancelBillSegment();
        if (billSegmentGeneratorData.getIsRebill().isTrue()
                || (billSegmentGeneratorData.getBillSegment() != null && cancelBseg != null)) {
            BillSegment cancelBillSegment = billSegmentGeneratorData.getIsRebill().isTrue() ? billSegmentGeneratorData
                    .getCancelBillSegmentId() : cancelBseg;
                    return cancelBillSegment.determineAccountingDateForRateApp();
        }

        boolean billGenFlg = false;
        BillCycle bCycle = billSegmentGeneratorData.getBillCycle();
        String bCycCd = null;
        if (bCycle != null)
            bCycCd = bCycle.getId().getTrimmedValue();
        // if batch flow and bill cycle is present
        if (!isOnlineConnection() && !isBlankOrNull(bCycCd))
            billGenFlg = true;

        if (isInvoiceAcct == Bool.TRUE && billGenFlg == true) {
            return invoiceDataVO.getAccountingDt();
        }

        return billSegmentGeneratorData.getAccountingDate();
    }

    private Language determineLanguageForRateApplication() {
        Person person = getMainCustomerOnAccount(billSegmentGeneratorData.getAccount());
        return (person != null && person.getLanguage() != null) ? person.getLanguage() : getActiveContextLanguage();
    }

    private Person getMainCustomerOnAccount(Account account) {
        Query<AccountPerson> query = createQuery(
                " from AccountPerson as accper where accper.id.account.id = :account and accper.isMainCustomer = 'Y' ",
                "");
        query.addResult("accper", "accper");
        query.bindEntity("account", account);

        AccountPerson acctPer = query.firstRow();
        return acctPer != null ? acctPer.getId().getPerson() : null;
    }

    /**
     * call Cobol Rate Engine for applying rate
     * if any error occurs from Rate Application
     * it sets data in BillSegment Calculation Header for error
     * it doesnt skip the contract if error occurs.
     * @param rateApplicationProcessorData
     */
    private void applyRate(RateApplicationProcessorData rateApplicationProcessorData) {

        RateApplicationProcessor rateApplicationProcessor = RateApplicationProcessor.Factory.newInstance();

        if (isDebugLogEnabled)
            logger.info("before call " + mustExitIterationLoop);

        try {
            rateApplicationProcessor.applyRate(rateApplicationProcessorData);
        } catch (ApplicationError ae) {
            // If billable charge is not present in the copybook,
            // it is SA based flow (Online or Billing batch).
            // So, we set the Exit Flag to False to avoid skipping of entire SA.
            if (billSegmentGeneratorData.getBillableCharge() == null) {
                billSegmentGeneratorData.setCanExitIterationLoop(Bool.FALSE);
            }
            // If billable charge is present in copybook,
            // it is Charge based flow (BlGen batch, Invoice online/batch, BillApi...).
            // So, we set the Exit Flag of only non recurring charges to True to process the next charge.
            else if (isNull(billSegmentGeneratorData.getBillableCharge().getRecurringFlg())
                    || isBlankOrNull(billSegmentGeneratorData.getBillableCharge().getRecurringFlg().trimmedValue())) {
                billSegmentGeneratorData.setCanExitIterationLoop(Bool.TRUE);
            }
            BillSegment_DTO billSegment_DTO = billSegmentData.getBillSegmentDto();
            billSegment_DTO.setBillSegmentStatus(BillSegmentStatusLookup.constants.ERROR);
            billSegmentData.setBillSegmentDto(billSegment_DTO);
            billSegmentCalculationHeaderData = rateApplicationProcessorData.getCalculationHeaderDataList();
            billSegmentItemData = rateApplicationProcessorData.getItemDataList();
            billSegmentReadData = rateApplicationProcessorData.getReadDataList();
            billSegmentServiceQuantityData = rateApplicationProcessorData.getServiceQuantityDataList();

            stampHeaderWithBillableChargeInError();

            throw ae;

        }

        billSegmentCalculationHeaderData = rateApplicationProcessorData.getCalculationHeaderDataList();
        billSegmentItemData = rateApplicationProcessorData.getItemDataList();
        billSegmentReadData = rateApplicationProcessorData.getReadDataList();
        billSegmentServiceQuantityData = rateApplicationProcessorData.getServiceQuantityDataList();
        billSegmentCalculationHeaderCount = BigInteger.valueOf(billSegmentCalculationHeaderData.size());
        stampHeaderWithBillableCharge();
    }

    /*
     * Populate the data for BillSegment Calculation Header
     */
    private void createBillSegmentCalculationHeader(BillableChargeLine billableChargeLine, ServiceAgreement sa) {
        if (sa == null && billSegmentCalculationHeaderCount.intValue() >= CobolConstants.CI_CONST_BI_MAX_CALC_HDR_COLL) {
            checkForBillSegmentCalculationHeader();
            addError(MessageRepository.calcHeaderCountLimitReached());
        }
        billSegmentCalculationHeaderCount = billSegmentCalculationHeaderCount.add(BigInteger.ONE);

        BillSegmentCalculationHeader_DTO billSegmentCalculationHeaderDto = createDTO(BillSegmentCalculationHeader.class);
        billSegmentCalculationHeaderDto.setId(new BillSegmentCalculationHeader_Id(BillSegment_Id.NULL,
                billSegmentCalculationHeaderCount));
        billSegmentCalculationHeaderDto.setBillableChargeId(billableCharge.getId());
        // Start & End date on Bill segment Calc Lines should be stamped with Bill segment start & End date instead of Billable Charge dates.
        BillSegment_DTO bsegDTO = billSegmentData.getBillSegmentDto();
        billSegmentCalculationHeaderDto.setStartDate(bsegDTO.getStartDate());
        billSegmentCalculationHeaderDto.setEndDate(bsegDTO.getEndDate());
        billSegmentCalculationHeaderDto.setRateVersionId(null);

        if (billableChargeLine != null) {
            billSegmentCalculationHeaderDto.setCalculatedAmount(BigDecimal.ZERO);

        } else if (sa != null) {
            billSegmentCalculationHeaderDto.setCalculatedAmount(BigDecimal.ZERO);
        }

        if (isInvoiceAcct == Bool.TRUE && invoiceDataVO.getCurrencyCd() != null) {
            billSegmentCalculationHeaderDto.setCurrencyId(new Currency_Id(invoiceDataVO.getCurrencyCd())); // Invoice Account Currency
        } else {
            billSegmentCalculationHeaderDto.setCurrencyId(serviceAgreement.getAccount().getCurrency().getId()); // Usage Account Currency
        }

        billSegmentCalculationHeaderDto.setDescriptionOnBill(billableCharge.getDescriptionOnBill());
        BillSegmentCalculationHeaderData billSegmentCalculationHeaderRecord = BillSegmentCalculationHeaderData.Factory
                .newInstance();
        billSegmentCalculationHeaderRecord.setBillSegmentCalculationHeaderDto(billSegmentCalculationHeaderDto);
        billSegmentCalculationHeaderRecord.setCalculationLineCount(BigInteger.ZERO);
        billSegmentCalculationHeaderRecord.setBillCalculationLineData(new ArrayList<BillCalculationLineData>());

        billSegmentCalculationHeaderData.add(billSegmentCalculationHeaderRecord);
    }

    /*
     * Populate the data for BillSegment Calculation Line.
     * Also handles currency conversion if needed.
     */
    private BigDecimal createBillSegmentCalculationLine(BillableChargeLine billableChargeLine) {
        BillCalculationLineData billSegmentCalculationLineRecord = BillCalculationLineData.Factory.newInstance();
        billSegmentCalculationLineRecord.setSequence(billableChargeLine.getId().getLineSequence());
        billSegmentCalculationLineRecord.setRcSequence(BigInteger.ZERO);
        billSegmentCalculationLineRecord.setShouldCalculateOnly(Bool.FALSE);
        billSegmentCalculationLineRecord.setShouldPrint(billableChargeLine.getShouldShowOnBill());
        billSegmentCalculationLineRecord.setShouldAppearInSummary(billableChargeLine.getShouldAppearInSummary());

        /* AshishS
         * Calling Currency Conversion Algo, If currency on Charge line is diff than Acct currency.
         * For both Usage and Invoice account billing.
         */
        String bchgCurrency = billableChargeLine.getCurrencyId().getIdValue(); // Currency from Billable Line charge
        String billingAcctCurrency = null; // Currency from Billing account, Usage/Invoice
        BigDecimal finalAmount = null;
        String exchRateId = null;
        BigDecimal exchRate = null;

        if (billableChargeLine.getChargeAmount() != null)
            finalAmount = billableChargeLine.getChargeAmount().getAmount(); // Final Amount, with or without conversion

        if (isInvoiceAcct == Bool.TRUE)
            billingAcctCurrency = invoiceDataVO.getCurrencyCd();
        else
            billingAcctCurrency = billSegmentGeneratorData.getAccount().getCurrency().getId().getIdValue();

        if (!bchgCurrency.equalsIgnoreCase(billingAcctCurrency)) { // Call Currency Conversion also, when both currencies are different
            Boolean negetive = false;
            BigDecimal amount = billableChargeLine.getChargeAmount().getAmount();
            if (amount.doubleValue() < 0) {
                negetive = true;
                amount = amount.multiply(BigDecimal.valueOf(-1));
            }
            String entityFlag = DivAlgEntityFlagLookup.constants.CURR_CONV_BILL_SEGMENTS.value();
            String cisDivision = null;
            CisDivision div = billSegmentGeneratorData.getAccount().fetchDivision();
            if (div != null)
                cisDivision = div.getId().getTrimmedValue();

            String algoName = fetchExchRateAlgorithm(cisDivision, entityFlag);

            Algorithm algorithm = new Algorithm_Id(algoName).getEntity();
            if (algorithm == null) {
                addError(AlgorithmMessageRepository.invalidAlgorithmCode(algoName));
            }

            CurrencyConversionAlgorithmSpot algorithmComp = AlgorithmComponentCache.getAlgorithmComponent(
                    new Algorithm_Id(algoName), CurrencyConversionAlgorithmSpot.class);
            algorithmComp.setAmount(amount);
            algorithmComp.setFromCurCode(bchgCurrency);
            algorithmComp.setToCurCode(billingAcctCurrency);

            Date cutOffDate = billSegmentGeneratorData.getCutoffDate();
            DateTime dateTime = new DateTime(cutOffDate.getYear(), cutOffDate.getMonth(), cutOffDate.getDay());
            algorithmComp.setEffDate(dateTime);
            algorithmComp.setCisDivision(cisDivision);

            //Start: values additionally passed for CurrencyConvAlgoSpot changes
            /*
             * Start Delete - 2017-09-22 - VRom
             */
            //algorithmComp.setContractId(billSegmentGeneratorData.getServiceAgreement().getId().getIdValue());
            //algorithmComp.setBillableChgId(billableCharge.getId().getIdValue());
            //algorithmComp.setAccountId(billSegmentGeneratorData.getAccount().getId().getIdValue());
            /*
             * End Delete - 2017-09-22 - VRom
             */
            //End : values additionally passed for CurrencyConvAlgoSpot changes
            algorithmComp.invoke();

            finalAmount = algorithmComp.getConvertedAmount();

            exchRateId = algorithmComp.getExchRateId();
            exchRate = algorithmComp.getExchangeRate();

            if (negetive == true) {
                finalAmount = finalAmount.multiply(BigDecimal.valueOf(-1));
            }

        }

        billSegmentCalculationLineRecord.setPriceAmount(billableChargeLine.getChargeAmount().getAmount());
        billSegmentCalculationLineRecord.setCalculatedAmount(finalAmount != null ? finalAmount : null);
        /*
         * Start Delete - 2017-09-22 - VRom
         */
        //billSegmentCalculationLineRecord.setPricingCcyId(new Currency_Id(bchgCurrency));
        /*
         * End Delete - 2017-09-22 - VRom
         */
        billSegmentCalculationLineRecord.setExchRateId(exchRateId);
        billSegmentCalculationLineRecord.setExchRate(exchRate);

        billSegmentCalculationLineRecord.setExemptAmount(null);
        billSegmentCalculationLineRecord.setBaseAmount(null);

        Currency_Id billingAcctCurr = new Currency_Id(billingAcctCurrency);
        billSegmentCalculationLineRecord.setCurrencyId(billingAcctCurr); // Account Currency Set, ChargeLine currency is also converted to billingAcctCurr.
        billSegmentCalculationLineRecord.setDistributionCodeId(billableChargeLine.getDistributionCodeId());
        billSegmentCalculationLineRecord.setIsStatistical(Bool.FALSE);
        billSegmentCalculationLineRecord.setMeasuresPeakQuantity(Bool.FALSE);
        billSegmentCalculationLineRecord.setUnitOfMeasureId(null);
        billSegmentCalculationLineRecord.setTimeOfUseId(null);
        billSegmentCalculationLineRecord.setServiceQuantityIdentifierId(null);
        billSegmentCalculationLineRecord.setBillableServiceQuantity(BigDecimal.ZERO);
        billSegmentCalculationLineRecord.setDescriptionOnBill(billableChargeLine.getDescriptionOnBill());
        billSegmentCalculationLineRecord.setShouldCreateBillLine(Bool.TRUE);
        billSegmentCalculationLineRecord.setCalcLineCharacteristicsCount(BigInteger.ZERO);
        billSegmentCalculationLineRecord
        .setBillCalculationLineCharacteristicData(new ArrayList<BillCalculationLineCharacteristicData>());
        addCalcLineToHeader(billSegmentCalculationLineRecord);

        return finalAmount;
    }

    /**
     * Fetches Currency Conversion Algorithm from Account Division.
     * @param divisionCd
     * @param entityFlag
     * @return
     */
    private String fetchExchRateAlgorithm(String divisionCd, String entityFlag) {
        String algorithmCd = null;
        PreparedStatement preparedStatement = null;
        try {
            String query = "SELECT ALG_CD, DIV_ALG_ENTITY_FLG FROM CI_CIS_DIV_ALG WHERE trim(CIS_DIVISION) = :divisionCd AND trim(DIV_ALG_ENTITY_FLG) = :entityFlag";
            preparedStatement = createPreparedStatement(query, "Fetch currency conversion alogrithm");
            preparedStatement.bindString("divisionCd", divisionCd.trim(), "Division code");
            preparedStatement.bindString("entityFlag", entityFlag.trim(), "Entitfy flag");
            preparedStatement.setAutoclose(false);
            List<SQLResultRow> resultList = preparedStatement.list();
            int size = resultList.size();
            if (size > 0) {
                SQLResultRow resultData = resultList.get(0);
                algorithmCd = resultData.getString("ALG_CD");
            }
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
                preparedStatement = null;
            }
        }
        if (algorithmCd == null || algorithmCd.trim().length() == 0) {
            // Please specific algorithm for entity flag
            addError(MessageRepository.currencyConvAlgoConfigError(divisionCd));
        }

        return algorithmCd;
    }

    private void addCalcLineToHeader(BillCalculationLineData billSegmentCalculationLineRecord) {
        BillSegmentCalculationHeaderData header = billSegmentCalculationHeaderData
                .get(billSegmentCalculationHeaderCount.intValue() - 1);
        header.getBillCalculationLineData().add(billSegmentCalculationLineRecord);
        header.setCalculationLineCount(header.getCalculationLineCount().add(BigInteger.ONE));
    }

    private void addChargeAmountToHeader(Money chargeAmount) {
        BillSegmentCalculationHeaderData header = billSegmentCalculationHeaderData
                .get(billSegmentCalculationHeaderCount.intValue() - 1);
        //Money totalAmount = header.getBillSegmentCalculationHeaderDto().getCalculatedAmount().add(chargeAmount);
        BigDecimal totalAmount = header.getBillSegmentCalculationHeaderDto().getCalculatedAmount().add(
                chargeAmount.getAmount());
        totalAmount = totalAmount == null ? null : BigDecimal.valueOf(totalAmount.doubleValue()).setScale(2, 4);
        header.getBillSegmentCalculationHeaderDto().setCalculatedAmount(totalAmount);
    }

    private void addBillableChargeLineCharacteristics(BillableChargeLine billableChargeLine) {

        billSegmentCalculationLineCharacteristicCount = billSegmentCalculationLineCharacteristicCount
                .add(BigInteger.ONE);

        BillCalculationLineData billCalcLineData = billSegmentCalculationHeaderData.get(
                billSegmentCalculationHeaderCount.intValue() - 1).getBillCalculationLineData().get(
                        billSegmentCalculationLineCount.intValue() - 1);
        /*      BillSegmentCalculationHeader header = billSegmentCalculationHeaderData.get(billSegmentCalculationHeaderCount.intValue() - 1).getBillSegmentCalculationHeaderDto().getEntity();
                BillCalculationLine billCalcLine = new BillCalculationLine_Id(header, billCalcLineData.getSequence()).getEntity();*/// unused

        BillCalculationLineCharacteristic_DTO billCalculationLineCharacteristicDto = createDTO(BillCalculationLineCharacteristic.class);
        BillCalculationLineCharacteristicData billCalcLineCharRecord = BillCalculationLineCharacteristicData.Factory
                .newInstance();
        billCalcLineCharRecord.setBillCalcLineDto(billCalculationLineCharacteristicDto);
        billCalcLineData.getBillCalculationLineCharacteristicData().add(billCalcLineCharRecord);
        billCalcLineData.setCalcLineCharacteristicsCount(billCalcLineData.getCalcLineCharacteristicsCount().add(
                BigInteger.ONE));
    }

    private void copyBillableChargeLineCharacteristics(BillableChargeLine billableChargeLine) {
        List<BillableChargeLineCharacteristic> billableChargeLineChars = fetchBillableChargeLineCharacteristics(billableChargeLine);
        for (BillableChargeLineCharacteristic billableChargeLineCharacteristic : billableChargeLineChars) {
            if (billSegmentCalculationLineCharacteristicCount.intValue() >= CobolConstants.CI_CONST_BI_MAX_LINE_CHAR_COLL)
                addError(MessageRepository.calcLineCharCountLimitReached(BigInteger
                        .valueOf(CobolConstants.CI_CONST_BI_MAX_LINE_CHAR_COLL)));
            createBillSegmentCalculationLineCharacteristic(billableChargeLineCharacteristic);
        }
    }

    private void createBillSegmentCalculationLineCharacteristic(
            BillableChargeLineCharacteristic billableChargeLineCharacteristic) {
        billSegmentCalculationLineCharacteristicCount = billSegmentCalculationLineCharacteristicCount
                .add(BigInteger.ONE);

        BillCalculationLineData billCalcLineData = billSegmentCalculationHeaderData.get(
                billSegmentCalculationHeaderCount.intValue() - 1).getBillCalculationLineData().get(
                        billSegmentCalculationLineCount.intValue() - 1);
        BillSegmentCalculationHeader header = billSegmentCalculationHeaderData.get(
                billSegmentCalculationHeaderCount.intValue() - 1).getBillSegmentCalculationHeaderDto().getEntity();
        BillCalculationLine billCalcLine = new BillCalculationLine_Id(header, billCalcLineData.getSequence())
        .getEntity();

        BillCalculationLineCharacteristic_DTO billCalculationLineCharacteristicDto = createDTO(BillCalculationLineCharacteristic.class);
        billCalculationLineCharacteristicDto.setId(new BillCalculationLineCharacteristic_Id(billCalcLine,
                billableChargeLineCharacteristic.getId().getCharacteristicType()));
        billCalculationLineCharacteristicDto.setCharacteristicValue(billableChargeLineCharacteristic
                .getCharacteristicValue());
        billCalculationLineCharacteristicDto.setAdhocCharacteristicValue(billableChargeLineCharacteristic
                .getAdhocCharacteristicValue());

        BillCalculationLineCharacteristicData billCalcLineCharRecord = BillCalculationLineCharacteristicData.Factory
                .newInstance();
        billCalcLineCharRecord.setBillCalcLineDto(billCalculationLineCharacteristicDto);

        billCalcLineData.getBillCalculationLineCharacteristicData().add(billCalcLineCharRecord);
        billCalcLineData.setCalcLineCharacteristicsCount(billCalcLineData.getCalcLineCharacteristicsCount().add(
                BigInteger.ONE));
    }

    private List<BillableChargeLineCharacteristic> fetchBillableChargeLineCharacteristics(
            BillableChargeLine billableChargeLine) {
        StringBuilder sbQuery = new StringBuilder(
                " from BillableChargeLineCharacteristic as bch, CharacteristicEntity as che");
        sbQuery.append(" where bch.id.billableChargeLine.id.billableCharge.id = :billableCharge ");
        sbQuery.append(" and bch.id.billableChargeLine.id.lineSequence = :lineSequence ");
        sbQuery.append(" and bch.id.characteristicType.id = che.id.characteristicType.id ");
        sbQuery.append(" and che.id.characteristicEntity = :characteristicEntity ");

        Query<BillableChargeLineCharacteristic> query = createQuery(sbQuery.toString(), "");
        query.bindEntity("billableCharge", billableCharge);
        query.bindBigInteger("lineSequence", billableChargeLine.getId().getLineSequence());
        query.bindLookup("characteristicEntity", CharacteristicEntityLookup.constants.BILL_SEGMENT_CALC_LINE);
        query.addResult("bch", "bch");
        return query.list();
    }

    private void checkForBillSegmentCalculationHeader() {
        if (billSegmentCalculationHeaderData.isEmpty()) {
            if (refServiceAgreement != null) {
                createBillSegmentCalculationHeader(null, refServiceAgreement);
            } else {
                createBillSegmentCalculationHeader(null, billSegmentGeneratorData.getServiceAgreement());
            }
        }
    }

    private void stampHeaderWithBillableCharge() {
        for (int index = 0; index < billSegmentCalculationHeaderData.size(); index++) {
            billSegmentCalculationHeaderData.get(index).getBillSegmentCalculationHeaderDto().setBillableChargeId(
                    billableCharge.getId());
            // added for currency to be stamped with invoice currency

            billSegmentCalculationHeaderData.get(index).getBillSegmentCalculationHeaderDto().setCurrencyId(
                    serviceAgreement.getAccount().getCurrency().getId());

            List<BillCalculationLineData> billCalcLineData = billSegmentCalculationHeaderData.get(index)
                    .getBillCalculationLineData();

            if (billCalcLineData != null && billCalcLineData.size() != 0) {

                for (int i = 0; i < billCalcLineData.size(); i++) {
                    BillCalculationLineData billCalcLine = billCalcLineData.get(i);
                    billCalcLine.setCurrencyId(serviceAgreement.getAccount().getCurrency().getId());
                    
                    /*
                     * Start Delete - 2017-09-22 - VRom
                     */
                    //if (pricingCurrencyCd != null)
                    //    billCalcLine.setPricingCcyId(new Currency_Id(pricingCurrencyCd));
                    //else
                    //    billCalcLine.setPricingCcyId(serviceAgreement.getAccount().getCurrency().getId());
                    /*
                     * End Delete - 2017-09-22 - VRom
                     */
                    
                    billCalcLine.mapDataFieldsToDto();

                }
            }
        }
    }

    private void stampHeaderWithBillableChargeInError() {

        BillSegmentCalculationHeader_DTO billSegmentCalculationHeader_DTO = (BillSegmentCalculationHeader_DTO) createDTO(BillSegmentCalculationHeader.class);
        billSegmentCalculationHeader_DTO
        .setId(new BillSegmentCalculationHeader_Id(billSegmentData.getBillSegmentDto().getId(), BigInteger.ONE));
        billSegmentCalculationHeader_DTO.setBillableChargeId(billableCharge.getId());
        billSegmentCalculationHeader_DTO.setStartDate(billSegmentStartDate);
        billSegmentCalculationHeader_DTO.setEndDate(billSegmentEndDate);
        billSegmentCalculationHeader_DTO.setCurrencyId(serviceAgreement.getAccount().getCurrency().getId());

        BillSegmentCalculationHeaderData_Impl billSegmentCalculationHeaderDataElement = new BillSegmentCalculationHeaderData_Impl();
        billSegmentCalculationHeaderDataElement.setBillSegmentCalculationHeaderDto(billSegmentCalculationHeader_DTO);

        billSegmentCalculationHeaderData.add(billSegmentCalculationHeaderDataElement);
        billSegmentCalculationHeaderCount = BigInteger.valueOf(billSegmentCalculationHeaderData.size());

    }

    /**
     * this method reads and validares characteristic  types from feature config
     */
    private void readAndValidateCharTypesFromFeatureConfig() {

        divisionCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "CDIV",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

        saTypeCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "CCON",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

        chargeTypeCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "CCHG",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

        billableChargeCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "RCBC",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

        prorationEndDateCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "CPRE",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

        prorationStartDateCharType = getFeatureConfigurationOptionValue(RECON_FEATURE_CONFIG_NAME, "CPRS",
                ExternalSystemTypeLookup.constants.RECONCILIATION);

    }

    /**
     * To get the feature config value
     * @param featureConfigIdName
     * @param lookUpFieldValue
     * @param externalSystemTypeLookup
     * @return
     */
    private String getFeatureConfigurationOptionValue(String featureConfigIdName, String lookUpFieldValue,
            ExternalSystemTypeLookup externalSystemTypeLookup) {

        FeatureConfiguration_Id featureConfigId = new FeatureConfiguration_Id(featureConfigIdName);

        FeatureConfigurationOption_Id featureConfigOptionId = new FeatureConfigurationOption_Id(featureConfigId,
                lookUpFieldValue, BigInteger.ONE);

        FeatureConfigurationOption featureConfigOption = featureConfigOptionId.getEntity();
        if (featureConfigOption != null) {
            return featureConfigOption.getValue();
        }

        return "";
    }

    /*
     * Clear all lists and data populated in the algo.
     * It is called when algo call returns.
     */
    private void clearData() {
        listBillableCharge = null;
        billableCharge = null;
        mustSkipIteration = Bool.FALSE;

    }

    /**
     * Class for temporary storing of billable charge related data to be used in the Recurring Algo
     */
    @SuppressWarnings("unused")
	private class BillableChargeWrapperObject {

        private BillableCharge_Id bchargeId = null;

        private Date bsegEndDate = null;

        private Date lastDatebillDate = null;

        private boolean invoiced = true;

        private boolean recurring = false;

        private String billPeriod = null;

        private boolean isProcessed = false;

        public BillableChargeWrapperObject(BillableCharge_Id bChargeId_loc, String billPeriod) {
            bchargeId = bChargeId_loc;
            invoiced = true;
            this.billPeriod = billPeriod;
        }

        public BillableChargeWrapperObject(BillableCharge_Id bChargeId_loc, boolean recurring_loc, String billPeriod) {
            bchargeId = bChargeId_loc;
            invoiced = true;
            recurring = recurring_loc;
            this.billPeriod = billPeriod;
        }

        public BillableCharge_Id getBchargeId() {
            return bchargeId;
        }

        public void setBchargeId(BillableCharge_Id bchargeId) {
            this.bchargeId = bchargeId;
        }

        public boolean isInvoiced() {
            return invoiced;
        }

        public void setInvoiced(boolean invoiced) {
            this.invoiced = invoiced;
        }

        public boolean isRecurring() {
            return recurring;
        }

        public void setRecurring(boolean recurring) {
            this.recurring = recurring;
        }

        public Date getBsegEndDate() {
            return bsegEndDate;
        }

        public void setBsegEndDate(Date bsegEndDate) {
            this.bsegEndDate = bsegEndDate;
        }

        public String getBillPeriod() {
            return billPeriod;
        }

        public void setBillPeriod(String billPeriod) {
            this.billPeriod = billPeriod;
        }

        public boolean getIsProcessed() {
            return isProcessed;
        }

        public void setIsProcessed(boolean isProcessed) {
            this.isProcessed = isProcessed;
        }

    }
}