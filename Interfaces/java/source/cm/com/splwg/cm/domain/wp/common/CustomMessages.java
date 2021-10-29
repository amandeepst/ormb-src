package com.splwg.cm.domain.wp.common;

import com.splwg.base.domain.common.message.AbstractMessageRepository;

public class CustomMessages extends AbstractMessageRepository {
	//Message Category
	public static final int MESSAGE_CATEGORY = 90000;

	//Common Message
	public static final int RUN_TIME_ERROR_IN_EXECUTION = 1000;

	//	 INT001 - Merchants Interface Messages
	public static final int RUN_TIME_ERROR_WHILE_PROCESSING_UNIT = 1000;
	public static final int INVALID_TRANSACTION_GROUP_TYPE = 1001;
	public static final int CHARACTERISTICS_MISSING = 1002;
	public static final int FIELDS_MISSING = 1003;
	public static final int INVALID_PER_OR_BUS_FLG = 1005;
	public static final int ACTIVE_CONTRACT_MISSING = 1006;

	//	 Merchant Hierarchy Interface
	public static final int PERSONID__CHECK_FAILED = 2101;
	public static final int PERSON_HIER_FAILED = 2102;
	public static final int PERSON_HIER_CHECK_FAILED = 2103;
	public static final int SAME_HIER_CHECK_FAILED = 2104;
	public static final int DIFF_DIVISION_CHECK_FAILED = 2105;
	public static final int BOTH_PERSONID__CHECK_FAILED = 2106;	
	public static final int PARENT_PERSONID__CHECK_FAILED = 2107;	
	public static final int CHILD_PERSONID__CHECK_FAILED = 2108;
	public static final int PERSON_ACCT_INACTIVE = 2109;
	public static final int ACCT_MISSING = 2110;

	// Payment Cancellation Interface
	public static final int PAYMENT_GENERIC = 1050;
	public static final int PAYMENT_UPLD_STG_NO_DATA_FOUND = 1051;
	public static final int PAYMENT_UPLD_NO_MATCH_STG_VALUES = 1052;
	public static final int PAYMENT_UPLD_NO_PAYMENT_ID = 1053;	
	public static final int PAYMENT_INVALID_CANCEL_REASON = 1054;

	// Correction Notes Interface
	public static final int CORRECTION_NOTE_GENERIC = 1100;
	public static final int CORRECTION_NOTE_NO_BILL = 1101;

	// INT0293031 - Account Hierarchies Interface Messages	
	public static final int PERSON_NOT_FOUND = 2201;
	public static final int PERSON_HIERARCHY_DOESNT_EXIST = 2202;
	public static final int MASTER_ACCOUNT_DOESNT_EXIST = 2203;
	public static final int CHILD_ACCOUNT_DOESNT_EXIST = 2204;
	public static final int MASTER_ACCOUNT_UPDATE_FAILED = 2205;
	public static final int MASTER_CONTRACT_CREATION_FAILED = 2206;
	public static final int MASTER_CONTRACT_UPDATION_FAILED = 2207;
	public static final int MASTER_MASTER_ASSOCIATION_FAILED = 2208;
	public static final int DIVISION_IS_DIFFERENT_FOR_MASTER_AND_MEMBER = 2209;
	public static final int CURRENCY_CODES_ARE_DIFFERENT_FOR_MASTER_AND_MEMBER = 2210;
	public static final int PARENT_AND_CHILD_PERSONS_ARE_SAME = 2211;
	public static final int DIVISION_IS_DIFFERENT_FOR_MASTER_AND_CHILD_PERSONS = 2212;
	public static final int CURRENCY_CODES_ARE_DIFFERENT_FOR_MASTER_AND_CHILD_PERSONS = 2213;
	public static final int CURRENCY_CODES_OF_MASTER_MERCHANT_AND_MASTER_ACCOUNT_DONOT_MATCH = 2214;
	public static final int CURRENCY_CODES_OF_CHILD_MERCHANT_AND_MEMBER_ACCOUNT_DONOT_MATCH = 2215;
	public static final int INVALID_HIERARCHY_TYPE = 2216;
	public static final int PER_ID_NBR_AND_PER_ID_NBR2_ARE_SAME = 2217;
	public static final int INVALID_CURRENCY_CODE_FOR_MASTER_PERSON = 2218;
	public static final int INVALID_CURRENCY_CODE_FOR_CHILD_PERSON = 2219;
	public static final int ACTIVE_SA_EXISTS = 2220;
	public static final int ACCT_NOT_ACTIVE = 2221;

	//	 INT012 Price Tariff Interface
	public static final int PL_START_DT_NOT_POPULATED = 1201;
	public static final int NO_TF_CODE_ATTACHED = 1202;
	public static final int MULTIPLE_TF_CODE_ATTACHED = 1203;
	public static final int NO_PL_CCY_ATTACHED = 1204;
	public static final int MULTIPLE_PL_CCY_ATTACHED = 1205;
	public static final int NO_TF_TYPE_ATTACHED = 1206;
	public static final int MULTIPLE_TF_TYPE_ATTACHED = 1207;
	public static final int PL_DESCR_NOT_FOUND = 1208;
	public static final int SIMILAR_PRICELISTS_EXIST = 1209;
	public static final int PM_DESCR_NOT_FOUND = 1210;
	public static final int SIMILAR_PRICEITEMS_EXIST = 1211;
	public static final int NO_CH_TYPE_ATTACHED = 1212;
	public static final int MULTIPLE_CH_TYPE_ATTACHED = 1213;
	public static final int NO_CH_STYPE_ATTACHED = 1214;
	public static final int MULTIPLE_CH_STYPE_ATTACHED = 1215;
	public static final int NO_CARD_TP_ATTACHED = 1216;
	public static final int MULTIPLE_CARD_TP_ATTACHED = 1217;
	public static final int NO_PCH_RFD_ATTACHED = 1218;
	public static final int MULTIPLE_PCH_RFD_ATTACHED = 1219;
	public static final int NO_ACQ_FLAG_ATTACHED = 1220;
	public static final int MULTIPLE_ACQ_FLAG_ATTACHED = 1221;
	public static final int NO_PRM_CHG_ATTACHED = 1222;
	public static final int MULTIPLE_PRM_CHG_ATTACHED = 1223;
	public static final int NO_CR_DR_ATTACHED = 1224;
	public static final int MULTIPLE_CR_DR_ATTACHED = 1225;
	public static final int NO_CHANNEL_ATTACHED = 1226;
	public static final int MULTIPLE_CHANNEL_ATTACHED = 1227;
	public static final int NO_SCHEME_ATTACHED = 1228;
	public static final int MULTIPLE_SCHEME_ATTACHED = 1229;
	public static final int CURRENCY_MISMATCH = 1230;
	public static final int NO_TF_RATE_ATTACHED = 1231;
	public static final int MULTIPLE_TF_RATE_ATTACHED = 1232;

	//	 INT015 Payment Requests Interface
	public static final int BILL_DT_COULD_NOT_BE_DETERMINED = 1310;
	public static final int BILL_DUE_DT_COULD_NOT_BE_DETERMINED = 1311;
	public static final int ACCOUNT_TYPE_COULD_NOT_BE_DETERMINED = 1312;
	public static final int DIVISION_COULD_NOT_BE_DETERMINED = 1313;
	public static final int EXTERNAL_PARTY_ID_COULD_NOT_BE_DETERMINED = 1314;
	public static final int PAY_TYPE_COULD_NOT_BE_DETERMINED = 1315;
	public static final int AMOUNT_COULD_NOT_BE_DETERMINED = 1316;
	public static final int CURRENCY_COULD_NOT_BE_DETERMINED = 1317;
	public static final int BATCH_PARM_MISSING = 1318;
    public static final int ERROR_INSERTING_IN_TABLE = 1319;
    public static final int UNABLE_TO_CREATE_PAY_REQ = 1320;
	

	// INT016 Transactional Pricing Data Interface

	//  For Bulk Insertion    
	public static final int EVENT_ID = 1511; 
	public static final int PRODUCT_CODE = 1512;
	public static final int BILL_CALC_LN_TYPE = 1513;
	public static final int CALCULATED_AMOUNT = 1514;
	public static final int CURRENCY_CODE = 1515;
	public static final int BILL_ID = 1516;
	public static final int ACCT_TYPE = 1517;
	public static final int CHARGING_CARD_PRODUCT = 1518;

	// For JAVA Validations
	public static final int EVENT_ID_COULD_NOT_BE_DETERMINED = 1501;
	public static final int PRODUCT_CODE_COULD_NOT_BE_DETERMINED = 1502;
	public static final int TRANSACTION_CALCULATION_ID_COULD_NOT_BE_DETERMINED = 1503;
	public static final int SEQUENCE_NUMBER_COULD_NOT_BE_DETERMINED = 1504;
	public static final int BILL_CALC_LINE_TYPE_COULD_NOT_BE_DETERMINED = 1505;
	public static final int CALCULATED_AMOUNT_COULD_NOT_BE_DETERMINED = 1506;
	public static final int CURRENCY_CODE_COULD_NOT_BE_DETERMINED = 1507;
	public static final int BILL_ID_COULD_NOT_BE_DETERMINED = 1508;
	public static final int ACCT_TYPE_COULD_NOT_BE_DETERMINED = 1509;
	public static final int CHARGING_CARD_PRODUCT_COULD_NOT_BE_DETERMINED = 1510;

	// INT005 Invoice Data Interface
	public static final int INV_BILLING_PARTY_ID = 1601;
	public static final int INV_WP_BU= 1602;
	public static final int INV_WIN_START_DT_END_DT= 1603;
	public static final int INV_BILL_AMT= 1604;
	public static final int INV_WP_TAX_REG_NBR= 1605;
	public static final int INV_MERCHANT_ACCT_HIST= 1606;
	public static final int INV_TAX_STAT= 1607;
	public static final int INV_TAX_STAT_DESCR= 1608;
	public static final int INV_SQI= 1609;
	public static final int INV_PRICE_CATEGORY= 1610;
	public static final int INV_PRICE_CATEGORY_DESCR= 1611;
	public static final int INV_LINE_AMT= 1612;
	public static final int INV_BCL_TYPE_DESCR= 1613;
	public static final int INV_TAX_RATE= 1614;
	public static final int INV_RATE= 1615;
	public static final int INV_PRICE_CURRENCY_CD= 1616;
	public static final int INV_VALUE_AMT= 1617;
	public static final int INV_FUND_CURRENCY_CD= 1618;
	public static final int INV_ADJ_DATA= 1619;
	public static final int INV_DUE_DT= 1620;


	// INT036 Recurring Charges Interface
	public static final int RCI_PERSON_NOT_FOUND = 1031;
	public static final int RCI_CHARGING_ACCOUNT_NOT_FOUND = 1032;
	public static final int RCI_RECURRING_CONTRACT_NOT_FOUND = 1033;
	public static final int RCI_BILLABLE_CHARGE_CREATION_FAILED = 1034;
	public static final int RCI_BILLABLE_CHARGE_UPDATE_FAILED = 1035;
	public static final int RCI_BILLABLE_CHARGE_UPDATING_NEW_QTY_FAILED = 1036;
	public static final int RCI_INVALID_BILL_PERIOD_CD = 1037;
	public static final int RCI_INVALID_ORIGINAL_FIELDS = 1038;
	public static final int RCI_INVALID_PRICEITEM_CD = 1039;
	public static final int RCI_SQI_CD_UPDATE_FAIL = 1040;

	// INT002 - Agreement Interface Message
	public static final int AGREEMENT_GENERIC = 1200;

	//INT002 Agreements Interface Messages (Removed from lookups)
	public static final int AGREEMENT_1NO_PARTY = 1801;
	public static final int AGREEMENT_5STRT_DT_GRTR_END = 1802;
	public static final int AGREEMENT_6NO_PRICE_ASSIGNMENT = 1803;
	public static final int AGREEMENT_7NO_CURRENCY_MATCH = 1804;
	public static final int AGREEMENT_8INVALID_RATE_TYPE = 1805;
	public static final int AGREEMENT_9PRICEITEM_NOT_PRICELIST = 1806;
	public static final int AGREEMENT_15DELTA_END_DT_REQUIRED = 1807;
	public static final int AGREEMENT_16OVERLAPPING_DT_RANGE = 1808;
	public static final int AGREEMENT_18MULTIPLE_TF_RATE = 1809;
	public static final int AGREEMENT_19DataMissingFromLineTable=1810;
	public static final int AGREEMENT_20DataMissingFromTierTable=1811;


	// ENH003 - Create Accounting Algorithm
	public static final int PI_CD_DESC_MISSING = 1070;
	public static final int TAX_RGME_CALC_DESC_NOT_FOUND = 1071;
	public static final int BANK_ACCT_NUM_MISSING = 1072;
	public static final int DS_CD_DESC_MISSING = 1073;
	public static final int ADJ_CD_DESC_MISSING = 1074;
	public static final int PER_TAXREG_MISSING = 1069;

	//	 ENH001 - Tax Determination Algorithm

	public static final int PROD_TAX_MISSING = 1081;
	public static final int BF_MISSING = 1082;
	public static final int BF_STAT_MISSING = 1083;
	public static final int TAX_STAT_OPTION_VAL_NOT_FOUND = 1084;
	public static final int SUPPLIER_TAXREG_MISSING = 1085;
	public static final int BILL_DATE_NOT_FOUND = 1086;
	public static final int BC_PERID_NOT_FOUND = 1087;
	public static final int SUPPLIER_LOCATION_MISSING = 1088;
	public static final int SUPPLIER_GEOGRAPHIC_CODE_MISSING = 1089;
	public static final int PRICE_CRCY_CODE_MISSING = 1090;

	//	 ENH009 - Minimum Charge Calculation Algorithm
	public static final int MC_TYPE_NOT_FOUND = 1077;
	public static final int PROD_NOT_FOUND=2001;



	//	 ENH011 - Create Debt Case Algorithm
	public static final int CASE_TYPE_NOT_FOUND = 1078;

	//INT035 Dunning_Letters Interface
	public static final int DUNNING_CASE_ID_NOT_FOUND = 3501;
	public static final int DUNNING_CC_ID_NOT_FOUND = 3502;
	public static final int DUNNING_CC_TYPE_CD_NOT_FOUND = 3503;
	public static final int DUNNING_CC_DTTM_NOT_FOUND = 3504;
	public static final int DUNNING_PER_ID_NOT_FOUND = 3505;
	public static final int DUNNING_PER_ID_NBR_NOT_FOUND = 3506;
	public static final int DUNNING_ACCT_ID_NOT_FOUND = 3507;
	public static final int DUNNING_ACCTTYPE_NOT_FOUND = 3508;
	public static final int DUNNING_BILL_ID_NOT_FOUND = 3509;
	public static final int DUNNING_BILL_DT_NOT_FOUND = 3510;
	public static final int DUNNING_DUE_DT_NOT_FOUND = 3511;
	public static final int DUNNING_BILL_AMT_NOT_FOUND = 3512;
	public static final int DUNNING_OUTSTANDING_AMT_NOT_FOUND = 3513;
	public static final int DUNNING_CURRENCY_CD_NOT_FOUND = 3514;

	//	ENH010 Tariff Rate Reprice Algorithm 		
	public static final int INVALID_PRODUCT_CODE_ERROR = 1701;	
	public static final int INVALID_PRICELISTID_ERROR = 1702;	
	public static final int MERCHANT_LEVEL_PRICING_ERROR = 1703;


	//	 INT006 Bill Cycle Upload Interface

	public static final int WINDOW_END_DT_BACKDATED_THAN_WINDOW_START_DT = 1121;
	public static final int WINDOW_END_DT_BACKDATED_THAN_ESTIMATION_DT = 1122;
	public static final int ESTIMATION_DT_BACKDATED_THAN_WINDOW_START_DATE = 1123;
	public static final int BILL_CYCLE_COULD_NOT_BE_DETRMINED = 1124;
	public static final int LANGUAGE_CODE_COULD_NOT_BE_DETERMINED = 1125;
	public static final int FREEZE_COMPLTE_SW_COULD_NOT_BE_DETERMINED = 1126;
	public static final int DESCR_COULD_NOT_BE_DETERMINED = 1127;

	//  INT009 Bill Period Upload Interface
	//public static final int BILL_DT_BACKDATED_THAN_SYSTEM_DT = 2501;
	public static final int BILL_DT_BACKDATED_THAN_CUTOFF_DT = 2502;
	public static final int BILL_PERIOD_COULD_NOT_BE_DETRMINED = 2503;
	public static final int LANG_CODE_COULD_NOT_BE_DETERMINED = 2504;
	public static final int DESCRIPTION_COULD_NOT_BE_DETERMINED = 2505;

	//	Reserve Algorithm 		
	public static final int ACCT_ID_NOT_FOUND = 1110;	
	public static final int FUND_SA_ID_NOT_FOUND = 1111;	
	public static final int RESERVE_SA_ID_NOT_FOUND = 1112;
	public static final int CTBD1_NOT_FOUND = 1113;
	public static final int CTBD2_NOT_FOUND = 1114;
	public static final int CTBD3_NOT_FOUND = 1115;
	public static final int CTBD4_NOT_FOUND = 1116;
	public static final int CTBD5_NOT_FOUND = 1117;
	public static final int CALC_AMT_NOT_FOUND = 1118;

	//	 IF112 Event Bill Interface Messages
	public static final int EVENTID_COULD_NOT_BE_DETERMINED = 1901;

	//	 INT007 Event Price Interface
	public static final int PRICEITEM_CD = 2401;
	public static final int CALC_AMT = 2402;
	public static final int BILL_REFERENCE = 2403;

	//	 INT008 Price Type Interface
	public static final int PRICETYPE_DESCR = 2301;
	public static final int CLASS_PTM = 2302;
	public static final int CLASS_GRP_CG = 2303;
	public static final int CLASS_GRP_CG_DESCR = 2304;
	public static final int CLASS_PTC_DESCR = 2305;

	//	INT010 Billable Charge Upload Program

	public static final int PER_ID_NOT_FOUND = 1501;	
	public static final int ACCOUNT_ID_NOT_FOUND = 1502;	
	public static final int SA_ID_NOT_FOUND = 1503;	
	public static final int CHG_AMT_AND_CRCY_CD_NOT_FOUND = 1504;
	public static final int BILL_PERIOD_CD_NOT_FOUND = 1505;
	public static final int CHG_TYPE_CD_NOT_FOUND = 1506;	
	public static final int SA_TYPE_CD_NOT_FOUND = 1507;
	public static final int PRICEITEM_CD_NOT_FOUND = 1508;
	public static final int DST_CD_NOT_FOUND = 1509;
	public static final int INVALID_BILL_CHG_DATA = 1510;
	public static final int END_DT_UPDATE = 1511;
	public static final int EVENT_ID_PRESENT = 1519; 
	public static final int BILL_AFTER_DT_NOT_NULL = 1520;

	//  Header Poller Job	
	public static final int HEADER_POLLING = 1300;

	//  WAF Algorithm

	public static final int WAF_CONTRACT_NOT_CREATED = 1301;
	public static final int FUND_SAID_NOT_FOUND = 1302;
	public static final int ACCTID_NOT_FOUND = 1303;	


	//	ADJ COntract Determination Algorithm	
	public static final int ADJ_CONTRACT_NOT_FOUND = 1305;

	// Accounting Data Interface
	public static final int ACCOUNTING_DATE = 1401;
	public static final int BUSINESS_UNIT = 1402;
	public static final int SCHEME_VAL = 1403;

	// Payment Confirmation Program	
	public static final int PAY_CNF_INVALID_FINANCIAL_DOC_DETAILS = 3101;
	public static final int PAY_CNF_NO_SA_ID= 3102;
	public static final int PAY_CNF_INVALID_ACCT_DETAILS = 3103;
	public static final int PAY_CNF_PREV_OPEN_MATCH_EVT_ID_NOT_FOUND = 3104;
	public static final int PAY_CNF_PREV_OPEN_MATCH_EVT_ID_NOT_CLOSED =3105;
	public static final int PAY_CNF_PAY_UPLD_NOT_UPDATED =3106;
	public static final int PAY_CNF_PEVT_DST_DTL_NOT_UPDATED =3107;
	public static final int PAY_CNF_TNDR_END_BAL_NOT_UPDATED =3108;
	public static final int PAY_CNF_BILL_MATCH_EVT_NOT_UPDATED =3109;
	public static final int PAY_CNF_MATCH_EVT_BAL_NOT_UPDATED =3110;
	public static final int PAY_CNF_TNDR_CTL_NOT_UPDATED =3111;
	public static final int PAY_CNF_DEP_CTL_NOT_UPDATED =3112;
	public static final int PAY_CNF_PAY_FROZEN_NOT_UPDATED =3113;
	public static final int PAY_CNF_FT_GL_NOT_UPDATED =3114;
	public static final int PAY_CNF_NO_DST_ID =3115	;
	public static final int PAY_CNF_ADJ_CHAR_NOT_UPDATED =3116	;
	public static final int PAY_CNF_BANK_ACCT_DETAILS =3117	;
	public static final int PAY_CNF_DUE_DT =3118	;

	//Merchant Hierarchy Change
	public static final int ACCT_NBR_TYP_CD_NOT_UPDATED =3119;
	public static final int ACCT_PER_REL_NOT_UPDATED =3120;
	public static final int BILL_CYC_CD_NOT_UPDATED =3121;
	public static final int SA_STATUS_FLG_NOT_UPDATED =3122;
	public static final int MASTER_SA_NOT_UPDATED =3123;
	public static final int MASTER_ACCT_CHAR_NOT_UPDATED =3124;

	
	//INT014 Bank Account Upload Interface

	public static final int BK_BANK_ACCT_NOT_FOUND = 3201;
	public static final int BK_CIS_DIVISION_NOT_FOUND = 3202;
	public static final int BK_DESCR_NOT_FOUND = 3203;
	public static final int BK_CURRENCY_CD_NOT_FOUND = 3204;
	public static final int BK_CURRENCY_CD_CAN_NOT_BE_UPDATED = 3205;

	//Invoice Recalc

	public static final int INV_RECALC_UPLD_FAIL = 4201;
	public static final int INV_RECALC_BATCH_EXECUTION_FAIL = 4202;

	//Write Off Interface

	public static final int PERSON_ID_NOT_FOUND = 1901;
	public static final int ACCT_NOT_FOUND = 1902;
	public static final int DIVISION_NOT_FOUND = 1903;
	public static final int ACCT_SA_NOT_FOUND = 1904;

	//INT019 Adjustment Type Interface

	public static final int AD_DST_CODE = 5201;
	public static final int AD_PRICEITEM = 5202;
	public static final int AD_ADJ_TYPE = 5203;
	public static final int AD_TAX = 5204;
	
	
	//INT034 Account Bill Cycle Update Interface

	public static final int PER_ID_NBR_NT_FOUND = 3401;
	public static final int ACCT_TYPE_NT_FOUND = 3402;
	public static final int CURRENCY_CD_NT_FOUND = 3403;
	public static final int ACCT_NBR_NT_FOUND = 3404;
	


	// Invoice Date Integrity 
	public static final int CM_INVOICE_DATA_LN_NO_RECORDS  = 4301;
	public static final int CM_INV_DATA_LN_BCL_NO_RECORDS = 4302;
	public static final int CM_INV_DATA_LN_RATE_NO_RECORDS = 4303;
	public static final int CM_INV_DATA_LN_SVC_QTY_NO_RECORDS = 4304;
	public static final int CM_INV_DATA_TAX_NO_REOCRDS = 4305;
	public static final int CM_TOTAL_CHARGES_MISMATCH = 4306;
	public static final int CM_LINE_CALCULATION_NOT_CORRECT = 4307;

	public static final int CM_INVOICE_DATA_LN_PRICE_RECORDS = 4308;
	public static final int CM_INVOICE_TAX_CALC_VERIFY = 4309;
    public static final int CM_MIN_CHARGE_BILLING_MISMATCH = 4310 ;
	public static final int CM_MIN_CHARGE_NON_BILLING_MISMATCH = 4311 ;

    public CustomMessages() {
		super(MESSAGE_CATEGORY);
	}

}
