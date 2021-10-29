/*******************************************************************************
* FileName                   : CustomTierAlgorithm_Impl.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : May 10, 2015 
* Version Number             : 0.4
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             May 10, 2015        Preeti Tiwari        Implemented Tiered Pricing and all the requirements for CD2.
0.2      NA             Aug 16, 2016        Preeti Tiwari        Implemented code review comments change.
0.3      NA             Apr 03, 2017        Vienna Rom			 Convert subquery to join, fixed sonar issues
0.4      NA             Jun 09, 2017        Vienna Rom	  		 Removed logger.debug and used StringBuilder
0.5      NA             May 28, 2018         RIA                 NAP-27796 Fix      
0.6      NA             Jun 19, 2018         RIA                 ORMB 2.6.0.1.0 upgrade fix   
*******************************************************************************/
package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.ibm.icu.text.DecimalFormat;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.RateComponentTypeLookup;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeaderData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentItemData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantityData;
import com.splwg.ccb.domain.common.characteristic.CharacteristicData;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.pricing.PricingMessages;
import com.splwg.ccb.domain.pricing.algorithm.aggregateSqs.AggregateServiceQuantities_Impl;
import com.splwg.ccb.domain.pricing.algorithm.ratecomponent.PriceComponentValueBean;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_Id;
import com.splwg.ccb.domain.pricing.pricecomp.PriceComp;
import com.splwg.ccb.domain.pricing.pricecomp.PriceComp_Id;
import com.splwg.ccb.domain.pricing.priceparm.PriceParamUtils;
import com.splwg.ccb.domain.pricing.ratecomponent.RcMap_Id;
import com.splwg.ccb.domain.rate.ApplyRateData;
import com.splwg.ccb.domain.rate.rateComponent.RateComponent;
import com.splwg.ccb.domain.rate.rateComponent.RateComponentValueAlgorithmSpot;
import com.splwg.ccb.domain.rate.rateVersion.ApplyRateVersionData;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 * @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = isErrorNoPrice, required = true, type = boolean)
 *            , @AlgorithmSoftParameter (name = tieringCondition, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = priceAssignidCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = personCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = accountCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = priceInformationRequired, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = priceCompIdCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = rateValueCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = noTieringQuantity, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = aggSrvQtyCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = acctExclusionCharTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = productUomRelFlag, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = stackingReq, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = hierarchyAlgoCd, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = srvQtyCharTypeCode, required = false, type = string)})
 */
public class CustomTierAlgorithm_Impl extends CustomTierAlgorithm_Gen implements
RateComponentValueAlgorithmSpot {
	private static final String TXN_AMT = "TXN_AMT";

	private static final String TXN_VOL = "TXN_VOL";

	private static final String MSC_PC = "MSC_PC";

	private static final String MSC_PI = "MSC_PI";

	private static final Logger logger = LoggerFactory.getLogger(CustomTierAlgorithm_Impl.class);

	private static final DecimalFormat format = new DecimalFormat("##################.##################");
	private String priceAsgnId;
	private String rcSeqNo;
	private String rsCode;
	private String acctId;
	private String personId;
	private String uomCode;
	private Date startDate;
	private Date endDate;
	private Date newStartDate;
	private Date newEndDate;
	private String saId;
	private String billId;
	private boolean isBilltemp;  
	private RateComponent rateComponent;
	private ApplyRateVersionData applyRateVersionData;
	private ApplyRateData applyRateData;
	private List<BillSegmentItemData> billSegmentItemData;
	private List<BillSegmentServiceQuantityData> billSegmentSQData;
	private List<BillSegmentCalculationHeaderData> bsegCalcHeaderData;
	private List<CharacteristicData> characteristicData;
	private BigDecimal crossReferenceAmount;
	private BigDecimal aggSqQuantity;
	private String priceCompId;
	private Bool isErrorNoPrice;
	private String tieringCondition;
	private String priceAssignidCharTypeCode;
	private String personCharTypeCode;
	private String accountCharTypeCode;
	private String priceInformationRequired;
	private String priceCompIdCharTypeCode;
	private String rateValueCharTypeCode;
	private String aggSqiQtyCharTypeCode;
	private String noTieringQuantity;
	private String acctExclusionCharTypeCode;
	private String productUomRelFlag;
	private String sqiQtyCharTypeCode;
	private BigDecimal value;
//	private String PA_PRICEASGN_ID = "PAPAIDCH";
//	private String PA_PER_ID = "PAPERCHR";
//	private String PA_ACCT_ID = "PAACCTCH";
	private String OPERAND_AND = "AND";
	private String OPERAND_OR = "OR";
	private String FLAT = "FLAT";
	private String INCREMENTAL_TIERED = "STEP";
	private String THRESHOLD_TIERED = "THRS";
	private String CHAR_COLLECTION_SEPERATOR_CODE = "***";
	private String CHAR_COLLECTION_SEPERATOR_VALUE = "***";
	private String YES = "Y";
	private String NO = "N";
	private String SA_PROPOSED_QUOTE_VALUE = "PROP";
	private String BILLTEMP_CHAR_TYPE = "BATCHSRC";
	private String TRIAL_BILL_CHAR_TYPE = "TRLBILL";
	private String BILLTEMP = "BILLTEMP";

	/**
	 * Initialize global variables
	 */
	public CustomTierAlgorithm_Impl()
	{
		this.priceAsgnId = null;
		this.rcSeqNo = null;
		this.rsCode = null;
		this.acctId = null;
		this.personId = null;
		this.uomCode = null;
		this.startDate = null;
		this.endDate = null;
		this.newStartDate = null;
		this.newEndDate = null;
		this.saId = null;
		this.billId = null;
		this.isBilltemp = false;	    
		this.aggSqQuantity = BigDecimal.ZERO;
		this.priceCompId = "";
		this.productUomRelFlag = NO;
	}

	public ApplyRateData getApplyRateData()
	{
		return this.applyRateData;
	}

	public ApplyRateVersionData getApplyRateVersionData()
	{
		return this.applyRateVersionData;
	}

	public List<BillSegmentCalculationHeaderData> getBillSegmentCalculationHeaderData()
	{
		return this.bsegCalcHeaderData;
	}

	public List<BillSegmentItemData> getBillSegmentItemData()
	{
		return this.billSegmentItemData;
	}

	public List<BillSegmentServiceQuantityData> getBillSegmentServiceQuantityData()
	{
		return this.billSegmentSQData;
	}

	public List<CharacteristicData> getCharacteristicData()
	{
		return this.characteristicData;
	}

	public BigDecimal getValue()
	{
		return this.value;
	}

	public void setValue(BigDecimal value)
	{
		this.value = value;
	}

	public void setApplyRateData(ApplyRateData applyRateData)
	{
		this.applyRateData = applyRateData;
	}

	public void setApplyRateVersionData(ApplyRateVersionData applyRateVersionData)
	{
		this.applyRateVersionData = applyRateVersionData;
	}

	public void setBillSegmentCalculationHeaderData(List<BillSegmentCalculationHeaderData> billSegmentCalculationHeaderData)
	{
		this.bsegCalcHeaderData = billSegmentCalculationHeaderData;
	}

	public void setBillSegmentItemData(List<BillSegmentItemData> billSegmentItemData)
	{
		this.billSegmentItemData = billSegmentItemData;
	}

	public void setBillSegmentServiceQuantityData(List<BillSegmentServiceQuantityData> billSegmentSqData)
	{
		this.billSegmentSQData = billSegmentSqData;
	}

	public void setCharacteristicData(List<CharacteristicData> characteristicData)
	{
		this.characteristicData = characteristicData;
	}

	public void setCrossReferenceAmount(BigDecimal crossReferenceAmount)
	{
		this.crossReferenceAmount = crossReferenceAmount;
	}

	public void setCrossReferenceFound(Bool crossReferenceFound)
	{
	}

	public void setCrossReferenceServiceQuantity(BigDecimal crossReferenceServiceQuantity)
	{
	}

	public void setRateComponent(RateComponent rateComponent)
	{
		this.rateComponent = rateComponent;
	}

	public void getValueAmount()
	{
	}

	public BigDecimal getAggSqQuantity()
	{
		return this.aggSqQuantity;
	}

	public String getPriceCompId()
	{
		return this.priceCompId;
	}

	public void setAggSqQuantity(BigDecimal aggSqQuantity)
	{
		this.aggSqQuantity = aggSqQuantity;
	}

	public void setPriceCompId(String priceCompId)
	{
		this.priceCompId = priceCompId;
	}

	/**
	 * Main processing.
	 */
	public void invoke()
	{
		readSoftParameters();
		initialize();
		validateInput();
		RateComponentBean rcBean = getRCMap();

		if (validate(rcBean))
		{
			if (FLAT.equalsIgnoreCase(rcBean.getTieredFlag().trim()) || INCREMENTAL_TIERED.equalsIgnoreCase(rcBean.getTieredFlag().trim()))
			{
				getValueAmt(rcBean.getRcMapId());
			} else if (THRESHOLD_TIERED.equalsIgnoreCase(rcBean.getTieredFlag().trim())) {
				List<PriceComp> priceComponentDetails = getPriceComponentList(rcBean.getRcMapId());
				if (validate(priceComponentDetails)) {
					iteratePriceComponent(priceComponentDetails);
				}
			}
		}
	}

	private void readSoftParameters()
	{
		this.isErrorNoPrice = getIsErrorNoPrice() != null ? getIsErrorNoPrice() : Bool.FALSE;
		this.tieringCondition = getTieringCondition();
		this.priceAssignidCharTypeCode = getPriceAssignidCharTypeCode();
		this.personCharTypeCode = getPersonCharTypeCode();
		this.accountCharTypeCode = getAccountCharTypeCode();
		this.priceInformationRequired = getPriceInformationRequired();
		this.priceCompIdCharTypeCode = getPriceCompIdCharTypeCode();
		this.rateValueCharTypeCode = getRateValueCharTypeCode();
		this.noTieringQuantity = getNoTieringQuantity();
		this.aggSqiQtyCharTypeCode = getAggSrvQtyCharTypeCode();
		this.acctExclusionCharTypeCode = getAcctExclusionCharTypeCode();
		this.productUomRelFlag = getProductUomRelFlag();
		if (this.productUomRelFlag == null) {
			this.productUomRelFlag = NO;
		}
		this.sqiQtyCharTypeCode = getSrvQtyCharTypeCode();
	}

	private void initialize()
	{
		List<CharacteristicData> charData = this.characteristicData;

		String trialBillId = "";
		if (validate(charData)) {
			for (CharacteristicData theCharData : charData) {
				if (null != theCharData.getCharacteristicType()) {
					String theCharDataValue=theCharData.getCharacteristicType().getId().getTrimmedValue();
					if (this.priceAssignidCharTypeCode.equalsIgnoreCase(theCharDataValue))
					{
						this.priceAsgnId = theCharData.getCharacteristicValue();
					}
					if (BILLTEMP_CHAR_TYPE.equalsIgnoreCase(theCharDataValue) && 
							theCharData.getCharacteristicValue() != null) {
						this.isBilltemp = theCharData.getCharacteristicValue().trim().equals(BILLTEMP);
					}
					if (this.personCharTypeCode.equalsIgnoreCase(theCharDataValue))
					{
						this.personId = theCharData.getCharacteristicValue();
					}
					if (this.accountCharTypeCode.equalsIgnoreCase(theCharDataValue))
					{
						this.acctId = theCharData.getCharacteristicValue();
					}
					if (TRIAL_BILL_CHAR_TYPE.equalsIgnoreCase(theCharDataValue) && 
							theCharData.getCharacteristicValue() != null) {
						trialBillId = theCharData.getCharacteristicValue();
					}
				}
			}
		}

		if (validate(this.rateComponent)) {
			this.rcSeqNo = this.rateComponent.getId().getRcSequence().toString();
		}

		if (validate(this.applyRateData)) {
			this.rsCode = this.applyRateData.getRateSchedule().getId().getTrimmedValue();
			this.startDate = this.applyRateData.getBillSegmentPeriodStart();
			this.endDate = this.applyRateData.getBillSegmentPeriodEnd();
			this.saId = this.applyRateData.getServiceAgreement().getId().getTrimmedValue();
			//Bill bill = this.applyRateData.getBill();
			//Start Change RIA
			Bill bill = new Bill_Id(this.applyRateData.getBillId()).getEntity();
			//End Change RIA
			if (bill != null)
				this.billId = bill.getId().getTrimmedValue();
			else {
				this.billId = trialBillId;
			}
		}
		//****************Added logic*************
		int tcnmCharTypeValue = 0;
		String tcicCharTypeValue = "";
		PreparedStatement preStmt1=null;
		PreparedStatement preStmt2=null;
		
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT PC.ADHOC_CHAR_VAL");
		stringBuilder.append(" FROM CI_PER_CHAR PC, CI_ACCT_PER AP, CI_SA SA");
		stringBuilder.append(" WHERE PC.PER_ID=AP.PER_ID");
		stringBuilder.append(" AND AP.ACCT_ID=SA.ACCT_ID");
		stringBuilder.append(" AND SA.SA_ID=:saId");
		stringBuilder.append(" AND PC.CHAR_TYPE_CD=:charTypeCd");
		
		try {
			preStmt2 = createPreparedStatement(stringBuilder.toString(),"");
			preStmt2.bindId("saId", new ServiceAgreement_Id(this.saId));
			preStmt2.bindId("charTypeCd", new CharacteristicType_Id("TCIC"));
			preStmt2.setAutoclose(false);
			if(notNull(preStmt2.firstRow())) {
				tcicCharTypeValue = preStmt2.firstRow().getString("ADHOC_CHAR_VAL");
			}else{
				addError(PricingMessages.personCharTypeCodeMissing());
			}
		} catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (preStmt2 != null) {
				preStmt2.close();
				preStmt2 = null;
			}
		}

		try {
			preStmt1 = createPreparedStatement(stringBuilder.toString(),"");
			preStmt1.bindId("saId", new ServiceAgreement_Id(this.saId));
			preStmt1.bindId("charTypeCd", new CharacteristicType_Id("TCNM"));
			preStmt1.setAutoclose(false);
			if(notNull(preStmt1.firstRow())) {
				tcnmCharTypeValue = Integer.parseInt(preStmt1.firstRow().getString("ADHOC_CHAR_VAL"));
			}else{
				addError(PricingMessages.personCharTypeCodeMissing());
			}
		} catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (preStmt1 != null) {
				preStmt1.close();
				preStmt1 = null;
			}
		}

		if (YES.equalsIgnoreCase(tcicCharTypeValue)) {
			this.newStartDate=this.startDate.addMonths(-tcnmCharTypeValue+1);
			int firstDay=this.newStartDate.getDay();
			this.newStartDate=this.newStartDate.addDays(-firstDay+1);
			this.newEndDate = new Date(this.endDate.getYear(), this.endDate.getMonth(), this.endDate.getMonthValue().getDaysInMonth());

		}else if (NO.equalsIgnoreCase(tcicCharTypeValue)) {
			this.newStartDate=this.startDate.addMonths(-tcnmCharTypeValue);
			int firstDay=this.newStartDate.getDay();
			this.newStartDate=this.newStartDate.addDays(-firstDay+1);
			this.newEndDate=this.endDate.addMonths(-1);
			this.newEndDate = new Date(this.newEndDate.getYear(), this.newEndDate.getMonth(), this.newEndDate.getMonthValue().getDaysInMonth());
		}	    

		//****************************************

		if (!validate(this.acctId) && !validate(this.personId)) {
			this.acctId = getAccountID();
		}
	}

	private void validateInput()
	{
		if (!validate(this.rcSeqNo)) {
			addError(PricingMessages.rcSeqNoMissing());
		}
		if (!validate(this.rsCode)) {
			addError(PricingMessages.rsCodeMissing());
		}
		if (!validate(this.priceAsgnId)) {
			addError(PricingMessages.priceAssignIdMissing());
		}

		if (!validate(this.priceAssignidCharTypeCode)) {
			addError(PricingMessages.priceAssignCharTypeCodeMissing());
		}
		if (!validate(this.personCharTypeCode)) {
			addError(PricingMessages.personCharTypeCodeMissing());
		}
		if (!validate(this.accountCharTypeCode)) {
			addError(PricingMessages.accountCharTypeCodeMissing());
		}
		if (!validate(this.acctId) && !validate(this.personId)) {
			addError(PricingMessages.personIdAccountIdMissing());
		}
		if (YES.equalsIgnoreCase(this.priceInformationRequired)) {
			if (!validate(this.priceCompIdCharTypeCode)) {
				addError(PricingMessages.pricecompIdCharTypeCodeMissing());
			}
			if (!validate(this.rateValueCharTypeCode)) {
				addError(PricingMessages.rateValueCharTypeCodeMissing());
			}
		}
	}

	private RateComponentBean getRCMap()
	{
		PreparedStatement preStmt;
		StringBuilder rcMapBuilder = new StringBuilder();

		if (notBlank(CommonUtils.CheckNull(this.sqiQtyCharTypeCode))){
			rcMapBuilder.append("SELECT A.RC_MAP_ID,A.TIERED_FLAG FROM CI_RC_MAP A,CI_PRICEASGN B WHERE B.PRICE_ASGN_ID=:priceAssignId ");
			rcMapBuilder.append("AND A.RS_CD=:rscode ");
			rcMapBuilder.append("AND A.RC_SEQ=(SELECT C.RC_SEQ FROM CI_RC_CHAR C WHERE C.RS_CD=:rscode AND C.CHAR_TYPE_CD='RATE_TP' AND C.CHAR_VAL='MSC_PC') ");
			rcMapBuilder.append("AND A.RS_CD=B.RS_CD ");
			rcMapBuilder.append("order by A.Effdt desc");

			preStmt = createPreparedStatement(rcMapBuilder.toString(),"");
			//RIA: Added for NAP-27796
			preStmt.setAutoclose(false);

			preStmt.bindId("priceAssignId", new PriceAsgn_Id(this.priceAsgnId));
			preStmt.bindString("rscode", this.rsCode, "RS_CD");
		}else{
			rcMapBuilder.append("SELECT A.RC_MAP_ID,A.TIERED_FLAG FROM CI_RC_MAP A,CI_PRICEASGN B WHERE B.PRICE_ASGN_ID=:priceAssignId ");
			rcMapBuilder.append("AND A.RS_CD=:rscode ");
			rcMapBuilder.append("AND A.RC_SEQ=:rcSeqNo ");
			rcMapBuilder.append("AND A.RS_CD=B.RS_CD ");
			rcMapBuilder.append("order by A.Effdt desc");

			preStmt = createPreparedStatement(rcMapBuilder.toString(),"");
			//RIA: Added for NAP-27796
			preStmt.setAutoclose(false);

			preStmt.bindId("priceAssignId", new PriceAsgn_Id(this.priceAsgnId));
			preStmt.bindString("rscode", this.rsCode, "RS_CD");
			preStmt.bindString("rcSeqNo", this.rcSeqNo, "RC_SEQ");
		}    

		RateComponentBean rcBean = null;

		SQLResultRow result = preStmt.firstRow();
		if (notNull(result)) {
			rcBean = new RateComponentBean();
			rcBean.setRcMapId(result.getString("RC_MAP_ID"));
			rcBean.setTieredFlag(result.getString("TIERED_FLAG"));
		}
		//RIA: Added for NAP-27796
		if(preStmt !=null){
			preStmt.close();
		}

		return rcBean;
	}
	
	private void getValueAmt(String rcMapId)
	{
		Query<QueryResultRow> queryRec = createQuery("From PriceComp priceComp where priceComp.priceAsgnId= :priceAsgnID and priceComp.rcMapId=:rcMapID","");
		queryRec.bindId("priceAsgnID", new PriceAsgn_Id(this.priceAsgnId));
		queryRec.bindId("rcMapID", new RcMap_Id(rcMapId));

		queryRec.addResult("priceCompID", "priceComp.id");
		queryRec.addResult("valueAmt", "priceComp.valueAmt");

		RateComponentTypeLookup rcType = this.rateComponent.getRateComponentType();

		if (queryRec.firstRow() == null && (rcType.isMinimumCharge() || rcType.isMaximumCharge() || rcType.isExactCharge()))
		{
			setValue(this.crossReferenceAmount);
		} else {
			QueryIterator<QueryResultRow> queryIterator = queryRec.iterate();
			while (queryIterator.hasNext()) {
				QueryResultRow qrc = queryIterator.nextRow();
				this.value = qrc.getBigDecimal("valueAmt");
				setValue(new BigDecimal(format.format(this.value)));
				setPriceCompId(((PriceComp_Id)qrc.get("priceCompID")).getIdValue());
				setAggSqQuantity(BigDecimal.ZERO);
				if (YES.equalsIgnoreCase(this.priceInformationRequired)) {
					appendCharDataList(getPriceCompId(), getAggSqQuantity(), INCREMENTAL_TIERED);
				}
			}
		}
	}

	private List<PriceComp> getPriceComponentList(String rcMapId)
	{
		Query<PriceComp> queryRec = createQuery(" from PriceComp a where a.priceAsgnId=:priceAsgnID and a.rcMapId=:rcMapID ", "get Price Component");

		queryRec.bindId("priceAsgnID", new PriceAsgn_Id(this.priceAsgnId));
		queryRec.bindId("rcMapID", new RcMap_Id(rcMapId));

		return queryRec.list();
	}

	private void iteratePriceComponent(List<PriceComp> priceComponentList)
	{
		Map<String, BigDecimal> map = new HashMap<String, BigDecimal>();
		int size = 0;
		for (PriceComp priceComp : priceComponentList)
		{
			String pcId = priceComp.getId().getIdValue();
			++size;
			PriceComponentValueBean pcValueBean = getPriceComponentValue(pcId, map);
			if (pcValueBean.isRateValueFlag()) {
				BigDecimal tempValue = priceComp.getValueAmt();
				this.value = tempValue.setScale(2, BigDecimal.ROUND_HALF_UP);
				setValue(new BigDecimal(format.format(this.value)));
				setPriceCompId(pcId);
				setAggSqQuantity(pcValueBean.getSqiQuantity());
				if (!(YES.equalsIgnoreCase(this.priceInformationRequired))) {
					break;
				}
			}

			if (priceComponentList.size() == size && !pcValueBean.isRateValueFlag() && this.isErrorNoPrice.isTrue() && YES.equalsIgnoreCase(this.noTieringQuantity))
			{
				addError(PricingMessages.noMatchRateFound(this.priceAsgnId));
			}
		}
	}

	protected boolean isQuoteAvailable(String contractId)
	{
		boolean isQuote = false;

		ServiceAgreement sa = new ServiceAgreement_Id(contractId).getEntity();

		if (sa != null && sa.getSpecialUsage() != null && SA_PROPOSED_QUOTE_VALUE.equals(sa.getSpecialUsage().trimmedValue()))
		{
			isQuote = true;
		}

		return isQuote;
	}

	private PriceComponentValueBean getPriceComponentValue(String priceCompId, Map<String, BigDecimal> map)
	{
		StringBuilder priceCompValueBuilder = new StringBuilder();
		priceCompValueBuilder.append("SELECT A.PRICEITEM_CD,A.TOU_CD,A.LOWER_LIMIT,A.Upper_Limit, B.SQI_CD, C.PRICEITEM_PAR_CD, A.PRICEITEM_PARM_GRP_ID from CI_PRICECOMP_TIER A,CI_PRICECRITERIA B,CI_PRICEITEM_REL C ");

		priceCompValueBuilder.append("where A.PRICECOMP_ID =:priceCompId ");
		priceCompValueBuilder.append("and A.PRICECRITERIA_CD=B.PRICECRITERIA_CD AND A.PRICECRITERIA_CD=C.PRICEITEM_REL_TYPE_FLG ");
		priceCompValueBuilder.append("order by A.TIER_SEQNO");

		PreparedStatement prpStmt = createPreparedStatement(priceCompValueBuilder.toString(),"");
		prpStmt.bindId("priceCompId", new PriceComp_Id(priceCompId));
		prpStmt.setAutoclose(false);
		BigDecimal serviceQuantityTemp = BigDecimal.ZERO;
		AggregateServiceQuantities_Impl aggServiceQuantityObj;
		BigDecimal serviceQuantity = BigDecimal.ZERO;

		boolean matchFound = false;
		PriceComponentValueBean priceCompValueBean = new PriceComponentValueBean();

		boolean isProposalContract = false;
		if (isQuoteAvailable(this.saId)) {
			isProposalContract = true;
		}	    

		boolean isPriceParam = PriceParamUtils.isPriceParams();
		BigInteger ppgID = BigInteger.ONE;

		for (SQLResultRow result : prpStmt.list()) {
			String priceItemCd = result.getString("PRICEITEM_CD");
			String timeofUseCd = result.getString("TOU_CD");
			BigDecimal lowerLimit = result.getBigDecimal("LOWER_LIMIT");
			BigDecimal upperLimit = result.getBigDecimal("UPPER_LIMIT");
			String servQtyCd = result.getString("SQI_CD");
			String compareServQtyCd = result.getString("PRICEITEM_PAR_CD");

			if (notBlank(CommonUtils.CheckNull(this.sqiQtyCharTypeCode))){
				servQtyCd=this.sqiQtyCharTypeCode;
			}
			//*********ADDED//
			if (MSC_PI.equals(CommonUtils.CheckNull(servQtyCd).trim())){
				servQtyCd=TXN_VOL;
			}else if (MSC_PC.equals(CommonUtils.CheckNull(servQtyCd).trim())){
				servQtyCd=TXN_AMT;
			}
			if (MSC_PI.equals(CommonUtils.CheckNull(compareServQtyCd).trim())){
				compareServQtyCd=TXN_VOL;
			}else if (MSC_PC.equals(CommonUtils.CheckNull(compareServQtyCd).trim())){
				compareServQtyCd=TXN_AMT;
			}
			
			BigDecimal childServiceQuantityTemp = BigDecimal.ZERO;
			BigDecimal newServiceQuantityTemp = BigDecimal.ZERO;
			BigDecimal newServiceQuantity = BigDecimal.ZERO;

			aggServiceQuantityObj = new AggregateServiceQuantities_Impl();
			aggServiceQuantityObj.isProposalContract = isProposalContract;
			aggServiceQuantityObj.productUomRelFlag = this.productUomRelFlag;

			//*************************Assign Input Parameter value to the SQI_CD in order to calculate Base % Charge and FX Income*************//	          

			//********ADDED//

			serviceQuantityTemp = aggServiceQuantityObj.getServiceQuantity(this.personId, this.acctId, priceItemCd, this.uomCode, servQtyCd, timeofUseCd, this.startDate, this.endDate, this.saId, this.acctExclusionCharTypeCode, ppgID, isPriceParam, this.billId, this.isBilltemp, getHierarchyAlgoCd());

			//*********************Added Logic*******************************//
			StringBuilder priceItemCodeBuilder = new StringBuilder();
			priceItemCodeBuilder.append("select C.PRICEITEM_CHLD_CD from ci_priceitem_rel C, CI_PRICECOMP_TIER A, CI_PRICECRITERIA B, ci_priceitem_rel D" +
					" where C.PRICEITEM_PAR_CD=B.PRICECRITERIA_CD AND C.PRICEITEM_REL_TYPE_FLG='TIER'" +
					" AND A.PRICECOMP_ID =:priceCompId and A.PRICECRITERIA_CD=D.PRICEITEM_REL_TYPE_FLG and D.PRICEITEM_CHLD_CD=B.PRICECRITERIA_CD");

			PreparedStatement priceItemCodeStmt = createPreparedStatement(priceItemCodeBuilder.toString(),"");
			//RIA: Added for NAP-27796
			priceItemCodeStmt.setAutoclose(false);
			priceItemCodeStmt.bindId("priceCompId", new PriceComp_Id(priceCompId));

			List<SQLResultRow> priceItemCodeBuilderList = priceItemCodeStmt.list();
			
			if (MSC_PI.equals(CommonUtils.CheckNull(servQtyCd).trim())){
				servQtyCd=TXN_VOL;
			}
			if (MSC_PC.equals(CommonUtils.CheckNull(servQtyCd).trim())){
				servQtyCd=TXN_AMT;
			}

			for (Iterator<SQLResultRow> itr1 = priceItemCodeBuilderList.iterator(); itr1.hasNext(); ) {
				SQLResultRow result1 = itr1.next();
				String childPriceItemCd = result1.getString("PRICEITEM_CHLD_CD");

				childServiceQuantityTemp = aggServiceQuantityObj.getServiceQuantity(this.personId, this.acctId, childPriceItemCd, this.uomCode, compareServQtyCd, timeofUseCd, this.newStartDate, this.newEndDate, this.saId, this.acctExclusionCharTypeCode, ppgID, isPriceParam, this.billId, this.isBilltemp, getHierarchyAlgoCd());
				newServiceQuantityTemp = newServiceQuantityTemp.add(childServiceQuantityTemp);
			}
			//RIA: Added for NAP-27796
			if(priceItemCodeStmt !=null){
				priceItemCodeStmt.close();
			}
			
			//*********************Added Logic*******************************//

			serviceQuantity = serviceQuantityTemp;
			newServiceQuantity = newServiceQuantityTemp;

			ServiceAgreement sa = new ServiceAgreement_Id(this.saId).getEntity();

			if (isBlankOrNull(this.billId) && !(sa.getProposalSAStatus().isQuotableProposal()))
				serviceQuantity = serviceQuantity.add(getSQFromBillableCharge(servQtyCd));

			map.put(priceItemCd + timeofUseCd + servQtyCd, serviceQuantity);

			if (OPERAND_AND.equalsIgnoreCase(this.tieringCondition)) {
				if ((lowerLimit.intValue() != 0) && (NO.equalsIgnoreCase(this.noTieringQuantity)))
				{
					if ((newServiceQuantity.compareTo(lowerLimit) > 0) && ((newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
					{
						matchFound = true;
					} else {
						priceCompValueBean.setRateValueFlag(false);
						priceCompValueBean.setSqiQuantity(serviceQuantity);
						return priceCompValueBean;
					}
				}

				if ((lowerLimit.intValue() == 0) && (NO.equalsIgnoreCase(this.noTieringQuantity)))
				{
					if (((newServiceQuantity.compareTo(lowerLimit) > 0) || (newServiceQuantity.compareTo(lowerLimit) == 0)) && ((newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
					{
						matchFound = true;
					} else {
						priceCompValueBean.setRateValueFlag(false);
						priceCompValueBean.setSqiQuantity(serviceQuantity);
						return priceCompValueBean;
					}

				}

				if (YES.equalsIgnoreCase(this.noTieringQuantity))
				{
					if ((newServiceQuantity.compareTo(lowerLimit) > 0) && ((newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
					{
						matchFound = true;
					} else {
						priceCompValueBean.setRateValueFlag(false);
						priceCompValueBean.setSqiQuantity(serviceQuantity);
						return priceCompValueBean;
					}
				}

			}
			else if (OPERAND_OR.equalsIgnoreCase(this.tieringCondition)) {
				if ((lowerLimit.intValue() != 0) && (NO.equalsIgnoreCase(this.noTieringQuantity)) && 
						(newServiceQuantity.compareTo(lowerLimit) > 0) && (
								(newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
				{
					priceCompValueBean.setRateValueFlag(true);
					priceCompValueBean.setSqiQuantity(serviceQuantity);
					return priceCompValueBean;
				}

				if ((lowerLimit.intValue() == 0) && (NO.equalsIgnoreCase(this.noTieringQuantity)) && 
						((newServiceQuantity.compareTo(lowerLimit) > 0) || (newServiceQuantity.compareTo(lowerLimit) == 0)) && (
								(newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
				{
					priceCompValueBean.setRateValueFlag(true);
					priceCompValueBean.setSqiQuantity(serviceQuantity);
					return priceCompValueBean;
				}

				if ((YES.equalsIgnoreCase(this.noTieringQuantity)) && 
						(newServiceQuantity.compareTo(lowerLimit) > 0) && (
								(newServiceQuantity.compareTo(upperLimit) < 0) || (newServiceQuantity.compareTo(upperLimit) == 0) || ((upperLimit.compareTo(BigDecimal.ZERO) == 0) && (upperLimit.compareTo(lowerLimit) < 0))))
				{
					priceCompValueBean.setRateValueFlag(true);
					priceCompValueBean.setSqiQuantity(serviceQuantity);
					return priceCompValueBean;
				}

			}

		}

		priceCompValueBean.setRateValueFlag(matchFound);
		priceCompValueBean.setSqiQuantity(serviceQuantity);
		if(notNull(prpStmt)){
			prpStmt.close();
		}
		return priceCompValueBean;
	}

	protected static boolean validate(Object obj)
	{
		if (obj == null) {
			return false;
		}
		return obj.toString().trim().length() != 0;
	}

	private void appendCharDataList(String priceCompIdValue, BigDecimal sqiQuantity, String tieredtypeFlag)
	{
		List<CharacteristicData> charData = this.characteristicData;
		Iterator<CharacteristicData> charItr;
		if (validate(charData)) {
			for (charItr = charData.iterator(); charItr.hasNext(); ) {
				CharacteristicData theCharData = charItr.next();
				if (null != theCharData.getCharacteristicType()) {
					if (CHAR_COLLECTION_SEPERATOR_CODE.equalsIgnoreCase(theCharData.getCharacteristicType().getId().getTrimmedValue()))
					{
						//Remove CHAR_COLLECTION_SEPERATOR_CODE
						charItr.remove();
					}
					if (this.priceCompIdCharTypeCode.equalsIgnoreCase(theCharData.getCharacteristicType().getId().getTrimmedValue()))
					{
						//Remove priceCompIdCharTypeCode
						charItr.remove();
					}
					if (this.rateValueCharTypeCode.equalsIgnoreCase(theCharData.getCharacteristicType().getId().getTrimmedValue()))
					{
						//Remove rateValueCharTypeCode
						charItr.remove();
					}
				}
			}
		}

		CharacteristicData charCollectionSepData = CharacteristicData.Factory.newInstance();
		charCollectionSepData.setCharacteristicType((CharacteristicType)new CharacteristicType_Id(CHAR_COLLECTION_SEPERATOR_CODE).getEntity());

		charCollectionSepData.setCharacteristicValue(CHAR_COLLECTION_SEPERATOR_VALUE);
		this.characteristicData.add(charCollectionSepData);

		CharacteristicData priceCompIdCharData = CharacteristicData.Factory.newInstance();
		priceCompIdCharData.setCharacteristicType((CharacteristicType)new CharacteristicType_Id(this.priceCompIdCharTypeCode).getEntity());
		priceCompIdCharData.setCharacteristicValue(priceCompIdValue);
		this.characteristicData.add(priceCompIdCharData);

		CharacteristicData rateValueCharData = CharacteristicData.Factory.newInstance();
		rateValueCharData.setCharacteristicType((CharacteristicType)new CharacteristicType_Id(this.rateValueCharTypeCode).getEntity());
		rateValueCharData.setCharacteristicValue(format.format(this.value));
		this.characteristicData.add(rateValueCharData);

		if (THRESHOLD_TIERED.equalsIgnoreCase(tieredtypeFlag)) {
			CharacteristicData aggSrvQtyCharData = CharacteristicData.Factory.newInstance();
			aggSrvQtyCharData.setCharacteristicType((CharacteristicType)new CharacteristicType_Id(this.aggSqiQtyCharTypeCode).getEntity());
			aggSrvQtyCharData.setCharacteristicValue(format.format(sqiQuantity));
			this.characteristicData.add(aggSrvQtyCharData);
		}
	}

	public String getAccountID()
	{
		return (new ServiceAgreement_Id(this.saId).getEntity()).getAccount().getId().getIdValue();
	}

	private BigDecimal getSQFromBillableCharge(String servQtyCd)
	{
		BigDecimal serviceQuantity = new BigDecimal(format.format(0L));

		List<BillSegmentServiceQuantityData> list = getBillSegmentServiceQuantityData();

		if (list != null)
		{
			for (BillSegmentServiceQuantityData bsData : list) {
				if (bsData.getBillSegmentServiceQuantityDto().getId().getServiceQuantityIdentifier().equals(servQtyCd))
				{
					BigDecimal billableServiceQuantity = bsData.getBillSegmentServiceQuantityDto().getBillableServiceQuantity();

					serviceQuantity = serviceQuantity.add(billableServiceQuantity);
				}
			}
		}

		return serviceQuantity;
	}
	private class RateComponentBean
	{
		private String rcMapId;
		private String tieredFlag;

		public String getRcMapId()
		{
			return this.rcMapId;
		}

		public void setRcMapId(String rcMapId) {
			this.rcMapId = rcMapId;
		}

		public String getTieredFlag() {
			return this.tieredFlag;
		}

		public void setTieredFlag(String tieredFlag) {
			this.tieredFlag = tieredFlag;
		}
	}
}