package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.List;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeaderData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentItemData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantityData;
import com.splwg.ccb.domain.common.characteristic.CharacteristicData;
import com.splwg.ccb.domain.rate.ApplyRateData;
import com.splwg.ccb.domain.rate.rateComponent.RateComponent;
import com.splwg.ccb.domain.rate.rateComponent.RateComponentValueAlgorithmSpot;
import com.splwg.ccb.domain.rate.rateVersion.ApplyRateVersionData;

/**
 * @author Swapnil
 *
@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = rcSequence1, required = true, type = integer)
 *            , @AlgorithmSoftParameter (name = multiplier, required = true, type = decimal)
 *            , @AlgorithmSoftParameter (name = roundToDecimal, required = true, type = integer)})
 */
public class CmMultiplyRateCompVal_Impl extends CmMultiplyRateCompVal_Gen implements
		RateComponentValueAlgorithmSpot {

	private ApplyRateData applyRateData;
	private ApplyRateVersionData applyRateVersionData;
	private List<BillSegmentCalculationHeaderData> billSegmentCalculationHeaderDataList;
	private List<BillSegmentItemData> billSegmentItemDataList;
	private List<BillSegmentServiceQuantityData> billSegmentServiceQuantityDataList;
	private List<CharacteristicData> characteristicDataList;
	private BigDecimal rateValue;
	private RateComponent rc;
	
	
	@Override
	public void invoke() {
		
			BigDecimal amount1=BigDecimal.ZERO;
			BigDecimal amount2=BigDecimal.ZERO;
			boolean firstFound=false;
			boolean secondFound=false;
		
			BillSegmentCalculationHeaderData bsegHeader = billSegmentCalculationHeaderDataList.get(0);
			List<BillCalculationLineData> calcLines = bsegHeader.getBillCalculationLineData();
			
			if(calcLines!=null && calcLines.size()>0) {
			
				
				for(BillCalculationLineData calcLine:  calcLines) {
							
					BigInteger calcLineSeq = calcLine.getRcSequence();
					 
					if(calcLineSeq.compareTo(getRcSequence1())==0){
						amount1= calcLine.getCalculatedAmount();
						firstFound=true;
					}
					
					/**if(calcLineSeq.compareTo(getMultiplier())==0){
						amount2= calcLine.getCalculatedAmount();
						secondFound=true;
					}**/
					    
					
					if(firstFound){
						break;
					}
				}
			}
	
		rateValue=getMultiplier().multiply(amount1);
		
		/** Set Rounding as per input decimal **/
		rateValue= rateValue.setScale(getRoundToDecimal().intValue(), BigDecimal.ROUND_HALF_DOWN);
			
	}

	@Override
	public ApplyRateData getApplyRateData() {

		return applyRateData;
	}

	@Override
	public ApplyRateVersionData getApplyRateVersionData() {

		return applyRateVersionData;
	}

	@Override
	public List<BillSegmentCalculationHeaderData> getBillSegmentCalculationHeaderData() {

		return billSegmentCalculationHeaderDataList;
	}

	@Override
	public List<BillSegmentItemData> getBillSegmentItemData() {

		return billSegmentItemDataList;
	}

	@Override
	public List<BillSegmentServiceQuantityData> getBillSegmentServiceQuantityData() {

		return billSegmentServiceQuantityDataList;
	}

	@Override
	public List<CharacteristicData> getCharacteristicData() {

		return characteristicDataList;
	}

	@Override
	public BigDecimal getValue() {

		return rateValue;
	}

	@Override
	public void setApplyRateData(ApplyRateData applyratedata) {
		this.applyRateData=applyratedata;
	}

	@Override
	public void setApplyRateVersionData(
			ApplyRateVersionData applyrateversiondata) {
		this.applyRateVersionData=applyrateversiondata;
		
	}

	@Override
	public void setBillSegmentCalculationHeaderData(
			List<BillSegmentCalculationHeaderData> list) {
		
		this.billSegmentCalculationHeaderDataList=list;
	}

	@Override
	public void setBillSegmentItemData(List<BillSegmentItemData> list) {
		this.billSegmentItemDataList=list;
		
	}

	@Override
	public void setBillSegmentServiceQuantityData(
			List<BillSegmentServiceQuantityData> list) {
		this.billSegmentServiceQuantityDataList=list;
		
	}

	@Override
	public void setCharacteristicData(List<CharacteristicData> list) {
		this.characteristicDataList=list;
	}

	@Override
	public void setCrossReferenceAmount(BigDecimal bigdecimal) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCrossReferenceFound(Bool bool) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setCrossReferenceServiceQuantity(BigDecimal bigdecimal) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setRateComponent(RateComponent ratecomponent) {
			this.rc=ratecomponent;
	}

	@Override
	public BigDecimal getAggSqQuantity() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getPriceCompId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setAggSqQuantity(BigDecimal arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void setPriceCompId(String arg0) {
		// TODO Auto-generated method stub
		
	}
}
