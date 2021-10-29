/*******************************************************************************
* FileName                   : MinChargeBsegTotNetChargeCalc_Impl.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Apr 01, 2016 
* Version Number             : 1.0
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Apr 01, 2016        Preeti Tiwari        Implemented all requirements for NAP-4.
0.2      NA             Aug 15, 2016        Preeti Tiwari        Implemented code review comments.  
0.3      NA             Apr 03, 2017        Vienna Rom			 Convert subquery to join, fixed sonar issues
0.4      NA    			Jun 07, 2017		Ankur Jain			 NAP-14404 fix
0.5      NA             Jun 09, 2017         Vienna Rom			 Removed logger.debug and used StringBuilder
0.6      NA             Sep 15, 2017         Ankur Jain			 NAP-19123 Redesign Implementation for performance change 
0.7      NA             Jan 03, 2018         Ankur Jain			 PAM-16876: (PROD DEFECT - Incorrect MMSC when price changed mid-month) Fix
0.8      NA             Jun 10, 2018         RIA                 Changes for ORMB2.6.0.1.0 upgrade
0.9      NA             Oct 31, 2018         RIA                 NAP-33825,NAP-35671 use winEndDt, perf changes
1.0	     NA				Mar 03, 2019	     Amandeep		     Min Charge Fix at Outlet level
*******************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicValue;
import com.splwg.base.domain.common.characteristicType.CharacteristicValue_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.security.user.User_Id;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.api.lookup.GlDistributionStatusLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.admin.billCycle.BillCycleSchedule;
import com.splwg.ccb.domain.admin.billPeriod.BillPeriod_Id;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode_Id;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic_Id;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeaderData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentItemData;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentServiceQuantityData;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.common.characteristic.CharacteristicData;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_DTO;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.ccb.domain.rate.ApplyRateData;
import com.splwg.ccb.domain.rate.rateComponent.RateComponent;
import com.splwg.ccb.domain.rate.rateComponent.RateComponentValueAlgorithmSpot;
import com.splwg.ccb.domain.rate.rateVersion.ApplyRateVersionData;
import com.splwg.ccb.domain.rate.rateVersion.RateVersion_Id;
import com.splwg.cm.domain.wp.batch.InvoiceDataInterfaceLookUp;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;


/**
 * Apply a minimum charge for the bill period if the merchants total charges for that period are less than a specified amount.
 *
 * @author Preeti
 *
@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter ( name = "DstIdValue1", required = true, type = string)
 *            , @AlgorithmSoftParameter ( name = "DstIdValue2", required = true, type = string)
 *            , @AlgorithmSoftParameter ( name = "ProductFlagValue", required = true, type = string)})
 */
/**
 * @author tutejaa105
 *
 */
public class MinChargeBsegTotNetChargeCalc_Impl extends MinChargeBsegTotNetChargeCalc_Gen implements
		RateComponentValueAlgorithmSpot {
	private static final Logger logger = LoggerFactory.getLogger(MinChargeBsegTotNetChargeCalc_Impl.class);
	private List<CharacteristicData> characteristicData;
	private ApplyRateData applyRateData;
	private ApplyRateVersionData applyRateVersionData;
	BigInteger envId= InstallationHelper.getEnvironmentId();


	private BigDecimal value;
	CharacteristicType_Id valueAmtChar = new CharacteristicType_Id("CMVALAMT");
	public static InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp(); 
	String personId=null;

	public MinChargeBsegTotNetChargeCalc_Impl()
	{
		value=BigDecimal.ZERO;
	}
	public ApplyRateData getApplyRateData() {
		return applyRateData;
	}

	public ApplyRateVersionData getApplyRateVersionData() {
		return applyRateVersionData;
	}

	public List<BillSegmentCalculationHeaderData> getBillSegmentCalculationHeaderData() {
		return null;
	}

	public List<BillSegmentItemData> getBillSegmentItemData() {
		return null;
	}

	public List<BillSegmentServiceQuantityData> getBillSegmentServiceQuantityData() {
		return null;
	}

	public List<CharacteristicData> getCharacteristicData() {
		return characteristicData;
	}

	public BigDecimal getValue() {
		return value;
	}

	public void setApplyRateData(ApplyRateData arg0) {
		applyRateData=arg0;
	}

	public void setApplyRateVersionData(ApplyRateVersionData applyrateVersion) {
		applyRateVersionData=applyrateVersion;
	}

	public void setBillSegmentCalculationHeaderData(
			List<BillSegmentCalculationHeaderData> arg0) {
	}

	public void setBillSegmentItemData(List<BillSegmentItemData> arg0) {
	}

	public void setBillSegmentServiceQuantityData(
			List<BillSegmentServiceQuantityData> arg0) {
	}

	public void setCharacteristicData(List<CharacteristicData> arg0) {
		characteristicData=arg0;
	}

	public void setCrossReferenceAmount(BigDecimal arg0) {
	}

	public void setCrossReferenceFound(Bool arg0) {
	}

	public void setCrossReferenceServiceQuantity(BigDecimal arg0) {
	}

	public void setRateComponent(RateComponent rc) {
	}

	/**
	 * invoke() method contains business logic that is executed when the
	 * algorithm is triggered.
	 */

	public void invoke() {
		logger.debug(" Process Execution Inside the Min Charge Algorithm");
		
		value = getTotBsCalcAmount();
		
	}

	/**
	 * getTotBsCalcAmount() method gives net minimum charge to be applied while creating a bill segment
	 * @return BigDecimal  totAmount
	 */
	private BigDecimal getTotBsCalcAmount() {
		Account_Id accountId = null;
		Bill_Id billId = null;
		BillCycleSchedule billCycleSchedule = null;
		//Start Change RIA

		Bill bill = new Bill_Id(applyRateData.getBillId()).getEntity();
		//End Change RIA

		Date processDate = null;
		if(notNull(bill)) {
			//Parent account from bill
			accountId = bill.getAccount().getId();
			billCycleSchedule =  bill.fetchBillCycleSchedule();
			processDate = getProcessDateTime().getDate();
			billId = bill.getId();
		}
		
		logger.debug("**** Process Execution Inside the Min Charge Algorithm :BillId******"+billId);
		PreparedStatement bsStatement = null; 
		BigDecimal minAmountOnParent = BigDecimal.ZERO;
		BigDecimal totAmount = BigDecimal.ZERO;
		String priceAsgnId = null;
		Date effDt = null;
		List<SQLResultRow> minChildResults = new ArrayList<SQLResultRow>();
		StringBuilder sb;		
		
		//Check whether minimum charge is assigned over parent merchant
		try {
			sb = new StringBuilder();
			sb.append("SELECT PC.VALUE_AMT, PER.PER_ID, PA.PRICE_ASGN_ID, PA.START_DT ");
		    sb.append("FROM CI_PRICECOMP PC, CI_PRICEASGN PA, CI_PARTY P,CI_ACCT_PER PER ");
		    sb.append("WHERE PC.PRICE_ASGN_ID=PA.PRICE_ASGN_ID ");
		    sb.append("AND PA.RS_CD='CHRGMPC' ");
		    sb.append("AND PA.PA_OWNER_TYPE_FLG='PRTY' ");
		    sb.append("AND PA.OWNER_ID=P.PARTY_UID ");
		    sb.append("AND P.PARTY_ID=PER.PER_ID ");
			sb.append("AND PER.ACCT_ID=:accountId ");
			sb.append("AND PA.START_DT<=:processDate AND (PA.END_DT IS NULL OR PA.END_DT>=:processDate) ");
			
			bsStatement = createPreparedStatement(sb.toString(),"");			
			bsStatement.bindId("accountId", accountId);
			bsStatement.bindDate("processDate", processDate);
			bsStatement.setAutoclose(false);
			SQLResultRow row = bsStatement.firstRow();
			if (notNull(row)) {
				minAmountOnParent = row.getBigDecimal("VALUE_AMT");	
				personId = row.getString("PER_ID");
				priceAsgnId = row.getString("PRICE_ASGN_ID");	
				effDt = row.getDate("START_DT");
			}
		} catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (bsStatement != null) {
				bsStatement.close();
				bsStatement = null;
			}
		}
		
		if (minAmountOnParent.compareTo(BigDecimal.ZERO) != 0){
												
			//Minimum charge is assigned over parent merchant
			return applyParentMinimumCharge(accountId, billCycleSchedule, billId,
					minAmountOnParent, personId);	
			
		}else{
			logger.debug("**** Process Execution Inside the Min Charge Algorithm : Outlet level pricing");

			//Check whether minimum charge is assigned over child merchants		
			try {
				bsStatement = null;
				sb = new StringBuilder();
				sb.append("SELECT PC.VALUE_AMT, SA2.SA_ID,AP.PER_ID,PA.PRICE_ASGN_ID,SA1.SA_ID BILL_SA ");
			    sb.append("FROM CI_PRICECOMP PC, CI_PRICEASGN PA, CI_PARTY P, CI_ACCT_PER AP, CI_SA SA1, CI_SA SA2, CI_SA_CHAR SC ");
			    sb.append("WHERE PC.PRICE_ASGN_ID=PA.PRICE_ASGN_ID ");
			    sb.append("AND PA.RS_CD='CHRGMPC' ");
			    sb.append("AND PA.PA_OWNER_TYPE_FLG='PRTY' ");
			    sb.append("AND PA.OWNER_ID=P.PARTY_UID ");
			    sb.append("AND P.PARTY_ID=AP.PER_ID ");
			    sb.append("AND AP.ACCT_ID=SA2.ACCT_ID ");
			    sb.append("AND SA2.SA_TYPE_CD='CHRG' ");
			    sb.append("AND SA2.SA_ID=TRIM(SC.CHAR_VAL_FK1) ");
			    sb.append("AND SC.CHAR_TYPE_CD='C1_SAFCD' ");
			    sb.append("AND SC.SA_ID=SA1.SA_ID ");
			    sb.append("AND SA1.SA_STATUS_FLG IN (:active, :pendStop) ");
				sb.append("AND SA2.SA_STATUS_FLG IN (:active, :pendStop) ");
			    sb.append("and SA1.SA_TYPE_CD=SA2.SA_TYPE_CD ");
			    sb.append("AND SA1.ACCT_ID=:accountId ");
			    sb.append("AND PA.START_DT<=:processDate AND (PA.END_DT IS NULL OR PA.END_DT>=:processDate) ");
			    
			    bsStatement = createPreparedStatement(sb.toString(),"");		
				bsStatement.bindId("accountId", accountId);
				bsStatement.bindDate("processDate", processDate);
				bsStatement.bindLookup("active",ServiceAgreementStatusLookup.constants.ACTIVE);
				bsStatement.bindLookup("pendStop",ServiceAgreementStatusLookup.constants.PENDING_STOP);
				bsStatement.setAutoclose(false);
				minChildResults = bsStatement.list();
				if(notNull(minChildResults) && !minChildResults.isEmpty()) {
					logger.debug("**** Process Execution Inside the Min Charge Algorithm : Outlet level pricing found");
					//Minimum charge is assigned over child merchants
					return applyChildMinimumCharge(accountId, billCycleSchedule, billId,
							minChildResults, priceAsgnId, effDt);
					
				}
				else {
					return applyParentMinimumChargeForChildBill(accountId, processDate);
				}
			} catch (Exception e) {
				logger.error("error",e);
			} finally {
				if (bsStatement != null) {
					bsStatement.close();
					bsStatement = null;
				}
			}
		}
		
		//Minimum charge is neither assigned over parent nor over child merchant
		
		totAmount = BigDecimal.ZERO;
		return totAmount;
		
	}
		

	
	private BigDecimal applyParentMinimumChargeForChildBill(Account_Id accountId, Date processDate) {
		PreparedStatement pStmt = null;
		BigDecimal valAmtOnParent = BigDecimal.ZERO;	
		String priceAsgnId = null;	
		Date effDt = null;
		
		try {
			StringBuilder queryStr = new StringBuilder();
			queryStr.append(" SELECT PC.VALUE_AMT, PC.PRICE_ASGN_ID, PA.START_DT, PP.PER_ID1 ")
				    .append(" FROM CI_PRICECOMP PC, CI_PRICEASGN PA, CI_PARTY P, ")
				    .append(" CI_ACCT_PER PER, CI_PER_PER PP WHERE          ")
				    .append(" PC.PRICE_ASGN_ID=PA.PRICE_ASGN_ID AND PA.RS_CD=:rsCd ")
				    .append(" AND PA.PA_OWNER_TYPE_FLG=:prty                  ")
				    .append(" AND PA.OWNER_ID=P.PARTY_UID                     ")
				    .append(" AND P.PARTY_ID=PP.PER_ID1                       ")
				    .append(" AND PER.PER_ID=PP.PER_ID2                       ")
				    .append(" AND PER.ACCT_ID=:accountId                      ")
				    .append(" AND PA.START_DT<=:processDate AND               ")
				    .append(" (PA.END_DT IS NULL OR PA.END_DT>=:processDate)  ");

			pStmt = createPreparedStatement(queryStr.toString(),"applyParentMinimumChargeForChildBill");
			pStmt.bindString("rsCd", "CHRGMPC", "RS_CD");
			pStmt.bindString("prty", invoiceDataInterfaceLookUp.getOwnerTypeFlag(), "PA_OWNER_TYPE_FLG");
			pStmt.bindId("accountId", accountId);
			pStmt.bindDate("processDate", processDate);
			
			pStmt.setAutoclose(false);
			SQLResultRow row = pStmt.firstRow();
			if (notNull(row)) {
				valAmtOnParent = row.getBigDecimal("VALUE_AMT");	
				priceAsgnId = row.getString("PRICE_ASGN_ID");	
				effDt = row.getDate("START_DT");
				personId = row.getString("PER_ID1");
			
			}
		} catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (pStmt != null) {
				pStmt.close();
				pStmt = null;
			}
		}
		
		return valAmtOnParent;
	}
		
	/**
	 * Get total amount to which the parent-level minimum charge will be applied to
	 * 
	 * @param accountId
	 * @param billCycleSchedule
	 * @param billId
	 * @param minAmountOnParent
	 * @param personId
	 * @return totAmount
	 */
	private BigDecimal applyParentMinimumCharge(Account_Id accountId, BillCycleSchedule billCycleSchedule, 
			Bill_Id billId,	BigDecimal minAmountOnParent, String personId) {
		
		BillPeriod_Id billPeriodId;
		StringBuilder sb;
		PreparedStatement bsStatement = null;
		BigDecimal totAmount = BigDecimal.ZERO;
		String minchrgCharValue = "";
		Date startDate = null;
		Date endDate = null;
		BigInteger currencyExponent = billId.getEntity().getAccount().getCurrency().getDecimalPositions();
		
		//Check for characteristic whether assigned or not
		minchrgCharValue = getMinimumChargePeriod(personId);
		
		
		//check whether minimum charge has duration
		if (notBlank(minchrgCharValue)){
			//Minimum charge has duration
			billPeriodId = new BillPeriod_Id(minchrgCharValue.trim());
			if(isNull(billPeriodId.getEntity())) {
				addError(CustomMessageRepository.billCycleError("Invalid Bill Period code assigned over merchant for minimum charge"));
			}
			
			//Get duration start and end dates
			SQLResultRow datesRow = getDurationDates(billCycleSchedule, billPeriodId);
			if (notNull(datesRow)) {
				startDate = datesRow.getDate("START_DT");
				endDate = datesRow.getDate("END_DT");
			} 

			//Check whether minimum charge end date for the bill period has passed
			if (notNull(startDate) && notNull(endDate)) {	
				try {
					sb = new StringBuilder();
					sb.append("SELECT SUM(ROUND(C.ADHOC_CHAR_VAL,:currencyExponent)) AS AMT");
					sb.append(" FROM CI_BILL_CHAR C, CI_BILL F");
					sb.append(" WHERE C.BILL_ID=F.BILL_ID");
					sb.append(" AND C.BILL_ID=:billId");
					sb.append(" AND C.CHAR_TYPE_CD='RUN_TOT'");
					sb.append(" AND F.WIN_START_DT BETWEEN :startDate AND :endDate");
					sb.append(" AND EXISTS (SELECT 1 FROM CI_PRICEITEM_REL E");
					sb.append(" WHERE E.PRICEITEM_REL_TYPE_FLG=:flgValue");
					sb.append(" AND E.PRICEITEM_CHLD_CD=RPAD(C.SRCH_CHAR_VAL,30))");
					bsStatement = createPreparedStatement(sb.toString(),"");
					bsStatement.bindString("flgValue", getProductFlagValue().trim(), "PRICEITEM_REL_TYPE_FLG");
					bsStatement.bindId("billId", billId);
					//bsStatement.bindId("accountId", accountId);
					bsStatement.bindDate("startDate",startDate);
					bsStatement.bindBigInteger("currencyExponent", currencyExponent);
					bsStatement.bindDate("endDate", endDate);
					bsStatement.setAutoclose(false);
					if (bsStatement.firstRow() != null && bsStatement.firstRow().getBigDecimal("AMT")!= null) {	
						totAmount = bsStatement.firstRow().getBigDecimal("AMT");
						if(totAmount.compareTo(minAmountOnParent) == 1){
							totAmount = minAmountOnParent;
						}
						else if(totAmount.compareTo(BigDecimal.ZERO) == -1){
							totAmount = BigDecimal.ZERO;
						}
					}
				} catch (Exception e) {
					logger.error("error",e);
				} finally {
					if (bsStatement != null) {
						bsStatement.close();
						bsStatement = null;
					}
				}
			}
			else{
				//If end date has not reached then pass total assigned minimum charge
				totAmount = minAmountOnParent;
			}
			
		}
		else{
			//Minimum charge without duration
			//char doesn't exist-Apply minimum charge normally
			try {
				sb = new StringBuilder();
				sb.append("SELECT SUM(ROUND(C.ADHOC_CHAR_VAL,:currencyExponent)) AS AMT");
				sb.append(" FROM CI_BILL_CHAR C");
				sb.append(" WHERE C.BILL_ID=:billId");
				sb.append(" AND C.CHAR_TYPE_CD='RUN_TOT'");
				sb.append(" AND EXISTS (SELECT 1 FROM CI_PRICEITEM_REL E");
				sb.append(" WHERE E.PRICEITEM_REL_TYPE_FLG=:flgValue");
				sb.append(" AND E.PRICEITEM_CHLD_CD=RPAD(C.SRCH_CHAR_VAL,30))");
				bsStatement = createPreparedStatement(sb.toString(),"");
				bsStatement.bindString("flgValue", getProductFlagValue().trim(), "PRICEITEM_REL_TYPE_FLG");
				bsStatement.bindId("billId", billId);
				bsStatement.bindBigInteger("currencyExponent", currencyExponent);
				bsStatement.setAutoclose(false);
				if (bsStatement.firstRow() != null && bsStatement.firstRow().getBigDecimal("AMT") != null) {
					totAmount =bsStatement.firstRow().getBigDecimal("AMT");
					if(totAmount.compareTo(minAmountOnParent) == 1){
						totAmount = minAmountOnParent;
					}
					else if(totAmount.compareTo(BigDecimal.ZERO) == -1){
						totAmount = BigDecimal.ZERO;
					}
				}
			} catch (Exception e) {
				logger.error("error",e);
			} finally {
				if (bsStatement != null) {
					bsStatement.close();
					bsStatement = null;
				}
			}
			
		}
		
		
		if(isNull(totAmount)){
			totAmount=BigDecimal.ZERO;
		}
		
		return totAmount;
	}
	
	private SQLResultRow getDurationDates(BillCycleSchedule billCycleSchedule, BillPeriod_Id billPeriodId) {
		StringBuilder sb;
		PreparedStatement ps = null;
		SQLResultRow datesRow = null;		
		
		try {
			sb = new StringBuilder();
			sb.append("select BILL_DT AS START_DT, CUTOFF_DT AS END_DT");
			sb.append(" from CM_BILL_PER_SCH where BILL_PERIOD_CD=:minchrgCharValue");
			sb.append(" and CUTOFF_DT between :winStartDt and :winEndDt");
			ps = createPreparedStatement(sb.toString(), "");
			ps.bindId("minchrgCharValue", billPeriodId);
			ps.bindDate("winStartDt", billCycleSchedule.getId().getWindowStartDate());
			ps.bindDate("winEndDt", billCycleSchedule.getWindowEndDate());
			ps.setAutoclose(false);
			datesRow = ps.firstRow();
		}catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}
		
		return datesRow;
	}
	
	private String getMinimumChargePeriod(String personId) {
		StringBuilder sb1;
		PreparedStatement ps = null;
		String minchrgCharValue = "";
		try {
			sb1 = new StringBuilder();
			sb1.append("SELECT ADHOC_CHAR_VAL ");
		    sb1.append("FROM CI_PER_CHAR WHERE PER_ID=:personId ");
		    sb1.append("AND CHAR_TYPE_CD='MINCHGNM' ");
		    sb1.append("AND EFFDT<=:processDate ");
		     
			ps = createPreparedStatement(sb1.toString(), "");
			ps.bindString("personId", personId, "PER_ID");
			ps.bindDate("processDate", getProcessDateTime().getDate());
			ps.setAutoclose(false);
			SQLResultRow row = ps.firstRow();
			if (notNull(row)) {
				minchrgCharValue = row.getString("ADHOC_CHAR_VAL");
			} 
		}catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
			}
		}
		return minchrgCharValue;
	}
	
	/**
	 * Get total unmet charge amount after applying minimum charge on child-level
	 * 
	 * @param accountId
	 * @param billCycleSchedule
	 * @param billId
	 * @param minChildResults
	 * @param priceAsgnId
	 * @param effDt
	 * @return
	 */
	private BigDecimal applyChildMinimumCharge(Account_Id accountId, BillCycleSchedule billCycleSchedule,
			Bill_Id billId, List<SQLResultRow> minChildResults, String priceAsgnId, Date effDt) {
		
		BigDecimal childMinAmt;
		BillPeriod_Id billPeriodId;
		ServiceAgreement_Id saId;
		ServiceAgreement_Id billSaId;
		String personId;
		String childPriceAssign;
		BigDecimal minChargeOnChild = BigDecimal.ZERO; 
		BigDecimal totAmount = BigDecimal.ZERO;
		String minchrgCharValue = ""; 
		Date startDate = null; 
		Date endDate = null;
		BigDecimal totalChildMinAmt=BigDecimal.ZERO;

		
		//Retrieve child contract IDs
		for (SQLResultRow resultSet : minChildResults) {
			saId = (ServiceAgreement_Id) resultSet.getId("SA_ID", ServiceAgreement.class);
			billSaId = (ServiceAgreement_Id) resultSet.getId("BILL_SA", ServiceAgreement.class);

			childMinAmt = resultSet.getBigDecimal("VALUE_AMT");
			
			totalChildMinAmt = totalChildMinAmt.add(childMinAmt);			
								
			personId = resultSet.getString("PER_ID");
			childPriceAssign =  resultSet.getString("PRICE_ASGN_ID");
			logger.debug("**** Process Execution Inside the Min Charge Algorithm : Outlet level pricing PriceAssign Id****"+childPriceAssign);
			//minimum charge assigned over child
			if(notNull(childMinAmt) && !childMinAmt.equals(BigDecimal.ZERO)) {
				
				//check for characteristic whether assigned or not
				minchrgCharValue = getMinimumChargePeriod(personId);

				//check whether minimum charge has duration
				if (CommonUtils.CheckNull(minchrgCharValue).equals("")) {
					//apply minimum charge for hierarchy without duration
					minChargeOnChild = aggMinChargeWithoutDuration(saId, billId, childMinAmt);
				}
				else {
					//Minimum charge has duration
					billPeriodId = new BillPeriod_Id(minchrgCharValue.trim());
					if(isNull(billPeriodId.getEntity())) {
						addError(CustomMessageRepository.billCycleError("Invalid Bill Period code assigned over merchant for minimum charge"));
					}
					
					//Get duration start and end dates
					SQLResultRow datesRow = getDurationDates(billCycleSchedule, billPeriodId);
					if (notNull(datesRow)) {
						startDate = datesRow.getDate("START_DT");
						endDate = datesRow.getDate("END_DT");
					} 

					//Apply minimum charge using dates only when end date has passed otherwise pass total minimum charge assigned over merchant
					if (notNull(startDate) && notNull(endDate)) {	
						minChargeOnChild = aggMinChargeWithDuration(saId, accountId, startDate, endDate, childMinAmt, billId);
					}
					else{
						minChargeOnChild=BigDecimal.ZERO;
					}
					
				}
				
				Bill bill=billId.getEntity();
				PriceAsgn priceAsgn=new PriceAsgn_Id(childPriceAssign).getEntity();
				Currency_Id currency=billSaId.getEntity().getCurrencyId();
				
				
				//createBillSegmnet on outlet level-
				
				BillSegment_Id bsegId=createBsegEntries(billCycleSchedule,billId,billSaId);	
				logger.info("Bill Segment Id Generated****with Entity***"+bsegId);				

				
				//BsegCalc
				minChargeOnChild=minChargeOnChild.setScale(currency.getEntity().getDecimalPositions().intValue(), BigDecimal.ROUND_HALF_UP);
				
				BillSegmentCalculationHeader_DTO bsegHeadDTO=(BillSegmentCalculationHeader_DTO) createDTO(BillSegmentCalculationHeader.class);
				bsegHeadDTO.setBillableChargeId(new BillableCharge_Id(" "));
				bsegHeadDTO.setCalculatedAmount(minChargeOnChild);
				bsegHeadDTO.setCurrencyId(currency);
				bsegHeadDTO.setDescriptionOnBill("Charging: Minimum Period Charge");
				bsegHeadDTO.setEndDate(billCycleSchedule.getWindowEndDate());
				bsegHeadDTO.setStartDate(billCycleSchedule.getId().getWindowStartDate());
				bsegHeadDTO.setRateVersionId(new RateVersion_Id(applyRateData.getRateSchedule(),new Date(2001, 01, 01)));
				BillSegmentCalculationHeader_Id bsegHeaderId=new BillSegmentCalculationHeader_Id(bsegId, BigInteger.ONE);
				bsegHeadDTO.setId(bsegHeaderId);				
				BillSegmentCalculationHeader bsegHeader=bsegHeadDTO.newEntity();
				
			
				BillCalculationLine_DTO billCalcLine=(BillCalculationLine_DTO)createDTO(BillCalculationLine.class);
				billCalcLine.setId(new BillCalculationLine_Id(bsegHeaderId, new BigInteger("3")));
				billCalcLine.setBaseAmount(BigDecimal.ZERO);
				billCalcLine.setBillableServiceQuantity(BigDecimal.ZERO);
				billCalcLine.setCalculatedAmount(minChargeOnChild);
				billCalcLine.setCurrencyId(currency);
				billCalcLine.setDescriptionOnBill("Minimum period charge - Unmet min charge");
				billCalcLine.setDistributionCodeId(new GeneralLedgerDistributionCode_Id("BASE_CHG"));
				billCalcLine.setExchRate(BigDecimal.ZERO);
				billCalcLine.setExemptAmount(BigDecimal.ZERO);
				billCalcLine.setMeasuresPeakQuantity(Bool.FALSE);
				billCalcLine.setPricamt(BigDecimal.ZERO);
				billCalcLine.setValueAmt(minChargeOnChild);
				billCalcLine.setTotaggsq(BigDecimal.ZERO);
				billCalcLine.setRcSequence(new BigInteger("30"));
				billCalcLine.setPricccycdId(new Currency_Id(new PriceAsgn_Id(childPriceAssign).getEntity().getPriceCurrencyCode()));
				billCalcLine.setShouldPrint(Bool.TRUE);
				billCalcLine.setShouldAppearInSummary(Bool.FALSE);
				BillCalculationLine billingCalcLine=billCalcLine.newEntity();
				
				
				//Bseg Calc Line Char DTO
				BillCalculationLineCharacteristic_DTO lineCharDTO=(BillCalculationLineCharacteristic_DTO) createDTO(BillCalculationLineCharacteristic.class);
				lineCharDTO.setCharacteristicValue("TOTAL_BB");
				
				BillCalculationLineCharacteristic_Id lineChar=new BillCalculationLineCharacteristic_Id
						(new BillCalculationLine_Id(bsegHeaderId, new BigInteger("3")), new CharacteristicType_Id("BCL-TYPE"));
				lineCharDTO.setId(lineChar);
				
				BillCalculationLineCharacteristic bsegCalcLineChar=lineCharDTO.newEntity();
				
				//BsegExt - through INSERT
				creatBsegExtEntries(bsegId,applyRateData,childPriceAssign);
				//BsegSq
				
				createBsegSqEntries(bsegId);
				
				//FT Generation-
				Money amount = new Money(minChargeOnChild, currency);
			    FinancialTransaction_DTO ftDTO = (FinancialTransaction_DTO)createDTO(FinancialTransaction.class);
			    ftDTO.setAccountingDate(getProcessDateTime().getDate());
			    
			    ftDTO.setCreationDateTime(getProcessDateTime());
			    ftDTO.setCurrencyId(currency);
			    ftDTO.setCurrentAmount(amount);
			    ftDTO.setDivisionId(billSaId.getEntity().getAccount().getDivisionId());
			    ftDTO.setFinancialTransactionType(FinancialTransactionTypeLookup.constants.BILL_SEGMENT);
			    ftDTO.setGlDistributionStatus(GlDistributionStatusLookup.constants.GENERATED);
			    ftDTO.setGlDivisionId(billSaId.getEntity().getServiceAgreementType().getGlDivision().getId());
			    ftDTO.setIsNewCharge(Bool.TRUE);
			    ftDTO.setShouldShowOnBill(Bool.TRUE);
			    ftDTO.setParentId(billId.getTrimmedValue());
			    ftDTO.setPayoffAmount(amount);
			    ftDTO.setServiceAgreementId(billSaId);
			    ftDTO.setSiblingId(bsegId.getTrimmedValue());
			    ftDTO.setBillId(billId);
			    ftDTO.setIsFrozen(Bool.TRUE);
			    ftDTO.setFreezeDateTime(getSystemDateTime());
			    ftDTO.setFrozenByUserId(new User_Id("SYSUSER"));
			    ftDTO.setFxlgCalcStatus("I");
			    ftDTO.setPresnBillId(billId.getTrimmedValue());
			    ftDTO.setArrearsDate(getProcessDateTime().getDate());
			    ftDTO.setScheduledDistributionDate(getProcessDateTime().getDate());
			    
			    FinancialTransaction ft = ftDTO.newEntity();
				logger.info("FinancialTransaction****"+ft.getId());				
	
				//FT GL entries-through INSERT
			    BigInteger sequence = BigInteger.ONE;
			   
			    createFtGl(ft, ft.getCurrentAmount().negate(), null, new GeneralLedgerDistributionCode_Id("BASE_CHG"), Bool.FALSE, sequence);

			    sequence = sequence.add(BigInteger.ONE);
			    createFtGl(ft, ft.getCurrentAmount(), null, billSaId.getEntity().getServiceAgreementType().getDistributionCode().getId(), Bool.TRUE, sequence);
	   			
				minChargeOnChild=BigDecimal.ZERO;				
			}
			
			//Reset values
			startDate = null;
			endDate = null;
			billPeriodId = null;
			minchrgCharValue = "";

		}
		

		if(notNull(totAmount)){
			totAmount=totAmount.negate();
		}
		//final amount
		if(isNull(totAmount)){
			totAmount=BigDecimal.ZERO;
		}
		return minChargeOnChild;
	}
	
	private void createBsegSqEntries(BillSegment_Id bsegId) {
		
		StringBuilder sqlString = new StringBuilder();
	    sqlString.append("INSERT INTO CI_BSEG_SQ ");
	    sqlString.append("(BSEG_ID,UOM_CD,TOU_CD,SQI_CD,INIT_SQ,BILL_SQ,VERSION) ");
	    sqlString.append("VALUES ");
	    sqlString.append("(:bsegId,:uomCd,' ',' ',:initSq,:billSq,'1') ");
	    PreparedStatement ps = createPreparedStatement(sqlString.toString(), getClass().getSimpleName());
	    ps.bindId("bsegId", bsegId);
	    ps.bindId("uomCd", new PriceItem_Id(applyRateData.getPriceItemCd()).getEntity().getUnitOfMeasureId());
	    ps.bindBigInteger("initSq", BigInteger.ZERO);
	    ps.bindBigInteger("billSq", BigInteger.ZERO);
	    ps.setAutoclose(false);
	    
	    ps.executeUpdate();
	    
	    if(ps != null){
	    	ps.close();
	    	ps=null;
	    }
		
	}
	
	
	private BillSegment_Id createBsegEntries(
			BillCycleSchedule billCycleSchedule, Bill_Id billId,
			ServiceAgreement_Id billSaId) {
		
		StringBuilder sqlString = new StringBuilder();
		BillSegment_Id bsegId=null;
		String bill=billId.getTrimmedValue();
		SecureRandom random = new SecureRandom();
		int num = random.nextInt(900000000) + 100000000;
		logger.debug("****************BILL ID***************************************"+billId);
		String bseg=bill.substring(0, 3).concat(String.valueOf(num));
		
		//Added logic to check for Existence for New FT Id creation
					
		while (checkbsegExists(bseg))
		{
			random = new SecureRandom();
			num = random.nextInt(900000000) + 100000000;
			bseg=bill.substring(0, 3).concat(String.valueOf(num));
		}
		
		bsegId=new BillSegment_Id(bseg);
		
	    sqlString.append("INSERT INTO CI_BSEG ");
	    sqlString.append("VALUES ");
	    sqlString.append("(:bsegId,:billCycCd,:winStartDt,' ',' ',:saId,:billId,:startDt,:endDt,:estSw,:closeSw,:sqOvrSw,:itemSw,' ', ");
	    sqlString.append(":bsegFlg,:creDt,:statDt,' ','1',' ',' ',' ',:ilmDt,:ilmSw) ");

	    PreparedStatement ps = createPreparedStatement(sqlString.toString(), getClass().getSimpleName());
	    ps.bindId("bsegId", bsegId);
	    ps.bindEntity("billCycCd", billCycleSchedule.fetchIdBillCycle());
	    ps.bindDate("winStartDt",billCycleSchedule.getId().getWindowStartDate());
	    ps.bindId("saId",billSaId);
	    ps.bindId("billId", billId);
	    ps.bindDate("startDt", billCycleSchedule.getId().getWindowStartDate());
	    ps.bindDate("endDt", billCycleSchedule.getWindowEndDate());
	    ps.bindBoolean("estSw", Bool.FALSE);
	    ps.bindBoolean("closeSw", Bool.FALSE);
	    ps.bindBoolean("sqOvrSw", Bool.FALSE);
	    ps.bindBoolean("itemSw", Bool.FALSE);
	    ps.bindLookup("bsegFlg", BillSegmentStatusLookup.constants.FROZEN);
	    ps.bindDateTime("creDt", getSystemDateTime());
	    ps.bindDateTime("statDt", getSystemDateTime());
	    ps.bindDate("ilmDt", getSystemDateTime().getDate());
	    ps.bindBoolean("ilmSw", Bool.FALSE);

	    ps.setAutoclose(false);
	    
	    ps.executeUpdate();
	    
	    if(ps != null){
	    	ps.close();
	    	ps=null;
	    }
	    
	    createBsegKeyEntries(bsegId);
	    
	    return bsegId;
	}
	
	
	private void createBsegKeyEntries(BillSegment_Id bsegId)  {
		
		PreparedStatement ps=null;
		StringBuilder sb=new StringBuilder();	
		
		try{
			sb.append("INSERT INTO CI_BSEG_K VALUES(:bsegId,:envId)");
			ps=createPreparedStatement(sb.toString(),"");
			ps.bindId("bsegId", bsegId);
			ps.bindBigInteger("envId",envId);
			ps.executeUpdate();
		}
		catch(Exception e){
			logger.error("Error in Inserting in CI_BSEG_K  :"+e);			
		}
		finally{
			if(ps!=null){
				ps.close();
				ps=null;
			}
		}
		
	}
	
	
	
	private boolean checkbsegExists(String bseg) {
		
		StringBuilder sb=new StringBuilder();
		sb.append("SELECT COUNT(*) COUNT_BSEG FROM CI_BSEG WHERE BSEG_ID=:bsegId");
		PreparedStatement ps=null;	
		BigInteger count=null;
		ps=createPreparedStatement(sb.toString(),"");
		ps.bindString("bsegId",bseg,"BSEG_ID");
		ps.setAutoclose(false);
		SQLResultRow rs=ps.firstRow();
		count=rs.getInteger("COUNT_BSEG");
		
		if (notNull(ps)) {
			ps.close();
			ps = null;
		}
		
		logger.debug(" checkBsegExists() method :: END");
		
		if(count.compareTo(BigInteger.ZERO) == 0){
			return false;
		}
		
		return true;

	}
	
	
	private void creatBsegExtEntries(BillSegment_Id bsegId,
			ApplyRateData applyRateData, String priceAsgnId) {
	    StringBuilder sqlString = new StringBuilder();
	    sqlString.append("INSERT INTO CI_BSEG_EXT ");
	    sqlString.append("(BSEG_ID,CONSTRUCT_ID,PRIORITY_NUM,TEMPLATE_PURPOSE_FLG,VERSION_NUM,USAGE_ACCT_ID, ");
	    sqlString.append("PRICEITEM_CD,PRICE_ASGN_ID,PRICEITEM_PARM_GRP_ID,VERSION,BSEG_TYPE_FLG) ");
	    sqlString.append("VALUES ");
	    sqlString.append("(:bsegId,' ',0,' ',0,' ',:priceItemCd,:priceAsgnId,'0',1,:post) ");
	    PreparedStatement ps = createPreparedStatement(sqlString.toString(), getClass().getSimpleName());
	    ps.bindId("bsegId", bsegId);
	    ps.bindId("priceItemCd", new PriceItem_Id(applyRateData.getPriceItemCd()));
	    ps.bindId("priceAsgnId",new PriceAsgn_Id(priceAsgnId));
	    ps.bindString("post", "POST", "BSEG_TYPE_FLG");
	    ps.setAutoclose(false);
	    
	    ps.executeUpdate();
	    
	    if(ps != null){
	    	ps.close();
	    	ps=null;
	    }
	    
	    
	
	}
	private void createFtGl(FinancialTransaction ft, Money amount, CharacteristicValue_Id charValId, GeneralLedgerDistributionCode_Id glDistCodeId, Bool isTotalAmount, BigInteger sequence)
	  {
	    StringBuilder sqlString = new StringBuilder();
	    sqlString.append("INSERT INTO CI_FT_GL ");
	    sqlString.append("(FT_ID, GL_SEQ_NBR, DST_ID, CHAR_TYPE_CD, AMOUNT, CHAR_VAL, TOT_AMT_SW, VERSION, STATISTIC_AMOUNT, GL_ACCT, VALIDATE_SW, GLA_VAL_DT) ");
	    
	    sqlString.append("VALUES ");
	    if ((notNull(charValId)) && (notNull(charValId.getEntity()))) {
	      sqlString.append("(:ftId,:glSeqNbr,:dstId,:charType,:amount,:charVal,:totalAmtSw,'1','0',' ','N',null)");
	    } else {
	      sqlString.append("(:ftId,:glSeqNbr,:dstId,' ',:amount,' ',:totalAmtSw,'1','0',' ','N',null)");
	    }
	    PreparedStatement ps = createPreparedStatement(sqlString.toString(), getClass().getSimpleName());
	    ps.bindId("ftId", ft.getId());
	    ps.bindBigInteger("glSeqNbr", sequence);
	    ps.bindId("dstId", glDistCodeId);
	    if ((notNull(charValId)) && (notNull(charValId.getEntity())))
	    {
	      ps.bindEntity("charType", charValId.getCharacteristicType());
	      ps.bindStringProperty("charVal", CharacteristicValue.properties.characteristicValue, charValId.getCharacteristicValue());
	    }
	    ps.bindMoney("amount", amount);
	    ps.bindBoolean("totalAmtSw", isTotalAmount);
	    ps.setAutoclose(false);
	    
	    ps.executeUpdate();
	    
	    if(ps != null){
	    	ps.close();
	    	ps=null;
	    }
	    
	    
	  }
	
	
	private BigDecimal aggMinChargeWithDuration(ServiceAgreement_Id saId, Account_Id accountId, Date startDate, Date endDate, BigDecimal childMinAmt, Bill_Id billId) {

		PreparedStatement bsStatement = null;
		BigDecimal minChargeToApply=BigDecimal.ZERO;
		BigDecimal childConsumption=BigDecimal.ZERO;
		BigInteger currencyExponent = billId.getEntity().getAccount().getCurrency().getDecimalPositions();

		//consumption by child
		try {	
			StringBuilder sb = new StringBuilder();
			sb.append("select SUM(C.CALC_AMT) as AMT");
			sb.append(" from CI_BSEG B, CI_BSEG_CALC C, CI_BILL_CHG D, CI_BILL F");
			sb.append(" where B.BILL_ID=F.BILL_ID");
			sb.append(" and F.ACCT_ID=:accountId");
			sb.append(" AND F.WIN_START_DT BETWEEN :startDate AND :endDate");
			sb.append(" and B.BSEG_ID=C.BSEG_ID");
			sb.append(" and C.BILLABLE_CHG_ID=D.BILLABLE_CHG_ID");
			sb.append(" and D.SA_ID=:saId");
			sb.append(" AND EXISTS (SELECT 1");
			sb.append(" FROM CI_PRICEITEM_REL E WHERE E.PRICEITEM_REL_TYPE_FLG=:flgValue AND E.PRICEITEM_CHLD_CD=D.PRICEITEM_CD)");
			bsStatement = createPreparedStatement(sb.toString(),"");
			bsStatement.bindString("flgValue", getProductFlagValue().trim(), "PRICEITEM_REL_TYPE_FLG");
			bsStatement.bindId("accountId", accountId);
			bsStatement.bindId("saId",saId);
			bsStatement.bindDate("startDate", startDate);
			bsStatement.bindDate("endDate", endDate);
			bsStatement.setAutoclose(false);
			
			if (notNull(bsStatement.firstRow())) {
				childConsumption = bsStatement.firstRow().getBigDecimal("AMT");
			}
		} catch (Exception e) {
			logger.error("error ",e);
		} finally {
			if (bsStatement != null) {
				bsStatement.close();
				bsStatement = null;
			}
		}	
		
		if(isNull(childConsumption)){
			childConsumption=BigDecimal.ZERO; 
		}
		if (childMinAmt.compareTo(childConsumption) > 0){
			if(childConsumption.compareTo(BigDecimal.ZERO) >= 0) {
				minChargeToApply = childMinAmt.subtract(childConsumption);
			}
			else{
				minChargeToApply=childMinAmt;
			}
		}else{
			minChargeToApply=BigDecimal.ZERO; 
		}

		return minChargeToApply;
	}
	
	private BigDecimal aggMinChargeWithoutDuration(ServiceAgreement_Id saId, Bill_Id billId, BigDecimal childMinAmt) {

		PreparedStatement bsStatement = null;
		BigDecimal minChargeToApply=BigDecimal.ZERO;
		BigDecimal childConsumption=BigDecimal.ZERO;
		//consumption by child
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("select SUM(C.CALC_AMT) as AMT");
			sb.append(" from CI_BSEG B, CI_BSEG_CALC C, CI_BILL_CHG D");
			sb.append(" where B.BILL_ID=:billId");
			sb.append(" and B.BSEG_ID=C.BSEG_ID");
			sb.append(" and C.BILLABLE_CHG_ID=D.BILLABLE_CHG_ID");
			sb.append(" and D.SA_ID=:saId");
			sb.append(" AND EXISTS (SELECT 1");
			sb.append(" FROM CI_PRICEITEM_REL E WHERE E.PRICEITEM_REL_TYPE_FLG=:flgValue AND E.PRICEITEM_CHLD_CD=D.PRICEITEM_CD)");
			bsStatement = createPreparedStatement(sb.toString(),"");
			bsStatement.bindString("flgValue", getProductFlagValue().trim(), "PRICEITEM_REL_TYPE_FLG");			
			bsStatement.bindId("billId", billId);
			bsStatement.bindId("saId", saId);
			bsStatement.setAutoclose(false);
			if (notNull(bsStatement.firstRow())) {
				childConsumption = bsStatement.firstRow().getBigDecimal("AMT");
			}
		} catch (Exception e) {
			logger.error("error",e);
		} finally {
			if (bsStatement != null) {
				bsStatement.close();
				bsStatement = null;
			}
		}		

		if(isNull(childConsumption)){
			childConsumption=BigDecimal.ZERO;
		}
		if (childMinAmt.compareTo(childConsumption) > 0){
			if(childConsumption.compareTo(BigDecimal.ZERO) >= 0) {
				minChargeToApply = childMinAmt.subtract(childConsumption);
			}
			else{
				minChargeToApply=childMinAmt;
			}
		}else{
			minChargeToApply=BigDecimal.ZERO;
		}

		return minChargeToApply;
	}

	public BigDecimal getAggSqQuantity() {
		return null;
	}
	public String getPriceCompId() {
		return null;
	}
	public void setAggSqQuantity(BigDecimal arg0) {
	}
	public void setPriceCompId(String arg0) {
	}
}