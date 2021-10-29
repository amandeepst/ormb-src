/*******************************************************************************
* FileName                   : TaxAmountCalculation_Impl
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 1.8
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Mar 24, 2015         Sunaina       Implemented all requirements for CD1.
0.2      NA             Oct 14, 2015         Sunaina       Update for End date and tax authority.
0.3      NA             May 17, 2015         Sunaina       Update for loggers.
0.4      NA             Dec 12, 2016         Preeti        Fix for charging bill issue-adhoc charges.
0.5      NA             Jan 24, 2017         Ankur		   Changed for performance improvement as per oracle reviews.
0.6      NA             Feb 15, 2017         Preeti		   PAM-11012 Bill date issue fix.
0.7      NA             Feb 17, 2017         Preeti		   Tax rounding issue fix.
0.8      NA             Apr 18, 2017         Preeti		   PAM-12364 Precision scenario added.
0.9      NA             Apr 19, 2017         Vienna Rom	   Performance and sonar issues.
1.0      NA             Jun 09, 2017         Vienna Rom	   Removed logger.debug and used StringBuilder.
1.1      NA             Aug 25, 2017         Preeti   	   Redesigned code to apply VAT as single line on Bill. 
1.2      NA             Jan 10, 2018         Ankur   	   PAM-16885 Duplicate tax characteristics issue fix
1.3      NA             Jan 25, 2018         Ankur   	   PAM-16712 Fix
1.4      NA             May 28, 2018         RIA           NAP-27796 Fix    
1.5      NA             May 27, 2018         Ankur   	   NAP-27640 Fix to distinguish between charge type & person level OOS TAX
1.6		 NA				Aug 21, 2018		 RIA		   NAP-31325
1.7	     NA				OCt 16, 2018		 Amandeep	   NAP-33633
1.8	     NA				Oct 31, 2018		 RIA	       NAP-34838
1.9		 NAP-35669		Nov 02, 2018		 Prerna		   NAP-35669 Performance changes
 *******************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicValue;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.ccb.api.lookup.BsegTypeFlgLookup;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.billCycle.BillCycle;
import com.splwg.ccb.domain.admin.billFactor.BillFactor;
import com.splwg.ccb.domain.admin.billFactor.BillFactor_Id;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode_Id;
import com.splwg.ccb.domain.admin.serviceAgreementType.SaTypePreBillCompletionAlgorithmSpot;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristic_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLineCharacteristics;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine_DTO;
import com.splwg.ccb.domain.billing.billSegment.BillCalculationLine_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader;
import com.splwg.ccb.domain.billing.billSegment.BillSegmentCalculationHeader_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.person.Person_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Sunaina
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (entityName = characteristicType, name = TaxRateCharacteristictypeCode, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = TaxRegimeCharacteristictypeCode, required = true, type = entity)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = BillCalculationLineTypeCharacteristictypeCode, required = true, type = entity)
 *            , @AlgorithmSoftParameter (name = TaxDistributionCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = taxAgencyCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (entityName = characteristicType, name = TaxScopeCharacteristictypeCode, required = true, type = entity)
 *            , @AlgorithmSoftParameter (name = outOfScope, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = reverseCharge, required = true, type = string)})
 */
public class TaxAmountCalculation_Impl extends TaxAmountCalculation_Gen implements
SaTypePreBillCompletionAlgorithmSpot {

	// **************************************************Initialization of global variables*******************************//

	private static final Logger logger = LoggerFactory.getLogger(TaxAmountCalculation_Impl.class);

	private static final String TX_S_IND = "TX_S_IND";
	private static final String CAN_GST = "CAN-GST";
	private static final String IND_IST = "IND-IST";
	private static final String OUT_OF_SCOPE_O = "OUT OF SCOPE-O";
	private static final String INTRA_EU = "INTRA-EU";
	private static final String HKG = "HKG";
	private static final String USA = "USA";
	private static final String CAN = "CAN";
	private static final String IND = "IND";
	private static final String CM_CTRY1 = "CM_CTRY1";
	private static final String CM_CTRY2 = "CM_CTRY2";
	private static final String CM_CTRY3 = "CM_CTRY3";
	private static final String CM_CTRY4 = "CM_CTRY4";
	private static final String TILDE ="~";
	private static final String NON_EC ="NON-EC";
	private static final String YES ="Y";
	private static final String OUT_OF_SCOPE ="OUT-OF-SCOPE";
	private static final String FIELD_AMT="AMT";
	private static final String FIELD_CHAR_VAL="CHAR_VAL";
	private static final String FIELD_CHAR_TYPE_CD="CHAR_TYPE_CD";
	private static final String FIELD_COUNTRY="COUNTRY";
	private static final String FIELD_GEOGRAPHIC_CODE="GEOGRAPHIC_CODE";
	private static final String FIELD_TAX_AUTHORITY="TAX_AUTHORITY";
	private static final String FIELD_STATE_BASED_TAX_SW="STATE_BASED_TAX_SW";
	private static final String FIELD_CIS_DIVISION="CIS_DIVISION";
	private static final String FIELD_CHAR_VAL_FK1="CHAR_VAL_FK1";
	private static final String FIELD_FT_ID="FT_ID";
	private ArrayList<List<String>> taxRateList = new ArrayList<List<String>>();

	private Bill bill;
	private BigDecimal taxRate = BigDecimal.ZERO;
	private Boolean isReverseChargeTaxApplied = Boolean.FALSE;
	private BillSegment_Id taxBillSegmentId;
	private static ConcurrentHashMap<String, String> supplierLocationMap=new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String, String> personGeoCodeMap=new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String,String> countryDataMap=new ConcurrentHashMap<String, String>();
	private static ConcurrentHashMap<String,BigDecimal> bfValueMap=new ConcurrentHashMap<String, BigDecimal>();

	public Bool getShouldSkipRemainingAlgorithms() {
		return null;
	}

	public void setBill(Bill arg0) {
		bill = arg0;
	}

	public void setBillCycle(BillCycle arg0) {
	}

	public void setEndDate(Date arg0) {
	}

	public void setServiceAgreementType(ServiceAgreementType arg0) {

	}

	public void setStartDate(Date arg0) {

	}

	/**
	 * Main processing.
	 */
	public void invoke() {

		// ******************************************Declaration of variables outside "for" loop**********************************//

		logger.debug("Inside TaxAmountCalculation_Impl invoke()");
		
		Date windowEndDate;
		BigDecimal taxAmount = null;
		BigDecimal totalTaxAmount = null;
		int count =0;
		String currency ="";
		BigDecimal totBsCalcamt = null;
		
		Boolean chargeExist=Boolean.FALSE;
		Boolean outOfScopeTaxFlg = Boolean.FALSE;
		Boolean zeroRatedFlg = Boolean.FALSE;
		Boolean zeroTariffFlg = Boolean.FALSE;

		Person_Id personId = retrievePersonId(bill.getId());
		Person per = personId.getEntity();

		// *********************************************Retrieve Tax Bill Segment Id*********************************************//
		retrieveTaxBillSegmentId(bill.getId());

		// *********************************************delete tax bill segment if no charges on bill*********************************************//
		chargeExist=chargesExist(bill.getId());
		
		if (!chargeExist){
			
			deleteTaxBillSegment(taxBillSegmentId);
			
		}else{		
		
		// *********************************************update amount to 0 for tax bill segment as default is set to 1*********************************************//
		updateFtforBS(taxBillSegmentId,BigDecimal.ZERO,Boolean.TRUE);

		// **************************Determine person division******************//
		String personDivision = per.getDivision();		

		String supplierLocation1 = "";
		String supplierLocation2 = "";
		String supplierLocation3 = "";
		String supplierLocation4 = "";
		List<String> supplierLocationList = new ArrayList<String>();	
		
		populateMaps();
		// ***********************************************Determine Supplier Location***********************************************//
		// Extract the Country characteristic on the division and read supplier's location. As there can be maximum 4 characteristics so, we are checking for all of them if they exist.
		if (notNull(personDivision)) {			
						
			supplierLocation1 = supplierLocationMap.get(personDivision.concat(TILDE).concat(CM_CTRY1));
			if(notBlank(supplierLocation1))
				supplierLocationList.add(supplierLocation1);

			supplierLocation2 = supplierLocationMap.get(personDivision.concat(TILDE).concat(CM_CTRY2));
			if(notBlank(supplierLocation2))
				supplierLocationList.add(supplierLocation2);

			supplierLocation3 = supplierLocationMap.get(personDivision.concat(TILDE).concat(CM_CTRY3));
			if(notBlank(supplierLocation3))
				supplierLocationList.add(supplierLocation3);

			supplierLocation4 = supplierLocationMap.get(personDivision.concat(TILDE).concat(CM_CTRY4));
			if(notBlank(supplierLocation4))
				supplierLocationList.add(supplierLocation4);
		}			

		// ****************************Retrieve person's country to determine merchant's tax regime************************		
		String personCountry = per.getCountry();
		String personState = per.getState();			

		// **************************Merchant's Tax Regime Retrieval***********************************//
		String geographicCodeForPerson = personGeoCodeMap.get(personCountry);

		//********************************If country is India or Canada- enable multiple tax rate flag********//
		Boolean multipleTaxRateFlag = Boolean.FALSE;
		if (IND.equals(personCountry) || CAN.equals(personCountry)) {
			multipleTaxRateFlag = Boolean.TRUE;
		}

		String[] countryDataArr = null;
		String supplierTaxReg = "";
		String geographicCodeForSupplier = "";
		String stateBasedTaxSw = "";
		Boolean stateBasedTaxFlag = Boolean.FALSE;

		// **************************Check if one of the supplier locations is USA or HKG**********************//
		boolean hasUsaOrHkgSupplierLocation = false;
		if (supplierLocationList.contains(USA)) {
			hasUsaOrHkgSupplierLocation = true;
		}

		//Performance Change
		//If Person country belong to list of supplier locations then use person country to determine country details. Otherwise use any of the supplier location.
		String key=null;
		if (supplierLocationList.contains(personCountry)) {
			key=personCountry;
		}
		else{
			// Here we are passing supplier location 1 as even if multiple supplier location exist in the system, they will all belong to same tax regime
			key=supplierLocation1;
		}
		String countryDataStr=countryDataMap.get(key);
		if(notBlank(countryDataStr))
			countryDataArr=countryDataStr.split(TILDE);
		if (notNull(countryDataArr) && countryDataArr.length==3) {
			supplierTaxReg = countryDataArr[0];//HMRC-VAT
			stateBasedTaxSw = countryDataArr[1];
			geographicCodeForSupplier = countryDataArr[2];//INTRA-EU,NON-EC
		}		

		//Canada has state based tax
		if (YES.equalsIgnoreCase(stateBasedTaxSw)) {
			// State based Tax is to be applied
			stateBasedTaxFlag = Boolean.TRUE;
		}

		BillFactor bf=null;
		ArrayList<BigDecimal> taxRatesList = null;
		ArrayList<String> taxCharValList = null;
		ArrayList<BigDecimal> bsegAmountList;
		ArrayList<String> bsegTaxCharValList;
		ArrayList<List<String>> taxRateAndCharValList = null;
		String taxStatusCharVal ="";
		ArrayList<String> taxDataList = null;
		Boolean isTaxExist=  Boolean.FALSE;
		CharacteristicType_Id pTaxStatCharType=null;
		BigDecimal bsegAmountListTotal = BigDecimal.ZERO;
		BigInteger currencyExponent = bill.getAccount().getCurrency().getDecimalPositions();
		

		// ***********************variables moved out of "for" loop****************************//
		windowEndDate = getProcessDateTime().getDate();
		//Process further If at least one of the supplier location available
		if (notBlank(supplierLocation1) || notBlank(supplierLocation2) || notBlank(supplierLocation3) || notBlank(supplierLocation4)) {
			//process only if tax regime exists
			if (notBlank(supplierTaxReg) || hasUsaOrHkgSupplierLocation) {

				if (hasUsaOrHkgSupplierLocation) {
					geographicCodeForSupplier = NON_EC;
				}

				if (notBlank(geographicCodeForSupplier)) {
					if (!hasUsaOrHkgSupplierLocation) {
						// Get Bill factor value
						bf = new BillFactor_Id(supplierTaxReg).getEntity();
						pTaxStatCharType = bf.getCharacteristicType().getId();
					}

					//Retrieve group of tax char values and corresponding amount
					bsegAmountList = new ArrayList<BigDecimal>();
					bsegTaxCharValList = new ArrayList<String>();
					
					for (SQLResultRow resultSet1 : getTaxInfo(pTaxStatCharType, currencyExponent)) {
							bsegTaxCharValList.add(resultSet1.getString(FIELD_CHAR_VAL));
							bsegAmountList.add(resultSet1.getBigDecimal(FIELD_AMT));
							bsegAmountListTotal = bsegAmountListTotal.add(resultSet1.getBigDecimal(FIELD_AMT));
						}
					
					if (!bsegTaxCharValList.isEmpty() && !bsegAmountList.isEmpty()) {						
						// Creating bill calculation line for each set of tax char value and bill segment amount on given bill
						for (int i = 0; i < bsegTaxCharValList.size(); i++) {

							if (notNull(bsegAmountList.get(i)) && notNull(bsegTaxCharValList.get(i).trim())) {
								
								taxRatesList = new ArrayList<BigDecimal>();
								taxCharValList = new ArrayList<String>();
								taxRateAndCharValList = new ArrayList<List<String>>();
								taxDataList = new ArrayList<String>();
								taxAmount = BigDecimal.ZERO;
								totalTaxAmount = BigDecimal.ZERO;
								totBsCalcamt = BigDecimal.ZERO;
								//********************************Decide tax char value to be applied*********************************************//

								// Check if Person's Country and Supplier's Country are same
								if (supplierLocationList.contains(personCountry) && !(USA.equals(personCountry) || HKG.equals(personCountry))) {
									taxStatusCharVal = bsegTaxCharValList.get(i).trim();
									bsegAmountListTotal = bsegAmountList.get(i);
								}
								// Check if both Person and Supplier are in European Union
								else if (INTRA_EU.equalsIgnoreCase(geographicCodeForPerson)	&& INTRA_EU.equalsIgnoreCase(geographicCodeForSupplier)) {
									// INTRA EU will be a part of Geographic code
										isReverseChargeTaxApplied = Boolean.TRUE;
										// Tax Agency Code is retrieved so, Supplier's Zero rated Tax is applied										
										
										taxStatusCharVal = OUT_OF_SCOPE_O;
										outOfScopeTaxFlg = Boolean.TRUE;
										
										if(i==1)
										break;
								}
								// Check if both Person is not in European Union and Supplier is in European Union, "OUT OF SCOPE" tax applied. This scenario will also set "Out of Scope" Tax for USA and HongKong
								else if (!(INTRA_EU.equalsIgnoreCase(geographicCodeForPerson)) || !(INTRA_EU.equalsIgnoreCase(geographicCodeForSupplier))) {
									// Out of Scope Tax is to be applied
									taxStatusCharVal = OUT_OF_SCOPE_O;
									supplierTaxReg=OUT_OF_SCOPE;
									outOfScopeTaxFlg=Boolean.TRUE;
									if(i==1)
									break;
								}

								//********************************Decide tax char value to be applied*********************************************//

								if (notNull(bf) || hasUsaOrHkgSupplierLocation) {
									if (notBlank(taxStatusCharVal)) {

										// ***********************Tax Status has been retrieved, Checking for tax rate now************************//
										taxRateAndCharValList = getTaxRate(bf,taxStatusCharVal, windowEndDate,personState);
										for (int j = 0; j < taxRateAndCharValList.size(); j++) {

											taxRatesList.add(new BigDecimal(taxRateAndCharValList.get(j).get(0)));
											taxCharValList.add(taxRateAndCharValList.get(j).get(1));
										}

										if (!taxRatesList.isEmpty()	&& !taxCharValList.isEmpty()) {
											if (stateBasedTaxFlag || multipleTaxRateFlag) {
												// *******************Creating bill calculation lines for multiple tax lines****************************//
												for (int j = 0; j < taxRatesList.size(); j++) {

													if (notNull(taxRatesList.get(i)) && notNull(taxCharValList.get(j).trim())) {

														taxDataList = createBillCalcLine(taxBillSegmentId,taxCharValList.get(j),taxRatesList.get(j),taxRatesList.size(),pTaxStatCharType,
																currency,taxAmount,supplierTaxReg,totBsCalcamt,totalTaxAmount,count,multipleTaxRateFlag,taxRatesList.get(0),bsegAmountListTotal,
																outOfScopeTaxFlg,zeroRatedFlg,zeroTariffFlg);
														count++;
														isTaxExist = new Boolean(taxDataList.get(0));
														totBsCalcamt = new BigDecimal(taxDataList.get(1));
														totalTaxAmount = new BigDecimal(taxDataList.get(2));
														currency = taxDataList.get(3);
													}
												}

											} 
											// *******************Creating bill calculation lines for single tax line****************************//
											else {
												if (notNull(taxRatesList.get(0)) && notNull(taxCharValList.get(0).trim())) {

													taxDataList = createBillCalcLine(taxBillSegmentId,taxCharValList.get(0),taxRatesList.get(0),taxRatesList.size(),pTaxStatCharType,
															currency,taxAmount,supplierTaxReg,totBsCalcamt,totalTaxAmount,count,multipleTaxRateFlag,BigDecimal.ZERO,bsegAmountListTotal,
															outOfScopeTaxFlg,zeroRatedFlg,zeroTariffFlg);
													count++;
													isTaxExist = new Boolean(taxDataList.get(0));
													totBsCalcamt = new BigDecimal(taxDataList.get(1));
													totalTaxAmount = new BigDecimal(taxDataList.get(2));
													currency = taxDataList.get(3);
												}
											}
											//***********************update FTs If tax applied successfully
											Boolean exceptionUpdatedToBsStat = new Boolean(taxDataList.get(4));
											if (!exceptionUpdatedToBsStat && !isTaxExist) {
												//update ci_ft for tax bill segment
												updateFtforBS(taxBillSegmentId,totalTaxAmount,Boolean.FALSE);
											}

										} else {
											updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.BF_STAT_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.BF_STAT_MISSING),""));
										}
									} else {
										updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.PROD_TAX_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.PROD_TAX_MISSING),""));
									}
								} else {
									updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.BF_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.BF_MISSING),""));
								}				

								taxDataList.clear();
							}
						}//new for loop
					}//if list not empty				

				} else {
					updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.SUPPLIER_GEOGRAPHIC_CODE_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.SUPPLIER_GEOGRAPHIC_CODE_MISSING),""));
				}
			} else {
				updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.SUPPLIER_TAXREG_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.SUPPLIER_TAXREG_MISSING),""));
			}
		} else {
			updateBsStat(taxBillSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.SUPPLIER_LOCATION_MISSING),"10",getErrorDescription(String.valueOf(CustomMessages.SUPPLIER_LOCATION_MISSING),""));
		}
		}//else charges exist
	}	

	private void populateMaps() {
		if(countryDataMap.isEmpty()){
			populateCountryDataMap();
		}
		
		if(personGeoCodeMap.isEmpty()){
			populateGeoCodeMap();
		}
		
		if(supplierLocationMap.isEmpty()){
			populateSupplierLocationMap();
		}
		
		if(bfValueMap.isEmpty()){
			populateBillFactorVal();
		}		
	}

	private Person_Id retrievePersonId(Bill_Id billId) {
		Person_Id perId = null;
		PreparedStatement preparedStatement = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT ACCTPER.PER_ID ");
			sb.append(" FROM CI_ACCT_PER ACCTPER, CI_BILL BILL ");
			sb.append(" WHERE ACCTPER.ACCT_ID = BILL.ACCT_ID ");
			sb.append(" AND BILL.BILL_ID = :billId ");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.bindId("billId", billId);
			preparedStatement.setAutoclose(false);
			SQLResultRow row = preparedStatement.firstRow();
			if (notNull(row)) {
				perId = (Person_Id) row.getId("PER_ID", Person.class);
			}
		} catch (Exception e) {
			logger.error("Exception in retrievePersonId()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		return perId;
	}

	private void retrieveTaxBillSegmentId(Bill_Id billId) {
		PreparedStatement preparedStatement = null;		
		try {
			preparedStatement = createPreparedStatement("SELECT A.BSEG_ID AS BSEG_ID " +
					"FROM CI_BSEG A, CI_BSEG_CALC B WHERE A.BILL_ID=:billId " +
					"AND A.BSEG_ID=B.BSEG_ID and b.rs_cd=:tax","");
			preparedStatement.bindId("billId", billId);
			preparedStatement.bindString("tax", "TAX","RS_CD");
			preparedStatement.setAutoclose(false);
			SQLResultRow row = preparedStatement.firstRow();
			if (notNull(row)) {
				taxBillSegmentId = (BillSegment_Id) row.getId("BSEG_ID",BillSegment.class);
			}
		} catch (Exception e) {
			logger.error("Exception in retrieveTaxBillSegmentId()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}
	
	private void populateSupplierLocationMap() {
		PreparedStatement preparedStatement = null;
		StringBuilder sb = new StringBuilder();	
		try {
			sb.append(" SELECT CIS_DIVISION,CHAR_TYPE_CD,CHAR_VAL_FK1 FROM CI_CIS_DIV_CHAR WHERE ");
			sb.append(" CHAR_TYPE_CD IN (:cmCtry1,:cmCtry2,:cmCtry3,:cmCtry4) AND EFFDT <= :sysDate ORDER BY EFFDT");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.bindDate("sysDate",getSystemDateTime().getDate());
			preparedStatement.bindString("cmCtry1",CM_CTRY1,FIELD_CHAR_TYPE_CD);
			preparedStatement.bindString("cmCtry2",CM_CTRY2,FIELD_CHAR_TYPE_CD);
			preparedStatement.bindString("cmCtry3",CM_CTRY3,FIELD_CHAR_TYPE_CD);
			preparedStatement.bindString("cmCtry4",CM_CTRY4,FIELD_CHAR_TYPE_CD);
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			if(notNull(resultList) && resultList.size()>0) {
				for(SQLResultRow result:resultList) {
					String division = CommonUtils.CheckNull(result.getString(FIELD_CIS_DIVISION)).trim();
					String charTypeCd = CommonUtils.CheckNull(result.getString(FIELD_CHAR_TYPE_CD)).trim();
					String key=division.concat(TILDE).concat(charTypeCd);
					String supplierLocation = CommonUtils.CheckNull(result.getString(FIELD_CHAR_VAL_FK1)).trim();
					supplierLocationMap.put(key, supplierLocation);
				}
			}
		} catch (Exception e) {
			logger.error("Exception in populateSupplierLocationMap()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	private List<SQLResultRow> getTaxInfo(CharacteristicType_Id pTaxStatCharType, BigInteger currencyExponent) {
		PreparedStatement preparedStatement1 = null;		
		try {
			preparedStatement1 = createPreparedStatement(" SELECT T1.CHAR_VAL AS CHAR_VAL, SUM(T1.ADHOC_CHAR_VAL) AS AMT  " +
					" FROM ( SELECT A.CHAR_VAL, ROUND(TO_NUMBER(C.ADHOC_CHAR_VAL), :currencyExponent) ADHOC_CHAR_VAL	" +
					" FROM CI_PRICEITEM_CHAR A, CI_BILL_CHAR C   " +
					" WHERE A.CHAR_TYPE_CD=:pTaxStatCharType AND A.PRICEITEM_CD=RPAD(C.SRCH_CHAR_VAL,30)  " +
					" AND C.BILL_ID=:billId AND C.CHAR_TYPE_CD='RUN_TOT'  " +
					" UNION ALL " +
					" SELECT A.CHAR_VAL,C.calc_amt " +
					" FROM CI_PRICEITEM_CHAR A, CI_BSEG_SQ B,CI_BSEG_CALC C,CI_BSEG D, CI_PRICEITEM E " +
					" WHERE A.CHAR_TYPE_CD = :pTaxStatCharType AND A.PRICEITEM_CD= E.PRICEITEM_CD " +
					" AND E.UOM_CD=B.UOM_CD AND B.UOM_CD<> ' ' AND B.BSEG_ID=C.BSEG_ID AND C.CALC_AMT <> 0 AND " +
					" C.EFFDT IS NOT NULL " +
					" AND C.BSEG_ID=D.BSEG_ID AND D.BILL_ID=:billId AND D.BSEG_STAT_FLG IN (:freezable, :frozen)) T1  " +
					" GROUP BY T1.CHAR_VAL","");
			preparedStatement1.bindId("billId", bill.getId());
			preparedStatement1.bindId("pTaxStatCharType", pTaxStatCharType);
			preparedStatement1.bindBigInteger("currencyExponent", currencyExponent);
			preparedStatement1.bindLookup("freezable", BillSegmentStatusLookup.constants.FREEZABLE);
			preparedStatement1.bindLookup("frozen", BillSegmentStatusLookup.constants.FROZEN);
			
			preparedStatement1.setAutoclose(false);
			return preparedStatement1.list();
		}
		finally {
			if(preparedStatement1 != null) {
				preparedStatement1.close();
				preparedStatement1 =null;
			}
		}
	}
		
	private void populateCountryDataMap() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT COUNTRY,TAX_AUTHORITY, STATE_BASED_TAX_SW, GEOGRAPHIC_CODE FROM CM_COUNTRY", "");
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			if(notNull(resultList) && resultList.size()>0) {
				for(SQLResultRow result:resultList) {
					String cntry = CommonUtils.CheckNull(result.getString(FIELD_COUNTRY)).trim();
					String taxAuth = CommonUtils.CheckNull(result.getString(FIELD_TAX_AUTHORITY)).trim();
					String StateTaxSw = CommonUtils.CheckNull(result.getString(FIELD_STATE_BASED_TAX_SW)).trim();
					String geoCode = CommonUtils.CheckNull(result.getString(FIELD_GEOGRAPHIC_CODE)).trim();
					String mapVal = taxAuth.concat(TILDE).concat(StateTaxSw).concat(TILDE).concat(geoCode);
					countryDataMap.put(cntry, mapVal);
				}
			}
		} catch (Exception e) {
			logger.error("Exception in populateCountryData()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}
	
	private void populateGeoCodeMap() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT COUNTRY,GEOGRAPHIC_CODE FROM CM_COUNTRY_PER ", "");
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			if(notNull(resultList) && resultList.size()>0) {
				for(SQLResultRow result:resultList) {
					String cntry = CommonUtils.CheckNull(result.getString(FIELD_COUNTRY)).trim();
					String geographicCode = CommonUtils.CheckNull(result.getString(FIELD_GEOGRAPHIC_CODE)).trim();
					personGeoCodeMap.put(cntry,geographicCode);
				}
			}
		} catch (Exception e) {
			logger.error("Exception in populateGeoCodeMap()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	private Boolean chargesExist(Bill_Id billId) {
		PreparedStatement preparedStatement = null;
        PreparedStatement checkForZeroCalcAmt = null;		
		Boolean chargesExist= Boolean.FALSE;		
		try {
			// RIA: NAP-31325: Added ci_bseg_ext join for Bill segment type = RGLR check
			preparedStatement = createPreparedStatement("select a.bseg_id from ci_bseg a, ci_bseg_ext b " +
					"where a.bill_id =:billId and a.bseg_id = b.bseg_id and b.bseg_type_flg = :rglr and rownum < 2 ","");
			preparedStatement.bindId("billId", billId);
			preparedStatement.bindLookup("rglr",BsegTypeFlgLookup.constants.REGULAR);
			preparedStatement.setAutoclose(false);
			SQLResultRow row = preparedStatement.firstRow();
			if (notNull(row)) {
				chargesExist = Boolean.TRUE;
			}
			else{
				StringBuilder sb = new StringBuilder();
				sb.append(" select a.bseg_id from ci_bseg a, ci_bseg_ext b, ci_bseg_calc c");
				sb.append(" where a.bill_id = :billId and a.bseg_id = b.bseg_id and b.bseg_id = c.bseg_id and b.bseg_type_flg = :post ");
				sb.append(" and b.priceitem_cd = :minChrg and c.calc_amt <> 0 ");
				checkForZeroCalcAmt = createPreparedStatement(sb.toString(), "");
				checkForZeroCalcAmt.bindId("billId", billId);
				checkForZeroCalcAmt.bindLookup("post",BsegTypeFlgLookup.constants.POST_PROCESSING);
				checkForZeroCalcAmt.bindString("minChrg","MINCHRGP","PRICEITEM_CD");
				checkForZeroCalcAmt.setAutoclose(false);
				SQLResultRow zeroCalcRow = checkForZeroCalcAmt.firstRow();
				if (notNull(zeroCalcRow)) {
					chargesExist = Boolean.TRUE;
				}
			}
		} catch (Exception e) {
			logger.error("Exception in chargesExist()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
			if (checkForZeroCalcAmt != null){
				checkForZeroCalcAmt.close();
			}
		}
		return chargesExist;
	}
	
	private void deleteTaxBillSegment(BillSegment_Id billSegmentId) {
		PreparedStatement preparedStatement = null;
		
		String ftId="";
		try {
			StringBuilder sb = new StringBuilder();
			sb.append(" select FT_ID from ci_ft ft ");
			sb.append(" where sibling_id=:billSegmentId");
			sb.append(" and ft_type_flg = :ftTypeFlg");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.bindLookup("ftTypeFlg", FinancialTransactionTypeLookup.constants.BILL_SEGMENT);
			preparedStatement.setAutoclose(false);
			SQLResultRow row = preparedStatement.firstRow();
			if (notNull(row)) {
				ftId = row.getString(FIELD_FT_ID);
			}
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_ft_gl WHERE FT_ID=:ft","");
			preparedStatement.bindString("ft", ftId, FIELD_FT_ID);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_ft_k WHERE FT_ID=:ft","");
			preparedStatement.bindString("ft", ftId, FIELD_FT_ID);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_ft WHERE FT_ID=:ft","");
			preparedStatement.bindString("ft", ftId, FIELD_FT_ID);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG_CALC_LN WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG_CALC WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG_EXT WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG_SQ WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG_K WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
		
		try {
			preparedStatement = createPreparedStatement("delete from CI_BSEG WHERE BSEG_ID=:billSegmentId","");
			preparedStatement.bindId("billSegmentId", billSegmentId);
			preparedStatement.setAutoclose(false);
			preparedStatement.executeUpdate();
		} catch (Exception e) {
			logger.error("Exception in deleteTaxBillSegment()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}
	
	private void updateFtforBS(BillSegment_Id billSegmentId, BigDecimal taxAmt, Boolean updateToZero) {
		PreparedStatement ftQuery = null;
		PreparedStatement ftUpdate = null;
		PreparedStatement ftGLUpdate = null;
		PreparedStatement ftGLTaxUpdate = null;

		BigDecimal roundedTaxAmt = taxAmt.setScale(2, BigDecimal.ROUND_HALF_UP);

		if (updateToZero){
			roundedTaxAmt=BigDecimal.ONE.negate();

			PreparedStatement updateBsCalcLn = null;
			try {
				updateBsCalcLn = createPreparedStatement("update ci_bseg_calc_ln set calc_amt=calc_amt+:calamt where bseg_id=:bSegId and seqno=1","");
				//RIA: Added for NAP-27796
				updateBsCalcLn.setAutoclose(false);
				updateBsCalcLn.bindBigDecimal("calamt", roundedTaxAmt);
				updateBsCalcLn.bindId("bSegId", billSegmentId);
				updateBsCalcLn.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in updateftForBs()", e);
			} finally {
				if (updateBsCalcLn != null) {
					updateBsCalcLn.close();
				}
			}
		}

		if (BigDecimal.ZERO.compareTo(roundedTaxAmt) != 0) {
			PreparedStatement updateBsCalc = null;
			try {
				updateBsCalc = createPreparedStatement("update ci_bseg_calc set calc_amt=calc_amt+:calamt where bseg_id=:bSegId","");
				//RIA: Added for NAP-27796
				updateBsCalc.setAutoclose(false);
				updateBsCalc.bindBigDecimal("calamt", roundedTaxAmt);
				updateBsCalc.bindId("bSegId", billSegmentId);
				updateBsCalc.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in updateftForBs()", e);
			} finally {
				if (updateBsCalc != null) {
					updateBsCalc.close();
				}
			}


			ftQuery = createPreparedStatement("select FT_ID from ci_ft ft where sibling_id=:bSegId", "");
			ftQuery.setAutoclose(false);
			ftQuery.bindId("bSegId", billSegmentId);

			FinancialTransaction_Id ftId;
			for (SQLResultRow resultSet : ftQuery.list()) {
				ftId = (FinancialTransaction_Id) resultSet.getId(FIELD_FT_ID,FinancialTransaction.class);

				try {
					ftUpdate = createPreparedStatement("UPDATE CI_ft SET CUR_AMT=CUR_AMT+:amt, TOT_AMT=TOT_AMT+:amt WHERE FT_ID=:ft","");
					//RIA: Added for NAP-27796
					ftUpdate.setAutoclose(false);
					ftUpdate.bindId("ft", ftId);
					ftUpdate.bindBigDecimal("amt", roundedTaxAmt);
					ftUpdate.executeUpdate();
				} catch (Exception e) {
					logger.error("Exception in updateftForBs()", e);
				} finally {
					if (ftUpdate != null ) {
						ftUpdate.close();
					}
				}

				try {
					ftGLUpdate = createPreparedStatement("UPDATE CI_FT_GL SET AMOUNT=AMOUNT+:amt WHERE FT_ID=:ft and TOT_AMT_SW='Y'","");
					//RIA: Added for NAP-27796
					ftGLUpdate.setAutoclose(false);
					ftGLUpdate.bindId("ft", ftId);
					ftGLUpdate.bindBigDecimal("amt", roundedTaxAmt);
					ftGLUpdate.executeUpdate();
				} catch (Exception e) {
					logger.error("Exception in updateftForBs()", e);
				} finally {
					if (ftGLUpdate != null) {
						ftGLUpdate.close();
					}
				}

				try {
					ftGLTaxUpdate = createPreparedStatement("UPDATE CI_FT_GL SET AMOUNT = AMOUNT+:amt WHERE FT_ID = :ftId AND DST_ID = :dstId","");
					//RIA: Added for NAP-27796
					ftGLTaxUpdate.setAutoclose(false);
					ftGLTaxUpdate.bindId("ftId", ftId);
					ftGLTaxUpdate.bindBigDecimal("amt",roundedTaxAmt.negate());
					ftGLTaxUpdate.bindString("dstId",getTaxDistributionCode().trim(), "DST_ID");
					ftGLTaxUpdate.execute();
				} catch (Exception e) {
					logger.error("Exception in updateftForBs()", e);
				} finally {
					if (ftGLTaxUpdate != null) {
						ftGLTaxUpdate.close();
					}
				}

			}//for
			//RIA: Added for NAP-27796
			if(ftQuery != null){
				ftQuery.close();
			}			
		}//if not 0
	}

	private void createBillCalcLineChar(BillCalculationLineCharacteristics billCalcLineChars,CharacteristicType_Id charType,String value) {
		try {
			BillCalculationLineCharacteristic_DTO calcLineCharDto = billCalcLineChars.newChildDTO();
			if (charType.getEntity().getCharacteristicType().isPredefinedValue())
				calcLineCharDto.setCharacteristicValue(value);
			if (charType.getEntity().getCharacteristicType().isAdhocValue())
				calcLineCharDto.setAdhocCharacteristicValue(value);
			billCalcLineChars.add(calcLineCharDto, charType);
		} catch (Exception e) {
			logger.error("Exception in createBillCalcLineChar()", e);
		}
	}

	private void outOfScope(String taxStatusCharVal) {
		String key=taxStatusCharVal.trim();
		taxRate=bfValueMap.get(key);
		if(notNull(taxRate)){
			taxRate=taxRate.setScale(3,BigDecimal.ROUND_HALF_UP);
			ArrayList<String> taxCharValueList = new ArrayList<String>();
			taxCharValueList.add(0, taxRate.toString());
			taxCharValueList.add(1, taxStatusCharVal);
			taxRateList.add(taxCharValueList);
	}
	}
	
	private void indIst(BillFactor bf, Date winEndDt) {
		PreparedStatement preparedStatement = null;
		ArrayList<String> taxCharValueList;
		String charValue;
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT CHAR_VAL, VAL FROM ");
		sb.append(" CI_BF_VAL WHERE BF_CD = :bf ");
		sb.append(" AND CHAR_VAL LIKE :ist AND CHAR_VAL NOT LIKE :zeroRated ");
		sb.append(" AND EFFDT <= :efftDate ORDER BY EFFDT DESC, VAL DESC");
		preparedStatement = createPreparedStatement(sb.toString(), "");
		preparedStatement.bindEntity("bf", bf);
		preparedStatement.bindStringProperty("ist",CharacteristicValue.properties.characteristicValue,"IST%");
		preparedStatement.bindStringProperty("zeroRated",CharacteristicValue.properties.characteristicValue,"%ZERO%");
		preparedStatement.bindDate("efftDate", winEndDt);
		preparedStatement.setAutoclose(false);
		List<SQLResultRow> resultList = preparedStatement.list();
		if(notNull(resultList) && resultList.size()>0) {
			for(SQLResultRow result:resultList) {

			charValue = result.getString("CHAR_VAL");
			taxRate = result.getBigDecimal("VAL").setScale(3,BigDecimal.ROUND_HALF_UP);
			taxCharValueList = new ArrayList<String>();
			taxCharValueList.add(0, taxRate.toString());
			taxCharValueList.add(1, charValue);
			taxRateList.add(taxCharValueList);
			}
		}
	}
	
	private ArrayList<List<String>> getTaxRate(BillFactor bf,String taxStatusCharVal, Date winEndDt, String personState) {
		ArrayList<String> taxCharValueList;
		StringBuilder sb;

		String canCharVal = "";
		String charValue;
		// Get Effective Tax Rate
		taxRateList.clear();
		if (OUT_OF_SCOPE_O.equals(taxStatusCharVal.trim())) {
			outOfScope(taxStatusCharVal);

		} else if (IND_IST.equals(bf.getId().getTrimmedValue())) {
			indIst(bf, winEndDt);
		
		} else if (CAN_GST.equals(bf.getId().getTrimmedValue())) {
			if (taxStatusCharVal.contains("STD")) {
				canCharVal = personState.concat("%STD%").trim();
			} else if (taxStatusCharVal.contains("ZERO")) {
				canCharVal = personState.concat("%ZERO%").trim();
			}
			PreparedStatement preparedStatement = null;
			sb = new StringBuilder();
			sb.append(" SELECT CHAR_VAL, VAL FROM ");
			sb.append(" CI_BF_VAL WHERE BF_CD = :bf ");
			sb.append(" AND CHAR_VAL LIKE :charVal ");
			sb.append(" AND EFFDT <= :efftDate ORDER BY EFFDT DESC");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.bindEntity("bf", bf);
			preparedStatement.bindStringProperty("charVal",CharacteristicValue.properties.characteristicValue,canCharVal);
			preparedStatement.bindDate("efftDate", winEndDt);
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			if(notNull(resultList) && resultList.size()>0) {
				for(SQLResultRow result:resultList) {

				charValue = result.getString("CHAR_VAL");
				taxRate = result.getBigDecimal("VAL").setScale(3,BigDecimal.ROUND_HALF_UP);
				taxCharValueList = new ArrayList<String>();
				taxCharValueList.add(0, taxRate.toString());
				taxCharValueList.add(1, charValue);
				taxRateList.add(taxCharValueList);
				}
			}
		} else {
			String key=bf.getId().getIdValue().trim().concat(TILDE).concat(taxStatusCharVal.trim());
			taxRate=bfValueMap.get(key);
			if(notNull(taxRate)){
				taxRate=taxRate.setScale(3,BigDecimal.ROUND_HALF_UP);
				taxCharValueList = new ArrayList<String>();
				taxCharValueList.add(0, taxRate.toString());
				taxCharValueList.add(1, taxStatusCharVal);
				taxRateList.add(taxCharValueList);
			}
		}
		return taxRateList;
	}
	
	
	
	private void populateBillFactorVal() {
		PreparedStatement preparedStatement = null;
		String key =null;
		StringBuilder sb=new StringBuilder();
		try {
			sb.append(" SELECT BF_CD,         ");
			sb.append("   CHAR_VAL,           ");
			sb.append("   VAL                 ");
			sb.append(" FROM CI_BF_VAL        ");
			sb.append(" WHERE EFFDT <=SYSDATE ");
			sb.append(" ORDER BY EFFDT        ");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.setAutoclose(false);
			List<SQLResultRow> resultList = preparedStatement.list();
			if(notNull(resultList) && resultList.size()>0) {
				for(SQLResultRow result:resultList) {
					String bfCd = CommonUtils.CheckNull(result.getString("BF_CD")).trim();
					String charVal = CommonUtils.CheckNull(result.getString(FIELD_CHAR_VAL)).trim();
					if(!(bfCd.equals(CAN_GST) || bfCd.equals(IND_IST))){
						if(charVal.equals(OUT_OF_SCOPE_O)){
							key = charVal;
						}
						else{
							key = bfCd.concat(TILDE).concat(charVal);
						}
						BigDecimal bfVal=result.getBigDecimal("VAL");
						bfValueMap.put(key,bfVal);
					}
				}
			}
		} catch (Exception e) {
			logger.error("Exception in populateGeoCodeMap()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	private ArrayList<String> createBillCalcLine(BillSegment_Id billSegmentId,String taxStatusCharVal, BigDecimal taxRateVal,int taxRateValListSize, CharacteristicType_Id pTaxStatCharType,
			String currency, BigDecimal taxAmount, String perTaxReg,BigDecimal totBsCalcamt, BigDecimal totalTaxAmount, int count,Boolean multipleTaxRateFlag, BigDecimal indServiceTax, BigDecimal bsCalcAmt,
			Boolean outOfScopeTaxFlg, Boolean zeroRatedFlg, Boolean zeroTariffFlg) {

		ArrayList<String> taxDataList = new ArrayList<String>();
		Bool isTaxExist = Bool.FALSE;
		Boolean exceptionFoundFlag = Boolean.FALSE;
		taxRate = taxRateVal;
		Bool isTaxValueExist = Bool.FALSE;		

		//******************************* create sequence number for tax calculation line********************//
		BigInteger calLineSize = BigInteger.ZERO;
		BigInteger sequence = BigInteger.ZERO;
		PreparedStatement selectMaxCalcLineSeq = null;
		try {
			selectMaxCalcLineSeq = createPreparedStatement("SELECT MAX(SEQNO) AS SEQ FROM CI_BSEG_CALC_LN WHERE BSEG_ID=:bsegId ","");
			selectMaxCalcLineSeq.bindId("bsegId", billSegmentId);
			selectMaxCalcLineSeq.setAutoclose(false);
			SQLResultRow rowSelectMaxCalcLineSeq = selectMaxCalcLineSeq.firstRow();
			if (notNull(rowSelectMaxCalcLineSeq)) {
				sequence = rowSelectMaxCalcLineSeq.getInteger("SEQ");
			}
			if (sequence == null) {
				sequence = BigInteger.ZERO;
			}
			calLineSize = BigInteger.TEN.add(sequence);
		} catch (Exception e) {
			logger.error("Exception in createBillCalcLine()", e);
		} finally {
			if (selectMaxCalcLineSeq != null) {
				selectMaxCalcLineSeq.close();
			}
		}

		BillSegmentCalculationHeader calcHeader = new BillSegmentCalculationHeader_Id(billSegmentId, BigInteger.ONE).getEntity();
		currency = calcHeader.getCurrency().getId().getTrimmedValue();
		int currencyExponent = calcHeader.getCurrency().getDecimalPositions().intValue();
		if (notNull(taxRate)) {
			CharacteristicType charTypeTaxStat = pTaxStatCharType.getEntity();
			try {
				if (count != 0 && TX_S_IND.equalsIgnoreCase(pTaxStatCharType.getTrimmedValue())) {
					taxAmount = ((indServiceTax.multiply(taxRate)).divide(new BigDecimal(100))).setScale(currencyExponent,BigDecimal.ROUND_HALF_UP);
				} else {
					taxAmount = ((bsCalcAmt.multiply(taxRate)).divide(new BigDecimal(100))).setScale(currencyExponent,BigDecimal.ROUND_HALF_UP);
				}

				BillCalculationLine_DTO calcLineDto = new BillCalculationLine_DTO();
				calcLineDto.setCalculatedAmount(taxAmount);
				calcLineDto.setDescriptionOnBill("Tax");
				calcLineDto.setCurrencyId(new Currency_Id(currency));
				calcLineDto.setPricccycdId(new Currency_Id(currency));
				if (count != 0 && TX_S_IND.equalsIgnoreCase(pTaxStatCharType.getTrimmedValue())) {
					calcLineDto.setBaseAmount(indServiceTax.setScale(currencyExponent,BigDecimal.ROUND_HALF_UP));
				} else {
					calcLineDto.setBaseAmount(bsCalcAmt.setScale(currencyExponent,BigDecimal.ROUND_HALF_UP));
				}
				calcLineDto.setDistributionCodeId(new GeneralLedgerDistributionCode_Id(getTaxDistributionCode().trim()));
				calcLineDto.setId(new BillCalculationLine_Id(calcHeader,calLineSize));
				BillCalculationLine billCalcLine = calcLineDto.newEntity();

				BillCalculationLineCharacteristics billCalcLineChars = billCalcLine.getCharacteristics();
				createBillCalcLineChar(billCalcLineChars,getBillCalculationLineTypeCharacteristictypeCode().getId(), getTaxDistributionCode());
				createBillCalcLineChar(billCalcLineChars,getTaxRateCharacteristictypeCode().getId(),taxRate.toString());

				createBillCalcLineChar(billCalcLineChars,getTaxRegimeCharacteristictypeCode().getId(),perTaxReg.trim());

				if (isReverseChargeTaxApplied) {
					createBillCalcLineChar(billCalcLineChars,getTaxScopeCharacteristictypeCode().getId(),getReverseCharge());
				}

				if (outOfScopeTaxFlg) {
					createBillCalcLineChar(billCalcLineChars,new CharacteristicType_Id("TX_S_OOS"),taxStatusCharVal.trim());
					isTaxValueExist = Bool.TRUE;
				} 
				else if (zeroRatedFlg) {
					createBillCalcLineChar(billCalcLineChars,new CharacteristicType_Id("TX_S_ZRT"),taxStatusCharVal.trim());
					isTaxValueExist = Bool.TRUE;
				}
				else if (zeroTariffFlg) {
					
					createBillCalcLineChar(billCalcLineChars,new CharacteristicType_Id("TX_S_ZTF"),taxStatusCharVal.trim());
					isTaxValueExist = Bool.TRUE;
				}
				
				else{
					
					for (CharacteristicValue value : charTypeTaxStat.getValues()) {
						if (value.fetchIdCharacteristicValue().trim().equalsIgnoreCase(taxStatusCharVal.trim())) {
							createBillCalcLineChar(billCalcLineChars,charTypeTaxStat.getId(),taxStatusCharVal.trim());
							isTaxValueExist = Bool.TRUE;
							break;
						}
					}
				}
			} catch (Exception e) {
				logger.error("Exception in createBillCalcLine()", e);
			}
		}

		if (isTaxValueExist.isTrue()) {
			// Total Calculation Amount
			if (count == 0) {
				totBsCalcamt = bsCalcAmt.add(taxAmount);
				totalTaxAmount = taxAmount;
			} else {
				totBsCalcamt = totBsCalcamt.add(taxAmount);
				totalTaxAmount = totalTaxAmount.add(taxAmount);
			}

		} else {
			exceptionFoundFlag = Boolean.TRUE;
			updateBsStat(billSegmentId,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.TAX_STAT_OPTION_VAL_NOT_FOUND),"10",getErrorDescription(String.valueOf(CustomMessages.TAX_STAT_OPTION_VAL_NOT_FOUND),taxStatusCharVal));
		}

		taxDataList.add(isTaxExist.toString());
		taxDataList.add(totBsCalcamt.toString());
		taxDataList.add(totalTaxAmount.toString());
		taxDataList.add(currency);
		taxDataList.add(exceptionFoundFlag.toString());
		return taxDataList;
	}

	private void updateBsStat(BillSegment_Id billSegmentId,String messageCategoryNumber,String messageNumber,String excpFlag,String messageInfo) {
		PreparedStatement preparedStatement = null;
		try {
			if (messageInfo != null	&& !"null".equalsIgnoreCase(messageInfo.trim())	&& messageInfo.trim().length() > 250) {
				messageInfo = messageInfo.substring(0, 250);
			}
			preparedStatement = createPreparedStatement("INSERT INTO CI_BSEG_EXCP (BSEG_ID,MESSAGE_CAT_NBR,MESSAGE_NBR,BSEG_EXCP_FLG,EXP_MSG,CRE_DTTM) values (:bsId, "
					+ " :messageCategory,:actualErrorMessageNumber,:bsExcepFlg,:errorDescription,SYSDATE)","");
			preparedStatement.bindId("bsId", billSegmentId);
			preparedStatement.bindString("messageCategory",messageCategoryNumber, "MESSAGE_CAT_NBR");
			preparedStatement.bindString("actualErrorMessageNumber",messageNumber, "MESSAGE_NBR");
			preparedStatement.bindString("bsExcepFlg", excpFlag,"BSEG_EXCP_FLG");
			preparedStatement.bindString("errorDescription", messageInfo,"EXP_MSG");
			preparedStatement.setAutoclose(false);
			preparedStatement.execute();
		} catch (Exception e) {
			logger.error("Exception in updateBsStat()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}
	}

	/**
	 * getErrorDescription() method selects error message description from ORMB message catalog. 
	 * @return errorInfo
	 */
	private static String getErrorDescription(String messageNumber,String taxStatusCharVal) {
		String errorInfo;
		if (messageNumber.equals(String.valueOf(CustomMessages.TAX_STAT_OPTION_VAL_NOT_FOUND))) {
			errorInfo = CustomMessageRepository.taxStatValueNotFound(messageNumber, taxStatusCharVal).getMessageText();
		} else {
			errorInfo = CustomMessageRepository.merchantError(messageNumber).getMessageText();
		}
		if (errorInfo.contains("Text:") && errorInfo.contains("Description:")) {
			errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),errorInfo.indexOf("Description:"));
		}
		return errorInfo;
	}
}
