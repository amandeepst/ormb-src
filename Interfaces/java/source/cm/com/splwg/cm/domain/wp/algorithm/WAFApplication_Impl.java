/*******************************************************************************
 * FileName                   : CmDetermineContractAlgComp_Impl
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : June 29 , 2015
 * Version Number             : 0.4
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Jun 29, 2015         Jaspreet    Implemented all requirements for CD2.
0.2      NA             Dec 10, 2016         Preeti      Fixed code to consider adjustments.
0.3      NA             Apr 03, 2017         Vienna Rom	 Convert subquery to join, fixed sonar issues
0.4      NA             Jun 09, 2017         Vienna Rom	 Removed logger.debug and used StringBuilder
 *******************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import java.util.HashMap;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.lookup.BusinessObjectActionLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.adjustment.adjustment.CreateFrozenAdjustmentData;
import com.splwg.ccb.domain.admin.adjustmentType.AdjustmentType_Id;
import com.splwg.ccb.domain.admin.billCycle.BillCycle;
import com.splwg.ccb.domain.admin.serviceAgreementType.SaTypePreBillCompletionAlgorithmSpot;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.person.Person_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author waliaj698
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = acctNbrTypeCd, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = saTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = wafSaTypeCode, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = wafAdjType, required = true, type = string)
 *            , @AlgorithmSoftParameter (name = wafIndCharacteristictypeCode, required = true, type =string)})
 */
public class WAFApplication_Impl extends WAFApplication_Gen implements
SaTypePreBillCompletionAlgorithmSpot {

	private static final Logger logger = LoggerFactory.getLogger(WAFApplication_Impl.class);
	private Bill bill;
	private String wafContractId;

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
	 * Main processing. Create WAF contract if not existing and transfer balance from sub-ledger to WAF contract.
	 */
	public void invoke() {

		//Retrieving AccountId
		String accountId = CommonUtils.CheckNull(retrieveAccountId(bill.getId())).trim();
		//Retrieving WAF Indicator
		String wafIndicator = CommonUtils.CheckNull(retrieveWAFInd(accountId));

		//If Y then WAF is Applicable on this Merchant/Account
		if("Y".equals(wafIndicator.trim())){
			
			//Retrieving Person
			String personId = CommonUtils.CheckNull(retrievePersonId(bill.getId())).trim();
			Person per = new Person_Id(personId).getEntity();

			String personDivision = "";
			if(notNull(per)){
				//Get Person's Division
				personDivision = per.getDivision();
			}

			//Create WAF contract if not already exists
			String wafContractStatus = createContract(accountId, getWafSaTypeCode(), personDivision);	

			if (CommonUtils.CheckNull(wafContractStatus).trim().startsWith("false")) {
				addError(CustomMessageRepository.WAFContractNotCreated());
			}else{
				//Calculate Bill value 
				BigDecimal billAmount = calculateBillAmount();
				//Create WAF Adjustments when bill is not of Zero Amount.
				if(billAmount.compareTo(BigDecimal.ZERO) != 0){
					//Fetch Funding Contract Id
					String fundContractId = retrieveFundingContract(accountId);
					ServiceAgreement fundContract = new ServiceAgreement_Id(fundContractId).getEntity();
					try{
						//Create Adjustment to move balances to WAF contract.
						//Below logic will create an adjustment of opposite sign from Bill Amount on Funding Contract 
						//and an equals and opposite signed adjustment on WAF Contract 
						CreateFrozenAdjustmentData frozenAdjustment = CreateFrozenAdjustmentData.Factory.newInstance();
						frozenAdjustment.setServiceAgreement(fundContract);//WAF CONTRACT 
						frozenAdjustment.setAdjustmentType(new AdjustmentType_Id(getWafAdjType()).getEntity());
						frozenAdjustment.setAdjustmentAmount(new Money(billAmount, bill.getAccount().getCurrency().getId()));
						frozenAdjustment.setAdjustmentDate(getSystemDateTime().getDate());
						frozenAdjustment.setAllowZeroAmount(Bool.FALSE);
						//Below line will create an opposite signed adjustment on Funding Contract.
						frozenAdjustment.setTransferServiceAgreement(new ServiceAgreement_Id(wafContractId).getEntity());//FUNDING CONTRACT
						fundContract.createFrozenAdjustment(frozenAdjustment);

					}catch(Exception e){
						logger.error("Exception while creating WAF adjustment", e);
					}
				}
			}			
		}
	}

	private String retrievePersonId(Bill_Id billId) {
		String perId = "";
		PreparedStatement preparedStatement = null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT ACCTPER.PER_ID ");
			sb.append("FROM CI_ACCT_PER ACCTPER, CI_BILL BILL ");
			sb.append("WHERE ACCTPER.ACCT_ID = BILL.ACCT_ID ");
			sb.append("AND BILL.BILL_ID = :billId ");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindId("billId", billId);
			SQLResultRow row = preparedStatement.firstRow();
			if(notNull(row)) {
				perId = row.getString("PER_ID");
			}
		} catch (Exception e) {
			logger.error("Exception in retrievePersonId()", e);
		} finally {
			if(preparedStatement!=null) {
				preparedStatement.close();
			}
		}
		return perId;
	}

	private String retrieveAccountId(Bill_Id billId) {
		String acctId = "";
		PreparedStatement preparedStatement=null;
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT B.ACCT_ID FROM CI_BILL B, CI_ACCT_NBR A");
			sb.append(" WHERE B.BILL_ID=:billId");
			sb.append(" AND B.ACCT_ID=A.ACCT_ID");
			sb.append(" AND A.ACCT_NBR_TYPE_CD='ACCTTYPE'");
			sb.append(" AND A.ACCT_NBR=:acctNbrTypeCd");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindId("billId", billId);
			preparedStatement.bindString("acctNbrTypeCd", getAcctNbrTypeCd(), "ACCT_NBR");
			SQLResultRow row = preparedStatement.firstRow();
			if(notNull(row)) {
				acctId = row.getString("ACCT_ID").trim();
			}
			else {
				addError(CustomMessageRepository.accountIdNotFound());
			}
		} catch (Exception e) {
			logger.error("Exception in retrieveAccountId()", e);
		} finally {
			if(preparedStatement!=null) {
				preparedStatement.close();
			}
		}
		return acctId;
	}


	private String retrieveWAFInd(String accountId) {
		String wafInd = "";
		PreparedStatement preparedStatement=null;
		try {				
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT CHAR_VAL FROM CI_ACCT_CHAR ");
			sb.append("WHERE ACCT_ID=:accountId AND CHAR_TYPE_CD=:wafIndCharacteristictypeCode");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindString("accountId", accountId, "ACCT_ID");
			preparedStatement.bindId("wafIndCharacteristictypeCode", new CharacteristicType_Id(getWafIndCharacteristictypeCode()));
			SQLResultRow row = preparedStatement.firstRow();
			if(notNull(row)) {
				wafInd = row.getString("CHAR_VAL").trim();
			}
		} catch (Exception e) {
			logger.error("Exception in retrieveWAFInd()", e);
		} finally {
			if(preparedStatement!=null) {
				preparedStatement.close();
			}
		}
		return wafInd;
	}

	private String retrieveFundingContract(String aAccountId){
		String contractId = "";
		PreparedStatement fundPreparedStatement = null;
		try {				
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT A.SA_ID AS SA_ID FROM CI_SA A, CI_SA_CHAR B");
			sb.append(" WHERE A.ACCT_ID=:acctId");
			sb.append(" AND trim(A.SA_TYPE_CD)=trim(:saTypeCd)");
			sb.append(" AND A.SA_STATUS_FLG IN ('20','30')");
			sb.append(" AND A.SA_ID=B.SA_ID");
			sb.append(" AND B.CHAR_TYPE_CD='SA_ID'");
			fundPreparedStatement = createPreparedStatement(sb.toString(),"");
			fundPreparedStatement.bindString("acctId", aAccountId, "ACCT_ID");
			fundPreparedStatement.bindString("saTypeCd", getSaTypeCode(), "SA_TYPE_CD");
			SQLResultRow row = fundPreparedStatement.firstRow();
			if(notNull(row)) {
				contractId = row.getString("SA_ID").trim();
			}
			else {
				addError(CustomMessageRepository.fundingSaIdNotFound());
			}
		} catch (Exception e) {
			logger.error("Exception in retrieveFundingContract()", e);
		} finally {
			if (fundPreparedStatement != null) {
				fundPreparedStatement.close();
			}
		}
		return contractId;
	}


	/**
	 * createContract() method creates a new contract record .
	 * 
	 * @param aInboundMerchantInterfaceId
	 * @param aAccountId
	 * @param contractTypeFlg
	 * @return
	 */
	private String createContract(String aAccountId, String contractTypeFlg,  String cisDivision) {
		PreparedStatement wafPreparedStatement = null;
		boolean newContractFlag;
		String contractId = "";
		try {
			StringBuilder sb = new StringBuilder();
			sb.append("SELECT A.SA_ID AS SA_ID FROM CI_SA A, CI_SA_CHAR B");
			sb.append(" WHERE A.ACCT_ID=:acctId");
			sb.append(" AND trim(A.SA_TYPE_CD)=trim(:saTypeCd)");
			sb.append(" AND A.SA_STATUS_FLG IN ('20','30')");
			sb.append(" AND A.SA_ID=B.SA_ID");
			sb.append(" AND B.CHAR_TYPE_CD='SA_ID'");
			wafPreparedStatement = createPreparedStatement(sb.toString(),"");
			wafPreparedStatement.bindString("acctId", aAccountId, "ACCT_ID");
			wafPreparedStatement.bindString("saTypeCd", getWafSaTypeCode(), "SA_TYPE_CD");
			SQLResultRow row = wafPreparedStatement.firstRow();
			if(notNull(row)) {
				contractId = row.getString("SA_ID");
				newContractFlag = false;
			} else {
				newContractFlag = true;
			}

			if (newContractFlag) { 			
				BusinessObjectInstance boInstance = BusinessObjectInstance.create("C1_SA");
				boInstance.set("division", cisDivision);
				boInstance.set("status", ServiceAgreementStatusLookup.constants.ACTIVE);
				boInstance.set("startDate", getSystemDateTime().getDate());
				boInstance.set("accountId", aAccountId);
				boInstance.set("saType", contractTypeFlg);

				BusinessObjectInstance createdBusinessObjectInstance = BusinessObjectDispatcher.execute(
						boInstance, BusinessObjectActionLookup.constants.FAST_ADD);

				contractId = createdBusinessObjectInstance.getString("serviceAgreement");
				createdBusinessObjectInstance = null;
				//Create entry in CI_SA_CHAR while creating contract
				COTSInstanceList serviceAgreementCharacteristicList = boInstance.getList("serviceAgreementCharacteristic");
				COTSInstanceListNode serviceAgreementCharacteristicListNode = serviceAgreementCharacteristicList.newChild();
				serviceAgreementCharacteristicListNode.set("characteristicType", "SA_ID");
				serviceAgreementCharacteristicListNode.set("adhocCharacteristicValue", contractId);
				serviceAgreementCharacteristicListNode.set("effectiveDate",getSystemDateTime().getDate());						
				boInstance.set("serviceAgreement", contractId);
				BusinessObjectDispatcher.execute(boInstance, BusinessObjectActionLookup.constants.FAST_UPDATE);
			}			
		} catch (Exception e) {
			logger.error("Exception occurred in createContract()" , e);
			String errorMessage = CommonUtils.CheckNull(e.getMessage());
			Map<String, String> errorMsg = new HashMap<String, String>();
			errorMsg = errorList(errorMessage);
			return "false" + "~" + errorMsg.get("Text") + "~"
			+ errorMsg.get("Category") + "~"
			+ errorMsg.get("Number");
		} finally {
			if (wafPreparedStatement != null) {
				wafPreparedStatement.close();

			}
		}
		wafContractId = contractId;
		return "true";
	}


	private BigDecimal calculateBillAmount(){
		BigDecimal billSegCalcAmt = BigDecimal.ZERO;
		BigDecimal adjCalcAmt = BigDecimal.ZERO;
		PreparedStatement preparedStatement=null;
		PreparedStatement preparedStatement1=null;
		StringBuilder sb;
		try {				
			sb = new StringBuilder();
			sb.append("SELECT SUM(C.CALC_AMT) AS AMT FROM CI_BSEG_CALC C, CI_BSEG B");
			sb.append(" WHERE C.BSEG_ID=B.BSEG_ID AND B.BILL_ID=:billId");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindEntity("billId", bill.getId().getEntity());
			SQLResultRow row = preparedStatement.firstRow();
			if(notNull(row)) {
				billSegCalcAmt = row.getBigDecimal("AMT");
				if (isNull(billSegCalcAmt)){
					billSegCalcAmt=BigDecimal.ZERO;
				}
			}
			sb = new StringBuilder();
			sb.append("SELECT SUM(F.CUR_AMT) AS AMT");
			sb.append(" FROM CI_FT F, CI_SA S, CI_BILL B");
			sb.append(" WHERE F.SA_ID=S.SA_ID");
			sb.append(" AND S.ACCT_ID=B.ACCT_ID");
			sb.append(" AND B.BILL_ID=:billId");
			sb.append(" AND S.SA_TYPE_CD ='FUND' ");			
			sb.append(" AND F.FT_TYPE_FLG IN ('AD','AX')");
			sb.append(" AND TRIM(F.BILL_ID) IS NULL");
			preparedStatement1 = createPreparedStatement(sb.toString(),"");
			preparedStatement1.bindEntity("billId", bill.getId().getEntity());
			SQLResultRow row1 = preparedStatement1.firstRow();
			if(notNull(row1)) {
				adjCalcAmt = row1.getBigDecimal("AMT");
				if (isNull(adjCalcAmt)){
					adjCalcAmt=BigDecimal.ZERO;
				}
			}
		} catch (Exception e) {
			logger.error("Exception in CalculateBillAmount()", e);
		} finally {
			if(preparedStatement!=null) {
				preparedStatement.close();				
			}
			if(preparedStatement1!=null) {
				preparedStatement1.close();				
			}
		}
		billSegCalcAmt=(billSegCalcAmt.add(adjCalcAmt)).setScale(2, BigDecimal.ROUND_HALF_UP);
		return billSegCalcAmt;
	}

	/**
	 * Map Interface is used to retrieve the Error Message Text, Error
	 * Message Number and Error message Category from Application Error
	 * Messages
	 * 
	 * @param aErrorMessage error message
	 * @return error map
	 */
	public Map<String, String> errorList(String aErrorMessage) {
		Map<String, String> errorMap = new HashMap<String, String>();
		String errorMessageNumber;
		String errorMessageCategory;
		if (aErrorMessage.contains("Number:")) {
			errorMessageNumber = aErrorMessage.substring(aErrorMessage.indexOf("Number:") + 8, 
					aErrorMessage.indexOf("Call Sequence:"));
			errorMap.put("Number", errorMessageNumber);
		}
		if (aErrorMessage.contains("Category:")) {
			errorMessageCategory = aErrorMessage.substring(aErrorMessage.indexOf("Category:") + 10, 
					aErrorMessage.indexOf("Number"));
			errorMap.put("Category", errorMessageCategory);
		}
		if (aErrorMessage.contains("Text:")
				&& aErrorMessage.contains("Description:")) {
			aErrorMessage = aErrorMessage.substring(aErrorMessage.indexOf("Text:"), 
					aErrorMessage.indexOf("Description:"));
		}
		if (aErrorMessage.length() > 250) {
			aErrorMessage = aErrorMessage.substring(0, 250);
			errorMap.put("Text", aErrorMessage);
		} else {
			aErrorMessage = aErrorMessage.substring(0, aErrorMessage.length());
			errorMap.put("Text", aErrorMessage);
		}
		return errorMap;
	}
}