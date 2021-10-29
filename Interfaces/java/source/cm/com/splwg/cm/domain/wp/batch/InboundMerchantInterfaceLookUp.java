/*******************************************************************************
* FileName                   : InboundMerchantInterfaceLookUp.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Mar 24, 2015
* Version Number             : 0.3
* Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Mar 24, 2015         Sunaina       Implemented all requirements for CD1.
0.2		 NA				Jun 15, 2015		 Preeti		   Implemented Requirements for CD2 (Tiered/WAF).
0.3		 NA				Sept 9, 2015		 Sunaina	   Changes for making characteristics dynamic.
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * InboundMerchantInterfaceLookUp class retrieves all the lookup values related with Merchant Interface.
 * 
 * @author Sunaina
 *
 */

public class InboundMerchantInterfaceLookUp extends GenericBusinessObject {
	
	public static final Logger logger = LoggerFactory.getLogger(InboundMerchantInterfaceLookUp.class);
	
	public InboundMerchantInterfaceLookUp() {
		setLookUpConstants();
	}
	
	private String upload = "";
	private String error = "";
	private String pending = "";
	private String completed = "";
	private String errorMessageCategory = "";
	private String personCharacteristic2 = "";
	private String fundAccountCharacteristic1 = "";
	private String personBusinessObject = "";
	private String accountBusinessObject = "";
	private String contractBusinessObject = "";
	private String idTypeCd = "";
	private String idTypeCd2 = "";
	private String accessGroup = "";
	private String accountRelationshipType = "";
	private String billRouteType = "";
	private String billCopies = "";
	private String fundingAccountType = "";
	private String chargingAccountType = "";
	private String chargebackAccountType = "";
	private String cardRewardAccountType = "";
	private String dynamicReservesContractType = "";
	private String staticReservesContractType = "";
	private String recurringChargesContractType = "";
	private String fundingAccountNumber = "";
	private String chargingAccountNumber = "";
	private String chargebackAccountNumber = "";
	private String cardRewardAccountNumber = "";
	private String igaCharacteristic = "";
	private String igaCharacteristicLabel = "";
	private String saCharacteristic = "";
	private String externalAccountIdentifier = "";
	private String internalAccountIdentifier = "";
	private String primaryFlag = "";
	private String address1 = "";
	private String msgNumber = "";
	private String collectionClass= "";
	private String defaultRateScheduleFlag = "";
	private String person = "";
	private String account = "";
	private String sa = "";
		
	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */
	private void setLookUpConstants(){
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL WHERE FIELD_NAME =:fieldName ","");
			preparedStatement.bindString("fieldName", "INT001_OPT_TYP_FLG", "FIELD_NAME");
			
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));
				if (fieldValue.equals("SARD")) {	//RSRV_D
					setDynamicReservesContractType(valueName);
				} else if (fieldValue.equals("SARS")) {	//RSRV_S
					setStaticReservesContractType(valueName);
				} else if (fieldValue.equals("SARE")) {	//RECUR
					setRecurringChargesContractType(valueName);
				} else if (fieldValue.equals("EAIT")) {	//C1_F_ANO
					setExternalAccountIdentifier(valueName);
				} else if (fieldValue.equals("IAIT")) {	//ACCTTYE
					setInternalAccountIdentifier(valueName);
				} else if (fieldValue.equals("PRIM")) {	//N
					setPrimaryFlag(valueName);
				} else if (fieldValue.equals("FUND")) {	//FUND
					setFundingAccountType(valueName);
				} else if (fieldValue.equals("CHRG")) {	//CHRG
					setChargingAccountType(valueName);
				} else if (fieldValue.equals("CHBK")) {	//CHBK
					setChargebackAccountType(valueName);
				} else if (fieldValue.equals("CRWD")) {	//CRWD
					setCardRewardAccountType(valueName);
				} else if (fieldValue.equals("FDAN")) {	// _FUND_
					setFundingAccountNumber(valueName);
				} else if (fieldValue.equals("CHAN")) {	//_CHRG_
					setChargingAccountNumber(valueName);
				} else if (fieldValue.equals("CBAN")) {	// _CHBK_
					setChargebackAccountNumber(valueName);
				} else if (fieldValue.equals("CRAN")) {	// _CRWD_
					setCardRewardAccountNumber(valueName);
				} else if (fieldValue.equals("PEND")) {	//PENDING
					setPending(valueName);
				} else if (fieldValue.equals("UPLD")) {	//UPLD
					setUpload(valueName);
				} else if (fieldValue.equals("ERRR")) {	//ERROR
					setError(valueName);
				} else if (fieldValue.equals("COMP")) {	//COMPLETED
					setCompleted(valueName);
				} else if (fieldValue.equals("MCAT")) {	//90000
					setErrorMessageCategory(valueName);
				} else if (fieldValue.equals("PCH2")) {	//WPBU
					setPersonCharacteristic2(valueName);
				} else if (fieldValue.equals("ACH1")) {	//IND WAF
					setFundAccountCharacteristic1(valueName);				
				} else if (fieldValue.equals("ACBO")) {	//C1_ACCOUNT_BO
					setAccountBusinessObject(valueName);
				} else if (fieldValue.equals("SABO")) {	//C1_SA
					setContractBusinessObject(valueName);
				} else if (fieldValue.equals("PRBO")) {	//C1_PERSON_BO
					setPersonBusinessObject(valueName);
				} else if (fieldValue.equals("IDTY")) {	//EXPRTYID
					setIdTypeCd(valueName);
				} else if (fieldValue.equals("VATI")) {	//VATID
					setIdTypeCd2(valueName);
				} else if (fieldValue.equals("AGRP")) {	// ***
					setAccessGroup(valueName);
				} else if (fieldValue.equals("BILL")) {	// 1
					setBillCopies(valueName);
				} else if (fieldValue.equals("BLRT")) {	//WPDOCUMK
					setBillRouteType(valueName);
				} else if (fieldValue.equals("ACRT")) {	//MAIN
					setAccountRelationshipType(valueName);
				} else if (fieldValue.equals("IGAC")) {	//C1_F_IGA
					setIgaCharacteristic(valueName);
				} else if (fieldValue.equals("IGAL")) {	//N
					setIgaCharacteristicLabel(valueName);
				} else if (fieldValue.equals("SAGC")) {	//SA_ID
					setSaCharacteristic(valueName);
				} else if (fieldValue.equals("ADDR")) {	//Address1
					setAddress1(valueName);
				} else if (fieldValue.equals("AEMN")) {	//0
					setMsgNumber(valueName);
				} else if (fieldValue.equals("DFRS")) {	//Y
					setDefaultRateScheduleFlag(valueName);
				} else if (fieldValue.equals("COLL")) {	//WPOVERDUE
					setCollectionClass(valueName);
				} else if (fieldValue.equals("PEET")) {	//PERS
					setPerson(valueName);
				} else if (fieldValue.equals("ACET")) {	//ACCT
					setAccount(valueName);
				} else if (fieldValue.equals("SAET")) {	//SA
					setSa(valueName);
				}
			
				fieldValue = "";
				valueName = "";
			}
		} catch(Exception e) {
			logger.error("Exception in Inbound Merchant Interface look Up "+ e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getAccessGroup() {
		return accessGroup;
	}

	public void setAccessGroup(String accessGroup) {
		this.accessGroup = accessGroup;
	}

	public String getAccountBusinessObject() {
		return accountBusinessObject;
	}

	public void setAccountBusinessObject(String accountBusinessObject) {
		this.accountBusinessObject = accountBusinessObject;
	}

	public String getAccountRelationshipType() {
		return accountRelationshipType;
	}

	public void setAccountRelationshipType(String accountRelationshipType) {
		this.accountRelationshipType = accountRelationshipType;
	}

	public String getAddress1() {
		return address1;
	}

	public void setAddress1(String address1) {
		this.address1 = address1;
	}

	public String getBillCopies() {
		return billCopies;
	}

	public void setBillCopies(String billCopies) {
		this.billCopies = billCopies;
	}

	public String getBillRouteType() {
		return billRouteType;
	}

	public void setBillRouteType(String billRouteType) {
		this.billRouteType = billRouteType;
	}

	public String getCardRewardAccountNumber() {
		return cardRewardAccountNumber;
	}

	public void setCardRewardAccountNumber(String cardRewardAccountNumber) {
		this.cardRewardAccountNumber = cardRewardAccountNumber;
	}

	public String getCardRewardAccountType() {
		return cardRewardAccountType;
	}

	public void setCardRewardAccountType(String cardRewardAccountType) {
		this.cardRewardAccountType = cardRewardAccountType;
	}

	public String getChargebackAccountNumber() {
		return chargebackAccountNumber;
	}

	public void setChargebackAccountNumber(String chargebackAccountNumber) {
		this.chargebackAccountNumber = chargebackAccountNumber;
	}

	public String getChargebackAccountType() {
		return chargebackAccountType;
	}

	public void setChargebackAccountType(String chargebackAccountType) {
		this.chargebackAccountType = chargebackAccountType;
	}

	public String getChargingAccountNumber() {
		return chargingAccountNumber;
	}

	public void setChargingAccountNumber(String chargingAccountNumber) {
		this.chargingAccountNumber = chargingAccountNumber;
	}

	public String getChargingAccountType() {
		return chargingAccountType;
	}

	public void setChargingAccountType(String chargingAccountType) {
		this.chargingAccountType = chargingAccountType;
	}
	public String getFundAccountCharacteristic1() {
		return fundAccountCharacteristic1;
	}

	public void setFundAccountCharacteristic1(String fundAccountCharacteristic1) {
		this.fundAccountCharacteristic1 = fundAccountCharacteristic1;
	}
	public String getCollectionClass() {
		return collectionClass;
	}

	public void setCollectionClass(String collectionClass) {
		this.collectionClass = collectionClass;
	}

	public String getCompleted() {
		return completed;
	}

	public void setCompleted(String completed) {
		this.completed = completed;
	}

	public String getContractBusinessObject() {
		return contractBusinessObject;
	}

	public void setContractBusinessObject(String contractBusinessObject) {
		this.contractBusinessObject = contractBusinessObject;
	}
	public String getDefaultRateScheduleFlag() {
		return defaultRateScheduleFlag;
	}

	public void setDefaultRateScheduleFlag(String defaultRateScheduleFlag) {
		this.defaultRateScheduleFlag = defaultRateScheduleFlag;
	}

	public String getDynamicReservesContractType() {
		return dynamicReservesContractType;
	}

	public void setDynamicReservesContractType(String dynamicReservesContractType) {
		this.dynamicReservesContractType = dynamicReservesContractType;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getErrorMessageCategory() {
		return errorMessageCategory;
	}

	public void setErrorMessageCategory(String errorMessageCategory) {
		this.errorMessageCategory = errorMessageCategory;
	}

	public String getExternalAccountIdentifier() {
		return externalAccountIdentifier;
	}

	public void setExternalAccountIdentifier(String externalAccountIdentifier) {
		this.externalAccountIdentifier = externalAccountIdentifier;
	}

	public String getFundingAccountNumber() {
		return fundingAccountNumber;
	}

	public void setFundingAccountNumber(String fundingAccountNumber) {
		this.fundingAccountNumber = fundingAccountNumber;
	}

	public String getFundingAccountType() {
		return fundingAccountType;
	}

	public void setFundingAccountType(String fundingAccountType) {
		this.fundingAccountType = fundingAccountType;
	}

	public String getIdTypeCd() {
		return idTypeCd;
	}

	public void setIdTypeCd(String idTypeCd) {
		this.idTypeCd = idTypeCd;
	}

	public String getIdTypeCd2() {
		return idTypeCd2;
	}

	public void setIdTypeCd2(String idTypeCd2) {
		this.idTypeCd2 = idTypeCd2;
	}

	public String getIgaCharacteristic() {
		return igaCharacteristic;
	}

	public void setIgaCharacteristic(String igaCharacteristic) {
		this.igaCharacteristic = igaCharacteristic;
	}

	public String getIgaCharacteristicLabel() {
		return igaCharacteristicLabel;
	}

	public void setIgaCharacteristicLabel(String igaCharacteristicLabel) {
		this.igaCharacteristicLabel = igaCharacteristicLabel;
	}

	public String getInternalAccountIdentifier() {
		return internalAccountIdentifier;
	}

	public void setInternalAccountIdentifier(String internalAccountIdentifier) {
		this.internalAccountIdentifier = internalAccountIdentifier;
	}
	public String getMsgNumber() {
		return msgNumber;
	}

	public void setMsgNumber(String msgNumber) {
		this.msgNumber = msgNumber;
	}

	public String getPending() {
		return pending;
	}

	public void setPending(String pending) {
		this.pending = pending;
	}

	public String getPersonBusinessObject() {
		return personBusinessObject;
	}

	public void setPersonBusinessObject(String personBusinessObject) {
		this.personBusinessObject = personBusinessObject;
	}
	public String getPersonCharacteristic2() {
		return personCharacteristic2;
	}

	public void setPersonCharacteristic2(String personCharacteristic2) {
		this.personCharacteristic2 = personCharacteristic2;
	}
	public String getPrimaryFlag() {
		return primaryFlag;
	}

	public void setPrimaryFlag(String primaryFlag) {
		this.primaryFlag = primaryFlag;
	}

	public String getRecurringChargesContractType() {
		return recurringChargesContractType;
	}

	public void setRecurringChargesContractType(String recurringChargesContractType) {
		this.recurringChargesContractType = recurringChargesContractType;
	}

	public String getSaCharacteristic() {
		return saCharacteristic;
	}

	public void setSaCharacteristic(String saCharacteristic) {
		this.saCharacteristic = saCharacteristic;
	}

	public String getStaticReservesContractType() {
		return staticReservesContractType;
	}

	public void setStaticReservesContractType(String staticReservesContractType) {
		this.staticReservesContractType = staticReservesContractType;
	}

	public String getUpload() {
		return upload;
	}

	public void setUpload(String upload) {
		this.upload = upload;
	}

	public String getPerson() {
		return person;
	}

	public void setPerson(String person) {
		this.person = person;
	}

	public String getAccount() {
		return account;
	}

	public void setAccount(String account) {
		this.account = account;
	}

	public String getSa() {
		return sa;
	}

	public void setSa(String sa) {
		this.sa = sa;
	}
	
}
