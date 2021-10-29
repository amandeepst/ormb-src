/*******************************************************************************
* FileName                   : InboundAccountHierarchyLookUps.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Mar 24, 2015
* Version Number             : 0.2
* Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name       | Nature of Change
0.1      NA             Mar 24, 2015        Abhishek Paliwal   	Implemented all requirements for CD1.
0.2      NA             Jun 07, 2017        Ankur/Gaurav   	    NAP-14404 fix
0.3		 NAP-38072		Dec 17, 2018		Prerna				Performance changes
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;

public class InboundAccountHierarchyLookUps extends GenericBusinessObject {
	private String upload = "";

	private String error = "";

	private String pending = "";

	private String completed = "";

	private String funding = "";

	private String charging = "";

	private String chargeback = "";

	private String personNotFoundError = "";

	private String personHierarchyError = "";

	private String masterAccountNotFoundError = "";

	private String memberAccountNotFoundError = "";

	private String masterAccountUpdateError = "";

	private String contractCreationError = "";

	private String contractUpdateError = "";

	private String masterMasterAssociationError = "";
	
	private String divisionOfAccountsError = "";
	
	private String currencyCodesOfAccountsError = "";
	
	private String parentAndChildPersonsAreSameError = "";
	
	private String divisionOfPersonsError = "";
	
	private String currencyCodesOfPersonsError = "";
	
	private String currencyCodesOfMasterPersonAndMasterAccountError = "";
	
	private String currencyCodesOfChildPersonAndMemberAccountError = "";
	
	private String invalidHierarchyTypeError = "";
	
	private String personIdOneAndPersonIdTwoAreSameError = "";
	
	private String invalidCurrencyCodeForMasterPersonError = "";
	
	private String invalidCurrencyCodeForChildPersonError = "";
	
	private String activeSAStatus = "";

	private String errorMessageGroup = "";

	private String personStatus = "";

	private String parentPersonStatus = "";

	private String childPersonStatus = "";

	private String personHierarchyStatus = "";

	private String masterAccountStatus = "";

	private String memberAccountStatus = "";

	private String masterAccountUpdateStatus = "";

	private String contractCreationStatus = "";

	private String contractUpdateStatus = "";

	private String masterMasterAssociationStatus = "";
	
	private String divisionOfAccountsStatus = "";
	
	private String currencyCodesOfAccountsStatus = "";
	
	private String samePersonsStatus = "";
	
	private String divisionOfPersonsStatus = "";
	
	private String currencyCodesOfPersonStatus = "";
	
	private String currencyCodesOfMasterPersonAndMasterAccountStatus = "";
	
	private String currencyCodesOfChildPersonAndMemberAccountStatus = "";
	
	private String invalidHierarchyTypeStatus = "";
	
	private String samePersonIdNumbersStatus = "";
	
	private String invalidCurrencyCodeForMasterPersonStatus ="";
	
	private String invalidCurrencyCodeForChildPersonStatus ="";
	
	private String startDateFutureDateStatus = "";

	private String accountBusinessObject = "";

	private String contractBusinessObject = "";

	private String fundingContractType = "";

	private String dynamicReservesContractType = "";

	private String staticReservesContractType = "";

	private String cardRewardContractType = "";

	private String chargingContractType = "";

	private String recurringChargesContractType = "";

	private String chargebackContractType = "";

	private String idTypeCd = "";

	private String cardReward = "";

	private String igaCharacteristic = "";

	private String igaCharacteristicLabel = "";

	private String dateTimeCharacteristic = "";

	private String fkRefInvoicingCharacteristic = "";
	
	private String fkRefCancelInvoicingCharacteristic = "";

	private String externalAccountIdentifier = "";

	private String active = "";

	private String pendingStop = "";	
	
	private String invalidHierarchyStartDateMsg="";
	
	private String billingOnMasterAcctMsg="";
	
	private String billingOnMemberAcctMsg="";
	
	private String multipleParentErrorMsg="";

	private String pendingBillError="";
	
	private String invalidEndDateError="";
	
	private String activeSAAlreadyExistsError="";
	
	private String acctNotActive ="";
	
	private String acctInactiveStatus="";

	public String getAcctInactiveStatus() {
		return acctInactiveStatus;
	}

	public void setAcctInactiveStatus(String acctInactiveStatus) {
		this.acctInactiveStatus = acctInactiveStatus;
	}

	public String getAcctNotActive() {
		return acctNotActive;
	}

	public void setAcctNotActive(String acctNotActive) {
		this.acctNotActive = acctNotActive;
	}

	public String getActiveSAAlreadyExistsError() {
		return activeSAAlreadyExistsError;
	}

	public void setActiveSAAlreadyExistsError(String activeSAAlreadyExistsError) {
		this.activeSAAlreadyExistsError = activeSAAlreadyExistsError;
	}

	public String getUpload() {
		return upload;
	}

	public void setUpload(String upload) {
		this.upload = upload;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getPending() {
		return pending;
	}

	public void setPending(String pending) {
		this.pending = pending;
	}

	public String getCompleted() {
		return completed;
	}

	public void setCompleted(String completed) {
		this.completed = completed;
	}

	public String getFunding() {
		return funding;
	}

	public void setFunding(String funding) {
		this.funding = funding;
	}

	public String getCharging() {
		return charging;
	}

	public void setCharging(String charging) {
		this.charging = charging;
	}

	public String getChargeback() {
		return chargeback;
	}

	public void setChargeback(String chargeback) {
		this.chargeback = chargeback;
	}

	public String getPersonNotFoundError() {
		return personNotFoundError;
	}

	public void setPersonNotFoundError(String personNotFoundError) {
		this.personNotFoundError = personNotFoundError;
	}

	public String getPersonHierarchyError() {
		return personHierarchyError;
	}

	public void setPersonHierarchyError(String personHierarchyError) {
		this.personHierarchyError = personHierarchyError;
	}

	public String getMasterAccountNotFoundError() {
		return masterAccountNotFoundError;
	}

	public void setMasterAccountNotFoundError(String masterAccountNotFoundError) {
		this.masterAccountNotFoundError = masterAccountNotFoundError;
	}

	public String getMemberAccountNotFoundError() {
		return memberAccountNotFoundError;
	}

	public void setMemberAccountNotFoundError(String memberAccountNotFoundError) {
		this.memberAccountNotFoundError = memberAccountNotFoundError;
	}

	public String getMasterAccountUpdateError() {
		return masterAccountUpdateError;
	}

	public void setMasterAccountUpdateError(String masterAccountUpdateError) {
		this.masterAccountUpdateError = masterAccountUpdateError;
	}

	public String getContractCreationError() {
		return contractCreationError;
	}

	public void setContractCreationError(String contractCreationError) {
		this.contractCreationError = contractCreationError;
	}

	public String getContractUpdateError() {
		return contractUpdateError;
	}

	public void setContractUpdateError(String contractUpdateError) {
		this.contractUpdateError = contractUpdateError;
	}

	public String getMasterMasterAssociationError() {
		return masterMasterAssociationError;
	}

	public void setMasterMasterAssociationError(String masterMasterAssociationError) {
		this.masterMasterAssociationError = masterMasterAssociationError;
	}

	public String getActiveSAStatus() {
		return activeSAStatus;
	}

	public void setActiveSAStatus(String activeSAStatus) {
		this.activeSAStatus = activeSAStatus;
	}

	public String getDivisionOfAccountsError() {
		return divisionOfAccountsError;
	}

	public void setDivisionOfAccountsError(String divisionOfAccountsError) {
		this.divisionOfAccountsError = divisionOfAccountsError;
	}

	public String getCurrencyCodesOfAccountsError() {
		return currencyCodesOfAccountsError;
	}

	public void setCurrencyCodesOfAccountsError(String currencyCodesOfAccountsError) {
		this.currencyCodesOfAccountsError = currencyCodesOfAccountsError;
	}

	public String getParentAndChildPersonsAreSameError() {
		return parentAndChildPersonsAreSameError;
	}

	public void setParentAndChildPersonsAreSameError(
			String parentAndChildPersonsAreSameError) {
		this.parentAndChildPersonsAreSameError = parentAndChildPersonsAreSameError;
	}

	public String getDivisionOfPersonsError() {
		return divisionOfPersonsError;
	}

	public void setDivisionOfPersonsError(String divisionOfPersonsError) {
		this.divisionOfPersonsError = divisionOfPersonsError;
	}

	public String getCurrencyCodesOfPersonsError() {
		return currencyCodesOfPersonsError;
	}

	public void setCurrencyCodesOfPersonsError(String currencyCodesOfPersonsError) {
		this.currencyCodesOfPersonsError = currencyCodesOfPersonsError;
	}

	public String getCurrencyCodesOfMasterPersonAndMasterAccountError() {
		return currencyCodesOfMasterPersonAndMasterAccountError;
	}

	public void setCurrencyCodesOfMasterPersonAndMasterAccountError(
			String currencyCodesOfMasterPersonAndMasterAccountError) {
		this.currencyCodesOfMasterPersonAndMasterAccountError = currencyCodesOfMasterPersonAndMasterAccountError;
	}

	public String getCurrencyCodesOfChildPersonAndMemberAccountError() {
		return currencyCodesOfChildPersonAndMemberAccountError;
	}

	public void setCurrencyCodesOfChildPersonAndMemberAccountError(
			String currencyCodesOfChildPersonAndMemberAccountError) {
		this.currencyCodesOfChildPersonAndMemberAccountError = currencyCodesOfChildPersonAndMemberAccountError;
	}

	public String getInvalidHierarchyTypeError() {
		return invalidHierarchyTypeError;
	}

	public void setInvalidHierarchyTypeError(String invalidHierarchyTypeError) {
		this.invalidHierarchyTypeError = invalidHierarchyTypeError;
	}

	public String getPersonIdOneAndPersonIdTwoAreSameError() {
		return personIdOneAndPersonIdTwoAreSameError;
	}

	public void setPersonIdOneAndPersonIdTwoAreSameError(
			String personIdOneAndPersonIdTwoAreSameError) {
		this.personIdOneAndPersonIdTwoAreSameError = personIdOneAndPersonIdTwoAreSameError;
	}

	public String getInvalidCurrencyCodeForMasterPersonError() {
		return invalidCurrencyCodeForMasterPersonError;
	}

	public void setInvalidCurrencyCodeForMasterPersonError(
			String invalidCurrencyCodeForMasterPersonError) {
		this.invalidCurrencyCodeForMasterPersonError = invalidCurrencyCodeForMasterPersonError;
	}

	public String getInvalidCurrencyCodeForChildPersonError() {
		return invalidCurrencyCodeForChildPersonError;
	}

	public void setInvalidCurrencyCodeForChildPersonError(
			String invalidCurrencyCodeForChildPersonError) {
		this.invalidCurrencyCodeForChildPersonError = invalidCurrencyCodeForChildPersonError;
	}

	public String getErrorMessageGroup() {
		return errorMessageGroup;
	}

	public void setErrorMessageGroup(String errorMessageGroup) {
		this.errorMessageGroup = errorMessageGroup;
	}

	public String getPersonStatus() {
		return personStatus;
	}

	public void setPersonStatus(String personStatus) {
		this.personStatus = personStatus;
	}

	public String getParentPersonStatus() {
		return parentPersonStatus;
	}

	public void setParentPersonStatus(String parentPersonStatus) {
		this.parentPersonStatus = parentPersonStatus;
	}

	public String getChildPersonStatus() {
		return childPersonStatus;
	}

	public void setChildPersonStatus(String childPersonStatus) {
		this.childPersonStatus = childPersonStatus;
	}

	public String getPersonHierarchyStatus() {
		return personHierarchyStatus;
	}

	public void setPersonHierarchyStatus(String personHierarchyStatus) {
		this.personHierarchyStatus = personHierarchyStatus;
	}

	public String getMasterAccountStatus() {
		return masterAccountStatus;
	}

	public void setMasterAccountStatus(String masterAccountStatus) {
		this.masterAccountStatus = masterAccountStatus;
	}

	public String getMemberAccountStatus() {
		return memberAccountStatus;
	}

	public void setMemberAccountStatus(String memberAccountStatus) {
		this.memberAccountStatus = memberAccountStatus;
	}

	public String getMasterAccountUpdateStatus() {
		return masterAccountUpdateStatus;
	}

	public void setMasterAccountUpdateStatus(String masterAccountUpdateStatus) {
		this.masterAccountUpdateStatus = masterAccountUpdateStatus;
	}

	public String getContractCreationStatus() {
		return contractCreationStatus;
	}

	public void setContractCreationStatus(String contractCreationStatus) {
		this.contractCreationStatus = contractCreationStatus;
	}

	public String getContractUpdateStatus() {
		return contractUpdateStatus;
	}

	public void setContractUpdateStatus(String contractUpdateStatus) {
		this.contractUpdateStatus = contractUpdateStatus;
	}

	public String getMasterMasterAssociationStatus() {
		return masterMasterAssociationStatus;
	}

	public void setMasterMasterAssociationStatus(
			String masterMasterAssociationStatus) {
		this.masterMasterAssociationStatus = masterMasterAssociationStatus;
	}

	public String getDivisionOfAccountsStatus() {
		return divisionOfAccountsStatus;
	}

	public void setDivisionOfAccountsStatus(String divisionOfAccountsStatus) {
		this.divisionOfAccountsStatus = divisionOfAccountsStatus;
	}

	public String getCurrencyCodesOfAccountsStatus() {
		return currencyCodesOfAccountsStatus;
	}

	public void setCurrencyCodesOfAccountsStatus(
			String currencyCodesOfAccountsStatus) {
		this.currencyCodesOfAccountsStatus = currencyCodesOfAccountsStatus;
	}

	public String getSamePersonsStatus() {
		return samePersonsStatus;
	}

	public void setSamePersonsStatus(String samePersonsStatus) {
		this.samePersonsStatus = samePersonsStatus;
	}

	public String getDivisionOfPersonsStatus() {
		return divisionOfPersonsStatus;
	}

	public void setDivisionOfPersonsStatus(String divisionOfPersonsStatus) {
		this.divisionOfPersonsStatus = divisionOfPersonsStatus;
	}

	public String getCurrencyCodesOfPersonStatus() {
		return currencyCodesOfPersonStatus;
	}

	public void setCurrencyCodesOfPersonStatus(String currencyCodesOfPersonStatus) {
		this.currencyCodesOfPersonStatus = currencyCodesOfPersonStatus;
	}

	public String getCurrencyCodesOfMasterPersonAndMasterAccountStatus() {
		return currencyCodesOfMasterPersonAndMasterAccountStatus;
	}

	public void setCurrencyCodesOfMasterPersonAndMasterAccountStatus(
			String currencyCodesOfMasterPersonAndMasterAccountStatus) {
		this.currencyCodesOfMasterPersonAndMasterAccountStatus = currencyCodesOfMasterPersonAndMasterAccountStatus;
	}

	public String getCurrencyCodesOfChildPersonAndMemberAccountStatus() {
		return currencyCodesOfChildPersonAndMemberAccountStatus;
	}

	public void setCurrencyCodesOfChildPersonAndMemberAccountStatus(
			String currencyCodesOfChildPersonAndMemberAccountStatus) {
		this.currencyCodesOfChildPersonAndMemberAccountStatus = currencyCodesOfChildPersonAndMemberAccountStatus;
	}

	public String getInvalidHierarchyTypeStatus() {
		return invalidHierarchyTypeStatus;
	}

	public void setInvalidHierarchyTypeStatus(String invalidHierarchyTypeStatus) {
		this.invalidHierarchyTypeStatus = invalidHierarchyTypeStatus;
	}

	public String getSamePersonIdNumbersStatus() {
		return samePersonIdNumbersStatus;
	}

	public void setSamePersonIdNumbersStatus(String samePersonIdNumbersStatus) {
		this.samePersonIdNumbersStatus = samePersonIdNumbersStatus;
	}

	public String getInvalidCurrencyCodeForMasterPersonStatus() {
		return invalidCurrencyCodeForMasterPersonStatus;
	}

	public void setInvalidCurrencyCodeForMasterPersonStatus(
			String invalidCurrencyCodeForMasterPersonStatus) {
		this.invalidCurrencyCodeForMasterPersonStatus = invalidCurrencyCodeForMasterPersonStatus;
	}

	public String getInvalidCurrencyCodeForChildPersonStatus() {
		return invalidCurrencyCodeForChildPersonStatus;
	}

	public void setInvalidCurrencyCodeForChildPersonStatus(
			String invalidCurrencyCodeForChildPersonStatus) {
		this.invalidCurrencyCodeForChildPersonStatus = invalidCurrencyCodeForChildPersonStatus;
	}

	public String getStartDateFutureDateStatus() {
		return startDateFutureDateStatus;
	}

	public void setStartDateFutureDateStatus(String startDateFutureDateStatus) {
		this.startDateFutureDateStatus = startDateFutureDateStatus;
	}

	public String getAccountBusinessObject() {
		return accountBusinessObject;
	}

	public void setAccountBusinessObject(String accountBusinessObject) {
		this.accountBusinessObject = accountBusinessObject;
	}

	public String getContractBusinessObject() {
		return contractBusinessObject;
	}

	public void setContractBusinessObject(String contractBusinessObject) {
		this.contractBusinessObject = contractBusinessObject;
	}

	public String getFundingContractType() {
		return fundingContractType;
	}

	public void setFundingContractType(String fundingContractType) {
		this.fundingContractType = fundingContractType;
	}

	public String getDynamicReservesContractType() {
		return dynamicReservesContractType;
	}

	public void setDynamicReservesContractType(String dynamicReservesContractType) {
		this.dynamicReservesContractType = dynamicReservesContractType;
	}

	public String getStaticReservesContractType() {
		return staticReservesContractType;
	}

	public void setStaticReservesContractType(String staticReservesContractType) {
		this.staticReservesContractType = staticReservesContractType;
	}

	public String getCardRewardContractType() {
		return cardRewardContractType;
	}

	public void setCardRewardContractType(String cardRewardContractType) {
		this.cardRewardContractType = cardRewardContractType;
	}

	public String getChargingContractType() {
		return chargingContractType;
	}

	public void setChargingContractType(String chargingContractType) {
		this.chargingContractType = chargingContractType;
	}

	public String getRecurringChargesContractType() {
		return recurringChargesContractType;
	}

	public void setRecurringChargesContractType(String recurringChargesContractType) {
		this.recurringChargesContractType = recurringChargesContractType;
	}

	public String getChargebackContractType() {
		return chargebackContractType;
	}

	public void setChargebackContractType(String chargebackContractType) {
		this.chargebackContractType = chargebackContractType;
	}

	public String getIdTypeCd() {
		return idTypeCd;
	}

	public void setIdTypeCd(String idTypeCd) {
		this.idTypeCd = idTypeCd;
	}

	public String getCardReward() {
		return cardReward;
	}

	public void setCardReward(String cardReward) {
		this.cardReward = cardReward;
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

	public String getDateTimeCharacteristic() {
		return dateTimeCharacteristic;
	}

	public void setDateTimeCharacteristic(String dateTimeCharacteristic) {
		this.dateTimeCharacteristic = dateTimeCharacteristic;
	}

	public String getFkRefInvoicingCharacteristic() {
		return fkRefInvoicingCharacteristic;
	}

	public void setFkRefInvoicingCharacteristic(String fkRefInvoicingCharacteristic) {
		this.fkRefInvoicingCharacteristic = fkRefInvoicingCharacteristic;
	}

	public String getExternalAccountIdentifier() {
		return externalAccountIdentifier;
	}

	public void setExternalAccountIdentifier(String externalAccountIdentifier) {
		this.externalAccountIdentifier = externalAccountIdentifier;
	}

	public String getActive() {
		return active;
	}

	public void setActive(String active) {
		this.active = active;
	}

	public String getPendingStop() {
		return pendingStop;
	}

	public void setPendingStop(String pendingStop) {
		this.pendingStop = pendingStop;
	}
	
	public String getInvalidHierarchyStartDateMsg() {
		return invalidHierarchyStartDateMsg;
	}

	public void setInvalidHierarchyStartDateMsg(String invalidHierarchyStartDateMsg) {
		this.invalidHierarchyStartDateMsg = invalidHierarchyStartDateMsg;
	}
	
	public String getBillingOnMasterAcctMsg() {
		return billingOnMasterAcctMsg;
	}

	public void setBillingOnMasterAcctMsg(String billingOnMasterAcctMsg) {
		this.billingOnMasterAcctMsg = billingOnMasterAcctMsg;
	}

	public String getBillingOnMemberAcctMsg() {
		return billingOnMemberAcctMsg;
	}

	public void setBillingOnMemberAcctMsg(String billingOnMemberAcctMsg) {
		this.billingOnMemberAcctMsg = billingOnMemberAcctMsg;
	}	
	
	public String getMultipleParentErrorMsg() {
		return multipleParentErrorMsg;
	}

	public void setMultipleParentErrorMsg(String multipleParentErrorMsg) {
		this.multipleParentErrorMsg = multipleParentErrorMsg;
	}
	
	public String getPendingBillError() {
		return pendingBillError;
	}

	public void setPendingBillError(String pendingBillError) {
		this.pendingBillError = pendingBillError;
	}

	public String getInvalidEndDateError() {
		return invalidEndDateError;
	}

	public void setInvalidEndDateError(String invalidEndDateError) {
		this.invalidEndDateError = invalidEndDateError;
	}

	public String getFkRefCancelInvoicingCharacteristic() {
		return fkRefCancelInvoicingCharacteristic;
	}

	public void setFkRefCancelInvoicingCharacteristic(
			String fkRefCancelInvoicingCharacteristic) {
		this.fkRefCancelInvoicingCharacteristic = fkRefCancelInvoicingCharacteristic;
	}

	public InboundAccountHierarchyLookUps(){}

	public void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL "
					+ "WHERE FIELD_NAME =:fieldName","");
			preparedStatement.bindString("fieldName", "INT293031_OPT_TYPE_FLG", "FIELD_NAME");
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				fieldValue = resultSet.getString("FIELD_VALUE");
				valueName = resultSet.getString("VALUE_NAME");
				// **********************************Errors******************************************
				switch (fieldValue) {
				case "MSG0":
					setPersonNotFoundError(valueName);
					break;
				case "ACSA":
					setActiveSAAlreadyExistsError(valueName);
					break;
				case "ACS1":
					setActiveSAStatus(valueName);
					break;
				case "MSG1":
					setPersonHierarchyError(valueName);
					break;
				case "MSG2":
					setMasterAccountNotFoundError(valueName);
					break;
				case "MSG3":
					setMemberAccountNotFoundError(valueName);
					break;
				case "ACIN":
					setAcctNotActive(valueName);
					break;
				case "ACI1":
					setAcctInactiveStatus(valueName);
					break;
				case "MSG4":
					setMasterAccountUpdateError(valueName);
					break;
				case "MSG5":
					setContractCreationError(valueName);
					break;
				case "MSG6":
					setContractUpdateError(valueName);
					break;
				case "MSG7":
					setMasterMasterAssociationError(valueName);
					break;
				case "MSG8":
					setDivisionOfAccountsError(valueName);
					break;
				case "MSG9":
					setCurrencyCodesOfAccountsError(valueName);
					break;
				case "MS10":
					setParentAndChildPersonsAreSameError(valueName);
					break;
				case "MS11":
					setDivisionOfPersonsError(valueName);
					break;
				case "MS12":
					setCurrencyCodesOfPersonsError(valueName);
					break;
				case "MS13":
					setCurrencyCodesOfMasterPersonAndMasterAccountError(valueName);
					break;
				case "MS14":
					setCurrencyCodesOfChildPersonAndMemberAccountError(valueName);
					break;
				case "MS15":
					setInvalidHierarchyTypeError(valueName);
					break;
				case "MS16":
					setPersonIdOneAndPersonIdTwoAreSameError(valueName);
					break;
				case "MS17":
					setInvalidCurrencyCodeForMasterPersonError(valueName);
					break;
				case "MS18":
					setInvalidCurrencyCodeForChildPersonError(valueName);
					break;
				case "MCAT":
					setErrorMessageGroup(valueName);
					break;
				case "PRST":
					setPersonStatus(valueName);
					break;
				case "PHST":
					setPersonHierarchyStatus(valueName);
					break;
				case "PPST":
					setParentPersonStatus(valueName);
					break;
				case "CHST":
					setChildPersonStatus(valueName);
					break;
				case "MAST":
					setMasterAccountStatus(valueName);
					break;
				case "MEST":
					setMemberAccountStatus(valueName);
					break;
				case "MAUS":
					setMasterAccountUpdateStatus(valueName);
					break;
				case "COCR":
					setContractCreationStatus(valueName);
					break;
				case "COUP":
					setContractUpdateStatus(valueName);
					break;
				case "MMAA":
					setMasterMasterAssociationStatus(valueName);
					break;
				case "PDST":
					setDivisionOfAccountsStatus(valueName);
					break;
				case "CCST":
					setCurrencyCodesOfAccountsStatus(valueName);
					break;
				case "PCAS":
					setSamePersonsStatus(valueName);
					break;
				case "DDMC":
					setDivisionOfPersonsStatus(valueName);
					break;
				case "CCMC":
					setCurrencyCodesOfPersonStatus(valueName);
					break;
				case "CCMM":
					setCurrencyCodesOfMasterPersonAndMasterAccountStatus(valueName);
					break;
				case "CCCM":
					setCurrencyCodesOfChildPersonAndMemberAccountStatus(valueName);
					break;
				case "INHI":
					setInvalidHierarchyTypeStatus(valueName);
					break;
				case "PINS":
					setSamePersonIdNumbersStatus(valueName);
					break;
				case "ICUM":
					setInvalidCurrencyCodeForMasterPersonStatus(valueName);
					break;
				case "ICUC":
					setInvalidCurrencyCodeForChildPersonStatus(valueName);
					break;
				case "FDST":
					setStartDateFutureDateStatus(valueName);
					break;

				case "SAFD":
					setFundingContractType(valueName);
					break;
				case "SARD":
					setDynamicReservesContractType(valueName);
					break;
				case "SARS":
					setStaticReservesContractType(valueName);
					break;
				case "SACR":
					setCardRewardContractType(valueName);
					break;
				case "SACH":
					setChargingContractType(valueName);
					break;
				case "SARE":
					setRecurringChargesContractType(valueName);
					break;
				case "SACB":
					setChargebackContractType(valueName);
					break;					
				case "INHD":
					setInvalidHierarchyStartDateMsg(valueName);
					break;				
				case "MAAB":
					setBillingOnMasterAcctMsg(valueName);
					break;					
				case "MABC":
					setBillingOnMemberAcctMsg(valueName);
					break;
				case "MAMP":
					setMultipleParentErrorMsg(valueName);
					break;
				case "PDBL":
					setPendingBillError(valueName);
					break;
					
				// **************** End of Contract Types****************
				// **************** General Variables********************
				case "EXAI":
					setExternalAccountIdentifier(valueName);
					break;
				case "IGAC":
					setIgaCharacteristic(valueName);
					break;
				case "IGAL":
					setIgaCharacteristicLabel(valueName);
					break;
				case "SAFC":
					setFkRefInvoicingCharacteristic(valueName);
					break;
				case "DTST":
					setDateTimeCharacteristic(valueName);
					break;
				// *************** End of General Varibales
				// **********************
				case "SAUP":
					setContractUpdateError(valueName);
					break;

				case "MECA":
					setErrorMessageGroup(valueName);
					break;

				case "PEND":
					setPending(valueName);
					break;
				case "UPLD":
					setUpload(valueName);
					break;
				case "ERRR":
					setError(valueName);
					break;
				case "COMP":
					setCompleted(valueName);
					break;

				case "IDTY":
					setIdTypeCd(valueName);
					break;
				case "CRWD":
					setCardReward(valueName);
					break;

				case "ACBO":
					setAccountBusinessObject(valueName);
					break;
				case "SABO":
					setContractBusinessObject(valueName);
					break;

				case "FUND":
					setFunding(valueName);
					break;
				case "CHRG":
					setCharging(valueName);
					break;
				case "CHBK":
					setChargeback(valueName);
					break;

				case "SAAC":
					setActive(valueName);
					break;
				case "SAPS":
					setPendingStop(valueName);
					break;
				case "ERSC":
					setInvalidEndDateError(valueName);
					break;
				case "SACF":
					setFkRefCancelInvoicingCharacteristic(valueName);
					break;
				}
			}
		} catch(Exception e) {
			e.printStackTrace();
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

}
