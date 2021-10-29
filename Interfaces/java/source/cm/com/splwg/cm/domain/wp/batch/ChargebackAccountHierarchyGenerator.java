/*******************************************************************************
 * FileName                   : ChargebackAccountHierarchyGenerator.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015 
 * Version Number             : 0.3
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 24, 2015        ABHISHEK PALIWAL     Implemented all requirements for CD1. 
0.2      NA             Apr 6, 2016         Sunaina              Updated as per Oracle Code review.
0.3      NA             Jun 7, 2017         Ankur/Gaurav         NAP-14404 fix 
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.cm.domain.wp.batch.InboundAccountHierarchyInterface.InboundAccountHierarchyInterface_Id;

public class ChargebackAccountHierarchyGenerator extends GenericBusinessObject {

	private List<String> chargebackCustomerInfo=null;
	private InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();

	public ChargebackAccountHierarchyGenerator(InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps){
		this.inboundAccountHierarchyLookUps = inboundAccountHierarchyLookUps;
	}
	/**
	 * @author Venkat
	 * inboundChargebackAccountHierarchy() method determines Master and Member
	 * Chargeback Accounts and Updates Master Chargeback Account Characteristics
	 * and creates or updates new Chargeback contract with the help of
	 * InboundAccountHierarchyInterfaceHelper class.
	 * 
	 * @param inboundAccountHierarchyId
	 * @param masterPerson
	 * @param memberPerson
	 * @return
	 */
	public List<String> inboundChargebackAccountHierarchy(
			InboundAccountHierarchyInterface_Id inboundAccountHierarchyId,
			String masterPerson, String memberPerson) {

		String masterAccountId = "";
		String memberAccountId = "";
		String divisionOfMasterAccount = "";
		String divisionOfMemberAccount = "";
		String currencyCdOfMasterAccount = "";
		String currencyCdOfMemberAccount = "";
		String hierarchyType = inboundAccountHierarchyId.getHierarchType();
		String perIdNbr = inboundAccountHierarchyId.getPerIdNbr();
		String cisDivision = inboundAccountHierarchyId.getCisDivision();
		String currencyCd = inboundAccountHierarchyId.getCurrencyCd();
		String masterAccountNumber = perIdNbr.concat("_" + hierarchyType + "_").concat(currencyCd);
		String perIdNbr2 = inboundAccountHierarchyId.getPerIdNbr2();
		String memberAccountNumber = perIdNbr2.concat("_" + hierarchyType + "_").concat(currencyCd);
		StringBuilder stringBuilder = new StringBuilder();
		PreparedStatement preparedStatement = null;
		
		// *************************************************
		// Determing Master Chargeback Account
		// ****************************************************
		stringBuilder.append(" SELECT a.ACCT_ID, a.CIS_DIVISION, a.CURRENCY_CD FROM CI_ACCT a, CI_ACCT_NBR B, CI_ACCT_PER C");
		stringBuilder.append( " WHERE A.ACCT_ID = B.ACCT_ID AND B.ACCT_ID = C.ACCT_ID  ");
		stringBuilder.append( " AND B.ACCT_NBR_TYPE_CD = :extAcctIdentifier AND B.ACCT_NBR=:accountNumber and C. PER_ID=:personId	" );
		stringBuilder.append(" AND a.CIS_DIVISION =:cisDivision AND a.CURRENCY_CD =:currencyCd" );

		preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
		preparedStatement.bindString("extAcctIdentifier", inboundAccountHierarchyLookUps.getExternalAccountIdentifier(), "ACCT_NBR_TYPE_CD");
		preparedStatement.bindString("accountNumber", masterAccountNumber, "ACCT_NBR");
		preparedStatement.bindString("cisDivision", cisDivision, "CIS_DIVISION");
		preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
		preparedStatement.bindString("personId", masterPerson, "PER_ID");
		preparedStatement.setAutoclose(false);

		if (notNull(preparedStatement.firstRow())) {
			masterAccountId = preparedStatement.firstRow().getString("ACCT_ID");
			divisionOfMasterAccount = preparedStatement.firstRow().getString("CIS_DIVISION");
			currencyCdOfMasterAccount = preparedStatement.firstRow().getString("CURRENCY_CD");
		} else {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterAccountNotFoundError();
			String primaryKey = perIdNbr;
			if (notBlank(masterPerson)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
				primaryKey = perIdNbr;
			}

			chargebackCustomerInfo=new ArrayList<String>();
			chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			chargebackCustomerInfo.add(2,primaryKey);
			chargebackCustomerInfo.add(3,hierarchyType);
			chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			chargebackCustomerInfo.add(5,messageNumberLocal);
			chargebackCustomerInfo.add(6,messageNumberLocal);
			chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getMasterAccountStatus());
			chargebackCustomerInfo.add(8,"false");
			return chargebackCustomerInfo;
		}
		
			preparedStatement.close();
			preparedStatement = null;
		

		// ************************************************** 
		// End of Determing Master Chargeback Account
		// ****************************************************

		// ************************************************** 
		// Determing Member Chargeback Account
		// ****************************************************

		preparedStatement = createPreparedStatement(stringBuilder.toString(),"");

		preparedStatement.bindString("extAcctIdentifier", inboundAccountHierarchyLookUps.getExternalAccountIdentifier(), "ACCT_NBR_TYPE_CD");
		preparedStatement.bindString("accountNumber", memberAccountNumber, "ACCT_NBR");
		preparedStatement.bindString("cisDivision", cisDivision, "CIS_DIVISION");
		preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
		preparedStatement.bindString("personId", memberPerson, "PER_ID");
		preparedStatement.setAutoclose(false);

		if (notNull(preparedStatement.firstRow())) {
			memberAccountId = preparedStatement.firstRow().getString("ACCT_ID");
			divisionOfMemberAccount = preparedStatement.firstRow().getString("CIS_DIVISION");
			currencyCdOfMemberAccount = preparedStatement.firstRow().getString("CURRENCY_CD");
		} else {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getMemberAccountNotFoundError();
			String primaryKey = perIdNbr2;
			if (notBlank(memberPerson)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
				primaryKey = perIdNbr2;
			}

			chargebackCustomerInfo=new ArrayList<String>();
			chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			chargebackCustomerInfo.add(2,primaryKey);
			chargebackCustomerInfo.add(3,hierarchyType);
			chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			chargebackCustomerInfo.add(5,messageNumberLocal);
			chargebackCustomerInfo.add(6,messageNumberLocal);
			chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getMemberAccountStatus());
			chargebackCustomerInfo.add(8,"false");
			return chargebackCustomerInfo;
		}
		
			preparedStatement.close();
			preparedStatement = null;
		

		// ************************************************ 
		// End of Determing Member Chargeback Account
		// **************************************************
		// ****************************************************** 
		// Member Chargeback AccountUpdate
		// ************************************************************
		InboundAccountHierarchyHelper inboundAccountHierarchyHelper = new InboundAccountHierarchyHelper(inboundAccountHierarchyLookUps);
		if(divisionOfMasterAccount.equals(divisionOfMemberAccount) && currencyCdOfMasterAccount.equals(currencyCdOfMemberAccount) 
				&& currencyCdOfMasterAccount.equals(currencyCd) ) {
			stringBuilder = new StringBuilder();
			stringBuilder.append("SELECT 1 FROM CM_INV_GRP_STG WHERE PER_ID_NBR=:personIdNbr2 " );
			stringBuilder.append("AND PER_ID_NBR2=:personIdNbr " );
			stringBuilder.append(" AND HIER_TYPE=:hierarchyType AND BO_STATUS_CD=:boStatusCd AND END_DT<=SYSDATE");
			PreparedStatement postHierarchyPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			postHierarchyPreparedStatement.bindString("personIdNbr2", inboundAccountHierarchyId.getPerIdNbr2(), "PER_ID_NBR2");
			postHierarchyPreparedStatement.bindString("personIdNbr", inboundAccountHierarchyId.getPerIdNbr(), "PER_ID_NBR");
			postHierarchyPreparedStatement.bindString("hierarchyType", inboundAccountHierarchyId.getHierarchType(), "HIER_TYPE");
			postHierarchyPreparedStatement.bindString("boStatusCd", inboundAccountHierarchyLookUps.getCompleted(), "BO_STATUS_CD");
			postHierarchyPreparedStatement.setAutoclose(false);
			if(notNull(postHierarchyPreparedStatement.firstRow()))
			{
				String memberAccountUpdateStatus = inboundAccountHierarchyHelper
						.accountCharecteristicUpdate(inboundAccountHierarchyId,
								memberAccountId,"N");

				if (CommonUtils.CheckNull(memberAccountUpdateStatus).trim().startsWith("false")) {
					String actualErrorMessageNumber = memberAccountUpdateStatus
							.substring(memberAccountUpdateStatus.lastIndexOf("~") + 1,
									memberAccountUpdateStatus.length()).trim();
					String Message = memberAccountUpdateStatus.substring(
							memberAccountUpdateStatus.indexOf("~") + 1,
							memberAccountUpdateStatus.lastIndexOf("~") - 1);
					String actualErrorMessageCategory = Message.substring(
							Message.indexOf("~") + 1).trim();
					String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterAccountUpdateError();
					String messageKey = memberAccountId;

					if (notBlank(actualErrorMessageNumber)
							|| notBlank(actualErrorMessageCategory)) {
						actualErrorMessageNumber = messageNumberLocal;
						actualErrorMessageCategory = inboundAccountHierarchyLookUps.getErrorMessageGroup();
					}

					chargebackCustomerInfo=new ArrayList<String>();
					chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
					chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
					chargebackCustomerInfo.add(2,messageKey);
					chargebackCustomerInfo.add(3,hierarchyType);
					chargebackCustomerInfo.add(4,actualErrorMessageCategory);
					chargebackCustomerInfo.add(5,messageNumberLocal);
					chargebackCustomerInfo.add(6,actualErrorMessageNumber);
					chargebackCustomerInfo.add(7,memberAccountUpdateStatus);
					chargebackCustomerInfo.add(8,"false");
					return chargebackCustomerInfo;
				}
			}
			
				postHierarchyPreparedStatement.close();
				postHierarchyPreparedStatement = null;
			
			// *************************************************** 
			// End of Member Chargeback AccountUpdate
			// ******************************************************

			// ****************************************************** 
			// Master Chargeback AccountUpdate
			// ************************************************************

			String masterAccountUpdateStatus = "";
			stringBuilder = new StringBuilder();
			stringBuilder.append("SELECT 1 FROM CI_ACCT_CHAR WHERE ACCT_ID=:masterAccountId " );
			stringBuilder.append("AND CHAR_TYPE_CD=:charTypeCd AND CHAR_VAL=:charVal AND EFFDT  = :effDt");
			PreparedStatement accountCharacteristicPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			accountCharacteristicPreparedStatement.bindString("masterAccountId", masterAccountId, "ACCT_ID");
			accountCharacteristicPreparedStatement.bindString("charTypeCd", inboundAccountHierarchyLookUps.getIgaCharacteristic(), "CHAR_TYPE_CD");
			accountCharacteristicPreparedStatement.bindString("charVal", inboundAccountHierarchyLookUps.getIgaCharacteristicLabel(), "CHAR_VAL");
			accountCharacteristicPreparedStatement.bindDate("effDt", inboundAccountHierarchyId.getStartDate());
			accountCharacteristicPreparedStatement.setAutoclose(false);
			if(accountCharacteristicPreparedStatement.list().size() == 0)
			{
				masterAccountUpdateStatus = inboundAccountHierarchyHelper.accountCharecteristicUpdate(inboundAccountHierarchyId,
						masterAccountId, inboundAccountHierarchyLookUps.getIgaCharacteristicLabel());

				if (CommonUtils.CheckNull(masterAccountUpdateStatus).trim().startsWith("false")) {
					String actualErrorMessageNumber = masterAccountUpdateStatus
							.substring(masterAccountUpdateStatus.lastIndexOf("~") + 1,
									masterAccountUpdateStatus.length()).trim();
					String Message = masterAccountUpdateStatus.substring(
							masterAccountUpdateStatus.indexOf("~") + 1,
							masterAccountUpdateStatus.lastIndexOf("~") - 1);
					String actualErrorMessageCategory = Message.substring(
							Message.indexOf("~") + 1).trim();
					String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterAccountUpdateError();
					String messageKey = masterAccountId;

					if (notBlank(actualErrorMessageNumber)
							|| notBlank(actualErrorMessageCategory)) {
						actualErrorMessageNumber = messageNumberLocal;
						actualErrorMessageCategory = inboundAccountHierarchyLookUps.getErrorMessageGroup();
					}

					chargebackCustomerInfo=new ArrayList<String>();
					chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
					chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
					chargebackCustomerInfo.add(2,messageKey);
					chargebackCustomerInfo.add(3,hierarchyType);
					chargebackCustomerInfo.add(4,actualErrorMessageCategory);
					chargebackCustomerInfo.add(5,messageNumberLocal);
					chargebackCustomerInfo.add(6,actualErrorMessageNumber);
					chargebackCustomerInfo.add(7,masterAccountUpdateStatus);
					chargebackCustomerInfo.add(8,"false");
					return chargebackCustomerInfo;
				}
			}
			
				accountCharacteristicPreparedStatement.close();
				accountCharacteristicPreparedStatement = null;
			

			// *************************************************** 
			// End of Master Chargeback AccountUpdate
			// ******************************************************

			// ************************************************ 
			// Member Account Validate
			// ****************************************************		
			stringBuilder = new StringBuilder();
			stringBuilder.append("SELECT ACCT_ID FROM CI_ACCT_CHAR WHERE ACCT_ID=:accountId AND CHAR_TYPE_CD=:charType " );
			stringBuilder.append("AND CHAR_VAL=:charValue ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");

			preparedStatement.bindString("accountId", memberAccountId, "ACCT_ID");
			preparedStatement.bindString("charType", inboundAccountHierarchyLookUps.getIgaCharacteristic(), "CHAR_TYPE_CD");
			preparedStatement.bindString("charValue", inboundAccountHierarchyLookUps.getIgaCharacteristicLabel(), "CHAR_VAL");
			preparedStatement.setAutoclose(false);

			if (notNull(preparedStatement.firstRow())) {
				String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterMasterAssociationError();
				String primaryKey = perIdNbr;
				if (notBlank(memberAccountId)) {
					messageNumberLocal = inboundAccountHierarchyLookUps.getMemberAccountNotFoundError();
					primaryKey = perIdNbr2;
				}

				chargebackCustomerInfo=new ArrayList<String>();
				chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
				chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
				chargebackCustomerInfo.add(2,primaryKey);
				chargebackCustomerInfo.add(3,hierarchyType);
				chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
				chargebackCustomerInfo.add(5,messageNumberLocal);
				chargebackCustomerInfo.add(6,messageNumberLocal);
				chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getMasterMasterAssociationStatus());
				chargebackCustomerInfo.add(8,"false");
				return chargebackCustomerInfo;

			}

				preparedStatement.close();
				preparedStatement = null;

			// ******************************************************* 
			// End of Validating Member Account
			// *************************************************************



			// ***************************************************
			// Charging Account Hierarchy for Chargeback Contract
			// ******************************************************

			String chargebackAccountHierarchyStatus = inboundAccountHierarchyHelper
					.createAccountHierarchy(
							inboundAccountHierarchyId,
							masterAccountId,
							memberAccountId,
							hierarchyType,
							inboundAccountHierarchyLookUps.getChargebackContractType());
			String serviceAgreementId = inboundAccountHierarchyHelper.serviceAgreementId;
			if (CommonUtils.CheckNull(chargebackAccountHierarchyStatus).trim()
					.startsWith("false")) {
				String Message = chargebackAccountHierarchyStatus.substring(
						chargebackAccountHierarchyStatus.indexOf("~") + 1,
						chargebackAccountHierarchyStatus.lastIndexOf("~") - 1);
				String actualErrorMessageCategory = Message.substring(
						Message.indexOf("~") + 1).trim();
				String actualErrorMessageNumber = chargebackAccountHierarchyStatus
						.substring(
								chargebackAccountHierarchyStatus.lastIndexOf("~") + 1,
								chargebackAccountHierarchyStatus.length()).trim();
				String messageNumberLocal = inboundAccountHierarchyLookUps.getContractUpdateError();
				String messageKey = serviceAgreementId;
				if (notBlank(serviceAgreementId)) {
					messageNumberLocal = inboundAccountHierarchyLookUps.getContractUpdateError();
					messageKey = masterAccountId;
				}

				chargebackCustomerInfo=new ArrayList<String>();
				chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
				chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
				chargebackCustomerInfo.add(2,messageKey);
				chargebackCustomerInfo.add(3,inboundAccountHierarchyLookUps.getChargebackContractType());
				chargebackCustomerInfo.add(4,actualErrorMessageCategory);
				chargebackCustomerInfo.add(5,messageNumberLocal);
				chargebackCustomerInfo.add(6,actualErrorMessageNumber);
				chargebackCustomerInfo.add(7,chargebackAccountHierarchyStatus);
				chargebackCustomerInfo.add(8,"false");
				return chargebackCustomerInfo;
			}
		}
		//		ERROR IS THROWN IF DIVISIONS ARE DIFFERENT FOR MASTER AND MEMBER ACCOUNTS
		if(!divisionOfMasterAccount.equals(divisionOfMemberAccount)) {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getDivisionOfAccountsError();
			String primaryKey = perIdNbr;
			if (notBlank(masterPerson)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
				primaryKey = perIdNbr;
			}

			chargebackCustomerInfo=new ArrayList<String>();
			chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			chargebackCustomerInfo.add(2,primaryKey);
			chargebackCustomerInfo.add(3,hierarchyType);
			chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			chargebackCustomerInfo.add(5,messageNumberLocal);
			chargebackCustomerInfo.add(6,messageNumberLocal);
			chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getDivisionOfAccountsStatus());
			chargebackCustomerInfo.add(8,"false");
			return chargebackCustomerInfo;
		}
		//		ERROR IERROR IS THROWN IF CURRENCY CODES ARE DIFFERENT FOR MASTER AND MEMBER ACCOUNTS
		if(!currencyCdOfMasterAccount.equals(currencyCdOfMemberAccount)) {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getCurrencyCodesOfAccountsError();
			String primaryKey = perIdNbr;
			if (notBlank(masterPerson)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
				primaryKey = perIdNbr;
			}

			chargebackCustomerInfo=new ArrayList<String>();
			chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			chargebackCustomerInfo.add(2,primaryKey);
			chargebackCustomerInfo.add(3,hierarchyType);
			chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			chargebackCustomerInfo.add(5,messageNumberLocal);
			chargebackCustomerInfo.add(6,messageNumberLocal);
			chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getCurrencyCodesOfAccountsStatus());
			chargebackCustomerInfo.add(8,"false");
			return chargebackCustomerInfo;
		}
		//		ERROR IERROR IS THROWN IF CURRENCY CODES ARE DIFFERENT FOR MASTER PERSON AND MASTER ACCOUNTS		
		if(!currencyCd.equals(currencyCdOfMasterAccount)) {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getCurrencyCodesOfMasterPersonAndMasterAccountError();
			String primaryKey = perIdNbr;
			if (notBlank(masterPerson)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
				primaryKey = perIdNbr;
			}

			chargebackCustomerInfo=new ArrayList<String>();
			chargebackCustomerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			chargebackCustomerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			chargebackCustomerInfo.add(2,primaryKey);
			chargebackCustomerInfo.add(3,hierarchyType);
			chargebackCustomerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			chargebackCustomerInfo.add(5,messageNumberLocal);
			chargebackCustomerInfo.add(6,messageNumberLocal);
			chargebackCustomerInfo.add(7,inboundAccountHierarchyLookUps.getCurrencyCodesOfMasterPersonAndMasterAccountStatus());
			chargebackCustomerInfo.add(8,"false");
			return chargebackCustomerInfo;

		}
		//		
		// ********************************************************** 
		// End of Charging Account Hierarchy for Chargeback Contract
		// *************************************************************


		chargebackCustomerInfo=new ArrayList<String>();
		chargebackCustomerInfo.add(0,null);
		chargebackCustomerInfo.add(1,null);
		chargebackCustomerInfo.add(2,null);
		chargebackCustomerInfo.add(3,null);
		chargebackCustomerInfo.add(4,null);
		chargebackCustomerInfo.add(5,null);
		chargebackCustomerInfo.add(6,null);
		chargebackCustomerInfo.add(7,null);
		chargebackCustomerInfo.add(8,"true");
		return chargebackCustomerInfo;
	}

}// End of ChargebackAccountHierarchyGenerator Class
