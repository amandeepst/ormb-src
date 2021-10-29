/*******************************************************************************
 * FileName                   : AccountHierarchyGenerator.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 20, 2019 
 * Version Number             : 0.1
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 20, 2019        Somya Sikka     	Created one generator file for all account types. 
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.installation.InstallationHelper;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.CustomerReadLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.cm.domain.wp.batch.InboundAccountHierarchyInterface.InboundAccountHierarchyInterface_Id;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

public class CmAccountHierarchyGenerator extends GenericBusinessObject {
	
private InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = null;
public static final Logger logger = LoggerFactory.getLogger(CmAccountHierarchyGenerator.class);
private final ReentrantLock lock = new ReentrantLock();
public String serviceAgreementId = "";
public String contractId ="";
public static final BigInteger ENV_ID = InstallationHelper.getEnvironmentId();


	
	public CmAccountHierarchyGenerator(InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps){
		this.inboundAccountHierarchyLookUps = inboundAccountHierarchyLookUps;
	}
	
private List<String> customerInfo=null;

/*
 * inboundAccountHierarchy() method determines Master and Member
 * Accounts and Updates Master Account Characteristics and
 * creates or updates new Contracts
 * respectively with the help of InboundAccountHierarchyInterfaceHelper
 * class.
 * 
 * @param inboundAccountHierarchyId
 * @param masterPerson
 * @param memberPerson
 * @return
 */
public List<String> inboundAccountHierarchy(
		InboundAccountHierarchyInterface_Id inboundAccountHierarchyId,
		String masterAccountId, String memberAccountId) {

	String hierarchyType = inboundAccountHierarchyId.getHierarchType();
	String perIdNbr = inboundAccountHierarchyId.getPerIdNbr();
	String perIdNbr2 = inboundAccountHierarchyId.getPerIdNbr2();
	String recChargeContract = inboundAccountHierarchyLookUps.getRecurringChargesContractType();
	String chargeContract = inboundAccountHierarchyLookUps.getChargingContractType();
	String chargeBackContract = inboundAccountHierarchyLookUps.getChargebackContractType();
	String fundContract = inboundAccountHierarchyLookUps.getFundingContractType();
	String cardRewardContract = inboundAccountHierarchyLookUps.getCardRewardContractType();
						
		if (notNull(memberAcctValidate(memberAccountId))) {
			String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterMasterAssociationError();
			String primaryKey = perIdNbr;
			if (notBlank(memberAccountId)) {
				messageNumberLocal = inboundAccountHierarchyLookUps.getMemberAccountNotFoundError();
				primaryKey = perIdNbr2;
			}
			customerInfo=new ArrayList<String>();
			customerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			customerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			customerInfo.add(2,primaryKey);
			customerInfo.add(3,hierarchyType);
			customerInfo.add(4,inboundAccountHierarchyLookUps.getErrorMessageGroup());
			customerInfo.add(5,messageNumberLocal);
			customerInfo.add(6,messageNumberLocal);
			customerInfo.add(7,inboundAccountHierarchyLookUps.getMasterMasterAssociationStatus());
			customerInfo.add(8,"false");
			return customerInfo;

		}


		// *******************************************************
		// End of Validating Member Account
		// *********************************************************

		if(hierarchyType.equals(inboundAccountHierarchyLookUps.getCharging())) {
			
			String contracthierarchyStatus = endContractHierarchy(inboundAccountHierarchyId, masterAccountId, memberAccountId, chargeContract);
			List<String> errorRecord = errorHandling(contracthierarchyStatus, inboundAccountHierarchyId, masterAccountId, chargeContract);
			if(notNull(errorRecord)) {
				return errorRecord;
			}
			contracthierarchyStatus = endContractHierarchy(inboundAccountHierarchyId, masterAccountId, memberAccountId, recChargeContract);
			errorRecord = errorHandling(contracthierarchyStatus, inboundAccountHierarchyId, masterAccountId, recChargeContract);
			if(notNull(errorRecord)) {
				return errorRecord;
			}		}
		
		if(hierarchyType.equals(inboundAccountHierarchyLookUps.getFunding())) {
			String contracthierarchyStatus = endContractHierarchy(inboundAccountHierarchyId, masterAccountId, memberAccountId, fundContract);
			List<String> errorRecord = errorHandling(contracthierarchyStatus, inboundAccountHierarchyId, masterAccountId, fundContract);
			if(notNull(errorRecord)) {
				return errorRecord;
			}
		}
		
		if(hierarchyType.equals(inboundAccountHierarchyLookUps.getChargeback())) {
			String contracthierarchyStatus = endContractHierarchy(inboundAccountHierarchyId, masterAccountId, memberAccountId, chargeBackContract);			
			List<String> errorRecord = errorHandling(contracthierarchyStatus, inboundAccountHierarchyId, masterAccountId, chargeBackContract);
			if(notNull(errorRecord)) {
				return errorRecord;
			}		}
		
		if(hierarchyType.equals(inboundAccountHierarchyLookUps.getCardReward())) {
			String contracthierarchyStatus = endContractHierarchy(inboundAccountHierarchyId, masterAccountId, memberAccountId, cardRewardContract);
			List<String> errorRecord = errorHandling(contracthierarchyStatus, inboundAccountHierarchyId, masterAccountId, cardRewardContract);
			if(notNull(errorRecord)) {
				return errorRecord;
			}
		}

	customerInfo=new ArrayList<String>();
	customerInfo.add(0,null);
	customerInfo.add(1,null);
	customerInfo.add(2,null);
	customerInfo.add(3,null);
	customerInfo.add(4,null);
	customerInfo.add(5,null);
	customerInfo.add(6,null);
	customerInfo.add(7,null);
	customerInfo.add(8,"true");
	return customerInfo;
}


 public List<String> errorHandling(String contracthierarchyStatus, InboundAccountHierarchyInterface_Id inboundAccountHierarchyId, String masterAccountId, String chargeContract) {
	 String contractUpdateError = inboundAccountHierarchyLookUps.getContractUpdateError();
	 if (CommonUtils.CheckNull(contracthierarchyStatus).trim().startsWith("false")) {
			String Message = contracthierarchyStatus.substring(
					contracthierarchyStatus.indexOf("~") + 1,
					contracthierarchyStatus.lastIndexOf("~"));
			String actualErrorMessageCategory = Message.substring(
					Message.indexOf("~") + 1).trim();

			String messageNumberLocal = contractUpdateError;
			String messageKey = serviceAgreementId;
			if (notBlank(serviceAgreementId)) {
				messageNumberLocal = contractUpdateError;
				messageKey = masterAccountId;
			}

			customerInfo=new ArrayList<String>();
			customerInfo.add(0,inboundAccountHierarchyId.getTransactionHeaderId());
			customerInfo.add(1,inboundAccountHierarchyId.getTransactionDetailId());
			customerInfo.add(2,messageKey);
			customerInfo.add(3,chargeContract);
			customerInfo.add(4,actualErrorMessageCategory);
			customerInfo.add(5,messageNumberLocal);
			customerInfo.add(6,actualErrorMessageCategory);
			customerInfo.add(7,contracthierarchyStatus);
			customerInfo.add(8,"false");
			return customerInfo;
		}
return null;
 }
 
 public SQLResultRow memberAcctUpdate(InboundAccountHierarchyInterface_Id inboundAccountHierarchyId, String memberAccountId) {
	    StringBuilder stringBuilder = new StringBuilder();
	    PreparedStatement postHierarchyPreparedStatement = null;
	    try {
			
			stringBuilder.append("SELECT SC.SRCH_CHAR_VAL FROM CI_SA_CHAR SC,CI_SA MB WHERE SC.CHAR_TYPE_CD=:fkRef AND MB.ACCT_ID=:memberAcctId "); 
			stringBuilder.append("AND SC.SA_ID=MB.SA_ID AND MB.SA_STATUS_FLG IN (:status1,:status2)");
						
			postHierarchyPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			postHierarchyPreparedStatement.bindString("memberAcctId", memberAccountId , "ACCT_ID");
			postHierarchyPreparedStatement.bindString("fkRef",inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(),"CHAR_TYPE_CD");
			postHierarchyPreparedStatement.bindString("status1", inboundAccountHierarchyLookUps.getActive(), "SA_STATUS_FLG");
			postHierarchyPreparedStatement.bindString("status2", inboundAccountHierarchyLookUps.getPendingStop(), "SA_STATUS_FLG");
			postHierarchyPreparedStatement.setAutoclose(false);
			return postHierarchyPreparedStatement.firstRow();
		 }
		 finally {
				if (postHierarchyPreparedStatement != null) {
					postHierarchyPreparedStatement.close();
					postHierarchyPreparedStatement = null;
				}
			}
 }
 
 public SQLResultRow memberAcctValidate(String memberAccountId) {
	 StringBuilder stringBuilder = new StringBuilder();
	 PreparedStatement preparedStatement = null;
	 try {
		stringBuilder.append("SELECT ACCT_ID FROM CI_ACCT_CHAR WHERE ACCT_ID=:accountId AND CHAR_TYPE_CD=:charType " );
		stringBuilder.append("AND CHAR_VAL=:charValue ");
		preparedStatement = createPreparedStatement(stringBuilder.toString(),"");

		preparedStatement.bindString("accountId", memberAccountId, "ACCT_ID");
		preparedStatement.bindString("charType", inboundAccountHierarchyLookUps.getIgaCharacteristic(), "CHAR_TYPE_CD");
		preparedStatement.bindString("charValue", inboundAccountHierarchyLookUps.getIgaCharacteristicLabel(), "CHAR_VAL");
		preparedStatement.setAutoclose(false);
		return preparedStatement.firstRow();
	 }
	 finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		
 }
 
 public void updateSa(String contractId, Date startDate, Date endDate, String memberAccountContract, String masterAccountId, String cisdivision, String saTypeCd) {
	 	ServiceAgreement_DTO serviceAgreementDTO=new ServiceAgreement_DTO();
		// updating CI_SA
		ServiceAgreement_DTO serviceAgreementDTO2= new ServiceAgreement_DTO();
		serviceAgreementDTO.setId(new ServiceAgreement_Id(contractId)); 
		serviceAgreementDTO2=serviceAgreementDTO.getEntity().getDTO();
		serviceAgreementDTO2.setStartDate(startDate);
		serviceAgreementDTO2.setAccountId(new Account_Id(masterAccountId));
		if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);

		} 
		else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);
		}
		else {
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
		}
		serviceAgreementDTO2.setEndDate(endDate);
		serviceAgreementDTO2.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisdivision),saTypeCd));
		serviceAgreementDTO.getEntity().setDTO(serviceAgreementDTO2);		
		
	//updating CI_SA_CHAR
	if ((notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) || isNull(endDate)) {	
					
				StringBuilder saCharUpdate = new StringBuilder();
				PreparedStatement preparedStatement = null; 
				saCharUpdate.append("UPDATE CI_SA_CHAR SET EFFDT=:effdt,CHAR_VAL_FK1=:memberSaId,SRCH_CHAR_VAL=:memberSaId ");
				saCharUpdate.append("WHERE SA_ID=:masterSaId AND CHAR_TYPE_CD=:fkRef ");

				preparedStatement =createPreparedStatement(saCharUpdate.toString(),"");
				preparedStatement.bindString("memberSaId",memberAccountContract,"SA_ID");
				preparedStatement.bindString("masterSaId",serviceAgreementDTO.getId().getIdValue(),"SA_ID");
				preparedStatement.bindString("fkRef",inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(),"CHAR_TYPE_CD");
				preparedStatement.bindDate("effdt",startDate);
				preparedStatement.executeUpdate();
			}
	
	else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
		StringBuilder saCharUpdate = new StringBuilder();
		PreparedStatement preparedStatement = null; 
		saCharUpdate.append("UPDATE CI_SA_CHAR SET CHAR_TYPE_CD=:fkCanRef ");
		saCharUpdate.append("WHERE SA_ID=:masterSaId AND CHAR_TYPE_CD=:fkRef AND SRCH_CHAR_VAL=:memberSaId");

		preparedStatement =createPreparedStatement(saCharUpdate.toString(),"");
		preparedStatement.bindString("memberSaId",memberAccountContract,"SA_ID");
		preparedStatement.bindString("masterSaId",serviceAgreementDTO.getId().getIdValue(),"SA_ID");
		preparedStatement.bindString("fkRef",inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(),"CHAR_TYPE_CD");
		preparedStatement.bindString("fkCanRef",inboundAccountHierarchyLookUps.getFkRefCancelInvoicingCharacteristic(),"CHAR_TYPE_CD");
		preparedStatement.executeUpdate();
	}
 }

//*************************************************
	// createAccountHierarchy method
	// ******************************************************
	/**
	 * createAccountHierarchy() method will act as a
	 *         intermediate between the main and
	 *         createServiceAgreementForMasterAccount methods. The
	 *         createAccountHierarchy retrieves the Member accounts contract
	 *         details and passes these as arguments while calling the
	 *         createServiceAgreementForMasterAccount() method.
	 * 
	 * @param inboundAccountHierarchyId
	 * @param masterAccountId
	 * @param memberAccountId
	 * @param hierarchyType
	 * @param contractType
	 * @return
	 */

	public String createAccountHierarchy(
			InboundAccountHierarchyInterface_Id inboundAccountHierarchyId,
			String masterAccountId, String memberAccountId,
			String hierarchyType, String contractType) {
		String memberAccountContract = "";

		String cisdivision = "";
		String saTypeCd = "";
		Date startDate;
		String newContractCreateStatus = "";

		PreparedStatement preparedStatement3 = null;
		StringBuilder stringBuilder = new StringBuilder();

		try {
			stringBuilder.append("SELECT SA_ID,CIS_DIVISION,SA_TYPE_CD,START_DT FROM CI_SA WHERE ACCT_ID=:accountId2  AND SA_TYPE_CD=:saTypeCd ");
			stringBuilder.append(" AND SA_STATUS_FLG IN (:status1,:status2)");
			preparedStatement3 = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement3.bindString("accountId2", memberAccountId, "ACCT_ID");
			preparedStatement3.bindString("saTypeCd", contractType, "SA_TYPE_CD");
			preparedStatement3.bindString("status1", inboundAccountHierarchyLookUps.getActive(), "SA_STATUS_FLG");
			preparedStatement3.bindString("status2", inboundAccountHierarchyLookUps.getPendingStop(), "SA_STATUS_FLG");
			preparedStatement3.setAutoclose(false);
			if (notNull(preparedStatement3.firstRow())) {
				memberAccountContract = preparedStatement3.firstRow().getString("SA_ID");
				cisdivision = preparedStatement3.firstRow().getString("CIS_DIVISION");
				saTypeCd = preparedStatement3.firstRow().getString("SA_TYPE_CD");
				startDate = preparedStatement3.firstRow().getDate("START_DT");
				
				newContractCreateStatus = createServiceAgreementForMasterAccount(
						inboundAccountHierarchyId, masterAccountId,
						memberAccountContract, cisdivision, saTypeCd, String.valueOf(startDate));

				if (CommonUtils.CheckNull(newContractCreateStatus).trim().startsWith("false")) {
					return newContractCreateStatus;

				}
			}
		} catch (Exception e) {
			// Exception handling : update the status as hierarchy failed
			logger.error("Error:- "+e.getMessage());
			String errorMessage = CommonUtils.CheckNull(e.getMessage());
			Map<String, String> errorMsg = new HashMap<>();
			errorMsg = errorList(errorMessage);
			return "false" + "~" + errorMsg.get("Text") + "~"
					+ errorMsg.get("Category") + "~" + errorMsg.get("Number");

		} finally {
			cisdivision = "";
			saTypeCd = "";
			if (preparedStatement3 != null) {
				preparedStatement3.close();
				preparedStatement3 = null;
			}
		}
		return "true";

	}

	// **************************************************
	// End of createAccountHierarchy method
	// ******************************************************

	public List<SQLResultRow> getSaId(String memberAccountContract, String masterAccountId, String saTypeCd) {
		StringBuilder stringBuilder = new StringBuilder();
		PreparedStatement contractPreparedStatement = null;
		List<SQLResultRow> resultList=null;
		try {
			stringBuilder.append("SELECT DISTINCT B.SA_ID FROM CI_SA A, CI_SA_CHAR B, CI_SA_CHAR C WHERE " );
			stringBuilder.append(" A.SA_ID=B.SA_ID AND A.SA_ID=C.SA_ID AND B.CHAR_TYPE_CD=:charTypeCd AND C.CHAR_TYPE_CD=:fkRef AND C.SRCH_CHAR_VAL=:memberContractId " );
			stringBuilder.append(" AND A.ACCT_ID=:accountId AND A.SA_TYPE_CD=:saTypeCd AND A.SA_STATUS_FLG IN (:status1,:status2)");
			contractPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			contractPreparedStatement.bindString("charTypeCd", inboundAccountHierarchyLookUps.getDateTimeCharacteristic(), "CHAR_TYPE_CD");
			contractPreparedStatement.bindString("fkRef", inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(), "CHAR_TYPE_CD");
			contractPreparedStatement.bindString("memberContractId", memberAccountContract, "C.SA_ID");
			contractPreparedStatement.bindString("accountId", masterAccountId, "ACCT_ID");
			contractPreparedStatement.bindString("saTypeCd", saTypeCd, "SA_TYPE_CD");
			contractPreparedStatement.bindString("status1", inboundAccountHierarchyLookUps.getActive(), "SA_STATUS_FLG");
			contractPreparedStatement.bindString("status2", inboundAccountHierarchyLookUps.getPendingStop(), "SA_STATUS_FLG");
			contractPreparedStatement.setAutoclose(false);
			resultList=contractPreparedStatement.list();
			
		}
		finally {
			if(contractPreparedStatement != null) {
				contractPreparedStatement.close();
				contractPreparedStatement= null;
			}
		}

		return resultList;
		
	}
	// *************************************************
	// createServiceAgreementForMasterAccount method
	// *****************************************************
	/**
	 * createServiceAgreementForMasterAccount() method
	 *         creates or updates the new service agreement of master account.
	 *         It updates the SA Characteristic details
	 * 
	 * @param inboundAccountHierarchyId
	 * @param masterAccountId
	 * @param memberAccountContract
	 * @param cisdivision
	 * @param saTypeCd
	 * @param startDt
	 * @return
	 */
	private String createServiceAgreementForMasterAccount(
			InboundAccountHierarchyInterface_Id inboundAccountHierarchyId,
			String masterAccountId, String memberAccountContract,
			String cisdivision, String saTypeCd, String startDt) {
		Account_Id acctId1 = new Account_Id(masterAccountId);
		Date masterSetupDate = acctId1.getEntity().getSetUpDate();
		Date startDate = inboundAccountHierarchyId.getStartDate();
		if(masterSetupDate.isAfter((startDate))){
			startDate = masterSetupDate;
		}
		
		Date endDate =inboundAccountHierarchyId.getEndDate();
					
		boolean newContractFlag = false;
		
		try {
			List<SQLResultRow> resultSet = getSaId(memberAccountContract, masterAccountId, saTypeCd);
			if (!resultSet.isEmpty()) {
				serviceAgreementId = contractId = resultSet.get(0).getString("SA_ID");
				newContractFlag = false;												
			} else {
				newContractFlag = true;
			}
				
		} catch (Exception e) {
			logger.error("Error:- "+e.getMessage());
			String errorMessage = CommonUtils.CheckNull(e.getMessage());
			Map<String, String> errorMsg = new HashMap<>();
			errorMsg = errorList(errorMessage);
			return "false" + "~" + errorMsg.get("Text") + "~"
					+ errorMsg.get("Category") + "~" + errorMsg.get("Number");
		}

		if(isNull(endDate)) {
			if (newContractFlag) {
				createContract(startDate, endDate, masterAccountId, cisdivision, saTypeCd, startDt, memberAccountContract);
			} else {
				try {
					updateContract(contractId, startDate, endDate, memberAccountContract, masterAccountId, cisdivision, saTypeCd);
				} catch (Exception e) {
					logger.error("Error:- " + e.getMessage());
					String errorMessage = CommonUtils.CheckNull(e.getMessage());
					Map<String, String> errorMsg = new HashMap<>();
					errorMsg = errorList(errorMessage);
					return "false" + "~" + errorMsg.get("Text") + "~"
							+ errorMsg.get("Category") + "~" + errorMsg.get("Number");
				}
			}
		}
		
		return "true";

	}

	// **************************************************
	// End of createServiceAgreementForMasterAccount method
	// ******************************************************

	private void updateContract(String contractId, Date startDate,
			Date endDate, String memberAccountContract, String masterAccountId,
			String cisdivision, String saTypeCd) {

		ServiceAgreement_DTO serviceAgreementDTO=new ServiceAgreement_DTO();
		// updating CI_SA
		ServiceAgreement_DTO serviceAgreementDTO2= new ServiceAgreement_DTO();
		serviceAgreementDTO.setId(new ServiceAgreement_Id(contractId)); 
		serviceAgreementDTO2=serviceAgreementDTO.getEntity().getDTO();
		serviceAgreementDTO2.setStartDate(startDate);
		serviceAgreementDTO2.setAccountId(new Account_Id(masterAccountId));
		if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);

		} 
		else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);
		}
		else {
			serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
		}
		serviceAgreementDTO2.setEndDate(endDate);
		serviceAgreementDTO2.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisdivision),saTypeCd));
		serviceAgreementDTO.getEntity().setDTO(serviceAgreementDTO2);		
		
	//updating CI_SA_CHAR
	if ((notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) || isNull(endDate)) {	
					
	StringBuilder saCharUpdate = new StringBuilder();
	PreparedStatement preparedStatement = null; 
				saCharUpdate.append("UPDATE CI_SA_CHAR SET EFFDT=:effdt,CHAR_VAL_FK1=:memberSaId,SRCH_CHAR_VAL=:memberSaId ");
				saCharUpdate.append("WHERE SA_ID=:masterSaId AND CHAR_TYPE_CD=:fkRef ");

				preparedStatement =createPreparedStatement(saCharUpdate.toString(),"");
				preparedStatement.bindString("memberSaId",memberAccountContract,"SA_ID");
				preparedStatement.bindString("masterSaId",serviceAgreementDTO.getId().getIdValue(),"SA_ID");
				preparedStatement.bindString("fkRef",inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(),"CHAR_TYPE_CD");
				preparedStatement.bindDate("effdt",startDate);
				preparedStatement.executeUpdate();
			}
	}


	public void createContract(Date startDate, Date endDate, String masterAccountId, String cisDivision, String saTypeCd, String startDt, String memberAccountContract) {

			 try{
				 ServiceAgreement_Id saId=createServiceAgreement(startDate, endDate,masterAccountId, cisDivision, saTypeCd);
				 createServiceAgreementCharacteristics(saId,startDate,endDate,memberAccountContract,startDt);
			 }
			 catch (Exception e) {
				logger.error("Exception during contract creation in SA Tables" + e.getMessage());
				 ServiceAgreement_Id saId=createServiceAgreement(startDate, endDate, masterAccountId, cisDivision, saTypeCd);
				createServiceAgreementCharacteristics(saId,startDate,endDate,memberAccountContract,startDt);
			}
		}

	private ServiceAgreement_Id createServiceAgreement(Date startDate, Date endDate, String masterAccountId, String cisDivision, String saTypeCd) {

		/*ServiceAgreement_DTO serviceAgreementDTO=new ServiceAgreement_DTO();
		serviceAgreementDTO.setStartDate(startDate);
		serviceAgreementDTO.setAccountId(new Account_Id(masterAccountId));
		if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
			serviceAgreementDTO.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);

		}else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
			serviceAgreementDTO.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);
		}
		else {
			serviceAgreementDTO.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
		}
		serviceAgreementDTO.setEndDate(endDate);
		serviceAgreementDTO.setCustomerRead(CustomerReadLookup.constants.NO);
		serviceAgreementDTO.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisDivision),saTypeCd));
		serviceAgreementDTO.newEntity();
		contractId=serviceAgreementDTO.getEntity().getId().getIdValue();*/

		ServiceAgreement_Id saId=generateContractId(masterAccountId);

		StringBuilder sb=new StringBuilder();
		PreparedStatement ps=null;
		sb.append("INSERT INTO CI_SA (SA_ID,CIS_DIVISION,SA_TYPE_CD,START_DT,SA_STATUS_FLG,ACCT_ID,END_DT,CUST_READ_FLG,CURRENCY_CD) " );
		sb.append("VALUES (:saId,:cisDivision,:saTypeCd,:startDate,:saStatusFlg,:acctId,:endDate,:custSw,:currencyCd)");
		ps=createPreparedStatement(sb.toString(),"");
		ps.setAutoclose(false);
		ps.bindId("saId",saId);
		ps.bindString("cisDivision",cisDivision,"CIS_DIVISION");
		ps.bindString("saTypeCd",saTypeCd,"SA_TYPE_CD");
		ps.bindDate("startDate",startDate);
		if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
			ps.bindLookup("saStatusFlg",ServiceAgreementStatusLookup.constants.PENDING_STOP);

		}else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
			ps.bindLookup("saStatusFlg",ServiceAgreementStatusLookup.constants.CANCELED);
		}
		else {
			ps.bindLookup("saStatusFlg",ServiceAgreementStatusLookup.constants.ACTIVE);
		}
		ps.bindString("acctId",masterAccountId,"ACCT_ID");
		ps.bindDate("endDate",endDate);
		ps.bindLookup("custSw",CustomerReadLookup.constants.NO);
		ps.bindEntity("currencyCd",new Account_Id(masterAccountId).getEntity().getCurrency());
		ps.executeUpdate();

		if(ps != null){
			ps.close();
			ps=null;
		}

		sb=new StringBuilder();
		sb.append("INSERT INTO CI_SA_K VALUES (:saId,:envId) ");
		ps=createPreparedStatement(sb.toString(),"");
		ps.bindId("saId",saId);
		ps.bindBigInteger("envId", ENV_ID);
		ps.setAutoclose(false);
		ps.executeUpdate();
		if(ps != null){
			ps.close();
			ps=null;
		}

		return saId;
	}

	private ServiceAgreement_Id generateContractId(String masterAccountId) {

		//Preparing a CallableStatement
		PreparedStatement ps=null;
		ps=createPreparedStatement("SELECT FN_GET_SA_ID(:acctId) SA_ID FROM DUAL","");
		ps.setAutoclose(false);
		ps.bindString("acctId",masterAccountId,"ACCT_ID");
		String sa=ps.firstRow().getString("SA_ID");
		ServiceAgreement_Id saId=new ServiceAgreement_Id(sa);
		if(ps != null){
			ps.close();
			ps=null;
		}

		return saId;
	}

	private void createServiceAgreementCharacteristics(ServiceAgreement_Id contractId, Date startDate, Date endDate, String memberAccountContract, String startDt) {
		ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristicDTO=new ServiceAgreementCharacteristic_DTO();

		if (((notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) || isNull(endDate))) {
			//values for CI_SA_CHAR

			serviceAgreementCharacteristicDTO.setAdhocCharacteristicValue(startDt);
			serviceAgreementCharacteristicDTO.setId(new ServiceAgreementCharacteristic_Id(contractId,new CharacteristicType_Id(inboundAccountHierarchyLookUps.getDateTimeCharacteristic()),startDate));
			serviceAgreementCharacteristicDTO.setSearchCharacteristicValue(startDt);
			serviceAgreementCharacteristicDTO.newEntity();

			ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristicDTO2=new ServiceAgreementCharacteristic_DTO();

			serviceAgreementCharacteristicDTO2.setId(new ServiceAgreementCharacteristic_Id(contractId,new CharacteristicType_Id(inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic()),startDate));
			serviceAgreementCharacteristicDTO2.setSearchCharacteristicValue(memberAccountContract);
			serviceAgreementCharacteristicDTO2.setCharacteristicValueForeignKey1(memberAccountContract);
			serviceAgreementCharacteristicDTO2.newEntity();
		}
	}


	public String endContractHierarchy(InboundAccountHierarchyInterface_Id inboundAccountHierarchyId, String masterAccountId, String memberAccountId, String contractType) {
	String hierarchyType = inboundAccountHierarchyId.getHierarchType();
	String accountHierarchyStatus = createAccountHierarchy(
					inboundAccountHierarchyId, masterAccountId,
					memberAccountId, hierarchyType,
					contractType);
	return accountHierarchyStatus;
	}

		// **************************************************
		// errorList method
		// ******************************************************
		/**
		 * errorList() method is used to retrieve the actual
		 *         error message number, message category , description and text
		 *         information from errorMessage
		 * 
		 * @param errorMessage
		 * @return
		 */
		public Map<String, String> errorList(String errorMessage) {
			Map<String, String> errorMap = new HashMap<String, String>();
			String errorMessageNumber = "";
			String errorMessageCategory = "";
			if (errorMessage.contains("Number:")) {
				errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));
				errorMap.put("Number", errorMessageNumber);
			}
			if (errorMessage.contains("Category:")) {
				errorMessageCategory = errorMessage.substring(errorMessage
						.indexOf("Category:") + 10, errorMessage.indexOf("Number"));
				errorMap.put("Category", errorMessageCategory);
			}
			if (errorMessage.contains("Text:")
					&& errorMessage.contains("Description:")) {
				errorMessage = errorMessage.substring(
						errorMessage.indexOf("Text:"), errorMessage
								.indexOf("Description:"));
			}
			if (errorMessage.length() > 250) {
				errorMessage = errorMessage.substring(0, 250);
				errorMap.put("Text", errorMessage);
			} else {
				errorMessage = errorMessage.substring(0, errorMessage.length());
				errorMap.put("Text", errorMessage);
			}
			return errorMap;
		}
		// **************************************
		// End of errorList method
		// ****************************************

}// End ofAccountHierarchyGenerator Class
