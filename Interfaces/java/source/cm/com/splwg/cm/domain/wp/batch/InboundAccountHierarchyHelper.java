/*******************************************************************************
 * FileName                   : InboundAccountHierarchyHelper.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 31, 2013
 * Version Number             : 0.6
 * Revision History     :
  VerNum | ChangeReqNum | Date Modification | Author Name    | Nature of Change
	0.1      NA             Mar 24, 2015     Abhishek Paliwal     Implemented all requirements for CD1.
	0.2      NA             Sep 21, 2015     Abhishek Paliwal  	  BO replaced from DTO
	0.3      NA             Oct 05, 2015     Abhishek Paliwal     Customer read flag related issue fixed
 	0.4      NA             Oct 27, 2015     Abhishek Paliwal     Charecteristics update related issue fixed.
 	0.5      NA             Apr 6, 2016      Sunaina              Updated as per Oracle Code review.
 	0.6      NA         	Jun 7, 2017      Ankur/Gaurav	      NAP-14404 fix
 	0.7      NA         	Sep 12, 2017     RIA	              NAP-32105 fix
	0.8      NA             Dec 05, 2018     RIA                  NAP-36897
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;
import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.CustomerReadLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType_Id;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.cm.domain.wp.batch.InboundAccountHierarchyInterface.InboundAccountHierarchyInterface_Id;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

public class InboundAccountHierarchyHelper extends GenericBusinessObject {

	public static final Logger logger = LoggerFactory
			.getLogger(InboundAccountHierarchyHelper.class);

	public String serviceAgreementId = "";
	
private InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = null;
	
	public InboundAccountHierarchyHelper() {
	}

	
	public InboundAccountHierarchyHelper(InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps){
		this.inboundAccountHierarchyLookUps = inboundAccountHierarchyLookUps;
	}
	
	/**
	 * @authorAbhishek accountCharecteristicUpdate() updates the 
	 *         masterAccount characteristic information details to make it
	 *         suitable for invoice grouping
	 * 
	 * @param inboundAccountHierarchyId
	 * @param masterAccountId
	 * @return
	 */


	// ***********************************************
		// accountCharecteristicUpdate method
		// **************************************************
		public String accountCharecteristicUpdate(
				InboundAccountHierarchyInterface_Id inboundAccountHierarchyId,
				String accountId, String characteristicValue) {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			//Date startDate = inboundAccountHierarchyId.getStartDate();
			Date effective_date = inboundAccountHierarchyId.getStartDate();
			try {
				// Since we have to update the CI_ACCT_CHAR table we need to delete
				// the contents from this table else it will throw unique key
				// Constraint error
				
				stringBuilder.append("DELETE FROM CI_ACCT_CHAR WHERE ACCT_ID=:accountId AND CHAR_TYPE_CD=:characteristicType");
				 preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountId", accountId, "ACCT_ID");
				preparedStatement.bindString("characteristicType", inboundAccountHierarchyLookUps.getIgaCharacteristic(), "CHAR_TYPE_CD");
				preparedStatement.executeUpdate();
				//To Update the CI_ACCT_CHAR table
				AccountCharacteristic_DTO accountCharacteristic_DTO=new AccountCharacteristic_DTO();
//				String effectiveDate[] = new String[3];
//				effectiveDate = startDate.split("-", 50);
//				//TODO: CHANGE HERE
//				Date effective_date=new Date(Integer
//						.parseInt(effectiveDate[0]), Integer
//						.parseInt(effectiveDate[1]), Integer
//						.parseInt(effectiveDate[2]));
				accountCharacteristic_DTO.setId(new AccountCharacteristic_Id(new CharacteristicType_Id(inboundAccountHierarchyLookUps.getIgaCharacteristic()),new Account_Id(accountId),effective_date));
				accountCharacteristic_DTO.setCharacteristicValue(characteristicValue);
				accountCharacteristic_DTO.newEntity();
			} catch (Exception e) {
				logger.error("Error:- ",e);//e.printStackTrace();
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~" + errorMsg.get("Number");
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return "true";

		}
	
	
	

	// ************************************************
	// End of accountCharecteristicUpdate method
	// ****************************************************
	
	// *************************************************
	// createAccountHierarchy method
	// ******************************************************
	/**
	 * @authorAbhishek createAccountHierarchy() method will act as a
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
		String startDate;
		String newContractCreateStatus = "";

		PreparedStatement preparedStatement3 = null;
		StringBuilder stringBuilder = new StringBuilder();

		try {
			stringBuilder.append("SELECT SA_ID,CIS_DIVISION,SA_TYPE_CD,START_DT FROM CI_SA WHERE ACCT_ID=:accountId2  AND SA_TYPE_CD=:saTypeCd");
			preparedStatement3 = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement3.bindString("accountId2", memberAccountId, "ACCT_ID");
			preparedStatement3.bindString("saTypeCd", contractType, "SA_TYPE_CD");
			preparedStatement3.setAutoclose(false);
			if (notNull(preparedStatement3.firstRow())) {
				memberAccountContract = preparedStatement3.firstRow().getString("SA_ID");
				cisdivision = preparedStatement3.firstRow().getString("CIS_DIVISION");
				saTypeCd = preparedStatement3.firstRow().getString("SA_TYPE_CD");
				startDate = String.valueOf(preparedStatement3.firstRow().getDate("START_DT"));

				newContractCreateStatus = createServiceAgreementForMasterAccount(
						inboundAccountHierarchyId, masterAccountId,
						memberAccountContract, cisdivision, saTypeCd, startDate);

				if (CommonUtils.CheckNull(newContractCreateStatus).trim().startsWith("false")) {
					return newContractCreateStatus;

				}
			}
		} catch (Exception e) {
			// Exception handling : update the status as hierarchy failed
			logger.error("Error:- ",e);//e.printStackTrace();
			String errorMessage = CommonUtils.CheckNull(e.getMessage());
			Map<String, String> errorMsg = new HashMap<String, String>();
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

	// *************************************************
	// createServiceAgreementForMasterAccount method
	// *****************************************************
	/**
	 * @authorAbhishek createServiceAgreementForMasterAccount() method
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
		//String startDate = inboundAccountHierarchyId.getStartDate();
		//String endDate = CommonUtils.CheckNull(inboundAccountHierarchyId.getEndDate());
		Date start_date = inboundAccountHierarchyId.getStartDate();
		Date endDate =inboundAccountHierarchyId.getEndDate();
		//DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		PreparedStatement contractPreparedStatement = null;
		
		StringBuilder saCharUpdate = new StringBuilder();
		PreparedStatement preparedStatement = null;
		
		boolean newContractFlag = false;
		String contractId = "";
		//java.util.Date expDate = null;
		//java.util.Date date = new java.util.Date();
		ServiceAgreement_DTO serviceAgreement_DTO=new ServiceAgreement_DTO();
		ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO=new ServiceAgreementCharacteristic_DTO();
		StringBuilder stringBuilder = new StringBuilder();
		try {
			//String startDateStringArray[] = new String[3];
			//startDateStringArray = startDate.split("-", 50);
			stringBuilder.append("SELECT DISTINCT B.SA_ID FROM CI_SA A, CI_SA_CHAR B, CI_SA_CHAR C WHERE " );
			stringBuilder.append(" A.SA_ID=B.SA_ID AND A.SA_ID=C.SA_ID AND B.CHAR_TYPE_CD=:charTypeCd AND C.CHAR_TYPE_CD=:fkRef AND C.CHAR_VAL_FK1=:memberContractId " );
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

			if (contractPreparedStatement.list().size() > 0) {
				serviceAgreementId = contractId = contractPreparedStatement.firstRow().getString("SA_ID");
				newContractFlag = false;
			} else {
				newContractFlag = true;
			}

			if(!newContractFlag){				
				// updating CI_SA
				ServiceAgreement_DTO serviceAgreement_DTO2= new ServiceAgreement_DTO();
				serviceAgreement_DTO.setId(new ServiceAgreement_Id(contractId)); 
				serviceAgreement_DTO2=serviceAgreement_DTO.getEntity().getDTO();
				
				/*Date start_date= new Date(Integer
						.parseInt(startDateStringArray[0]), Integer
						.parseInt(startDateStringArray[1]), Integer
						.parseInt(startDateStringArray[2]));*/
				serviceAgreement_DTO2.setStartDate(start_date);
				//String endDateStringArray[] = new String[3];
				/*if(notBlank(endDate)) {
					
					endDateStringArray = endDate.split("-", 50);
					serviceAgreement_DTO2.setEndDate( new Date(Integer
							.parseInt(endDateStringArray[0]), Integer
							.parseInt(endDateStringArray[1]), Integer
							.parseInt(endDateStringArray[2])));
					expDate = dateFormat.parse(endDate);
				}	*/
				serviceAgreement_DTO2.setAccountId(new Account_Id(masterAccountId));
				//if (expDate!=null && notBlank(endDate) && expDate.compareTo(date) < 0) {
				if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
					serviceAgreement_DTO2.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);

				} 
				else if(notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())){
					serviceAgreement_DTO2.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);
				}
				else {
					serviceAgreement_DTO2.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
				}
				serviceAgreement_DTO2.setEndDate(endDate);
				serviceAgreement_DTO2.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisdivision),saTypeCd));
				serviceAgreement_DTO.getEntity().setDTO(serviceAgreement_DTO2);		
				
			//updating CI_SA_CHAR
			//if (((expDate!=null && notBlank(endDate) && expDate.compareTo(date) > 0) || !notBlank(endDate))) {
			if ((notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) || isNull(endDate)) {	
							
				//ServiceAgreementCharacteristic_DTO	serviceAgreementCharacteristic_DTO2=new ServiceAgreementCharacteristic_DTO();
											
												/*Date effective_date=new Date(
														Integer.parseInt(startDateStringArray[0]), Integer
														.parseInt(startDateStringArray[1]), Integer
														.parseInt(startDateStringArray[2]));*/
											
							
							saCharUpdate.append("UPDATE CI_SA_CHAR SET EFFDT=:effdt,CHAR_VAL_FK1=:memberSaId,SRCH_CHAR_VAL=:memberSaId ");
							saCharUpdate.append("WHERE SA_ID=:masterSaId AND CHAR_TYPE_CD='C1_SAFCD' ");

							preparedStatement =createPreparedStatement(saCharUpdate.toString(),"");
							preparedStatement.bindString("memberSaId",memberAccountContract,"SA_ID");
							preparedStatement.bindString("masterSaId",serviceAgreement_DTO.getId().getIdValue(),"SA_ID");
							//preparedStatement.bindDate("effdt",effective_date);
							preparedStatement.bindDate("effdt",start_date);
							preparedStatement.executeUpdate();
			}
			}else{
				
				//values for CI_SA
				
//				Date start_date= new Date(Integer
//						.parseInt(startDateStringArray[0]), Integer
//						.parseInt(startDateStringArray[1]), Integer
//						.parseInt(startDateStringArray[2]));
				serviceAgreement_DTO.setStartDate(start_date);
				/*String endDateStringArray[] = new String[3];
				if(notBlank(endDate)) {
					endDateStringArray = endDate.split("-", 50);
					serviceAgreement_DTO.setEndDate( new Date(Integer
							.parseInt(endDateStringArray[0]), Integer
							.parseInt(endDateStringArray[1]), Integer
							.parseInt(endDateStringArray[2])));
					expDate = dateFormat.parse(endDate);
				}	*/	
				serviceAgreement_DTO.setAccountId(new Account_Id(masterAccountId));
				/*if (expDate!=null && notBlank(endDate) && expDate.compareTo(date) < 0) {
					serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);

				}*/
				if (notNull(endDate) && endDate.isBefore(getProcessDateTime().getDate())) {
					serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);

				}
				else {
					serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
				}
				serviceAgreement_DTO.setCustomerRead(CustomerReadLookup.constants.NO);
				serviceAgreement_DTO.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisdivision),saTypeCd));
				serviceAgreement_DTO.newEntity();
				contractId=serviceAgreement_DTO.getEntity().getId().getIdValue();
				if (((notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) || isNull(endDate))) {
					//values for CI_SA_CHAR 				

					serviceAgreementCharacteristic_DTO.setAdhocCharacteristicValue(startDt);
					/*Date effective_date=new Date(Integer.parseInt(startDateStringArray[0]),
					Integer.parseInt(startDateStringArray[1]),
					Integer.parseInt(startDateStringArray[2]));*/
					//serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(inboundAccountHierarchyLookUps.getDateTimeCharacteristic()),effective_date));
					serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(inboundAccountHierarchyLookUps.getDateTimeCharacteristic()),start_date));
					serviceAgreementCharacteristic_DTO.setSearchCharacteristicValue(startDt);
					serviceAgreementCharacteristic_DTO.newEntity();
					
					ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO2=new ServiceAgreementCharacteristic_DTO();
					//serviceAgreementCharacteristic_DTO2.setAdhocCharacteristicValue(startDt);
/*					Date effective_date2=new Date(Integer.parseInt(startDateStringArray[0]),
							Integer.parseInt(startDateStringArray[1]),
							Integer.parseInt(startDateStringArray[2]));*/
					serviceAgreementCharacteristic_DTO2.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic()),start_date));
					serviceAgreementCharacteristic_DTO2.setSearchCharacteristicValue(memberAccountContract);
					serviceAgreementCharacteristic_DTO2.setCharacteristicValueForeignKey1(memberAccountContract);
					serviceAgreementCharacteristic_DTO2.newEntity();
					
				}	
				
			}
				
		} catch (Exception e) {
			logger.error("Error:- ",e);//e.printStackTrace();
			String errorMessage = CommonUtils.CheckNull(e.getMessage());
			Map<String, String> errorMsg = new HashMap<String, String>();
			errorMsg = errorList(errorMessage);
			return "false" + "~" + errorMsg.get("Text") + "~"
					+ errorMsg.get("Category") + "~" + errorMsg.get("Number");
		} finally {
			if (contractPreparedStatement != null) {
				contractPreparedStatement.close();
				contractPreparedStatement = null;
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return "true";

	}

	// **************************************************
	// End of createServiceAgreementForMasterAccount method
	// ******************************************************

	// **************************************************
	// errorList method
	// ******************************************************
	/**
	 * @author Abhishek errorList() method is used to retrieve the actual
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

}// End of InboundAccountHierarchyHelper class

