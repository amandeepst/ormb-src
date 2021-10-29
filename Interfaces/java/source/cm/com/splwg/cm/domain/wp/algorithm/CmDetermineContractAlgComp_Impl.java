/*******************************************************************************
 * FileName                   : CmDetermineContractAlgComp_Impl
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Sep 03, 2015
 * Version Number             : 0.6
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Sep 03, 2015         Jaspreet    Implemented all requirements for CD2.
0.2      NA             Oct 05, 2015         Jaspreet    Implemented fix for Hierarchy scenarios.
0.3      NA             Mar 23, 2017         Preeti      Performance review update.
0.4      NA             Apr 25, 2017         Vienna      Added creation of WAF contract.
0.5      NA             Jun 09, 2017         Vienna      Removed logger.debug and used StringBuilder.
0.6      NA             Feb 01, 2018         Preeti      NAP-22632 Making contract determination logic generic.
0.7		 NAP-39326		Jan 21, 2018		 Somya		 Contract determination to move adjustment up hierarchy
*******************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import com.splwg.base.api.businessObject.BusinessObjectDispatcher;
import com.splwg.base.api.businessObject.BusinessObjectInstance;
import com.splwg.base.api.businessObject.COTSInstanceList;
import com.splwg.base.api.businessObject.COTSInstanceListNode;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.lookup.BusinessObjectActionLookup;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.adjustment.adjustmentUploadStaging.AdjustmentUpload;
import com.splwg.ccb.domain.adjustment.adjustmentUploadStaging.AdjustmentUpload_Id;
import com.splwg.ccb.domain.admin.adjustmentType.AdjustmentTypeDetermineSAAlgorithmSpot;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.cm.domain.wp.batch.InboundMerchantInterfaceLookUp;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author waliaj698
 *
 @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = charTypeCd, required = true, type = string)})
 */
public class CmDetermineContractAlgComp_Impl extends CmDetermineContractAlgComp_Gen implements AdjustmentTypeDetermineSAAlgorithmSpot {
	
	private static final Logger logger = LoggerFactory.getLogger(CmDetermineContractAlgComp_Impl.class);
	
	// Hard Parameters
	private AdjustmentUpload_Id adjustmentUploadId;
	private ServiceAgreement contract;
	private Bool isInSuspense;
	
	public void setAdjustmentUploadStagingId(AdjustmentUpload_Id paramAdjustmentUploadId) {
		adjustmentUploadId = paramAdjustmentUploadId;
	}

	public ServiceAgreement getServiceAgreement() {
		return contract;
	}

	public boolean isInSuspense() {
		return isInSuspense.value();
	}

	/**
	 * Main processing.
	 */
	public void invoke() {
		
		// Get the adjustment upload entity
		AdjustmentUpload adjustmentUpload = adjustmentUploadId.getEntity();
		PreparedStatement preparedStatement = null;
		InboundMerchantInterfaceLookUp inboundMerchantInterfaceLookUp = new InboundMerchantInterfaceLookUp();		
		String adjUploadCtl = adjustmentUpload.getAdjustmentStagingControlID().getId().getIdValue().toString();
		String adjUploadId = adjustmentUploadId.getIdValue().toString();
		String contractId = fetchContractId(adjUploadCtl, adjUploadId,inboundMerchantInterfaceLookUp);
		
		if(notBlank(contractId)){
			ServiceAgreement_Id contId = new ServiceAgreement_Id(contractId);
			contract = contId.getEntity();
			
			try {
				StringBuilder sb = new StringBuilder();
				sb.append(" UPDATE CM_ADJ_STG SET BO_STATUS_CD = :status, STATUS_UPD_DTTM = SYSTIMESTAMP");
				sb.append(" WHERE ADJ_STG_UP_ID = :adjUploadId AND ADJ_STG_CTL_ID = :adjUploadCtl");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString("status",  inboundMerchantInterfaceLookUp.getCompleted().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("adjUploadId", adjUploadId, "ADJ_STG_UP_ID");
				preparedStatement.bindString("adjUploadCtl", adjUploadCtl, "ADJ_STG_CTL_ID");
				preparedStatement.executeUpdate();
			} catch(Exception e) {
				logger.error("Exception in invoke()", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}else {

			try {
				StringBuilder sb = new StringBuilder();
				sb.append(" UPDATE CM_ADJ_STG SET BO_STATUS_CD = :status, STATUS_UPD_DTTM = SYSTIMESTAMP,");
				sb.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo");
				sb.append(" WHERE ADJ_STG_UP_ID = :adjUploadId AND ADJ_STG_CTL_ID = :adjUploadCtl");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString("status",  inboundMerchantInterfaceLookUp.getError().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("msgCatNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.MESSAGE_CATEGORY)), "MESSAGE_CAT_NBR");
				preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.ADJ_CONTRACT_NOT_FOUND)), "MESSAGE_NBR");
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.ADJ_CONTRACT_NOT_FOUND), "ERROR_INFO");
				preparedStatement.bindString("adjUploadId", adjUploadId, "ADJ_STG_UP_ID");
				preparedStatement.bindString("adjUploadCtl", adjUploadCtl, "ADJ_STG_CTL_ID");
				preparedStatement.executeUpdate();
			} catch(Exception e) {
				logger.error("Exception in invoke()", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();

				}	
			}

			//Calling addError
			addError(CustomMessageRepository.ADJContractNotFound());
		}
		
		
		// Set the isInSuspense hard parameter with the value from the adjustment upload
		if(adjustmentUpload.getAdjustmentSuspense().isInSuspense()) {
			isInSuspense = Bool.TRUE;
		}
		else {
			isInSuspense = Bool.FALSE;
		}
		
	}
	
	/**
	 * getErrorDescription() method selects error message description from ORMB message catalog.
	 * 
	 * @return errorInfo
	 */
	private static String getErrorDescription(int messageNumber) {
		String errorInfo;
		errorInfo = CustomMessageRepository.merchantError(CommonUtils.CheckNull(String.valueOf(messageNumber))).getMessageText();
		
		if (errorInfo.contains("Text:")
				&& errorInfo.contains("Description:")) {
			errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
					errorInfo.indexOf("Description:"));
		}
		return errorInfo;
	}
	//NAP-39326
	/**
	 * @param adjCatCd
	 * @return Boolean
	 * This method will return TRUE if IGA hierarchy needs to be checked.
	 */
	private Bool isIGALookupReq(String adjCatCd) {
		PreparedStatement ps = null;
		StringBuilder queryStr = new StringBuilder();
		
		try {
			queryStr.append(" SELECT CHAR_VAL from CI_ADJ_TY_CHAR WHERE CHAR_TYPE_CD =:charType AND ADJ_TYPE_CD =:adjType AND CHAR_VAL=:yes");
			
			ps = createPreparedStatement(queryStr.toString(), "isIGALookupReq");
			ps.bindString("charType","CM_IGA", "CHAR_TYPE_CD");
			ps.bindString("adjType", adjCatCd, "ADJ_TYPE_CD");
			ps.bindString("yes", "Y", "CHAR_VAL");
			SQLResultRow result = ps.firstRow();
			if (notNull(result)) {
				return Bool.TRUE;
				}
	
		} catch (Exception e) {
			logger.error("Exception occurred in isIGALookupReq() ", e);
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
				queryStr = null;
			}
		}
		return Bool.FALSE;
	}
	private String fetchContractId(String adjUploadCtl, String adjUploadId, InboundMerchantInterfaceLookUp inboundMerchantInterfaceLookUp) {
		PreparedStatement preparedStatement = null;
		SQLResultRow row;
		String contractId = "";
		String acctId = "";
		String acctNbr = "";
		String cisDivision = "";
		String saTypeCd = "";
		String acctType = "";
		String adjCatCd = "";
		StringBuilder sb;
		String contId = null;
		try{
			sb = new StringBuilder();
			sb.append(" SELECT ACCT_NBR, CIS_DIVISION, SA_TYPE_CD, ADJ_CAT_CD ");
			sb.append(" FROM CM_ADJ_STG WHERE ADJ_STG_UP_ID = :adjUploadId AND ADJ_STG_CTL_ID = :adjUploadCtl");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindString("adjUploadId", adjUploadId, "ADJ_STG_UP_ID");
			preparedStatement.bindString("adjUploadCtl", adjUploadCtl, "ADJ_STG_CTL_ID");
			row = preparedStatement.firstRow();
			if(notNull(row)) {
				acctNbr = row.getString("ACCT_NBR");
				cisDivision = row.getString("CIS_DIVISION");
				saTypeCd = row.getString("SA_TYPE_CD");
				adjCatCd = row.getString("ADJ_CAT_CD");
			}
			
			row = null;
			preparedStatement = null;	
			sb = new StringBuilder();
			sb.append(" SELECT SA.SA_ID FROM CI_SA SA, CI_ACCT_NBR ACCTNBR, CI_ACCT ACCT");
			sb.append(" WHERE SA.ACCT_ID = ACCTNBR.ACCT_ID AND ACCTNBR.ACCT_ID = ACCT.ACCT_ID AND ACCT.CIS_DIVISION = :cisDivision");
			sb.append(" AND ACCTNBR.ACCT_NBR = :acctNbr AND (SA.END_DT IS NULL OR SA.END_DT >= SYSDATE) AND SA.SA_TYPE_CD = :saTypeCd");
			sb.append(" AND NOT EXISTS (SELECT 'X' FROM CI_SA_CHAR CH WHERE CH.SA_ID = SA.SA_ID AND CH.CHAR_TYPE_CD = :charTypeCd) AND SA.SA_STATUS_FLG IN ('20','30')");
			preparedStatement = createPreparedStatement(sb.toString(),"");
			preparedStatement.bindString("acctNbr", acctNbr, "ACCT_NBR");
			preparedStatement.bindString("cisDivision", cisDivision, "CIS_DIVISION");
			preparedStatement.bindString("saTypeCd", saTypeCd, "SA_TYPE_CD");
			preparedStatement.bindString("charTypeCd", getCharTypeCd(), "CHAR_TYPE_CD");
			row = preparedStatement.firstRow();
			if(notNull(row)) {				
				contractId = row.getString("SA_ID");
				//NAP-39326
				if(isIGALookupReq(adjCatCd).isTrue()) {
					contId = fetchCompanyContractId(contractId);
					contractId = notNull(contId)?contId:contractId;
				}
								
			}else{
				row = null;
				preparedStatement = null;	
				sb = new StringBuilder();
				sb.append(" SELECT ACTNBR.ACCT_ID, ACTNBR.ACCT_NBR FROM CI_ACCT_NBR ACCTNBR, CI_ACCT ACCT, CI_ACCT_NBR ACTNBR");
				sb.append(" WHERE ACCTNBR.ACCT_ID = ACCT.ACCT_ID AND ACCT.CIS_DIVISION = :cisDivision");
				sb.append(" AND ACCTNBR.ACCT_NBR = :acctNbr");
				sb.append(" AND ACCT.ACCT_ID=ACTNBR.ACCT_ID AND ACTNBR.ACCT_NBR_TYPE_CD=:acctType");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString("acctNbr", acctNbr, "ACCT_NBR");
				preparedStatement.bindString("cisDivision", cisDivision, "CIS_DIVISION");
				preparedStatement.bindString("acctType", inboundMerchantInterfaceLookUp.getInternalAccountIdentifier().trim(), "ACCT_NBR_TYPE_CD");
				row = preparedStatement.firstRow();
				if(notNull(row)) {
					acctId = row.getString("ACCT_ID");
					acctType = row.getString("ACCT_NBR").trim();
				}
			}
			
			//Create contract if not already exists
			if(isBlankOrNull(contractId) && notBlank(saTypeCd) && !(acctType.equalsIgnoreCase(saTypeCd.trim())) && notBlank(acctId)){
				contractId = createContract(acctId, saTypeCd, cisDivision);	
			}
			
		} catch(Exception e) {
			logger.error("Exception in fetchContractId()", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				
			}	
		}
		return contractId;
	}
	
	//NAP-39326
	/**
	 * This method returns contract Id of Parent Account if IGA hierarchy is setup
	 * 
	 * @param contractId of outlet
	 * @return Contract Id of Company Account
	 */
	private String fetchCompanyContractId(String contractId) {
		String contId = null;
		PreparedStatement ps = null;
		StringBuilder queryStr = new StringBuilder();
		
		try {
			queryStr.append(" SELECT S.SA_ID FROM CI_SA_CHAR SC, CI_SA S WHERE SC.SRCH_CHAR_VAL =:contractId AND ")
			.append(" S.SA_ID =SC.SA_ID AND SC.CHAR_TYPE_CD =:refChar AND S.SA_STATUS_FLG IN (:active,:pendStop) ");
			
			ps = createPreparedStatement(queryStr.toString(), "fetchCompanyContractId");
			ps.bindString("contractId",contractId, "SRCH_CHAR_VAL");
			ps.bindString("refChar", getCharTypeCd(), "CHAR_TYPE_CD");
			ps.bindLookup("active", ServiceAgreementStatusLookup.constants.ACTIVE);
			ps.bindLookup("pendStop", ServiceAgreementStatusLookup.constants.PENDING_STOP);
			SQLResultRow result = ps.firstRow();
			if (notNull(result)) {
				contId = result.getString("SA_ID");
				}
		
			return contId;
		
		} catch (Exception e) {
			logger.error("Exception occurred in fetchCompanyContractId() ", e);
		} finally {
			if (ps != null) {
				ps.close();
				ps = null;
				queryStr = null;
			}
		}
		return contId;
				
	}

	/**
	 * createContract() method creates a new contract record .
	 * 
	 * @param aInboundMerchantInterfaceId
	 * @param perIdNbr
	 * @param contractTypeFlg
	 * @return
	 */
	private String createContract(String acctId, String contractTypeFlg,  String cisDivision) {
		
		String contractId = "";
		try {
						 			
			BusinessObjectInstance boInstance = BusinessObjectInstance.create("C1_SA");
			boInstance.set("division", cisDivision);
			boInstance.set("status", ServiceAgreementStatusLookup.constants.ACTIVE);
			boInstance.set("startDate", getSystemDateTime().getDate());
			boInstance.set("accountId", acctId);
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
			
		} catch (Exception e) {
			logger.error("Exception occurred in createContract()" , e);			
		} 
		return contractId;
	}
}
