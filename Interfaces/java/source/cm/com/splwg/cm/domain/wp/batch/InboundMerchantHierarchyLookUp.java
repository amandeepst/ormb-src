/*******************************************************************************
* FileName                   : InboundMerchantHierarchyLookUp.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Mar 24, 2015
* Version Number             : 0.1
* Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Mar 24, 2015         Preeti       Implemented all requirements for CD1.
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.utils.CommonUtils;

/**
 * @author Preeti(IBM)
 *
 *This LookUp code will be used by Inbound Merchant Hierarchy Batch Interface to avoid hard codings.
 */
public class InboundMerchantHierarchyLookUp extends GenericBusinessObject {
	private String upload = "";

	private String error = "";

	private String pending = "";

	private String completed = "";

	private String errorMessageCategory = "";

	private String personIdCheckFailedError = "";
	
	private String personIdBothCheckFailedError = "";
	
	private String personIdParentCheckFailedError = "";
	
	private String personIdChildCheckFailedError = "";

	private String personHierFailedError = "";
	
	private String personHierCheckFailedError = "";
	
	private String sameHierCheckFailedError = "";
	
	private String diffDivisionCheckError = "";
	
	private String errorInfo = "";

	private String idTypeCd = "";
	
	private String mrchRelationshipType = "";
	
	private String stmtRelationshipType = "";
	
	private String childRelationshipType = "";
	
	private String messageForPersonIdCheck = "";
	
	private String messageForBothPersonIdCheck = "";
	
	private String messageForParentPersonIdCheck = "";
	
	private String messageForChildPersonIdCheck = "";
	
	private String messageForExistingHierCheck = "";
	
	private String messageForDuplicateHierCheck = "";
	
	private String messageForDivisionCheck = "";

	private String personBusinessObject = "";
	
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

	public String getErrorMessageCategory() {
		return errorMessageCategory;
	}

	public void setErrorMessageCategory(String errorMessageCategory) {
		this.errorMessageCategory = errorMessageCategory;
	}

	public String getPersonIdCheckFailedError() {
		return personIdCheckFailedError;
	}

	public void setPersonIdCheckFailedError(String personIdCheckFailedError) {
		this.personIdCheckFailedError = personIdCheckFailedError;
	}

	public String getPersonIdBothCheckFailedError() {
		return personIdBothCheckFailedError;
	}

	public void setPersonIdBothCheckFailedError(String personIdBothCheckFailedError) {
		this.personIdBothCheckFailedError = personIdBothCheckFailedError;
	}

	public String getPersonIdParentCheckFailedError() {
		return personIdParentCheckFailedError;
	}

	public void setPersonIdParentCheckFailedError(
			String personIdParentCheckFailedError) {
		this.personIdParentCheckFailedError = personIdParentCheckFailedError;
	}

	public String getPersonIdChildCheckFailedError() {
		return personIdChildCheckFailedError;
	}

	public void setPersonIdChildCheckFailedError(
			String personIdChildCheckFailedError) {
		this.personIdChildCheckFailedError = personIdChildCheckFailedError;
	}

	public String getPersonHierFailedError() {
		return personHierFailedError;
	}

	public void setPersonHierFailedError(String personHierFailedError) {
		this.personHierFailedError = personHierFailedError;
	}

	public String getPersonHierCheckFailedError() {
		return personHierCheckFailedError;
	}

	public void setPersonHierCheckFailedError(String personHierCheckFailedError) {
		this.personHierCheckFailedError = personHierCheckFailedError;
	}

	public String getSameHierCheckFailedError() {
		return sameHierCheckFailedError;
	}

	public void setSameHierCheckFailedError(String sameHierCheckFailedError) {
		this.sameHierCheckFailedError = sameHierCheckFailedError;
	}

	public String getDiffDivisionCheckError() {
		return diffDivisionCheckError;
	}

	public void setDiffDivisionCheckError(String diffDivisionCheckError) {
		this.diffDivisionCheckError = diffDivisionCheckError;
	}

	public String getErrorInfo() {
		return errorInfo;
	}

	public void setErrorInfo(String errorInfo) {
		this.errorInfo = errorInfo;
	}

	public String getIdTypeCd() {
		return idTypeCd;
	}

	public void setIdTypeCd(String idTypeCd) {
		this.idTypeCd = idTypeCd;
	}

	public String getMrchRelationshipType() {
		return mrchRelationshipType;
	}

	public void setMrchRelationshipType(String mrchRelationshipType) {
		this.mrchRelationshipType = mrchRelationshipType;
	}

	public String getStmtRelationshipType() {
		return stmtRelationshipType;
	}

	public void setStmtRelationshipType(String stmtRelationshipType) {
		this.stmtRelationshipType = stmtRelationshipType;
	}

	public String getChildRelationshipType() {
		return childRelationshipType;
	}

	public void setChildRelationshipType(String childRelationshipType) {
		this.childRelationshipType = childRelationshipType;
	}

	public String getMessageForPersonIdCheck() {
		return messageForPersonIdCheck;
	}

	public void setMessageForPersonIdCheck(String messageForPersonIdCheck) {
		this.messageForPersonIdCheck = messageForPersonIdCheck;
	}

	public String getMessageForBothPersonIdCheck() {
		return messageForBothPersonIdCheck;
	}

	public void setMessageForBothPersonIdCheck(String messageForBothPersonIdCheck) {
		this.messageForBothPersonIdCheck = messageForBothPersonIdCheck;
	}

	public String getMessageForParentPersonIdCheck() {
		return messageForParentPersonIdCheck;
	}

	public void setMessageForParentPersonIdCheck(
			String messageForParentPersonIdCheck) {
		this.messageForParentPersonIdCheck = messageForParentPersonIdCheck;
	}

	public String getMessageForChildPersonIdCheck() {
		return messageForChildPersonIdCheck;
	}

	public void setMessageForChildPersonIdCheck(String messageForChildPersonIdCheck) {
		this.messageForChildPersonIdCheck = messageForChildPersonIdCheck;
	}

	public String getMessageForExistingHierCheck() {
		return messageForExistingHierCheck;
	}

	public void setMessageForExistingHierCheck(String messageForExistingHierCheck) {
		this.messageForExistingHierCheck = messageForExistingHierCheck;
	}

	public String getMessageForDuplicateHierCheck() {
		return messageForDuplicateHierCheck;
	}

	public void setMessageForDuplicateHierCheck(String messageForDuplicateHierCheck) {
		this.messageForDuplicateHierCheck = messageForDuplicateHierCheck;
	}

	public String getMessageForDivisionCheck() {
		return messageForDivisionCheck;
	}

	public void setMessageForDivisionCheck(String messageForDivisionCheck) {
		this.messageForDivisionCheck = messageForDivisionCheck;
	}

	public String getPersonBusinessObject() {
		return personBusinessObject;
	}

	public void setPersonBusinessObject(String personBusinessObject) {
		this.personBusinessObject = personBusinessObject;
	}

	public void setLookUpConstants() {
		try {
			PreparedStatement preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME "
					+ "FROM CI_LOOKUP_VAL WHERE FIELD_NAME = 'INT034_OPT_TYPE_FLG' ","");
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE"));
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));

				switch (fieldValue) {
				case "PEND":
					setPending(valueName);
					break;
				case "UPLD":
					setUpload(valueName);
					break;
				case "EROR":
					setError(valueName);
					break;
				case "COMP":
					setCompleted(valueName);
					break;
				case "MCAT":
					setErrorMessageCategory(valueName);
					break;
				case "MSG1":
					setPersonIdCheckFailedError(valueName);
					break;
				case "MSG2":
					setPersonHierFailedError(valueName);
					break;
				case "MSG3":
					setPersonHierCheckFailedError(valueName);
					break;
				case "MSG4":
					setSameHierCheckFailedError(valueName);
					break;
				case "MSG5":
					setDiffDivisionCheckError(valueName);
					break;
				case "MSG7":
					setPersonIdBothCheckFailedError(valueName);
					break;
				case "MSG8":
					setPersonIdParentCheckFailedError(valueName);
					break;
				case "MSG9":
					setPersonIdChildCheckFailedError(valueName);
					break;
				case "INFO":
					setErrorInfo(valueName);
					break;
				case "IDTY":
					setIdTypeCd(valueName);
					break;
				case "MRCH":
					setMrchRelationshipType(valueName);
					break;
				case "STMT":
					setStmtRelationshipType(valueName);
					break;
				case "CHLD":
					setChildRelationshipType(valueName);
					break;
				case "MTD1":
					setMessageForPersonIdCheck(valueName);
					break;
				case "MTD2":
					setMessageForExistingHierCheck(valueName);
					break;
				case "MTD3":
					setMessageForDuplicateHierCheck(valueName);
					break;
				case "MTD4":
					setMessageForDivisionCheck(valueName);
					break;
				case "MTD6":
					setMessageForBothPersonIdCheck(valueName);
					break;
				case "MTD7":
					setMessageForParentPersonIdCheck(valueName);
					break;
				case "MTD8":
					setMessageForChildPersonIdCheck(valueName);
					break;
				case "PRBO":
					setPersonBusinessObject(valueName);
					break;
				}
				
							}
		} catch (Exception e) {
			System.out.println(e);
		}
	}

}
