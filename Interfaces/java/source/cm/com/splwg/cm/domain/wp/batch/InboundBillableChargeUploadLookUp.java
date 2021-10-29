/*******************************************************************************
 * FileName                   : InboundBillableChargeUploadlookUp.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Feb 26, 2015
 * Version Number             : 0.1
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             11-Feb-2015         Sunaina       Implemented all requirement as in tax Determination Algorithm TDD v0.06.
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
 * InboundBillableChargeUploadlookUp class retrieves all the lookup values related with Inbound Billable Charge upload Program.
 * 
 * @author Sunaina
 *
 */

public class InboundBillableChargeUploadLookUp extends GenericBusinessObject {
	
	public static final Logger logger = LoggerFactory.getLogger(InboundBillableChargeUploadLookUp.class);

	public InboundBillableChargeUploadLookUp() {
		setLookUpConstants();
	}

	private String upldStatus = "";
	private String errorStatus = "";
	private String compStatus = "";
	private String pendStatus = "";

	private String userId = "";
	private String fileName = "";
	private String billTypeSwitch = "";
	private String accessGrpCd = "";
	private String boCode = "";
	private String boStatusCode = "";
	private String fileType = "";
	private String descr = "";
	private String offCycleCharTypeCode = "";
	private String donotRerateCharTypeCode = "";
	
	private String idTypeCode = "";
	private String accountIdentifier = "";
	private String staticReserve = "";
	private String dynamicReserve = "";
	private String fund = "";
	private String chrg = "";
	private String recur = "";
	private String waf = "";
	private String dstId = "";
	private String bclTypeCharTypeCode = "";
	private String mscPi = "";
	private int sequenceNumber = 0;
	

	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */
	private void setLookUpConstants(){
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL WHERE FIELD_NAME =:fieldName ", "");
			preparedStatement.bindString("fieldName", "INT010_OPT_TYP_FLG", "FIELD_NAME");

			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));
				if (fieldValue.equals("COMP")) {	// "COMPLETED"
					setCompStatus(valueName);
				} else if (fieldValue.equals("UPLD")) {	//"UPLD"
					setUpldStatus(valueName);
				} else if (fieldValue.equals("PEND")) {	//"PENDING"
					setPendStatus(valueName);
				} else if (fieldValue.equals("EROR")) {	//"ERROR"
					setErrorStatus(valueName);
				} else if (fieldValue.equals("USER")) { //"SYSUSER"
					setUserId(valueName);
				} else if (fieldValue.equals("FLNM")) { //"ODB"
					setFileName(valueName);
				} else if (fieldValue.equals("BLTS")) { //"ADB"
					setBillTypeSwitch(valueName);
				} else if (fieldValue.equals("AGRP")) { //"***"
					setAccessGrpCd(valueName);
				} else if (fieldValue.equals("BOCD")) { //"C1-ODB-BCH"
					setBoCode(valueName);
				} else if (fieldValue.equals("BSCD")) { //"Uploaded"
					setBoStatusCode(valueName);
				} else if (fieldValue.equals("FLTY")) {//"CSV"
					setFileType(valueName);
				} else if (fieldValue.equals("DESC")) {//"Billable Charge Upload"
					setDescr(valueName);
				} else if (fieldValue.equals("CTCD")) {//"OFFCYCBL"
					setOffCycleCharTypeCode(valueName);
				} else if (fieldValue.equals("CVAL")) {//"Y"
					setCharVal(valueName);
				} else if (fieldValue.equals("DNTR")) {//"DNTRERT"
					setDonotRerateCharTypeCode(valueName);
				} else if (fieldValue.equals("IDTY")) { //"EXPRTYID"
					setIdTypeCode(valueName);
				} else if (fieldValue.equals("ACID")) {	//"ACCTTYPE"
					setAccountIdentifier(valueName);
				} else if (fieldValue.equals("RSRD")) {	//"RSRV_D"
					setDynamicReserve(valueName);
				} else if (fieldValue.equals("RSRS")) {	//"RSRV_S"
					setStaticReserve(valueName);
				} else if (fieldValue.equals("FUND")) {	//"FUND"
					setFund(valueName);
				} else if (fieldValue.equals("CHRG")) {	//"CHRG"
					setChrg(valueName);
				} else if (fieldValue.equals("RECU")) {	//"RECUR"
					setRecur(valueName);
				} else if (fieldValue.equals("SWAF")) {	//"WH_FUND"
					setWaf(valueName);
				} else if (fieldValue.equals("DSID")) {//"BASE_CHG"
					setDstId(valueName);
				} else if (fieldValue.equals("BCLT")) {//"BCL-TYPE"
					setBclTypeCharTypeCode(valueName);
				} else if (fieldValue.equals("MSCP")) {//"MSC_PI"
					setMscPi(valueName);
 				} else if (fieldValue.equals("SEQN")) {
					setSequenceNumber(Integer.valueOf(valueName));
				}
				fieldValue = "";
				valueName = "";
			}
		} catch(Exception e) {
			logger.error("error in file "+e.getMessage());
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getOffCycleCharTypeCode() {
		return offCycleCharTypeCode;
	}


	public void setOffCycleCharTypeCode(String offCycleCharTypeCode) {
		this.offCycleCharTypeCode = offCycleCharTypeCode;
	}


	public String getDonotRerateCharTypeCode() {
		return donotRerateCharTypeCode;
	}


	public void setDonotRerateCharTypeCode(String donotRerateCharTypeCode) {
		this.donotRerateCharTypeCode = donotRerateCharTypeCode;
	}


	public String getCharVal() {
		return charVal;
	}


	public void setCharVal(String charVal) {
		this.charVal = charVal;
	}

	private String charVal = "";

	public String getDescr() {
		return descr;
	}


	public void setDescr(String descr) {
		this.descr = descr;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getFileName() {
		return fileName;
	}

	public void setFileName(String fileName) {
		this.fileName = fileName;
	}

	public String getBillTypeSwitch() {
		return billTypeSwitch;
	}

	public void setBillTypeSwitch(String billTypeSwitch) {
		this.billTypeSwitch = billTypeSwitch;
	}

	public String getAccessGrpCd() {
		return accessGrpCd;
	}

	public void setAccessGrpCd(String accessGrpCd) {
		this.accessGrpCd = accessGrpCd;
	}

	public String getBoCode() {
		return boCode;
	}

	public void setBoCode(String boCode) {
		this.boCode = boCode;
	}

	public String getBoStatusCode() {
		return boStatusCode;
	}

	public void setBoStatusCode(String boStatusCode) {
		this.boStatusCode = boStatusCode;
	}

	public String getFileType() {
		return fileType;
	}

	public void setFileType(String fileType) {
		this.fileType = fileType;
	}

	public String getCompStatus() {
		return compStatus;
	}


	public void setCompStatus(String compStatus) {
		this.compStatus = compStatus;
	}


	public String getErrorStatus() {
		return errorStatus;
	}


	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}


	public String getPendStatus() {
		return pendStatus;
	}


	public void setPendStatus(String pendStatus) {
		this.pendStatus = pendStatus;
	}


	public String getUpldStatus() {
		return upldStatus;
	}


	public void setUpldStatus(String upldStatus) {
		this.upldStatus = upldStatus;
	}

	public String getIdTypeCode() {
		return idTypeCode;
	}

	public void setIdTypeCode(String idTypeCode) {
		this.idTypeCode = idTypeCode;
	}

	public String getAccountIdentifier() {
		return accountIdentifier;
	}

	public void setAccountIdentifier(String accountIdentifier) {
		this.accountIdentifier = accountIdentifier;
	}

	public int getSequenceNumber() {
		return sequenceNumber;
	}

	public void setSequenceNumber(int sequenceNumber) {
		this.sequenceNumber = sequenceNumber;
	}
	
	public String getStaticReserve() {
		return staticReserve;
	}

	public void setStaticReserve(String staticReserve) {
		this.staticReserve = staticReserve;
	}

	public String getDynamicReserve() {
		return dynamicReserve;
	}

	public void setDynamicReserve(String dynamicReserve) {
		this.dynamicReserve = dynamicReserve;
	}

	public String getFund() {
		return fund;
	}

	public void setFund(String fund) {
		this.fund = fund;
	}

	public String getChrg() {
		return chrg;
	}

	public void setChrg(String chrg) {
		this.chrg = chrg;
	}

	public String getRecur() {
		return recur.trim();
	}

	public void setRecur(String recur) {
		this.recur = recur;
	}

	public String getWaf() {
		return waf;
	}

	public void setWaf(String waf) {
		this.waf = waf;
	}

	public String getDstId() {
		return dstId;
	}

	public void setDstId(String dstId) {
		this.dstId = dstId;
	}
	
	public String getBclTypeCharTypeCode() {
		return bclTypeCharTypeCode;
	}

	public void setBclTypeCharTypeCode(String bclTypeCharTypeCode) {
		this.bclTypeCharTypeCode = bclTypeCharTypeCode;
	}

	public String getMscPi() {
		return mscPi;
	}

	public void setMscPi(String mscPi) {
		this.mscPi = mscPi;
	}
	
}
