/*******************************************************************************
 * FileName                   : InvoiceDataInterfaceLookUp.java

 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Aug 17, 2015
 * Version Number             : 0.9
 * Revision History     :
 VerNum | ChangeReqNum | Date Modification | Author Name                 | Nature of Change
 0.1      NA             16-Aug-2013         Sunaina/Preeti/Gaurav       Implemented all requirement as per CD2.
 
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

public class InvoiceDataInterfaceLookUp extends GenericBusinessObject {

public static final Logger logger = LoggerFactory.getLogger(InvoiceDataInterfaceLookUp.class);
	
	public InvoiceDataInterfaceLookUp() {
		setLookUpConstants();
	}
	
	private String invDataErr = "";
	
	private String initStatus = "";
	
	private String errorStatus = "";
	
	private String updateError = "";

	private String adFt = "";

	private String axFt = "";

	private String bsFt = "";

	private String bxFt = "";
	
	private String taxAgencyCode = "";
	
	private String accountType = "";

	private String chrg = "";
	
	

	private String fund = "";
	
	private String chbk = "";
	
	private String crwd = "";

	private String billStatusFlag = "";

	private String externalPartyId = "";

	private String businessUnit = "";
	
	private String taxRegime = "";
	
	private String languageCode = "";
	
	private String bclTypeCharType = "";

	private String taxRateCharType = "";

	private String taxScope = "";
	
	private String compBoStatusCode = "";
	
	private String ownerTypeFlag = "";
	
	private String rateTpCharType = "";
	
	private String extractFlag = "";
	
	private String txnVol = "";
	
	private String overpayAdjCode = "";
	
	
	

	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */

	private void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL " +
			"WHERE FIELD_NAME =:fieldName ","");
			preparedStatement.bindString("fieldName", "INT005_OPT_TYPE_FLG", "FIELD_NAME");
			
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));

				if (fieldValue.equals("INVE")) {
					setInvDataErr(valueName);
				}else if (fieldValue.equals("INIT")) {
					setInitStatus(valueName);
				} else if (fieldValue.equals("ERRR")) {
					setErrorStatus(valueName);
				} else if (fieldValue.equals("UPDE")) {
					setUpdateError(valueName);
				}  else if (fieldValue.equals("ADFT")) {
					setAdFt(valueName);
				} else if (fieldValue.equals("AXFT")) {
					setAxFt(valueName);
				} else if (fieldValue.equals("BSFT")) {
					setBsFt(valueName);
				} else if (fieldValue.equals("BXFT")) {
					setBxFt(valueName);
				} else if (fieldValue.equals("TAXA")) {
					setTaxAgencyCode(valueName);
				} else if (fieldValue.equals("ACCT")) {
					setAccountType(valueName);
				} else if (fieldValue.equals("BLST")) {
					setBillStatusFlag(valueName);
				} else if (fieldValue.equals("EXID")) {
					setExternalPartyId(valueName);
				} else if (fieldValue.equals("WPBU")) {
					setBusinessUnit(valueName);
				} else if (fieldValue.equals("RGME")) {
					setTaxRegime(valueName);
				}  else if (fieldValue.equals("LANG")) {
					setLanguageCode(valueName);
				} else if (fieldValue.equals("BCLT")) {
					setBclTypeCharType(valueName);
				} else if (fieldValue.equals("TXRT")) {
					setTaxRateCharType(valueName);
				} else if (fieldValue.equals("TXSC")) {
					setTaxScope(valueName);
				}  else if (fieldValue.equals("COMP")) {
					setCompBoStatusCode(valueName);
				} else if (fieldValue.equals("RATE")) {
					setRateTpCharType(valueName);
				} else if (fieldValue.equals("PRTY")) {
					setOwnerTypeFlag(valueName);
				} else if (fieldValue.equals("EXPR")) {
					setExtractFlag(valueName);
				} else if (fieldValue.equals("TXNV")) {
					setTxnVol(valueName);
				} else if (fieldValue.equals("ANBR")) {
					setChrg(valueName);
				} else if (fieldValue.equals("CHBK")) {
					setChbk(valueName);
				} else if (fieldValue.equals("FUND")) {
					setFund(valueName);
				} else if (fieldValue.equals("CRWD")) {
					setCrwd(valueName);
				}else if (fieldValue.equals("OPAC")) {
					setOverpayAdjCode(valueName);
				}
			}
		} catch (Exception e) {
			logger.error("Excpetion in invoice Data Interface Look Up " + e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getAccountType() {
		return accountType;
	}

	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}

	public String getBclTypeCharType() {
		return bclTypeCharType;
	}

	public void setBclTypeCharType(String bclTypeCharType) {
		this.bclTypeCharType = bclTypeCharType;
	}

	public String getBillStatusFlag() {
		return billStatusFlag;
	}

	public void setBillStatusFlag(String billStatusFlag) {
		this.billStatusFlag = billStatusFlag;
	}
	public String getErrorStatus() {
		return errorStatus;
	}

	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}

	public String getExtractFlag() {
		return extractFlag;
	}

	public void setExtractFlag(String extractFlag) {
		this.extractFlag = extractFlag;
	}

	public String getExternalPartyId() {
		return externalPartyId;
	}

	public void setExternalPartyId(String externalPartyId) {
		this.externalPartyId = externalPartyId;
	}

	public String getInitStatus() {
		return initStatus;
	}

	public void setInitStatus(String initStatus) {
		this.initStatus = initStatus;
	}

	public String getInvDataErr() {
		return invDataErr;
	}

	public void setInvDataErr(String invDataErr) {
		this.invDataErr = invDataErr;
	}

	public String getLanguageCode() {
		return languageCode;
	}

	public void setLanguageCode(String languageCode) {
		this.languageCode = languageCode;
	}

	public String getTaxRateCharType() {
		return taxRateCharType;
	}

	public void setTaxRateCharType(String taxRateCharType) {
		this.taxRateCharType = taxRateCharType;
	}

	public String getUpdateError() {
		return updateError;
	}

	public void setUpdateError(String updateError) {
		this.updateError = updateError;
	}

	public String getTaxAgencyCode() {
		return taxAgencyCode;
	}

	public void setTaxAgencyCode(String taxAgencyCode) {
		this.taxAgencyCode = taxAgencyCode;
	}

	public String getAdFt() {
		return adFt;
	}

	public void setAdFt(String adFt) {
		this.adFt = adFt;
	}

	public String getAxFt() {
		return axFt;
	}

	public void setAxFt(String axFt) {
		this.axFt = axFt;
	}

	public String getBsFt() {
		return bsFt;
	}

	public void setBsFt(String bsFt) {
		this.bsFt = bsFt;
	}

	public String getBxFt() {
		return bxFt;
	}

	public void setBxFt(String bxFt) {
		this.bxFt = bxFt;
	}

	public String getRateTpCharType() {
		return rateTpCharType;
	}

	public void setRateTpCharType(String rateTpCharType) {
		this.rateTpCharType = rateTpCharType;
	}
	
	public String getBusinessUnit() {
		return businessUnit;
	}

	public void setBusinessUnit(String businessUnit) {
		this.businessUnit = businessUnit;
	}
	
	public String getTaxRegime() {
		return taxRegime;
	}

	public void setTaxRegime(String taxRegime) {
		this.taxRegime = taxRegime;
	}

	public String getTaxScope() {
		return taxScope;
	}

	public void setTaxScope(String taxScope) {
		this.taxScope = taxScope;
	}


	public String getCompBoStatusCode() {
		return compBoStatusCode;
	}

	public void setCompBoStatusCode(String compBoStatusCode) {
		this.compBoStatusCode = compBoStatusCode;
	}

	public String getOwnerTypeFlag() {
		return ownerTypeFlag;
	}

	public void setOwnerTypeFlag(String ownerTypeFlag) {
		this.ownerTypeFlag = ownerTypeFlag;
	}

	public String getTxnVol() {
		return txnVol;
	}

	public void setTxnVol(String txnVol) {
		this.txnVol = txnVol;
	}
	
	public String getChrg() {
		return chrg;
	}

	public void setChrg(String chrg) {
		this.chrg = chrg;
	}

	public String getFund() {
		return fund;
	}

	public void setFund(String fund) {
		this.fund = fund;
	}

	public String getChbk() {
		return chbk;
	}

	public void setChbk(String chbk) {
		this.chbk = chbk;
	}

	public String getCrwd() {
		return crwd;
	}

	public void setCrwd(String crwd) {
		this.crwd = crwd;
	}
	
	public String getOverpayAdjCode() {
		return overpayAdjCode;
	}
	public void setOverpayAdjCode(String overpayAdjCode){
		this.overpayAdjCode = overpayAdjCode;
	}

}
