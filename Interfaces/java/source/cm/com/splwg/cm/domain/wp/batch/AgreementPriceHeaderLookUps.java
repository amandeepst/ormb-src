/*******************************************************************************
 * FileName                   : AgreementPriceHeaderLookUps.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 0.3
 * Revision History     :
VerNum | ChangeReqNum | Date Modification|   Author Name        | Nature of Change
0.1      NA             Mar 24, 2015         Gaurav Sood    	  Implemented all the requirements for CD1.
0.2      NA             Sep 23, 2015         Preeti Tiwari    	  Implemented all the requirements for CD2.
0.3		 NA				Jul 31, 2018	 	 RIA			      NAP-30332 Rate schedule feature config setup
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

public class AgreementPriceHeaderLookUps extends GenericBusinessObject {
	public static final Logger logger = LoggerFactory.getLogger(AgreementPriceHeaderLookUps.class);
	
	private String upload="";
	private String error="";
	private String pending="";
	private String completed="";
	private String priceBO = "";
	private String messageCatNum = "";
	private String messageNum = "";
	private String party = "";
	private String tierRsCode = "";
	private String exprtyId = "";
	private String pers = "";
	private String rateType = "";
	private String plst = "";
	private String featureConfig = "";
	private String featureConfigLookup = "";

	public AgreementPriceHeaderLookUps() {
		setLookUpConstants();
	}

	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */
	private void setLookUpConstants(){
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL WHERE FIELD_NAME =:fieldName","");
			preparedStatement.bindString("fieldName", "INT002_OPT_TYPE_FLAG", "FIELD_NAME");

			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));
				if (fieldValue.equals("CMPL")) {
					setCompleted(valueName);
				} else if (fieldValue.equals("ERRR")) {
					setError(valueName);
				} else if (fieldValue.equals("MECN")) {
					setMessageCatNum(valueName);;
				} else if (fieldValue.equals("MENO")) {
					setMessageNum(valueName);;
				} else if (fieldValue.equals("PNDG")) {
					setPending(valueName);
				} else if (fieldValue.equals("PRBO")) {
					setPriceBO(valueName);
				} else if (fieldValue.equals("PRTY")) {
					setParty(valueName);
				} else if (fieldValue.equals("UPLD")) {
					setUpload(valueName);
				} else if (fieldValue.equals("TIER")) {
					setTierRsCode(valueName);
				} else if (fieldValue.equals("EXPR")) {
					setExprtyId(valueName);
				} else if (fieldValue.equals("PERS")) {
					setPers(valueName);
				} else if (fieldValue.equals("RTTP")) {
					setRateType(valueName);
				} else if (fieldValue.equals("PLST")) {
					setPlst(valueName);
				} else if (fieldValue.equals("FTCN")) {
					setFeatureConfig(valueName);
				} else if (fieldValue.equals("FCLK")) {
					setFeatureConfigLookup(valueName);
				}		

				fieldValue = "";
				valueName = "";
			}
		} catch(Exception e) {
			logger.error("error in lookup " +e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getTierRsCode() {
		return tierRsCode;
	}

	public void setTierRsCode(String tierRsCode) {
		this.tierRsCode = tierRsCode;
	}

	public String getCompleted() {
		return completed;
	}

	public void setCompleted(String completed) {
		this.completed = completed;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}
	public String getMessageCatNum() {
		return messageCatNum;
	}

	public void setMessageCatNum(String messageCatNum) {
		this.messageCatNum = messageCatNum;
	}

	public String getMessageNum() {
		return messageNum;
	}

	public void setMessageNum(String messageNum) {
		this.messageNum = messageNum;
	}

	public String getParty() {
		return party;
	}

	public void setParty(String party) {
		this.party = party;
	}

	public String getPending() {
		return pending;
	}

	public void setPending(String pending) {
		this.pending = pending;
	}

	public String getPriceBO() {
		return priceBO;
	}

	public void setPriceBO(String priceBO) {
		this.priceBO = priceBO;
	}

	public String getUpload() {
		return upload;
	}

	public void setUpload(String upload) {
		this.upload = upload;
	}   
	
	public String getExprtyId() {
		return exprtyId;
	}

	public void setExprtyId(String exprtyId) {
		this.exprtyId = exprtyId;
	}

	public String getPers() {
		return pers;
	}

	public void setPers(String pers) {
		this.pers = pers;
	}

	public String getRateType() {
		return rateType;
	}

	public void setRateType(String rateType) {
		this.rateType = rateType;
	}

	public String getPlst() {
		return plst;
	}

	public void setPlst(String plst) {
		this.plst = plst;
	}

	public String getFeatureConfig() {
		return featureConfig;
	}

	public void setFeatureConfig(String featureConfig) {
		this.featureConfig = featureConfig;
	}
	
	public String getFeatureConfigLookup() {
		return featureConfigLookup;
	}

	public void setFeatureConfigLookup(String featureConfigLookup) {
		this.featureConfigLookup = featureConfigLookup;
	}
	
}
