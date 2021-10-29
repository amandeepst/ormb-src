/*******************************************************************************
 * FileName                   : PriceTypeDataInterfaceLookUp.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 0.2
 * Revision History     :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             Mar 24, 2015         Preeti       Implemented all requirements for CD1.
 0.2      NA             May 02, 2015         Preeti       Implemented code review changes.
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

public class PriceTypeDataInterfaceLookUp extends GenericBusinessObject {
	public static final Logger logger = LoggerFactory.getLogger(PriceTypeDataInterfaceLookUp.class);
	
	public PriceTypeDataInterfaceLookUp() {
		setLookUpConstants();
	}

	private String successStatus = "";

	private String languageCode = "";
	
	private String bclTypeCode = "";
	
	private String charEntityCode = "";

	private String tempTable1 = "";
	
	private String tempTable11 = "";

	private String tempTable2 = "";

	private String tempTable3 = "";

	private String tempTable4 = "";

	private String tempTable5 = "";

	private String tempTable6 = "";
	
	private String initStatus = "";
	
	private String errorStatus = "";
	
	private String exportFlag = "";
			
	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */

	public void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL " +
			"WHERE FIELD_NAME =:fieldName","");
			preparedStatement.bindString("fieldName", "INT291_OPT_TYPE_FLG", "FIELD_NAME");
			
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME")).trim();

				if (fieldValue.equals("BCLT")) {
					setBclTypeCode(valueName);
				} else if (fieldValue.equals("CENT")) {
					setCharEntityCode(valueName);				
				} else if (fieldValue.equals("SUCC")) {
					setSuccessStatus(valueName);				
				} else if (fieldValue.equals("LANG")) {
					setLanguageCode(valueName);
				} else if (fieldValue.equals("TMP1")) {
					setTempTable1(valueName);
				} else if (fieldValue.equals("TMP2")) {
					setTempTable2(valueName);
				} else if (fieldValue.equals("TMP3")) {
					setTempTable3(valueName);
				} else if (fieldValue.equals("TMP4")) {
					setTempTable4(valueName);
				} else if (fieldValue.equals("TMP5")) {
					setTempTable5(valueName);
				} else if (fieldValue.equals("TMP6")) {
					setTempTable6(valueName);
				} else if (fieldValue.equals("TMP7")) {
					setTempTable11(valueName);
				} else if (fieldValue.equals("INIT")) {
					setInitStatus(valueName);
				} else if (fieldValue.equals("ERRR")) {
					setErrorStatus(valueName);
				} else if (fieldValue.equals("EXPR")) {
					setExportFlag(valueName);
				}
			}
		} catch (Exception e) {
			logger.error("Exception in setLookUpConstants "+e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getSuccessStatus() {
		return successStatus;
	}

	public void setSuccessStatus(String successStatus) {
		this.successStatus = successStatus;
	}
		
	public String getErrorStatus() {
		return errorStatus;
	}

	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}

	public String getExportFlag() {
		return exportFlag;
	}

	public void setExportFlag(String exportFlag) {
		this.exportFlag = exportFlag;
	}

	public String getInitStatus() {
		return initStatus;
	}

	public void setInitStatus(String initStatus) {
		this.initStatus = initStatus;
	}

	public String getLanguageCode() {
		return languageCode;
	}

	public void setLanguageCode(String languageCode) {
		this.languageCode = languageCode;
	}
	
	public String getTempTable1() {
		return tempTable1;
	}

	public void setTempTable1(String tempTable1) {
		this.tempTable1 = tempTable1;
	}
	public String getTempTable11() {
		return tempTable11;
	}

	public void setTempTable11(String tempTable11) {
		this.tempTable11 = tempTable11;
	}

	public String getTempTable2() {
		return tempTable2;
	}

	public void setTempTable2(String tempTable2) {
		this.tempTable2 = tempTable2;
	}

	public String getTempTable3() {
		return tempTable3;
	}

	public void setTempTable3(String tempTable3) {
		this.tempTable3 = tempTable3;
	}

	public String getTempTable4() {
		return tempTable4;
	}

	public void setTempTable4(String tempTable4) {
		this.tempTable4 = tempTable4;
	}

	public String getTempTable5() {
		return tempTable5;
	}

	public void setTempTable5(String tempTable5) {
		this.tempTable5 = tempTable5;
	}

	public String getTempTable6() {
		return tempTable6;
	}

	public void setTempTable6(String tempTable6) {
		this.tempTable6 = tempTable6;
	}
	
	public String getBclTypeCode() {
		return bclTypeCode;
	}

	public void setBclTypeCode(String bclTypeCode) {
		this.bclTypeCode = bclTypeCode;
	}
	
	public String getCharEntityCode() {
		return charEntityCode;
	}

	public void setCharEntityCode(String charEntityCode) {
		this.charEntityCode = charEntityCode;
	}	
}