/*******************************************************************************
 * FileName                   : EventPriceDataInterfaceLookUp.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 0.1
 * Revision History     :
 VerNum | ChangeReqNum | Date Modification | Author Name  | Nature of Change
 0.1      NA             Mar 24, 2015         Preeti      	Implemented all requirements for CD1.
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

public class EventPriceDataInterfaceLookUp extends GenericBusinessObject {
	
	public static final Logger logger = LoggerFactory.getLogger(EventPriceDataInterfaceLookUp.class);
	
	public EventPriceDataInterfaceLookUp() {
		setLookUpConstants();
	}

	private String successStatus = "";
	
	private String billStatusFlag = "";
	
	private String eventTypeId= "";
	
	private String eventProcessId = "";

	private String languageCode = "";	
	
	private String tempTable1 = "";

	private String tempTable2 = "";

	private String tempTable3 = "";

	private String tempTable4 = "";

	private String tempTable5 = "";

	private String tempTable6 = "";
	
	private String tempTable7 = "";
	
	private String tempTable8= "";
	
	private String initStatus = "";
	
	private String errorStatus = "";
	
	private String compStatus = "";
	
	private String inpdStatus = "";
	
	private String exportFlag = "";
	
	private String tempTable9;

	private String tempTable10;

	private String tempTable11;

	private String externalPartyId;

	private String acctNbrTypeCode;

	private String bclTypeCode;
	
	private String evtPriceCode;

	private String charVal;
	
	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */

	private void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL " +
			"WHERE FIELD_NAME =:fieldName ","");
			preparedStatement.bindString("fieldName", "INTEP_OPT_TYPE_FLG", "FIELD_NAME");
			
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{
				fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
				valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME")).trim();

				if (fieldValue.equals("EXPI")) {
					setExternalPartyId(valueName.trim());
				} else if (fieldValue.equals("CHRV")) {
					setCharVal(valueName.trim());
				} else if (fieldValue.equals("BCLT")) {
					setBclTypeCode(valueName.trim());
				} else if (fieldValue.equals("ETPI")) {
					setEventTypeId(valueName.trim());
				} else if (fieldValue.equals("EPRI")) {
					setEventProcessId(valueName.trim());
				} else if (fieldValue.equals("BSFC")) {
					setBillStatusFlag(valueName.trim());
				} else if (fieldValue.equals("SUCC")) {
					setSuccessStatus(valueName.trim());				
				} else if (fieldValue.equals("LANG")) {
					setLanguageCode(valueName.trim());
				} else if (fieldValue.equals("TMP1")) {
					setTempTable1(valueName.trim());
				} else if (fieldValue.equals("TMP2")) {
					setTempTable2(valueName.trim());
				} else if (fieldValue.equals("TMP3")) {
					setTempTable3(valueName.trim());
				} else if (fieldValue.equals("TMP4")) {
					setTempTable4(valueName.trim());
				} else if (fieldValue.equals("TMP5")) {
					setTempTable5(valueName.trim());
				} else if (fieldValue.equals("TMP6")) {
					setTempTable6(valueName.trim());
				} else if (fieldValue.equals("TMP7")) {
					setTempTable7(valueName.trim());
				} else if (fieldValue.equals("TMP8")) {
					setTempTable8(valueName.trim());
				} else if (fieldValue.equals("TMP9")) {
					setTempTable9(valueName.trim());
				} else if (fieldValue.equals("TM10")) {
					setTempTable10(valueName.trim());
				} else if (fieldValue.equals("TM11")) {
					setTempTable11(valueName.trim());
				} else if (fieldValue.equals("INIT")) {
					setInitStatus(valueName.trim());
				} else if (fieldValue.equals("COMP")) {
					setCompStatus(valueName.trim());
				} else if (fieldValue.equals("INPD")) {
					setInpdStatus(valueName.trim());
				} else if (fieldValue.equals("ERRR")) {
					setErrorStatus(valueName.trim());
				} else if (fieldValue.equals("ACNT")) {
					setAcctNbrTypeCode(valueName.trim());
				} else if (fieldValue.equals("EXPR")) {
					setExportFlag(valueName.trim());
				} else if (fieldValue.equals("EVTP")) {
					setEvtPriceCode(valueName.trim());
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

	public String getAcctNbrTypeCode() {
		return acctNbrTypeCode;
	}

	public void setAcctNbrTypeCode(String acctNbrTypeCode) {
		this.acctNbrTypeCode = acctNbrTypeCode;
	}
	
	public String getEvtPriceCode() {
		return evtPriceCode;
	}

	public void setEvtPriceCode(String evtPriceCode) {
		this.evtPriceCode = evtPriceCode;
	}

	public String getBclTypeCode() {
		return bclTypeCode;
	}

	public void setBclTypeCode(String bclTypeCode) {
		this.bclTypeCode = bclTypeCode;
	}
	public String getCharVal() {
		return charVal;
	}

	public void setCharVal(String charVal) {
		this.charVal = charVal;
	}

	public String getEventTypeId() {
		return eventTypeId;
	}

	public void setEventTypeId(String eventTypeId) {
		this.eventTypeId = eventTypeId;
	}

	public String getEventProcessId() {
		return eventProcessId;
	}

	public void setEventProcessId(String eventProcessId) {
		this.eventProcessId = eventProcessId;
	}

	public String getBillStatusFlag() {
		return billStatusFlag;
	}

	public void setBillStatusFlag(String billStatusFlag) {
		this.billStatusFlag = billStatusFlag;
	}
	public String getExternalPartyId() {
		return externalPartyId;
	}

	public void setExternalPartyId(String externalPartyId) {
		this.externalPartyId = externalPartyId;
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

	public String getInpdStatus() {
		return inpdStatus;
	}

	public void setInpdStatus(String inpdStatus) {
		this.inpdStatus = inpdStatus;
	}

	public String getTempTable7() {
		return tempTable7;
	}

	public void setTempTable7(String tempTable7) {
		this.tempTable7 = tempTable7;
	}

	public String getTempTable8() {
		return tempTable8;
	}

	public void setTempTable8(String tempTable8) {
		this.tempTable8 = tempTable8;
	}

	public String getCompStatus() {
		return compStatus;
	}

	public void setCompStatus(String compStatus) {
		this.compStatus = compStatus;
	}
	public String getTempTable9() {
		return tempTable9;
	}

	public void setTempTable9(String tempTable9) {
		this.tempTable9 = tempTable9;
	}

	public String getTempTable10() {
		return tempTable10;
	}

	public void setTempTable10(String tempTable10) {
		this.tempTable10 = tempTable10;
	}

	public String getTempTable11() {
		return tempTable11;
	}

	public void setTempTable11(String tempTable11) {
		this.tempTable11 = tempTable11;
	}
}
