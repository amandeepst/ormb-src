/*******************************************************************************
* FileName                   : PaymentsRequestInterfaceLookUp.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Mar 24, 2015 
* Version Number             : 0.1
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 24, 2015        Preeti Tiwari     Implemented all requirements for CD1. 
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

public class PaymentsRequestInterfaceLookUp extends GenericBusinessObject {
	
	public static final Logger logger = LoggerFactory.getLogger(PaymentsRequestInterfaceLookUp.class);
	
	public PaymentsRequestInterfaceLookUp() {
		setLookUpConstants();
	}
	private String cmPayReqTmp1 = "";
	
	private String cmPayReqTmp2 = "";
	
	private String cmPayReqTmp3 = "";
	
	private String cmPayReqMain = "";
	
    private String cmPayReqErr = "";
	
	private String reqTypeStd = "";
	
	private String reqTypeRev = "";
	
	private String acctNbrTypeCd = "";
	
	private String indChbkFlgTypeCd = "";
	
	private String initialStatus = "";
	
	private String completeStatus = "";
	
	private String errorStatus = "";
	
	private String successStatus = "";
	
	private String yesStatus = "";
	
	private String acctTypeChbk = "";
	
	private String acctTypeFund = "";
	
	private String acctTypeCrwd = "";
	
	private String acctTypeChrg = "";
	
	private String contractTypeReserve = "";
	
	private String msgCat = "";
	
	private String extPartyId = "";
	
	private String ftTypeFlgAd = "";
	
	private String ftTypeFlgAx = "";
	
	private String ftTypeFlgBs = "";
	
	private String ftTypeFlgBx = "";
	
	private String pchRfdCharTypeCd = "";
	
	private String purchase = "";
	
	private String refund = "";
	
	private String saId = "";
	
	private String tenderTypeCdRcpt = "";
	
	private String tenderTypeCdPay = "";
	
	public void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			// Retrieving Fields
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL"
					+ " WHERE FIELD_NAME = 'INT15_OPT_TYPE_FLG' ","");
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				fieldValue = resultSet.getString("FIELD_VALUE");
				valueName = resultSet.getString("VALUE_NAME");
				// **********************************Error
				// Messages******************************************
				if (fieldValue.equals("PTT1")) {
					setCmPayReqTmp1(valueName);
				} else if (fieldValue.equals("PTT2")) {
					setCmPayReqTmp2(valueName);
				} else if (fieldValue.equals("PTT3")) {
					setCmPayReqTmp3(valueName);				
				} else if (fieldValue.equals("PMTN")) {
					setCmPayReqMain(valueName);
				} else if (fieldValue.equals("PTER")) {
					setCmPayReqErr(valueName);
				} else if (fieldValue.equals("RSTD")) {
					setReqTypeStd(valueName);
				} else if (fieldValue.equals("RREV")) {
					setReqTypeRev(valueName);
				} else if (fieldValue.equals("ACNT")) {
					setAcctNbrTypeCd(valueName);
				}else if (fieldValue.equals("INDF")) {
					setIndChbkFlgTypeCd(valueName);
				}else if (fieldValue.equals("INSI")) {
					setInitialStatus(valueName);
				}else if (fieldValue.equals("COSC")) {
					setCompleteStatus(valueName);
				}else if (fieldValue.equals("ERRE")) {
					setErrorStatus(valueName);
				}else if (fieldValue.equals("SUCS")) {
					setSuccessStatus(valueName);
				}else if (fieldValue.equals("YESY")) {
					setYesStatus(valueName);
				}else if (fieldValue.equals("ATCB")) {
					setAcctTypeChbk(valueName);
				}else if (fieldValue.equals("ATFD")) {
					setAcctTypeFund(valueName);
				}else if (fieldValue.equals("ATCR")) {
					setAcctTypeCrwd(valueName);
				}else if (fieldValue.equals("ATCG")) {
					setAcctTypeChrg(valueName);
				}else if (fieldValue.equals("CTRS")) {
					setContractTypeReserve(valueName);
				}else if (fieldValue.equals("MSGC")) {
					setMsgCat(valueName);
				}else if (fieldValue.equals("EXPI")) {
					setExtPartyId(valueName);
				}else if (fieldValue.equals("FTAD")) {
					setFtTypeFlgAd(valueName);
				}else if (fieldValue.equals("FTAX")) {
					setFtTypeFlgAx(valueName);
				}else if (fieldValue.equals("FTBS")) {
					setFtTypeFlgBs(valueName);
				}else if (fieldValue.equals("FTBX")) {
					setFtTypeFlgBx(valueName);
				}else if (fieldValue.equals("PRCT")) {
					setPchRfdCharTypeCd(valueName);
				}else if (fieldValue.equals("PURC")) {
					setPurchase(valueName);
				}else if (fieldValue.equals("REFD")) {
					setRefund(valueName);
				}else if (fieldValue.equals("SAID")) {
					setSaId(valueName);
				}else if (fieldValue.equals("TDRT")) {
					setTenderTypeCdRcpt(valueName);
				}else if (fieldValue.equals("TDPY")) {
					setTenderTypeCdPay(valueName);
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

	public String getCmPayReqTmp1() {
		return cmPayReqTmp1;
	}

	public void setCmPayReqTmp1(String cmPayReqTmp1) {
		this.cmPayReqTmp1 = cmPayReqTmp1;
	}

	public String getCmPayReqTmp2() {
		return cmPayReqTmp2;
	}

	public void setCmPayReqTmp2(String cmPayReqTmp2) {
		this.cmPayReqTmp2 = cmPayReqTmp2;
	}

	public String getCmPayReqTmp3() {
		return cmPayReqTmp3;
	}

	public void setCmPayReqTmp3(String cmPayReqTmp3) {
		this.cmPayReqTmp3 = cmPayReqTmp3;
	}

	public String getCmPayReqMain() {
		return cmPayReqMain;
	}

	public void setCmPayReqMain(String cmPayReqMain) {
		this.cmPayReqMain = cmPayReqMain;
	}

	public String getCmPayReqErr() {
		return cmPayReqErr;
	}

	public void setCmPayReqErr(String cmPayReqErr) {
		this.cmPayReqErr = cmPayReqErr;
	}

	public String getReqTypeStd() {
		return reqTypeStd;
	}

	public void setReqTypeStd(String reqTypeStd) {
		this.reqTypeStd = reqTypeStd;
	}

	public String getReqTypeRev() {
		return reqTypeRev;
	}

	public void setReqTypeRev(String reqTypeRev) {
		this.reqTypeRev = reqTypeRev;
	}

	public String getAcctNbrTypeCd() {
		return acctNbrTypeCd;
	}

	public void setAcctNbrTypeCd(String acctNbrTypeCd) {
		this.acctNbrTypeCd = acctNbrTypeCd;
	}

	public String getIndChbkFlgTypeCd() {
		return indChbkFlgTypeCd;
	}

	public void setIndChbkFlgTypeCd(String indChbkFlgTypeCd) {
		this.indChbkFlgTypeCd = indChbkFlgTypeCd;
	}

	public String getInitialStatus() {
		return initialStatus;
	}

	public void setInitialStatus(String initialStatus) {
		this.initialStatus = initialStatus;
	}

	public String getCompleteStatus() {
		return completeStatus;
	}

	public void setCompleteStatus(String completeStatus) {
		this.completeStatus = completeStatus;
	}

	public String getErrorStatus() {
		return errorStatus;
	}

	public void setErrorStatus(String errorStatus) {
		this.errorStatus = errorStatus;
	}

	public String getSuccessStatus() {
		return successStatus;
	}

	public void setSuccessStatus(String successStatus) {
		this.successStatus = successStatus;
	}

	public String getYesStatus() {
		return yesStatus;
	}

	public void setYesStatus(String yesStatus) {
		this.yesStatus = yesStatus;
	}

	public String getAcctTypeChbk() {
		return acctTypeChbk;
	}

	public void setAcctTypeChbk(String acctTypeChbk) {
		this.acctTypeChbk = acctTypeChbk;
	}

	public String getAcctTypeFund() {
		return acctTypeFund;
	}

	public void setAcctTypeFund(String acctTypeFund) {
		this.acctTypeFund = acctTypeFund;
	}

	public String getAcctTypeCrwd() {
		return acctTypeCrwd;
	}

	public void setAcctTypeCrwd(String acctTypeCrwd) {
		this.acctTypeCrwd = acctTypeCrwd;
	}

	public String getAcctTypeChrg() {
		return acctTypeChrg;
	}

	public void setAcctTypeChrg(String acctTypeChrg) {
		this.acctTypeChrg = acctTypeChrg;
	}

	public String getContractTypeReserve() {
		return contractTypeReserve;
	}

	public void setContractTypeReserve(String contractTypeReserve) {
		this.contractTypeReserve = contractTypeReserve;
	}

	public String getMsgCat() {
		return msgCat;
	}

	public void setMsgCat(String msgCat) {
		this.msgCat = msgCat;
	}

	public String getExtPartyId() {
		return extPartyId;
	}

	public void setExtPartyId(String extPartyId) {
		this.extPartyId = extPartyId;
	}

	public String getFtTypeFlgAd() {
		return ftTypeFlgAd;
	}

	public void setFtTypeFlgAd(String ftTypeFlgAd) {
		this.ftTypeFlgAd = ftTypeFlgAd;
	}

	public String getFtTypeFlgAx() {
		return ftTypeFlgAx;
	}

	public void setFtTypeFlgAx(String ftTypeFlgAx) {
		this.ftTypeFlgAx = ftTypeFlgAx;
	}

	public String getFtTypeFlgBs() {
		return ftTypeFlgBs;
	}

	public void setFtTypeFlgBs(String ftTypeFlgBs) {
		this.ftTypeFlgBs = ftTypeFlgBs;
	}

	public String getFtTypeFlgBx() {
		return ftTypeFlgBx;
	}

	public void setFtTypeFlgBx(String ftTypeFlgBx) {
		this.ftTypeFlgBx = ftTypeFlgBx;
	}

	public String getPchRfdCharTypeCd() {
		return pchRfdCharTypeCd;
	}

	public void setPchRfdCharTypeCd(String pchRfdCharTypeCd) {
		this.pchRfdCharTypeCd = pchRfdCharTypeCd;
	}

	public String getPurchase() {
		return purchase;
	}

	public void setPurchase(String purchase) {
		this.purchase = purchase;
	}

	public String getRefund() {
		return refund;
	}

	public void setRefund(String refund) {
		this.refund = refund;
	}

	public String getSaId() {
		return saId;
	}

	public void setSaId(String saId) {
		this.saId = saId;
	}

	public String getTenderTypeCdRcpt() {
		return tenderTypeCdRcpt;
	}

	public void setTenderTypeCdRcpt(String tenderTypeCdRcpt) {
		this.tenderTypeCdRcpt = tenderTypeCdRcpt;
	}

	public String getTenderTypeCdPay() {
		return tenderTypeCdPay;
	}

	public void setTenderTypeCdPay(String tenderTypeCdPay) {
		this.tenderTypeCdPay = tenderTypeCdPay;
	}

}
