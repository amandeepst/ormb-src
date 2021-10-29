/*******************************************************************************
 * FileName                   : InboundPaymentsLookUp.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Aug 17, 2015
 * Version Number             : 0.2
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Aug 17, 2015		Sunaina		  Implemented all the requirements for CD2.
0.2		 NA				Feb 20, 2017		Vienna		  Amendment for using adjustment to pay bill 
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
 * InboundPaymentsLookUp class retrieves all the lookup values related with Payment Confirmation Interface.
 * 
 * @author Sunaina
 *
 */

public class InboundPaymentsLookUp extends GenericBusinessObject {

	public static final Logger logger = LoggerFactory.getLogger(InboundPaymentsLookUp.class);

	public InboundPaymentsLookUp() {
		setLookUpConstants();
	}

	private String upload = "";
	private String error = "";
	private String pending = "";
	private String completed = "";


	private String rcptTenderType = "";
	private String payTenderType = "";

	private String matchType = "";
	private String dstRuleCode = "";
	private String glDivision = "";

	private String initPevtStatus = "";
	private String compPevtStatus = "";
	private String userId = "";
	private String accountType = "";
	private String payIdChar = "";

	private String fund = "";
	private String chrg = "";
	private String recur = "";
	private String chbk = "";
	private String overpayChrg = "";
	private String overpayChbk = "";
	private String overpayFund = "";
	
	private String tenderSource = "";
	private String bankAcctKey = "";
	private String bankId = "";
	

	private String settlementIdNbr="";

	/**
	 * setLookUpConstants retrieves the lookup values (constants for the program) and sets them.
	 *
	 */
	private void setLookUpConstants(){
		PreparedStatement preparedStatement = null;
		try{
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME FROM CI_LOOKUP_VAL WHERE FIELD_NAME =:fieldName ","");
			preparedStatement.bindString("fieldName", "IF330_CNF_OPT_TYP_FLG", "FIELD_NAME");

			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for(SQLResultRow resultSet : preparedStatement.list())
			{fieldValue = CommonUtils.CheckNull(resultSet.getString("FIELD_VALUE")).trim();
			valueName = CommonUtils.CheckNull(resultSet.getString("VALUE_NAME"));
			if (fieldValue.equals("UPLD")) {	//UPLD
				setUpload(valueName);
			} else if (fieldValue.equals("PEND")) {	//PENDING
				setPending(valueName);
			} else if (fieldValue.equals("EROR")) {	//ERROR
				setError(valueName);
			} else if (fieldValue.equals("COMP")) {	//COMPLETED
				setCompleted(valueName);
			} else if (fieldValue.equals("RCPT")) {	//RCPT
				setRcptTenderType(valueName);
			} else if (fieldValue.equals("PAYT")) {	//PAY
				setPayTenderType(valueName);
			} else if (fieldValue.equals("MTYP")) {	//BILL-ID
				setMatchType(valueName);
			} else if (fieldValue.equals("DSTR")) {	//SA_ID
				setDstRuleCode(valueName);
			} else if (fieldValue.equals("GDIV")) {	//WP
				setGlDivision(valueName);
			} else if (fieldValue.equals("INIT")) {	//10
				setInitPevtStatus(valueName);
			} else if (fieldValue.equals("COMS")) {	//40
				setCompPevtStatus(valueName);
			} else if (fieldValue.equals("USER")) {	//SYSUSER
				setUserId(valueName);
			} else if (fieldValue.equals("ACCT")) {	//ACCTTYPE
				setAccountType(valueName);
			} else if (fieldValue.equals("PAYD")) {	//PAYID
				setPayIdChar(valueName);
			} else if (fieldValue.equals("FUND")) {	//FUND
				setFund(valueName);
			} else if (fieldValue.equals("CHRG")) {	//CHRG
				setChrg(valueName);
			} else if (fieldValue.equals("RECR")) {	//RECUR
				setRecur(valueName);
			} else if (fieldValue.equals("CHBK")) {	//CHBK
				setChbk(valueName);
			} else if (fieldValue.equals("OPCR")) {	//MOVRPAYC
				setOverpayChrg(valueName);
			} else if (fieldValue.equals("OPCF")) {	//MOVRPAYF
				setOverpayFund(valueName);
			} else if (fieldValue.equals("OPCB")) {	//MOVRPYCB
				setOverpayChbk(valueName);

			}else if (fieldValue.equals("TNDR")) {	//ATND
				setTenderSource(valueName);
			}else if(fieldValue.equals("BKEY")){ //AB01
				setBankAcctKey(valueName);
			}else if(fieldValue.equals("BNID")){ //AB001
				setBankId(valueName);
			}else if(fieldValue.equals("SETD")){ //SETT_ID
				setSettlementIdNbr(valueName);
			}

			fieldValue = "";
			valueName = "";
			}
		} catch(Exception e) {
			logger.error("Exception in Inbound ayment Look Up " + e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getUpload() {
		return upload;
	}


	public void setUpload(String upload) {
		this.upload = upload;
	}


	public String getError() {
		return error;
	}

	/**
	 * @return the settlementIdNbr
	 */
	public String getSettlementIdNbr() {
		return settlementIdNbr;
	}

	/**
	 * @param settlementIdNbr the settlementIdNbr to set
	 */
	public void setSettlementIdNbr(String settlementIdNbr) {
		this.settlementIdNbr = settlementIdNbr;
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


	public String getRcptTenderType() {
		return rcptTenderType;
	}


	public void setRcptTenderType(String rcptTenderType) {
		this.rcptTenderType = rcptTenderType;
	}


	public String getPayTenderType() {
		return payTenderType;
	}


	public void setPayTenderType(String payTenderType) {
		this.payTenderType = payTenderType;
	}


	public String getMatchType() {
		return matchType;
	}


	public void setMatchType(String matchType) {
		this.matchType = matchType;
	}


	public String getDstRuleCode() {
		return dstRuleCode;
	}


	public void setDstRuleCode(String dstRuleCode) {
		this.dstRuleCode = dstRuleCode;
	}


	public String getGlDivision() {
		return glDivision;
	}


	public void setGlDivision(String glDivision) {
		this.glDivision = glDivision;
	}

	public String getInitPevtStatus() {
		return initPevtStatus;
	}

	public void setInitPevtStatus(String initPevtStatus) {
		this.initPevtStatus = initPevtStatus;
	}

	public String getCompPevtStatus() {
		return compPevtStatus;
	}

	public void setCompPevtStatus(String compPevtStatus) {
		this.compPevtStatus = compPevtStatus;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getAccountType() {
		return accountType;
	}

	public void setAccountType(String accountType) {
		this.accountType = accountType;
	}


	public String getPayIdChar() {
		return payIdChar;
	}

	public void setPayIdChar(String payIdChar) {
		this.payIdChar = payIdChar;
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
		return recur;
	}

	public void setRecur(String recur) {
		this.recur = recur;
	}

	public String getChbk() {
		return chbk;
	}

	public void setChbk(String chbk) {
		this.chbk = chbk;
	}

	public String getOverpayChrg() {
		return overpayChrg;
	}

	public void setOverpayChrg(String overpayChrg) {
		this.overpayChrg = overpayChrg;
	}

	public String getOverpayChbk() {
		return overpayChbk;
	}

	public void setOverpayChbk(String overpayChbk) {
		this.overpayChbk = overpayChbk;
	}

	public String getOverpayFund() {
		return overpayFund;
	}

	public void setOverpayFund(String overpayFund) {
		this.overpayFund = overpayFund;
	}

	public String getTenderSource() {
		return tenderSource;
	}
	
	public void setTenderSource(String tenderSource) {
		this.tenderSource = tenderSource;
	}
	
	public String getBankAcctKey() {
		return bankAcctKey;
	}
	
	public void setBankAcctKey(String bankAcctKey) {
		this.bankAcctKey = bankAcctKey;
	}
	
	public String getBankId() {
		return bankId;
	}
	
	public void setBankId(String bankId) {
		this.bankId = bankId;
	}

}
