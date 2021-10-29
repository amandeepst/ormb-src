/*******************************************************************************
 * FileName                   : CmSkipPostProcessingBillingAlgComp_Impl.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jan 12, 2018
 * Version Number             : 0.2
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name       | Nature of Change
0.1		 NA				Jan 12, 2018		Vienna Rom		  	Initial version
0.2      NA             Mar 27, 2018        Vienna Rom    		Running Totals implementation 
 *******************************************************************************/
package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;

import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.ccb.domain.admin.serviceAgreementType.SaTypePostProcessingAlgorithmSpot;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author vrom
 *
@AlgorithmComponent ()
 */
public class CmSkipPostProcessingBillingAlgComp_Impl extends
		CmSkipPostProcessingBillingAlgComp_Gen implements
		SaTypePostProcessingAlgorithmSpot {

	private static final Logger logger = LoggerFactory.getLogger(CmSkipPostProcessingBillingAlgComp_Impl.class);
	
	//Hard parameters
	private String accountId;
	private Date accountingDate;
	private Bool allowEstSw;
	private Bool asgnSeqNbrSw;
	private BigInteger batchRunNo;
	private Bool billCompleteSw;
	private String billCycCode;
	private Date billDate;
	private String billGenType;
	private String billId;
	private Bool billOptSw;
	private String bsbsxAccountId;
	private String bsbsxBillCycCode;
	private String bsbsxBillId;
	private Date bsbsxEndDate;
	private String bsbsxSaId;
	private Date bsbsxStartDate;
	private String cisDivision;
	private Date cutOffDate;
	private Date endDate;
	private Date estDate;
	private Bool freezeCmpSw;
	private Bool midTrxCommitSw;
	private String saId;
	private String saTypeCode;
	private Bool skipSw;
	private Date startDate;
	private String trialBillId;
	private String updateSw;
	
	/**
	 * Main processing
	 */
	public void invoke() {

		//If regular bill that is not yet for bill completion, skip the rest of the post-processing (billing) algorithms
		if((notBlank(billCycCode) && freezeCmpSw.isFalse())) {
			skipSw = Bool.TRUE;
		}
		//Else, proceed with usual processing
		else {
			skipSw = Bool.FALSE;
		}
		
		logger.debug("***skipSw "+skipSw);

	}

	public String getAccountId() {
		return accountId;
	}

	public Date getAccountingDate() {
		return accountingDate;
	}

	public Bool getAllowEstSw() {
		return allowEstSw;
	}

	public Bool getAsgnSeqNbrSw() {
		return asgnSeqNbrSw;
	}

	public BigInteger getBatchRunNo() {
		return batchRunNo;
	}

	public Bool getBillCompleteSw() {
		return billCompleteSw;
	}

	public String getBillCycCode() {
		return billCycCode;
	}

	public Date getBillDate() {
		return billDate;
	}

	public String getBillGenType() {
		return billGenType;
	}

	public String getBillId() {
		return billId;
	}

	public Bool getBillOptSw() {
		return billOptSw;
	}

	public String getBsbsxAccountId() {
		return bsbsxAccountId;
	}

	public String getBsbsxBillCycCode() {
		return bsbsxBillCycCode;
	}

	public String getBsbsxBillId() {
		return bsbsxBillId;
	}

	public Date getBsbsxEndDate() {
		return bsbsxEndDate;
	}

	public String getBsbsxSaId() {
		return bsbsxSaId;
	}

	public Date getBsbsxStartDate() {
		return bsbsxStartDate;
	}

	public String getCisDivision() {
		return cisDivision;
	}

	public Date getCutOffDate() {
		return cutOffDate;
	}

	public Date getEndDate() {
		return endDate;
	}

	public Date getEstDate() {
		return estDate;
	}

	public Bool getFreezeCmpSw() {
		return freezeCmpSw;
	}

	public Bool getMidTrxCommitSw() {
		return midTrxCommitSw;
	}

	public String getSaId() {
		return saId;
	}

	public String getSaTypeCode() {
		return saTypeCode;
	}

	public Bool getSkipSw() {
		return skipSw;
	}

	public Date getStartDate() {
		return startDate;
	}

	public String getTrialBillId() {
		return trialBillId;
	}

	public String getUpdateSw() {
		return updateSw;
	}

	public void setAccountId(String arg0) {
		accountId = arg0;
	}

	public void setAccountingDate(Date arg0) {
		accountingDate = arg0;
	}

	public void setAllowEstSw(Bool arg0) {
		allowEstSw = arg0;
	}

	public void setAsgnSeqNbrSw(Bool arg0) {
		asgnSeqNbrSw = arg0;
	}

	public void setBatchRunNo(BigInteger arg0) {
		batchRunNo = arg0;
	}

	public void setBillCompleteSw(Bool arg0) {
		billCompleteSw = arg0;
	}

	public void setBillCycCode(String arg0) {
		billCycCode = arg0;
	}

	public void setBillDate(Date arg0) {
		billDate = arg0;
	}

	public void setBillGenType(String arg0) {
		billGenType = arg0;
	}

	public void setBillId(String arg0) {
		billId = arg0;
	}

	public void setBillOptSw(Bool arg0) {
		billOptSw = arg0;
	}

	public void setBsbsxAccountId(String arg0) {
		bsbsxAccountId = arg0;
	}

	public void setBsbsxBillCycCode(String arg0) {
		bsbsxBillCycCode = arg0;
	}

	public void setBsbsxBillId(String arg0) {
		bsbsxBillId = arg0;
	}

	public void setBsbsxEndDate(Date arg0) {
		bsbsxEndDate = arg0;
	}

	public void setBsbsxSaId(String arg0) {
		bsbsxSaId = arg0;
	}

	public void setBsbsxStartDate(Date arg0) {
		bsbsxStartDate = arg0;
	}

	public void setCisDivision(String arg0) {
		cisDivision = arg0;
	}

	public void setCutOffDate(Date arg0) {
		cutOffDate = arg0;
	}

	public void setEndDate(Date arg0) {
		endDate = arg0;
	}

	public void setEstDate(Date arg0) {
		estDate = arg0;
	}

	public void setFreezeCmpSw(Bool arg0) {
		freezeCmpSw = arg0;
	}

	public void setMidTrxCommitSw(Bool arg0) {
		midTrxCommitSw = arg0;
	}

	public void setSaId(String arg0) {
		saId = arg0;
	}

	public void setSaTypeCode(String arg0) {
		saTypeCode = arg0;
	}

	public void setSkipSw(Bool arg0) {
		skipSw = arg0;
	}

	public void setStartDate(Date arg0) {
		startDate = arg0;
	}

	public void setTrialBillId(String arg0) {
		trialBillId = arg0;
	}

	public void setUpdateSw(String arg0) {
		updateSw = arg0;
	}

}
