package com.splwg.cm.domain.wp.ilm.algorithm;

import com.splwg.base.api.BusinessEntity;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject;
import com.splwg.base.domain.ilm.genericalgorithm.ILMEligibilitySpot;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.cm.domain.wp.ilm.CmAdjustmentStaging;
import com.splwg.cm.domain.wp.ilm.CmAdjustmentStaging_DTO;
import com.splwg.cm.domain.wp.ilm.CmAdjustmentStaging_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author RIA
 *
@AlgorithmComponent ()
 */
public class CmAdjustmentStagingILMAlgoComp_Impl extends
CmAdjustmentStagingILMAlgoComp_Gen implements ILMEligibilitySpot {

	public static final Logger logger = LoggerFactory.getLogger(CmAdjustmentStagingILMAlgoComp_Gen.class);

	private BusinessEntity entity;

	@Override
	public void invoke() {
		logger.debug("CmAdjustmentStagingILMAlgoComp_Impl :: invoke() method :: START");

		boolean isEligibleForArchival = true;
		CmAdjustmentStaging_DTO cmAdjStgDto = (CmAdjustmentStaging_DTO) entity.getDTO();

		CmAdjustmentStaging_Id cmAdjStgId = cmAdjStgDto.getId();
		isEligibleForArchival = checkIfEligibleForArchival(cmAdjStgId);

		if(isEligibleForArchival) {
			CmAdjustmentStaging cmAcctStg = cmAdjStgDto.getEntity();
			cmAdjStgDto.setIsEligibleForArchiving(Bool.TRUE);
			cmAcctStg.setDTO(cmAdjStgDto);	
		}

		logger.debug("CmAdjustmentStagingILMAlgoComp_Impl :: invoke() method :: END");
	}

	/**
	 * Check Archival eligibility
	 * @param cmAdjStgId
	 * @return
	 */
	private boolean checkIfEligibleForArchival(CmAdjustmentStaging_Id cmAdjStgId) {
		boolean isEligible = true;
		StringBuilder strBuilder = new StringBuilder();
		PreparedStatement pstmt = null;

		try {
			strBuilder.append(" SELECT FT.BILL_ID ");
			strBuilder.append(" FROM CI_FT FT, CI_ADJ_STG_UP ADUP ");
			strBuilder.append(" WHERE FT.SIBLING_ID = ADUP.ADJ_ID ");
			strBuilder.append(" AND FT.FT_TYPE_FLG =:ftTypeFlag ");
			strBuilder.append(" AND ADUP.ADJ_STG_UP_ID =:cmAdjStgId ");

			pstmt = createPreparedStatement(strBuilder.toString(), "");
			pstmt.bindLookup("ftTypeFlag", FinancialTransactionTypeLookup.constants.ADJUSTMENT);
			pstmt.bindId("cmAdjStgId", cmAdjStgId);

			SQLResultRow result = pstmt.firstRow();
			if(notNull(result)) {
				String strBillId = result.get("BILL_ID").toString().trim();
				if(isBlankOrNull(strBillId)) {
					isEligible = false;	
				}
			}
		}finally {
			if (notNull(pstmt))
				pstmt.close();
		}
		return isEligible;
	}

	@Override
	public void setBusinessEntity(BusinessEntity arg0) {
		entity = arg0;
	}

	@Override
	public void setCutOffDate(Date arg0) {

	}

	@Override
	public void setMaintenanceObject(MaintenanceObject arg0) {

	}

}
