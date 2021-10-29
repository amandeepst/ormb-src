package com.splwg.cm.domain.wp.ilm.algorithm;

import com.splwg.base.api.BusinessEntity;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject;
import com.splwg.base.domain.ilm.genericalgorithm.ILMEligibilitySpot;
import com.splwg.ccb.domain.banking.transactionFeed.transactionDetail.TransactionCalc;
import com.splwg.ccb.domain.banking.transactionFeed.transactionDetail.TransactionCalc_DTO;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Swapnil
 *
@AlgorithmComponent ()
 */
public class CmTransactionCalcILMAlgoComp_Impl extends
CmTransactionCalcILMAlgoComp_Gen implements ILMEligibilitySpot {

	private BusinessEntity entity;
	private MaintenanceObject mo;
	public static final Logger logger = LoggerFactory.getLogger(CmTransactionCalcILMAlgoComp_Impl.class);
	@Override
	public void invoke() {
		logger.debug("CmTransactionCalcILMAlgoComp_Impl :: invoke() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement preparedStatement;
		TransactionCalc_DTO calc_DTO = (TransactionCalc_DTO) entity.getDTO();
		TransactionCalc calc = calc_DTO.getEntity();
		if(notNull(calc.getIsEligibleForArchiving()) && calc.getIsEligibleForArchiving().isTrue()){

			try {

				sb.append("UPDATE CI_TXN_CALC_LN SET ILM_ARCH_SW = :ilmArchSw ");
				sb.append("WHERE TXN_CALC_ID = :txnCalcId");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindId("txnCalcId", calc.getId());
				preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
				if(notNull(preparedStatement)){
					preparedStatement.close();
					preparedStatement=null;
				}

				if(calc.getTransactionCalcLineChar().iterator().hasNext()){
					sb = new StringBuilder();
					sb.append("UPDATE CI_TXN_CALC_LN_CHAR SET ILM_ARCH_SW = :ilmArchSw ");
					sb.append("WHERE TXN_CALC_ID = :txnCalcId");
					preparedStatement = createPreparedStatement(sb.toString(),"");
					preparedStatement.bindId("txnCalcId", calc.getId());
					preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
					preparedStatement.setAutoclose(false);
					preparedStatement.executeUpdate();
					if(notNull(preparedStatement)){
						preparedStatement.close();
						preparedStatement=null;
					}                                                           
				}

				sb = new StringBuilder();
				sb.append("UPDATE CI_TXN_SQ SET ILM_ARCH_SW = :ilmArchSw ");
				sb.append("WHERE TXN_CALC_ID = :txnCalcId");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindId("txnCalcId", calc.getId());
				preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
				if(notNull(preparedStatement)){
					preparedStatement.close();
					preparedStatement=null;
				}
			}
			catch (ApplicationError e) {
				logger.error("Error while updating ILM_ARCH_SW ",e);
				addError(CustomMessageRepository.exceptionInExecution(e.getServerMessage().getMessageText()));
			}

		}
	}

	@Override
	public void setBusinessEntity(BusinessEntity paramBusinessEntity) {
		// TODO Auto-generated method stub
		entity = paramBusinessEntity;
	}

	@Override
	public void setMaintenanceObject(MaintenanceObject paramMaintenanceObject) {
		// TODO Auto-generated method stub
		mo = paramMaintenanceObject;
	}

	@Override
	public void setCutOffDate(Date paramDate) {
		// TODO Auto-generated method stub

	}

}
