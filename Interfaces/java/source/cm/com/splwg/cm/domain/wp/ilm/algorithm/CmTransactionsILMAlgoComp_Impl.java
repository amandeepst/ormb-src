package com.splwg.cm.domain.wp.ilm.algorithm;

import com.splwg.base.api.BusinessEntity;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject;
import com.splwg.base.domain.ilm.genericalgorithm.ILMEligibilitySpot;
import com.splwg.ccb.domain.banking.transactionFeed.transactionDetail.TransactionDtl;
import com.splwg.ccb.domain.banking.transactionFeed.transactionDetail.TransactionDtl_DTO;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Swapnil
 *
@AlgorithmComponent ()
 */
public class CmTransactionsILMAlgoComp_Impl extends
CmTransactionsILMAlgoComp_Gen implements ILMEligibilitySpot {

	private BusinessEntity entity;
	private MaintenanceObject mo;
	public static final Logger logger = LoggerFactory.getLogger(CmTransactionsILMAlgoComp_Impl.class);
	@Override
	public void invoke() {
		logger.debug("CmTransactionsILMAlgoComp_Impl :: invoke() method :: START");
		StringBuilder sb = new StringBuilder();
		PreparedStatement preparedStatement;
		TransactionDtl_DTO detail_DTO = (TransactionDtl_DTO) entity.getDTO();
		TransactionDtl detail = detail_DTO.getEntity();
		if(notNull(detail.getIsEligibleForArchiving()) && detail.getIsEligibleForArchiving().isTrue()){

			try {
				sb.append("UPDATE CI_TXN_DTL_PRITM SET ILM_ARCH_SW=:ilmArchSw ");
				sb.append("WHERE TXN_DETAIL_ID =:txnDetailId");
				preparedStatement = createPreparedStatement(sb.toString(),"");
				preparedStatement.bindString("txnDetailId", detail.getId().getTrimmedValue(), "TXN_DETAIL_ID");
				preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
				if(notNull(preparedStatement)){
					preparedStatement.close();
					preparedStatement=null;
				}

				if(detail.getRollbackTransactionDetail().iterator().hasNext()){
					sb = new StringBuilder();
					sb.append("UPDATE CI_ROLLBACK_TXN_DETAIL SET ILM_ARCH_SW=:ilmArchSw ");
					sb.append("WHERE TXN_DETAIL_ID =:txnDetailId");
					preparedStatement = createPreparedStatement(sb.toString(),"");
					preparedStatement.bindString("txnDetailId", detail.getId().getTrimmedValue(), "TXN_DETAIL_ID");
					preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
					preparedStatement.setAutoclose(false);
					preparedStatement.executeUpdate();
					if(notNull(preparedStatement)){
						preparedStatement.close();
						preparedStatement=null;
					}
				}

				if(detail.getTransactionErrorDetail().iterator().hasNext()){
					sb = new StringBuilder();
					sb.append("UPDATE CI_TXN_DETAIL_EXCP SET ILM_ARCH_SW=:ilmArchSw ");
					sb.append("WHERE TXN_DETAIL_ID =:txnDetailId");
					preparedStatement = createPreparedStatement(sb.toString(),"");
					preparedStatement.bindString("txnDetailId", detail.getId().getTrimmedValue(), "TXN_DETAIL_ID");
					preparedStatement.bindString("ilmArchSw", "Y", "ILM_ARCH_SW");
					preparedStatement.setAutoclose(false);
					preparedStatement.executeUpdate();
					if(notNull(preparedStatement)){
						preparedStatement.close();
						preparedStatement=null;
					}

				}
			} catch (ApplicationError e) {
				logger.error("Exception occured while updating ILM_ARCH_SW",e);;
				addError(CustomMessageRepository.exceptionInExecution(e.getServerMessage().getMessageText()));
			}
		}
	}

	@Override
	public void setBusinessEntity(BusinessEntity arg0) {
		// TODO Auto-generated method stub
		entity = arg0;
	}

	@Override
	public void setCutOffDate(Date arg0) {
		// TODO Auto-generated method stub

	}

	@Override
	public void setMaintenanceObject(MaintenanceObject arg0) {
		// TODO Auto-generated method stub
		mo = arg0;
	}

}
