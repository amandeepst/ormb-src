package com.splwg.cm.domain.wp.batch;


import java.util.ArrayList;
import java.util.List;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 *
@BatchJob (modules = { "demo"})
 */
public class CmMasterAccountHierarchy extends CmMasterAccountHierarchy_Gen {
	
	InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();
    private static final String CHAR_TYPE="CHAR_TYPE_CD";

	public static final Logger logger = LoggerFactory.getLogger(CmMasterAccountHierarchy.class);

	@Override
	public JobWork getJobWork() {
		inboundAccountHierarchyLookUps.setLookUpConstants();

		List<ThreadWorkUnit> threadWorkUnitList= getMasterAccountData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "+ threadWorkUnitList.size());
		inboundAccountHierarchyLookUps = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
		
	}

	private List<ThreadWorkUnit> getMasterAccountData() {
		PreparedStatement preparedStatement = null;
		Account_Id acctId = null;
		StringBuilder stringBuilder = new StringBuilder();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();

		try {
			stringBuilder.append("SELECT SA.ACCT_ID FROM CI_SA SA,CI_SA_CHAR SCHAR WHERE SA.SA_ID=SCHAR.SA_ID ");
			stringBuilder.append("AND SCHAR.CHAR_TYPE_CD=:saRefChar AND SA.SA_STATUS_FLG IN (:pendStop,:active) ");
			stringBuilder.append("AND EXISTS(SELECT 1 FROM CI_ACCT_CHAR WHERE ACCT_ID=SA.ACCT_ID "); 
			stringBuilder.append("AND CHAR_TYPE_CD=:igaChar AND CHAR_VAL=:status) ");
			stringBuilder.append("AND NOT EXISTS(SELECT 1 FROM CI_ACCT_NBR WHERE ACCT_ID=SA.ACCT_ID AND ACCT_NBR_TYPE_CD = :acctType) ");
			stringBuilder.append("GROUP BY SA.ACCT_ID ");	
			
			preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
			preparedStatement.bindString("saRefChar", inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(), CHAR_TYPE);
			preparedStatement.bindString("igaChar", inboundAccountHierarchyLookUps.getIgaCharacteristic(), CHAR_TYPE);
			preparedStatement.bindString("acctType", "INACTIVE", "ACCT_NBR_TYPE_CD");
			preparedStatement.bindLookup("pendStop", ServiceAgreementStatusLookup.constants.PENDING_STOP);
			preparedStatement.bindLookup("active", ServiceAgreementStatusLookup.constants.ACTIVE);
			preparedStatement.bindString("status", "N", "CHAR_VAL");
			preparedStatement.setAutoclose(false);
			
			for (SQLResultRow resultSet : preparedStatement.list()) {				
				acctId = new Account_Id(resultSet.getString("ACCT_ID"));
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(acctId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				acctId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getMasterAccountData() ", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		return threadWorkUnitList;
	}

	public Class<CmMasterAccountHierarchyWorker> getThreadWorkerClass() {
		return CmMasterAccountHierarchyWorker.class;
	}

	public static class CmMasterAccountHierarchyWorker extends
			CmMasterAccountHierarchyWorker_Gen {
		
		InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();

		
		@Override
		public void initializeThreadWork(
				boolean initializationPreviouslySuccessful)
				throws ThreadAbortedException, RunAbortedException {
			
			inboundAccountHierarchyLookUps.setLookUpConstants();
		}

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			Account_Id acctId= (Account_Id) unit.getPrimaryId();
			//******Updating Account Char*******
			if(acctId != null && acctId.getEntity() != null){
				updateAccountChar(acctId);
			}
			
			return true;
		}

		private void updateAccountChar(Account_Id acctId) {
			
			PreparedStatement ps=null;			
			try{
				StringBuilder sb=new StringBuilder().append("UPDATE CI_ACCT_CHAR SET CHAR_VAL=:charVal WHERE ACCT_ID=:acctID AND CHAR_TYPE_CD=:igaChar");
				ps=createPreparedStatement(sb.toString(),"");
				ps.bindString("charVal", "Y", "CHAR_VAL");
				ps.bindId("acctID", acctId);
				ps.bindString("igaChar", inboundAccountHierarchyLookUps.getIgaCharacteristic(), CHAR_TYPE);
				ps.executeUpdate();
			}
			catch(Exception e){
				logger.error("Error Record while Updating Account Characteristics for Account : "+acctId);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			}
			finally{
				if(ps != null){
					ps.close();
					ps=null;
				}
			}
		}	
	}

}
