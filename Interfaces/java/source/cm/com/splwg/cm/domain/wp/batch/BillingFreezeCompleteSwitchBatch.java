/*******************************************************************************
 * FileName                   : BillingFreezeCompleteSwitchBatch.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jan 12, 2018
 * Version Number             : 0.1
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name       | Nature of Change
0.1		 NA				Jan 12, 2018		Vienna Rom		  	Initial version 
 *******************************************************************************/
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
import com.splwg.ccb.domain.admin.billCycle.BillCycle;
import com.splwg.ccb.domain.admin.billCycle.BillCycle_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author vrom
 *
@BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billCycCd, type = string)
 *            , @BatchJobSoftParameter (name = firstRunSW, required = true, type = string)})
 */
public class BillingFreezeCompleteSwitchBatch extends
		BillingFreezeCompleteSwitchBatch_Gen {

	public static final Logger logger = LoggerFactory.getLogger(BillingFreezeCompleteSwitchBatch.class);
	
	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		List<ThreadWorkUnit> threadWorkUnitList = getBillCycleData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}
	
	private List<ThreadWorkUnit> getBillCycleData() {
		PreparedStatement preparedStatement = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		StringBuilder stringBuilder = new StringBuilder();
		String firstRunSwitch = getParameters().getFirstRunSW();
		String billCycCd = getParameters().getBillCycCd();
		//Retrieve all bill cycle codes with bill cycle schedules having Freeze and Complete switch set to N 
		//and Accounting Date on or before the processing date
		try {
			stringBuilder.append("SELECT BC.BILL_CYC_CD FROM CI_BILL_CYC BC WHERE EXISTS ( " );
			stringBuilder.append("SELECT 1 FROM CI_BILL_CYC_SCH BCS " );
			stringBuilder.append("WHERE BC.BILL_CYC_CD=BCS.BILL_CYC_CD " );
			if (billCycCd != null){
				stringBuilder.append("AND BC.BILL_CYC_CD=:billCycCd " );
			}
			stringBuilder.append("AND :processDate BETWEEN BCS.WIN_START_DT AND BCS.WIN_END_DT ) " );
			
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
			if (billCycCd != null){
				preparedStatement.bindString("billCycCd", billCycCd, "BILL_CYC_CD");
			}
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(resultSet.getId("BILL_CYC_CD", BillCycle.class));
				threadworkUnit.addSupplementalData("firstRunSwitch", firstRunSwitch);
				threadWorkUnitList.add(threadworkUnit);
			}
		} catch (Exception e) {
			logger.error("Exception in getBillCycleData()", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		return threadWorkUnitList;
	}

	public Class<BillingFreezeCompleteSwitchBatchWorker> getThreadWorkerClass() {
		return BillingFreezeCompleteSwitchBatchWorker.class;
	}

	public static class BillingFreezeCompleteSwitchBatchWorker extends
			BillingFreezeCompleteSwitchBatchWorker_Gen {
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork method");
			
		}

		/**
		 * Main threadWorkUnit processing
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			BillCycle_Id billCycleCd = (BillCycle_Id)unit.getPrimaryId();
			String freezeAndComplteSw = (String) unit.getSupplementallData("firstRunSwitch");
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			
			if (freezeAndComplteSw.equals("Y")){

			try {
				stringBuilder.append("UPDATE CI_BILL_CYC_SCH SET FREEZE_COMPLETE_SW='Y' " );
				stringBuilder.append("WHERE BILL_CYC_CD=:billCycleCd " );
				stringBuilder.append("AND FREEZE_COMPLETE_SW='N' " );
				stringBuilder.append("AND :processDate BETWEEN WIN_START_DT AND WIN_END_DT " );
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindId("billCycleCd", billCycleCd);
				preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
				preparedStatement.executeUpdate();
				
			} catch (Exception e) {
				logger.error("Error updating CI_BILL_CYC_SCH ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
					}
				}
			
			}
			else if (freezeAndComplteSw.equals("N")){
				try {
					stringBuilder.append("UPDATE CI_BILL_CYC_SCH SET FREEZE_COMPLETE_SW='N' " );
					stringBuilder.append("WHERE BILL_CYC_CD=:billCycleCd " );
					stringBuilder.append("AND FREEZE_COMPLETE_SW='Y' " );
					stringBuilder.append("AND WIN_END_DT > :processDate " );
					
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindId("billCycleCd", billCycleCd);
					preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
					preparedStatement.executeUpdate();
					
				} catch (Exception e) {
					logger.error("Error updating CI_BILL_CYC_SCH ", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
				
			}
			return true;
		}

	}

}
