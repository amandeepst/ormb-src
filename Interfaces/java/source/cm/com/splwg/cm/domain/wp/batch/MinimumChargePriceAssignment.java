/*******************************************************************************
 * FileName                   : MinimumChargePriceAssignment.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jul 04, 2017
 * Version Number             : 0.2
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Jul 04, 2017         Ankur       Implemented all requirements for updating minimum charge pricing.
0.2      NA             May 01, 2018         Ankur       NAP-26526 & NAP-26537 changed to complete_dttm
0.3      NA             Sep 11, 2018         RIA         NAP-32008 - Check Pricing View if enabled.
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationInfo;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationOptionsCache;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfiguration_Id;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.util.CommonGenericMethod;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author jaina555
 *
@BatchJob (multiThreaded = false, rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = processingDate, type = string)
 *            , @BatchJobSoftParameter (name = billStatFlg, required = true, type = string)
 *            , @BatchJobSoftParameter (name = paTypeFlag, required = true, type = string)})
 */
public class MinimumChargePriceAssignment extends MinimumChargePriceAssignment_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(MinimumChargePriceAssignment.class);

	public JobWork getJobWork() {
		// TODO Auto-generated method stub
		logger.debug("Inside getJobWork");
		ThreadWorkUnit threadworkUnit = new ThreadWorkUnit();
		threadworkUnit.setPrimaryId(new BatchControl_Id(getParameters().getBatchControlId().getIdValue()));
		ArrayList<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList.add(threadworkUnit);

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	public Class<MinimumChargePriceAssignmentWorker> getThreadWorkerClass() {
		return MinimumChargePriceAssignmentWorker.class;
	}

	public static class MinimumChargePriceAssignmentWorker extends
			MinimumChargePriceAssignmentWorker_Gen {
		//Default constructor
		
		
		public MinimumChargePriceAssignmentWorker() {
				}

		

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0) throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork()");
			
			super.initializeThreadWork(arg0);
		}
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			// TODO Auto-generated method stub
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			
			String paTypeFlag=getParameters().getPaTypeFlag();
			String billStatFlg=getParameters().getBillStatFlg();
			
			 
			
			
			String processingDate=getParameters().getProcessingDate();
			
				
			String pricingViewEnabled = CommonGenericMethod.getFeatureConfigValue("C1_FM", "VPMV");
			
			if(billStatFlg.equals("P") && paTypeFlag.equals("RGLR"))
			{
				/**
				 * This query will update minimum charge pricing of a merchant to 'RGLR' which have un-billed adhoc billable charges whose start date is one day less than processing date
				 */
				try {
					
					stringBuilder.append("UPDATE CI_PRICEASGN SET PA_TYPE_FLAG=:paTypeFlag  WHERE PRICEITEM_CD='MINCHRGP                      ' AND OWNER_ID IN ( ");
					stringBuilder.append("SELECT  PARTY.PARTY_UID FROM CI_PARTY PARTY,CI_ACCT_PER ACCTPER,CI_SA SA,CI_BILL_CHG BCHG ");
					stringBuilder.append("WHERE PARTY.PARTY_ID=ACCTPER.PER_ID AND ACCTPER.ACCT_ID=SA.ACCT_ID AND SA.SA_ID = (NVL((SELECT SA.SA_ID FROM CI_SA_CHAR SC,CI_SA SA ");
					stringBuilder.append("WHERE SA.SA_ID=SC.SA_ID AND SC.CHAR_TYPE_CD=:saChar AND SC.SRCH_CHAR_VAL=BCHG.SA_ID AND SA.SA_STATUS_FLG in (:pendStop,:active)),BCHG.SA_ID)) AND ");
					stringBuilder.append("BCHG.ADHOC_BILL_SW='Y' AND NOT EXISTS(SELECT 1 FROM CI_BSEG_CALC CALC WHERE CALC.BILLABLE_CHG_ID=BCHG.BILLABLE_CHG_ID) ");
					if(isNull(processingDate))
					stringBuilder.append("AND BCHG.BILL_AFTER_DT<=:sysdate) ");
					else
					stringBuilder.append("AND BCHG.START_DT=TRUNC(TO_DATE('"+processingDate+"','DD-MON-YY')-1)) ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("paTypeFlag", paTypeFlag, "PA_TYPE_FLAG");
					preparedStatement.bindId("saChar", new CharacteristicType_Id("C1_SAFCD"));
					preparedStatement.bindLookup("pendStop",ServiceAgreementStatusLookup.constants.PENDING_STOP);
					preparedStatement.bindLookup("active",ServiceAgreementStatusLookup.constants.ACTIVE);
					if(processingDate==null)
						preparedStatement.bindDate("sysdate", getSystemDateTime().getDate());
					preparedStatement.executeUpdate();
					
					if(pricingViewEnabled.equals("true")){
						preparedStatement = null;
						stringBuilder = new StringBuilder();
						
						stringBuilder.append(" UPDATE CI_PRC_AGRD SET PA_TYPE_FLAG = :paTypeFlag WHERE INIT_PRICEITEM_CD = 'MINCHRGP                      ' AND ACCT_ID IN ( ");
						stringBuilder.append(" SELECT  ACCTPER.ACCT_ID FROM CI_PARTY PARTY,CI_ACCT_PER ACCTPER,CI_SA SA,CI_BILL_CHG BCHG ");
						stringBuilder.append(" WHERE PARTY.PARTY_ID=ACCTPER.PER_ID AND ACCTPER.ACCT_ID=SA.ACCT_ID AND SA.SA_ID=BCHG.SA_ID AND ");
						stringBuilder.append(" BCHG.ADHOC_BILL_SW='Y' AND NOT EXISTS (SELECT 1 FROM CI_BSEG_CALC CALC WHERE CALC.BILLABLE_CHG_ID=BCHG.BILLABLE_CHG_ID) ");
						if(isNull(processingDate))
							stringBuilder.append("AND BCHG.START_DT=:sysdate) ");
						else
							stringBuilder.append("AND BCHG.START_DT=TRUNC(TO_DATE('"+processingDate+"','DD-MON-YY')-1)) ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("paTypeFlag", paTypeFlag, "PA_TYPE_FLAG");
						if(processingDate==null)
							preparedStatement.bindDate("sysdate",getSystemDateTime().getDate());
						preparedStatement.executeUpdate();
					}
					
				} catch (ThreadAbortedException e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
					throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
				} catch (Exception e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
				}finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}

			}
			else if(billStatFlg.equals("C") && paTypeFlag.equals("POST"))
			{
				/**
				 * This query will update minimum charge pricing of a merchant to 'POST' if bill is in Completed state
				 */
				try {
					stringBuilder.append("UPDATE CI_PRICEASGN SET PA_TYPE_FLAG=:paTypeFlag  WHERE PRICEITEM_CD='MINCHRGP                      ' AND OWNER_ID IN ( ");
					stringBuilder.append("SELECT PARTY.PARTY_UID FROM CI_PARTY PARTY,CI_ACCT_PER ACCTPER,CI_BILL BILL ");
					stringBuilder.append("WHERE PARTY.PARTY_ID=ACCTPER.PER_ID AND ACCTPER.ACCT_ID=BILL.ACCT_ID AND BILL.BILL_STAT_FLG=:billStatFlg  ");
					if(isNull(processingDate))
					stringBuilder.append("AND TRUNC(BILL.COMPLETE_DTTM)=TRUNC(SYSDATE) AND BILL.ADHOC_BILL_SW='Y') ");
					else
					stringBuilder.append("AND TRUNC(BILL.BILL_DT)=TRUNC(TO_DATE('"+processingDate+"','DD-MON-YY')) AND BILL.ADHOC_BILL_SW='Y') ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("paTypeFlag", paTypeFlag, "PA_TYPE_FLAG");
					preparedStatement.bindString("billStatFlg", billStatFlg, "BILL_STAT_FLG");
					preparedStatement.executeUpdate();
					
					if(pricingViewEnabled.equals("true")){
						preparedStatement = null;
						stringBuilder = new StringBuilder();
						
						stringBuilder.append(" UPDATE CI_PRC_AGRD SET PA_TYPE_FLAG = :paTypeFlag WHERE INIT_PRICEITEM_CD = 'MINCHRGP                      ' AND ACCT_ID IN ( ");
						stringBuilder.append(" SELECT  ACCTPER.ACCT_ID FROM CI_PARTY PARTY,CI_ACCT_PER ACCTPER,CI_BILL BILL ");
						stringBuilder.append(" WHERE PARTY.PARTY_ID=ACCTPER.PER_ID AND ACCTPER.ACCT_ID=BILL.ACCT_ID AND BILL.BILL_STAT_FLG=:billStatFlg ");
						if(isNull(processingDate))
						stringBuilder.append("AND TRUNC(BILL.COMPLETE_DTTM)=TRUNC(SYSDATE) AND BILL.ADHOC_BILL_SW='Y') ");
						else
						stringBuilder.append("AND TRUNC(BILL.COMPLETE_DTTM)=TRUNC(TO_DATE('"+processingDate+"','DD-MON-YY')) AND BILL.ADHOC_BILL_SW='Y') ");
						
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("paTypeFlag", paTypeFlag, "PA_TYPE_FLAG");
						preparedStatement.bindString("billStatFlg", billStatFlg, "BILL_STAT_FLG");
						preparedStatement.executeUpdate();
					}
					
				} catch (ThreadAbortedException e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
					throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
				} catch (Exception e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
				}finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}

			}
			else
			{
				logger.info("Invalid bill status flag or Pa type flag");
				return false;
			}
			

			
			return true;
		}
		

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}

	}

}
