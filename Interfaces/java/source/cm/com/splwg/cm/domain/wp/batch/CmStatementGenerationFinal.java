/*******************************************************************************
 * FileName                   : CmStatementGenerationFinal.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 29, 2018 
 * Version Number             : 0.2
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             May 29, 2018        Ankur Jain           Implemented all the requirements for NAP-21261.
0.2      NA             Jul 21, 2018        Viklap Rangare       NAP-30770 - Changed PARTY_ID column to BILLING_PARTY_ID 
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import java.util.ArrayList;
import java.util.List;

/**
 * @author jaina555
 *
@BatchJob (modules = { "demo"})
 */
public class CmStatementGenerationFinal extends CmStatementGenerationFinal_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CmStatementGenerationFinal.class);
	public static final String FUND_AMT= "F_M_AMT         ";
	public static final String ILM_DT= "ILM_DT";
	public static final String STMT_ID= "STMT_ID";
	public static final String UNIQUE_TIMESTAMP = "uniqueTimeStamp";
	public static final String MAX_ILM_DT = "maxIlmDt";

	@Override
	public JobWork getJobWork() {
		logger.info("Inside getJobWork()");
		
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList=getStatementGenData();
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * getStatementGenData() method selects statement Ids for which Statements needs to be generated .
	 * 
	 * @return List ThreadWorkUnit
	 */
	
	private List<ThreadWorkUnit> getStatementGenData() {
		logger.info("Inside getStatementGenData() method");
		PreparedStatement preparedStatement = null;		
		String stmtId ="";
		DateTime uniqueTimeStamp = null;
		DateTime maxIlmDt = null;
		StringBuilder stringBuilder = new StringBuilder();
		Statement_Id statement_Id = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		maxIlmDt=getMaxIlmDt();
		try {
			/**
			 * Select unique statement Ids along with ILM_DT from temporary table
			 */
			stringBuilder.append("SELECT DISTINCT STMT_ID,ILM_DT FROM CM_STMT_GEN_HDR_TMP ");	
			preparedStatement = createPreparedStatement(stringBuilder.toString(),""); 
			preparedStatement.setAutoclose(false);
			
			 for (SQLResultRow sqlRow : preparedStatement.list()) {
				
				 stmtId = sqlRow.getString(STMT_ID);
				 uniqueTimeStamp = sqlRow.getDateTime(ILM_DT);
				 statement_Id = new Statement_Id(stmtId);
				//*************************
				 threadworkUnit = new ThreadWorkUnit();
				 threadworkUnit.setPrimaryId(statement_Id);
				 threadworkUnit.addSupplementalData(UNIQUE_TIMESTAMP, uniqueTimeStamp);
				 threadworkUnit.addSupplementalData(MAX_ILM_DT, maxIlmDt);
				 threadWorkUnitList.add(threadworkUnit);
				 threadworkUnit = null;
				 statement_Id = null;
				//*************************
				logger.info("Statement Ids added to Id class");
				
				}

		} catch (ThreadAbortedException e){
				logger.error("Inside getStatementGenData() method of CmStatementGenerationFinal, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Statement Data Outbound method, Error -", e);
			}
		finally{
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			}
		
		return threadWorkUnitList;
	}

	private DateTime getMaxIlmDt() {
		PreparedStatement preparedStatement1;
		DateTime maxIlmDt = null;
		Date sysdate = getSystemDateTime().getDate();
		preparedStatement1 = createPreparedStatement("SELECT max(ILM_DT) as ILM_DT FROM CM_STMT_GEN_HDR", "");
		List<SQLResultRow> rows = preparedStatement1.list();
		if (!rows.isEmpty()) {
			maxIlmDt = rows.get(0).getDateTime(ILM_DT) == null ? new DateTime(sysdate, new Time(0, 0, 0)) : rows.get(0).getDateTime(ILM_DT);
		}
		logger.info("The maximum Ilm date time is = " + maxIlmDt);
		if (preparedStatement1 != null) {
			preparedStatement1.close();
		}
		return maxIlmDt;
	}


	public Class<CmStatementGenerationFinalWorker> getThreadWorkerClass() {
		return CmStatementGenerationFinalWorker.class;
	}

	public static class CmStatementGenerationFinalWorker extends
			CmStatementGenerationFinalWorker_Gen {
		
		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.info("Inside initializeThreadWork() method for batch thread number: "+ getBatchThreadNumber());
			super.initializeThreadWork(arg0);
		}

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * extracting statements details from ORMB. It validates the
		 * extracted data and populates the target tables accordingly.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			Statement_Id statement_Id = (Statement_Id)unit.getPrimaryId();
			String stmtId = statement_Id.getStmtId();
			DateTime uniqueTimeStamp = (DateTime)unit.getSupplementallData(UNIQUE_TIMESTAMP);
			DateTime maxIlmDt = (DateTime)unit.getSupplementallData(MAX_ILM_DT);

			String newStmtId = "";
			PreparedStatement preparedStatement = null;	
			StringBuilder stringBuilder = new StringBuilder();
			
			/**
			 * Generate unique statement Id
			 */
			try {
					stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT CM_STMT_ID_SEQ.NEXTVAL as SEQ_NUM FROM DUAL ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.setAutoclose(false);
					SQLResultRow sQLResultRow = preparedStatement.firstRow();
					newStmtId = sQLResultRow.getInteger("SEQ_NUM").toString();
			} catch (ThreadAbortedException e){
				logger.error("Inside executeWorkUnit() method of CmStatementGenerationFinal, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Statement Generation Final, Error -", e);
			}
			finally{
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
			}
			
			
			/**
			 * Insert unique header data into CM_STMT_GEN_HDR table
			 */
			try {
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CM_STMT_GEN_HDR ");
					stringBuilder.append("(STMT_ID,STMT_CONFIG_ID,STMT_PARTY_ID,STMT_TYPE,CIS_DIVISION,"
							+ "CURRENCY_CD,PRV_STMT_ID,UPLOAD_DTTM,EXTRACT_FLG, ");
					stringBuilder.append("EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW)  ");
					stringBuilder.append("SELECT DISTINCT :newStmtId,STMT_CONFIG_ID,STMT_PARTY_ID,STMT_TYPE,"
							+ "CIS_DIVISION,CURRENCY_CD,PRV_STMT_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM, ");
					stringBuilder.append("ILM_DT,ILM_ARCH_SW FROM CM_STMT_GEN_HDR_TMP WHERE STMT_ID=:stmtId ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("newStmtId", newStmtId, STMT_ID);
					preparedStatement.bindString("stmtId", stmtId, STMT_ID);
					preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e){
				logger.error("Inside executeWorkUnit() method of CmStatementGenerationFinal, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Statement Generation Final, Error -", e);
			}
			finally{
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
			}
			
			
			/**
			 * Insert unique detail data into CM_STMT_GEN_DTL table
			 */
			try {
					stringBuilder = new StringBuilder();
					stringBuilder.append("INSERT INTO CM_STMT_GEN_DTL  ");
					stringBuilder.append("(STMT_DETAIL_ID,STMT_ID,PARTY_ID,AGGR_PARTY_ID,BILL_ID,UPLOAD_DTTM,"
							+ "EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
					stringBuilder.append("SELECT  MIN(STMT_DETAIL_ID),:newStmtId,BILLING_PARTY_ID,AGGR_PARTY_ID,"
							+ "BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW FROM CM_STMT_GEN_DTL_TMP ");
					stringBuilder.append("WHERE STMT_ID=:stmtId  ");
					stringBuilder.append("GROUP BY :newStmtId,BILLING_PARTY_ID,AGGR_PARTY_ID,BILL_ID,UPLOAD_DTTM,"
							+ "EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("newStmtId", newStmtId, STMT_ID);
					preparedStatement.bindString("stmtId", stmtId, STMT_ID);
					preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e){
				logger.error("Inside executeWorkUnit() method of CmStatementGenerationFinal, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Statement Generation Final, Error -", e);
			}
			finally{
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
			}
			
			/**
			 * Insert unique tax data into CM_STMT_TAX table
			 */
			try {
				StringBuilder stringBuilder1 = new StringBuilder();
				stringBuilder1.append("INSERT INTO CM_STMT_TAX ");
				stringBuilder1.append("(STMT_ID,CALC_AMT,BASE_AMT,TAX_STAT,TAX_STAT_DESCR,TAX_RATE,UPLOAD_DTTM, ");
				stringBuilder1.append("EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				stringBuilder1.append("SELECT DISTINCT STMT_ID,CALC_AMT,BASE_AMT,TAX_STAT,TAX_STAT_DESCR,TAX_RATE, ");
				stringBuilder1.append("UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW FROM ");
				stringBuilder1.append("(SELECT STMT_ID,SUM(BCL.CALC_AMT * BCL.TAX_RATE/100) AS CALC_AMT, ");
				stringBuilder1.append("SUM(BCL.CALC_AMT) AS BASE_AMT,BCL.TAX_STAT,BCL.TAX_STAT_DESCR,BCL.TAX_RATE, ");
				stringBuilder1.append("B.UPLOAD_DTTM,B.EXTRACT_FLG,B.EXTRACT_DTTM,B.ILM_DT,B.ILM_ARCH_SW FROM ");
				stringBuilder1.append("CM_STMT_GEN_DTL B,CM_INVOICE_DATA_LN C,CM_INV_DATA_LN_BCL BCL ");
				stringBuilder1.append("WHERE B.bill_id=C.bill_id and C.billing_party_id=B.party_id AND ");
				stringBuilder1.append("B.ILM_DT=:uniqueTimeStamp AND C.bill_id = BCL.bill_Id AND C.ILM_DT = BCL.ILM_DT ");
				stringBuilder1.append("AND BCL.bseg_id=C.bseg_id AND B.STMT_ID=:newStmtId  ");
				stringBuilder1.append("AND BCL.BCL_TYPE <> :bclType AND C.ILM_DT >=:maxIlmDt ");
				stringBuilder1.append("GROUP BY STMT_ID,BCL.TAX_STAT,BCL.TAX_STAT_DESCR,BCL.TAX_RATE,B.UPLOAD_DTTM, ");
				stringBuilder1.append("B.EXTRACT_FLG,B.EXTRACT_DTTM,B.ILM_DT,B.ILM_ARCH_SW) ");

				preparedStatement = createPreparedStatement(stringBuilder1.toString(),"");
				preparedStatement.bindString("newStmtId", newStmtId, STMT_ID);
				preparedStatement.bindString("bclType", FUND_AMT, "BCL_TYPE");
				preparedStatement.bindDateTime(UNIQUE_TIMESTAMP, uniqueTimeStamp);
				preparedStatement.bindDate(MAX_ILM_DT,maxIlmDt.toDate());

				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e){
				logger.error("Inside executeWorkUnit() method of CmStatementGenerationFinal, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Statement Generation Final, Error -", e);
			}
			finally{
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return true;
		}
		
		/**
		 * finalizeThreadWork() is execute by the batch program once per thread
		 * after processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.info("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}

	}

	public static final class Statement_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String stmtId;
		
		public Statement_Id(String stmtId) {
			setStmtId(stmtId);
			}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
			/*
			empty method
			 */
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}
		
		public String getStmtId() {
			return stmtId;
		}

		public void setStmtId(String stmtId) {
			this.stmtId = stmtId;
		}

	}
}
