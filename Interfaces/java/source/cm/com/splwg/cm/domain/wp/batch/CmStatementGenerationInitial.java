/*******************************************************************************
 * FileName                   : CmStatementGenerationInitial.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Apr 19, 2018 
 * Version Number             : 0.2
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Apr 19, 2018        Ankur Jain           Implemented all the requirements for NAP-21261. 
0.2      NA             Jul 21, 2018        Viklap Rangare       NAP-30770 - Changed PARTY_ID column to BILLING_PARTY_ID 
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
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author jaina555
 *
@BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = reRunDate, type = date)})
 */
public class CmStatementGenerationInitial extends CmStatementGenerationInitial_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CmStatementGenerationInitial.class);
	
	public JobWork getJobWork() {

		logger.info("Inside getJobWork()");
		
		// To truncate temporary tables
		truncateFromStatementTmpTable("CM_STMT_GEN_HDR_TMP");
		truncateFromStatementTmpTable("CM_STMT_GEN_DTL_TMP");
		
		List<ThreadWorkUnit> threadWorkUnitList = getStatementBillData();
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}
	
	/**
	 * truncateFromStatementTmpTable() method will truncate from the table provided as
	 * input.
	 * 
	 * @param inputStatementTmpTable
	 */
	@SuppressWarnings("deprecation")
	private void truncateFromStatementTmpTable(String inputStatementTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("TRUNCATE TABLE "+ inputStatementTmpTable);
			preparedStatement.execute();
			logger.info("Temp tables " +inputStatementTmpTable +"deleted");
			}
		catch (ThreadAbortedException e) {
			logger.error("Inside truncateFromStatementTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside truncateFromStatementTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	
	/**
	 * getStatementBillData() method selects Charging Bill Ids for which statements needs to be generated .
	 * 
	 * @return List ThreadWorkUnit
	 */
	
	private List<ThreadWorkUnit> getStatementBillData() {
		logger.info("Inside getStatementBillData() method");
		PreparedStatement preparedStatement = null;		

		Date sysdate = getSystemDateTime().getDate();
		DateTime maxIlmDt = new DateTime(sysdate, new Time(0, 0, 0));
		DateTime uniqueTimeStamp = getSystemDateTime(); 
		String billId ="";
		String cisDivision ="";
		String currencyCd ="";
		StringBuilder stringBuilder = new StringBuilder();
		StatementBillData_Id statementBillDataId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		Date reRunDate = getParameters().getReRunDate();
		
		maxIlmDt=getMaxIlmDt(reRunDate,sysdate);

		try {
			if (notNull(reRunDate)) {				
				stringBuilder.append("SELECT ")
							 .append(" /*+ ")				
							 .append(" FULL(@\"SEL$683B0107\" \"L\"@\"SEL$2\") ")
							 .append(" FULL(@\"SEL$683B0107\" \"A\"@\"SEL$2\") ")
							 .append(" FULL(@\"SEL$683B0107\" \"B\"@\"SEL$2\") ")
							 .append(" FULL(@\"SEL$C772B8D1\" \"D\"@\"SEL$1\") ")
							 .append(" */ ")	
							 .append(" D.BILL_ID, D.CIS_DIVISION, D.CURRENCY_CD FROM CM_INVOICE_DATA D WHERE D.ILM_DT>=:maxIlmDt AND ")
				 			 .append(" D.ILM_DT< :finalDt AND D.ACCT_TYPE=:chrg AND EXISTS (SELECT 1 FROM CM_STMT_CONF_DTL A,CM_STMT_CONF_HDR B, ")
				 			 .append(" CM_INVOICE_DATA_LN L WHERE D.BILL_ID = L.BILL_ID AND A.BILLING_PARTY_ID = L.BILLING_PARTY_ID ")
				 			 .append(" AND A.BO_STATUS_CD=:actv AND A.BO_STATUS_CD=B.BO_STATUS_CD AND A.STMT_CONFIG_ID=B.STMT_CONFIG_ID AND L.ILM_DT>=:maxIlmDt ")
				 			 .append(" AND (:maxIlmDt BETWEEN A.START_DT AND A.END_DT OR A.END_DT is null)) " );
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),""); 
				preparedStatement.bindDateTime("finalDt", maxIlmDt.addDays(1));				
			}
			else {
				stringBuilder.append("SELECT D.BILL_ID, D.CIS_DIVISION, D.CURRENCY_CD FROM CM_INVOICE_DATA D WHERE D.ILM_DT>:maxIlmDt AND ")
							 .append(" D.ACCT_TYPE=:chrg AND EXISTS (SELECT 1 FROM CM_STMT_CONF_DTL A,CM_STMT_CONF_HDR B, CM_INVOICE_DATA_LN L ")
							 .append(" WHERE D.BILL_ID = L.BILL_ID AND A.BILLING_PARTY_ID = L.BILLING_PARTY_ID ")
							 .append(" AND A.BO_STATUS_CD=:actv AND A.BO_STATUS_CD=B.BO_STATUS_CD AND A.STMT_CONFIG_ID=B.STMT_CONFIG_ID AND L.ILM_DT>=:maxIlmDt ")
							 .append(" AND (:maxIlmDt BETWEEN A.START_DT AND A.END_DT OR A.END_DT is null)) " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			}
					 
			preparedStatement.bindDateTime("maxIlmDt", maxIlmDt);
			preparedStatement.bindString("chrg", "CHRG", "ACCT_TYPE");
			preparedStatement.bindString("actv", "ACTV", "BO_STATUS_CD");			
			preparedStatement.setAutoclose(false);
			
			 for (SQLResultRow sqlRow : preparedStatement.list()) {
				
				 billId = sqlRow.getString("BILL_ID");
				 cisDivision = sqlRow.getString("CIS_DIVISION");
				 currencyCd = sqlRow.getString("CURRENCY_CD");
				 statementBillDataId = new StatementBillData_Id(billId, cisDivision,currencyCd);
				
				//*************************
				 threadworkUnit = new ThreadWorkUnit();
				 threadworkUnit.setPrimaryId(statementBillDataId);
				 threadworkUnit.addSupplementalData("uniqueTimeStamp", uniqueTimeStamp);
				 threadworkUnit.addSupplementalData("maxIlmDt", maxIlmDt);
				 threadWorkUnitList.add(threadworkUnit);
				 threadworkUnit = null;
				 statementBillDataId = null;
				//*************************
				logger.info("Statement Data Ids added to Id class");
				
				}

		} catch (ThreadAbortedException e){
				logger.error("Inside getStatementBillData() method of CmStatementGenerationInitial, Error -", e);
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

	

	private DateTime getMaxIlmDt(Date reRunDate, Date sysdate) {
		DateTime maxIlmDt = new DateTime(sysdate, new Time(0, 0, 0));
		PreparedStatement preparedStatement=null;
			
		try {
			if(notNull(reRunDate)) {
				maxIlmDt = new DateTime(reRunDate,new Time (0,0,0));
			}
			else {
				preparedStatement = createPreparedStatement("SELECT max(ILM_DT) as ILM_DT FROM CM_STMT_GEN_HDR","");
				List<SQLResultRow> rows = preparedStatement.list();
				if (!rows.isEmpty()) {
					
					maxIlmDt = rows.get(0).getDateTime("ILM_DT")==null?new DateTime(sysdate, new Time(0, 0, 0)):rows.get(0).getDateTime("ILM_DT");
				}
			}
			
			logger.info("The maximum Ilm date time is = " + maxIlmDt);
		} catch (RuntimeException e) {
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Calling gather stats, Error -", e);
		}
		finally
		{
		 if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		}
		return maxIlmDt;
	}

	public Class<CmStatementGenerationInitialWorker> getThreadWorkerClass() {
		return CmStatementGenerationInitialWorker.class;
	}

	public static class CmStatementGenerationInitialWorker extends
			CmStatementGenerationInitialWorker_Gen {
		
		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				{
			logger.info("Inside initializeThreadWork() method for batch thread number: "+ getBatchThreadNumber());
			super.initializeThreadWork(arg0);
		}
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			
			return new StandardCommitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * extracting statements details from ORMB. It validates the
		 * extracted data and populates the target tables accordingly.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit) {

			
			StatementBillData_Id statementBillDataId = (StatementBillData_Id)unit.getPrimaryId(); 
			String billId = statementBillDataId.getBillId();
			String cisDivision = statementBillDataId.getCisDivision();
			String currencyCd = statementBillDataId.getCurrencyCd();
			DateTime uniqueTimeStamp = (DateTime)unit.getSupplementallData("uniqueTimeStamp");
			DateTime maxIlmDt = (DateTime)unit.getSupplementallData("maxIlmDt");
			
			String parentBillId = "";
			String prevStmtId = "";
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			Boolean stmtRecalcFlag = Boolean.FALSE;
		
			/** Check if billId exists as child bill id or not */
			try {
					stringBuilder.append("SELECT PARENT_BILL_ID FROM CM_INV_RELATION_STG WHERE CHILD_BILL_ID= trim(:childBillID) AND "
							+ "RELATIONSHIP_TYPE='53'");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("childBillID", billId, "BILL_ID");
					preparedStatement.setAutoclose(false);
					SQLResultRow sqlRow=preparedStatement.firstRow();
					
					if(notNull(sqlRow))
					{
						parentBillId = sqlRow.getString("PARENT_BILL_ID");
						stmtRecalcFlag = Boolean.TRUE;
					}
						
				} catch (ThreadAbortedException e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
					throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
				} catch (Exception e) {
					logger.error("Inside executeWorkUnit() method, Error -", e);
				}
				finally
				{
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
				}
			}
				
			if(stmtRecalcFlag)
			{

				/** find previous statement Id*/
				prevStmtId=getPreviousStatementId(parentBillId);
			
				/** Populate temporary detail & header table*/
				insertStmtTemp(prevStmtId,uniqueTimeStamp);
 
			}
			else
			{
				//Normal Statements functionality
				/** find all the parties associated to current bill */
				getConfigDataForInserts(billId,cisDivision,currencyCd,uniqueTimeStamp,maxIlmDt);

			}
			
			
			return true;
		}
		
		private void getConfigDataForInserts(String billId, String cisDivision, String currencyCd, DateTime uniqueTimeStamp, DateTime maxIlmDt) {
			
		PreparedStatement preparedStatement =null;
		PreparedStatement preparedStatement2 =null;
		List<SQLResultRow> stmtTempList = new ArrayList<>();

			try {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT DISTINCT BILLING_PARTY_ID FROM CM_INVOICE_DATA_LN WHERE BILL_ID= trim(:billId) AND ILM_DT>=:maxIlmDt");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("billId", billId, "BILL_ID");
				preparedStatement.bindDate("maxIlmDt",maxIlmDt.getDate());
				preparedStatement.setAutoclose(false);
				
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String partyId = resultSet.getString("BILLING_PARTY_ID");

					logger.debug("******************Party_id**************"+ partyId);
					logger.debug("******************maxIlmDt**************"+ maxIlmDt);
					/** find statement config related information */
					stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT A.STMT_DETAIL_ID,A.BILLING_PARTY_ID,A.AGGR_PARTY_ID,B.STMT_CONFIG_ID,B.STMT_PARTY_ID,B.STMT_TYPE FROM  "); 
					stringBuilder.append("CM_STMT_CONF_DTL A,CM_STMT_CONF_HDR B WHERE   ");
					stringBuilder.append("A.BILLING_PARTY_ID = trim(:partyId) AND A.BO_STATUS_CD=:actv AND A.BO_STATUS_CD=B.BO_STATUS_CD  ");
					stringBuilder.append("AND A.STMT_CONFIG_ID=B.STMT_CONFIG_ID  ");
					stringBuilder.append("AND (A.END_DT is null OR :maxIlmDt BETWEEN B.START_DT AND B.END_DT) ");
					preparedStatement2 = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement2.bindString("partyId", partyId.trim(),"BILLING_PARTY_ID");
					preparedStatement2.bindString("actv", "ACTV", "BO_STATUS_CD");
					preparedStatement2.bindDate("maxIlmDt",maxIlmDt.toDate());
					preparedStatement2.setAutoclose(false);
				
					stmtTempList = preparedStatement2.list();

					if (preparedStatement2 != null) {
						preparedStatement2.close();
						preparedStatement2 = null;
					}
					if(!stmtTempList.isEmpty())
					{
						for (SQLResultRow rs : stmtTempList){
						insertStmtTempRecords(rs,billId,uniqueTimeStamp,cisDivision,currencyCd);
						}
					}
					
			}
					
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			finally
			{
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
				
		}

		private void insertStmtTempRecords(SQLResultRow sQLResultRow, String billId,
				DateTime uniqueTimeStamp, String cisDivision, String currencyCd) {

			/** Populate temporary detail table*/
			
			PreparedStatement preparedStatement2=null;
			String stmtId = "";						
			String partyId = sQLResultRow.getString("BILLING_PARTY_ID");
			String aggrPartyId = sQLResultRow.getString("AGGR_PARTY_ID");
			String stmtConfigId = sQLResultRow.getString("STMT_CONFIG_ID");
			String stmtPartyId = sQLResultRow.getString("STMT_PARTY_ID");
			String stmtType = sQLResultRow.getString("STMT_TYPE");
			stmtId = stmtId.concat(stmtConfigId).concat(cisDivision).concat(currencyCd);
			logger.debug("***********Inside insertStmtTempRecords stmtConfigId***********" +stmtConfigId);
			
			try{
			StringBuilder stringBuilder = new StringBuilder();
			stringBuilder.append("INSERT INTO CM_STMT_GEN_HDR_TMP ");
			stringBuilder.append("(STMT_ID,STMT_CONFIG_ID,STMT_PARTY_ID,STMT_TYPE,CIS_DIVISION,CURRENCY_CD,PRV_STMT_ID,UPLOAD_DTTM,EXTRACT_FLG, ");
			stringBuilder.append("EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
			stringBuilder.append("VALUES(:stmtId,:stmtConfigId,:stmtPartyId,:stmtType,:cisDivision,:currencyCd,' ',:uniqueTimeStamp,'Y','',:uniqueTimeStamp,'Y') ");
			preparedStatement2 = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement2.bindString("stmtId", stmtId, "STMT_ID");
			preparedStatement2.bindString("stmtConfigId", stmtConfigId, "STMT_CONFIG_ID");
			preparedStatement2.bindString("stmtPartyId", stmtPartyId, "STMT_PARTY_ID");
			preparedStatement2.bindString("stmtType", stmtType, "STMT_TYPE");
			preparedStatement2.bindString("cisDivision", cisDivision, "CIS_DIVISION");
			preparedStatement2.bindString("currencyCd", currencyCd, "CURRENCY_CD");
			preparedStatement2.bindDateTime("uniqueTimeStamp", uniqueTimeStamp);
			preparedStatement2.executeUpdate();
			
			}catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			finally
			{
				if (preparedStatement2 != null) {
					preparedStatement2.close();
					preparedStatement2 = null;
				}
			}
						
			try{
			StringBuilder stringBuilder = new StringBuilder();

			stringBuilder.append("INSERT INTO CM_STMT_GEN_DTL_TMP ");
			stringBuilder.append("(STMT_DETAIL_ID,STMT_ID,BILLING_PARTY_ID,AGGR_PARTY_ID,BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
			stringBuilder.append("VALUES(CM_STMT_DETAIL_ID_SEQ.NEXTVAL,:stmtId,:partyId,:aggrPartyId,trim(:billId),:uniqueTimeStamp,'Y','',"
					+ ":uniqueTimeStamp,'Y')  ");
			preparedStatement2 = createPreparedStatement(stringBuilder.toString(),"");

			preparedStatement2.bindString("stmtId", stmtId, "STMT_ID");
			preparedStatement2.bindString("partyId", partyId, "BILLING_PARTY_ID");
			preparedStatement2.bindString("aggrPartyId", aggrPartyId, "AGGR_PARTY_ID");
			preparedStatement2.bindString("billId", billId, "BILL_ID");
			preparedStatement2.bindDateTime("uniqueTimeStamp", uniqueTimeStamp);
			preparedStatement2.executeUpdate();
			
			}catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			finally
			{
				if (preparedStatement2 != null) {
					preparedStatement2.close();
					preparedStatement2 = null;
				}
			}	
		}
		

		private void insertStmtTemp(String prevStmtId, DateTime uniqueTimeStamp) {

			PreparedStatement preparedStatement=null;
		 	StringBuilder stringBuilder = new StringBuilder();

			try {
			 	stringBuilder.append("INSERT INTO CM_STMT_GEN_HDR_TMP ");
				stringBuilder.append("(STMT_ID,STMT_CONFIG_ID,STMT_PARTY_ID,STMT_TYPE,CIS_DIVISION,CURRENCY_CD,PRV_STMT_ID,UPLOAD_DTTM,EXTRACT_FLG, ");
				stringBuilder.append("EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) ");
				stringBuilder.append("SELECT STMT_ID||'RECALC',STMT_CONFIG_ID,STMT_PARTY_ID,STMT_TYPE,CIS_DIVISION,CURRENCY_CD,STMT_ID as PRV_STMT_ID,:uniqueTimeStamp,EXTRACT_FLG, ");
			 	stringBuilder.append("EXTRACT_DTTM,:uniqueTimeStamp,ILM_ARCH_SW ");
			 	stringBuilder.append("FROM CM_STMT_GEN_HDR WHERE STMT_ID=:prevStmtId "); 
			 	preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			 	preparedStatement.bindString("prevStmtId", prevStmtId, "PRV_STMT_ID");
			 	preparedStatement.bindDateTime("uniqueTimeStamp", uniqueTimeStamp);
			 	preparedStatement.executeUpdate();
			
		} catch (ThreadAbortedException e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
			 throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		 } catch (Exception e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
		 }
		 finally
		 {
			 if (preparedStatement != null) {
				 preparedStatement.close();
				 preparedStatement = null;
			 }
		 }
			
			try {
			 	stringBuilder = new StringBuilder();
			 	stringBuilder.append("INSERT INTO CM_STMT_GEN_DTL_TMP ");
			 	stringBuilder.append("(STMT_DETAIL_ID,STMT_ID,BILLING_PARTY_ID,AGGR_PARTY_ID,BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ILM_DT,ILM_ARCH_SW) "); 
			 	stringBuilder.append("SELECT CM_STMT_DETAIL_ID_SEQ.NEXTVAL,A.STMT_ID||'RECALC',A.BILLING_PARTY_ID,A.AGGR_PARTY_ID,NVL(B.CHILD_BILL_ID,A.BILL_ID),:uniqueTimeStamp,A.EXTRACT_FLG,A.EXTRACT_DTTM,:uniqueTimeStamp,A.ILM_ARCH_SW "); 
			 	stringBuilder.append("FROM CM_STMT_GEN_DTL A,CM_INV_RELATION_STG b WHERE A.STMT_ID=:prevStmtId ");
			 	stringBuilder.append("AND A.BILL_ID=B.PARENT_BILL_ID(+) AND B.RELATIONSHIP_TYPE(+)='53' ");
			 	preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			 	preparedStatement.bindString("prevStmtId", prevStmtId, "PRV_STMT_ID");
			 	preparedStatement.bindDateTime("uniqueTimeStamp", uniqueTimeStamp);
			 	preparedStatement.executeUpdate();
				
		 } catch (ThreadAbortedException e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
			 throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		 } catch (Exception e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
		 }
		 finally
		 {
			 if (preparedStatement != null) {
				 preparedStatement.close();
				 preparedStatement = null;
			 }
		 }
			
		}

		private String getPreviousStatementId(String parentBillId) {
			PreparedStatement preparedStatement=null;
			String prevStmtId=null;
			
			try {
			 	StringBuilder stringBuilder = new StringBuilder();
			 	stringBuilder.append("SELECT STMT_ID FROM CM_STMT_GEN_DTL WHERE bill_id= trim(:billId)");
			 	preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			 	preparedStatement.bindString("billId", parentBillId, "BILL_ID");
			 	preparedStatement.setAutoclose(false);
			 	SQLResultRow sqlRow=preparedStatement.firstRow();
			 	if(notNull(sqlRow))
			 	{
			 		prevStmtId = sqlRow.getString("STMT_ID");
			 	}
				
		 } catch (ThreadAbortedException e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
			 throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		 } catch (Exception e) {
			 logger.error("Inside executeWorkUnit() method, Error -", e);
		 }
		 finally
		 {
			 if (preparedStatement != null) {
				 preparedStatement.close();
				 preparedStatement = null;
			 }
		 }
			return prevStmtId;
		}

		/**
		 * finalizeThreadWork() is execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork()  {
			logger.info("Inside finalizeThreadWork() method");
			super.finalizeThreadWork();
		}
	}

	public static final class StatementBillData_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String billId;
		private String cisDivision;
		private String currencyCd;
		
		public StatementBillData_Id(String billId, String cisDivision, String currencyCd) {
			setBillId(billId);
			setCisDivision(cisDivision);
			setCurrencyCd(currencyCd);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}
		
		public String getBillId() {
			return billId;
		}

		public void setBillId(String billId) {
			this.billId = billId;
		}

		public String getCisDivision() {
			return cisDivision;
		}

		public void setCisDivision(String cisDivision) {
			this.cisDivision = cisDivision;
		}

		public String getCurrencyCd() {
			return currencyCd;
		}

		public void setCurrencyCd(String currencyCd) {
			this.currencyCd = currencyCd;
		}

	}
}
