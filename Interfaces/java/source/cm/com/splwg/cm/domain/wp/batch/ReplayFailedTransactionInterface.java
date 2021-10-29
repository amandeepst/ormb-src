/*******************************************************************************
 * FileName                   : ReplayFailedTransactionInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Dec 08, 2017
 * Version Number             : 0.3
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name 		  | Nature of Change
0.1		 NA				Dec 08, 2017		Ankur		            NAP-18746 Replay failed transaction
0.2		 NA				Jan 11, 2018		Ankur		            PAM-16877 CM_INVDT failing due to incorrect funding currency fix
0.3		 NA				Mar 13, 2018		Ankur		            PAM-17752 & PAM-17763
0.4		 NAP-33051		Sept 06, 2018		Prerna Mehta			Fixed production issues	
0.5		 NAP-34391		Oct 01, 2018	    Prerna Mehta			Fixed Performance issue
0.6		 NAP-33051		Oct 29, 2018	    Prerna Mehta			Added check for curr_sys_prcs_dt
0.7		 NAP-40972		Feb 21, 2019	    RIA			            Changes to implement Hummingbird.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.TransactionDtlStatusLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author jaina555
 *
 *This Batch Program gives ability to auto replay failed transactions (once) under certain failure conditions
@BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = numberOfReplays, type = integer)})
 */
public class ReplayFailedTransactionInterface extends ReplayFailedTransactionInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(ReplayFailedTransactionInterface.class);

	public JobWork getJobWork() {

		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList = getFailedTransactionData();
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}


	/**
	 * getFailedTransactionData() method selects distinct Failed Transactions Ids 
	 * from CI_TXN_DETAIL 
	 * 
	 * @return List ThreadWorkUnit
	 */
	private List<ThreadWorkUnit> getFailedTransactionData() {

		PreparedStatement preparedStatement = null;
		Txn_Detail_Id txnDetailId = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		ThreadWorkUnit threadWorkUnit = null;
		StringBuilder stringBuilder = new StringBuilder();
		BigInteger failedTxnDetailId=BigInteger.ZERO;
		Date txnDttm = null;
		String acctNbr = "";
		String udfChar25 = "";
		String udfChar36 = "";
		String extTxnNbr = "";
		BigInteger replayCount=getParameters().getNumberOfReplays();
		if(isNull(replayCount))
			replayCount = BigInteger.ONE;

		//else
		//	replayCount = BigInteger.ONE.add(replayCount);

		/** selects distinct Failed Transactions Ids and create worker units
		 */
		try {
			//			stringBuilder.append("SELECT TXN_DETAIL_ID,TXN_DTTM,ACCT_NBR,UDF_CHAR_25,UDF_CHAR_36 FROM CI_TXN_DETAIL WHERE BO_STATUS_CD IN (:error,:ignr) " );
			//			//stringBuilder.append(" AND  TRUNC(CURR_SYS_PRCS_DT-TXN_UPLOAD_DTTM)<=:replayCount " );
			//			stringBuilder.append(" AND  CURR_SYS_PRCS_DT >= :sysDate - :replayCount " );

			//Changes to fetch records form Replay Transaction Error table: CM_TXN_ERROR

			stringBuilder.append("(SELECT TXN_DETAIL_ID, TXN_DTTM, ACCT_NBR, UDF_CHAR_25, UDF_CHAR_36, EXT_TXN_NBR " );

			stringBuilder.append("FROM CI_TXN_DETAIL_STG " );
			stringBuilder.append("WHERE BO_STATUS_CD IN (:error,:ignr) " );
			stringBuilder.append("AND LAST_SYS_PRCS_DT >= :sysDate - :replayCount) " );
			stringBuilder.append("UNION " );
			stringBuilder.append("(SELECT A.TXN_DETAIL_ID, A.TXN_DTTM, A.ACCT_NBR, A.UDF_CHAR_25,A.UDF_CHAR_36, A.EXT_TXN_NBR " );
			stringBuilder.append("FROM CI_TXN_DETAIL_STG A, CM_TXN_ERROR B " );
			stringBuilder.append("WHERE A.TXN_DETAIL_ID=B.TXN_DETAIL_ID " );
			stringBuilder.append("AND B.STATUS = :errorStatus " );
			stringBuilder.append("AND A.LAST_SYS_PRCS_DT >= :sysDate - :replayCount) " );


			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("error", TransactionDtlStatusLookup.constants.TXN_ERROR.value().trim(),"BO_STATUS_CD");
			preparedStatement.bindString("errorStatus", TransactionDtlStatusLookup.constants.TXN_ERROR.value().trim()," ");
			preparedStatement.bindString("ignr", TransactionDtlStatusLookup.constants.TXN_IGNORED.value().trim(),"BO_STATUS_CD");
			preparedStatement.bindDate("sysDate", getSystemDateTime().getDate());
			preparedStatement.bindBigInteger("replayCount", replayCount);
			preparedStatement.setAutoclose(false);

			for (SQLResultRow resultSet : preparedStatement.list()) {
				failedTxnDetailId = resultSet.getInteger("TXN_DETAIL_ID");
				txnDttm = resultSet.getDate("TXN_DTTM");
				acctNbr = resultSet.getString("ACCT_NBR");
				udfChar25 = resultSet.getString("UDF_CHAR_25");
				udfChar36 = resultSet.getString("UDF_CHAR_36");
				extTxnNbr = resultSet.getString("EXT_TXN_NBR");

				//replayCount = resultSet.getInteger("REPLAYCOUNT");

				txnDetailId = new Txn_Detail_Id(failedTxnDetailId);
				threadWorkUnit = new ThreadWorkUnit();
				threadWorkUnit.setPrimaryId(txnDetailId);
				threadWorkUnit.addSupplementalData("txnDttm", txnDttm);
				threadWorkUnit.addSupplementalData("acctNbr", acctNbr);
				threadWorkUnit.addSupplementalData("udfChar25", udfChar25);
				threadWorkUnit.addSupplementalData("udfChar36", udfChar36);
				threadWorkUnit.addSupplementalData("extTxnNbr", extTxnNbr);
				//threadWorkUnit.addSupplementalData("replayCount", replayCount);
				threadWorkUnitList.add(threadWorkUnit);
				threadWorkUnit = null;
				resultSet = null;
				txnDetailId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getFailedTransactionData" , e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return threadWorkUnitList;
	}

	public Class<ReplayFailedTransactionInterfaceWorker> getThreadWorkerClass() {
		return ReplayFailedTransactionInterfaceWorker.class;
	}

	public static class ReplayFailedTransactionInterfaceWorker extends ReplayFailedTransactionInterfaceWorker_Gen {

		// Default constructor
		public ReplayFailedTransactionInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once per
		 * thread by the framework.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
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
		 * each row of processing.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {			

			Txn_Detail_Id txnDetailId = (Txn_Detail_Id) unit.getPrimaryId();
			BigInteger failedTxnDetailId=txnDetailId.getTxnDetailId();
			Date txnDttm = (Date)unit.getSupplementallData("txnDttm");
			String acctNbr = (String)unit.getSupplementallData("acctNbr");
			String udfChar25 = (String)unit.getSupplementallData("udfChar25");
			String udfChar36 = (String)unit.getSupplementallData("udfChar36");
			String extTxnNbr = (String)unit.getSupplementallData("extTxnNbr");
			Bool resFlg=Bool.TRUE;

			//Changes for the addition of new error table, CM_TXN_ERROR
			updateTransactionErrorRecords(failedTxnDetailId, extTxnNbr);



			//BigInteger replayCount = (BigInteger)unit.getSupplementallData("replayCount");
			//final BigInteger THRESHOLD_REPLAY = new BigInteger("1");
			//int comparisonResult = replayCount.compareTo(THRESHOLD_REPLAY);


			//logger.info("number of replays "+replayCount);
			//StringBuilder stringBuilder = new StringBuilder();
			//PreparedStatement preparedStatement = null;

			String acctNbrArray[] = acctNbr.concat(" ").split("_");
			//String perIdNbr = "";
			//String acctType = "";
			String oldCurrencyAcctNbr = "";
			String oldCurrencyUdfChar36 = "";
			//String newCurrencyCd = "";
			//String acctId = "";
			//Date setUpDt = null;


			/*if(comparisonResult>0)
			{
				try {
			 *//**If number of replays exceeds a threshold then update status of transaction to 'DNRP'   *//*

					stringBuilder.append("UPDATE CI_ROLLBACK_TXN_DETAIL SET BO_STATUS_CD='DNRP' WHERE TXN_DETAIL_ID=:failedTxnDetailId " );
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Exception in executeWorkUnit" , e);
					throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}		
			}*/
			//else
			//	{
			try
			{
				//perIdNbr = CommonUtils.CheckNull(acctNbrArray[0]).trim();
				//acctType = CommonUtils.CheckNull(acctNbrArray[1]).trim(); 
				if(acctNbrArray.length>1)
					oldCurrencyAcctNbr = CommonUtils.CheckNull(acctNbrArray[2]).trim();
			}
			catch(Exception e)
			{
				logger.error("Exception in executeWorkUnit due to acctNbr "+acctNbr , e);
				return false;
			}	

			//Validation check for UDF_CHAR_36
			if(notBlank(udfChar36)){
				String udfChar36Array[] = udfChar36.concat(" ").split("_");
				if(udfChar36Array.length>1)
					oldCurrencyUdfChar36=CommonUtils.CheckNull(udfChar36Array[2]).trim();
				resFlg=checkAndUpdateTxnDtl(failedTxnDetailId,udfChar36,oldCurrencyUdfChar36,
						udfChar25,"UDF_CHAR_36",Boolean.FALSE,txnDttm);
				if(resFlg.isFalse())
				{	
					return false;
				}
			}

			//Validation check for ACCT_NBR
			resFlg=checkAndUpdateTxnDtl(failedTxnDetailId,acctNbr,oldCurrencyAcctNbr,
					udfChar25,"ACCT_NBR",Boolean.TRUE,txnDttm);
			if(resFlg.isFalse())
			{	
				return false;
			}		
			return true;
		}

		/**
		 * This method will fetch setup date based on account id retrieved from  failed transaction
		 * @param acctId
		 * @return setup date
		 */
		private Date fetchSetupDate(String acctId){
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement=null;
			Date setupDt=null;
			try {
				stringBuilder.append("SELECT SETUP_DT FROM CI_ACCT WHERE ACCT_ID=:acctId " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctId", acctId, "ACCT_ID");
				preparedStatement.setAutoclose(false);
				SQLResultRow sQLResultRow = preparedStatement.firstRow();
				setupDt = sQLResultRow.getDate("SETUP_DT");
			} catch (Exception e) {
				logger.error("Exception in fetchSetupDate" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return setupDt;
		}

		/**
		 * This method will perform validation for account number ,currency and transaction date fields passed as parameter 
		 * as well as update them with correct values
		 * @param failedTxnDetailId
		 * @param txnAcctNbr
		 * @param oldCurrencyCd
		 * @param udfChar25
		 * @param column
		 * @param isAcctNbrCheck
		 * @return String
		 */
		private Bool checkAndUpdateTxnDtl(BigInteger failedTxnDetailId,String txnAcctNbr,
				String oldCurrencyCd,String udfChar25,String column,Boolean isAcctNbrCheck,Date txnDttm){
			/**Fetch account identifier ,account number & currency code on the basis of account number received in CI_TXN_DETAIL table*/
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			String acctId = "";
			String newCurrencyCd="";
			String acctNbr="";
			try {
				stringBuilder.append("SELECT ACCT_ID,ACCT_NBR,REGEXP_SUBSTR (ACCT_NBR, '[^_]+', 1, 3) AS CURRENCY_CD FROM CI_ACCT_NBR WHERE UPPER(ACCT_NBR) LIKE :acctNbr||'%'" );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctNbr", txnAcctNbr, "ACCT_NBR");
				preparedStatement.setAutoclose(false);
				int count = preparedStatement.list().size();
				if(count==0)
				{
					logger.error(column+" of CI_TXN_DETAIL_STG TABLE is invalid "+txnAcctNbr);
					return Bool.FALSE;					
				}
				else if(count>1)
				{
					logger.error(column+" of CI_TXN_DETAIL_STG TABLE has more than one currency "+txnAcctNbr);
					return Bool.FALSE;
				}
				else
				{
					SQLResultRow sQLResultRow = preparedStatement.firstRow();
					acctId = sQLResultRow.getString("ACCT_ID");
					acctNbr = sQLResultRow.getString("ACCT_NBR");
					newCurrencyCd = sQLResultRow.getString("CURRENCY_CD");
				}

			} catch (Exception e) {
				logger.error("Exception in checkAndUpdateTxnDtl" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			/**if currency value is missing from txnAcctNbr then update txnAcctNbr with correct currency value */
			/**if udf_char_25 is blank in the field of transaction then update udf_char_25 with correct funding currency value */
			if(oldCurrencyCd.equals("") || (isAcctNbrCheck && udfChar25.trim().equals("")))
			{
				//acctNbr = perIdNbr.concat("_").concat(acctType).concat("_").concat("GBP");
				stringBuilder = new StringBuilder();

				try {
					stringBuilder.append("UPDATE CI_TXN_DETAIL_STG SET "+column+"=:acctNbr,TXN_DTTM=:sysDate ");
					if(isAcctNbrCheck)
						stringBuilder.append(" ,UDF_CHAR_25=:newCurrencyCd ");
					stringBuilder.append(" WHERE TXN_DETAIL_ID=:failedTxnDetailId " );
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
					preparedStatement.bindString("acctNbr", acctNbr, column);
					preparedStatement.bindDate("sysDate", getSystemDateTime().getDate().addDays(-1));
					if(isAcctNbrCheck)
						preparedStatement.bindString("newCurrencyCd", newCurrencyCd, "UDF_CHAR_25");
					preparedStatement.setAutoclose(false);
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Exception in checkAndUpdateTxnDtl()" , e);
					throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}	
			}
			else{
				Date setUpDt=fetchSetupDate(acctId);
				if (txnDttm.compareTo(setUpDt)<0)
				{
					updateTxnDateTime(failedTxnDetailId);
				}
			}
			return Bool.TRUE;
		}

		/**
		 * This method will update TXN_DTTM to SYSDATE-1
		 */
		public void updateTxnDateTime(BigInteger failedTxnDetailId) {

			logger.debug("Inside updateTxnDateTime() method");
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try {
				stringBuilder.append("UPDATE CI_TXN_DETAIL_STG SET TXN_DTTM=:sysDate WHERE TXN_DETAIL_ID=:failedTxnDetailId " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.bindDate("sysDate", getSystemDateTime().getDate().addDays(-1));
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

		}

		public BigInteger fetchCountOfErrorrecords(BigInteger failedTxnDetailId){
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			BigInteger count = null;
			try{

				stringBuilder.append("SELECT COUNT(*) AS COUNT FROM CM_TXN_ERROR WHERE " );
				stringBuilder.append("TXN_DETAIL_ID =:failedTxnDetailId " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.setAutoclose(false);

				count = preparedStatement.firstRow().getInteger("COUNT");
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return count;

		}

		public String fetchBoStatusCode(BigInteger failedTxnDetailId){

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			String status = " ";

			try{
				stringBuilder.append("SELECT  BO_STATUS_CD FROM  CI_TXN_DETAIL_STG WHERE TXN_DETAIL_ID=:failedTxnDetailId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.setAutoclose(false);

				SQLResultRow statusResult = preparedStatement.firstRow();
				if(notNull(statusResult)){
					status = preparedStatement.firstRow().getString("BO_STATUS_CD");
				}
			}
			catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return status;

		}


		public BigInteger fechNumberOfReplays(BigInteger failedTxnDetailId){

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			BigInteger replays = null;

			try{
				stringBuilder.append("SELECT NO_OF_REPLAY FROM  CM_TXN_ERROR WHERE TXN_DETAIL_ID=:failedTxnDetailId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.setAutoclose(false);
				//preparedStatement.executeUpdate();

				SQLResultRow noOfReplays = preparedStatement.firstRow();
				if(notNull(noOfReplays)){
					replays = preparedStatement.firstRow().getInteger("NO_OF_REPLAY");
				}

			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}


			return replays;
		}

		public void insertErrorRecords(BigInteger failedTxnDetailId, String extTxnNbr){

			String status = fetchBoStatusCode(failedTxnDetailId);
			String messagelanguage = getMessagelanguage(failedTxnDetailId);
			Date errorDate = getProcessDateTime().getDate().addDays(-1);

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try{

				stringBuilder.append("Insert into CM_TXN_ERROR (TXN_DETAIL_ID,EXT_TXN_NBR, " );
				stringBuilder.append("ERROR_INFO,ERROR_DTTM,NO_OF_REPLAY,STATUS, " );
				stringBuilder.append("LAST_PRCS_DTTM,FIXED_DTTM,ILM_DT,ILM_ARCH_SW) " );
				stringBuilder.append("values (:failedTxnDetailId,:extTxnNbr,:messagelanguage,:errorDate, " );
				stringBuilder.append("0,:status,:lastprcsDt,'',:lastprcsDt,'N') " );

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.bindString("extTxnNbr", extTxnNbr, "EXT_TXN_NBR");
				preparedStatement.bindString("messagelanguage", messagelanguage, "ERROR_INFO");
				preparedStatement.bindDate("lastprcsDt", getProcessDateTime().getDate());
				preparedStatement.bindDate("errorDate", errorDate);
				preparedStatement.bindString("status", status.trim()," ");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			}

			catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		public void updateErrorTable(BigInteger failedTxnDetailId){
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try{
				stringBuilder.append("UPDATE CM_TXN_ERROR SET NO_OF_REPLAY=NO_OF_REPLAY+1, LAST_PRCS_DTTM=:lastprcsDt WHERE TXN_DETAIL_ID=:failedTxnDetailId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.bindDate("lastprcsDt", getProcessDateTime().getDate());
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			}
			catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

		}

		public void updateDonotReplay(BigInteger failedTxnDetailId){

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			try{

				stringBuilder.append("UPDATE CM_TXN_ERROR SET STATUS='DNRP',LAST_PRCS_DTTM=:lastprcsDt WHERE TXN_DETAIL_ID=:failedTxnDetailId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.bindDate("lastprcsDt", getProcessDateTime().getDate());
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			}
			catch (Exception e) {
				logger.error("Exception in executeWorkUnit" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}


		public void updateTransactionErrorRecords(BigInteger failedTxnDetailId, String extTxnNbr) {

			logger.debug("Inside updatetransactionErrorRecords() method");
			//			String messagelanguage = getMessagelanguage(failedTxnDetailId);
			//			Date errorDate = getProcessDateTime().getDate().addDays(-1);
			BigInteger count = fetchCountOfErrorrecords(failedTxnDetailId);


			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;

			try {


				if(count.equals(BigInteger.ZERO)){

					insertErrorRecords(failedTxnDetailId,extTxnNbr);					
				}

				else{
					try {
						String status = fetchBoStatusCode(failedTxnDetailId);

						if(status.trim().equals(TransactionDtlStatusLookup.constants.TXN_COMPLETED.value())){
							try {
								stringBuilder = new StringBuilder();
								preparedStatement = null;
								stringBuilder.append("UPDATE CM_TXN_ERROR SET STATUS='COMP', NO_OF_REPLAY=NO_OF_REPLAY+1, LAST_PRCS_DTTM=:fixedDt ,FIXED_DTTM=:fixedDt WHERE TXN_DETAIL_ID=:failedTxnDetailId ");
								preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
								preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
								preparedStatement.bindDate("fixedDt", getProcessDateTime().getDate().addDays(-1));
								preparedStatement.executeUpdate();

							} catch (Exception e) {
								logger.error("Exception in updateTransactionErrorRecords()" , e);
								throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
							}
							finally {
								if (preparedStatement != null) {
									preparedStatement.close();
									preparedStatement = null;
								}
							}
						}

						else if(status.trim().equals(TransactionDtlStatusLookup.constants.TXN_ERROR.value()) || status.trim().equals(TransactionDtlStatusLookup.constants.TXN_IGNORED.value())){

							BigInteger replays = fechNumberOfReplays(failedTxnDetailId);
							BigInteger replayParameter = getParameters().getNumberOfReplays();

							if(replays.compareTo(replayParameter)<0){
								updateErrorTable(failedTxnDetailId);
							}
							else{
								updateDonotReplay(failedTxnDetailId);
							}
						}

					} catch (Exception e) {
						logger.error("Exception in updateTransactionErrorRecords()" , e);
						throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
					}
					finally {
						if (preparedStatement != null) {
							preparedStatement.close();
							preparedStatement = null;
						}
					}


				}
			}
			catch (Exception e) {
				logger.error("Exception in updateTransactionErrorRecords()" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		private String getMessagelanguage(BigInteger failedTxnDetailId ){

			PreparedStatement preparedStatement = null;
			String messagelanguage = " ";
			BigInteger messageNbr = null ;
			BigInteger messageCategory = null;

			try {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT EXCP.MESSAGE_NBR, EXCP.MESSAGE_CAT_NBR from CI_TXN_DETAIL_EXCP EXCP, CM_TXN_INTERSECTION INTER, CI_TXN_DETAIL_STG STG ");
				stringBuilder.append("WHERE EXCP.TXN_DETAIL_ID=INTER.SUMMARY_ID ");
				stringBuilder.append("AND STG.TXN_DETAIL_ID = INTER.ORG_TXN_ID ");
				stringBuilder.append("AND STG.TXN_DETAIL_ID = :failedTxnDetailId  ");
				stringBuilder.append("AND INTER.ORG_TXN_ID = :failedTxnDetailId  ");
				stringBuilder.append("order by EXCP.CURR_SYS_PRCS_DT desc ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindBigInteger("failedTxnDetailId", failedTxnDetailId);
				preparedStatement.setAutoclose(false);

				SQLResultRow messageDetails = preparedStatement.firstRow();
				if(notNull(messageDetails)){
					messageNbr = messageDetails.getInteger("MESSAGE_NBR");
					messageCategory = messageDetails.getInteger("MESSAGE_CAT_NBR");

				}
			}catch (Exception e) {
				logger.error("Exception in updateTransactionErrorRecords()" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			}
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			try {
				StringBuilder stringBuilder = new StringBuilder();
				stringBuilder.append("SELECT MESSAGE_TEXT FROM CI_MSG_L WHERE MESSAGE_NBR=:messageNbr AND MESSAGE_CAT_NBR=:messageCategory");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");

				preparedStatement.bindBigInteger("messageNbr", messageNbr);
				preparedStatement.bindBigInteger("messageCategory", messageCategory);
				preparedStatement.setAutoclose(false);

				SQLResultRow messageText = preparedStatement.firstRow();
				if(notNull(messageText)){
					messagelanguage = messageText.getString("MESSAGE_TEXT");
				}

			} catch (Exception e) {
				logger.error("Exception in updateTransactionErrorRecords()" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			}
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			return messagelanguage;
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

		@Override
		public void finalizeJobWork() throws Exception {
			super.finalizeJobWork();
			//updateTxnDtlStatus();
		}

		/**
		 * This method will update status of all records(in error or ignore status) to 'DNRP' in ci_txn_detail 
		 * for which CURR_SYS_PRCS_DT is less than SYSDATE - replay count
		 */
		/*public void updateTxnDtlStatus(){
			StringBuilder stringBuilder=new StringBuilder();
			PreparedStatement preparedStatement=null;
			BigInteger replayCount=getParameters().getNumberOfReplays();
			if(isNull(replayCount))
				replayCount = BigInteger.ONE;
			else
				replayCount = BigInteger.ONE.add(replayCount);
			try {				
				stringBuilder.append("UPDATE CI_ROLLBACK_TXN_DETAIL SET BO_STATUS_CD=:dnrp WHERE BO_STATUS_CD IN (:error,:ignr) " );
				//stringBuilder.append(" AND  TRUNC(CURR_SYS_PRCS_DT-TXN_UPLOAD_DTTM)>:replayCount " );
				stringBuilder.append(" AND  CURR_SYS_PRCS_DT < :sysDate - :replayCount " ); 
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("error", TransactionDtlStatusLookup.constants.TXN_ERROR.value(),"BO_STATUS_CD");
				preparedStatement.bindString("ignr", TransactionDtlStatusLookup.constants.TXN_IGNORED.value(),"BO_STATUS_CD");
				preparedStatement.bindDate("sysDate",getSystemDateTime().getDate());
				preparedStatement.bindBigInteger("replayCount",replayCount);
				preparedStatement.bindString("dnrp","DNRP","BO_STATUS_CD");
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Exception in updateTxnDtlStatus()" , e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}*/
	}


	public static final class Txn_Detail_Id implements Id {

		private static final long serialVersionUID = 1L;

		private BigInteger txnDetailId;


		public Txn_Detail_Id(BigInteger txnDetailId) {
			setTxnDetailId(txnDetailId);
		}

		public static long getSerialversionuid() {
			return serialVersionUID;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public BigInteger getTxnDetailId() {
			return txnDetailId;
		}

		public void setTxnDetailId(BigInteger txnDetailId) {
			this.txnDetailId = txnDetailId;
		}

	}
}
