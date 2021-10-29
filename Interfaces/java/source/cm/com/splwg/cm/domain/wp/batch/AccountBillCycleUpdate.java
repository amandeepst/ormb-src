/*******************************************************************************
 * FileName                   : AccountBillCycleUpdate.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Oct 13, 2017
 * Version Number             : 0.4
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name 		  | Nature of Change
0.1		 NA				Oct 13, 2017		Preeti		            NAP-18703 Update Account Bill cycle.
0.2		 NA				Feb 20, 2018		Ankur		            PAM-17462 Fix
0.3		 NA				Apr 09, 2018		Ankur		            NAP-25252 emegency billing issue update code to use adhoc sw bill 'y'
0.4      NA             May 01, 2018        Ankur                   NAP-26526 & NAP-26537 changed to complete_dttm      
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
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
 * This Batch Program updates Bill cycle on the account to enable Generation of more than 1 Bill for a given day.
 *
   @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billCycleCode, type = string)})
 */
public class AccountBillCycleUpdate extends AccountBillCycleUpdate_Gen {

	public static final Logger logger = LoggerFactory.getLogger(AccountBillCycleUpdate.class);

	// Default constructor
	public AccountBillCycleUpdate() {
	}

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork");
		
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList=getEmergencyData();
		logger.debug("No. of rows for processing "+threadWorkUnitList);
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}	
	
	/**
	 * getEmergencyData() method selects Bill Ids for processing by this Interface.
	 * 
	 * @return List Invoice_Data_id
	 */
	
	private List<ThreadWorkUnit> getEmergencyData() {
		logger.info("Inside getEmergencyData() method");
		PreparedStatement preparedStatement = null;		
		String perIdNbr = "";
		String acctType = "";
		String currencyCd = "";
		StringBuilder stringBuilder = new StringBuilder();
		EmergencyData_Id emergencyData = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		String billCycleCodeParameter = CommonUtils.CheckNull(getParameters().getBillCycleCode()).trim();
		
		/**Select per_id_nbr,acct_type & currency_Cd from emergency table of merchant for which we want to run billing twice in single day*/

		try {
			if(!(billCycleCodeParameter.equals(""))){
				stringBuilder.append("SELECT PER_ID_NBR,ACCT_TYPE,CURRENCY_CD FROM CM_EMERG_CYCLE_STG WHERE BO_STATUS_CD='UPLD'");
			}
			else
			{
				stringBuilder.append("SELECT PER_ID_NBR,ACCT_TYPE,CURRENCY_CD FROM CM_EMERG_CYCLE_STG WHERE BO_STATUS_CD='INPD'");	
			}
			preparedStatement = createPreparedStatement(stringBuilder.toString(),""); 
			preparedStatement.setAutoclose(false);
			
			 for (SQLResultRow sqlRow : preparedStatement.list()) {
				
				 perIdNbr = sqlRow.getString("PER_ID_NBR");
				 acctType = sqlRow.getString("ACCT_TYPE");
				 currencyCd = sqlRow.getString("CURRENCY_CD");
				 emergencyData = new EmergencyData_Id(perIdNbr, acctType,currencyCd);
				
				//*************************
				 threadworkUnit = new ThreadWorkUnit();
				 threadworkUnit.setPrimaryId(emergencyData);
				 threadWorkUnitList.add(threadworkUnit);
				 threadworkUnit = null;
				 emergencyData = null;
				//*************************
				logger.info("Emergency Data Ids added to Id class");
				
				}

		} catch (ThreadAbortedException e){
				logger.error("Inside getEmergencyData() method of InvoiceDataInterface, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Account Bill Cyle Update method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		
		return threadWorkUnitList;
	}

	
	public Class<AccountBillCycleUpdateWorker> getThreadWorkerClass() {
		return AccountBillCycleUpdateWorker.class;
	}

	public static class AccountBillCycleUpdateWorker extends
	AccountBillCycleUpdateWorker_Gen {		

		// Default constructor
		public AccountBillCycleUpdateWorker() {
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
			String perIdNbr = "";
			String acctType = "";
			String currencyCd = "";
			String acctId = "";
			String aStatus = "";
			String aMessageCategory = "";
			String aMessageNumber = "";
			String aErrorMessage = "";
			EmergencyData_Id emergencyData = (EmergencyData_Id)unit.getPrimaryId();
			perIdNbr = emergencyData.getPerIdNbr().trim();
			acctType = emergencyData.getAcctType().trim();
			currencyCd = emergencyData.getCurrencyCd().trim();
			String acctNbr = perIdNbr.concat("_").concat(acctType).concat("_").concat(currencyCd);
			Boolean isValidData = validateEmergencyData (perIdNbr, acctType, currencyCd);
			String billCycleCodeParameter = CommonUtils.CheckNull(getParameters().getBillCycleCode()).trim();
			String billCycleCode ="";
					
			if(!isValidData)
			{
				logger.error(acctNbr+" is not valid");
				return false;
			}
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			/** This query will fetch account id for combination of per_id_nbr,acct_nbr & currency_cd
			 */
			try {
				stringBuilder.append("SELECT ACCT_ID FROM CI_ACCT_NBR WHERE UPPER(ACCT_NBR)=:acctType");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctType", acctNbr, "ACCT_NBR");
				preparedStatement.setAutoclose(false);
				SQLResultRow sqlRow=preparedStatement.firstRow();
				if(sqlRow!=null)
				{
					acctId = sqlRow.getString("ACCT_ID");
				}
				else
				{
					aStatus = "ERROR";
					aMessageCategory = String.valueOf(CustomMessages.MESSAGE_CATEGORY);;
					aMessageNumber = CommonUtils.CheckNull(String.valueOf(CustomMessages.ACCT_NBR_NT_FOUND));
					aErrorMessage = getErrorDescription(String.valueOf(CustomMessages.ACCT_NBR_NT_FOUND));
					updateEmergencyCycleStaging(aStatus, aMessageCategory, aMessageNumber, aErrorMessage, perIdNbr, acctType, currencyCd);
					logger.error(acctNbr+" is not valid");
					return false;
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
			
			
			/**If bill cycle code parameter value is provided then first back up base bill cycle code value 
			 * for the party ids present in staging table and then update base table with emergency bill cycle code
			 */

			if(!(billCycleCodeParameter.equals(""))){
			
			 stringBuilder = new StringBuilder();
			
			try {
				/** This query is fetching existing bill cycle code on an account
				 */
				
				stringBuilder.append("SELECT BILL_CYC_CD FROM CI_ACCT WHERE ACCT_ID=:acctId");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctId", acctId, "ACCT_ID");
				preparedStatement.setAutoclose(false);
				SQLResultRow sqlRow=preparedStatement.firstRow();
				billCycleCode = sqlRow.getString("BILL_CYC_CD");
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
			
			stringBuilder = new StringBuilder();
			
			/**This query will take back up of existing bill cycle on an account into CM_EMERG_CYCLE_STG table
			 */
			try {
				stringBuilder.append("UPDATE CM_EMERG_CYCLE_STG SET BILL_CYC_CD=:billCycleCode WHERE PER_ID_NBR=:perIdNbr AND ACCT_TYPE=:acctType AND CURRENCY_CD=:currencyCd ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("perIdNbr", perIdNbr, "PER_ID_NBR");
				preparedStatement.bindString("acctType", acctType, "ACCT_NBR");
				preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
				preparedStatement.bindString("billCycleCode", billCycleCode, "BILL_CYC_CD");
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
			
			stringBuilder = new StringBuilder();
			
			/**This query will update account with emergency bill cycle code
			 */
			try {
				stringBuilder.append("UPDATE CI_ACCT SET BILL_CYC_CD=:billCycleCodeParameter WHERE ACCT_ID=:acctId");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("acctId", acctId, "ACCT_ID");
				preparedStatement.bindString("billCycleCodeParameter", billCycleCodeParameter, "BILL_CYC_CD");
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
			updateEmergencyCycleStaging("INPD", "0", "0", " ", perIdNbr, acctType, currencyCd);
	}
			
			/**If bill cycle code parameter value is not provided then revert back bill cycle code in base table with what's backed up in staging table.
			 */
			else{
				
				 stringBuilder = new StringBuilder();
				 
				 /** This query will fetch backed up bill cycle code from CM_EMERG_CYCLE_STG
					 */
					try {
						stringBuilder.append("SELECT BILL_CYC_CD FROM CM_EMERG_CYCLE_STG WHERE PER_ID_NBR=:perIdNbr AND ACCT_TYPE=:acctType AND CURRENCY_CD=:currencyCd ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("perIdNbr", perIdNbr, "PER_ID_NBR");
						preparedStatement.bindString("acctType", acctType, "ACCT_NBR");
						preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
						preparedStatement.setAutoclose(false);
						SQLResultRow sqlRow=preparedStatement.firstRow();
						billCycleCode = sqlRow.getString("BILL_CYC_CD");
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
					
					stringBuilder = new StringBuilder();
				
					/** This query will set original bill cycle of an account
					 */
					try {
						stringBuilder.append("UPDATE CI_ACCT SET BILL_CYC_CD=:billCycleCode WHERE ACCT_ID=:acctId");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("acctId", acctId, "ACCT_ID");
						preparedStatement.bindString("billCycleCode", billCycleCode, "BILL_CYC_CD");
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
					
					stringBuilder = new StringBuilder();
					
					/** This query will set adhoc switch to  'Y' for emergency bills for given account
					 */
					try {
						stringBuilder.append("update ci_bill set adhoc_bill_sw='Y' where trunc(COMPLETE_DTTM)=trunc(sysdate) and bill_cyc_cd='EMDY' and acct_id=:acctId");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("acctId", acctId, "ACCT_ID");
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
					
					
					updateEmergencyCycleStaging("COMPLETED", "0", "0", " ", perIdNbr, acctType, currencyCd);		
			}

			return true;
		}			

		private boolean validateEmergencyData (String perIdNbr, String acctType, String currencyCd) {
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			BigInteger count = new BigInteger("0");
			Boolean isValidData=true;
			String aStatus = "";
			String aMessageCategory = "";
			String aMessageNumber = "";
			String aErrorMessage = "";
			
			 	try {
					stringBuilder.append("SELECT COUNT(*) AS COUNT FROM CI_PER_ID WHERE PER_ID_NBR=:perIdNbr AND ID_TYPE_CD='EXPRTYID'");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("perIdNbr", perIdNbr, "PER_ID_NBR");
					preparedStatement.setAutoclose(false);
					SQLResultRow sqlRow=preparedStatement.firstRow();
					count = sqlRow.getInteger("COUNT");
					if(count.equals(BigInteger.ZERO))
						{
							isValidData=false;
							aStatus = "ERROR";
							aMessageCategory = String.valueOf(CustomMessages.MESSAGE_CATEGORY);;
							aMessageNumber = CommonUtils.CheckNull(String.valueOf(CustomMessages.PER_ID_NBR_NT_FOUND));
							aErrorMessage = getErrorDescription(String.valueOf(CustomMessages.PER_ID_NBR_NT_FOUND));
							updateEmergencyCycleStaging(aStatus, aMessageCategory, aMessageNumber, aErrorMessage, perIdNbr, acctType, currencyCd);
							return isValidData;
							
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
			 	
			 	if(!(acctType.equals("FUND")||acctType.equals("CHRG")||acctType.equals("CHBK")||acctType.equals("CRWD")))
			 	{
			 		isValidData=false;
			 		aStatus = "ERROR";
					aMessageCategory = String.valueOf(CustomMessages.MESSAGE_CATEGORY);;
					aMessageNumber = CommonUtils.CheckNull(String.valueOf(CustomMessages.ACCT_TYPE_NT_FOUND));
					aErrorMessage = getErrorDescription(String.valueOf(CustomMessages.ACCT_TYPE_NT_FOUND));
					updateEmergencyCycleStaging(aStatus, aMessageCategory, aMessageNumber, aErrorMessage, perIdNbr, acctType, currencyCd);
					return isValidData;
			 	}
				
			
			 	stringBuilder = new StringBuilder();
					
				try {
						stringBuilder.append("SELECT COUNT(*) AS COUNT FROM CI_CURRENCY_CD WHERE CURRENCY_CD=:currencyCd");
						preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
						preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
						preparedStatement.setAutoclose(false);
						SQLResultRow sqlRow=preparedStatement.firstRow();
						count = sqlRow.getInteger("COUNT");
						if(count.equals(BigInteger.ZERO))
						{
							isValidData=false;
							aStatus = "ERROR";
							aMessageCategory = String.valueOf(CustomMessages.MESSAGE_CATEGORY);;
							aMessageNumber = CommonUtils.CheckNull(String.valueOf(CustomMessages.CURRENCY_CD_NT_FOUND));
							aErrorMessage = getErrorDescription(String.valueOf(CustomMessages.CURRENCY_CD_NT_FOUND));
							updateEmergencyCycleStaging(aStatus, aMessageCategory, aMessageNumber, aErrorMessage, perIdNbr, acctType, currencyCd);
							return isValidData;
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
			return isValidData;
		}

		/**
		 * updateEmergencyCycleStaging() method will update the CM_EMERG_CYCLE_STG
		 * staging table with the processing status
		 * 
		 * @param aStatus
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 * @param perIdNbr
		 * @param acctType
		 * @param currencyCd
		 */
		private void updateEmergencyCycleStaging(String aStatus,
				String aMessageCategory, String aMessageNumber,String aErrorMessage,
				String perIdNbr, String acctType, String currencyCd){

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			if (aErrorMessage.length() > 255) {
				aErrorMessage = aErrorMessage.substring(0, 249);
			}
			
			String billCycleCodeParameter = CommonUtils.CheckNull(getParameters().getBillCycleCode()).trim();
			String initStatus="";
			
				if(!(billCycleCodeParameter.equals(""))){
					initStatus = "UPLD";
				}
				else
				{
					initStatus = "INPD";	
				}
				
			try {
				stringBuilder.append("UPDATE CM_EMERG_CYCLE_STG SET BO_STATUS_CD =:finalStatus, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append("MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription ");
				stringBuilder.append("WHERE PER_ID_NBR = :perIdNbr AND ACCT_TYPE = :acctType ");
				stringBuilder.append("AND CURRENCY_CD = :currencyCd AND BO_STATUS_CD = :initStatus ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("finalStatus", aStatus.trim(), "BO_STATUS_CD");
				preparedStatement.bindString("initStatus", initStatus, "BO_STATUS_CD");
				preparedStatement.bindBigInteger("messageCategory", new BigInteger(aMessageCategory.trim()));
				preparedStatement.bindBigInteger("messageNumber", new BigInteger(aMessageNumber.trim()));
				preparedStatement.bindString("errorDescription", CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
				preparedStatement.bindString("perIdNbr", perIdNbr, "PER_ID_NBR");
				preparedStatement.bindString("acctType", acctType, "ACCT_NBR");
				preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Error in updateEmergencyCycleStaging");

			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		
		/**
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 * 
		 * @return errorInfo
		 */
		public static String getErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.accountBillCycleUpdateError(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
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

	}// end worker class
	
	public static final class EmergencyData_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String perIdNbr;
		private String acctType;
		private String currencyCd;

		public EmergencyData_Id(String perIdNbr, String acctType, String currencyCd) {
			setPerIdNbr(perIdNbr);
			setAcctType(acctType);
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
		
		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public String getAcctType() {
			return acctType;
		}

		public void setAcctType(String acctType) {
			this.acctType = acctType;
		}

		public String getCurrencyCd() {
			return currencyCd;
		}

		public void setCurrencyCd(String currencyCd) {
			this.currencyCd = currencyCd;
		}

	}
}