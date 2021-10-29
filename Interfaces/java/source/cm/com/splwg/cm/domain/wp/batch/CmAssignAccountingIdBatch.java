/*******************************************************************************
 * FileName                   : CmAssignAccountingIdBatch.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 09, 2016
 * Version Number             : 0.2
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1		 NA				Jun	24, 2018		Kaustubh Kale		 Initial Version
0.2		 NA			    Aug 21, 2018		Kaustubh Kale		 Added ACCT_NBR extra column (NAP-31893)
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
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.datatypes.LookupHelper;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId;
import com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO;
import com.splwg.cm.domain.wp.entity.CmFtGlFxAccountingId;
import com.splwg.cm.domain.wp.entity.CmFtGlFxAccountingId_DTO;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author RIA
 *
@BatchJob (rerunnable = false,
 *      modules = { "demo"})
 */
public class CmAssignAccountingIdBatch extends CmAssignAccountingIdBatch_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(CmAssignAccountingIdBatch.class);

	public JobWork getJobWork() {
		logger.debug("Inside getJobWork()");
		
		List<ThreadWorkUnit> threadWorkUnitList = getGlAccounts();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.debug("No of rows selected for processing are - "+ rowsForProcessing);
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	private List<ThreadWorkUnit> getGlAccounts() {
		logger.debug("Inside getGlAccounts() method");
		
		BatchControl_Id batchControlId = getGLBatchControlId();
		BigInteger batchNumber = batchControlId.getEntity().getNextBatchNumber();
		
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		AccountingIdData accountingIdData = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		String glAcct = "";
		try {
			stringBuilder.append(" SELECT DISTINCT(FTGL.GL_ACCT) ");
			stringBuilder.append(" FROM CM_FT_GL_ASL_STG FTGL, CI_FT_PROC FP ");
			stringBuilder.append(" WHERE FP.BATCH_CD =:batchCode ");
			stringBuilder.append(" AND FP.BATCH_NBR =:batchNumber ");
			stringBuilder.append(" AND FP.FT_ID = FTGL.FT_ID ");
			stringBuilder.append(" AND FTGL.ACCOUNTING_ID = ' ' ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindId("batchCode", batchControlId);
			preparedStatement.bindBigInteger("batchNumber", batchNumber);
			preparedStatement.setAutoclose(false);

			for (SQLResultRow resultSet : preparedStatement.list()) {
				glAcct = CommonUtils.CheckNull(resultSet.getString("GL_ACCT"));
				if(!(glAcct.trim().equals("")))
				{	
					accountingIdData = new AccountingIdData(glAcct);				
					threadworkUnit = new ThreadWorkUnit();
					threadworkUnit.setPrimaryId(accountingIdData);
					threadWorkUnitList.add(threadworkUnit);
					threadworkUnit = null;
					accountingIdData = null;
				}
			}
		} catch (ThreadAbortedException e) {
			logger.error("Inside getGlAccounts() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getGlAccounts() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}			
		return threadWorkUnitList;
	}

	private BatchControl_Id getGLBatchControlId() {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		String batchControl = "";
		BatchControl_Id batchControlId = null;
		try {
			stringBuilder.append(" SELECT GL_BATCH_CD FROM CI_INSTALLATION ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.setAutoclose(false);
			SQLResultRow result = preparedStatement.firstRow();
			if(notNull(result)) {
				batchControl = result.getString("GL_BATCH_CD");
				batchControlId = new BatchControl_Id(batchControl);
			}
		} catch (ThreadAbortedException e) {
			logger.error("Inside getGLBatchControlId() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getGLBatchControlId() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}			
		return batchControlId;
	}

	public Class<CmAssignAccountingIdBatchWorker> getThreadWorkerClass() {
		return CmAssignAccountingIdBatchWorker.class;
	}

	public static class CmAssignAccountingIdBatchWorker extends CmAssignAccountingIdBatchWorker_Gen {
		
		private BatchControl_Id batchControlId = null;
		private BigInteger batchNumber = BigInteger.ZERO;

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}
		
		@Override
		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException, RunAbortedException {
			batchControlId = getGLBatchControlIdFromInstallationOption();
			batchNumber = batchControlId.getEntity().getNextBatchNumber();
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());	
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			AccountingIdData accountingIdData = (AccountingIdData) unit.getPrimaryId();
			String glAccount = accountingIdData.getGlAcct();
			logger.debug("glAccount = " + glAccount);
			
			// Process List of Combination ASL Accounting IDs
			String strAslAccountingId = " ";
			try {
				stringBuilder.append(" SELECT DISTINCT B.CURRENCY_CD, B.ACCOUNTING_DT, B.COUNTERPARTY, ");
				stringBuilder.append(" B.BUSINESS_UNIT, B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, B.FT_TYPE_FLG, B.ACCT_NBR, SUM(B.AMOUNT) AS AMOUNT "); // NAP-31893
				stringBuilder.append(" FROM CM_FT_GL_ASL_STG B, CI_FT_PROC FP ");
				stringBuilder.append(" WHERE B.GL_ACCT = :glAccount ");
				stringBuilder.append(" AND FP.FT_ID=B.FT_ID ");
				stringBuilder.append(" AND FP.BATCH_CD = :batchCode ");
				stringBuilder.append(" AND FP.BATCH_NBR = :batchNumber ");
				stringBuilder.append(" GROUP BY B.CURRENCY_CD, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT, ");
				stringBuilder.append(" B.COST_CENTRE, B.INTERCOMPANY, B.GL_ACCT, B.SCHEME, B.FT_TYPE_FLG, SIGN(B.AMOUNT), B.ACCT_NBR "); // NAP-31893
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("glAccount", glAccount, "GL_ACCT");
				preparedStatement.bindId("batchCode", batchControlId);
				preparedStatement.bindBigInteger("batchNumber", batchNumber);
				preparedStatement.setAutoclose(false);

				for (SQLResultRow listOfCombAslAcctId : preparedStatement.list()) {
					String currencyCd = CommonUtils.CheckNull(listOfCombAslAcctId.getString("CURRENCY_CD"));
					String accountingDate = CommonUtils.CheckNull(listOfCombAslAcctId.getString("ACCOUNTING_DT"));
					String counterparty = CommonUtils.CheckNull(listOfCombAslAcctId.getString("COUNTERPARTY"));
					String businessUnit = CommonUtils.CheckNull(listOfCombAslAcctId.getString("BUSINESS_UNIT"));
					String costCentre = CommonUtils.CheckNull(listOfCombAslAcctId.getString("COST_CENTRE"));
					String intercompany = CommonUtils.CheckNull(listOfCombAslAcctId.getString("INTERCOMPANY"));
					String glAcct = CommonUtils.CheckNull(listOfCombAslAcctId.getString("GL_ACCT"));
					String scheme = CommonUtils.CheckNull(listOfCombAslAcctId.getString("SCHEME"));
					String ftTypeFlg = CommonUtils.CheckNull(listOfCombAslAcctId.getString("FT_TYPE_FLG"));
					String acctNbr = CommonUtils.CheckNull(listOfCombAslAcctId.getString("ACCT_NBR")); // NAP-31893
					String amount = CommonUtils.CheckNull(listOfCombAslAcctId.getString("AMOUNT"));
					
					strAslAccountingId = fetchAslAccountingId(currencyCd, accountingDate, counterparty, businessUnit, costCentre, intercompany, glAcct, scheme, ftTypeFlg, acctNbr, amount);
					if(isBlankOrNull(strAslAccountingId)) {
						CmFtGlAslAccountingId_DTO cmFtGlAslAcctIdDto = new CmFtGlAslAccountingId_DTO();
						cmFtGlAslAcctIdDto.setAccountingDate(formatDate(accountingDate));
						cmFtGlAslAcctIdDto.setBatchControl(getBatchControlId().getIdValue());
						cmFtGlAslAcctIdDto.setBatchNumber(getBatchNumber());
						cmFtGlAslAcctIdDto.setBusinessUnit(businessUnit);
						cmFtGlAslAcctIdDto.setCostCenter(costCentre);
						cmFtGlAslAcctIdDto.setCounterparty(counterparty);
						cmFtGlAslAcctIdDto.setCurrencyId(new Currency_Id(currencyCd));
						cmFtGlAslAcctIdDto.setFinancialTransactionType(LookupHelper.getLookupInstance(FinancialTransactionTypeLookup.class, ftTypeFlg));
						cmFtGlAslAcctIdDto.setGlAccount(glAcct);
						cmFtGlAslAcctIdDto.setIntercompany(intercompany);
						cmFtGlAslAcctIdDto.setScheme(scheme);
						cmFtGlAslAcctIdDto.setAccountNumber(acctNbr); // NAP-31893
						cmFtGlAslAcctIdDto.setAmount(new Money(amount, new Currency_Id(currencyCd)));
						CmFtGlAslAccountingId aslAccountingIdEntity = cmFtGlAslAcctIdDto.newEntity();
						strAslAccountingId = aslAccountingIdEntity.getId().getIdValue();
					}
					updateAslStgAccountingId(strAslAccountingId, currencyCd, accountingDate, counterparty, businessUnit, costCentre, intercompany, glAcct, scheme, ftTypeFlg, acctNbr, amount);
				}
			} catch (ThreadAbortedException e) {
				logger.error("Error selecting FT GL ASL Records -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error selecting FT GL ASL Records -", e);
			} 
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			// Process List of Combination FX Accounting IDs
			String strFxAccountingId = " ";
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append(" SELECT DISTINCT B.FUND_CURRENCY, B.BIN_SETTLE_CURRENCY, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT ");
				stringBuilder.append(" FROM CM_FT_GL_FX_STG B, CI_FT_PROC FP ");
				stringBuilder.append(" WHERE B.GL_ACCT = :glAccount ");
				stringBuilder.append(" AND FP.FT_ID = B.FT_ID ");
				stringBuilder.append(" AND FP.BATCH_CD = :batchCode ");
				stringBuilder.append(" AND FP.BATCH_NBR = :batchNumber ");
				stringBuilder.append(" GROUP BY B.FUND_CURRENCY, B.BIN_SETTLE_CURRENCY, B.ACCOUNTING_DT, B.COUNTERPARTY, B.BUSINESS_UNIT ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("glAccount", glAccount, "");
				preparedStatement.bindId("batchCode", batchControlId);
				preparedStatement.bindBigInteger("batchNumber", batchNumber);
				preparedStatement.setAutoclose(false);

				for (SQLResultRow listOfCombFxAcctId : preparedStatement.list()) {
					String fundCurrency = CommonUtils.CheckNull(listOfCombFxAcctId.getString("FUND_CURRENCY"));
					String binSettleCurrency = CommonUtils.CheckNull(listOfCombFxAcctId.getString("BIN_SETTLE_CURRENCY"));
					String accountingDate = CommonUtils.CheckNull(listOfCombFxAcctId.getString("ACCOUNTING_DT"));
					String counterparty = CommonUtils.CheckNull(listOfCombFxAcctId.getString("COUNTERPARTY"));
					String businessUnit = CommonUtils.CheckNull(listOfCombFxAcctId.getString("BUSINESS_UNIT"));
					
					strFxAccountingId = fetchFxAccountingId(fundCurrency, binSettleCurrency, accountingDate, counterparty, businessUnit, glAccount);
					if(isBlankOrNull(strFxAccountingId)) {
						CmFtGlFxAccountingId_DTO cmFtGlFxAcctIdDto = new CmFtGlFxAccountingId_DTO();
						cmFtGlFxAcctIdDto.setAccountingDate(formatDate(accountingDate));
						cmFtGlFxAcctIdDto.setBatchControl(getBatchControlId().getIdValue());
						cmFtGlFxAcctIdDto.setBatchNumber(getBatchNumber());
						cmFtGlFxAcctIdDto.setBinSettlementCurrency(binSettleCurrency);
						cmFtGlFxAcctIdDto.setBusinessUnit(businessUnit);
						cmFtGlFxAcctIdDto.setCounterparty(counterparty);
						cmFtGlFxAcctIdDto.setFundCurrency(fundCurrency);
						cmFtGlFxAcctIdDto.setGlAccount(glAccount);
						CmFtGlFxAccountingId fxAccountingIdEntity = cmFtGlFxAcctIdDto.newEntity();
						strFxAccountingId = fxAccountingIdEntity.getId().getIdValue();
					}
					updateFxStgAccountingId(strFxAccountingId, fundCurrency, binSettleCurrency, accountingDate, counterparty, businessUnit, glAccount);
				}
			} catch (ThreadAbortedException e) {
				logger.error("Error selecting FT GL FX Records -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error selecting FT GL FX Records -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return true;
		}

		/**
		 * Fetch Accounting ID from CM_FT_GL_ASL_ACCTING_ID table
		 * @param currencyCd
		 * @param accountingDate
		 * @param counterparty
		 * @param businessUnit
		 * @param costCentre
		 * @param intercompany
		 * @param glAcct
		 * @param scheme
		 * @param ftTypeFlg
		 * @param acctNbr 
		 * @param amount 
		 * @return accountingId
		 */
		private String fetchAslAccountingId(String currencyCd, String accountingDate, String counterparty, String businessUnit, 
				String costCentre, String intercompany, String glAcct, String scheme, String ftTypeFlg, String acctNbr, String amount) {
			logger.debug("Inside fetchAslAccountingId() method");
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String accountingId = null;
			boolean amountSign = new Money(amount, new Currency_Id(currencyCd)).isPositive();
			boolean zeroSign = new Money(amount, new Currency_Id(currencyCd)).isZero();
			
			try {
				stringBuilder.append(" SELECT ACCOUNTING_ID FROM CM_FT_GL_ASL_ACCTING_ID ");
				stringBuilder.append(" WHERE ACCOUNTING_DT = :accountingDate ");
				stringBuilder.append(" AND BATCH_CD =:batchCode ");
				stringBuilder.append(" AND BATCH_NBR =:batchNumber ");
				stringBuilder.append(" AND BUSINESS_UNIT =:businessUnit ");
				stringBuilder.append(" AND COST_CENTRE =:costCentre ");
				stringBuilder.append(" AND COUNTERPARTY =:counterparty ");
				stringBuilder.append(" AND CURRENCY_CD =:currencyCd ");
				stringBuilder.append(" AND FT_TYPE_FLG =:ftTypeFlg ");
				stringBuilder.append(" AND GL_ACCT =:glAcct ");
				stringBuilder.append(" AND INTERCOMPANY =:intercompany ");
				stringBuilder.append(" AND SCHEME =:scheme ");
				stringBuilder.append(" AND ACCT_NBR =:acctNbr "); // NAP-31893
				if(amountSign && !zeroSign)
					stringBuilder.append(" AND AMOUNT > 0 ");
				else if(!amountSign && zeroSign)
					stringBuilder.append(" AND AMOUNT = 0 ");
				else
					stringBuilder.append(" AND AMOUNT < 0 ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingDate", accountingDate, "");
				preparedStatement.bindId("batchCode", getBatchControlId());
				preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
				preparedStatement.bindString("businessUnit", businessUnit, "");
				preparedStatement.bindString("costCentre", costCentre, "");
				preparedStatement.bindString("counterparty", counterparty, "");
				preparedStatement.bindString("currencyCd", currencyCd, "");
				preparedStatement.bindString("ftTypeFlg", ftTypeFlg, "");
				preparedStatement.bindString("glAcct", glAcct, "");
				preparedStatement.bindString("intercompany", intercompany, "");
				preparedStatement.bindString("scheme", scheme, "");
				preparedStatement.bindString("acctNbr", acctNbr, ""); // NAP-31893
				preparedStatement.setAutoclose(false);
				
				SQLResultRow result = preparedStatement.firstRow();
				if(notNull(result)) {
					accountingId = result.getString("ACCOUNTING_ID");
				}
			} catch (ThreadAbortedException e) {
				logger.error("Error selecting FT GL ASL Accounting ID Records -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error selecting FT GL ASL Accounting ID Records -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return accountingId;
		}
		
		/**
		 * Update ACCOUNTING_ID in CM_FT_GL_ASL_STG table
		 * @param strAslAccountingId
		 * @param currencyCd
		 * @param accountingDate
		 * @param counterparty
		 * @param businessUnit
		 * @param costCentre
		 * @param intercompany
		 * @param glAcct
		 * @param scheme
		 * @param ftTypeFlg
		 * @param acctNbr 
		 * @param amount 
		 */
		private void updateAslStgAccountingId(String strAslAccountingId, String currencyCd, String accountingDate, String counterparty,
				String businessUnit, String costCentre, String intercompany, String glAcct, String scheme, String ftTypeFlg, String acctNbr, String amount) {
			logger.debug("Inside updateAslStgAccountingId() method");
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			boolean amountSign = new Money(amount, new Currency_Id(currencyCd)).isPositive();
			boolean zeroSign = new Money(amount, new Currency_Id(currencyCd)).isZero();
			
			try {
				stringBuilder.append(" UPDATE CM_FT_GL_ASL_STG B "); 
				stringBuilder.append(" SET B.ACCOUNTING_ID =:accountingId ");
				stringBuilder.append(" WHERE B.CURRENCY_CD =:currencyCd ");
				stringBuilder.append(" AND B.ACCOUNTING_DT =:accountingDate ");
				stringBuilder.append(" AND B.COUNTERPARTY =:counterparty ");
				stringBuilder.append(" AND B.BUSINESS_UNIT =:businessUnit ");
				stringBuilder.append(" AND B.COST_CENTRE =:costCenter ");
				stringBuilder.append(" AND B.INTERCOMPANY =:intercomapny ");
				stringBuilder.append(" AND B.GL_ACCT =:glAcct ");
				stringBuilder.append(" AND B.SCHEME =:scheme ");
				stringBuilder.append(" AND B.FT_TYPE_FLG =:ftTypeFlg ");
				stringBuilder.append(" AND B.ACCT_NBR =:acctNbr "); // NAP-31893
				if(amountSign && !zeroSign)
					stringBuilder.append(" AND B.AMOUNT > 0 ");
				else if(!amountSign && zeroSign)
					stringBuilder.append(" AND B.AMOUNT = 0 ");
				else
					stringBuilder.append(" AND B.AMOUNT < 0 ");
				stringBuilder.append(" AND B.ACCOUNTING_ID = ' ' ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingId", strAslAccountingId, "");
				preparedStatement.bindString("currencyCd", currencyCd, "");
				preparedStatement.bindString("accountingDate", accountingDate, "");
				preparedStatement.bindString("counterparty", counterparty, "");
				preparedStatement.bindString("businessUnit", businessUnit, "");
				preparedStatement.bindString("costCenter", costCentre, "");
				preparedStatement.bindString("intercomapny", intercompany, "");
				preparedStatement.bindString("glAcct", glAcct, "");
				preparedStatement.bindString("scheme", scheme, "");				
				preparedStatement.bindString("ftTypeFlg", ftTypeFlg, "");
				preparedStatement.bindString("acctNbr", acctNbr, ""); // NAP-31893
				
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Error updating CM_FT_GL_ASL_STG Record -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error updating CM_FT_GL_ASL_STG Record -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			commit();
		}
		
		/**
		 * Fetch Accounting ID from CM_FT_GL_FX_ACCTING_ID table
		 * @param fundCurrency
		 * @param binSettleCurrency
		 * @param accountingDate
		 * @param counterparty
		 * @param businessUnit
		 * @return
		 */
		private String fetchFxAccountingId(String fundCurrency,	String binSettleCurrency, String accountingDate, 
				String counterparty, String businessUnit, String glAcct) {
			logger.debug("Inside fetchFxAccountingId() method");
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String accountingId = null;
			
			try {
				stringBuilder.append(" SELECT ACCOUNTING_ID FROM CM_FT_GL_FX_ACCTING_ID ");
				stringBuilder.append(" WHERE ACCOUNTING_DT = :accountingDate ");
				stringBuilder.append(" AND BATCH_CD =:batchCode ");
				stringBuilder.append(" AND BATCH_NBR =:batchNumber ");
				stringBuilder.append(" AND BIN_SETTLE_CURRENCY =:binSettleCurrency ");
				stringBuilder.append(" AND BUSINESS_UNIT =:businessUnit ");
				stringBuilder.append(" AND COUNTERPARTY =:counterparty ");
				stringBuilder.append(" AND FUND_CURRENCY =:fundCurrency ");
				stringBuilder.append(" AND GL_ACCT =:glAcct ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingDate", accountingDate, "");
				preparedStatement.bindId("batchCode", getBatchControlId());
				preparedStatement.bindBigInteger("batchNumber", getBatchNumber());
				preparedStatement.bindString("binSettleCurrency", binSettleCurrency, "");
				preparedStatement.bindString("businessUnit", businessUnit, "");
				preparedStatement.bindString("counterparty", counterparty, "");
				preparedStatement.bindString("fundCurrency", fundCurrency, "");
				preparedStatement.bindString("glAcct", glAcct, "");
				preparedStatement.setAutoclose(false);
				
				SQLResultRow result = preparedStatement.firstRow();
				if(notNull(result)) {
					accountingId = result.getString("ACCOUNTING_ID");
				}
			} catch (ThreadAbortedException e) {
				logger.error("Error selecting FT GL FX Accounting ID Records -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error selecting FT GL FX Accounting ID Records -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return accountingId;
		}

		/**
		 * Update ACCOUNTING_ID in CM_FT_GL_FX_STG table
		 * @param fundCurrency
		 * @param binSettleCurrency
		 * @param accountingDate
		 * @param counterparty
		 * @param businessUnit
		 * @param glAccount
		 */
		private void updateFxStgAccountingId(String strFxAccountingId, String fundCurrency, String binSettleCurrency, 
				String accountingDate, String counterparty, String businessUnit, String glAcct) {
			logger.debug("Inside updateFxStgAccountingId() method");
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append(" UPDATE CM_FT_GL_FX_STG B "); 
				stringBuilder.append(" SET B.ACCOUNTING_ID =:accountingId ");
				stringBuilder.append(" WHERE B.ACCOUNTING_DT =:accountingDate ");
				stringBuilder.append(" AND B.BIN_SETTLE_CURRENCY =:binSettleCurrency ");
				stringBuilder.append(" AND B.BUSINESS_UNIT =:businessUnit ");
				stringBuilder.append(" AND B.COUNTERPARTY =:counterparty ");
				stringBuilder.append(" AND B.FUND_CURRENCY =:fundCurrency ");
				stringBuilder.append(" AND B.GL_ACCT =:glAcct ");
				stringBuilder.append(" AND B.ACCOUNTING_ID = ' ' ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("accountingId", strFxAccountingId, "");
				preparedStatement.bindString("accountingDate", accountingDate, "");
				preparedStatement.bindString("binSettleCurrency", binSettleCurrency, "");
				preparedStatement.bindString("businessUnit", businessUnit, "");
				preparedStatement.bindString("counterparty", counterparty, "");
				preparedStatement.bindString("fundCurrency", fundCurrency, "");
				preparedStatement.bindString("glAcct", glAcct, "");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Error updating CM_FT_GL_FX_STG Record -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Error updating CM_FT_GL_FX_STG Record -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			commit();
		}
		
		/**
		 * Commit Data
		 */
		private void commit() {
			PreparedStatement ps = null;
			try {
				ps = createPreparedStatement("commit","");
				ps.execute();
			} catch (RuntimeException e) {
				logger.error("Inside commit() method, Error commiting data -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} finally {
				if (ps != null) {
					ps.close();
					ps = null;
				}
			}
		}
		
		/**
		 * Format Date to yyyy-MM-dd
		 * @param strDate 
		 */
		private Date formatDate(String strDate) {
			DateFormat formatter = new DateFormat("yyyy-MM-dd-HH.mm.ss");
			Date date = null;
			try {
				date = formatter.parseDate(strDate);
			} catch (DateFormatParseException e) {
				logger.error("Error formatting date ",e);
			}
			return date;
		}
		
		/**
		 * Get GL Batch Control Id from Installation Option
		 * @return batchControlId
		 */
		private BatchControl_Id getGLBatchControlIdFromInstallationOption() {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String batchControl = "";
			BatchControl_Id batchControlId = null;
			try {
				stringBuilder.append(" SELECT GL_BATCH_CD FROM CI_INSTALLATION ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.setAutoclose(false);
				SQLResultRow result = preparedStatement.firstRow();
				if(notNull(result)) {
					batchControl = result.getString("GL_BATCH_CD");
					batchControlId = new BatchControl_Id(batchControl);
				}
			} catch (ThreadAbortedException e) {
				logger.error("Inside getGLBatchControlId() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside getGLBatchControlId() method, Error -", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}			
			return batchControlId;
		}
	}
	
	public static final class AccountingIdData implements Id {

		private static final long serialVersionUID = 1L;

		private String glAcct;

		public AccountingIdData(String glAcct) {
			setGlAcct(glAcct);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getGlAcct() {
			return glAcct;
		}

		public void setGlAcct(String glAcct) {
			this.glAcct = glAcct;
		}
	}
}
