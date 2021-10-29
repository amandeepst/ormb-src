/*******************************************************************************
 * FileName                   : InboundAccountHierarchy.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 1.2
 * Revision History     		 :
VerNum | ChangeReqNum | Date Modification | Author Name          | Nature of Change
0.1      NA               Mar 24, 2015      Abhishek Paliwal      Implemented all requirements for CD1.
0.2      NA               Jun 29, 2015      Preeti Tiwari         Defect PAM-2107 fix.
0.3      NA               Sep 21, 2015      Abhishek Paliwal      Bo replaced by DTO
0.4      NA               Oct 02, 2015      Sunaina               Changed execution strategy to Standard Commit Strategy
0.5      NA               Apr 6,  2016      Sunaina               Updated as per Oracle Code review.
0.6      NA               Apr 13, 2016      Preeti Tiwari         Removed check for Merchant Hierarchy.
0.7      NA               Apr 22, 2016      Preeti Tiwari         Updated to set Pending status in execute work unit.
0.8      NA               May 04, 2016      Preeti Tiwari         Updated to fix SQL warnings.
0.9      NA               Jun 07, 2017      Ankur/Gaurav          NAP-14404 fix.
1.0      NA               Feb 05, 2018      Preeti Tiwari         NAP-22634 Remove creation of account hierarchy for reserve contracts on Funding.
1.1		 NAP-24086		  Mar 16, 2018		Rakesh Ranjan		  NAP-24086 Included ILM_ARCH_SW to be updated to Y for completed status.
1.2		 NA		          Sep 07, 2018		Swapnil		          NAP-32105 Fix
1.3      NA               Dec 05, 2018      RIA                   NAP-36897
1.4		 NAP-42018		  Mar 27, 2019		Somya Sikka			  Performance fixes and removal of redundant code
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

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
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Venkat/Abhishek Paliwal
 * 
 * @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules={"demo"},
 *      softParameters = { @BatchJobSoftParameter (name = hierarchyType, type = string)
 *      , @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 *      
 */
public class InboundAccountHierarchyInterface extends
InboundAccountHierarchyInterface_Gen {
	
	InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();


	public static final Logger logger = LoggerFactory
			.getLogger(InboundAccountHierarchyInterface.class);

	/**
	 * getJobWork() method selects data for processing by Inbound Account
	 * Hierarchy Interface. The source of data is selected from CM_INV_GRP_STG
	 * table and then passed to the Worker inner class by framework.
	 */
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork() method");
		
		inboundAccountHierarchyLookUps.setLookUpConstants();
		List<ThreadWorkUnit> threadWorkUnitList = getAcctHierPerIdNbrsData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "
				+ threadWorkUnitList.size());
		inboundAccountHierarchyLookUps = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// *********************** getAcctHierPerIdNbrsData Method******************************

	/**
	 * getAcctHierPerIdNbrsData() method selects distinct sets of PER_ID_NBR and PER_ID_NBR2 from CM_INV_GRP_STG staging table.
	 * 
	 * @return List AcctHierPerIdNbrs_Id
	 */
	private List<ThreadWorkUnit> getAcctHierPerIdNbrsData() {
		PreparedStatement preparedStatement = null;
		AcctHierPerIdNbrs_Id accthierperidnbrsId = null;
		StringBuilder stringBuilder = new StringBuilder();
		List<AcctHierPerIdNbrs_Id> rowsForProcessingList = new ArrayList<AcctHierPerIdNbrs_Id>();
		String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
		String paramHierarchyType = CommonUtils.CheckNull(getParameters().getHierarchyType()).trim();
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();

		try {
			stringBuilder.append("SELECT DISTINCT STG.PER_ID_NBR2,STG.CIS_DIVISION FROM CM_INV_GRP_STG STG ");
			stringBuilder.append( " WHERE STG.BO_STATUS_CD = :selectBoStatus1");

			if (notBlank(txnSourceCode)) {
				stringBuilder.append(" AND STG.TXN_SOURCE_CD= :txnSourceCode ");
			}
			if(notBlank(paramHierarchyType)) {
				stringBuilder.append(" AND STG.HIER_TYPE=:hierarchyType ");
			}
//			stringBuilder.append(" AND NOT EXISTS(SELECT 1 FROM CM_INV_GRP_END_STG WHERE BO_STATUS_CD= :selectBoStatus1 ");
//			stringBuilder.append(" AND PER_ID_NBR=STG.PER_ID_NBR2 AND CIS_DIVISION=STG.CIS_DIVISION AND ACCT_TYPE=STG.HIER_TYPE ");
//			stringBuilder.append(" AND CURRENCY_CD=STG.CURRENCY_CD) ");
			
			preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
			preparedStatement.bindString("selectBoStatus1", inboundAccountHierarchyLookUps.getUpload(), "BO_STATUS_CD");
			if (notBlank(txnSourceCode)) {
				preparedStatement.bindString("txnSourceCode", txnSourceCode, "TXN_SOURCE_CD");
			}
			if (notBlank(paramHierarchyType)) {
				preparedStatement.bindString("hierarchyType", paramHierarchyType, "HIER_TYPE");
			}
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {

				String childPerIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR2"));
				String division = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));
				accthierperidnbrsId = new AcctHierPerIdNbrs_Id(childPerIdNbr, division);
				rowsForProcessingList.add(accthierperidnbrsId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(accthierperidnbrsId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				accthierperidnbrsId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getAcctHierPerIdNbrsData() ", e);
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

	public Class<InboundAccountHierarchyInterfaceWorker> getThreadWorkerClass() {
		return InboundAccountHierarchyInterfaceWorker.class;
	}

	public static class InboundAccountHierarchyInterfaceWorker extends
	InboundAccountHierarchyInterfaceWorker_Gen {
		private ArrayList<ArrayList<String>> updateAccountHierarchyStatusList = new ArrayList<ArrayList<String>>();

		private ArrayList<String> eachCustomerStatusList = null;

		private CmAccountHierarchyGenerator inboundAccountHierarchyGenerator = null;

		private InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();
		
		private String paramTxnSourceCode=null;
		
		private String paramHierarchyType=null;
		
		//private String perIdTypeCd=null;
		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			inboundAccountHierarchyLookUps.setLookUpConstants();
			inboundAccountHierarchyGenerator = new CmAccountHierarchyGenerator(inboundAccountHierarchyLookUps);
			paramTxnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
			paramHierarchyType = CommonUtils.CheckNull(getParameters().getHierarchyType()).trim();
			//perIdTypeCd = inboundAccountHierarchyLookUps.getIdTypeCd();

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
		 * every row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to create / update
		 * account hierarchy records.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			AcctHierPerIdNbrs_Id acctHierPerIdNbrsId = (AcctHierPerIdNbrs_Id) unit.getPrimaryId();
			List<InboundAccountHierarchyInterface_Id> inboundAccountHierarchyInterfaceId = getAccountHierarchyStagingData(acctHierPerIdNbrsId);
			String personIdNumber = "";
			String personIdNumber2 = "";
			String cisDivision = "";
			String personId1 = "";
			String personId2 = "";
			SQLResultRow resultRowSet = null;
			String currencyCodeOfChildPerson = "";
			String hierarchyType = "";
			String masterAccountId="";
			String memberAccountId="";
			Bool isParentPersonExists=Bool.TRUE;
			
			PreparedStatement preparedStatement = null;
			Map<String,SQLResultRow> masterAcctMap=new HashMap<String,SQLResultRow>();
			Map<String,SQLResultRow> memberAcctMap=new HashMap<String,SQLResultRow>();
			
			// *****************************************************
			// Determining Parent Person
			// *****************************************************

			personIdNumber2 = acctHierPerIdNbrsId.getPerIdNbr();
			cisDivision = acctHierPerIdNbrsId.getCisDivision();
			for (int i = 0; i < inboundAccountHierarchyInterfaceId.size(); i++) {
				try {					
					Bool SAExists = doesActiveSAexists(inboundAccountHierarchyInterfaceId.get(i));
					hierarchyType = inboundAccountHierarchyInterfaceId.get(i).getHierarchType();
					
					removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));

					setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
					
					Date endDate=inboundAccountHierarchyInterfaceId.get(i).getEndDate();

					if(isNull(endDate) && SAExists.isTrue() ) {
						String messageNumberLocal = inboundAccountHierarchyLookUps.getActiveSAStatus();
						String primaryKey = personIdNumber2;
						return logError(inboundAccountHierarchyInterfaceId
								.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
								.get(i).getTransactionDetailId(), primaryKey, hierarchyType,
								inboundAccountHierarchyLookUps.getErrorMessageGroup(),
								messageNumberLocal, messageNumberLocal,
								inboundAccountHierarchyLookUps.getActiveSAAlreadyExistsError());
					
					}
					else {
					setPendingStatus(inboundAccountHierarchyInterfaceId.get(i));
					personIdNumber = inboundAccountHierarchyInterfaceId.get(i).getPerIdNbr();
					currencyCodeOfChildPerson = inboundAccountHierarchyInterfaceId.get(i).getCurrencyCd();
					
					Currency_Id currency =  new Currency_Id (currencyCodeOfChildPerson);

					if (!hierarchyType
							.equalsIgnoreCase(inboundAccountHierarchyLookUps.getFunding())
							&& !hierarchyType
							.equalsIgnoreCase(inboundAccountHierarchyLookUps.getCharging())
							&& !hierarchyType
							.equalsIgnoreCase(inboundAccountHierarchyLookUps.getChargeback())
							&& !hierarchyType
							.equalsIgnoreCase(inboundAccountHierarchyLookUps.getCardReward())) {
						String messageNumberLocal = inboundAccountHierarchyLookUps.getInvalidHierarchyTypeError();
						String primaryKey = personIdNumber;
						return logError(inboundAccountHierarchyInterfaceId
								.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
								.get(i).getTransactionDetailId(), primaryKey, hierarchyType,
								inboundAccountHierarchyLookUps.getErrorMessageGroup(),
								messageNumberLocal, messageNumberLocal,
								inboundAccountHierarchyLookUps.getInvalidHierarchyTypeStatus());
					}

					if(notNull(currency.getEntity())) {

						//*****************************************************
						//Determining Person ID Number Existence
						//********************************************************

						if(notBlank(personIdNumber2) && !personIdNumber.equals(personIdNumber2)) {
							
							// *******************************************************
							// Determining Master Account
							// *************************************************************
							String masterAccountNumber = personIdNumber.concat("_" + hierarchyType + "_").concat(currency.getIdValue());
							if(notNull(masterAcctMap) && !masterAcctMap.isEmpty() && masterAcctMap.containsKey(masterAccountNumber)){
								resultRowSet = masterAcctMap.get(masterAccountNumber);
								personId1 = resultRowSet.getString("PER_ID");
								masterAccountId = resultRowSet.getString("ACCT_ID");
							}
							else{
								resultRowSet=fetchAcctPerId(personIdNumber, cisDivision, inboundAccountHierarchyInterfaceId.get(i), masterAccountNumber);
								if(notNull(resultRowSet)) {
									masterAcctMap.put(masterAccountNumber, resultRowSet);
									personId1 = resultRowSet.getString("PER_ID");
									masterAccountId = resultRowSet.getString("ACCT_ID");
								}
							}
							
							if(isBlankOrNull(masterAccountId)){
								String messageNumberLocal = inboundAccountHierarchyLookUps.getMasterAccountNotFoundError();
								String primaryKey = personIdNumber;
								
								return logError(inboundAccountHierarchyInterfaceId
										.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
										.get(i).getTransactionDetailId(), primaryKey,
										hierarchyType,
										inboundAccountHierarchyLookUps.getErrorMessageGroup(),
										messageNumberLocal, messageNumberLocal,
										inboundAccountHierarchyLookUps.getMasterAccountStatus());

							}
							if(isBlankOrNull(personId1)){
								isParentPersonExists=Bool.FALSE;
							}
							
							if(isParentPersonExists.isTrue()){
								// *******************************************************
								// Determining Member Account
								// *******************************************************
								String memberAccountNumber = personIdNumber2.concat("_" + hierarchyType + "_").concat(currency.getIdValue());
								
								if(notNull(memberAcctMap) && !memberAcctMap.isEmpty() && memberAcctMap.containsKey(memberAccountNumber))
								{
									resultRowSet = memberAcctMap.get(memberAccountNumber);
									personId2 = resultRowSet.getString("PER_ID");
									memberAccountId = resultRowSet.getString("ACCT_ID");
								}
								else{
									resultRowSet=fetchAcctPerId(personIdNumber2, cisDivision, inboundAccountHierarchyInterfaceId.get(i), memberAccountNumber);
									if(notNull(resultRowSet)) {
										memberAcctMap.put(memberAccountNumber, resultRowSet);
										personId2 = resultRowSet.getString("PER_ID");
										memberAccountId = resultRowSet.getString("ACCT_ID");
										}
								}
								if(isBlankOrNull(personId2)){
									String messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
									String primaryKey = personIdNumber2;

									return logError(inboundAccountHierarchyInterfaceId
											.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
											.get(i).getTransactionDetailId(), primaryKey,
											hierarchyType,
											inboundAccountHierarchyLookUps.getErrorMessageGroup(),
											messageNumberLocal, messageNumberLocal,
											inboundAccountHierarchyLookUps.getChildPersonStatus());
								}
								if(isBlankOrNull(memberAccountId)){
									String messageNumberLocal = inboundAccountHierarchyLookUps.getMemberAccountNotFoundError();
									String primaryKey = personIdNumber2;
									
									return logError(inboundAccountHierarchyInterfaceId
											.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
											.get(i).getTransactionDetailId(), primaryKey,
											hierarchyType,
											inboundAccountHierarchyLookUps.getErrorMessageGroup(),
											messageNumberLocal, messageNumberLocal,
											inboundAccountHierarchyLookUps.getMemberAccountStatus());
								}	
								

							}
							else{
								String messageNumberLocal = inboundAccountHierarchyLookUps.getPersonNotFoundError();
								String primaryKey = personIdNumber;

								return logError(inboundAccountHierarchyInterfaceId
										.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
										.get(i).getTransactionDetailId(), primaryKey,
										hierarchyType,
										inboundAccountHierarchyLookUps.getErrorMessageGroup(),
										messageNumberLocal, messageNumberLocal,
										inboundAccountHierarchyLookUps.getParentPersonStatus());
							}
							
														
							// *****************************************************
							// Verifying Person Hierarchy
							// *****************************************************
							if(!personId1.equals(personId2)) {

								// ****************************************************
								// Account Hierarchy
								// ********************************************************

									List<String> dataFromGenrator=new ArrayList<String>();
									dataFromGenrator.addAll(inboundAccountHierarchyGenerator.inboundAccountHierarchy(inboundAccountHierarchyInterfaceId.get(i), masterAccountId, memberAccountId));
									if(dataFromGenrator.get(8).toString().trim().equals("false")){
										return logError(
												dataFromGenrator.get(0).toString().trim(),
												dataFromGenrator.get(1).toString().trim(),
												dataFromGenrator.get(2).toString().trim(),
												dataFromGenrator.get(3).toString().trim(),
												dataFromGenrator.get(4).toString().trim(),
												dataFromGenrator.get(5).toString().trim(),
												dataFromGenrator.get(6).toString().trim(),
												dataFromGenrator.get(7).toString().trim());
									}
								
								// Update status of row after successful completion
								updateAccountHierarchyStagingStatus(
										inboundAccountHierarchyInterfaceId
										.get(i).getTransactionHeaderId(),inboundAccountHierarchyInterfaceId
										.get(i).getTransactionDetailId(),
										inboundAccountHierarchyLookUps.getCompleted(), "0", "0", " ");
							}
						}
					} else {
						String messageNumberLocal = inboundAccountHierarchyLookUps.getInvalidCurrencyCodeForChildPersonError();
						String primaryKey = personIdNumber;
						return logError(inboundAccountHierarchyInterfaceId
								.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
								.get(i).getTransactionDetailId(), primaryKey, hierarchyType,
								inboundAccountHierarchyLookUps.getErrorMessageGroup(),
								messageNumberLocal, messageNumberLocal,
								inboundAccountHierarchyLookUps.getInvalidCurrencyCodeForChildPersonStatus());
					}

					if(personIdNumber.equals(personIdNumber2)) {
						String messageNumberLocal = inboundAccountHierarchyLookUps.getParentAndChildPersonsAreSameError();
						String primaryKey = personIdNumber;
						return logError(inboundAccountHierarchyInterfaceId
								.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
								.get(i).getTransactionDetailId(), primaryKey, hierarchyType,
								inboundAccountHierarchyLookUps.getErrorMessageGroup(), 
								messageNumberLocal, messageNumberLocal,
								inboundAccountHierarchyLookUps.getSamePersonsStatus());
					}
					if(personId1.equals(personId2)) {
						String messageNumberLocal = inboundAccountHierarchyLookUps.getParentAndChildPersonsAreSameError();
						String primaryKey = personIdNumber;
						return logError(inboundAccountHierarchyInterfaceId
								.get(i).getTransactionHeaderId(), inboundAccountHierarchyInterfaceId
								.get(i).getTransactionDetailId(), primaryKey, hierarchyType,
								inboundAccountHierarchyLookUps.getErrorMessageGroup(),
								messageNumberLocal, messageNumberLocal,
								inboundAccountHierarchyLookUps.getSamePersonsStatus());
					}
				}
				}catch (RuntimeException e) {
					logger.error("Inside catch block of AccounthIerarchy-", e);
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}

			return true;
		}

			private SQLResultRow fetchAcctPerId(String perIdNbr, String division, InboundAccountHierarchyInterface_Id inboundAccountHierarchyId, String accountNumber){
			StringBuilder stringBuilder= null;
			PreparedStatement preparedStatement = null;	
			String currencyCd = inboundAccountHierarchyId.getCurrencyCd();
			SQLResultRow resultRow = null;
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append(" SELECT A.ACCT_ID, C.PER_ID FROM CI_ACCT A, CI_ACCT_NBR B, CI_ACCT_PER C, CI_PER P                    ");
				stringBuilder.append(" WHERE A.ACCT_ID = B.ACCT_ID AND B.ACCT_ID = C.ACCT_ID AND P.PER_ID =C.PER_ID                         ");
				stringBuilder.append(" AND B.ACCT_NBR_TYPE_CD = :extAcctIdentifier AND B.ACCT_NBR=:accountNumber 						 	");
				stringBuilder.append(" AND P.CIS_DIVISION = A.CIS_DIVISION AND A.CIS_DIVISION =:cisDivision AND A.CURRENCY_CD =:currencyCd  ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				//preparedStatement.bindString("personIdNbr", perIdNbr, "PER_ID_NBR");
				preparedStatement.bindString("cisDivision", division, "CIS_DIVISION");
				preparedStatement.bindString("extAcctIdentifier", inboundAccountHierarchyLookUps.getExternalAccountIdentifier(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("accountNumber", accountNumber, "ACCT_NBR");
				preparedStatement.bindString("currencyCd", currencyCd, "CURRENCY_CD");
				preparedStatement.setAutoclose(false);
				resultRow = preparedStatement.firstRow();
				
			} catch (Exception e) {
				logger.error("Inside catch block -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return resultRow;
		}

		protected final void removeSavepoint(String savePointName)
		{
			FrameworkSession session = (FrameworkSession)SessionHolder.getSession();
			if (session.hasActiveSavepointWithName(savePointName)) {
				session.removeSavepoint(savePointName);
			}
		}
		private Bool doesActiveSAexists(InboundAccountHierarchyInterface_Id inboundAccountHierarchyInterface_Id) {
		
			StringBuilder stringBuilder= null;
			PreparedStatement preparedStatement = null;	
			SQLResultRow resultRow = null;
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append(" SELECT 1 FROM CI_SA S, CI_SA S1, CI_SA_CHAR C, CI_ACCT_PER AP, CI_PER_ID P, ");
				stringBuilder.append(" CI_ACCT_PER AP1, CI_PER_ID P1 WHERE S.ACCT_ID=AP.ACCT_ID AND C.CHAR_TYPE_CD=:fkRef ");
				stringBuilder.append(" AND AP.PER_ID=P.PER_ID AND C.SRCH_CHAR_VAL = S.SA_ID AND C.SA_ID=S1.SA_ID  ");
				stringBuilder.append(" AND S1.SA_STATUS_FLG IN (:pendStop, :active) AND S1.ACCT_ID=AP1.ACCT_ID AND S.CURRENCY_CD =:currencyCd and S.CIS_DIVISION =:cisDiv ");
				stringBuilder.append(" AND AP1.PER_ID =P1.PER_ID AND P1.PER_ID_NBR<>:perIdNbr1 AND P1.ID_TYPE_CD=:exprtyId AND S1.SA_TYPE_CD=:hierFlg AND ");
				stringBuilder.append(" (S1.END_DT >:startDt OR S1.END_DT IS NULL) AND P.PER_ID_NBR=:perIdNbr2 AND P.ID_TYPE_CD=:exprtyId  ");
				stringBuilder.append(" AND EXISTS (SELECT 1 FROM CI_ACCT_NBR WHERE ACCT_ID = S.ACCT_ID AND ACCT_NBR_TYPE_CD = :extAcctIdentifier)  ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"doesActiveSAexists");
				preparedStatement.bindString("pendStop", inboundAccountHierarchyLookUps.getPendingStop(), "SA_STATUS_FLG");
				preparedStatement.bindString("active", inboundAccountHierarchyLookUps.getActive(), "SA_STATUS_FLG");
				preparedStatement.bindDate("startDt", inboundAccountHierarchyInterface_Id.getStartDate());
				preparedStatement.bindString("fkRef", inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(), "CHAR_TYPE_CD");
				preparedStatement.bindString("perIdNbr2", inboundAccountHierarchyInterface_Id.getPerIdNbr2(), "PER_ID_NBR");
				preparedStatement.bindString("currencyCd", inboundAccountHierarchyInterface_Id.getCurrencyCd(), "CURRENCY_CD");
				preparedStatement.bindString("cisDiv", inboundAccountHierarchyInterface_Id.getCisDivision(), "CIS_DIVISION");
				preparedStatement.bindString("perIdNbr1", inboundAccountHierarchyInterface_Id.getPerIdNbr(), "PER_ID_NBR");
				preparedStatement.bindString("hierFlg", inboundAccountHierarchyInterface_Id.getHierarchType(), "SA_TYPE_CD");
				preparedStatement.bindString("exprtyId", inboundAccountHierarchyLookUps.getIdTypeCd(), "ID_TYPE_CD");
				preparedStatement.bindString("extAcctIdentifier", inboundAccountHierarchyLookUps.getExternalAccountIdentifier(), "ACCT_NBR_TYPE_CD");
					
				preparedStatement.setAutoclose(false);
				resultRow = preparedStatement.firstRow(); 
				if(notNull(resultRow)) {
					return Bool.TRUE;
				}
				
			} catch (Exception e) {
				logger.error("Inside catch block -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		
			return Bool.FALSE;
		}

		protected final void setSavePoint(String savePointName){
			// Create save point before any change is done for the current transaction.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.setSavepoint(savePointName);
		}

		protected final void rollbackToSavePoint(String savePointName){
			// In case error occurs, rollback all changes for the current transaction and log error.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}
	
		private List<InboundAccountHierarchyInterface_Id> getAccountHierarchyStagingData(AcctHierPerIdNbrs_Id acctHierPerIdNbrsId) {
			PreparedStatement preparedStatement = null;			
			InboundAccountHierarchyInterface_Id inboundAccountHierarchyId = null;
			List<InboundAccountHierarchyInterface_Id> stagingList = new ArrayList<InboundAccountHierarchyInterface_Id>();
			StringBuilder stringBuilder= null;
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append(" SELECT STG.TXN_HEADER_ID, STG.TXN_DETAIL_ID, STG.HIER_TYPE, STG.PER_ID_NBR, STG.CURRENCY_CD, ");
				stringBuilder.append(" STG.START_DT, STG.END_DT FROM CM_INV_GRP_STG STG WHERE STG.BO_STATUS_CD=:selectBoStatus1 " );
				stringBuilder.append(" AND (STG.START_DT<=:currDate OR STG.START_DT IS NULL) ");
				stringBuilder.append(" AND STG.PER_ID_NBR2 = :personIdNbr AND STG.CIS_DIVISION=:division " );
				
				if(notBlank(paramHierarchyType)) {
					stringBuilder.append(" AND STG.HIER_TYPE=:hierarchyType ");
				}
				if (notBlank(paramTxnSourceCode)) {
					stringBuilder.append(" AND STG.TXN_SOURCE_CD= :txnSourceCode " );
				} 
				stringBuilder.append(" AND NOT EXISTS(SELECT 1 FROM CM_INV_GRP_END_STG WHERE BO_STATUS_CD= :selectBoStatus1 ");
				stringBuilder.append(" AND PER_ID_NBR=STG.PER_ID_NBR2 AND CIS_DIVISION=STG.CIS_DIVISION AND ACCT_TYPE=STG.HIER_TYPE ");
				stringBuilder.append(" AND CURRENCY_CD=STG.CURRENCY_CD) ");
				
				stringBuilder.append( " ORDER BY TO_NUMBER(STG.TXN_HEADER_ID), STG.TXN_DETAIL_ID");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus1",inboundAccountHierarchyLookUps.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindDate("currDate", getSystemDateTime().getDate());
				preparedStatement.bindString("personIdNbr", acctHierPerIdNbrsId.getPerIdNbr(), "PER_ID_NBR2");
				preparedStatement.bindString("division", acctHierPerIdNbrsId.getCisDivision(), "CIS_DIVISION");
				if(notBlank(paramHierarchyType)) {
					preparedStatement.bindString("hierarchyType", paramHierarchyType, "HIER_TYPE");
				}
				if (notBlank(paramTxnSourceCode)) {
					preparedStatement.bindString("txnSourceCode",paramTxnSourceCode, "TXN_SOURCE_CD");
				}
				preparedStatement.setAutoclose(false);

				for (SQLResultRow resultSet : preparedStatement.list()) {
					String transactionHeaderId = resultSet.getString("TXN_HEADER_ID");
					String transactionDetailId = String.valueOf(resultSet.getInteger("TXN_DETAIL_ID"));	
					String hierarchyType = resultSet.getString("HIER_TYPE");
					String perIdNbr1 = resultSet.getString("PER_ID_NBR");
					String currencyCode = resultSet.getString("CURRENCY_CD");
					Date startDate=resultSet.getDate("START_DT");
					Date endDate=resultSet.getDate("END_DT");
					inboundAccountHierarchyId = new InboundAccountHierarchyInterface_Id(
							true, transactionHeaderId, transactionDetailId,
							hierarchyType, perIdNbr1, acctHierPerIdNbrsId.getPerIdNbr(), 
							currencyCode, startDate, endDate,acctHierPerIdNbrsId.getCisDivision());
					stagingList.add(inboundAccountHierarchyId);
					resultSet = null;
					inboundAccountHierarchyId = null;
				}
			} catch (Exception e) {
				logger.error("Inside catch block of AccounthIerarchy-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			//update only future dated records with corresponding error message.
			updateFutureDatedRecords(acctHierPerIdNbrsId.getPerIdNbr());
			
			return stagingList;
		}
		
		/**
		 * update future dated records with error message.
		 * @param perIdNbr
		 */
		private void updateFutureDatedRecords(String perIdNbr){
			StringBuilder stringBuilder= null;
			PreparedStatement preparedStatement = null;	
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_INV_GRP_STG SET ERROR_INFO = 'Future dated record: Row will get processed once becomes effective.' ");
				stringBuilder.append("WHERE (START_DT>:currDate) ");
				stringBuilder.append("AND PER_ID_NBR = :personIdNbr ");				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");	
				preparedStatement.bindString("personIdNbr",perIdNbr, "PER_ID_NBR");
				preparedStatement.bindDate("currDate", getSystemDateTime().getDate());
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Inside catch block -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}	
		}
				
			/**
			 * setPendingStatus sets record being processed into Pending state.
			 * @param aInboundMerchantInterfaceId
			 */
			private void setPendingStatus(InboundAccountHierarchyInterface_Id aInboundAccountHierarchyInterfaceId) {
				PreparedStatement preparedStatement = null;
				StringBuilder stringBuilder = new StringBuilder();
				//Set records that would be processed as PENDING
				//Update only effective dated records for processing.
				try {
					stringBuilder.append("UPDATE CM_INV_GRP_STG SET BO_STATUS_CD =:status1, STATUS_UPD_DTTM = :tmStamp ");
					stringBuilder.append("WHERE BO_STATUS_CD =:status2 AND (START_DT<= :currDate) ");
					stringBuilder.append("AND TXN_HEADER_ID =:txnHeaderId AND TXN_DETAIL_ID=:txnDetailId ");				
					preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
					preparedStatement.bindString("txnHeaderId", aInboundAccountHierarchyInterfaceId.getTransactionHeaderId(), "TXN_HEADER_ID");
					preparedStatement.bindString("txnDetailId", aInboundAccountHierarchyInterfaceId.getTransactionDetailId(), "TXN_DETAIL_ID");
					preparedStatement.bindString("status1", inboundAccountHierarchyLookUps.getPending(), "BO_STATUS_CD");
					preparedStatement.bindString("status2", inboundAccountHierarchyLookUps.getUpload(), "BO_STATUS_CD");
					preparedStatement.bindDate("currDate",getSystemDateTime().getDate());
					preparedStatement.bindDateTime("tmStamp",getSystemDateTime());
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Inside catch block of getMerchantHierarchyData() method-", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}	
			}

			/**
			 * finalizeThreadWork() is execute by the batch program once per thread
			 * after processing all units.
			 */
			public void finalizeThreadWork() throws ThreadAbortedException,
			RunAbortedException {
				logger.debug("Inside finalizeThreadWork() method");
				// Logic to update erroneous records
				if (updateAccountHierarchyStatusList.size() > 0) {
					Iterator<ArrayList<String>> updateAccountStatusItr = updateAccountHierarchyStatusList
							.iterator();
					logger.debug("updateAccountStatusItr" + updateAccountStatusItr);
					updateAccountHierarchyStatusList = null;
					ArrayList<String> rowList = null;
					while (updateAccountStatusItr.hasNext()) {
						rowList = (ArrayList<String>) updateAccountStatusItr.next();
						updateAccountHierarchyStagingStatus(String.valueOf(rowList
								.get(0)), String.valueOf(rowList.get(1)), String
								.valueOf(rowList.get(2)), String.valueOf(rowList
										.get(3)), String.valueOf(rowList.get(4)), String.valueOf(rowList.get(5)));
						rowList = null;
					}
					updateAccountStatusItr = null;
				}

				super.finalizeThreadWork();
			}

			/**
			 * logError() method stores the error information in the List and does
			 * rollback all the database transaction of this unit.
			 * 
			 * @param errorMessage
			 * @param transactionHeaderId
			 * @param messageCategory
			 * @param messageNumber
			 * @return
			 */
			protected boolean logError(String transactionHeaderId, String transactionDetailId,
					String messageKey, String type, String messageCategory,
					String messageNumber, String actualErrorMessageNumber,
					String errorMessage) {
				String actualErrorMessage = errorMessage.replace("false", "")
						.replace("~Text:", "");
				String completeErrorMessage = "";

				switch (Integer.valueOf(messageNumber)) {
				case 2201:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.personNotFoundError(transactionHeaderId, messageKey,
									type, actualErrorMessage).getMessageText());
					break;
				case 2202:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.personHierarchyError(transactionHeaderId, messageKey,
									type, actualErrorMessage).getMessageText());
					break;
				case 2203:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.masterAccountValidationError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2204:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.memberAccountValidationError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2205:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.masterAccountUpdateError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2206:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.accountHierarchyCreationError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2207:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.accountHierarchyUpdateError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2208:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.masterMasterAssociationError(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2209:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.divisionIsDifferentForMasterAndMember(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2210:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.currencyIsDifferentForMasterAndMember(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2211:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.parentAndChildPersonsAreSame(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2220:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.getActiveSAAlreadyExistsError(transactionHeaderId, 
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2221:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.getAcctIsInactiveError(transactionHeaderId, 
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2212:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.divisionIsDifferentForParentAndChildPersons(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;			
				case 2214:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.currencyCodesOfMasterMerchantAndMasterAccountDonotMatch(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2215:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.currencyCodesOfChildMerchantAndMemberAccountDonotMatch(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2216:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.invalidHierarchyType(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2217:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.perIdNbrsAreSame(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				case 2219:
					completeErrorMessage = String.valueOf(CustomMessageRepository
							.invalidCurrencyCodeForChildPerson(transactionHeaderId,
									messageKey, type, actualErrorMessage)
									.getMessageText());
					break;
				default:
					logger.debug("Invalid Error message number");

				}

				eachCustomerStatusList = new ArrayList<String>();
				eachCustomerStatusList.add(0, transactionHeaderId);
				eachCustomerStatusList.add(1, transactionDetailId);
				eachCustomerStatusList.add(2, inboundAccountHierarchyLookUps.getError());
				eachCustomerStatusList.add(3, messageCategory);
				eachCustomerStatusList.add(4, messageNumber);
				eachCustomerStatusList.add(5, completeErrorMessage);
				updateAccountHierarchyStatusList.add(eachCustomerStatusList);
				eachCustomerStatusList = null;

				// Does rollback for this unit and the code exits from
				rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
				// executeWorkUnit for this unit
				switch (Integer.valueOf(messageNumber)) {
				case 2201:
					addError(CustomMessageRepository.personNotFoundError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2202:
					addError(CustomMessageRepository.personHierarchyError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2203:
					addError(CustomMessageRepository.masterAccountValidationError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2204:

					addError(CustomMessageRepository.memberAccountValidationError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2205:
					addError(CustomMessageRepository.masterAccountUpdateError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2206:
					addError(CustomMessageRepository.accountHierarchyCreationError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2207:
					addError(CustomMessageRepository.accountHierarchyUpdateError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2208:
					addError(CustomMessageRepository.masterMasterAssociationError(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2209:
					addError(CustomMessageRepository.divisionIsDifferentForMasterAndMember(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2210:
					addError(CustomMessageRepository.currencyIsDifferentForMasterAndMember(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2211:
					addError(CustomMessageRepository.parentAndChildPersonsAreSame(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2212:
					addError(CustomMessageRepository.divisionIsDifferentForParentAndChildPersons(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;			
				case 2214:
					addError(CustomMessageRepository.currencyCodesOfMasterMerchantAndMasterAccountDonotMatch(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2215:
					addError(CustomMessageRepository.currencyCodesOfChildMerchantAndMemberAccountDonotMatch(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2216:
					addError(CustomMessageRepository.invalidHierarchyType(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2217:
					addError(CustomMessageRepository.perIdNbrsAreSame(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2219:
					addError(CustomMessageRepository.invalidCurrencyCodeForChildPerson(
							transactionHeaderId, messageKey, type,
							actualErrorMessage));
					break;
				case 2220:
					addError(CustomMessageRepository.getActiveSAAlreadyExistsError(
							transactionHeaderId, messageKey, type, 
							actualErrorMessage));
					break;
				case 2221:
					addError(CustomMessageRepository.getAcctIsInactiveError(
							transactionHeaderId, messageKey, type, 
							actualErrorMessage));
					break;
				
				default:
					logger.debug("Invalid Error Message Number");
				}

				return false; // intentionally kept false as rollback has to occur
				// here
			}

			/**
			 * updateAccountHierarchyStagingStatus() method updates the
			 * CM_INV_GRP_STG table with processing status.
			 * 
			 * @param transactionHeaderId
			 * @param status
			 * @param messageCategoryNumber
			 * @param messageNumber
			 */
			private void updateAccountHierarchyStagingStatus(
					String transactionHeaderId,String transactionDetailId, String status,
					String messageCategoryNumber, String messageNumber,
					String errorDescription) {
				PreparedStatement preparedStatement = null;
				StringBuilder stringBuilder = new StringBuilder();
				if (errorDescription.length() > 255) {
					errorDescription = errorDescription.substring(0, 249);
				}
				
				//Fix for NAP-32105 
				if (isNull(messageCategoryNumber))
					messageCategoryNumber = "0";
				//end of fix for NAP-32105
				
				try {
					stringBuilder.append("UPDATE CM_INV_GRP_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
					stringBuilder.append( " MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription " );
					if(inboundAccountHierarchyLookUps.getCompleted().equalsIgnoreCase(status)){
						stringBuilder.append( " , ILM_ARCH_SW ='Y' ");
					}
					stringBuilder.append(" WHERE TXN_HEADER_ID =:headerId AND TXN_DETAIL_ID=:detailId");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("status", status.trim(), "BO_STATUS_CD");
					preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
					preparedStatement.bindString("messageNumber", messageNumber, "MESSAGE_NBR");
					preparedStatement.bindString("errorDescription", errorDescription, "ERROR_INFO");
					preparedStatement.bindString("headerId", transactionHeaderId, "TXN_HEADER_ID");
					preparedStatement.bindString("detailId", transactionDetailId, "TXN_DETAIL_ID");
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Inside catch block of updateAccountHierarchyStagingStatus() method-", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}//if
				}//finally

			}//end method

		}//end worker class

		public static final class AcctHierPerIdNbrs_Id implements Id {

			private static final long serialVersionUID = 1L;

			private String perIdNbr;
			private String cisDivision;

			public AcctHierPerIdNbrs_Id(String perIdNbr, String cisDivision) {
				setPerIdNbr(perIdNbr);
				setCisDivision(cisDivision);
			}

			public String getPerIdNbr() {
				return perIdNbr;
			}

			public void setPerIdNbr(String perIdNbr) {
				this.perIdNbr = perIdNbr;
			}

			public String getCisDivision() {
				return cisDivision;
			}

			public void setCisDivision(String cisDivision) {
				this.cisDivision = cisDivision;
			}

			public boolean isNull() {
				return false;
			}

			public void appendContents(StringBuilder arg0) {
			}
		}

		/**
		 * InboundAccountHierarchyInterface_Id class implements the setter and
		 * getter methods for all the attributes of the staging table
		 * 
		 * @author ibm_admin
		 * 
		 */
		public static final class InboundAccountHierarchyInterface_Id implements Id {

			private static final long serialVersionUID = 1L;

			private String transactionHeaderId = null;

			private String transactionDetailId = null;

			private String hierarchyType = null;

			private String perIdNbr = null;

			private String perIdNbr2 = null;

			private String currencyCd = null;

			private Date startDate = null;
			
			private Date endDate = null;

			private String cisDivision = null;

			public InboundAccountHierarchyInterface_Id(boolean b,
					String a_transactionHeaderId, String a_transactionDetailId,
					String a_hierarchyType, String a_perIdNbr, 
					String a_perIdNbr2, String a_currencyCd, Date a_startDate,
					Date a_endDate, String a_cisDivision) {

				setTransactionHeaderId(a_transactionHeaderId);
				setTransactionDetailId(a_transactionDetailId);
				setHierarchType(a_hierarchyType);
				setPerIdNbr(a_perIdNbr);			
				setPerIdNbr2(a_perIdNbr2);
				setCurrencyCd(a_currencyCd);
				setStartDate(a_startDate);
				setEndDate(a_endDate);
				setCisDivision(a_cisDivision);
			}

			public String getCisDivision() {
				return cisDivision;
			}

			public void setCisDivision(String cisDivision) {
				this.cisDivision = cisDivision;
			}

			public Date getStartDate() {
				return startDate;
			}

			public void setStartDate(Date startDate) {
				this.startDate = startDate;
			}

			public Date getEndDate() {
				return endDate;
			}

			public void setEndDate(Date endDate) {
				this.endDate = endDate;
			}

			public String getCurrencyCd() {
				return currencyCd;
			}

			public void setCurrencyCd(String currencyCd) {
				this.currencyCd = currencyCd;
			}

			public String getHierarchType() {
				return hierarchyType;
			}

			public void setHierarchType(String hierarchyType) {
				this.hierarchyType = hierarchyType;
			}

			public String getPerIdNbr() {
				return perIdNbr;
			}

			public void setPerIdNbr(String perIdNbr) {
				this.perIdNbr = perIdNbr;
			}

			public String getPerIdNbr2() {
				return perIdNbr2;
			}

			public void setPerIdNbr2(String perIdNbr2) {
				this.perIdNbr2 = perIdNbr2;
			}

			public String getTransactionDetailId() {
				return transactionDetailId;
			}

			public void setTransactionDetailId(String transactionDetailId) {
				this.transactionDetailId = transactionDetailId;
			}

			public String getTransactionHeaderId() {
				return transactionHeaderId;
			}

			public void setTransactionHeaderId(String transactionHeaderId) {
				this.transactionHeaderId = transactionHeaderId;
			}

			public boolean isNull() {
				return false;
			}

			public void appendContents(StringBuilder arg0) {

			}

		}

	}
