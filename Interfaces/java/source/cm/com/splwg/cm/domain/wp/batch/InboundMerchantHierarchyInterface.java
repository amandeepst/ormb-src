/*******************************************************************************
 * FileName                   : InboundMerchantHierarchyInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 0.7
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name 		  | Nature of Change
0.1		 NA				Mar 24, 2015		Preeti		            Implemented all the requirements for CD1.	
0.2		 NA				Aug 10, 2015		Abhishek Paliwal		BO replaced by DTO	
0.3		 NA				Sep 30, 2015		Preeti		            Removal of empty finally blocks/ applied standard commit strategy.
0.4      NA				Apr 05, 2016		Sunaina					Updated as per Oracle Code Review.
0.5      NA				Apr 22, 2016		Preeti					Updated to set Pending status in execute work unit.
0.6      NA				May 04, 2016		Preeti					Updated to fix SQL warnings.
0.7		 NAP-24086		Mar 16, 2018		Rakesh					NAP-24086 Included ILM_ARCH_SW to be updated to Y for completed status. 
0.7		 NAP-39775		Feb 08, 2018		RIA					    NAP-39775 Included Perf Changes.
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.ccb.domain.admin.personRelationshipType.PersonRelationshipType_Id;
import com.splwg.ccb.domain.customerinfo.person.*;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.cm.domain.wp.batch.InboundMerchantHierarchyLookUp;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
 * This Batch Interface Program creates parent-child relationships between Merchants present in ORMB 
 * on the basis of input data in CM_MERCH_HIER_STG staging table as populated by ODI.
 *
   @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = hierarchyType, required = true, type = string)
 *      , @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 */
public class InboundMerchantHierarchyInterface extends
InboundMerchantHierarchyInterface_Gen {

	public static final Logger logger = LoggerFactory
			.getLogger(InboundMerchantHierarchyInterface.class);

	// Default constructor
	public InboundMerchantHierarchyInterface() {
	}

	private InboundMerchantHierarchyLookUp merchantHierarchyLookup = null;

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork() method of Merchant Hierarhy Interface");
		merchantHierarchyLookup = new InboundMerchantHierarchyLookUp();
		merchantHierarchyLookup.setLookUpConstants();
		List<ThreadWorkUnit> threadWorkUnitList = getMerchantHierarchyData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "
				+ threadWorkUnitList.size());
		merchantHierarchyLookup = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// *********************** getMerchantHierarchyData
	// Method******************************

	/**
	 * getMerchantHierarchyData() method selects distinct sets of PER_ID_NBR
	 * and CIS_DIVISION from CM_MERCH_HIER_STG staging table.
	 * 
	 * @return List PerIdNbrs_Id
	 */
	private List<ThreadWorkUnit> getMerchantHierarchyData() {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		MerchantProcessingData_Id merchantProcessingDataId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		List<MerchantProcessingData_Id> rowsForProcessingList = new ArrayList<MerchantProcessingData_Id>();
		String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
		String perIdNbr = "";
		String personDivision = "";

		
		try {
			stringBuilder.append("SELECT PER_ID_NBR2, CIS_DIVISION FROM CM_MERCH_HIER_STG ");// Performance Changes for threading
			stringBuilder.append(" WHERE BO_STATUS_CD = :selectBoStatus1 AND (START_DT<=SYSDATE)");
			if (notBlank(txnSourceCode)) {

				//Pick only effective dated records for processing.
				stringBuilder.append("AND TXN_SOURCE_CD= :txnSourceCode");
				preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus1", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
			}else {
				preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus1", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
			}
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR2"));
				personDivision = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));

				merchantProcessingDataId = new MerchantProcessingData_Id(
						perIdNbr, personDivision);
				rowsForProcessingList.add(merchantProcessingDataId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(merchantProcessingDataId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				merchantProcessingDataId = null;
			}
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
		return threadWorkUnitList;
	}

	public Class<InboundMerchantHierarchyInterfaceWorker> getThreadWorkerClass() {
		return InboundMerchantHierarchyInterfaceWorker.class;
	}

	public static class InboundMerchantHierarchyInterfaceWorker extends
	InboundMerchantHierarchyInterfaceWorker_Gen {

		private String parentPerId = "";

		private String childPerId = "";

		private ArrayList<ArrayList<String>> updateMerchantHierStatusList = new ArrayList<ArrayList<String>>();

		private ArrayList<String> eachCustomerHierStatusList = null;

		private InboundMerchantHierarchyLookUp merchantHierarchyLookup = null;

		// Default constructor
		public InboundMerchantHierarchyInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once per
		 * thread by the framework.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			merchantHierarchyLookup = new InboundMerchantHierarchyLookUp();
			merchantHierarchyLookup.setLookUpConstants();
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
		 * each row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to create / update
		 * Parent-Child Relationship records.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
//			String parentPerId = "";
//
//			String childPerId = "";
			
			MerchantProcessingData_Id merchantProcessingDataId = (MerchantProcessingData_Id) unit.getPrimaryId();
			List<InboundMerchantHierInterface_Id> inboundMerchantHierInterfaceId = getMerchantHierarchyData(merchantProcessingDataId);
			String skipRowsFlag = "false";//This flag will be used to determine whether the rows should be skipped or not
			//as per the status of earlier rows processed with same parent and child merchant combination

			for (int merchantCount = 0; (merchantCount < inboundMerchantHierInterfaceId
					.size())&&(CommonUtils.CheckNull(skipRowsFlag)
							.trim().startsWith("false")); merchantCount++) {


				removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));

				setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));//Required to nullify the effect of database transactions in case of error scenario
				logger.debug("Processing of Transaction Header Id - "+ inboundMerchantHierInterfaceId.get(merchantCount).getTransactionHeaderId()
								+ " Transaction Detail Id - "+ inboundMerchantHierInterfaceId.get(merchantCount).getTransactionDetailId());

				try {
					setPendingStatus(inboundMerchantHierInterfaceId.get(merchantCount));
					boolean validaitonFlag = true;
					validaitonFlag = validateMerchant(inboundMerchantHierInterfaceId.get(merchantCount));
					// If validation has failed, then exit processing
					if (validaitonFlag) {								
						//return validaitonFlag;
						//}

						// ****************** Hierarchy Creation / Updation
						// ******************
						String returnStatus = createOrUpdatePersonHierarchy(inboundMerchantHierInterfaceId
								.get(merchantCount));
						if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {
							String[] returnStatusArray = returnStatus.split("~");
							returnStatusArray[3] = returnStatusArray[3].replace("Text:", "");
							//skipRowsFlag = "true";

							logError(
									inboundMerchantHierInterfaceId.get(
											merchantCount).getTransactionHeaderId(),
											inboundMerchantHierInterfaceId.get(
													merchantCount).getTransactionDetailId(),
													returnStatusArray[1].trim(), returnStatusArray[2].trim(),
													returnStatusArray[3].trim(),inboundMerchantHierInterfaceId.get(
															merchantCount).getParentMerchant(),
															inboundMerchantHierInterfaceId.get(
																	merchantCount).getChildMerchant(),skipRowsFlag);
						} else {
							updateStagingTableStatus(
									inboundMerchantHierInterfaceId.get(
											merchantCount).getTransactionHeaderId(),
											inboundMerchantHierInterfaceId.get(
													merchantCount).getTransactionDetailId(),
													merchantHierarchyLookup.getCompleted(), "0",
													"0", " ",inboundMerchantHierInterfaceId.get(
															merchantCount).getParentMerchant(),
															inboundMerchantHierInterfaceId.get(
																	merchantCount).getChildMerchant(),skipRowsFlag);
						}
					}//IF VALIDATION FLAG
				} catch (Exception e) {
					logger.error("Exception in executeWorkUnit: " + e);
				}
			}// for loop
			inboundMerchantHierInterfaceId = null;
			return true;
		}

		protected final void removeSavepoint(String savePointName)
		{
			FrameworkSession session = (FrameworkSession)SessionHolder.getSession();
			if (session.hasActiveSavepointWithName(savePointName)) {
				session.removeSavepoint(savePointName);
			}
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

		/**
		 * getMerchantHierarchyStagingData() method selects applicable data rows
		 * from CM_MERCH_HIER_STG staging table.
		 * 
		 * @return List InboundMerchantHierInterface_Id
		 */
		private List<InboundMerchantHierInterface_Id> getMerchantHierarchyData(
				MerchantProcessingData_Id merchantProcessingDataId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			InboundMerchantHierInterface_Id inboundMerchantHierInterfaceId = null;
			List<InboundMerchantHierInterface_Id> inboundMerchantHierInterfaceIdList = new ArrayList<InboundMerchantHierInterface_Id>();
			String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();

			try {
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("SELECT TXN_HEADER_ID, TXN_DETAIL_ID, PER_ID_NBR, ");
				stringBuilder.append(" PER_ID_NBR2, START_DT, END_DT, CIS_DIVISION FROM CM_MERCH_HIER_STG ");
				stringBuilder.append(" WHERE BO_STATUS_CD =:boStatus AND PER_ID_NBR2 = :personIdNbr ");
				stringBuilder.append(" AND CIS_DIVISION =:personDivision");
				
				if (notBlank(txnSourceCode)) {
					stringBuilder.append("AND TXN_SOURCE_CD= :txnSourceCode");
					stringBuilder.append(" ORDER BY TXN_HEADER_ID, TXN_DETAIL_ID ");
					preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
					preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
					preparedStatement.bindString("boStatus", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
					preparedStatement.bindString("personIdNbr", merchantProcessingDataId.getChildMerchant(), "PER_ID_NBR");
					preparedStatement.bindString("personDivision", merchantProcessingDataId.getPersonDivision(), "CIS_DIVISION");
				} else {
					stringBuilder.append(" ORDER BY TXN_HEADER_ID, TXN_DETAIL_ID ");
					preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("boStatus", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
					preparedStatement.bindString("personIdNbr", merchantProcessingDataId.getChildMerchant(), "PER_ID_NBR");
					preparedStatement.bindString("personDivision", merchantProcessingDataId.getPersonDivision(), "CIS_DIVISION");
				}
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String transactionHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID"));
					String transactionDetailId = CommonUtils.CheckNull(String.valueOf((resultSet.getInteger("TXN_DETAIL_ID"))));
					String perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR"));
					String perIdNbr2 = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR2"));
					Date merchantHierarchyStartDate = resultSet.getDate("START_DT");
					Date merchantHierarchyEndDate = resultSet.getDate("END_DT");
					String personDivision = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));

					inboundMerchantHierInterfaceId = new InboundMerchantHierInterface_Id(
							transactionHeaderId, transactionDetailId, perIdNbr,
							perIdNbr2, merchantHierarchyStartDate,
							merchantHierarchyEndDate, personDivision);

					inboundMerchantHierInterfaceIdList.add(inboundMerchantHierInterfaceId);
					resultSet = null;
					inboundMerchantHierInterfaceId = null;
				}
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
			//update only future dated records with corresponding error message.
			try {
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("UPDATE CM_MERCH_HIER_STG SET ERROR_INFO = 'Future dated record: Row will get processed once becomes effective.' ");
				stringBuilder.append("WHERE (START_DT>SYSDATE) ");
				stringBuilder.append("AND PER_ID_NBR2 = :personIdNbr ");
				stringBuilder.append("AND CIS_DIVISION =:personDivision");				
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");	
					preparedStatement.bindString("personIdNbr", merchantProcessingDataId.getChildMerchant(), "PER_ID_NBR");
					preparedStatement.bindString("personDivision", merchantProcessingDataId.getPersonDivision(), "CIS_DIVISION");
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
			return inboundMerchantHierInterfaceIdList;
		}
		/**
		 * setPendingStatus sets record being processed into Pending state.
		 * @param aInboundMerchantInterfaceId
		 */
		private void setPendingStatus(InboundMerchantHierInterface_Id aInboundMerchantHierInterfaceId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			//Set records that would be processed as PENDING
			//Update only effective dated records for processing.
			try {
				stringBuilder.append("UPDATE CM_MERCH_HIER_STG SET BO_STATUS_CD =:status1, STATUS_UPD_DTTM = SYSTIMESTAMP ");
				stringBuilder.append("WHERE BO_STATUS_CD =:status2 AND (START_DT<=SYSDATE) ");
				stringBuilder.append("AND TXN_HEADER_ID =:txnHeaderId AND TXN_DETAIL_ID=:txnDetailId ");				
					preparedStatement = createPreparedStatement( stringBuilder.toString(),"");
					preparedStatement.bindString("txnHeaderId", aInboundMerchantHierInterfaceId.getTransactionHeaderId(), "TXN_HEADER_ID");
					preparedStatement.bindString("txnDetailId", aInboundMerchantHierInterfaceId.getTransactionDetailId(), "TXN_DETAIL_ID");
					preparedStatement.bindString("status1", merchantHierarchyLookup.getPending(), "BO_STATUS_CD");
					preparedStatement.bindString("status2", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
					preparedStatement.setAutoclose(false);
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
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");

			// Logic to update erroneous records
			if (updateMerchantHierStatusList.size() > 0) {
				Iterator<ArrayList<String>> updateAccountHierStatusItr = updateMerchantHierStatusList
						.iterator();
				updateMerchantHierStatusList = null;
				ArrayList<String> rowList = null;
				while (updateAccountHierStatusItr.hasNext()) {
					rowList = (ArrayList<String>) updateAccountHierStatusItr.next();
					updateStagingTableStatus(String.valueOf(rowList.get(0)),
							String.valueOf(rowList.get(1)), String
							.valueOf(rowList.get(2)), String
							.valueOf(rowList.get(3)), String
							.valueOf(rowList.get(4)), String
							.valueOf(rowList.get(5)), String
							.valueOf(rowList.get(6)), String
							.valueOf(rowList.get(7)), String
							.valueOf(rowList.get(8)));
					rowList = null;
				}
				updateAccountHierStatusItr = null;
			}
			merchantHierarchyLookup = null;
			super.finalizeThreadWork();
		}

		/**
		 * createOrUpdatePersonHier() method Creates or Updates person-child
		 * relationship.
		 * 
		 * @param inboundMerchantInterfaceId
		 * @return
		 * @throws RunAbortedException
		 */
		private String createOrUpdatePersonHierarchy(
				InboundMerchantHierInterface_Id aInboundMerchantHierInterfaceId) {
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			SQLResultRow resultSet = null;
			
			try{
				stringBuilder.append("SELECT 1 FROM CI_PER_PER WHERE PER_ID1=:personId ");
				stringBuilder.append("AND PER_ID2=:personId2 AND PER_REL_TYPE_CD=:personRelationshipType ");
				preparedStatement =  createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("personId2", childPerId, "PER_ID");//childPerId
				preparedStatement.bindString("personId", parentPerId, "PER_ID");//parentPerId
				preparedStatement.bindString("personRelationshipType", "CHILD", "PER_REL_TYPE_CD");
				preparedStatement.setAutoclose(false);
				resultSet = preparedStatement.firstRow();
				
			}
			
			catch (Exception e) {
				logger.error("Inside catch block of createOrUpdatePersonHier() method-", e);
			}
			
			finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			try{
			
				if(notNull(resultSet)) {
					
					stringBuilder = null;
					stringBuilder = new StringBuilder();
					preparedStatement = null;
					
					stringBuilder.append("UPDATE CI_PER_PER SET START_DT=:startDt, ");
					stringBuilder.append("END_DT=:endDt WHERE PER_ID1 =:personId ");
					stringBuilder.append("AND PER_ID2 =:personId2 AND PER_REL_TYPE_CD =:personRelationshipType ");
					preparedStatement =  createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("personId2", childPerId, "PER_ID");//childPerId
					preparedStatement.bindString("personId", parentPerId, "PER_ID");//parentPerId
					preparedStatement.bindDate("startDt", aInboundMerchantHierInterfaceId.getMerchantHierarchyStartDate());
					preparedStatement.bindDate("endDt", aInboundMerchantHierInterfaceId.getMerchantHierarchyEndDate());
					preparedStatement.bindString("personRelationshipType", "CHILD", "PER_REL_TYPE_CD");
					preparedStatement.setAutoclose(false);
					preparedStatement.executeUpdate();
					
					
				}
				
				else{
					stringBuilder = null;
					stringBuilder = new StringBuilder();
					preparedStatement = null;
					
					stringBuilder.append("Insert into CI_PER_PER (PER_ID1,PER_ID2,PER_REL_TYPE_CD,START_DT,END_DT,VERSION,FINAN_REL_SW) ");
					stringBuilder.append("values (:personId,:personId2,:personRelationshipType, ");
					stringBuilder.append(" :startDt,:endDt,1,'Y') ");
					preparedStatement =  createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("personId2", childPerId, "PER_ID");//childPerId
					preparedStatement.bindString("personId", parentPerId, "PER_ID");//parentPerId
					preparedStatement.bindString("personRelationshipType", "CHILD", "PER_REL_TYPE_CD");
					if(aInboundMerchantHierInterfaceId.getMerchantHierarchyStartDate()!=null){
						preparedStatement.bindDate("startDt", aInboundMerchantHierInterfaceId.getMerchantHierarchyStartDate());
					}
					else{
						preparedStatement.bindDate("startDt", null);
					}
					
					if(aInboundMerchantHierInterfaceId.getMerchantHierarchyEndDate()!=null)
					{
						preparedStatement.bindDate("endDt", aInboundMerchantHierInterfaceId.getMerchantHierarchyEndDate());
					}
					else{
						preparedStatement.bindDate("endDt", null);
					}
					
					preparedStatement.bindString("personRelationshipType", "CHILD", "PER_REL_TYPE_CD");
					preparedStatement.setAutoclose(false);
					preparedStatement.execute();
					
					
					
					
					
				}
				
			}
			
			catch (Exception e) {
				logger.error("Inside catch block of createOrUpdatePersonHier() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());

				String errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));

				String errorMessageCategory = errorMessage.substring(
						errorMessage.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));

				if (errorMessage.contains("Text:")
						&& errorMessage.contains("Description:")) {
					errorMessage = errorMessage.substring(errorMessage
							.indexOf("Text:"), errorMessage
							.indexOf("Description:"));
				}
				if (errorMessage.length() > 250) {
					errorMessage = errorMessage.substring(0, 250);
				} else {
					errorMessage = errorMessage.substring(0, errorMessage
							.length());
				}
				return "false" + "~" + errorMessageCategory + "~"
				+ errorMessageNumber + "~" + errorMessage;
			}
		finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			
			/*try {
				stringBuilder.append("DELETE FROM CI_PER_PER WHERE PER_ID1 =:personId  ");
				stringBuilder.append( "AND PER_ID2 =:personId2 AND PER_REL_TYPE_CD =:personRelationshipType");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("personId2", childPerId, "PER_ID");//childPerId
				preparedStatement.bindString("personId", parentPerId, "PER_ID");//parentPerId
				preparedStatement.bindString("personRelationshipType", "CHILD", "PER_REL_TYPE_CD");
				preparedStatement.executeUpdate();

				preparedStatement.close();
				preparedStatement = null;
				PersonPerson_DTO personPerson_DTO2=new PersonPerson_DTO();
				if (aInboundMerchantHierInterfaceId.getMerchantHierarchyStartDate() != null) {
					personPerson_DTO2.setId(new PersonPerson_Id(new PersonRelationshipType_Id(merchantHierarchyLookup.getChildRelationshipType()),new Person_Id(parentPerId), new Person_Id(childPerId), aInboundMerchantHierInterfaceId.getMerchantHierarchyStartDate()));
				}else{
					personPerson_DTO2.setId(new PersonPerson_Id(new PersonRelationshipType_Id(merchantHierarchyLookup.getChildRelationshipType()),new Person_Id(parentPerId), new Person_Id(childPerId), null));	
				}
				personPerson_DTO2.setHasFinancialRelationship(Bool.TRUE);
				if (aInboundMerchantHierInterfaceId.getMerchantHierarchyEndDate() != null) {
					personPerson_DTO2.setEndDate(aInboundMerchantHierInterfaceId.getMerchantHierarchyEndDate());
				}else{
					personPerson_DTO2.setEndDate(null);
				}
				personPerson_DTO2.newEntity();
			} catch (Exception e) {
				logger.error("Inside catch block of createOrUpdatePersonHier() method-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());

				String errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));

				String errorMessageCategory = errorMessage.substring(
						errorMessage.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));

				if (errorMessage.contains("Text:")
						&& errorMessage.contains("Description:")) {
					errorMessage = errorMessage.substring(errorMessage
							.indexOf("Text:"), errorMessage
							.indexOf("Description:"));
				}
				if (errorMessage.length() > 250) {
					errorMessage = errorMessage.substring(0, 250);
				} else {
					errorMessage = errorMessage.substring(0, errorMessage
							.length());
				}
				return "false" + "~" + errorMessageCategory + "~"
				+ errorMessageNumber + "~" + errorMessage;
			}*/
			resultSet = null;
			return "true";
		}

		/**
		 * personIdCheck() method Checks whether Parent and child merchant
		 * exists or not in ORMB.
		 * 
		 * @param inboundMerchantInterfaceId
		 * @return
		 * @throws RunAbortedException
		 */
		private boolean validateMerchant(
				InboundMerchantHierInterface_Id aInboundMerchantHierInterfaceId) {
			String parentMerchantNumber = CommonUtils.CheckNull(aInboundMerchantHierInterfaceId.getParentMerchant());
			String childMerchantNumber = CommonUtils.CheckNull(aInboundMerchantHierInterfaceId.getChildMerchant());
			String parentPerIdDivision = null;
			String childPerIdDivision = null;
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
//			String parentPerId = "";
//			String childPerId = "";
			
			
			//Performance Changes to remove inner query
			try {
				stringBuilder.append(" SELECT PER.PER_ID, PER.CIS_DIVISION FROM CI_PER PER, CI_PER_ID PERID ");
				stringBuilder.append( " WHERE PERID.PER_ID=PER.PER_ID ");
				stringBuilder.append( " AND PERID.PER_ID_NBR=:perIdNbr ");
				stringBuilder.append( " AND PERID.ID_TYPE_CD =:idTypeCode ");
				stringBuilder.append( " AND PER.CIS_DIVISION =:personDivision ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("perIdNbr", parentMerchantNumber, "PER_ID_NBR");
				// lookup value - EXPRTYID
				preparedStatement.bindString("idTypeCode", merchantHierarchyLookup.getIdTypeCd(), "ID_TYPE_CD");
				preparedStatement.bindString("personDivision", aInboundMerchantHierInterfaceId.getPersonDivision(), "CIS_DIVISION");
				preparedStatement.setAutoclose(false);

				if(notNull(preparedStatement.firstRow())) {
					parentPerId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("PER_ID"));
					parentPerIdDivision = CommonUtils.CheckNull(preparedStatement.firstRow().getString("CIS_DIVISION"));
				} else {
					
					updateStagingTableStatus(aInboundMerchantHierInterfaceId.getTransactionHeaderId(),
							aInboundMerchantHierInterfaceId.getTransactionDetailId(), 
							merchantHierarchyLookup.getError(), "90000",
							"2101", "Parent person does not exist in ORMB.",
							aInboundMerchantHierInterfaceId.getParentMerchant(),
							aInboundMerchantHierInterfaceId.getChildMerchant(),"false");
					return false;
				}
			
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				
				stringBuilder.append(" SELECT PER.PER_ID, PER.CIS_DIVISION FROM CI_PER PER, CI_PER_ID PERID ");
				stringBuilder.append( " WHERE PERID.PER_ID=PER.PER_ID ");
				stringBuilder.append( " AND PERID.PER_ID_NBR=:perIdNbr ");
				stringBuilder.append( " AND PERID.ID_TYPE_CD =:idTypeCode ");
				stringBuilder.append( " AND PER.CIS_DIVISION =:personDivision ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("perIdNbr", childMerchantNumber, "PER_ID_NBR");
				// lookup value - EXPRTYID
				preparedStatement.bindString("idTypeCode", merchantHierarchyLookup.getIdTypeCd(), "ID_TYPE_CD");
				preparedStatement.bindString("personDivision", aInboundMerchantHierInterfaceId.getPersonDivision(), "CIS_DIVISION");
				preparedStatement.setAutoclose(false);
				if(notNull(preparedStatement.firstRow())) {
					childPerId = CommonUtils.CheckNull(preparedStatement.firstRow().getString("PER_ID"));
					childPerIdDivision = CommonUtils.CheckNull(preparedStatement.firstRow().getString("CIS_DIVISION"));
				} else {
					
					updateStagingTableStatus(aInboundMerchantHierInterfaceId.getTransactionHeaderId(),
							aInboundMerchantHierInterfaceId.getTransactionDetailId(), 
							merchantHierarchyLookup.getError(), "90000",
							"2101", "Child person does not exist in ORMB.",
							aInboundMerchantHierInterfaceId.getParentMerchant(),
							aInboundMerchantHierInterfaceId.getChildMerchant(),"false");
					return false;
				}
				if (!parentPerIdDivision.equals(childPerIdDivision)) {
					
					updateStagingTableStatus(aInboundMerchantHierInterfaceId.getTransactionHeaderId(),
							aInboundMerchantHierInterfaceId.getTransactionDetailId(), 
							merchantHierarchyLookup.getError(), "90000",
							"2105", "Parent & Child person have different divisions.",
							aInboundMerchantHierInterfaceId.getParentMerchant(),
							aInboundMerchantHierInterfaceId.getChildMerchant(),"false");
					return false;
				}
			} catch (Exception e) {
				logger.error("Inside catch block of validateMerchant() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return true;
		}

		/**
		 * logError() method stores the error information in the List and does
		 * rollback all the database transaction of this unit.
		 * @param aChildMerchant 
		 * @param aParentMerchant 
		 * @param skipRowsFlag 
		 * 
		 * @param errorMessage
		 * @param transactionHeaderId
		 * @param transactionDetailId
		 * @param messageCategory
		 * @param messageNumber
		 * @return
		 */
		private boolean logError(String aTransactionHeaderId,
				String aTransactionDetailId, String aMessageCategory,
				String aMessageNumber, String aMessageDescription, String aParentMerchant, String aChildMerchant, String skipRowsFlag) {

			eachCustomerHierStatusList = new ArrayList<String>();
			eachCustomerHierStatusList.add(0, aTransactionHeaderId);
			eachCustomerHierStatusList.add(1, aTransactionDetailId);
			eachCustomerHierStatusList.add(2, merchantHierarchyLookup.getError()); // lookup value-  ERROR
			eachCustomerHierStatusList.add(3, aMessageCategory);
			eachCustomerHierStatusList.add(4, aMessageNumber);
			eachCustomerHierStatusList.add(5, aMessageDescription);
			eachCustomerHierStatusList.add(6, aParentMerchant);
			eachCustomerHierStatusList.add(7, aChildMerchant);
			eachCustomerHierStatusList.add(8, skipRowsFlag);
			updateMerchantHierStatusList.add(eachCustomerHierStatusList);
			eachCustomerHierStatusList = null;

			// Excepted to do rollback
			rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
			addError(CustomMessageRepository.exceptionInExecution(aMessageDescription));			

			return false; // intentionally kept false as rollback has to occur
			// here
		}


		/**
		 * updateHierStagingStatus() method updates the CM_MERCH_HIER_STG table
		 * with processing status.
		 * 
		 * @param transactionHeaderId
		 * @param transactionDetailId
		 * @param personRelationshipType
		 * @param status
		 * @param messageCategoryNumber
		 * @param skipRowsFlag 
		 * @param parentMerchant
		 * @param childMerchant 
		 * @param actualErrorMessageNumber
		 */
		private void updateStagingTableStatus(String aTransactionHeaderId,
				String aTransactionDetailId, String processingStatus,
				String messageCategoryNumber, String aMessageNumber,
				String aMessageDescription, String aPerIdNbr,
				String aPerIdNbr2, String skipRowsFlag) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				if (CommonUtils.CheckNull(aMessageDescription).trim().length() > 250) {
					aMessageDescription = aMessageDescription.substring(0, 250);
				}
				stringBuilder.append("UPDATE CM_MERCH_HIER_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append( " MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:actualErrorMessageNumber, ERROR_INFO =:errorDescription ");
				if(merchantHierarchyLookup.getCompleted().equalsIgnoreCase(processingStatus)){
					stringBuilder.append( " , ILM_ARCH_SW ='Y' ");
				}
				stringBuilder.append( " WHERE TXN_HEADER_ID =:headerId AND TXN_DETAIL_ID=:detailId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");				
				preparedStatement.bindString("status", processingStatus, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("actualErrorMessageNumber", aMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", aMessageDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
				preparedStatement.bindString("detailId", aTransactionDetailId, "TXN_DETAIL_ID");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Inside catch block of updateStagingTableStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			//This logic is required to update those rows, with corresponding error messages, which were skipped for processing through execute work unit
			//since one row with same parent and child merchant combination already has got failed
			if (CommonUtils.CheckNull(skipRowsFlag).trim().startsWith("true")){
				String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();	
				try {
					messageCategoryNumber="0";
					aMessageNumber="0";
					aMessageDescription="Row couldn't be processed: One row is already in error for same Parent merchant and Child merchant.";
					stringBuilder = null;
					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CM_MERCH_HIER_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
					stringBuilder.append( " MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:actualErrorMessageNumber, ERROR_INFO =:errorDescription ");
					stringBuilder.append( " WHERE ((TXN_HEADER_ID >:headerId) OR (TXN_HEADER_ID =:headerId AND TXN_DETAIL_ID>:detailId))" );
					stringBuilder.append(" AND PER_ID_NBR=:perIdNbr AND PER_ID_NBR2=:perIdNbr2 AND BO_STATUS_CD =:status1");
					
					if (notBlank(txnSourceCode)) {
						stringBuilder.append("AND TXN_SOURCE_CD= :txnSourceCode");
						preparedStatement = createPreparedStatement( stringBuilder.toString() ,"");
						preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
						preparedStatement.bindString("status1", merchantHierarchyLookup.getPending(), "BO_STATUS_CD");
						preparedStatement.bindString("status", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
						preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
						preparedStatement.bindString("actualErrorMessageNumber", aMessageNumber, "MESSAGE_NBR");
						preparedStatement.bindString("errorDescription", aMessageDescription, "ERROR_INFO");
						preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
						preparedStatement.bindString("detailId", aTransactionDetailId, "TXN_DETAIL_ID");
						preparedStatement.bindString("perIdNbr", aPerIdNbr, "PER_ID_NBR");
						preparedStatement.bindString("perIdNbr2", aPerIdNbr2, "PER_ID_NBR2");
					} else {
						preparedStatement = createPreparedStatement( stringBuilder.toString() ,"");
						preparedStatement.bindString("status1", merchantHierarchyLookup.getPending(), "BO_STATUS_CD");
						preparedStatement.bindString("status", merchantHierarchyLookup.getUpload(), "BO_STATUS_CD");
						preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
						preparedStatement.bindString("actualErrorMessageNumber", aMessageNumber, "MESSAGE_NBR");
						preparedStatement.bindString("errorDescription", aMessageDescription, "ERROR_INFO");
						preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
						preparedStatement.bindString("detailId", aTransactionDetailId, "TXN_DETAIL_ID");
						preparedStatement.bindString("perIdNbr", aPerIdNbr, "PER_ID_NBR");
						preparedStatement.bindString("perIdNbr2", aPerIdNbr2, "PER_ID_NBR2");
					}
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error("Inside catch block of updateStagingTableStatus() method-", e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}
		}// end method

	}// end worker class

	public static final class MerchantProcessingData_Id implements Id {

		private static final long serialVersionUID = 1L;

		//private String parentMerchant;
		private String childMerchant;

		private String personDivision;

		public MerchantProcessingData_Id(String childMerchant,
				String personDivision) {
			setChildMerchant(childMerchant);
			setPersonDivision(personDivision);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getPersonDivision() {
			return personDivision;
		}

		public void setPersonDivision(String personDivision) {
			this.personDivision = personDivision;
		}

		public String getChildMerchant() {
			return childMerchant;
		}

		public void setChildMerchant(String childMerchant) {
			this.childMerchant = childMerchant;
		}
	}

	public static final class InboundMerchantHierInterface_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String transactionHeaderId;

		private String transactionDetailId;

		private String parentMerchant;

		private String childMerchant;

		private Date merchantHierarchyStartDate;

		private Date merchantHierarchyEndDate;

		private String personDivision;

		public InboundMerchantHierInterface_Id(String transactionHeaderId,
				String transactionDetailId, String parentMerchant,
				String childMerchant, Date merchantHierarchyStartDate,
				Date merchantHierarchyEndDate, String personDivision) {
			setTransactionHeaderId(transactionHeaderId);
			setParentMerchant(parentMerchant);
			setChildMerchant(childMerchant);
			setTransactionDetailId(transactionDetailId);
			setMerchantHierarchyStartDate(merchantHierarchyStartDate);
			setMerchantHierarchyEndDate(merchantHierarchyEndDate);
			setPersonDivision(personDivision);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getTransactionHeaderId() {
			return transactionHeaderId;
		}

		public void setTransactionHeaderId(String transactionHeaderId) {
			this.transactionHeaderId = transactionHeaderId;
		}

		public String getTransactionDetailId() {
			return transactionDetailId;
		}

		public void setTransactionDetailId(String transactionDetailId) {
			this.transactionDetailId = transactionDetailId;
		}

		public String getPersonDivision() {
			return personDivision;
		}

		public void setPersonDivision(String personDivision) {
			this.personDivision = personDivision;
		}

		public String getChildMerchant() {
			return childMerchant;
		}

		public void setChildMerchant(String childMerchant) {
			this.childMerchant = childMerchant;
		}

		public String getParentMerchant() {
			return parentMerchant;
		}

		public void setParentMerchant(String parentMerchant) {
			this.parentMerchant = parentMerchant;
		}

		public void setMerchantHierarchyEndDate(Date merchantHierarchyEndDate) {
			this.merchantHierarchyEndDate = merchantHierarchyEndDate;
		}

		public void setMerchantHierarchyStartDate(
				Date merchantHierarchyStartDate) {
			this.merchantHierarchyStartDate = merchantHierarchyStartDate;
		}

		public Date getMerchantHierarchyEndDate() {
			return merchantHierarchyEndDate;
		}

		public Date getMerchantHierarchyStartDate() {
			return merchantHierarchyStartDate;
		}

	} // end Id class
}
