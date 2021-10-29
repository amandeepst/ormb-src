/*******************************************************************************
 * FileName                   : RepriceBillableChargesInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : May 12, 2017
 * Version Number             : 0.1
 * Revision History     	  :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				May 12, 2017        Vienna Rom	  	NAP-14640 Initial version.
0.2		 NA				Jun 11, 2018        Rakesh Ranjan	NAP-27233 Include deletion of bill segment and bill and also.
																deletion of bill details with bill Cycle Change Flag as Y
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;


import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hibernate.internal.SessionImpl;

import com.splwg.base.api.Query;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.api.lookup.BillableChargeStatusLookup;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType_Id;
import com.splwg.ccb.domain.banking.transactionFeed.transactionFeedAgg.BillDataDeletionUtil;
import com.splwg.ccb.domain.billing.bill.BillCharacteristic;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.customerinfo.account.AccountNumber;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.person.Person_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author vrom
 *
@BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (entityName = characteristicType, name = billCycleChangeFlag, required = true, type = entity)})
 */
public class RepriceBillableChargesInterface extends
		RepriceBillableChargesInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(RepriceBillableChargesInterface.class);
	private static final CustomCreditNoteInterfaceLookUp customCreditNoteInterfaceLookUp = new CustomCreditNoteInterfaceLookUp();

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		
		List<ThreadWorkUnit> threadWorkUnitList = getRepriceData();
	
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * getRepriceData() method
	 * selects distinct set of PER_ID_NBR from CM_PRICE_RECALC_STG staging table.
	 * 
	 * @return List 
	 */
	private List<ThreadWorkUnit> getRepriceData() {
		PreparedStatement preparedStatement = null;
		RepriceProcessingData_Id repriceProcessingDataId;
		
		ThreadWorkUnit threadworkUnit;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		
		String perIdNbr;
		
		//Fetch Per_Id_Nbr for repricing
		try {			
			preparedStatement = createPreparedStatement("SELECT DISTINCT PER_ID_NBR FROM CM_PRICE_RECALC_STG "
					+ " WHERE BO_STATUS_CD = :selectBoStatus1","");
			preparedStatement.bindString("selectBoStatus1",customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR"));
				repriceProcessingDataId = new RepriceProcessingData_Id(perIdNbr);
				
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(repriceProcessingDataId);
				threadworkUnit.addSupplementalData("Entity", "PER");
				threadWorkUnitList.add(threadworkUnit);
			}
		} catch (Exception e) {
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}		
		
		//**Fetch Bill_Id having bill cycle change characteristic set as Y
		StringBuilder fetchBillsWithBillCycleChangeChar = new StringBuilder();
					
		fetchBillsWithBillCycleChangeChar.append("from BillCharacteristic billChar ");
		fetchBillsWithBillCycleChangeChar.append("where billChar.id.characteristicType=:charType ");
		fetchBillsWithBillCycleChangeChar.append("and billChar.characteristicValue=:charVal ");
		logger.info("fetchBillsWithBillCycleChangeChar: "+fetchBillsWithBillCycleChangeChar.toString());
		Query<Bill_Id> fetchBillDetailsQuery = createQuery(fetchBillsWithBillCycleChangeChar.toString(), this.getClass().getName());
		fetchBillDetailsQuery.bindEntity("charType",getParameters().getBillCycleChangeFlag());
		fetchBillDetailsQuery.bindStringProperty("charVal", BillCharacteristic.properties.characteristicValue, "Y");
		fetchBillDetailsQuery.addResult("billId", "billChar.id.bill.id");
		
		for (Bill_Id billId : fetchBillDetailsQuery.list()) {
			threadworkUnit = new ThreadWorkUnit();
			threadworkUnit.setPrimaryId(billId);
			threadworkUnit.addSupplementalData("Entity", "BILL");
			threadWorkUnitList.add(threadworkUnit);
		}
	
		return threadWorkUnitList;
	}

	public Class<RepriceBillableChargesInterfaceWorker> getThreadWorkerClass() {
		return RepriceBillableChargesInterfaceWorker.class;
	}

	public static class RepriceBillableChargesInterfaceWorker extends
			RepriceBillableChargesInterfaceWorker_Gen {

		private static final String EXCEPTION_IN_DELETE_ORIGINAL_CHARGES_METHOD = "Inside catch block of deleteOriginalCharges() method-";
		private static final String ZERO = "0";
		private static final String BILL = "BILL";
		private static final String PER = "PER";
		private static final String TFM = "TFM";
		private static final String TXN_CALC_LN_CHAR = "CI_TXN_CALC_LN_CHAR";
		private static final String TXN_CALC_LN = "CI_TXN_CALC_LN";
		private static final String TXN_SQ = "CI_TXN_SQ";
		private static final String TXN_CALC = "CI_TXN_CALC";
		private static final String EXTERNAL_PARTY_ID_DOES_NOT_EXIST = "External Party Id does not exist in ORMB.";
		private ArrayList<ArrayList<String>> updateRepriceStagingStatusList = new ArrayList<ArrayList<String>>();
		private Person_Id personId;
		private List<String> billIdValues = new ArrayList<String>();
		private int maxBatchCount = 10000;
		

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}


		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * each row of processing. The selected row for processing is read
		 * (comes as input) and then processed further.
		 */
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			String entityValue = (String)unit.getSupplementallData("Entity");
			Bill_Id bill_Id = null;
			
			FrameworkSession newSession = (FrameworkSession) SessionHolder.getSession();
			SessionImpl sessionImpl = (SessionImpl) newSession.getHibernateSession();
			Connection connection = sessionImpl.connection();
			
			if (entityValue.equalsIgnoreCase(BILL)) {
				bill_Id = (Bill_Id) unit.getPrimaryId();	
				Map<String,List<String>> idValues = getBillDetails(bill_Id, entityValue);
				
				List<String> billableChgsIdValuesListForRepricing = idValues.get("billableChgsIdValuesForRepricing");
				List<String> billableChgsIdValuesListForBillTemp = idValues.get("billableChgsIdValuesForBillTemp");
				List<String> billSegmentIdsList = idValues.get("billSegmentIdValues");
				List<String> ftIdsList = idValues.get("ftIdValues");
				try{
					deleteOriginalBillDetails(connection, billableChgsIdValuesListForRepricing,billableChgsIdValuesListForBillTemp,billSegmentIdsList,ftIdsList,entityValue);
					billIdValues.add(bill_Id.getIdValue());
				}catch(SQLException e){
					bill_Id=null;
					e.printStackTrace();
					logger.error(e.getMessage());
				}
				
			}else{
				RepriceProcessingData_Id repriceProcessingDataId = (RepriceProcessingData_Id) unit.getPrimaryId();
				List<InboundRepriceProcessingData_Id> inboundRepriceProcessingDataId = getRepriceEventData(repriceProcessingDataId);
				
				for(InboundRepriceProcessingData_Id rowList : inboundRepriceProcessingDataId) {

					//Required to nullify the effect of database transactions in case of error scenario
					removeSavepoint("Rollback".concat(getParameters().getThreadCount().toString()));					
					setSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));
					
					try {
						boolean validationFlag = validateExternalPartyId(rowList);
						// If validation has failed, then exit processing
						if (!validationFlag) {								
							return validationFlag;
						}

						// ****************** Reprice billable charges ******************

						String returnStatus = repriceBillableChargesOfExternalPartyId(connection, rowList, entityValue);
						
						if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {
							String[] returnStatusArray = returnStatus.split("~");
							returnStatusArray[3] = returnStatusArray[3].replace("Text:", "");

							return logError(rowList.getEventId(),
									returnStatusArray[1].trim(), returnStatusArray[2].trim(),
									returnStatusArray[3].trim(), rowList.getPerIdNbr());
						} else {
							updateStagingTableStatus(
									rowList.getEventId(),
									customCreditNoteInterfaceLookUp.getCompleted().trim(), 
									ZERO, ZERO, " ", rowList.getPerIdNbr());
						}//else
					}catch(SQLException e){
						e.printStackTrace();
						logger.error(e.getMessage());
					}
					catch (Exception e) {
						logger.error("Exception in executeWorkUnit: " + e);
						return logError(rowList.getEventId(), ZERO, ZERO, e.getMessage(), rowList.getPerIdNbr());
					} 
				}
			}
			
			return true;
		}
		
		/**
		 * getRepriceEventData() method selects applicable data rows
		 * from CM_PRICE_RECALC_STG staging table.
		 * 
		 * @return List InboundRepriceProcessingData_Id
		 */
		private List<InboundRepriceProcessingData_Id> getRepriceEventData(RepriceProcessingData_Id repriceProcessingDataId) {
			PreparedStatement preparedStatement = null;
			InboundRepriceProcessingData_Id inboundRepriceProcessingDataId;
			List<InboundRepriceProcessingData_Id> inboundRepriceProcessingDataIdList = new ArrayList<InboundRepriceProcessingData_Id>();
			String eventId;
			String eventCode;
			String perIdNbr;
			String reasonCode;
			
			try {				
				preparedStatement = createPreparedStatement("SELECT EVENT_ID, EVENT_CD, PER_ID_NBR, "
						+ " REASON_CD FROM CM_PRICE_RECALC_STG "
						+ " WHERE BO_STATUS_CD =:boStatus AND PER_ID_NBR = :perIdNbr ","");
				preparedStatement.bindString("boStatus",customCreditNoteInterfaceLookUp.getUpload().trim(), "BO_STATUS_CD");
				preparedStatement.bindString("perIdNbr",repriceProcessingDataId.getPerIdNbr().trim(),"PER_ID_NBR");
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					eventId = CommonUtils.CheckNull(resultSet.getString("EVENT_ID"));
					eventCode = CommonUtils.CheckNull(resultSet.getString("EVENT_CD"));
					perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR"));
					reasonCode = CommonUtils.CheckNull(resultSet.getString("REASON_CD"));

					inboundRepriceProcessingDataId = new InboundRepriceProcessingData_Id(eventId, eventCode, perIdNbr, reasonCode);

					inboundRepriceProcessingDataIdList.add(inboundRepriceProcessingDataId);
				}
			} catch (Exception e) {
				logger.error("Inside catch block of getRepriceEventData() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return inboundRepriceProcessingDataIdList;
		}
		
		/**
		 * validateExternalPartyId() method checks whether the External Party Id exists in ORMB.
		 * 
		 * @param aInboundCreditNoteProcessingDataId
		 * @return status
		 * @throws RunAbortedException
		 */
		private boolean validateExternalPartyId(InboundRepriceProcessingData_Id aInboundRepriceProcessingDataId) {
			String externalPartyId = CommonUtils.CheckNull(aInboundRepriceProcessingDataId.getPerIdNbr());
			String validationMessageNumber;
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement("SELECT PER_ID FROM CI_PER_ID WHERE ID_TYPE_CD='EXPRTYID'" +
						" AND PER_ID_NBR=:externalPartyId","");
				preparedStatement.bindString("externalPartyId", externalPartyId.trim(),"PER_ID_NBR");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					personId = (Person_Id)row.getId("PER_ID", Person.class);
				} else {
					validationMessageNumber = EXTERNAL_PARTY_ID_DOES_NOT_EXIST;
					updateStagingTableStatus(aInboundRepriceProcessingDataId.getEventId(), customCreditNoteInterfaceLookUp.getError().trim(), ZERO,
							ZERO, validationMessageNumber, aInboundRepriceProcessingDataId.getPerIdNbr());
					return false;
				}
			} catch (Exception e) {
				logger.error("Inside catch block of validateExternalPartyId() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return true;
		}
		
		/**
		 * repriceBillableChargesOfExternalPartyId() method performs repricing of unbilled billable charges for a given party id
		 * @param connection 
		 * 
		 * @param aInboundRepriceProcessingDataId
		 * @param entityValue 
		 * @return status
		 */
		private String repriceBillableChargesOfExternalPartyId(Connection connection, InboundRepriceProcessingData_Id aInboundRepriceProcessingDataId, String entityValue) throws SQLException{
			
			Bill_Id billId = getBillIdForRepricingAcct(); 
			Map<String,List<String>> idValues = getBillDetails(billId, entityValue);
			
			List<String> billableChgsIdValuesListForRepricing = idValues.get("billableChgsIdValuesForRepricing");
			List<String> billableChgsIdValuesListForBillTemp = idValues.get("billableChgsIdValuesForBillTemp");
			List<String> billSegmentIdsList = idValues.get("billSegmentIdValues");
			List<String> ftIdsList = idValues.get("ftIdValues");
			
			//Insert txn details back to CI_TXN_DETAIL_STG
			boolean txnsReuploaded = reuploadTransactions(connection, billableChgsIdValuesListForRepricing);

			//Do the rest of processing if there are transactions reuploaded
			if(txnsReuploaded) {
				
				//Insert negated event in CM_EVENT_PRICE
				insertEventPrice(connection, billableChgsIdValuesListForRepricing);
				
				//Delete records in TFM and billable charge tables
				deleteOriginalBillDetails(connection, billableChgsIdValuesListForRepricing,billableChgsIdValuesListForBillTemp,billSegmentIdsList,ftIdsList, entityValue);
				billIdValues.add(billId.getIdValue());
				deleteOriginalTxnCharges(connection, billableChgsIdValuesListForRepricing);
				

			}

			return "true";
		}

		/**
		 * getBillIdForRepricingAcct() method fetches Bill Id for repricing account
		 *
		 * @return billId
		 */
		private Bill_Id getBillIdForRepricingAcct() {
			
			StringBuilder fetchPendingBillStringBuilder = new StringBuilder();
			fetchPendingBillStringBuilder.append("from AccountPerson acctPer, AccountNumber acctNbr, Bill bill ");
			fetchPendingBillStringBuilder.append("WHERE acctNbr.id.account=acctPer.id.account ");
			fetchPendingBillStringBuilder.append("AND acctNbr.id.accountIdentifierType=:acctNbrType ");
			fetchPendingBillStringBuilder.append("AND acctPer.id.person=:personId ");
			fetchPendingBillStringBuilder.append("AND acctNbr.accountNumber=:accountNbr ");
			fetchPendingBillStringBuilder.append("AND bill.account=acctNbr.id.account ");
			fetchPendingBillStringBuilder.append("AND bill.billStatus=:billStatus ");
			
			Query<Bill_Id> fetchPendingBill = createQuery(fetchPendingBillStringBuilder.toString(), this.getClass().getName());
			fetchPendingBill.bindId("acctNbrType", new AccountNumberType_Id("ACCTTYPE"));
			fetchPendingBill.bindId("personId", personId);
			fetchPendingBill.bindStringProperty("accountNbr", AccountNumber.properties.accountNumber, "CHRG");
			fetchPendingBill.bindLookup("billStatus", BillStatusLookup.constants.PENDING);
			fetchPendingBill.addResult("billId", "bill.id");
			
			Bill_Id billId = fetchPendingBill.firstRow();
			
			return billId;
		}

		/**
		 * getBillDetails() method fetches bill segment id, ft id, billable charge id for a given bill id
		 * 
		 * @param billId
		 * @param entityValue 
		 * @return idValues
		 */
		private Map<String, List<String>> getBillDetails(Bill_Id billId, String entityValue) {
			
			/*SELECT * FROM  CI_BILL_CHG CHG, CI_BSEG_CALC BSEGCALC, CI_BSEG BSEG, CI_FT FT
			WHERE BSEG.BILL_ID = '026181737552'
			AND BSEGCALC.BSEG_ID = BSEG.BSEG_ID
			AND FT.SIBLING_ID=BSEG.BSEG_ID
			AND CHG.BILLABLE_CHG_ID = BSEGCALC.BILLABLE_CHG_ID
			AND CHG.BILLABLE_CHG_STAT='10'
			AND BSEG.BSEG_STAT_FLG = '30'
			AND CHG.FEED_SOURCE_FLG='TFM'
			*/
			StringBuilder fetchBillDetailsStringBuilder = new StringBuilder();
			fetchBillDetailsStringBuilder.append("from BillSegment billSegment,BillableCharge billableCharge, ");
			fetchBillDetailsStringBuilder.append("BillSegmentCalculationHeader besgCalculationHeader, FinancialTransaction ft ");
			fetchBillDetailsStringBuilder.append("where billSegment.billId = :billId ");
			fetchBillDetailsStringBuilder.append("and billableCharge.billableChargeStatus = :billableChargeStatus ");
			fetchBillDetailsStringBuilder.append("and besgCalculationHeader.billableChargeId = billableCharge.id ");
			fetchBillDetailsStringBuilder.append("and ft.siblingId = besgCalculationHeader.id.billSegment ");
			fetchBillDetailsStringBuilder.append("and billSegment.id = besgCalculationHeader.id.billSegment ");
			//fetchBillDetailsStringBuilder.append("and billSegment.billSegmentStatus = :billSegmentStatus ");
			// Delete frozen bill segments for bill cycle change 
			Query<QueryResultRow> fetchBillDetailsQuery = createQuery(fetchBillDetailsStringBuilder.toString(), this.getClass().getName());
			fetchBillDetailsQuery.bindId("billId", billId);
			fetchBillDetailsQuery.bindLookup("billableChargeStatus", BillableChargeStatusLookup.constants.BILLABLE);
			//fetchBillDetailsQuery.bindLookup("billSegmentStatus",BillSegmentStatusLookup.constants.FREEZABLE);
			fetchBillDetailsQuery.addResult("billableChargeIds","billableCharge.id");
			fetchBillDetailsQuery.addResult("feedSourceFlg","billableCharge.feedSrcFlg");
			fetchBillDetailsQuery.addResult("billSegmentCalculationHeader","besgCalculationHeader.id.billSegment");
			fetchBillDetailsQuery.addResult("financialTxnIds","ft.id");
			
			
			List<QueryResultRow> billDetailsList = fetchBillDetailsQuery.list();
			List<String> billableChgsIdValuesForRepricing = new ArrayList<String>();
			List<String> billableChgsIdValuesForBillTemp = new ArrayList<String>();
			List<String> billSegmentIdValues = new ArrayList<String>();
			List<String> ftIdValues = new ArrayList<String>();
			Map<String,List<String>> idValues = new HashMap<String, List<String>>();
			
			for(QueryResultRow idValue : billDetailsList){
				
				if( (PER.equalsIgnoreCase(entityValue)) && (TFM.equalsIgnoreCase(idValue.get("feedSourceFlg").toString().trim())) ){
					billableChgsIdValuesForRepricing.add( ((BillableCharge_Id) idValue.get("billableChargeIds")).getIdValue() );
				}
				billableChgsIdValuesForBillTemp.add( ((BillableCharge_Id) idValue.get("billableChargeIds")).getIdValue() );
				billSegmentIdValues.add( ((BillSegment) idValue.get("billSegmentCalculationHeader")).getId().getIdValue() );
				ftIdValues.add( ((FinancialTransaction_Id) idValue.get("financialTxnIds")).getIdValue() );
				
				logger.info("changed");
				logger.info("billableCharges: "+((BillableCharge_Id) idValue.get("billableChargeIds")).getIdValue());
				logger.info("billSegments: "+((BillSegment) idValue.get("billSegmentCalculationHeader")).getId().getIdValue());
				logger.info("ftId: "+((FinancialTransaction_Id) idValue.get("financialTxnIds")).getIdValue());
			}
			logger.info("bill info are done");
			idValues.put("billableChgsIdValuesForRepricing", billableChgsIdValuesForRepricing);
			idValues.put("billableChgsIdValuesForBillTemp", billableChgsIdValuesForBillTemp);
			idValues.put("billSegmentIdValues", billSegmentIdValues);
			idValues.put("ftIdValues", ftIdValues);
			
			return idValues;
			
		}

		private void insertEventPrice(Connection connection, List<String> billableChgsIdValuesListForRepricing) throws SQLException {
			//**********************CM_EVENT_PRICE********************
			java.sql.PreparedStatement preparedStatement = null;
			int insertCount=0;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_EVENT_PRICE ");
				stringBuilder.append("(EVENT_ID, PRICEITEM_CD, PRICE_CATEGORY, CALC_AMT, CURRENCY_CD, ");
				stringBuilder.append("ACCT_TYPE, BILL_REFERENCE, INVOICEABLE_FLG, CREDIT_NOTE_FLG, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM,ACCRUED_DATE,SETT_LEVEL_GRANULARITY,PRICING_SEGMENT) ");
				stringBuilder.append("SELECT DISTINCT A.EVENT_ID, A.PRICEITEM_CD, A.PRICE_CATEGORY, (A.CALC_AMT*(-1)), ");
				stringBuilder.append("A.CURRENCY_CD, A.ACCT_TYPE, A.BILL_REFERENCE, A.INVOICEABLE_FLG, ");
				stringBuilder.append("'Y', SYSTIMESTAMP, A.EXTRACT_FLG, A.EXTRACT_DTTM, A.ACCRUED_DATE, ");
				stringBuilder.append("A.SETT_LEVEL_GRANULARITY, A.PRICING_SEGMENT ");	
				stringBuilder.append("FROM CM_EVENT_PRICE A, CI_TXN_DTL_PRITM TDP, CI_TXN_DETAIL TD ");
				stringBuilder.append("WHERE TD.TXN_DETAIL_ID=TDP.TXN_DETAIL_ID AND TD.BO_STATUS_CD='COMP' AND TD.EXT_TXN_NBR=A.EVENT_ID ");
				stringBuilder.append("AND TDP.BILLABLE_CHG_ID=? ");
				
				preparedStatement = connection.prepareStatement(stringBuilder.toString());
				
				for (String billableChargeId : billableChgsIdValuesListForRepricing) {
					preparedStatement.setString(1, billableChargeId);
					preparedStatement.addBatch();
					if ((insertCount+=1) == maxBatchCount) {
						preparedStatement.executeBatch();
						insertCount = 0;
			          }
					
				}
				if (insertCount > 0){
					logger.info("CM_EVENT_PRICE: "+insertCount);
					preparedStatement.executeBatch();
					insertCount = 0;
		        }
				
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}finally {
				if (notNull(preparedStatement)) {
					preparedStatement.close();
					preparedStatement=null;
				}
			}
		}

		/**
		 * reuploadTransactions() inserts transaction details back into CI_TXN_DETAIL_STG
		 * @param connection 
		 * @param billableChgsIdValuesListForRepricing 
		 * 
		 * @return re-uploaded transactions count
		 */
		private boolean reuploadTransactions(Connection connection, List<String> billableChgsIdValuesListForRepricing) throws SQLException {
			java.sql.PreparedStatement preparedStatement = null;
			int insertCount=0;
			StringBuilder stringBuilder;
			
			//int rowsUpdatedArr[]=new int[billableChgsIdValuesListForRepricing.size()];
			//int rowsUpdated=0;
			Boolean rowsUpdated=false;

			//Populate Detail staging table with original transactions. Set DISAGG_SW='N' to ensure that it is processed as a new txn.
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append("INSERT INTO CI_TXN_DETAIL_STG (TXN_DETAIL_ID,TXN_HEADER_ID,TXN_SOURCE_CD, ");
				stringBuilder.append("TXN_REC_TYPE_CD,TXN_DTTM,EXT_TXN_NBR,CUST_REF_NBR,CIS_DIVISION,ACCT_ID,TXN_VOL,TXN_AMT,CURRENCY_CD,MANUAL_SW, ");
				stringBuilder.append("USER_ID,HOW_TO_USE_TXN_FLG,MESSAGE_CAT_NBR,MESSAGE_NBR,UDF_CHAR_1,UDF_CHAR_2,UDF_CHAR_3,UDF_CHAR_4,UDF_CHAR_5, ");
				stringBuilder.append("UDF_CHAR_6,UDF_CHAR_7,UDF_CHAR_8,UDF_CHAR_9,UDF_CHAR_10,UDF_CHAR_11,UDF_CHAR_12,UDF_CHAR_13,UDF_CHAR_14, ");
				stringBuilder.append("UDF_CHAR_15,UDF_NBR_1,UDF_NBR_2,UDF_NBR_3,UDF_NBR_4,UDF_NBR_5,UDF_NBR_6,UDF_NBR_7,UDF_NBR_8,UDF_NBR_9, ");
				stringBuilder.append("UDF_NBR_10,UDF_AMT_1,UDF_CURRENCY_CD_1,UDF_AMT_2,UDF_CURRENCY_CD_2,UDF_AMT_3,UDF_CURRENCY_CD_3,UDF_AMT_4, ");
				stringBuilder.append("UDF_CURRENCY_CD_4,UDF_AMT_5,UDF_CURRENCY_CD_5,UDF_DTTM_1,UDF_DTTM_2,UDF_DTTM_3,UDF_DTTM_4,UDF_DTTM_5, ");
				stringBuilder.append("BUS_OBJ_CD,BO_STATUS_CD,STATUS_UPD_DTTM,version,DO_NOT_AGG_SW,TXN_UPLOAD_DTTM,ACCT_NBR_TYPE_CD,ACCT_NBR, ");
				stringBuilder.append("UDF_CHAR_16,UDF_CHAR_17,UDF_CHAR_18,UDF_CHAR_19,UDF_CHAR_20,UDF_CHAR_21,UDF_CHAR_22,UDF_CHAR_23,UDF_CHAR_24, ");
				stringBuilder.append("UDF_CHAR_25,RULE_CD,MESSAGE_DESC,DISAGG_SW,DISAGG_CNT,PROCESSING_DT,LAST_SYS_PRCS_DT,UDF_CHAR_26,UDF_CHAR_27, ");
				stringBuilder.append("UDF_CHAR_28,UDF_CHAR_29,UDF_CHAR_30,UDF_CHAR_31,UDF_CHAR_32,UDF_CHAR_33,UDF_CHAR_34,UDF_CHAR_35,UDF_CHAR_36, ");
				stringBuilder.append("UDF_CHAR_37,UDF_CHAR_38,UDF_CHAR_39,UDF_CHAR_40,UDF_CHAR_41,UDF_CHAR_42,UDF_CHAR_43,UDF_CHAR_44,UDF_CHAR_45, ");
				stringBuilder.append("UDF_CHAR_46,UDF_CHAR_47,UDF_CHAR_48,UDF_CHAR_49,UDF_CHAR_50,UDF_NBR_11,UDF_NBR_12,UDF_NBR_13,UDF_NBR_14, ");
				stringBuilder.append("UDF_NBR_15,UDF_NBR_16,UDF_NBR_17,UDF_NBR_18,UDF_NBR_19,UDF_NBR_20,UDF_AMT_6,UDF_AMT_7,UDF_AMT_8,UDF_AMT_9, ");
				stringBuilder.append("UDF_AMT_10,UDF_CURRENCY_CD_6,UDF_CURRENCY_CD_7,UDF_CURRENCY_CD_8,UDF_CURRENCY_CD_9,UDF_CURRENCY_CD_10) ");
				stringBuilder.append("SELECT DISTINCT A.TXN_DETAIL_ID,A.TXN_HEADER_ID,A.TXN_SOURCE_CD, ");
				stringBuilder.append("A.TXN_REC_TYPE_CD,A.TXN_DTTM,A.EXT_TXN_NBR,A.CUST_REF_NBR,A.CIS_DIVISION,' ',A.TXN_VOL,A.TXN_AMT,A.CURRENCY_CD,A.MANUAL_SW, ");
				stringBuilder.append("?,A.HOW_TO_USE_TXN_FLG,A.MESSAGE_CAT_NBR,A.MESSAGE_NBR,A.UDF_CHAR_1,A.UDF_CHAR_2,A.UDF_CHAR_3,A.UDF_CHAR_4,A.UDF_CHAR_5, ");
				stringBuilder.append("A.UDF_CHAR_6,A.UDF_CHAR_7,A.UDF_CHAR_8,A.UDF_CHAR_9,A.UDF_CHAR_10,A.UDF_CHAR_11,A.UDF_CHAR_12,A.UDF_CHAR_13,A.UDF_CHAR_14, ");
				stringBuilder.append("A.UDF_CHAR_15,A.UDF_NBR_1,A.UDF_NBR_2,A.UDF_NBR_3,A.UDF_NBR_4,A.UDF_NBR_5,A.UDF_NBR_6,A.UDF_NBR_7,A.UDF_NBR_8,A.UDF_NBR_9, ");
				stringBuilder.append("A.UDF_NBR_10,A.UDF_AMT_1,A.UDF_CURRENCY_CD_1,A.UDF_AMT_2,A.UDF_CURRENCY_CD_2,A.UDF_AMT_3,A.UDF_CURRENCY_CD_3,A.UDF_AMT_4, ");
				stringBuilder.append("A.UDF_CURRENCY_CD_4,A.UDF_AMT_5,A.UDF_CURRENCY_CD_5,A.UDF_DTTM_1,A.UDF_DTTM_2,A.UDF_DTTM_3,A.UDF_DTTM_4,A.UDF_DTTM_5, ");
				stringBuilder.append("A.BUS_OBJ_CD,?,A.STATUS_UPD_DTTM,A.VERSION,A.DO_NOT_AGG_SW,A.TXN_UPLOAD_DTTM,A.ACCT_NBR_TYPE_CD,A.ACCT_NBR, ");
				stringBuilder.append("A.UDF_CHAR_16,A.UDF_CHAR_17,A.UDF_CHAR_18,A.UDF_CHAR_19,A.UDF_CHAR_20,A.UDF_CHAR_21,A.UDF_CHAR_22,A.UDF_CHAR_23,A.UDF_CHAR_24, ");
				stringBuilder.append("A.UDF_CHAR_25,' ',A.MESSAGE_DESC,'N',A.DISAGG_CNT,null,A.LAST_SYS_PRCS_DT,A.UDF_CHAR_26,A.UDF_CHAR_27, ");
				stringBuilder.append("A.UDF_CHAR_28,A.UDF_CHAR_29,A.UDF_CHAR_30,A.UDF_CHAR_31,A.UDF_CHAR_32,A.UDF_CHAR_33,A.UDF_CHAR_34,A.UDF_CHAR_35,A.UDF_CHAR_36, ");
				stringBuilder.append("A.UDF_CHAR_37,A.UDF_CHAR_38,A.UDF_CHAR_39,A.UDF_CHAR_40,A.UDF_CHAR_41,A.UDF_CHAR_42,A.UDF_CHAR_43,A.UDF_CHAR_44,A.UDF_CHAR_45, ");
				stringBuilder.append("A.UDF_CHAR_46,A.UDF_CHAR_47,A.UDF_CHAR_48,A.UDF_CHAR_49,A.UDF_CHAR_50,A.UDF_NBR_11,A.UDF_NBR_12,A.UDF_NBR_13,A.UDF_NBR_14, ");
				stringBuilder.append("A.UDF_NBR_15,A.UDF_NBR_16,A.UDF_NBR_17,A.UDF_NBR_18,A.UDF_NBR_19,A.UDF_NBR_20,A.UDF_AMT_6,A.UDF_AMT_7,A.UDF_AMT_8,A.UDF_AMT_9, ");
				stringBuilder.append("A.UDF_AMT_10,A.UDF_CURRENCY_CD_6,A.UDF_CURRENCY_CD_7,A.UDF_CURRENCY_CD_8,A.UDF_CURRENCY_CD_9,A.UDF_CURRENCY_CD_10 ");
				stringBuilder.append("FROM CI_TXN_DTL_PRITM TDP, CI_TXN_DETAIL A ");
				stringBuilder.append("WHERE A.TXN_DETAIL_ID=TDP.TXN_DETAIL_ID AND A.BO_STATUS_CD='COMP' ");
				stringBuilder.append("AND TDP.BILLABLE_CHG_ID=? ");
				
				logger.info("stringBuilder: "+stringBuilder.toString());
				
				preparedStatement = connection.prepareStatement(stringBuilder.toString());
				
				for (String billableChargeId : billableChgsIdValuesListForRepricing) {
					logger.info("billableChargeId: "+billableChargeId.trim());
					preparedStatement.setString(1, this.getSubmissionParameters().getUserId().getTrimmedValue());
					preparedStatement.setString(2, customCreditNoteInterfaceLookUp.getUpload().trim());
					preparedStatement.setString(3, billableChargeId);
					preparedStatement.addBatch();
					if ((insertCount+=1) == maxBatchCount) {
						logger.info("executing prepared statement 1");
						//preparedStatement.executeBatch();
						preparedStatement.executeBatch();
						rowsUpdated=true;
						insertCount = 0;
			          }
					
				}
				if (insertCount > 0){
					logger.info("executing prepared statement 2");
					preparedStatement.executeBatch();
					rowsUpdated=true;
					insertCount = 0;
		        }
				
				//rowsUpdated = preparedStatement.executeUpdate();
				logger.info("Detail Staging table populated with original transactions"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of reuploadTransactions() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}

			return rowsUpdated;//
		}
		
		private void deleteOriginalBillDetails(Connection connection, List<String> billableChgsIdValuesListForRepricing,List<String> billableChgsIdValuesListForBillTemp,
				List<String> billSegmentIdsList, List<String> ftIdsList, String entityValue) throws SQLException{
			
			//Delete Bill Segments, FT's
			BillDataDeletionUtil.deleteBillSegment(connection,billSegmentIdsList, ftIdsList, maxBatchCount);
			
			//Delete from CI_BILLTEMP table
			java.sql.PreparedStatement pstmtBillTempDeletion = null;
			int deleteCount=0;
			try{
				pstmtBillTempDeletion = connection.prepareStatement(" DELETE FROM CI_BILLTEMP WHERE BILLABLE_CHG_ID=? ");
				
				for (String billableChargeId : billableChgsIdValuesListForBillTemp) {
				    logger.info("deleting bill temp");
					pstmtBillTempDeletion.setString(1, billableChargeId);
					pstmtBillTempDeletion.addBatch();

			          if ((deleteCount+=1) == maxBatchCount) {
			        	  pstmtBillTempDeletion.executeBatch();
			              deleteCount = 0;
			          }
			    }
				if (deleteCount > 0){
					logger.info("deleting confirmed");
					pstmtBillTempDeletion.executeBatch();
					deleteCount = 0;
		        }
				
			}finally{
				if(notNull(pstmtBillTempDeletion)) {
					pstmtBillTempDeletion.close();
					pstmtBillTempDeletion = null;
	   		  } 
			}
			
			//Delete billable charges for repricing
			if(entityValue.equalsIgnoreCase(PER)){
				Map<String, String> billChgIdMap = new HashMap<String, String>();
				for(String bchgId : billableChgsIdValuesListForRepricing){
					billChgIdMap.put(bchgId, bchgId);
				}
				BillDataDeletionUtil.deleteBillableChargeDetails(connection, billChgIdMap, maxBatchCount);
			}
		}

		/**
		 * deleteOriginalCharges() deletes original charges from TFM and billable charge tables so that they are not billed again.
		 * @param connection 
		 * @param billableChgsIdValuesListForRepricing 
		 * 
		 * @return status
		 */
		private boolean deleteOriginalTxnCharges(Connection connection, 
									List<String> billableChgsIdValuesListForRepricing) throws SQLException{
			java.sql.PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder;
			//int rowsUpdated;
			int insertCount = 0;
			
			try {
				
				deleteFromTxnCalc(TXN_CALC_LN_CHAR, connection, billableChgsIdValuesListForRepricing);
				deleteFromTxnCalc(TXN_CALC_LN, connection, billableChgsIdValuesListForRepricing);
				deleteFromTxnCalc(TXN_SQ, connection, billableChgsIdValuesListForRepricing);
				deleteFromTxnCalc(TXN_CALC, connection, billableChgsIdValuesListForRepricing);
				
			} catch (Exception e) {
				logger.error(EXCEPTION_IN_DELETE_ORIGINAL_CHARGES_METHOD, e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			
			try {
				stringBuilder = new StringBuilder();				
				stringBuilder.append("DELETE FROM CI_TXN_DETAIL WHERE TXN_DETAIL_ID IN (SELECT TD.TXN_DETAIL_ID ");
				stringBuilder.append("FROM CI_TXN_DTL_PRITM TDP, CI_TXN_DETAIL TD ");
				stringBuilder.append("WHERE TD.TXN_DETAIL_ID=TDP.TXN_DETAIL_ID AND TD.BO_STATUS_CD='COMP'");
				stringBuilder.append(" AND TDP.BILLABLE_CHG_ID=? )");
				preparedStatement = connection.prepareStatement(stringBuilder.toString());
				for (String billableChargeId : billableChgsIdValuesListForRepricing) {
					preparedStatement.setString(1, billableChargeId);
					preparedStatement.addBatch();
					if ((insertCount+=1) == maxBatchCount) {
						preparedStatement.executeBatch();
						insertCount = 0;
			          }
					
				}
				if (insertCount > 0){
					preparedStatement.executeBatch();
					insertCount = 0;
		        }
				
				//rowsUpdated=preparedStatement.executeUpdate();
				//logger.info("Data from CI_TXN_DETAIL table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error("Inside catch block of deleteFromTfmAndBillChgTables() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}

			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append("DELETE FROM CI_TXN_DTL_PRITM WHERE TXN_DETAIL_ID IN (SELECT TD.TXN_DETAIL_ID ");
				stringBuilder.append("FROM CI_TXN_DTL_PRITM TDP, CI_TXN_DETAIL_STG TD ");
				stringBuilder.append("WHERE TD.TXN_DETAIL_ID=TDP.TXN_DETAIL_ID ");
				stringBuilder.append(" AND TDP.BILLABLE_CHG_ID=? )");
				preparedStatement = connection.prepareStatement(stringBuilder.toString());
				//rowsUpdated=preparedStatement.executeUpdate();
				for (String billableChargeId : billableChgsIdValuesListForRepricing) {
					preparedStatement.setString(1, billableChargeId);
					preparedStatement.addBatch();
					if ((insertCount+=1) == maxBatchCount) {
						preparedStatement.executeBatch();
						insertCount = 0;
			          }
					
				}
				if (insertCount > 0){
					preparedStatement.executeBatch();
					insertCount = 0;
		        }
				
				//logger.info("Data from CI_TXN_DTL_PRITM table deleted"+rowsUpdated);
			} catch (Exception e) {
				logger.error(EXCEPTION_IN_DELETE_ORIGINAL_CHARGES_METHOD, e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (notNull(preparedStatement)) {
					preparedStatement.close();
					preparedStatement=null;
				}
			}

			return true;
		}
		
		
		private void deleteFromTxnCalc(String table, Connection connection,
								List<String> billableChgsIdValuesListForRepricing) throws SQLException{
			
			java.sql.PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder;
			int insertCount = 0;
			try{
				stringBuilder = new StringBuilder();
				stringBuilder.append("DELETE FROM ");
				stringBuilder.append(table); 
				stringBuilder.append(" WHERE TXN_CALC_ID IN (SELECT TDP.TXN_CALC_ID ");
				stringBuilder.append("FROM CI_TXN_DTL_PRITM TDP, CI_TXN_DETAIL TD ");
				stringBuilder.append("WHERE TD.TXN_DETAIL_ID=TDP.TXN_DETAIL_ID AND TD.BO_STATUS_CD='COMP' ");
				stringBuilder.append("AND TDP.BILLABLE_CHG_ID=?)");
				
				preparedStatement = connection.prepareStatement(stringBuilder.toString());
				logger.info("stringBuilder: "+stringBuilder.toString());
				for (String billableChargeId : billableChgsIdValuesListForRepricing) {
					preparedStatement.setString(1, billableChargeId);
					preparedStatement.addBatch();
					if ((insertCount+=1) == maxBatchCount) {
						preparedStatement.executeBatch();
						insertCount = 0;
			          }
					
				}
				if (insertCount > 0){
					preparedStatement.executeBatch();
					insertCount = 0;
		        }
			}finally{
				if (notNull(preparedStatement)) {
					preparedStatement.close();
					preparedStatement=null;
				}
			}
		}


		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			for(ArrayList<String> rowList : updateRepriceStagingStatusList) {
				updateStagingTableStatus(rowList.get(0), rowList.get(1), rowList.get(2), 
						rowList.get(3), rowList.get(4), rowList.get(5));
			}
			FrameworkSession newSession = (FrameworkSession) SessionHolder.getSession();
			SessionImpl sessionImpl = (SessionImpl) newSession.getHibernateSession();
			Connection connection = sessionImpl.connection();
			try{
				BillDataDeletionUtil.deleteEmptyBills(connection, billIdValues, maxBatchCount);
			 } catch (SQLException e) {
	                e.printStackTrace();
	          }
			
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
			// In case error occurs, roll back all changes for the current transaction and log error.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}

		/**
		 * logError() method stores the error information in the List and does
		 * roll back all the database transaction of this unit.
		 * 
		 * @param aEventId 
		 * @param aMessageCategory 
		 * @param aMessageNumber 
		 * @param aMessageDescription
		 * @param perIdNbr
		 * @return false
		 */
		private boolean logError(String aEventId, String aMessageCategory,
				String aMessageNumber, String aMessageDescription, String perIdNbr) {
			ArrayList<String> eachRepriceStagingStatusList = new ArrayList<String>();
			eachRepriceStagingStatusList.add(0, aEventId);
			eachRepriceStagingStatusList.add(1,customCreditNoteInterfaceLookUp.getError().trim()); // lookup value ERROR
			eachRepriceStagingStatusList.add(2, aMessageCategory);
			eachRepriceStagingStatusList.add(3, aMessageNumber);
			eachRepriceStagingStatusList.add(4, aMessageDescription);
			eachRepriceStagingStatusList.add(5, perIdNbr);
			updateRepriceStagingStatusList.add(eachRepriceStagingStatusList);
			eachRepriceStagingStatusList = null;
			// Excepted to do roll back
			rollbackToSavePoint("Rollback".concat(getParameters().getThreadCount().toString()));
			addError(CustomMessageRepository.exceptionInExecution(aMessageDescription));			
			return false; // intentionally kept false as roll back has to occur here
		}


		/**
		 * updateStagingTableStatus() method updates the CM_PRICE_RECALC_STG table
		 * with processing status. 
		 * @param aEventId
		 * @param processingStatus
		 * @param messageCategoryNumber
		 * @param aMessageNumber 
		 * @param perIdNbr
		 * @param actualErrorMessageNumber
		 */
		private void updateStagingTableStatus(String aEventId, String processingStatus,
				String messageCategoryNumber, String aMessageNumber,
				String aMessageDescription, String perIdNbr) {
			PreparedStatement preparedStatement = null;
			try {
				if (CommonUtils.CheckNull(aMessageDescription).trim().length() > 250) {
					aMessageDescription = aMessageDescription.substring(0, 250);
				}
				preparedStatement = createPreparedStatement("UPDATE CM_PRICE_RECALC_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, "
						+ " MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:actualErrorMessageNumber, ERROR_INFO =:errorDescription "
						+ " WHERE EVENT_ID =:eventId","");				
				preparedStatement.bindString("status", processingStatus,"BO_STATUS_CD");
				preparedStatement.bindString("messageCategory",messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("actualErrorMessageNumber",aMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription",aMessageDescription, "ERROR_INFO");
				preparedStatement.bindString("eventId", aEventId,"EVENT_ID");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error("Inside catch block of updateStagingTableStatus() method-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
	}
	
	public static final class RepriceProcessingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String perIdNbr;

		public RepriceProcessingData_Id(String perIdNbr) {
			setPerIdNbr(perIdNbr);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}		
	}

	public static final class InboundRepriceProcessingData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String eventId;
		private String eventCode;
		private String perIdNbr;
		private String reasonCode;

		public InboundRepriceProcessingData_Id(String eventId,
				String eventCode, String perIdNbr,	String reasonCode) {
			setEventId(eventId);
			setEventCode(eventCode);
			setPerIdNbr(perIdNbr);
			setReasonCode(reasonCode);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getEventId() {
			return eventId;
		}

		public void setEventId(String eventId) {
			this.eventId = eventId;
		}

		public String getEventCode() {
			return eventCode;
		}

		public void setEventCode(String eventCode) {
			this.eventCode = eventCode;
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public String getReasonCode() {
			return reasonCode;
		}

		public void setReasonCode(String reasonCode) {
			this.reasonCode = reasonCode;
		}
	} // end Id class
}