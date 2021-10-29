/**************************************************************************
 *
 * PROGRAM DESCRIPTION:
 * 
 * This batch process creates Bill download staging records for all Bills 
 * that are ready to be consumed by an External System.  Each Bill download 
 * staging record is marked with a batch process ID and run number.
 * 
 *
 **************************************************************************
 *
 * CHANGE HISTORY:
 *
 * Date:        by:      Reason:
 * YYYY-MM-DD   IN       Reason text.
 * 
 * 2018-01-03	AVilla	 Initial Version. RIA_Payment Request Interface_TDD-v0.3
 **************************************************************************/

package com.splwg.cm.domain.wp.batch;
	
import java.math.BigInteger;

import com.splwg.base.api.Query;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadIterationStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.batch.WorkUnitResult;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.StringId;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.domain.batch.batchControl.BatchControl;
import com.splwg.base.domain.batch.batchControl.BatchControl_Id;
import com.splwg.base.domain.common.generalProcess.GeneralProcess_DTO;
import com.splwg.base.domain.common.generalProcess.GeneralProcess_Id;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject_Id;
import com.splwg.ccb.api.lookup.AdjustmentStatusLookup;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
	
	
	/**
	 * @author AVilla
	 *
	@BatchJob (multiThreaded = true, rerunnable = true, modules = {},
	 *      softParameters = { @BatchJobSoftParameter (entityName = batchControl, name = batchControl, required = true ,type = entity)
	 *      , @BatchJobSoftParameter (name = numberOfDaysToProcess, required = true, type = integer)})
	 */
	
		public class CmBillDownloadStaging extends
				CmBillDownloadStaging_Gen {
		
			// Constants
			private static final String BILL_MO = "BILL";
		
			// Soft Parameter
			private static BatchControl inputBatchControl;
			private static BigInteger inputNumberOfDaysToProcess;
			private static DateTime numDaysToProcess;
			
			/**
			 * Method to validate soft parameters 
			 * @param isNewRun
			 * 
			 */
			public void validateSoftParameters(boolean isNewRun) {
				
				//Retrieve and validate if the Batch Control Parameter exists		
				inputBatchControl = getParameters().getBatchControl();
				inputNumberOfDaysToProcess = getParameters().getNumberOfDaysToProcess();
				
				if (isNull(inputBatchControl)){
					addError(CustomMessageRepository.batchParameterMissing(
							"batchControl", 
							this.getBatchControlId().getIdValue().trim()));
				}
				
				if (isNull(inputNumberOfDaysToProcess)){
					addError(CustomMessageRepository.batchParameterMissing(
							"numberOfDaysToProcess", 
							this.getBatchControlId().getIdValue().trim()));
				}
			}
			
			public Class<CmBillDownloadStagingWorker> getThreadWorkerClass() {
				return CmBillDownloadStagingWorker.class;
			}
				
			public static class CmBillDownloadStagingWorker extends
					CmBillDownloadStagingWorker_Gen {
						
				// Work variables
				private Date processDate = this.getProcessDateTime().getDate();
				private Bill_Id billId;
				private BigInteger nextBatchRun = BigInteger.ZERO;
				private BatchControl_Id batchCd;
				private MaintenanceObject_Id billMo;
				private DateTime processDateTime = this.getProcessDateTime();
				
				public ThreadExecutionStrategy createExecutionStrategy() {
					return new ThreadIterationStrategy(this);
				}
				
		        @SuppressWarnings("rawtypes")
		        protected QueryIterator<Bill_Id> getQueryIteratorForThread(StringId lowId, StringId highId) {
						       
		        	QueryIterator<Bill_Id> qIter = null;
		        	
			        Query<Bill_Id> query = createQuery(" FROM " +
									        		" 	Bill bill " +
									        		" WHERE bill.id BETWEEN :lowId AND :highId " +
									        		" 	AND bill.billStatus = :billStatFlg " +
									        		"	AND bill.creationDateTime <= :getProcessDateTime " +   
									        		"	AND bill.creationDateTime >= :numDaysToProcess " +
									        		" 	AND NOT EXISTS " +
									        		" 	( FROM " +
									        		"		GeneralProcess genProc " +
									        		"		WHERE genProc.maintenanceObject = 'BILL'" +
									        		" 		AND genProc.primaryKeyValue1 = bill.id " +
									        		" 		AND genProc.batchControl = :batchControl) ","");
			        
			        query.bindId("lowId", lowId); 
	 				query.bindId("highId", highId);
	 				query.bindLookup("billStatFlg", BillStatusLookup.constants.COMPLETE);
	 				query.bindDateTime("getProcessDateTime", this.getProcessDateTime());
	 				query.bindDateTime("numDaysToProcess", numDaysToProcess);
	 				query.bindEntity("batchControl", inputBatchControl);
	 				query.addResult("billId", "bill.id");
	 		        query.orderBy("billId", Query.ASCENDING);
	 		        
	 		        if(notNull(query)){
	 		        	qIter = query.iterate();
	 		        }
		 			
					return qIter;	
		        }
		
		
				/**
				 * This method generates a unit of work.
				 */
				protected ThreadWorkUnit getNextWorkUnit(QueryResultRow row) {
					ThreadWorkUnit unit = new ThreadWorkUnit();
					unit.setPrimaryId(row.getId("billId", Bill.class));
					return unit;
				}
		        
				/**
				 * An implemented method that is executed at the start of each thread run for initialization.
				 * @param initializationSuccessful boolean indicating successful processing
				 * @throws ThreadAbortedException Thread Aborted Exception
		         * @throws RunAbortedException Run Aborted Exception
				 */
				public void initializeThreadWork(
						boolean initializationPreviouslySuccessful)
						throws ThreadAbortedException, RunAbortedException {
					
					//Retrieve Batch Control Parameters	
					inputBatchControl = getParameters().getBatchControl();
					inputNumberOfDaysToProcess = getParameters().getNumberOfDaysToProcess();
					numDaysToProcess = this.getProcessDateTime().addDays(-inputNumberOfDaysToProcess.intValue());
					
					nextBatchRun = getBatchCtrlNextBatchRunNumber(inputBatchControl);
					startEntityIdQueryIteratorForThread(Bill_Id.class);			
				}
				
				/**
				 * This method processes every record retrieved by the Query Iterator.
				 */
				public WorkUnitResult executeWorkUnitDetailedResult(ThreadWorkUnit unit)
						throws ThreadAbortedException, RunAbortedException {
					
					// Initialize variable
					billId = null;
					batchCd = null;
					billMo = null;
					//genProc = null;
					
					// Get bill Id, batch control cd, bill MO
					billId = (Bill_Id) unit.getPrimaryId();		
					
					batchCd = inputBatchControl.getId();
					
					billMo = new MaintenanceObject_Id(BILL_MO);
					
					
					// Add records in the General Process table
					if(checkIfBillHasBillSegment(billId).equals(Bool.TRUE)){
						
						insertGenProcRecords(batchCd, 
								nextBatchRun, 
								billMo, 
								billId,
								processDateTime);
					}
					
					
					WorkUnitResult results = new WorkUnitResult(true);
					results.setUnitsProcessed(1);
					return results;
				}
										
				public void finalizeThreadWork() throws ThreadAbortedException,RunAbortedException {
					
				}
				
				/**
				 * This method creates general process record.
				 */
				public void insertGenProcRecords(BatchControl_Id batchCd, BigInteger batchNbr, 
						MaintenanceObject_Id billMo, Bill_Id billId, DateTime processDateTime) {
				
					GeneralProcess_DTO genProcDto = new GeneralProcess_DTO();
					genProcDto.setId(GeneralProcess_Id.NULL);
					genProcDto.setMaintenanceObjectId(billMo);
					genProcDto.setPrimaryKeyValue1(billId.getIdValue().trim());
					genProcDto.setBatchControlId(batchCd);
					genProcDto.setBatchNumber(batchNbr);
					genProcDto.setCreationDateTime(processDateTime);
					genProcDto.newEntity();
	
				}
				
				/**
				 * This method gets the next batch run number of the input batch control.
				 */
				public BigInteger getBatchCtrlNextBatchRunNumber(BatchControl batchCtrl) {
				
					BigInteger nextBatchRunNumber = BigInteger.ZERO;
					
					BatchControl_Id batchCtrlId = new BatchControl_Id(batchCtrl.getId().getIdValue().trim());
					
					if(notNull(batchCtrlId.getEntity())) {
					
						nextBatchRunNumber = batchCtrlId.getEntity().getNextBatchNumber();
					}
					
					return nextBatchRunNumber;
				}
				
				/*
				 * This method checks if the bill has at least 1 frozen bill segment
				 */
				public Bool checkIfBillHasBillSegment(Bill_Id billId){
					
					Bool hasBsegFlg = Bool.FALSE;
					PreparedStatement statement = null;
					
					String sql = " SELECT 'X' FROM CI_BILL BILL, CI_BSEG BSEG " +
								" WHERE BILL.BILL_ID = BSEG.BILL_ID " +
								" AND BSEG.BSEG_STAT_FLG = :bsegStatFlg " +
								" AND BILL.BILL_ID = :billId " +
								" AND ROWNUM <= 1 ";
					
					try{
						
						statement = createPreparedStatement(sql, "");
						statement.setAutoclose(false);
						statement.bindId("billId", billId);
						statement.bindLookup("bsegStatFlg", BillSegmentStatusLookup.constants.FROZEN);
						
						if(statement.list().size() > 0){
							hasBsegFlg = Bool.TRUE;
						}
						
					}finally{
						if(notNull(statement)){
							statement.close();
						}
					}

					return hasBsegFlg;
				}
				
				/*
				 * This method checks if the bill has at least 1 adjustment
				 */
				public Bool checkIfBillHasAdjustment(Bill_Id billId){
					
					Bool hasAdjFlg = Bool.FALSE;
					
					PreparedStatement statement = null;
					
					String sql = " SELECT 'X' FROM CI_ADJ ADJ, CI_FT FT " +
								" WHERE FT.BILL_ID = :billId " +
								" AND FT.FT_TYPE_FLG = :adjType " +
								" AND ADJ.ADJ_ID = FT.SIBLING_ID " +
								" AND ADJ.ADJ_STATUS_FLG = :adjStatFlg " + 
								" AND ROWNUM <= 1 ";
					 
					try{
						
						statement = createPreparedStatement(sql, "");
						statement.setAutoclose(false);
						statement.bindId("billId", billId);
						statement.bindLookup("adjType", FinancialTransactionTypeLookup.constants.ADJUSTMENT);
						statement.bindLookup("adjStatFlg", AdjustmentStatusLookup.constants.FROZEN);
						
						if(statement.list().size() > 0){
							hasAdjFlg = Bool.TRUE;
						}
						
					}finally{
						if(notNull(statement)){
							statement.close();
						}
					}
					
					return hasAdjFlg;
				}	
			}
		}