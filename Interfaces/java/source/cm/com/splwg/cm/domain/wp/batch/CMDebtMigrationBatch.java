/********************************************************************************************************************
* FileName                   : CMDebtMigrationBatch.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Aug 01, 2018 
* Version Number             : 0.1
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Aug 01, 2018        Rakesh Ranjan        Initial Draft version
0.2      NAP-32005      Aug 29, 2018        Prerna Mehta         Added logic to insert data in CM_BILL_ID_MAP,CM_BILL_PAYMENT_DTL and CM_BILL_DUE_DT tables
0.3		 NAP-36699		Dec 03, 2018		Somya Sikka			 Changed payment type to CR/DR instead of DEBT_MIGRATION while inserting in CM_BILL_PAYMENT_DTL                
***********************************************************************************************************************/

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
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Time;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_DTO;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author ranjanr686
 *
@BatchJob (modules = { "demo"})
 */
public class CMDebtMigrationBatch extends CMDebtMigrationBatch_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(NonTransPriceDataInterface.class);
	public static final String BILL_ID = "billId";

	public JobWork getJobWork() {
		
		logger.info(getSystemDateTime().addDays(-2));
		resetBillMapIdSq();
		List<ThreadWorkUnit> threadWorkUnitList = getBillIdWithMigrationData();
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	private List<ThreadWorkUnit> getBillIdWithMigrationData() {
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();

		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		String billId = "";
		Date debtDate;
		Bill_Id bill_Id = null;
		String eventId = null;
		stringBuilder.append("SELECT DISTINCT BILL.BILL_ID AS BILL_ID ,MIN(BMAP.DEBT_DT) AS DEBT_DT,min(EVENT_ID) AS EVENT_ID ");
		stringBuilder.append("FROM CI_BILL BILL, CI_BSEG BSEG, CI_BSEG_CALC CALC, CI_BILL_CHG BCHG, CM_BCHG_ATTRIBUTES_MAP BMAP ");
		stringBuilder.append("WHERE BILL.BILL_ID       = BSEG.BILL_ID ");
		stringBuilder.append("AND BSEG.BSEG_ID         = CALC.BSEG_ID ");
		stringBuilder.append("AND CALC.BILLABLE_CHG_ID = BCHG.BILLABLE_CHG_ID ");
		stringBuilder.append("AND BCHG.BILLABLE_CHG_ID = BMAP.BILLABLE_CHG_ID ");
		stringBuilder.append("AND BILL.ADHOC_BILL_SW   = 'Y' ");
		stringBuilder.append("AND BILL.BILL_STAT_FLG = 'C' ");
		stringBuilder.append("AND BCHG.PRICEITEM_CD   IN ( 'MIGCHRG', 'MIGFUND', 'MIGCHBK','MIGCHRG2' ) ");
		stringBuilder.append("AND BILL.ILM_DT=:batchDt ");
		stringBuilder.append("AND NOT EXISTS(SELECT 1 FROM CI_BILL_CHAR WHERE BILL_ID=BILL.BILL_ID AND CHAR_TYPE_CD = 'NON_ZERO') ");
		stringBuilder.append("GROUP BY BILL.BILL_ID ");
		try{
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");					
			preparedStatement.bindDate("batchDt", getProcessDateTime().getDate()); 
			preparedStatement.setAutoclose(false); 
			for (SQLResultRow sqlRow : preparedStatement.list()) {
				billId = sqlRow.getString("BILL_ID");
				debtDate = sqlRow.getDate("DEBT_DT");
				eventId = sqlRow.getString("EVENT_ID");
				bill_Id = new Bill_Id(billId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(bill_Id);
				threadworkUnit.addSupplementalData("debtDate", debtDate);
				threadworkUnit.addSupplementalData("eventId", eventId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				bill_Id = null;
			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getBillIdWithMigrationData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getBillIdWithMigrationData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}	
		return threadWorkUnitList;
	}

	private void resetBillMapIdSq() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement(" {CALL CM_BILL_MAP_ID}");
			preparedStatement.execute();
						
		} catch (RuntimeException e) {
			logger.error("Inside resetBillMapIdSq() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	public Class<CMDebtMigrationBatchWorker> getThreadWorkerClass() {
		return CMDebtMigrationBatchWorker.class;
	}

	public static class CMDebtMigrationBatchWorker extends
			CMDebtMigrationBatchWorker_Gen {
		
		private static final EventPriceDataInterfaceLookUp eventPriceDataInterfaceLookUp = new EventPriceDataInterfaceLookUp();

		public ThreadExecutionStrategy createExecutionStrategy() {
			
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			Bill_Id bill_Id = (Bill_Id) unit.getPrimaryId();
			DateTime debtDate = null;
			Boolean eligibleForPaymentReuest;
			Date debtDateStr = (Date)unit.getSupplementallData("debtDate"); 
			if(notNull(debtDateStr)){
				 debtDate = new DateTime(debtDateStr, Time.MIN);
			}			
			String eventId = unit.getSupplementallData("eventId").toString().trim();
			DateTime maxDate  = getSystemDateTime().addDays(-2);
			if(notNull(debtDate)){
				debtDate = debtDate.isAfter(maxDate) ? maxDate :debtDate ;				
			}
			else{
				debtDate=maxDate;
			}
			try{
				// Check for MIGCHG2
				eligibleForPaymentReuest = eligibleForPayReq(bill_Id);
				
				if (!eligibleForPaymentReuest){
					Bill bill = bill_Id.getEntity();
					Bill_DTO billDto = bill.getDTO();
					billDto.setCompletedDatetime(debtDate);
					billDto.setBillDate(debtDate.getDate());			
					bill.setDTO(billDto);
				
					//logic to insert records in  Bill ID Map, Bill Payment Detail, bill due date tables
					insertInBillIdMAp(bill_Id);
					insertInBillPayDtl(bill_Id);
					insertInBillPayDtlSnapshot(bill_Id);
					insertInBillDueDt(bill_Id, debtDate.getDate(), eventId);
					insertNonZeroBillChar(bill_Id, "Y");
				}
				else {
					insertNonZeroBillChar(bill_Id, "N");
				}
			}catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("System Error.Please check logs for more details"));
			}	


			return true;
		}

		private int insertInBillPayDtlSnapshot(Bill_Id referenceBillId) {
			PreparedStatement preparedStatement = null;
			int count=0;
			try{
				StringBuilder createSnapshotEntries = new StringBuilder();

				createSnapshotEntries.append(" INSERT INTO CM_BILL_PAYMENT_DTL_SNAPSHOT ");
				createSnapshotEntries.append(" (BILL_BALANCE_ID ,LATEST_PAY_DTL_ID ,LATEST_UPLOAD_DTTM ,PAY_DT ,BILL_DT,PARTY_ID , ");
				createSnapshotEntries.append(" LCP_DESCRIPTION ,LCP,ACCT_TYPE ,ACCOUNT_DESCRIPTION ,EXT_TRANSMIT_ID ,BILL_ID ,ALT_BILL_ID , ");
				createSnapshotEntries.append(" LINE_ID ,LINE_AMT ,PREV_UNPAID_AMT ,LATEST_PAY_AMT ,UNPAID_AMT ,BILL_AMOUNT, BILL_BALANCE , ");
				createSnapshotEntries.append(" CURRENCY_CD ,LATEST_STATUS ,PAY_TYPE ,ILM_DT ,ILM_ARCH_SW ,OVERPAID ,RECORD_STAT , ");
				createSnapshotEntries.append(" STATUS_UPD_DTTM ,MESSAGE_CAT_NBR,MESSAGE_NBR,ERROR_INFO,EXT_SOURCE_CD,CREDIT_NOTE_ID) ");
				createSnapshotEntries.append(" SELECT BILL_BALANCE_ID_SEQ.NEXTVAL, PAY.PAY_DTL_ID, PAY.UPLOAD_DTTM, PAY.PAY_DT , ");
				createSnapshotEntries.append(" MAP.BILL_DT,MAP.PER_ID_NBR ,L.DESCR,trim(DCHAR.CHAR_VAL),MAP.ACCT_TYPE, ");
				createSnapshotEntries.append(" CASE WHEN MAP.ACCT_TYPE = 'CHRG' THEN 'Charging' ");
				createSnapshotEntries.append(" WHEN MAP.ACCT_TYPE = 'FUND' THEN 'Funding' ");
				createSnapshotEntries.append(" WHEN MAP.ACCT_TYPE = 'CHBK' THEN 'Chargebacks' ");
				createSnapshotEntries.append(" END AS ACCOUNT_DESCRIPTION, ");
				createSnapshotEntries.append(" PAY.EXT_TRANSMIT_ID,PAY.BILL_ID , MAP.ALT_BILL_ID, PAY.LINE_ID, PAY.LINE_AMT, ");
				createSnapshotEntries.append(" PAY.PREV_UNPAID_AMT , PAY.PAY_AMT, PAY.UNPAID_AMT ,  PAY.LINE_AMT AS BILL_AMOUNT, ");
				createSnapshotEntries.append(" UNPAID_AMT AS BILL_BALANCE, PAY.CURRENCY_CD , PAY.STATUS_CD, PAY.PAY_TYPE PAY_TYPE, ");
				createSnapshotEntries.append(" PAY.ILM_DT, PAY.ILM_ARCH_SW,PAY.OVERPAID,PAY.RECORD_STAT,PAY.STATUS_UPD_DTTM, ");
				createSnapshotEntries.append(" PAY.MESSAGE_CAT_NBR , PAY.MESSAGE_NBR,PAY.ERROR_INFO, PAY.EXT_SOURCE_CD,PAY.CREDIT_NOTE_ID ");
				createSnapshotEntries.append(" FROM CI_CIS_DIVISION_L L, CI_CIS_DIV_CHAR DCHAR, CM_BILL_ID_MAP map ,CM_BILL_PAYMENT_DTL PAY ");
				createSnapshotEntries.append(" WHERE MAP.BILL_ID = PAY.BILL_ID  ");
				createSnapshotEntries.append(" AND L.CIS_DIVISION = MAP.CIS_DIVISION ");
				createSnapshotEntries.append(" AND L.CIS_DIVISION = DCHAR.CIS_DIVISION ");
				createSnapshotEntries.append(" AND DCHAR.CHAR_TYPE_CD= 'BOLE    ' ");
				createSnapshotEntries.append(" AND PAY.BILL_ID=:billId AND PAY.STATUS_CD='MIGRATED_DEBT' ");

				preparedStatement = createPreparedStatement(createSnapshotEntries.toString(),"");
				preparedStatement.bindId(BILL_ID, referenceBillId);

				count=preparedStatement.executeUpdate();
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillPayDtlSnapshot() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillPayDtlSnapshot() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in CM_BILL_PAYMENT_DTL_SNAPSHOT table"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return count;
		}

		/**
		 * This method will insert record in CM_BILL_ID_MAP table
		 * @param bill_Id
		 * @return int
		 */
		private int insertInBillIdMAp(Bill_Id bill_Id ) {
			PreparedStatement preparedStatement = null;
			int count=0;
			try{
				StringBuilder queryString = new StringBuilder();	
				queryString.append(" Insert into CM_BILL_ID_MAP (EVENT_TYPE_ID,CIS_DIVISION,PER_ID_NBR,EVENT_PROCESS_ID,BILL_REFERENCE,BILL_ID,BILL_START_DT,BILL_END_DT,       ");
				queryString.append(" ALT_BILL_ID,BILL_DT,BILL_AMT,CURRENCY_CD,CR_NOTE_FR_BILL_ID,UPLOAD_DTTM,EXTRACT_FLG,EXTRACT_DTTM,ACCT_TYPE,ILM_DT,ILM_ARCH_SW, BILL_MAP_ID)             ");
				queryString.append(" SELECT tmp.EVENT_TYPE_ID,																													");
				queryString.append("   tmp.CIS_DIVISION,                                                                                                                        ");
				queryString.append("   (SELECT A.per_id_nbr                                                                                                                     ");
				queryString.append("   FROM CI_PER_ID A                                                                                                                         ");
				queryString.append("   WHERE A.PER_ID  =tmp.per_id                                                                                                              ");
				queryString.append("   AND A.ID_TYPE_CD=:externalPartyId                                                                                                        ");
				queryString.append("   ) AS per_id_nbr,                                                                                                                         ");
				queryString.append("   tmp.EVENT_PROCESS_ID,                                                                                                                    ");
				queryString.append("   CONCAT(tmp.PER_ID,CONCAT('+',CONCAT(tmp.ACCT_ID,CONCAT('+',CONCAT(tmp.BILL_CYC_CD,CONCAT('+',to_char(tmp.BILL_START_DT,'yyyy-mm-dd-hh.mi.SS.sssss'))))))) AS BILL_REFERENCE,  ");
				queryString.append("   tmp.bill_id,                                                                                                                             ");
				queryString.append("   tmp.bill_start_dt,                                                                                                                       ");
				queryString.append("   tmp.BILL_END_DT,                                                                                                                         ");
				queryString.append("   tmp.ALT_BILL_ID,                                                                                                                         ");
				queryString.append("   tmp.bill_dt,                                                                                                                             ");
				queryString.append("   tmp.bill_amt,                                                                                                                            ");
				queryString.append("   tmp.CURRENCY_CD,                                                                                                                         ");
				queryString.append("   tmp.CR_NOTE_FR_BILL_ID,                                                                                                                  ");
				queryString.append("   tmp.UPLOAD_DTTM,                                                                                                                         ");
				queryString.append("   tmp.EXTRACT_FLG,                                                                                                                         ");
				queryString.append("   tmp.EXTRACT_DTTM,                                                                                                                        ");
				queryString.append("   tmp.ACCT_TYPE,                                                                                                                           ");
				queryString.append("   tmp.ILM_DT,                                                                                                                              ");
				queryString.append("   tmp.ILM_ARCH_SW,                                                                                                                          ");
				queryString.append("   bill_id_map_seq.nextval                                                                                                                         ");
				queryString.append(" FROM                                                                                                                                       ");
				queryString.append("   (SELECT :eventTypeId AS EVENT_TYPE_ID,                                                                                                   ");
				queryString.append("     acct.CIS_DIVISION,                                                                                                                     ");
				queryString.append("     (SELECT X.PER_ID FROM CI_ACCT_PER X WHERE X.ACCT_ID=bill.acct_id                                                                       ");
				queryString.append("     )            AS per_id,                                                                                                                ");
				queryString.append("     bill.acct_id AS acct_id,                                                                                                               ");
				queryString.append("     CASE                                                                                                                                   ");
				queryString.append("       WHEN BILL.BILL_CYC_CD=' '                                                                                                            ");
				queryString.append("       THEN 'ADHC'                                                                                                                          ");
				queryString.append("       ELSE BILL.BILL_CYC_CD                                                                                                                ");
				queryString.append("     END          AS BILL_CYC_CD,                                                                                                           ");
				queryString.append("     bill.bill_id AS bill_id,                                                                                                               ");
				queryString.append("     :eventProcessId       AS EVENT_PROCESS_ID,                                                                                             ");
				queryString.append("     CASE                                                                                                                                   ");
				queryString.append("       WHEN BILL.BILL_CYC_CD=' '                                                                                                            ");
				queryString.append("       THEN SYSTIMESTAMP                                                                                                                    ");
				queryString.append("       ELSE BILL.win_start_dt                                                                                                               ");
				queryString.append("     END AS BILL_START_DT,                                                                                                                  ");
				queryString.append("     CASE                                                                                                                                   ");
				queryString.append("       WHEN BILL.BILL_CYC_CD=' '                                                                                                            ");
				queryString.append("       THEN SYSTIMESTAMP                                                                                                                    ");
				queryString.append("       ELSE BILL.BILL_DT                                                                                                                    ");
				queryString.append("     END AS BILL_END_DT,                                                                                                                    ");
				queryString.append("     bill.ALT_BILL_ID,                                                                                                                      ");
				queryString.append("     BILL.BILL_DT,                                                                                                                          ");
				queryString.append("     (SELECT srch_char_val                                                                                                                  ");
				queryString.append("     FROM ci_bill_char bchar                                                                                                                ");
				queryString.append("     WHERE bchar.bill_id   =bill.bill_id                                                                                                    ");
				queryString.append("     AND bchar.CHAR_TYPE_CD='BILL_AMT'                                                                                                      ");
				queryString.append("     ) AS bill_amt,                                                                                                                         ");
				queryString.append("     acct.CURRENCY_CD,                                                                                                                      ");
				queryString.append("     bill.CR_NOTE_FR_BILL_ID,                                                                                                               ");
				queryString.append("     sysdate AS UPLOAD_DTTM,                                                                                                                ");
				queryString.append("     :yesStatus     AS EXTRACT_FLG,                                                                                                         ");
				queryString.append("     NULL    AS EXTRACT_DTTM,                                                                                                               ");
				queryString.append("     (SELECT acct_nbr                                                                                                                       ");
				queryString.append("     FROM ci_acct_nbr acctnbr                                                                                                               ");
				queryString.append("     WHERE acctnbr.acct_id       =bill.acct_id                                                                                              ");
				queryString.append("     AND acctnbr.ACCT_NBR_TYPE_CD=:acctNbrTypeCd                                                                                            ");
				queryString.append("     )       AS acct_type,                                                                                                                  ");
				queryString.append("     sysdate AS ILM_DT,                                                                                                                     ");
				queryString.append("     'Y'    AS ILM_ARCH_SW                                                                                                                  ");
				queryString.append("   FROM ci_bill bill,                                                                                                                       ");
				queryString.append("     ci_acct acct                                                                                                                           ");
				queryString.append("   WHERE bill.acct_id=acct.acct_id                                                                                                          ");
				queryString.append("   AND bill.bill_id  =:billId                                                                                                               ");
				queryString.append("   ) tmp                                                                                                                                    ");
				preparedStatement = createPreparedStatement(queryString.toString(),"");
				BigInteger eventType = new BigInteger(eventPriceDataInterfaceLookUp.getEventTypeId().trim());
				preparedStatement.bindString("externalPartyId", eventPriceDataInterfaceLookUp.getExternalPartyId().trim(), "ID_TYPE_CD");
				preparedStatement.bindString("yesStatus", eventPriceDataInterfaceLookUp.getExportFlag().trim(), "EXTRACT_FLG");
				preparedStatement.bindBigInteger("eventTypeId", eventType);
				preparedStatement.bindString("eventProcessId", eventPriceDataInterfaceLookUp.getEventProcessId().trim(), "EVENT_PROCESS_ID");
				preparedStatement.bindString("acctNbrTypeCd", eventPriceDataInterfaceLookUp.getAcctNbrTypeCode().trim(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindId(BILL_ID, bill_Id);
				count=preparedStatement.executeUpdate();
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillIdMAp() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillIdMAp() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in Bill ID Map"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return count;
		}
		
		/**
		 * This method will insert record in CM_BILL_PAYMENT_DTL table
		 * @param bill_Id
		 * @return int
		 */
		private int insertInBillPayDtl(Bill_Id bill_Id ) {
			PreparedStatement preparedStatement = null;
			int count=0;
			try{
				StringBuilder queryString = new StringBuilder();	
				queryString.append(" INSERT                                                  ");
				queryString.append(" INTO CM_BILL_PAYMENT_DTL                                ");
				queryString.append("   (                                                     ");
				queryString.append("     PAY_DTL_ID,                                         ");
				queryString.append("     UPLOAD_DTTM,                                        ");
				queryString.append("     PAY_DT,                                             ");
				queryString.append("     BILL_ID,                                            ");
				queryString.append("     LINE_ID,                                            ");
				queryString.append("     LINE_AMT,                                           ");
				queryString.append("     PAY_TYPE,                                           ");
				queryString.append("     PREV_UNPAID_AMT,                                    ");
				queryString.append("     PAY_AMT,                                            ");
				queryString.append("     UNPAID_AMT,                                         ");
				queryString.append("     CURRENCY_CD,                                        ");
				queryString.append("     STATUS_CD,                                          ");
				queryString.append("     STATUS_UPD_DTTM,									 ");
				queryString.append("     EXT_TRANSMIT_ID,                                    ");
				queryString.append("     ILM_DT,                                             ");
				queryString.append("     OVERPAID,                                           ");
				queryString.append("     RECORD_STAT                                         ");
				queryString.append("   )                                                     ");
				queryString.append(" SELECT PAY_DTL_ID_SQ.NEXTVAL AS PAY_DTL_ID,             ");
				queryString.append("   SYSTIMESTAMP               AS UPLOAD_DTTM,            ");
				queryString.append("   SYSTIMESTAMP               AS PAY_DT,                 ");
				queryString.append("   :bill_id                   AS bill_id,                ");
				queryString.append("   '1'                        AS LINE_ID,                ");
				queryString.append("   bchar.srch_char_val        AS line_amt,               ");
				//NAP-36699
				queryString.append("   (CASE WHEN bchar.srch_char_val<=0 THEN :cr WHEN bchar.srch_char_val>0 THEN :dr END)  AS PAY_TYPE,  ");
				queryString.append("   bchar.srch_char_val        AS PREV_UNPAID_AMT,        ");
				queryString.append("   0                          AS PAY_AMT,                ");
				queryString.append("   bchar.srch_char_val        AS UNPAID_AMT,             ");
				queryString.append("   (SELECT currency_cd                                   ");
				queryString.append("   FROM ci_acct                                          ");
				queryString.append("   WHERE acct_id =                                       ");
				queryString.append("     (SELECT acct_id FROM ci_bill WHERE bill_id=:bill_id ");
				queryString.append("     )                                                   ");
				queryString.append("   )               AS currency_cd,                       ");
				queryString.append("   'MIGRATED_DEBT' AS STATUS_CD,                         ");
				queryString.append("   SYSTIMESTAMP    AS STATUS_UPD_DTTM,        	  		 ");
				queryString.append("   NULL            AS EXT_TRANSMIT_ID,                   ");
				queryString.append("   SYSTIMESTAMP    AS ILM_DT,                            ");
				queryString.append("   NULL            AS OVERPAID,                          ");
				queryString.append("   'MIGRATED_DEBT' AS RECORD_STAT                        ");
				queryString.append(" FROM ci_bill_char bchar                                 ");
				queryString.append(" WHERE bchar.bill_id   = :bill_id                        ");
				queryString.append(" AND bchar.CHAR_TYPE_CD='BILL_AMT'                       ");
				preparedStatement = createPreparedStatement(queryString.toString(),"");
				preparedStatement.bindId("bill_id", bill_Id);
				preparedStatement.bindString("cr", "CR", "srch_char_val");
				preparedStatement.bindString("dr", "DR", "srch_char_val");
				count=preparedStatement.executeUpdate();
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillPayDtl() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillPayDtl() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in Bill Payment detail table"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return count;
		}
		
		/**
		 * This method will insert record in CM_BILL_DUE_DT table
		 * @param bill_Id
		 * @param debtDate
		 * @param eventId
		 * @return int
		 */
		private int insertInBillDueDt(Bill_Id bill_Id,Date debtDate,String eventId ) {
			PreparedStatement preparedStatement = null;
			int count=0;
			try{
				StringBuilder queryString = new StringBuilder();	
				queryString.append(" INSERT                   "); 
				queryString.append(" INTO CM_BILL_DUE_DT      ");
				queryString.append("   (                      ");
				queryString.append("     BANK_ENTRY_EVENT_ID, ");
				queryString.append("     BILL_ID,             ");
				queryString.append("     DUE_DT,              ");
				queryString.append("     IS_MERCH_BALANCED,   ");
				queryString.append("     UPLOAD_DTTM,         ");
				queryString.append("     STATUS_UPD_DTTM,     ");
				queryString.append("     PAY_DT,              ");
				queryString.append("     LINE_ID,             ");
				queryString.append("     BANKING_ENTRY_STATUS ");
				queryString.append("   )                      ");
				queryString.append("   VALUES                 ");
				queryString.append("   (                      ");
				queryString.append("     :eventId,            ");
				queryString.append("     :billId,             ");
				queryString.append("     :debtDt,              ");
				queryString.append("     'N',                 ");
				queryString.append("     SYSTIMESTAMP,        ");
				queryString.append("     NULL,                ");
				queryString.append("     SYSTIMESTAMP,        ");
				queryString.append("     '1',                 ");
				queryString.append("     'DEBT_MIGRATION'      ");
				queryString.append("   )                     ");
				preparedStatement = createPreparedStatement(queryString.toString(),"");
				preparedStatement.bindString("eventId",eventId, "BANK_ENTRY_EVENT_ID");
				preparedStatement.bindDate("debtDt",debtDate );
				preparedStatement.bindId(BILL_ID, bill_Id);
				count=preparedStatement.executeUpdate();
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in Bill due date table"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return count;
		}
		
		public boolean eligibleForPayReq(Bill_Id billId){
			boolean flag = false;
			PreparedStatement preparedStatement = null;
			try{
				StringBuilder queryString = new StringBuilder();	
				queryString.append(" SELECT BILL_ID FROM CI_BILL_CHAR "); 
				queryString.append(" WHERE BILL_ID = :billId ");
				queryString.append(" AND CHAR_TYPE_CD = :charTypeCd ");
				queryString.append(" AND trim(SRCH_CHAR_VAL) = 'MIGCHRG2' ");
				
				preparedStatement = createPreparedStatement(queryString.toString(),"");
				preparedStatement.bindId(BILL_ID, billId);
				preparedStatement.bindString("charTypeCd", "RUN_TOT", "CHAR_TYPE_CD");
				preparedStatement.setAutoclose(false); 
				SQLResultRow sqlRow=preparedStatement.firstRow();
				if (sqlRow != null){				
					flag = true;
				}
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in Bill due date table"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return flag;
		}
		
		private void insertNonZeroBillChar(Bill_Id billId, String charVal) {
			PreparedStatement preparedStatement = null;
			try{
				StringBuilder queryString = new StringBuilder();	
				queryString.append(" INSERT INTO CI_BILL_CHAR "); 
				queryString.append(" VALUES (:billId, ");
				queryString.append(" 'NON_ZERO', ");
				queryString.append(" 1,1, ");
				queryString.append(" :charVal ");
				queryString.append(" ,' ',' ',' ',' ',' ',' ',' ') ");
				preparedStatement = createPreparedStatement(queryString.toString(),"");
				preparedStatement.bindId(BILL_ID, billId);
				preparedStatement.bindString("charVal", charVal, "CHAR_VAL");
				preparedStatement.executeUpdate();
			}catch (ThreadAbortedException e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside insertInBillDueDt() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error while inserting in Bill due date table"));
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

	}

}
