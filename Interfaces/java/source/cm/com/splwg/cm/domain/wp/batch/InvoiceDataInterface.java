/*******************************************************************************
 1.0      NA             May 09, 2017        Ankur 		  PAM-12774 fixed performance issue
 1.1      NA             May 22, 2017        Ankur		  PAM-12834 Gather statics implementation
 1.2      NA             Jun 08, 2017        Ankur		  PAM-13423 Performance issue fix.
 1.3      NA             Jun 14, 2017        Preeti		  Summary Invoice implementation.
 1.4      NA             Jun 22, 2017        Vienna		  NAP-15568 Renamed nested Id class
 1.5      NA             Jul 24, 2017        Ankur		  PAM-14311 performance issue fix
 1.6      NA             Aug 10, 2017        Ankur		  PAM-14311 Roll back parallel hint
 1.7      NA             Aug 18, 2017        Ankur		  Redesign Implementation
 1.8      NA             Sep 01, 2017        Ankur		  PAM-13530 & PAM-13987 Fix
 1.9      NA             Sep 14, 2017        Ankur		  PAM-14587 Fix
 2.0      NA             Sep 21, 2017        Ankur		  PAM-15336 Fix
 2.1      NA             Sep 22, 2017        Ankur		  PAM-15123 Fix
 2.2      NA             Sep 25, 2017        Ankur		  PAM-15411 Fix
 2.3      NA             Oct 10, 2017        Preeti		  Added Non Zero Balance Migration logic
 2.4      NA             Oct 25, 2017        Ankur		  PAM-15984 Fix
 2.5      NA             Nov 09, 2017        Ankur		  PAM-16182 Fix
 2.6      NA             Nov 13, 2017        Ankur		  Rolled back PAM-16182 Fix.
 2.7      NA             Jan 24, 2018        Preeti		  NAP-21722/NAP-22184: Utilize Txn and FT mapping.
 2.8      NA             Feb 02, 2018        Vienna        NAP-22076 populate ILM_DT and ILM_ARCH_SW
 2.9      NA             Feb 12, 2018        Vienna        NAP-21881 perf changes
 3.0      NA             Mar 13, 2018        Vienna        NAP-22076 ILM_ARCH_SW must be Y
 3.1      NA             Apr 04, 2018        Ankur         NAP-25225 & NAP-24635 wrong pricing currency fix
 3.2      NA             Apr 09, 2018        Ankur         NAP-25484 remove CM_BILL_CYC_SCH
 3.3      NA             Apr 13, 2018        Ankur         PAM-17718 Fix
 3.4		 NA				Apr 16, 2018		Nitika		  NAP-25164/NAP-24284 Overpayment fix
 3.5      NA             Apr 20, 2018        Ankur         NAP-26013 Fix & NAP-26095 Fix
 3.6      NA             Apr 25, 2018        Ankur         NAP-26141 RECRRATE issue fix
 3.7      NA             Apr 26, 2018        Ankur         NAP-26030 F_M_AMT signage change for refund
 3.8      NA             May 01, 2018        Ankur         NAP-26526 & NAP-26537 changed to complete_dttm
 3.9      NA             May 02, 2018        Ankur         NAP-26684 used SYSTIMESTAMP in place of TRUNC(SYSTIMESTAMP)
 4.0      NA             May 08, 2018        Ankur         NAP-26905 Incorrect signage issue for refund rate
 4.1      NA             May 14, 2018        Ankur         NAP-27294 Fixed
 4.2		 NA				Jul 05, 2018        Somya         NAP-27307 Fixed
 4.3		 NA				Sep 12, 2018		Amandeep 	  Removal Of ILM_DATE in job work query
 4.4		 NA				Sep 17, 2018		Somya		  Add PRICE_ASGN_ID to CM_INV_DATA_LN_RATE table, changes to map VAT reg based on country
 4.5 	 NAP-33633 		Oct 15, 2018		Amandeep	  L0 Product Mapped from CM_TXN_ATTRIBUTES_MAP
 4.6		 NAP-34677      Dec 03, 2018		Somya		  Added logic to insert value amount depending on merchant hierarchy in CM_INV_BSEG_RATE_INFO table
 4.7		 NAP-38214		Jan 02, 2019		Somya		  Added NVL to prevent SQL error while inserting null value_amt in CM_INV_BSEG_RATE_INFO table
 4.8		 NAP-39105		Jan 17, 2019		Vikalp		  Removal of Seq No from Invoice BCL info table
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.BillSegmentStatusLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author rainas403
 *
 * @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = billDate, type = date)
 *            , @BatchJobSoftParameter (name = chunkSize, required = true, type = integer)})
 *
 */
public class InvoiceDataInterface extends InvoiceDataInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(InvoiceDataInterface.class);

	private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = null;

	/**
	 * getJobWork() method passes data for processing to the Worker inner class by
	 * the framework.
	 */
	@Override
	public JobWork getJobWork() {

		invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();
		// To delete from error table
		deleteFromTable(invoiceDataInterfaceLookUp.getInvDataErr());
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		threadWorkUnitList = getInvoiceData();
		invoiceDataInterfaceLookUp = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * deleteFromTable() method will delete from the table provided as input.
	 *
	 * @param inputTable
	 */

	private void deleteFromTable(String inputTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("DELETE FROM " + inputTable
							+ " E WHERE EXISTS (SELECT 1 FROM CM_INVOICE_DATA D WHERE E.BILL_ID =BILL_ID AND EXTRACT_FLG =:yes)",
					"deleteFromTable");
			preparedStatement.bindString("yes", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
			preparedStatement.executeUpdate();

		} catch (RuntimeException e) {
			logger.error("Inside deleteFromTable() method of InvoiceDataInterface, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured while deleting from " + inputTable + " - " + e.toString() ));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	// *********************** getInvoiceData Method******************************

	/**
	 * getInvoiceData() method selects Bill Ids for processing by this Interface.
	 *
	 * @return List Invoice_Data_id
	 */

	private List<ThreadWorkUnit> getInvoiceData() {
		PreparedStatement preparedStatement = null;
		DateTime maxUploadDttm = null;
		DateTime ilmDateTime = getSystemDateTime();
		String invLowBillid = "";
		String invHighBillId = "";
		int chunkSize = getParameters().getChunkSize().intValue();
		int threadCount = getParameters().getThreadCount().intValue();
		Date billDate = getParameters().getBillDate();
		StringBuilder stringBuilder = new StringBuilder();
		InvoiceData_Id invoiceData = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		String table="CM_BSEG_INFO";

		truncateBsegInfo(table);



		// Clear temp table
		deleteTempTable();
		// This will determine maximum upload date of CM_INVOICE_DATA table
		maxUploadDttm = getMaxCreateDttm();
		// Load Temp table
		loadTempTable(billDate, maxUploadDttm);
		// fetching chunckSize
		chunkSize = getChunkSize(billDate, chunkSize, threadCount, maxUploadDttm);

		/**
		 * select all completed bills whose creation date is greater than bills already
		 * present in CM_INVOICE_DATA table and using chunk size create worker units
		 * which will have lower and higher bill id.
		 */

		try {

			stringBuilder.append("WITH TBL AS (SELECT  BILL_ID FROM  CM_INVOICE_BILL_ID ");
			stringBuilder.append("ORDER BY BILL_ID)  ");
			stringBuilder.append("SELECT THREAD_NUM, MIN(BILL_ID) AS LOW_INV_BILL_ID, ");
			stringBuilder.append("MAX(BILL_ID) AS HIGH_INV_BILL_ID FROM (SELECT BILL_ID, ");
			stringBuilder.append("CEIL((ROWNUM)/:CHUNKSIZE) AS THREAD_NUM FROM TBL) ");
			stringBuilder.append("GROUP BY THREAD_NUM ORDER BY 1 ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.bindBigInteger("CHUNKSIZE", new BigInteger(String.valueOf(chunkSize)));
			preparedStatement.setAutoclose(false);

			for (SQLResultRow sqlRow : preparedStatement.list()) {
				invLowBillid = sqlRow.getString("LOW_INV_BILL_ID");
				invHighBillId = sqlRow.getString("HIGH_INV_BILL_ID");
				invoiceData = new InvoiceData_Id(invLowBillid, invHighBillId);

				// *************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(invoiceData);
				threadworkUnit.addSupplementalData("maxUploadDttm", maxUploadDttm);
				threadworkUnit.addSupplementalData("ilmDateTime", ilmDateTime);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				invoiceData = null;
				// *************************

			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getInvoiceData() method of InvoiceDataInterface, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occurred in getJobWork() while picking Bill Ids - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside invoice data interface method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}

		return threadWorkUnitList;
	}


	private void truncateBsegInfo(String inputBsegTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("TRUNCATE TABLE "+ inputBsegTable,"");
			preparedStatement.execute();
		}
		catch (Exception e) {
			logger.error("Inside truncateFromBsegTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public void deleteTempTable() {

		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			stringBuilder.append(" delete from cm_invoice_bill_id ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.execute();
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteTempTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured while deleting from CM_INVOICE_BILL_ID - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside getJobWork() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		try {
			preparedStatement = createPreparedStatement("commit", "");
			preparedStatement.execute();

		} catch (RuntimeException e) {
			logger.error("Inside getJobWork() method, Error - ", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured in commit query  inside  deleteTempTable() method - " + e.toString()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public void loadTempTable(Date billDate, DateTime maxUploadDttm) {

		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			stringBuilder.append(" INSERT INTO cm_invoice_bill_id ");
			stringBuilder.append(
					" SELECT bill_id, alt_bill_id, acct_id, bill_dt, grp_ref_val, win_start_dt, cr_note_fr_bill_id, bill_cyc_cd ");
			stringBuilder.append(" FROM ci_bill b  ");
			stringBuilder.append(" WHERE bill_stat_flg = :billStatusFlag ");
			stringBuilder.append(" AND cr_note_fr_bill_id = ' ' ");
			if (billDate == null) {
				stringBuilder.append(" AND (complete_dttm > :maxUploadDttm ");
			} else {
				stringBuilder.append(" AND (bill_dt =:billDt  ");
			}
			stringBuilder.append(" AND NOT EXISTS (SELECT 'X' FROM cm_invoice_data WHERE b.bill_id = bill_id)) ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			if (billDate == null) {
				preparedStatement.bindDateTime("maxUploadDttm", maxUploadDttm);
			} else {
				preparedStatement.bindDate("billDt", billDate);
			}
			preparedStatement.bindString("billStatusFlag", invoiceDataInterfaceLookUp.getBillStatusFlag().trim(),
					"BILL_STAT_FLG");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows inserted into table cm_invoice_bill_id - " + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside loadTempTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured while inserting records in CM_INVOICE_BILL_ID - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside loadTempTable() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		try {
			preparedStatement = createPreparedStatement("commit", "");
			preparedStatement.execute();

		} catch (RuntimeException e) {
			logger.error("Inside loadTempTable() method, Error - ", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured in commit query inside loadTempTable() method - " + e.toString()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public DateTime getMaxCreateDttm() {

		PreparedStatement preparedStatement = null;
		DateTime maxUploadDttm = null;
		try {
			preparedStatement = createPreparedStatement(
					"SELECT MAX(ILM_DT) AS UPLOAD_DTTM FROM CM_INVOICE_DATA  WHERE CR_NOTE_FR_BILL_ID IS NULL", "");
			preparedStatement.setAutoclose(false);
			SQLResultRow sqlResultRow = preparedStatement.firstRow();
			if (notNull(sqlResultRow)) {
				maxUploadDttm = sqlResultRow.getDateTime("UPLOAD_DTTM");
			}
		} catch (RuntimeException e) {
			throw new RunAbortedException(
					CustomMessageRepository.exceptionInExecution("While fetching MAX(ILM_DT) from CM_INVOICE_DATA - " + e.toString()));
		} catch (Exception e) {
			logger.error("Inside getInvoiceData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return maxUploadDttm;
	}

	public int getChunkSize(Date billDate, int chunkSize, int threadCount, DateTime maxUploadDttm) {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		BigInteger maxRecordcount = BigInteger.valueOf(0);
		int countPerThread;
		try {
			stringBuilder.append("SELECT COUNT(B.BILL_ID) AS COUNT FROM CI_BILL B ");
			stringBuilder.append("WHERE BILL_STAT_FLG= :billStatusFlag AND CR_NOTE_FR_BILL_ID=' ' ");
			if (billDate == null) {
				stringBuilder.append("AND (COMPLETE_DTTM > :maxUploadDttm  ");
			} else {
				stringBuilder.append("AND (BILL_DT =:billDt ");
			}
			stringBuilder.append("AND NOT EXISTS(SELECT 1 FROM CM_INVOICE_DATA WHERE B.BILL_ID = BILL_ID)) ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");

			if (billDate == null) {
				preparedStatement.bindDateTime("maxUploadDttm", maxUploadDttm);
			} else {
				preparedStatement.bindDate("billDt", billDate);
			}

			preparedStatement.bindString("billStatusFlag", invoiceDataInterfaceLookUp.getBillStatusFlag().trim(),
					"BILL_STAT_FLG");
			preparedStatement.setAutoclose(false);
			SQLResultRow sqlRow = preparedStatement.firstRow();
			maxRecordcount = sqlRow.getInteger("COUNT");
			countPerThread = (int) Math.ceil(maxRecordcount.doubleValue() / threadCount);
			if (countPerThread > chunkSize) {
				chunkSize = countPerThread;
			}

		} catch (RuntimeException e) {
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution("Error occured inside getChunkSize() method - " + e.toString()));
		} catch (Exception e) {
			logger.error("Calling gather count, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return chunkSize;
	}

	public Class<InvoiceDataInterfaceWorker> getThreadWorkerClass() {
		return InvoiceDataInterfaceWorker.class;
	}

	public static class InvoiceDataInterfaceWorker extends InvoiceDataInterfaceWorker_Gen {
		private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = null;
		String errMsg;

		// Default constructor
		public InvoiceDataInterfaceWorker() {
			// Default constructor
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by the
		 * framework per thread.
		 */
		@Override
		public void initializeThreadWork(boolean arg0) throws ThreadAbortedException, RunAbortedException {
			errMsg = null;
			logger.debug("Inside initializeThreadWork() method for batch thread number: " + getBatchThreadNumber());
			if (invoiceDataInterfaceLookUp == null) {
				invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();
			}
			super.initializeThreadWork(arg0);
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.debug("Inside createExecutionStrategy() method");
			return new StandardCommitStrategy(this);
		}

		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {

			logger.debug("Inside executeWorkUnit() for thread 1 number - " + getBatchThreadNumber());

			String invLowBillId = "";
			String invHighBillId = "";
			DateTime maxUploadDttm = (DateTime) unit.getSupplementallData("maxUploadDttm");
			DateTime ilmDateTime = (DateTime) unit.getSupplementallData("ilmDateTime");
			Date billDate = getParameters().getBillDate();
			PreparedStatement preparedStatement = null;
			InvoiceData_Id invoiceData = (InvoiceData_Id) unit.getPrimaryId();
			invLowBillId = invoiceData.getInvLowBillId();
			invHighBillId = invoiceData.getInvHighBillId();

			/**
			 * Fetch bill information like bill id,alternative bill id,billing
			 * account,billing division,billing account type, billing date, bill cycle &
			 * schedule,billing currency,billing amount & world pay tax registration number
			 * in CM_INV_BILL_ACCT_INFO
			 */

			// insert into CM_INV_BILL_ACCT_INFO
			insertIntoCMInvBillAcctInfo(billDate, maxUploadDttm, invLowBillId, invHighBillId);

			/**
			 * Copy valid bill information from CM_INV_BILL_ACCT_INFO and fetch additional
			 * information like billing party identifier,merchant tax registration number &
			 * world pay business unit into CM_INV_BILL_PER_INFO
			 */
			insertIntoCMInvBillPerInfo();


			/**
			 * Copy valid bill information from CM_INV_BILL_PER_INFO and fetch addition
			 * information like tax authority & tax type into CM_INV_BILL_TAX_INFO
			 */
			insertIntoCmInvBillTaxInfo();

			/**
			 * Fetch adjustment related information for bills like adjustment
			 * identifier,adjustment amount,adjustment type,adjustment currency,adjustment
			 * description.
			 */

			insertIntoCMInvBillAdjInfo();

			/**
			 * For bill retrieved in CM_INV_BILL_PER_INFO,Fetch bill segments which have Tax
			 * info.Fetch bill identifier, bill segment identifier,tax rate,tax status,tax
			 * status character,tax status character description & bill segment sequence
			 * number into CM_INV_BSEG_TAX_INFO
			 */

			insertIntoCmInvBsegTaxInfo();

			/*********** Insert data in Invoice Error table *************************/

			try {
				insertIntoInvErrorTable();
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit() : Inserting data in Invoice Error table - ", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Exception occurred while Inserting data in Invoice Error table - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}

			// new temp table for bseg_info
			insertIntoCMBsegInfo(ilmDateTime);

			/*********** Insert data in Invoice Staging tables *************************/

			try {
				insertIntoInvoiceStagingTables(ilmDateTime);
			} catch (Exception e) {
				logger.error("Exception in executeWorkUnit() : Inserting data in Invoice Staging tables - ", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Exception occurred while Inserting data in Invoice Staging tables inside " +
								"insertIntoInvoiceStagingTables() method - " + e.toString()));

			} finally {
				closeConnection(preparedStatement);

			}

			return true;
		}


		private void insertIntoCMBsegInfo(DateTime ilmDateTime) {
			// TODO Auto-generated method stub
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			try {
				stringBuilder.append(
						"INSERT INTO CM_BSEG_INFO(BILL_ID,BILL_DT,BSEG_ID,BILLABLE_CHG_ID,SA_ID,ACCT_ID,ACCT_TYPE,BSEG_STAT_FLG, ");
				stringBuilder.append(
						"REBILL_SEG_ID,PRICEITEM_CD,PRICE_ASGN_ID,BSEG_TYPE_FLG,CURRENCY_CD,RS_CD,STATUS_COD,CALC_AMT,ILM_DT) ");
				stringBuilder.append("SELECT BSEG.BILL_ID,TMP1.BILL_DT,BSEG.BSEG_ID,CALC.BILLABLE_CHG_ID,BSEG.SA_ID, ");
				stringBuilder.append("TMP1.ACCT_ID,TMP1.ACCT_TYPE,BSEG.BSEG_STAT_FLG,BSEG.REBILL_SEG_ID, ");
				stringBuilder.append("EXT.PRICEITEM_CD,EXT.PRICE_ASGN_ID,EXT.BSEG_TYPE_FLG, ");
				stringBuilder.append("TMP1.CURRENCY_CD ,CALC.RS_CD,TMP1.STATUS_COD,CALC.CALC_AMT,:ilmDateTime ");
				stringBuilder.append("FROM CI_BSEG BSEG,CI_BSEG_CALC CALC ,CI_BSEG_EXT EXT ,CM_INV_BILL_PER_INFO TMP1 ");
				stringBuilder.append("WHERE BSEG.BILL_ID=TMP1.BILL_ID ");
				stringBuilder.append("AND CALC.BSEG_ID= EXT.BSEG_ID ");
				stringBuilder.append("AND BSEG.BSEG_ID = CALC.BSEG_ID ");
				stringBuilder.append("AND CALC.RS_CD <> 'TAX     ' ");
				stringBuilder.append("AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=BSEG.BILL_ID) ");

				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();

			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table cm_bseg_info - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table cm_bseg_info - " + e.toString()));
			} finally {

				closeConnection(preparedStatement);

			}

		}

		public void insertIntoCmInvBsegTaxInfo() {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INV_BSEG_TAX_INFO  ");
				stringBuilder.append("(BILL_ID, BSEG_ID, CALC_AMT, BASE_AMT, CHAR_VAL, TAX_RATE, TAX_STAT, ");
				stringBuilder.append(
						"TAX_STAT_CHAR, TAX_STAT_DESCR, STATUS_COD, MESSAGE_NBR, ERROR_INFO, REVERSE_CHRG_SW ) ");
				stringBuilder.append(
						"SELECT DISTINCT BILLPER.BILL_ID,BSEG.BSEG_ID,LN.CALC_AMT,LN.BASE_AMT,CLCHAR1.CHAR_VAL, ");
				stringBuilder.append(
						"CLCHAR.ADHOC_CHAR_VAL AS TAX_RATE, NVL2(TRIM(REGEXP_SUBSTR (CLCHAR1.CHAR_VAL, '[^-]+', 1, 2)), ");
				stringBuilder.append(
						"TRIM(REGEXP_SUBSTR (CLCHAR1.CHAR_VAL, '[^-]+', 1, 2)),TRIM(CLCHAR1.CHAR_VAL)) AS TAX_STAT, ");
				stringBuilder.append("CLCHAR1.CHAR_TYPE_CD AS TAX_STAT_CHAR, TRIM(VALL.DESCR) AS TAX_STAT_DESCR, ");
				stringBuilder.append(":initStatus  AS STATUS_COD, 0   AS MESSAGE_NBR, ' ' AS ERROR_INFO, ");
				stringBuilder.append(
						"CASE WHEN EXISTS (SELECT 1 FROM CI_BSEG_CL_CHAR WHERE BSEG_ID = BSEG.BSEG_ID AND CHAR_TYPE_CD = 'TX_SCOPE') THEN 'Y' ");
				stringBuilder.append("ELSE 'N' END AS REVERSE_CHRG_SW ");
				stringBuilder.append(
						"FROM CM_INV_BILL_PER_INFO BILLPER, CM_ACCT_REF AR,CI_BSEG BSEG,CI_BSEG_CALC CALC,CI_BSEG_CALC_LN LN,CI_BSEG_CL_CHAR CLCHAR, CI_BSEG_CL_CHAR CLCHAR1, CI_CHAR_VAL_L VALL ");
				stringBuilder.append(
						"WHERE BILLPER.ACCT_TYPE=AR.ACCT_TYPE AND AR.TYPE_VAL=:typeVal AND BILLPER.BILL_ID=BSEG.BILL_ID AND BSEG.BSEG_ID=CALC.BSEG_ID ");
				stringBuilder.append(
						"AND CALC.BILLABLE_CHG_ID=' ' AND RS_CD='TAX     ' AND ((BSEG.BSEG_STAT_FLG =:frozen) OR (BSEG.BSEG_STAT_FLG='60' AND BSEG.REBILL_SEG_ID=' ')) ");
				stringBuilder.append("AND CALC.BSEG_ID=LN.BSEG_ID AND LN.SEQNO=CLCHAR.SEQNO ");
				stringBuilder.append("AND BSEG.BSEG_ID = CLCHAR.BSEG_ID AND CLCHAR.BSEG_ID = CLCHAR1.BSEG_ID ");
				stringBuilder
						.append("AND CLCHAR1.CHAR_VAL = VALL.CHAR_VAL AND CLCHAR1.CHAR_TYPE_CD = VALL.CHAR_TYPE_CD ");
				stringBuilder.append(
						"AND CLCHAR.CHAR_TYPE_CD = :taxRate AND CLCHAR1.CHAR_TYPE_CD LIKE 'TX_S%' AND CLCHAR1.CHAR_TYPE_CD <> :taxScope ");
				stringBuilder.append(
						"AND CLCHAR.SEQNO = CLCHAR1.SEQNO AND VALL.LANGUAGE_CD= :langCode AND BILLPER.STATUS_COD = :initStatus");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(),
						"STATUS_COD");
				preparedStatement.bindString("langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(),
						"LANGUAGE_CD");
				preparedStatement.bindString("taxRate", invoiceDataInterfaceLookUp.getTaxRateCharType().trim(),
						"CHAR_TYPE_CD");
				preparedStatement.bindString("taxScope", invoiceDataInterfaceLookUp.getTaxScope().trim(),
						"CHAR_TYPE_CD");
				preparedStatement.bindString("typeVal", "POST", "TYPE_VAL");
				preparedStatement.bindLookup("frozen", BillSegmentStatusLookup.constants.FROZEN);
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INV_BSEG_TAX_INFO data extraction - ", e);
				errMsg = "Insert err:CM_INV_BSEG_TAX_INFO";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in table CM_INV_BSEG_TAX_INFO - " + e.toString()));
			} catch (Exception e) {
				errMsg = "Insert err:CM_INV_BSEG_TAX_INFO";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in table CM_INV_BSEG_TAX_INFO - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}

		}

		public void closeConnection(PreparedStatement preparedStatement) {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}

		}

		public void executeQuery(StringBuilder query, String message, ArrayList<ArrayList<Object>> list) {
			logger.debug("Executing query: " + message);
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement(query.toString(), "");
				for (ArrayList<Object> objList : list) {
					if (objList != null) {
						if (objList.get(1) instanceof String) {
							preparedStatement.bindString(objList.get(0) + "", objList.get(1) + "", objList.get(2) + "");
						} else if (objList.get(1) instanceof DateTime) {
							preparedStatement.bindDateTime(objList.get(0) + "", (DateTime) objList.get(1));
						} else if (objList.get(1) instanceof BigInteger) {
							preparedStatement.bindBigInteger(objList.get(0) + "", (BigInteger) objList.get(1));
						} else if (objList.get(1) instanceof BigDecimal) {
							preparedStatement.bindBigDecimal(objList.get(0) + "", (BigDecimal) objList.get(1));
						} else if (objList.get(1) instanceof Date) {
							preparedStatement.bindDate(objList.get(0) + "", (Date) objList.get(1));
						} else if (objList.get(1) instanceof CharacteristicType_Id) {
							preparedStatement.bindId(objList.get(0) + "", (CharacteristicType_Id) objList.get(1));
						} else if ("NullableDateTime".equalsIgnoreCase(objList.get(2) + "")) {

							preparedStatement.bindDateTime(objList.get(0) + "", (DateTime) objList.get(1));
						}
					}
				}
				int count = preparedStatement.executeUpdate();
				logger.debug("Rows inserted by query " + message + " are: " + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table/s inside executeQuery() method - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in Table/s inside executeQuery() method - " + e.toString() ));
			}

			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}

		public ArrayList<ArrayList<Object>> addParams(ArrayList<ArrayList<Object>> arrayList, String arg0, Object arg1,
													  String arg2) {
			ArrayList<Object> list = new ArrayList<>();
			list.add(arg0);
			list.add(arg1);
			list.add(arg2);
			arrayList.add(list);
			return arrayList;
		}

		public void insertIntoCMInvBillAcctInfo(Date billDate, DateTime maxUploadDttm, String invLowBillId,
												String invHighBillId) {

			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();
			StringBuilder stringBuilder = new StringBuilder();

			stringBuilder.append(" INSERT INTO CM_INV_BILL_ACCT_INFO ");
			stringBuilder.append(" (BILL_ID, ALT_BILL_ID, ACCT_ID, CIS_DIVISION, ACCT_TYPE, BILL_DT, BILL_CYC_CD,  ");
			stringBuilder.append(" WIN_START_DT, WIN_END_DT, CURRENCY_CD, BILL_AMT, WP_TAX_REG_NBR,  ");
			stringBuilder.append(" CR_NOTE_FR_BILL_ID, STATUS_COD, MESSAGE_NBR, ERROR_INFO)  ");
			stringBuilder.append(
					" SELECT BILL.BILL_ID, BILL.ALT_BILL_ID, BILL.ACCT_ID, ACCT.CIS_DIVISION, NBR.ACCT_NBR AS ACCT_TYPE, BILL.BILL_DT,  ");
			stringBuilder
					.append(" CASE WHEN BILL.GRP_REF_VAL!=' ' THEN 'WPDY' ELSE BILL.BILL_CYC_CD END AS BILL_CYC_CD,  ");
			stringBuilder.append(
					" CASE WHEN BILL.GRP_REF_VAL!=' ' THEN BILL.BILL_DT ELSE BILL.WIN_START_DT END AS WIN_START_DT,  ");
			stringBuilder.append(" BILL.BILL_DT AS WIN_END_DT,  ");
			stringBuilder.append(" ACCT.CURRENCY_CD,  FT.SRCH_CHAR_VAL AS BILL_AMT,  ");
			stringBuilder.append(
					" NVL((SELECT D.CHAR_VAL FROM CI_CIS_DIV_CHAR D, CI_PER P, CI_ACCT_PER AP WHERE P.COUNTRY =D.CHAR_VAL_FK1  ");
			stringBuilder.append(" AND P.PER_ID=AP.PER_ID AND D.CIS_DIVISION=ACCT.CIS_DIVISION  ");
			stringBuilder.append(
					" AND AP.ACCT_ID=ACCT.ACCT_ID AND D.CHAR_TYPE_CD = :taxAgcd),DIVCHAR.CHAR_VAL) AS WP_TAX_REG_NBR,TRIM(CR_NOTE_FR_BILL_ID), ");
			stringBuilder.append(" :initStatus AS STATUS_COD,  ");
			stringBuilder.append(" 0 AS MESSAGE_NBR, ' '  AS ERROR_INFO  ");
			stringBuilder.append(
					" FROM CM_INVOICE_BILL_ID BILL, CI_ACCT ACCT, CI_ACCT_NBR NBR ,CI_CIS_DIV_CHAR DIVCHAR ,CI_BILL_CHAR FT ");
			stringBuilder.append(" WHERE BILL.ACCT_ID = ACCT.ACCT_ID AND NBR.ACCT_ID = ACCT.ACCT_ID  ");
			stringBuilder.append(" AND NBR.ACCT_NBR_TYPE_CD = :acctType   ");
			stringBuilder.append(" AND DIVCHAR.CIS_DIVISION = ACCT.CIS_DIVISION AND DIVCHAR.CHAR_TYPE_CD = :taxAgcd  ");
			stringBuilder.append(
					" AND DIVCHAR.EFFDT =(SELECT MIN(EFFDT) FROM CI_CIS_DIV_CHAR WHERE CIS_DIVISION = ACCT.CIS_DIVISION AND CHAR_TYPE_CD = :taxAgcd)  ");
			stringBuilder.append(" AND FT.BILL_ID = BILL.BILL_ID AND FT.CHAR_TYPE_CD='BILL_AMT'  ");
			stringBuilder.append(
					" AND NOT EXISTS (SELECT 1 FROM CI_BILL_CHAR D where BILL.BILL_ID=D.BILL_ID  AND D.CHAR_TYPE_CD='NON_ZERO') ");
			stringBuilder.append(" AND NOT EXISTS(SELECT 'X' FROM CM_INVOICE_DATA WHERE BILL.BILL_ID = BILL_ID ) ");
			stringBuilder.append(" AND BILL.BILL_ID BETWEEN  :invLowBillId and :invHighBillId  ");

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "taxAgcd", invoiceDataInterfaceLookUp.getTaxAgencyCode().trim(), "CHAR_TYPE_CD");
			addParams(paramsList, "acctType", invoiceDataInterfaceLookUp.getAccountType().trim(), "ACCT_NBR_TYPE_CD");
			addParams(paramsList, "invLowBillId", invLowBillId.trim(), "BILL_ID");
			addParams(paramsList, "invHighBillId", invHighBillId.trim(), "BILL_ID");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_BILL_ACCT_INFO ...", paramsList);

		}

		public void insertIntoCMInvBillPerInfo() {
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();
			StringBuilder stringBuilder = new StringBuilder();

			stringBuilder.append("INSERT INTO CM_INV_BILL_PER_INFO ");
			stringBuilder.append("(BILL_ID,ALT_BILL_ID, ACCT_ID, CIS_DIVISION, ACCT_TYPE, BILL_DT, BILL_CYC_CD, ");
			stringBuilder.append("WIN_START_DT, WIN_END_DT, CURRENCY_CD, BILL_AMT, WP_TAX_REG_NBR, ");
			stringBuilder.append(
					"CR_NOTE_FR_BILL_ID, BILLING_PARTY_ID, MERCH_TAX_REG_NBR, WPBU, STATUS_COD, MESSAGE_NBR, ERROR_INFO)  ");
			stringBuilder.append(
					"SELECT  TMP1.BILL_ID, TMP1.ALT_BILL_ID, TMP1.ACCT_ID, TMP1.CIS_DIVISION, TMP1.ACCT_TYPE, TMP1.BILL_DT, TMP1.BILL_CYC_CD, ");
			stringBuilder.append(
					"TMP1.WIN_START_DT, TMP1.WIN_END_DT, TMP1.CURRENCY_CD, TMP1.BILL_AMT, TMP1.WP_TAX_REG_NBR, TMP1.CR_NOTE_FR_BILL_ID, ");
			stringBuilder.append(
					"PERID1.PER_ID_NBR AS BILLING_PARTY_ID, SUBSTR(PERID2.PER_ID_NBR, 1, 16)  AS MERCH_TAX_REG_NBR, PERCHAR.ADHOC_CHAR_VAL AS WPBU, ");
			stringBuilder.append(
					"CASE WHEN PERCHAR.ADHOC_CHAR_VAL=' ' THEN :errorStatus ELSE :initStatus END AS STATUS_COD, ");
			stringBuilder.append("CASE WHEN PERCHAR.ADHOC_CHAR_VAL=' ' THEN :messageNbr ELSE 0 END AS MESSAGE_NBR, ");
			stringBuilder.append("CASE WHEN PERCHAR.ADHOC_CHAR_VAL=' ' THEN :errorInfo ELSE ' ' END AS ERROR_INFO ");
			stringBuilder.append(
					"FROM CI_ACCT_PER ACCTPER, CM_INV_BILL_ACCT_INFO TMP1 ,CI_PER_ID PERID1 ,CI_PER_ID PERID2 ,CI_PER_CHAR PERCHAR ");
			stringBuilder.append(
					"WHERE TMP1.ACCT_ID = ACCTPER.ACCT_ID AND PERID1.ID_TYPE_CD = :exprtyId AND PERID1.PER_ID = ACCTPER.PER_ID ");
			stringBuilder.append("AND PERID2.ID_TYPE_CD(+) = :taxAgcd AND PERID2.PER_ID(+) = ACCTPER.PER_ID ");
			stringBuilder.append(
					"AND PERCHAR.CHAR_TYPE_CD = :wpBu AND PERCHAR.PER_ID = ACCTPER.PER_ID AND TMP1.STATUS_COD=:initStatus ");

			addParams(paramsList, "taxAgcd", invoiceDataInterfaceLookUp.getTaxAgencyCode().trim(), "ID_TYPE_CD");
			addParams(paramsList, "exprtyId", invoiceDataInterfaceLookUp.getExternalPartyId().trim(), "ID_TYPE_CD");
			addParams(paramsList, "wpBu", invoiceDataInterfaceLookUp.getBusinessUnit().trim(), "CHAR_TYPE_CD");
			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "errorStatus", invoiceDataInterfaceLookUp.getErrorStatus().trim(), "STATUS_COD");
			addParams(paramsList, "messageNbr",
					new BigInteger(CommonUtils.CheckNull(String.valueOf(CustomMessages.INV_WP_BU))), "");
			addParams(paramsList, "errorInfo", getErrorDescription(CustomMessages.INV_WP_BU), "ERROR_INFO");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_BILL_PER_INFO (BILL_ID,ALT_BILL_ID", paramsList);

		}

		public void insertIntoCmInvBillTaxInfo() {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("INSERT INTO CM_INV_BILL_TAX_INFO  ");
				stringBuilder.append(
						"(TAX_AUTHORITY, TAX_TYPE, BILL_ID, STATUS_COD, MESSAGE_NBR, ERROR_INFO, TAX_BSEG_ID, TAX_RGME) ");
				stringBuilder.append(
						"SELECT DISTINCT TRIM(REGEXP_SUBSTR (TAX_AUTHORITY, '[^-]+', 1, 1)) AS TAX_AUTHORITY , ");
				stringBuilder.append("TRIM(REGEXP_SUBSTR (TAX_AUTHORITY, '[^-]+', 1, 2)) AS TAX_TYPE , BSEG.BILL_ID, ");
				stringBuilder.append(
						":initStatus  AS STATUS_COD, 0   AS MESSAGE_NBR, ' ' AS ERROR_INFO, CLCHAR.BSEG_ID as TAX_BSEG_ID, CLCHAR.CHAR_VAL as TAX_RGME ");
				stringBuilder.append(
						"FROM CI_BSEG BSEG, CI_BSEG_CL_CHAR CLCHAR, CM_COUNTRY CTRY, CM_INV_BILL_PER_INFO TBL1 , CM_ACCT_REF AR ");
				stringBuilder.append(
						"WHERE AR.ACCT_TYPE=TBL1.ACCT_TYPE AND AR.TYPE_VAL =:typeVal AND TBL1.BILL_ID = BSEG.BILL_ID AND BSEG.BSEG_ID = CLCHAR.BSEG_ID ");
				stringBuilder.append("AND CLCHAR.CHAR_TYPE_CD= :taxRgme AND CTRY.TAX_AUTHORITY = CLCHAR.CHAR_VAL ");
				stringBuilder.append(
						"AND TBL1.STATUS_COD=:initStatus AND ((BSEG.BSEG_STAT_FLG =:frozen) OR (BSEG.BSEG_STAT_FLG='60' AND BSEG.REBILL_SEG_ID=' '))");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("taxRgme", invoiceDataInterfaceLookUp.getTaxRegime().trim(),
						"CHAR_TYPE_CD");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(),
						"STATUS_COD");
				preparedStatement.bindString("typeVal", "POST", "TYPE_VAL");
				preparedStatement.bindLookup("frozen", BillSegmentStatusLookup.constants.FROZEN);
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INV_BILL_TAX_INFO data extraction - ", e);
				errMsg = "Insert err:CM_INV_BILL_TAX_INFO";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BILL_TAX_INFO - " + e.toString()));
			} catch (Exception e) {
				errMsg = "Insert err:CM_INV_BILL_TAX_INFO";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_BILL_TAX_INFO - " + e.toString()));
			} finally {
				closeConnection(preparedStatement);
			}
		}

		public void insertIntoCMInvBillAdjInfo() {
			ArrayList<ArrayList<Object>> paramsList = new ArrayList<>();
			StringBuilder stringBuilder = new StringBuilder();

			stringBuilder.append("INSERT INTO CM_INV_BILL_ADJ_INFO ");
			stringBuilder.append(
					"(BILL_ID, ADJ_ID, ADJ_AMT, ADJ_TYPE_CD, CURRENCY_CD, DESCR,STATUS_COD, MESSAGE_NBR, ERROR_INFO) ");
			stringBuilder.append("SELECT TBL1.BILL_ID, AD.ADJ_ID, AD.ADJ_AMT, ");
			stringBuilder.append(
					"ADJ.ADJ_TYPE_CD, AD.CURRENCY_CD, ADJ.DESCR,:initStatus AS STATUS_COD, 0 AS MESSAGE_NBR, ' ' AS ERROR_INFO ");
			stringBuilder.append("FROM CI_ADJ_CHAR FT, CI_ADJ_TYPE_L ADJ, CI_ADJ AD, CM_INV_BILL_PER_INFO TBL1 ");
			stringBuilder.append("WHERE ADJ.ADJ_TYPE_CD = AD.ADJ_TYPE_CD AND FT.ADJ_ID=AD.ADJ_ID ");
			stringBuilder.append(
					"AND ADJ.LANGUAGE_CD = :langCode AND FT.SRCH_CHAR_VAL = TBL1.BILL_ID AND FT.CHAR_TYPE_CD='BILL_ID' ");
			stringBuilder.append("AND TBL1.STATUS_COD = :initStatus ");

			addParams(paramsList, "initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(), "STATUS_COD");
			addParams(paramsList, "langCode", invoiceDataInterfaceLookUp.getLanguageCode().trim(), "LANGUAGE_CD");
			executeQuery(stringBuilder, "INSERT INTO CM_INV_BILL_ADJ_INFO O (BILL_ID...", paramsList);

		}

		public void commit() {
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement(" COMMIT", "");
				preparedStatement.execute();
			} catch (RuntimeException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occurred while committing records"));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		public void insertIntoInvErrorTable() {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String messageCategoryNumber = CommonUtils.CheckNull(String.valueOf(CustomMessages.MESSAGE_CATEGORY));

			/**
			 * Error out all the bills which do not have window start date for a bill in
			 * CM_INV_BILL_PER_INFO
			 */

			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_ERR ");
				stringBuilder.append("(BILL_ID, BSEG_ID, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder.append("SELECT BILL_ID,'',:msgCatNbr, :messageNbr, :errorInfo ");
				stringBuilder
						.append("FROM CM_INV_BILL_PER_INFO WHERE WIN_START_DT IS NULL AND STATUS_COD=:initStatus ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus().trim(),
						"STATUS_COD");
				preparedStatement.bindBigInteger("msgCatNbr", new BigInteger(messageCategoryNumber));
				preparedStatement.bindBigInteger("messageNbr",
						new BigInteger(CommonUtils.CheckNull(String.valueOf(CustomMessages.INV_WIN_START_DT_END_DT))));
				preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.INV_WIN_START_DT_END_DT),
						"ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query 2 for CM_INV_DATA_ERR data insertion - ", e);
				errMsg = "Insertion err:CM_INV_DATA_ERR ";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_DATA_ERR table - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				errMsg = "Insertion err:CM_INV_DATA_ERR";
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			/**
			 * Insert all the bills which got errored out as a part of CM_INV_BILL_PER_INFO
			 * into CM_INV_DATA_ERR table
			 */

			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_ERR ");
				stringBuilder.append("(BILL_ID, BSEG_ID, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder
						.append("SELECT DISTINCT TBL1.BILL_ID, '', :msgCatNbr, TBL1.MESSAGE_NBR, TBL1.ERROR_INFO ");
				stringBuilder.append("FROM CM_INV_BILL_PER_INFO TBL1 WHERE STATUS_COD = :errorStatus ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindBigInteger("msgCatNbr", new BigInteger(messageCategoryNumber));
				preparedStatement.bindString("errorStatus", invoiceDataInterfaceLookUp.getErrorStatus().trim(),
						"STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query 3 for CM_INV_DATA_ERR data insertion - ", e);
				errMsg = "Insertion err:CM_INV_DATA_ERR";
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution("Error occurred while inserting records in CM_INV_DATA_ERR table - " + e.toString()));
			} catch (Exception e) {
				logger.error("Inside invoice data interface method, Error -", e);
				errMsg = "Insertion err:CM_INV_DATA_ERR";
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

		}

		public void insertIntoInvoiceStagingTables(DateTime ilmDateTime) {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			/**
			 * Insert all the relevant information from CM_INV_BILL_PER_INFO &
			 * CM_INV_BILL_TAX_INFO into CM_INVOICE_DATA for bills which do not exists in
			 * CM_INV_DATA_ERR table
			 */

			try {
				stringBuilder.append("INSERT INTO CM_INVOICE_DATA ");
				stringBuilder.append("(BILL_ID, ALT_BILL_ID, BILLING_PARTY_ID, CIS_DIVISION, ACCT_TYPE, WPBU,   ");
				stringBuilder.append("BILL_DT, BILL_CYC_CD, WIN_START_DT, WIN_END_DT, CURRENCY_CD, CALC_AMT,    ");
				stringBuilder.append(
						"MERCH_TAX_REG_NBR, WP_TAX_REG_NBR, TAX_AUTHORITY, TAX_TYPE, PREVIOUS_AMT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder
						.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, TAX_BSEG_ID, TAX_RGME) ");
				stringBuilder.append("SELECT TBL1.BILL_ID, TBL1.ALT_BILL_ID, TBL1.BILLING_PARTY_ID, ");
				stringBuilder.append(
						"TBL1.CIS_DIVISION, TBL1.ACCT_TYPE, TBL1.WPBU, TBL1.BILL_DT, TBL1.BILL_CYC_CD, TBL1.WIN_START_DT, ");
				stringBuilder.append("TBL1.WIN_END_DT, TBL1.CURRENCY_CD, ");
				// NAP-25164 START
				// Update for Over payment
				stringBuilder.append(
						"CASE WHEN (SELECT COUNT(ADJ_ID) FROM CM_INV_BILL_ADJ_INFO WHERE BILL_ID = TBL1.BILL_ID AND ADJ_TYPE_CD = :overpay GROUP BY BILL_ID) > 0 ");
				stringBuilder.append(
						"THEN (TBL1.BILL_AMT - (SELECT SUM(ADJ_AMT) FROM CM_INV_BILL_ADJ_INFO WHERE BILL_ID = TBL1.BILL_ID AND ADJ_TYPE_CD = :overpay GROUP BY BILL_ID)) ");
				stringBuilder.append("ELSE TBL1.BILL_AMT END AS BILL_AMT, ");
				// NAP-25164 END
				stringBuilder.append("TBL1.MERCH_TAX_REG_NBR, ");
				stringBuilder.append("TBL1.WP_TAX_REG_NBR,TBL3.TAX_AUTHORITY, NVL(TBL3.TAX_TYPE,'TAX'), ");
				stringBuilder.append("0 AS PREVIOUS_AMT, CR_NOTE_FR_BILL_ID, ");
				stringBuilder.append("SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, ");
				stringBuilder.append(":ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, ");
				stringBuilder.append(
						"COALESCE(TBL3.TAX_BSEG_ID, CASE WHEN TBL1.ACCT_TYPE = 'CHRG' THEN (SELECT INFO.BSEG_ID ");
				stringBuilder.append(
						"FROM CM_INV_BSEG_TAX_INFO INFO WHERE INFO.BILL_ID = TBL1.BILL_ID  GROUP BY INFO.BSEG_ID) ELSE ' ' END) AS TAX_BSEG_ID, ");
				stringBuilder.append(
						"COALESCE(TBL3.TAX_RGME, CASE WHEN TBL1.ACCT_TYPE = 'CHRG' THEN (SELECT CHR.CHAR_VAL FROM CI_BSEG_CL_CHAR CHR, CM_INV_BSEG_TAX_INFO INFO ");
				stringBuilder.append(
						"WHERE CHR.BSEG_ID = INFO.BSEG_ID AND INFO.BILL_ID = TBL1.BILL_ID  AND CHR.CHAR_TYPE_CD = :taxRgme GROUP BY CHR.CHAR_VAL) ELSE ' ' END) AS TAX_RGME ");
				stringBuilder.append("FROM CM_INV_BILL_PER_INFO TBL1,CM_INV_BILL_TAX_INFO TBL3 ");
				stringBuilder.append("WHERE TBL1.BILL_ID = TBL3.BILL_ID(+)  AND TBL1.STATUS_COD = TBL3.STATUS_COD(+) ");
				stringBuilder.append(
						"AND TBL1.STATUS_COD= :initStatus AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL1.BILL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.bindString("overpay", invoiceDataInterfaceLookUp.getOverpayAdjCode(), "ADJ_TYPE_CD");
				preparedStatement.bindString("taxRgme", invoiceDataInterfaceLookUp.getTaxRegime().trim(),
						"CHAR_TYPE_CD");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INVOICE_DATA data insertion - ", e);
				errMsg = "CM_INVOICE_DATA:Const violated";
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INVOICE_DATA - " + e.toString()));

			} catch (Exception e) {
				errMsg = "CM_INVOICE_DATA:Const violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INVOICE_DATA - " + e.toString()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			/**
			 * Insert bill calculation line of TAX type for all bill segments present in
			 * CM_INV_BSEG_TAX_INFO into CM_INV_DATA_TAX table for all the bills which do
			 * not exists in CM_INV_DATA_ERR table
			 */

			try {
				stringBuilder.append(" INSERT INTO CM_INV_DATA_TAX ");
				stringBuilder.append(" (BILL_ID, CALC_AMT, BASE_AMT ,CHAR_VAL,TAX_STAT, TAX_STAT_CHAR ,TAX_STAT_DESCR, ");
				stringBuilder.append(
						" TAX_RATE, UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW, REVERSE_CHRG_SW) ");
				stringBuilder.append(" SELECT DISTINCT TBL7.BILL_ID,TBL7.CALC_AMT, TBL7.BASE_AMT, TBL7.CHAR_VAL, ");
				stringBuilder.append(" TBL7.TAX_STAT, TBL7.TAX_STAT_CHAR, TBL7.TAX_STAT_DESCR, TBL7.TAX_RATE,  SYSTIMESTAMP AS ");
				stringBuilder.append(
						" UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW, ");
				stringBuilder.append(" TBL7.REVERSE_CHRG_SW ");
				stringBuilder.append(" FROM CM_INV_BSEG_TAX_INFO TBL7 ");
				stringBuilder.append(
						" WHERE TBL7.STATUS_COD = :initStatus AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL7.BILL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query 1 for CM_INV_DATA_TAX data insertion - ", e);
				errMsg = "CM_INV_DATA_TAX:Const violated";
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INV_DATA_TAX - " + e.toString()));

			} catch (Exception e) {
				errMsg = "CM_INV_DATA_TAX:Const violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INV_DATA_TAX - " + e.toString()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			stringBuilder = null;
			stringBuilder = new StringBuilder();

			/**
			 * Insert adjustment information from CM_INV_BILL_ADJ_INFO table into
			 * CM_INV_DATA_ADJ for all the bills which do not exists in CM_INV_DATA_ERR
			 * table
			 */

			try {
				stringBuilder.append("INSERT INTO CM_INV_DATA_ADJ ");
				stringBuilder.append("(BILL_ID, ADJ_ID, ADJ_AMT, ADJ_TYPE_CD, CURRENCY_CD, DESCR, ");
				stringBuilder.append("UPLOAD_DTTM, EXTRACT_FLG, EXTRACT_DTTM, ILM_DT, ILM_ARCH_SW) ");
				stringBuilder.append("SELECT DISTINCT TBL19.BILL_ID, TBL19.ADJ_ID, TBL19.ADJ_AMT, ");
				stringBuilder.append("TBL19.ADJ_TYPE_CD, TBL19.CURRENCY_CD, TBL19.DESCR, ");
				stringBuilder.append(
						"SYSTIMESTAMP AS UPLOAD_DTTM, :extractFlag AS EXTRACT_FLG, '' AS EXTRACT_DTTM, :ilmDateTime AS ILM_DT, 'Y' AS ILM_ARCH_SW ");
				stringBuilder.append("FROM CM_INV_BILL_ADJ_INFO TBL19 ");
				stringBuilder.append("WHERE TBL19.STATUS_COD = :initStatus  ");
				stringBuilder
						.append("AND NOT EXISTS (SELECT 1 FROM CM_INV_DATA_ERR ERR WHERE ERR.BILL_ID=TBL19.BILL_ID) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
				preparedStatement.bindString("initStatus", invoiceDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("extractFlag", invoiceDataInterfaceLookUp.getExtractFlag(), "EXTRACT_FLG");
				preparedStatement.bindDateTime("ilmDateTime", ilmDateTime);
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Exception in executeWorkUnit() : Query  for CM_INV_DATA_ADJ data insertion - ", e);
				errMsg = "CM_INV_DATA_ADJ:Const violated";
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INV_DATA_TAX - " + e.toString()));
			} catch (Exception e) {
				errMsg = "CM_INV_DATA_ADJ:Const violated";
				logger.error("Inside invoice data interface method, Error -", e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution("Error occured while inserting records in CM_INV_DATA_TAX - " + e.toString()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			commit();

		}

		public static String getErrorDescription(int messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.merchantError(CommonUtils.CheckNull(String.valueOf(messageNumber)))
					.getMessageText();
			if (errorInfo.contains("Text:") && errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"), errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		/**
		 * finalizeThreadWork() execute by the batch program once per thread after
		 * processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException, RunAbortedException {
			errMsg = null;
			super.finalizeThreadWork();
		}

	}

	public static final class InvoiceData_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String invLowBillId;
		private String invHighBillId;

		public InvoiceData_Id(String invLowBillId, String invHighBillId) {
			setInvLowBillId(invLowBillId);
			setInvHighBillId(invHighBillId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getInvLowBillId() {
			return invLowBillId;
		}

		public void setInvLowBillId(String invLowBillId) {
			this.invLowBillId = invLowBillId;
		}

		public String getInvHighBillId() {
			return invHighBillId;
		}

		public void setInvHighBillId(String invHighBillId) {
			this.invHighBillId = invHighBillId;
		}

	}

}
