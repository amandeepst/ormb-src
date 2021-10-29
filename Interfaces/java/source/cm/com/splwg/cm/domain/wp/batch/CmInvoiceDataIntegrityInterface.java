/*******************************************************************************
 * FileName                   : CmInvoiceDataIntegrityInterface.java
 * Date of Creation           : Dec 19, 2018
 * Version Number             : 0.1
 * 
 * Revision History           
	VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
	0.1      NA             Dec 19, 2018        Rajesh        Implemented all requirements.
	0.2      NA             Mar 12, 2019        RIA           NAP-42162 Implemented Tax Calculation Check.

*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.PriceStatusFlagLookup;
import com.splwg.ccb.api.lookup.SwitchFlagLookup;
import com.splwg.ccb.domain.admin.idType.IdType_Id;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author pandar231
 *
@BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = chunkSize, required = true, type = integer)
 *            , @BatchJobSoftParameter (entityName = bill, name = billId, type = entity)})
 */
public class CmInvoiceDataIntegrityInterface extends
		CmInvoiceDataIntegrityInterface_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(CmInvoiceDataIntegrityInterface.class);
	
	public JobWork getJobWork()
	{
		logger.debug("Inside CmInvoiceDataIntegrityInterface getJobWork 500");
		int chunkSize = getParameters().getChunkSize().intValue();
		Bill billId = getParameters().getBillId();
		
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		threadWorkUnitList = getInvData(chunkSize, billId);
		
		logger.debug("No. of rows for processing :" + threadWorkUnitList.size());
		logger.debug("getParameters().getChunkSize() :" + chunkSize);
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	
	// *********************** getInvoiceData Method******************************

	/**
	 * getInvoiceData() method selects Bill Ids for processing by this Interface.
	 * 
	 * @return List Invoice_Data_id
	 */
		
	private List<ThreadWorkUnit> getInvData(int chunkSize, Bill billId)
	{
		logger.debug("Inside CmInvoiceDataIntegrityInterface getInvData 170");
		
		Date ilmDate = getProcessDateTime().getDate();
		StringBuilder getInvDataStrBuilder = new StringBuilder();
		PreparedStatement getInvDataPrepStat = null;	
		InvData_Id invoiceData = null;
		String invLowBillid = null;
		String invHighBillId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		List<SQLResultRow> getInvDataList;
		
		try
		{	
			getInvDataStrBuilder.append(" WITH INVDATA AS ");
			getInvDataStrBuilder.append(" (SELECT BILL_ID FROM CM_INVOICE_DATA INVD WHERE EXTRACT_FLG = :yes ");
			getInvDataStrBuilder.append(" AND CR_NOTE_FR_BILL_ID is null");

			if (notNull(billId)) {
				getInvDataStrBuilder.append(" AND BILL_ID =:billId ");
			}
			else {
				getInvDataStrBuilder.append(" AND ILM_DT >= :processDate AND ILM_DT < :finalDate");
			}
			getInvDataStrBuilder.append(" AND NOT EXISTS ");
			getInvDataStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_EXCP EXP ");
			getInvDataStrBuilder.append(" WHERE EXP.BILL_ID = INVD.BILL_ID) ");
			getInvDataStrBuilder.append(" ORDER BY BILL_ID) ");
			getInvDataStrBuilder.append(" SELECT THREAD_NUM, MIN(BILL_ID) AS LOW_INV_BILL_ID, ");
			getInvDataStrBuilder.append(" MAX(BILL_ID) AS HIGH_INV_BILL_ID ");
			getInvDataStrBuilder.append(" FROM (SELECT BILL_ID, CEIL((ROWNUM)/:chunkSize) AS THREAD_NUM FROM INVDATA) ");
			getInvDataStrBuilder.append(" GROUP BY THREAD_NUM ORDER BY 1 ");
			
			getInvDataPrepStat = createPreparedStatement(getInvDataStrBuilder.toString(), "get Invoice Data"); 
			getInvDataPrepStat.bindLookup("yes", SwitchFlagLookup.constants.YES_FIELD);
			getInvDataPrepStat.bindBigInteger("chunkSize", new BigInteger(String.valueOf(chunkSize)));
			
			if (notNull(billId)) {
				getInvDataPrepStat.bindId("billId", billId.getId());
			}
			else {
				getInvDataPrepStat.bindDate("processDate", ilmDate);	
				getInvDataPrepStat.bindDate("finalDate", ilmDate.addDays(1));				

			}
			
			getInvDataPrepStat.setAutoclose(false);
			getInvDataList = getInvDataPrepStat.list();
			
			if (notNull(getInvDataList))
			{
				logger.debug("Inside CmInvoiceDataIntegrityInterface getInvDataList.size() :" + getInvDataList.size()); 
				for (SQLResultRow sqlRow : getInvDataList)
				{
					invLowBillid = sqlRow.getString("LOW_INV_BILL_ID");
					invHighBillId = sqlRow.getString("HIGH_INV_BILL_ID");
					invoiceData = new InvData_Id(invLowBillid, invHighBillId);
					threadworkUnit = new ThreadWorkUnit();
					threadworkUnit.setPrimaryId(invoiceData);
					
					threadWorkUnitList.add(threadworkUnit);
					threadworkUnit = null;
					invoiceData = null;
					
				}
			}
			
		}
		catch (ThreadAbortedException e)
		{
			logger.error("Inside getInvoiceData() method of InvoiceDataInterface, Error -", e);
			throw new RunAbortedException(CustomMessageRepository.exceptionInExecution("Error occurred in getJobWork() while picking Bill Ids"));
		}
		catch (Exception e)
		{
			logger.error("Inside invoice data interface method, Error -", e);
		}
		finally
		{
			if (getInvDataPrepStat != null)
			{
				getInvDataPrepStat.close();
				getInvDataPrepStat = null;
			}
		}
		
		return threadWorkUnitList;
	
	}
	
	public Class<CmInvoiceDataIntegrityInterfaceWorker> getThreadWorkerClass() {
		return CmInvoiceDataIntegrityInterfaceWorker.class;
	}

	/**
	 * @author tutejaa105
	 *
	 */
	public static class CmInvoiceDataIntegrityInterfaceWorker extends CmInvoiceDataIntegrityInterfaceWorker_Gen {
		
		public final String TAX = "TAX";
		
		public final String CHARGE = "CHRG";
		
		public final String MINCHARGE = "MINCHRGP  ";
		
		public final String TRANSACTION_VOLUME = "TXN_VOL";
		//public final String NUMBER = "NUMBER";
		
		public final String TOTAL_BB_BCL_TYPE = "TOTAL_BB";
		public final String PI_MBA_BCL_TYPE = "PI_MBA";
		public final String PC_MBA_BCL_TYPE = "PC_MBA";
		public final String PI_RECUR_BCL_TYPE = "PI_RECUR";
		public final String IC_BM_BCL_TYPE = "IC_BM";
		public final String CS_BM_BCL_TYPE = "CS_BM";
		public final String CS_BM1_BCL_TYPE = "CS_BM1";
		public final String CS_BM2_BCL_TYPE = "CS_BM2";
		public final String ASFPC_MB_BCL_TYPE = "ASFPC_MB";
		public final String ASFPI_MB_BCL_TYPE = "ASFPI_MB";
		public final String CT_MBA_BCL_TYPE = "CT_MBA";
		public final String FMAMT = "F_M_AMT";
		public final String RATE_TYPE_FLAG = "RATE_FLG";
		public final String YES = "Y";
		public final String FND_BASE_FX = "F_B_MFX";
		public final String SQI_NUMBER = "NUMBER";
		public final String ASF_PI_RATE_TP = "ASF_PI";
		public final String ASF_PC_RATE_TP = "ASF_PC";
		public final String MSC_PC_RATE_TP = "MSC_PC";
		public final String CST_PC_RATE_TP = "CST_PC";
		public final String MSC_PI_RATE_TP = "MSC_PI";
		public final String MIN_P_CHRG_RATE_TP = "MIN_P_CHRG";
		public final String M_PI_RATE_TP = "M_PI";
		public final String P_B_EFX = "P_B_EFX";
		public final String AP_B_EFX = "AP_B_EFX";
		
		private boolean isLineClaculations = false;
		private boolean isMinChargeCalculations = false;

		Set<String> billErrSet = null;
		
		CustomMessages cstmMessage = new CustomMessages();
		public final BigInteger messageCat = new BigInteger(String.valueOf(cstmMessage.MESSAGE_CATEGORY));
		public final BigInteger invDtLnNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INVOICE_DATA_LN_NO_RECORDS));
		public final BigInteger invDtBclNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INV_DATA_LN_BCL_NO_RECORDS));
		public final BigInteger invDtLnRateNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INV_DATA_LN_RATE_NO_RECORDS));
		public final BigInteger invDtLnSvcQtNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INV_DATA_LN_SVC_QTY_NO_RECORDS));
		public final BigInteger invDtLnPriceCategoryNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INVOICE_DATA_LN_PRICE_RECORDS));

		public final BigInteger invDtTaxNoRecMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_INV_DATA_TAX_NO_REOCRDS));
		public final BigInteger totalChargesMismatchMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_TOTAL_CHARGES_MISMATCH));
		public final BigInteger lineCalcNotCorrectMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_LINE_CALCULATION_NOT_CORRECT));
		public final BigInteger verifyTaxCalc = new BigInteger(String.valueOf(cstmMessage.CM_INVOICE_TAX_CALC_VERIFY));
		public final BigInteger minChargeBillingMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_MIN_CHARGE_BILLING_MISMATCH));
		public final BigInteger minChargeNonBillingMsgNbr = new BigInteger(String.valueOf(cstmMessage.CM_MIN_CHARGE_NON_BILLING_MISMATCH));

		public ThreadExecutionStrategy createExecutionStrategy()
		{
			return new StandardCommitStrategy(this);
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean initializationPreviouslySuccessful) throws ThreadAbortedException, RunAbortedException
		{
			billErrSet = new HashSet<String>();
		}
		
		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface executeWorkUnit 20072019");
			
			Date processDate = getProcessDateTime().getDate();
//			Date finalDate=processDate.addDays(1);
//			Bill bill = getParameters().getBillId();
			
			InvData_Id invData = (InvData_Id) unit.getPrimaryId();
			Bill_Id billId = null;
			Date billDate;
			List<SQLResultRow> billList = null;
			
			try
			{
				billList = getBillList(invData);
				
				if (notNull(billList))
				{
					for (SQLResultRow billIdStr : billList)
					{
						billId = new Bill_Id(billIdStr.getString("BILL_ID"));
						billDate = billId.getEntity().getBillDate();
						
						logger.debug("Invoke checkBillSegs start:");
						checkBillSegs(billId);
						
						//logger.debug("Invoke checkLinePriceCategory start:");
						checkLinePriceCategory(billId);
							
						logger.debug("Invoke checkInvoiceDataBCL start:");
						checkInvoiceDataBCL(billId);
							
						logger.debug("Invoke checkInvoiceDataLnRate start:");
						checkInvoiceDataLnRate(billId);
							
						logger.debug("Invoke checkInvoiceDataLnSQ start:");
						checkInvoiceDataLnSQ(billId);
							
						logger.debug("Invoke checkInvoiceDataTax start:");
						checkInvoiceDataTax(billId);
						
						logger.debug("Invoke checkInvoiceDataTaxCalcVerify start:");
						checkInvoiceDataTaxCalcVerify(billId);
							
						logger.debug("Invoke checkTotalCharges start:");
						checkTotalCharges(billId);

						/*
						logger.debug("Invoke checkLineClaculations start:");
						checkLineCalculations(billId);
						*/
						logger.debug("Invoke chec Minimum Charge Calculations on Billing level start:");
						checkMinChargesBillingCalculations(billId,billDate);

						logger.debug("Invoke chec Minimum Charge Calculations on Non Billing level start:");
						checkMinChargesNonBillingCalculations(billId,billDate);

					}
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, executeWorkUnit method, Error -", e);
			}
			
			logger.debug("Executework unit  successful:");
			
			return true;
		}

		/**
		 * Checks Minimum CHarge Calculations on Non Billing level and displays results when there is an issue
		 * @param billId
		 * @param billDate
		 */
		private void checkMinChargesNonBillingCalculations(Bill_Id billId, Date billDate) {
			StringBuilder minChargeQueryNonBilling = new StringBuilder();
			PreparedStatement ps = null;
			List<SQLResultRow> billErrList;

			try
			{
				minChargeQueryNonBilling.append(" WITH charges AS ");
				minChargeQueryNonBilling.append(" (SELECT invdt.billing_party_id as bill_party,ln.billing_party_id, ");
				minChargeQueryNonBilling.append(" SUM(bcl.calc_amt) AS applicable_chg_amt FROM ");
				minChargeQueryNonBilling.append(" cm_invoice_data invdt,cm_invoice_data_ln ln, ");
				minChargeQueryNonBilling.append(" cm_inv_data_ln_bcl bcl,ci_priceitem_rel prel, ");
				minChargeQueryNonBilling.append(" (SELECT invdt.billing_party_id ");
				minChargeQueryNonBilling.append(" FROM ci_per_id per, cm_invoice_data invdt , ci_per persn ");
				minChargeQueryNonBilling.append(" WHERE invdt.billing_party_id = per.per_id_nbr ");
				minChargeQueryNonBilling.append(" AND invdt.acct_type     = :charge ");
				minChargeQueryNonBilling.append(" AND invdt.bill_id = :billId AND per.id_type_cd = :idTypeCd ");
				minChargeQueryNonBilling.append(" AND per.per_id = persn.per_id ");
				minChargeQueryNonBilling.append(" AND invdt.cis_division = persn.cis_division ");
				minChargeQueryNonBilling.append(" ) bill_range ");
				minChargeQueryNonBilling.append(" WHERE trim(ln.price_category) = trim(prel.priceitem_chld_cd) ");
				minChargeQueryNonBilling.append(" AND invdt.bill_id = ln.bill_id AND invdt.bill_id = :billId ");
				minChargeQueryNonBilling.append(" AND invdt.billing_party_id = bill_range.billing_party_id ");
				minChargeQueryNonBilling.append(" AND ln.bseg_id = bcl.bseg_id AND bcl_type <> 'F_M_AMT'  ");
				minChargeQueryNonBilling.append(" AND prel.priceitem_par_cd  = 'MINCHRGP' ");
				minChargeQueryNonBilling.append(" GROUP BY invdt.billing_party_id ,ln.billing_party_id ");
				minChargeQueryNonBilling.append(" ), ");
				minChargeQueryNonBilling.append(" mmsc_rate AS (SELECT per.per_id_nbr AS party_id, ");
				minChargeQueryNonBilling.append(" pc.value_amt      AS mmsc_rate, ");
				minChargeQueryNonBilling.append(" pa.price_currency_cd as currency_cd , ");
				minChargeQueryNonBilling.append(" perper.cis_division as lcp ");
				minChargeQueryNonBilling.append(" FROM ci_priceasgn pa, ci_pricecomp pc,ci_party party,  ");
				minChargeQueryNonBilling.append(" ci_per_id per , ci_per perper  ");
				minChargeQueryNonBilling.append(" WHERE pa.price_asgn_id          = pc.price_asgn_id ");
				minChargeQueryNonBilling.append(" AND party.party_uid = pa.owner_id AND per.per_id = party.party_id ");
				minChargeQueryNonBilling.append(" AND :billDate BETWEEN pa.start_dt AND NVL(pa.end_dt,sysdate + 10) ");
				minChargeQueryNonBilling.append(" AND pa.priceitem_cd = 'MINCHRGP' AND per.id_type_cd  = :idTypeCd ");
				minChargeQueryNonBilling.append(" AND pa.price_status_flag = :actvStatus ");
				minChargeQueryNonBilling.append(" AND per.per_id = perper.per_id ");
				minChargeQueryNonBilling.append(" ), ");
				minChargeQueryNonBilling.append(" applied_mmsc AS (SELECT invdt.billing_party_id AS bill_party,  ");
				minChargeQueryNonBilling.append(" ln.billing_party_id, invdt.bill_id,   ");
				minChargeQueryNonBilling.append(" bcl.calc_amt AS mmsc_chg, invdt.currency_cd ");
				minChargeQueryNonBilling.append(" AS currency_cd , invdt.cis_division as lcp   ");
				minChargeQueryNonBilling.append(" FROM cm_invoice_data invdt,cm_invoice_data_ln ");
				minChargeQueryNonBilling.append(" ln,  cm_inv_data_ln_bcl bcl WHERE ");
				minChargeQueryNonBilling.append(" invdt.bill_id = ln.bill_id AND ln.bseg_id = bcl.bseg_id  ");
				minChargeQueryNonBilling.append(" AND invdt.billing_party_id <> ln.billing_party_id ");
				minChargeQueryNonBilling.append(" AND ln.price_category      = 'MINCHRGP' ");
				minChargeQueryNonBilling.append(" and invdt.bill_id = :billId and ln.bill_id = :billId ");
				minChargeQueryNonBilling.append(" and bcl.bill_id = :billId  )   ");
				minChargeQueryNonBilling.append(" SELECT app.bill_id,' ' AS BSEG_ID, rate.party_id, ");
				minChargeQueryNonBilling.append(" rate.mmsc_rate,  ");
				minChargeQueryNonBilling.append(" NVL(chg.applicable_chg_amt,0) APPLICABLE_CHARGES, ");
				minChargeQueryNonBilling.append(" app.mmsc_chg AS actual_mmsc_chg, ");
				minChargeQueryNonBilling.append(" CASE ");
				minChargeQueryNonBilling.append(" WHEN NVL(applicable_chg_amt,0) < 0 ");
				minChargeQueryNonBilling.append(" THEN round(rate.mmsc_rate, ccy.decimal_positions) ");
				minChargeQueryNonBilling.append(" WHEN NVL(applicable_chg_amt,0) > round(rate.mmsc_rate, ccy.decimal_positions) "); 
				minChargeQueryNonBilling.append(" THEN 0 ");
				minChargeQueryNonBilling.append(" ELSE round(rate.mmsc_rate, ccy.decimal_positions) - NVL(applicable_chg_amt,0) ");
				minChargeQueryNonBilling.append(" END AS derived_mmsc, ");
				minChargeQueryNonBilling.append(" app.mmsc_chg - ");
				minChargeQueryNonBilling.append(" CASE ");
				minChargeQueryNonBilling.append(" WHEN  NVL(applicable_chg_amt,0) < 0 ");
				minChargeQueryNonBilling.append(" THEN  round(rate.mmsc_rate,ccy.decimal_positions) ");
				minChargeQueryNonBilling.append(" WHEN  NVL(applicable_chg_amt,0) > round(rate.mmsc_rate, ccy.decimal_positions) ");
				minChargeQueryNonBilling.append(" THEN 0 ");
				minChargeQueryNonBilling.append(" ELSE round(rate.mmsc_rate, ccy.decimal_positions) -  NVL(applicable_chg_amt,0) ");
				minChargeQueryNonBilling.append(" END AS diff FROM charges chg, mmsc_rate rate, ");
				minChargeQueryNonBilling.append(" applied_mmsc app, ci_currency_cd ccy   ");
				minChargeQueryNonBilling.append(" WHERE chg.billing_party_id (+) = app.billing_party_id ");
				minChargeQueryNonBilling.append(" AND app.billing_party_id = rate.party_id ");
				minChargeQueryNonBilling.append(" AND app.currency_cd = rate.currency_cd ");
				minChargeQueryNonBilling.append(" AND rate.currency_cd = ccy.currency_cd AND rate.lcp = app.lcp ");
				
				ps = createPreparedStatement(minChargeQueryNonBilling.toString(), "get Min Charge Bills on Non billing");
				ps.bindId("billId", billId);
				//ps.bindId("minCharType", new CharacteristicType_Id("MINCHGNM"));
				ps.bindId("idTypeCd", new IdType_Id("EXPRTYID"));
				ps.bindLookup("actvStatus", PriceStatusFlagLookup.constants.ACTIVE);
				ps.bindDate("billDate", billDate);
				//ps.bindString("fmAmt", FMAMT, "BCL_TYPE");
				ps.bindString("charge", CHARGE, "ACCT_TYPE");
				ps.setAutoclose(false);

				billErrList = ps.list();
				logger.debug("billErrList.size() :" + billErrList.size());

				if (notNull(billErrList) && billErrList.size() > 0 )
				{
					isMinChargeCalculations = true;
					addInvoiceDataError(billErrList, messageCat, minChargeNonBillingMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(minChargeNonBillingMsgNbr.intValue()).getMessageText());
					isMinChargeCalculations = false;
				}

			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkMinChargesNonBillingCalculations method, Error -", e);
			}
			finally
			{
				if (ps != null)
				{
					ps.close();
					ps = null;
				}
			}

		}

		/**
		 * checks Minimum CHarge Calculations on Billing level and displays results when there is an issue
		 * @param billId
		 * @param billDate
		 */
		private void checkMinChargesBillingCalculations(Bill_Id billId, Date billDate) {
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkMinChargesCalculations");

			StringBuilder minChargeQueryBilling = new StringBuilder();
			PreparedStatement ps = null;
			List<SQLResultRow> billErrList;

			try
			{
				minChargeQueryBilling.append(" WITH charges AS (SELECT invdt.billing_party_id as bill_party, ");
				minChargeQueryBilling.append(" SUM(bcl.calc_amt) AS applicable_chg_amt ");
				minChargeQueryBilling.append(" FROM cm_invoice_data invdt,cm_invoice_data_ln ln, ");
				minChargeQueryBilling.append(" cm_inv_data_ln_bcl bcl,ci_priceitem_rel prel, ");
				minChargeQueryBilling.append(" (SELECT invdt.billing_party_id ");
				minChargeQueryBilling.append(" FROM ci_per_id per,cm_invoice_data invdt ,ci_per persons ");
				minChargeQueryBilling.append(" WHERE invdt.billing_party_id = per.per_id_nbr ");
				minChargeQueryBilling.append(" AND invdt.acct_type     = :charge ");
				minChargeQueryBilling.append(" AND invdt.bill_id       = :billId ");
				minChargeQueryBilling.append(" AND per.id_type_cd 	  = :idTypeCd ");
				minChargeQueryBilling.append(" AND per.per_id = persons.per_id ");
				minChargeQueryBilling.append(" AND invdt.cis_division = persons.cis_division ");
				minChargeQueryBilling.append(" ) bill_range ");
				minChargeQueryBilling.append(" WHERE trim(ln.price_category) = trim(prel.priceitem_chld_cd) ");
				minChargeQueryBilling.append(" AND invdt.bill_id        = ln.bill_id ");
				minChargeQueryBilling.append(" AND invdt.bill_id = :billId ");
				minChargeQueryBilling.append(" AND invdt.billing_party_id = bill_range.billing_party_id ");
				minChargeQueryBilling.append(" AND ln.bseg_id = bcl.bseg_id AND bcl_type <> 'F_M_AMT' ");
				minChargeQueryBilling.append(" AND prel.priceitem_par_cd  = 'MINCHRGP' ");
				minChargeQueryBilling.append(" GROUP BY invdt.billing_party_id ");
				minChargeQueryBilling.append(" ), ");
				minChargeQueryBilling.append(" mmsc_rate AS (SELECT per.per_id_nbr AS party_id, ");  
				minChargeQueryBilling.append(" pc.value_amt      AS mmsc_rate, ");
				minChargeQueryBilling.append(" pa.price_currency_cd as currency_cd , ");
				minChargeQueryBilling.append(" person.cis_division as lcp ");
				minChargeQueryBilling.append(" FROM ci_priceasgn pa, ci_pricecomp pc,ci_party party, ");
				minChargeQueryBilling.append(" ci_per_id per  , ci_per person ");
				minChargeQueryBilling.append(" WHERE pa.price_asgn_id          = pc.price_asgn_id ");
				minChargeQueryBilling.append(" AND party.party_uid = pa.owner_id AND per.per_id = party.party_id ");
				minChargeQueryBilling.append(" AND :billDate BETWEEN pa.start_dt AND NVL(pa.end_dt,sysdate + 10) ");
				minChargeQueryBilling.append(" AND pa.priceitem_cd = 'MINCHRGP' AND per.id_type_cd  = :idTypeCd ");
				minChargeQueryBilling.append(" AND pa.price_status_flag = :actvStatus ");
				minChargeQueryBilling.append(" AND per.per_id = person.per_id ");
				minChargeQueryBilling.append(" ), ");
				minChargeQueryBilling.append(" applied_mmsc AS ");
				minChargeQueryBilling.append(" (SELECT invdt.billing_party_id AS bill_party, ");
				minChargeQueryBilling.append(" ln.billing_party_id,invdt.bill_id, ");
				minChargeQueryBilling.append(" bcl.calc_amt AS mmsc_chg, ");
				minChargeQueryBilling.append(" invdt.currency_cd AS currency_cd, ");
				minChargeQueryBilling.append(" invdt.cis_division as lcp ");
				minChargeQueryBilling.append(" FROM cm_invoice_data invdt,cm_invoice_data_ln ln,cm_inv_data_ln_bcl bcl ");
				minChargeQueryBilling.append(" WHERE invdt.bill_id             = ln.bill_id ");
				minChargeQueryBilling.append(" AND ln.bseg_id             = bcl.bseg_id ");
				minChargeQueryBilling.append(" AND invdt.billing_party_id = ln.billing_party_id ");
				minChargeQueryBilling.append(" AND ln.price_category      = 'MINCHRGP' ");
				minChargeQueryBilling.append(" and invdt.bill_id = :billId and ln.bill_id = :billId ");
				minChargeQueryBilling.append(" and bcl.bill_id = :billId ) ");
				minChargeQueryBilling.append(" SELECT app.bill_id,' ' AS BSEG_ID,rate.party_id,rate.mmsc_rate, ");
				minChargeQueryBilling.append(" NVL(chg.applicable_chg_amt,0) APPLICABLE_CHARGES, ");
				minChargeQueryBilling.append(" app.mmsc_chg AS actual_mmsc_chg, ");
				minChargeQueryBilling.append(" CASE ");
				minChargeQueryBilling.append(" WHEN NVL(applicable_chg_amt,0) < 0 ");
				minChargeQueryBilling.append(" THEN round(rate.mmsc_rate, ccy.decimal_positions) ");
				minChargeQueryBilling.append(" WHEN NVL(applicable_chg_amt,0) > round(rate.mmsc_rate, ccy.decimal_positions) ");
				minChargeQueryBilling.append(" THEN 0 ");
				minChargeQueryBilling.append(" ELSE round(rate.mmsc_rate, ccy.decimal_positions) - NVL(applicable_chg_amt,0) ");
				minChargeQueryBilling.append(" END AS derived_mmsc, ");
				minChargeQueryBilling.append(" app.mmsc_chg - ");
				minChargeQueryBilling.append(" CASE ");
				minChargeQueryBilling.append(" WHEN  NVL(applicable_chg_amt,0) < 0 ");
				minChargeQueryBilling.append(" THEN  round(rate.mmsc_rate,ccy.decimal_positions) ");
				minChargeQueryBilling.append(" WHEN  NVL(applicable_chg_amt,0) > round(rate.mmsc_rate, ccy.decimal_positions) ");
				minChargeQueryBilling.append(" THEN 0 ");
				minChargeQueryBilling.append(" ELSE round(rate.mmsc_rate, ccy.decimal_positions) -  NVL(applicable_chg_amt,0) ");
				minChargeQueryBilling.append(" END AS diff ");
				minChargeQueryBilling.append(" FROM charges chg, mmsc_rate rate,applied_mmsc app,ci_currency_cd ccy ");
				minChargeQueryBilling.append(" WHERE chg.bill_party (+) = app.billing_party_id ");
				minChargeQueryBilling.append(" AND app.bill_party = rate.party_id ");
				minChargeQueryBilling.append(" AND app.currency_cd = rate.currency_cd ");
				minChargeQueryBilling.append(" and rate.currency_cd = ccy.currency_cd ");
				minChargeQueryBilling.append(" AND rate.lcp = app.lcp ");

				ps = createPreparedStatement(minChargeQueryBilling.toString(), "get Min Charge Bills");
				ps.bindId("billId", billId);
				//ps.bindId("minCharType", new CharacteristicType_Id("MINCHGNM"));
				ps.bindId("idTypeCd", new IdType_Id("EXPRTYID"));
				ps.bindLookup("actvStatus", PriceStatusFlagLookup.constants.ACTIVE);
				ps.bindDate("billDate", billDate);
				//ps.bindString("fmAmt", FMAMT, "BCL_TYPE");
				ps.bindString("charge", CHARGE, "ACCT_TYPE");
				ps.setAutoclose(false);

				billErrList = ps.list();
				logger.debug("billErrList.size() :" + billErrList.size());

				if (notNull(billErrList) && billErrList.size() > 0 )
				{
					isMinChargeCalculations = true;
					addInvoiceDataError(billErrList, messageCat, minChargeBillingMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(minChargeBillingMsgNbr.intValue()).getMessageText());
					isMinChargeCalculations = false;
				}

			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkMinChargesBillingCalculations method, Error -", e);
			}
			finally
			{
				if (ps != null)
				{
					ps.close();
					ps = null;
				}
			}

		}


		@Override
		public void finalizeThreadWork() throws ThreadAbortedException
		{
			logger.debug("Inside finalizeThreadWork :");
			
			String billIdStr = null;
			Bill_Id billId = null;
			Date billDate = null;
			Iterator<String> billErrSetIter = billErrSet.iterator();
			StringBuilder insertInvDataExcepStrBuilder;
			PreparedStatement insertInvDataPrepStat = null;
			StringBuilder updateInvDataStrBuilder;
			PreparedStatement updateInvDataPrepStat = null;
			
			logger.debug("billErrSet.size() :" + billErrSet.size());
			
			try 
			{
				while (billErrSetIter.hasNext())
				{
					insertInvDataExcepStrBuilder = new StringBuilder();
					billIdStr = billErrSetIter.next();
					billId = new Bill_Id(billIdStr);
					billDate = billId.getEntity().getBillDate();
					
					// INSERT INTO CM_INV_DATA_EXCP
					insertInvDataExcepStrBuilder.append(" INSERT INTO CM_INV_DATA_EXCP (BILL_ID, BILL_DT, ERROR_DT) ");
					insertInvDataExcepStrBuilder.append(" VALUES (:billId, :billDate, :errDate) ");
					
					insertInvDataPrepStat = createPreparedStatement(insertInvDataExcepStrBuilder.toString(), "Insert into CM_INV_DATA_EXCP");
					insertInvDataPrepStat.bindId("billId", billId);
					insertInvDataPrepStat.bindDate("billDate", billDate);
					insertInvDataPrepStat.bindDate("errDate", getSystemDateTime().getDate());
					
					insertInvDataPrepStat.executeUpdate();
					
					// UPDATE CM_INVOICE_DATA
					updateInvDataStrBuilder = new StringBuilder();
					updateInvDataStrBuilder.append("UPDATE CM_INVOICE_DATA SET EXTRACT_FLG = :no WHERE BILL_ID = :billId");
					updateInvDataPrepStat = createPreparedStatement(updateInvDataStrBuilder.toString(), "update Invoice Data");
					updateInvDataPrepStat.bindLookup("no", SwitchFlagLookup.constants.NO_FIELD);
					updateInvDataPrepStat.bindId("billId", billId);
					
					updateInvDataPrepStat.executeUpdate();
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, finalizeThreadWork method, Error -", e);
			}
			finally
			{
				billErrSet.clear();
				
				if (notNull(insertInvDataPrepStat))
				{
					insertInvDataPrepStat.close();
					insertInvDataPrepStat = null;
				}
				
				if (notNull(updateInvDataPrepStat))
				{
					updateInvDataPrepStat.close();
					updateInvDataPrepStat = null;
				}
			}
			
		}
		

		/**
		 * checkBillSegs() method checks all applicable bill segments exist in invoice data line
		 * @param billId
		 */
//		private void checkBillSegs(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkBillSegs(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkBillSegs");
			
			StringBuilder getBsegsStrBuilder = new StringBuilder();
			PreparedStatement getBsegsPrepStat = null;
			List<SQLResultRow> billBsegErrList;
			
			try
			{
				getBsegsStrBuilder.append(" SELECT BILL_ID, BSEG.BSEG_ID ");
				getBsegsStrBuilder.append(" FROM CI_BSEG BSEG, CI_BSEG_CALC CALC ");
				getBsegsStrBuilder.append(" WHERE BSEG.BILL_ID =:billId ");
				getBsegsStrBuilder.append(" AND BSEG.BSEG_ID = CALC.BSEG_ID ");
				getBsegsStrBuilder.append(" AND CALC.RS_CD <> :tax ");
				getBsegsStrBuilder.append(" AND NOT EXISTS ");
				getBsegsStrBuilder.append(" (SELECT 1 FROM CM_INVOICE_DATA_LN LN WHERE ");
				getBsegsStrBuilder.append(" BSEG.BSEG_ID = LN.BSEG_ID) ");

				
				getBsegsPrepStat = createPreparedStatement(getBsegsStrBuilder.toString(), "get Bill Segments");
				getBsegsPrepStat.bindId("billId", billId);
				
				getBsegsPrepStat.bindString("tax", TAX, "RS_CD");
				getBsegsPrepStat.setAutoclose(false);
				
				billBsegErrList = getBsegsPrepStat.list();
				logger.debug("billBsegErrList.size() :" + billBsegErrList.size());
				
				if (notNull(billBsegErrList) && billBsegErrList.size() > 0)
				{
					addInvoiceDataError(billBsegErrList, messageCat, invDtLnNoRecMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(invDtLnNoRecMsgNbr.intValue()).getMessageText());
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkBillSegs method, Error -", e);
			}
			finally
			{
				if (notNull(getBsegsPrepStat))
				{
					getBsegsPrepStat.close();
					getBsegsPrepStat = null;
				}
			}
			
		}
		
		
		/**
		 * checkInvoiceDataTax() method checks all applicable bill segments exist in invoice data Tax
		 * @param billId
		 */
//		private void checkInvoiceDataTax(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkInvoiceDataTax(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkInvoiceDataTax");
			
			StringBuilder getInvoiceDataTaxStrBuilder = new StringBuilder();
			PreparedStatement getInvoiceDataTaxPrepStat = null;
			List<SQLResultRow> invoiceDataTaxErrList;
			
			try
			{
				getInvoiceDataTaxStrBuilder.append(" SELECT INVDT.BILL_ID, ' ' BSEG_ID ");
				getInvoiceDataTaxStrBuilder.append(" FROM CM_INVOICE_DATA INVDT ");
				getInvoiceDataTaxStrBuilder.append(" WHERE INVDT.BILL_ID =:billId ");
				getInvoiceDataTaxStrBuilder.append(" AND TRIM(ACCT_TYPE) = :charge AND");
//				if(isNull(bill)){
//					getInvoiceDataTaxStrBuilder.append(" INVDT.ILM_DT >= :processDate AND INVDT.ILM_DT < :finalDate AND ");
//				}
				getInvoiceDataTaxStrBuilder.append(" NOT EXISTS ");
				getInvoiceDataTaxStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_TAX TAX WHERE ");
//				if(isNull(bill)){
//					getInvoiceDataTaxStrBuilder.append(" TAX.ILM_DT >= :processDate AND TAX.ILM_DT < :finalDate AND ");
//				}
				getInvoiceDataTaxStrBuilder.append(" TAX.BILL_ID=INVDT.BILL_ID) ");
				
				getInvoiceDataTaxPrepStat = createPreparedStatement(getInvoiceDataTaxStrBuilder.toString(), "get Invoice Data Tax");
				getInvoiceDataTaxPrepStat.bindId("billId", billId);
//				if(isNull(bill)){
//					getInvoiceDataTaxPrepStat.bindDate("processDate",processDate);
//					getInvoiceDataTaxPrepStat.bindDate("finalDate",finalDate);
//
//				}
				getInvoiceDataTaxPrepStat.bindString("charge", CHARGE, "ACCT_TYPE");
				getInvoiceDataTaxPrepStat.setAutoclose(false);
				
				invoiceDataTaxErrList = getInvoiceDataTaxPrepStat.list();
				logger.debug("invoiceDataTaxErrList.size() :" + invoiceDataTaxErrList.size());
				
				if (notNull(invoiceDataTaxErrList) && invoiceDataTaxErrList.size() > 0)
				{
					addInvoiceDataError(invoiceDataTaxErrList, messageCat, invDtTaxNoRecMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(invDtTaxNoRecMsgNbr.intValue()).getMessageText());
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataTax method, Error -", e);
			}
			finally
			{
				if (notNull(getInvoiceDataTaxPrepStat))
				{
					getInvoiceDataTaxPrepStat.close();
					getInvoiceDataTaxPrepStat = null;
				}
			}
		} 
		
		//NAP-42162
		
		/**
		 * checkInvoiceDataTaxCalcVerify() check ensures that the base amount and calculated
		 *  amount for the tax lines match the data found in the BCL table. 
		 * @param billId
		 */
		//private void checkInvoiceDataTaxCalcVerify(Bill_Id billId, Bill bill,
				//Date processDate, Date finalDate) {
		private void checkInvoiceDataTaxCalcVerify(Bill_Id billId){
			
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkTotalCharges");
			
			StringBuilder checkInvoiceDataTaxCalcSb = new StringBuilder();
			PreparedStatement preparedStatement = null;
			List<SQLResultRow> taxCalcVerifyErrList =  null;
			
			
			try{
			
			
			checkInvoiceDataTaxCalcSb.append("WITH DERIVED_BASE_AMT AS (SELECT BILL_ID, TAX_STAT, TAX_RATE, ");
			checkInvoiceDataTaxCalcSb.append(" SUM(CALC_AMT) AS DERIVED_BASE_AMT  FROM CM_INV_DATA_LN_BCL ");
			checkInvoiceDataTaxCalcSb.append("  WHERE BCL_TYPE <> 'F_M_AMT'  AND BILL_ID     = :billId ");
//			if(isNull(bill)){
//				checkInvoiceDataTaxCalcSb.append(" AND ILM_DT >= :processDate AND ILM_DT < :finalDate");
//			}
			
			checkInvoiceDataTaxCalcSb.append(" GROUP BY BILL_ID, TAX_STAT, TAX_RATE ) ");
			checkInvoiceDataTaxCalcSb.append(" SELECT T1.BILL_ID, ' ' AS BSEG_ID, ' ' AS PRICE_CATEGORY, T3.ILM_DT,  T1.TAX_STAT,  DERIVED_BASE_AMT,  NVL(BASE_AMT,0) AS TAX_BASE_AMT, ");
			checkInvoiceDataTaxCalcSb.append(" T1.TAX_RATE,  ROUND((DERIVED_BASE_AMT) *(T1.TAX_RATE /100),DECIMAL_POSITIONS)  AS DERIVED_TAX_AMT, ");
			checkInvoiceDataTaxCalcSb.append(" NVL(T2.CALC_AMT,0)  AS TAX_APPLIED_AMT,  ROUND((DERIVED_BASE_AMT) *(T1.TAX_RATE /100),DECIMAL_POSITIONS) - NVL(T2.CALC_AMT,0) AS DIFF");
			checkInvoiceDataTaxCalcSb.append(" FROM DERIVED_BASE_AMT T1 ,  CM_INV_DATA_TAX T2,   CM_INVOICE_DATA T3,  CI_CURRENCY_CD T4  ");
			checkInvoiceDataTaxCalcSb.append(" WHERE T1.BILL_ID   = T2.BILL_ID (+) AND T1.TAX_STAT    = T2.TAX_STAT AND T2.BILL_ID     = T3.BILL_ID ");
			checkInvoiceDataTaxCalcSb.append(" AND T4.CURRENCY_CD = T3.CURRENCY_CD ");
//			if(isNull(bill)){
//				checkInvoiceDataTaxCalcSb.append(" AND T2.ILM_DT >= :processDate AND T2.ILM_DT < :finalDate");
//			}
//			if(isNull(bill)){
//				checkInvoiceDataTaxCalcSb.append(" AND T3.ILM_DT >= :processDate AND T3.ILM_DT < :finalDate");
//			}
			
			checkInvoiceDataTaxCalcSb.append(" AND T1.BILL_ID = :billId AND T2.BILL_ID = :billId AND T3.BILL_ID = :billId GROUP BY T1.BILL_ID, ");
			checkInvoiceDataTaxCalcSb.append("  T1.TAX_STAT,  T3.ILM_DT,  DERIVED_BASE_AMT,  BASE_AMT,  T1.TAX_RATE,  T2.CALC_AMT,  ");
			checkInvoiceDataTaxCalcSb.append(" ROUND((DERIVED_BASE_AMT) *(T1.TAX_RATE /100),DECIMAL_POSITIONS),  ROUND((DERIVED_BASE_AMT) *(T1.TAX_RATE /100),DECIMAL_POSITIONS)- nvl(T2.CALC_AMT,0) ");
			checkInvoiceDataTaxCalcSb.append(" HAVING (ROUND((DERIVED_BASE_AMT) *(T1.TAX_RATE /100),DECIMAL_POSITIONS) - nvl(T2.CALC_AMT,0) <>0  ");
			checkInvoiceDataTaxCalcSb.append(" OR DERIVED_BASE_AMT <> nvl(BASE_AMT,0) OR BASE_AMT is null)  ");
			
			preparedStatement =  createPreparedStatement(checkInvoiceDataTaxCalcSb.toString(),"Verify Tax Data Calculation");
			
			preparedStatement.bindId("billId", billId);
//			if(isNull(bill)){
//				preparedStatement.bindDate("processDate",processDate);
//				preparedStatement.bindDate("finalDate",finalDate);
//
//			}
			
			preparedStatement.setAutoclose(false);
			
			taxCalcVerifyErrList = preparedStatement.list();
			logger.debug("lineCalcErrList.size() :" + taxCalcVerifyErrList.size());
			
			if (notNull(taxCalcVerifyErrList) && taxCalcVerifyErrList.size() > 0)
			{
				isLineClaculations = true;
				addInvoiceDataError(taxCalcVerifyErrList, messageCat, verifyTaxCalc, CustomMessageRepository.dataIntegrityErrMessage(verifyTaxCalc.intValue()).getMessageText());
				isLineClaculations = false;
			}
			
		}
			
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataTaxCalcVerify method, Error -", e);
			}
			finally
			{
				if (notNull(preparedStatement))
				{
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		
		
		/**
		 * checkTotalCharges() method checks total charges equals sum of sections
		 * @param billId
		 */
//		private void checkTotalCharges(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkTotalCharges(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkTotalCharges");
			
			StringBuilder getTotalChargesStrBuilder = new StringBuilder();
			PreparedStatement getTotalChargesPrepStat = null;
			List<SQLResultRow> totalChargesErrList;
			
			try
			{
				getTotalChargesStrBuilder.append(" WITH CHG AS (SELECT BILL_ID, SUM(CALC_AMT) AS CHG_LINE_AMT ");
				getTotalChargesStrBuilder.append(" FROM CM_INV_DATA_LN_BCL WHERE TRIM (BCL_TYPE) <> :fmAmt ");
				getTotalChargesStrBuilder.append(" AND BILL_ID = :billId ");
				//Add ILMCheck filter for Perf Issues
//				if(isNull(bill)){
//					getTotalChargesStrBuilder.append(" AND ILM_DT >= :processDate AND ILM_DT < :finalDate");
//				}
				getTotalChargesStrBuilder.append(" GROUP BY BILL_ID), ");
				getTotalChargesStrBuilder.append(" TAX AS (SELECT BILL_ID, SUM(CALC_AMT) AS TAX_AMT ");
				getTotalChargesStrBuilder.append(" FROM CM_INV_DATA_TAX WHERE BILL_ID = :billId ");
//				if(isNull(bill)){
//					getTotalChargesStrBuilder.append(" AND ILM_DT >= :processDate AND ILM_DT < :finalDate");
//				}
				getTotalChargesStrBuilder.append(" GROUP BY BILL_ID) ");
				getTotalChargesStrBuilder.append(" SELECT INVDT.BILL_ID, ' ' BSEG_ID ");
				getTotalChargesStrBuilder.append(" FROM CM_INVOICE_DATA INVDT, CHG, TAX ");
				getTotalChargesStrBuilder.append(" WHERE INVDT.BILL_ID = CHG.BILL_ID (+) ");
				getTotalChargesStrBuilder.append(" AND INVDT.BILL_ID = TAX.BILL_ID (+) ");
				getTotalChargesStrBuilder.append(" AND INVDT.ACCT_TYPE = :charge ");
				getTotalChargesStrBuilder.append(" AND INVDT.BILL_ID = :billId ");
//				if(isNull(bill)){
//					getTotalChargesStrBuilder.append(" AND INVDT.ILM_DT >= :processDate AND INVDT.ILM_DT < :finalDate");
//				}
				getTotalChargesStrBuilder.append(" AND ABS(INVDT.CALC_AMT - (NVL(CHG_LINE_AMT, 0) + NVL(TAX_AMT, 0)))>0.05 ");
				
				getTotalChargesPrepStat = createPreparedStatement(getTotalChargesStrBuilder.toString(), "get Total Charges");
				getTotalChargesPrepStat.bindId("billId", billId);
//				if(isNull(bill)){
//					getTotalChargesPrepStat.bindDate("processDate",processDate);
//					getTotalChargesPrepStat.bindDate("finalDate",finalDate);
//
//				}
				getTotalChargesPrepStat.bindString("fmAmt", FMAMT, "BCL_TYPE");
				getTotalChargesPrepStat.bindString("charge", CHARGE, "ACCT_TYPE");
				getTotalChargesPrepStat.setAutoclose(false);
				
				totalChargesErrList = getTotalChargesPrepStat.list();
				logger.debug("totalChargesErrList.size() :" + totalChargesErrList.size());
				
				if (notNull(totalChargesErrList) && totalChargesErrList.size() > 0)
				{
					addInvoiceDataError(totalChargesErrList, messageCat, totalChargesMismatchMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(totalChargesMismatchMsgNbr.intValue()).getMessageText());
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkTotalCharges method, Error -", e);
			}
			finally
			{
				if (notNull(getTotalChargesPrepStat))
				{
					getTotalChargesPrepStat.close();
					getTotalChargesPrepStat = null;
				}
			}
		}
		
		
		/**
		 * checkLineClaculations() method checks line calculations
		 * @param billId
		 */
//		private void checkLineCalculations(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
/*		private void checkLineCalculations(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkLineClaculations");
			
			StringBuilder getLineClacStrBuilder = new StringBuilder();
			PreparedStatement getLineCalcPrepStat = null;
			List<SQLResultRow> lineCalcErrList;
			
			try
			{

				getLineClacStrBuilder.append(" WITH rate AS	");																		
				getLineClacStrBuilder.append(" (SELECT rate.bseg_id,price_asgn_id,MAX(msc_pi) AS msc_pi,MAX(msc_pc) AS msc_pc, "); 
				getLineClacStrBuilder.append(" MAX(asf_pc) AS asf_pc, ");
				getLineClacStrBuilder.append(" MAX(asf_pi) AS asf_pi, ");
				getLineClacStrBuilder.append(" nvl(ap2b.svc_qty,nvl(p2b.svc_qty,1)) as pr_to_bill_fx "); 
				getLineClacStrBuilder.append(" FROM ");
				getLineClacStrBuilder.append(" (SELECT bseg_id, NVL(price_asgn_id,'0') AS price_asgn_id, ");
				getLineClacStrBuilder.append(" CASE WHEN TRIM(rate_tp) IN (:mscPI, :minPChrg, :mpi) ");
				getLineClacStrBuilder.append(" THEN rate ELSE NULL END MSC_PI, ");
				getLineClacStrBuilder.append(" CASE ");
				getLineClacStrBuilder.append(" WHEN TRIM(rate_tp) IN (:mscPC, :cstPC) ");
				getLineClacStrBuilder.append(" THEN rate "); 
				getLineClacStrBuilder.append(" ELSE NULL "); 
				getLineClacStrBuilder.append(" END MSC_PC, "); 
				getLineClacStrBuilder.append(" CASE ");
				getLineClacStrBuilder.append(" WHEN TRIM(rate_tp) IN (:asfPC) ");
				getLineClacStrBuilder.append(" THEN rate ");
				getLineClacStrBuilder.append(" ELSE NULL ");
				getLineClacStrBuilder.append(" END ASF_PC, "); 
				getLineClacStrBuilder.append(" CASE ");
				getLineClacStrBuilder.append(" WHEN TRIM(rate_tp) IN (:asfPI) ");
				getLineClacStrBuilder.append(" THEN rate ");
				getLineClacStrBuilder.append(" ELSE NULL ");
				getLineClacStrBuilder.append(" END ASF_PI ");
				getLineClacStrBuilder.append(" FROM cisadm.cm_inv_data_ln_rate WHERE BILL_ID = :billId ");
				getLineClacStrBuilder.append(" ) rate, "); 
				getLineClacStrBuilder.append(" cisadm.cm_inv_data_ln_svc_qty p2b, ");
				getLineClacStrBuilder.append(" cisadm.cm_inv_data_ln_svc_qty ap2b ");
				getLineClacStrBuilder.append(" where rate.bseg_id = p2b.bseg_id (+) ");
				getLineClacStrBuilder.append(" and rate.bseg_id = ap2b.bseg_id (+) ");
				getLineClacStrBuilder.append(" and p2b.sqi_cd (+) = :pbefx ");
				getLineClacStrBuilder.append(" and ap2b.sqi_cd (+) =:apbefx ");
				getLineClacStrBuilder.append(" GROUP BY rate.bseg_id, ");
				getLineClacStrBuilder.append(" price_asgn_id, ");
				getLineClacStrBuilder.append(" nvl(ap2b.svc_qty,nvl(p2b.svc_qty,1))), ");
				getLineClacStrBuilder.append(" fund AS "); 
				getLineClacStrBuilder.append(" (SELECT a.bseg_id, ");
				getLineClacStrBuilder.append(" SUM(calc_amt*nvl(svc_qty,1)) AS FUND_AMT ");
				getLineClacStrBuilder.append(" FROM cisadm.cm_inv_data_ln_bcl a, "); 
				getLineClacStrBuilder.append(" cisadm.cm_inv_data_ln_svc_qty b "); 
				getLineClacStrBuilder.append(" WHERE a.bseg_id = b.bseg_id (+) ");
				getLineClacStrBuilder.append(" and TRIM(bcl_type) = :fmAmt ");
				getLineClacStrBuilder.append(" and sqi_cd (+) = :fndBaseFx AND A.BILL_ID = :billId ");
				getLineClacStrBuilder.append(" GROUP BY a.bseg_id ");
				getLineClacStrBuilder.append(" ), ");
				getLineClacStrBuilder.append(" ic_chg AS ");
				getLineClacStrBuilder.append(" (SELECT bseg_id, ");
				getLineClacStrBuilder.append(" SUM(calc_amt) AS ic_amt ");
				getLineClacStrBuilder.append(" FROM cisadm.cm_inv_data_ln_bcl ");
				getLineClacStrBuilder.append(" WHERE TRIM(bcl_type) = :icBM AND BILL_ID = :billId "); 
				getLineClacStrBuilder.append(" GROUP BY bseg_id ");
				getLineClacStrBuilder.append(" ), ");
				getLineClacStrBuilder.append(" sf_chg AS "); 
				getLineClacStrBuilder.append(" (SELECT bseg_id, ");
				getLineClacStrBuilder.append(" SUM(calc_amt) AS sf_amt "); 
				getLineClacStrBuilder.append(" FROM cisadm.cm_inv_data_ln_bcl ");
				getLineClacStrBuilder.append(" WHERE TRIM(bcl_type) IN (:csBM, :csBM1, :csBM2, :asfpcMB, :asfpiMB, :ctMBA) ");
				getLineClacStrBuilder.append(" AND BILL_ID = :billId GROUP BY bseg_id ");
				getLineClacStrBuilder.append(" ), ");
				getLineClacStrBuilder.append(" acq_chg AS ");
				getLineClacStrBuilder.append(" (SELECT bseg_id, ");
				getLineClacStrBuilder.append(" SUM(calc_amt) AS chg_amt ");
				getLineClacStrBuilder.append(" FROM cisadm.cm_inv_data_ln_bcl ");
				getLineClacStrBuilder.append(" WHERE TRIM(bcl_type) IN (:totalBb, :piMBA, :pcMBA, :piRECUR) ");
				getLineClacStrBuilder.append(" AND BILL_ID = :billId GROUP BY bseg_id ");
				getLineClacStrBuilder.append(" ) "); 
				getLineClacStrBuilder.append(" SELECT invdt.bill_id,' ' AS bseg_id,invdt.currency_cd, ccy.decimal_positions, udf_char_25 AS sett_ccy, price_category,SUM(fund_amt) AS txn_value, ");
				getLineClacStrBuilder.append(" price_asgn_id, msc_pc,SUM(svc_qty) AS txn_vol, msc_pi,SUM(ic_amt) AS ic_amt,asf_pc,asf_pi,SUM(sf_amt)AS sf_amt, ");
				getLineClacStrBuilder.append(" SUM(chg_amt) AS msc_amt,NVL(SUM(ic_amt),0) + NVL(SUM(sf_amt),0) + NVL(SUM(chg_amt),0) AS tot_chg, ");			
				getLineClacStrBuilder.append(" NVL(SUM(ic_amt),0) + NVL(SUM(sf_amt),0) + ROUND(((NVL(msc_pc,0)*NVL(SUM(fund_amt),0))+ (NVL(msc_pi,0)*pr_to_bill_fx*NVL(SUM(svc_qty),0))),ccy.decimal_positions)AS tot_derived_chg ");
				getLineClacStrBuilder.append(" FROM cisadm.cm_invoice_data invdt, cisadm.cm_invoice_data_ln ln,cisadm.cm_inv_data_ln_svc_qty sq, fund,rate, ic_chg,sf_chg,acq_chg, cisadm.ci_currency_cd ccy "); 
				getLineClacStrBuilder.append(" WHERE invdt.bill_id = ln.bill_id ");
				getLineClacStrBuilder.append(" AND ln.bseg_id= sq.bseg_id (+) ");
				getLineClacStrBuilder.append(" AND ln.bseg_id= fund.bseg_id (+) ");
				getLineClacStrBuilder.append(" AND ln.bseg_id= rate.bseg_id (+) ");
				
				getLineClacStrBuilder.append(" AND ln.bseg_id= ic_chg.bseg_id (+) ");
				getLineClacStrBuilder.append(" AND ln.bseg_id= sf_chg.bseg_id (+) ");
				getLineClacStrBuilder.append(" AND ln.bseg_id= acq_chg.bseg_id (+) ");
				getLineClacStrBuilder.append(" AND invdt.currency_cd = ccy.currency_cd "); 
				getLineClacStrBuilder.append(" AND sq.sqi_cd (+) in (:transVol, :number) ");
				getLineClacStrBuilder.append(" AND NVL(ln.udf_char_25, ln.currency_cd) = ln.currency_cd ");
				getLineClacStrBuilder.append(" AND ln.ilm_dt > :sysdate");
				getLineClacStrBuilder.append(" AND invdt.acct_type = :charge ");
				getLineClacStrBuilder.append(" AND price_category <> :minChrg ");
				getLineClacStrBuilder.append(" AND INVDT.BILL_ID = :billId "); 
				getLineClacStrBuilder.append(" GROUP BY invdt.bill_id, invdt.currency_cd,ccy.decimal_positions,udf_char_25,price_category,price_asgn_id, "); 
				getLineClacStrBuilder.append(" msc_pc,msc_pi, asf_pc,asf_pi , "); 
				getLineClacStrBuilder.append(" pr_to_bill_fx "); 
				getLineClacStrBuilder.append(" HAVING NVL(SUM(ic_amt),0) + NVL(SUM(sf_amt),0) + NVL(SUM(chg_amt),0) <> NVL(SUM(ic_amt),0) + NVL(SUM(sf_amt),0) + ");
				getLineClacStrBuilder.append(" ROUND(((NVL(msc_pc,0) * NVL(SUM(fund_amt),0)) + (NVL(msc_pi,0)*pr_to_bill_fx*NVL(SUM(svc_qty),0))),ccy.decimal_positions) "); 
				getLineClacStrBuilder.append(" AND NVL(SUM(ic_amt),0)+ NVL(SUM(sf_amt),0) + NVL(SUM(chg_amt),0) <> 0 "); 

				getLineCalcPrepStat = createPreparedStatement(getLineClacStrBuilder.toString(), "check Line Claculations");

				getLineCalcPrepStat.bindString("mscPI", MSC_PI_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("minPChrg", MIN_P_CHRG_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("mpi", M_PI_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("mscPC", MSC_PC_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("cstPC", CST_PC_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("asfPC", ASF_PC_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("asfPI", ASF_PI_RATE_TP, "RATE_TP");
				getLineCalcPrepStat.bindString("fmAmt", FMAMT, "BCL_TYPE");
				getLineCalcPrepStat.bindString("icBM", IC_BM_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("csBM", CS_BM_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("csBM1", CS_BM1_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("csBM2", CS_BM2_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("asfpcMB", ASFPC_MB_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("asfpiMB", ASFPI_MB_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("ctMBA", CT_MBA_BCL_TYPE, "BCL_TYPE");				
				getLineCalcPrepStat.bindString("totalBb", TOTAL_BB_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("piMBA", PI_MBA_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("pcMBA", PC_MBA_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("piRECUR", PI_RECUR_BCL_TYPE, "BCL_TYPE");
				getLineCalcPrepStat.bindString("transVol", TRANSACTION_VOLUME, "SQI_CD");
				getLineCalcPrepStat.bindString("fndBaseFx", FND_BASE_FX, "SQI_CD");
				getLineCalcPrepStat.bindString("pbefx", P_B_EFX, "SQI_CD");
				getLineCalcPrepStat.bindString("apbefx", AP_B_EFX, "SQI_CD");
				getLineCalcPrepStat.bindString("number", SQI_NUMBER, "SQI_CD");
				getLineCalcPrepStat.bindString("charge", CHARGE, "ACCT_TYPE");
				getLineCalcPrepStat.bindString("minChrg", MINCHARGE, "PRICE_CATEGORY");
				getLineCalcPrepStat.bindDate("sysdate", getSystemDateTime().getDate().addDays(-30));
				getLineCalcPrepStat.bindId("billId", billId);
				getLineCalcPrepStat.setAutoclose(false);
				
				lineCalcErrList = getLineCalcPrepStat.list();
				logger.debug("lineCalcErrList.size() :" + lineCalcErrList.size());
				
				if (notNull(lineCalcErrList) && lineCalcErrList.size() > 0)
				{
					isLineClaculations = true;
					addInvoiceDataError(lineCalcErrList, messageCat, lineCalcNotCorrectMsgNbr, "");
					isLineClaculations = false;
				}
				
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkLineClaculations method, Error -", e);
			}
			finally
			{
				if (notNull(getLineCalcPrepStat))
				{
					getLineCalcPrepStat.close();
					getLineCalcPrepStat = null;
				}
			}
		}
		
		***/
		/**
		 * checkInvoiceDataBCL() method checks all applicable bill segments exist in invoice data line
		 * @param billId
		 */
//		private void checkInvoiceDataBCL(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkInvoiceDataBCL(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkInvoiceDataBCL");
			
			StringBuilder getInvoiceDataBCLStrBuilder = new StringBuilder();
			PreparedStatement getInvoiceDataBCLPrepStat = null;
			List<SQLResultRow> billInvoiceDataBCLErrList;
			
			try
			{
				getInvoiceDataBCLStrBuilder.append(" SELECT LN.BILL_ID, LN.BSEG_ID ");
				getInvoiceDataBCLStrBuilder.append(" FROM CM_INVOICE_DATA_LN LN ");
				getInvoiceDataBCLStrBuilder.append(" WHERE LN.BILL_ID =:billId ");
//				if(isNull(bill)){
//					getInvoiceDataBCLStrBuilder.append(" AND LN.ILM_DT >= :processDate AND LN.ILM_DT < :finalDate");
//				}
				getInvoiceDataBCLStrBuilder.append(" AND NOT EXISTS ");
				getInvoiceDataBCLStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_LN_BCL BCL ");
				getInvoiceDataBCLStrBuilder.append(" WHERE BCL.BSEG_ID=LN.BSEG_ID ");
//				if(isNull(bill)){
//					getInvoiceDataBCLStrBuilder.append(" AND BCL.ILM_DT >= :processDate AND BCL.ILM_DT < :finalDate");
//				}
				getInvoiceDataBCLStrBuilder.append(" AND TRIM(BCL.BCL_TYPE) <> :fmAmt ) ");
				
				getInvoiceDataBCLPrepStat = createPreparedStatement(getInvoiceDataBCLStrBuilder.toString(), "get Invoice Data BCL"); 
				getInvoiceDataBCLPrepStat.bindId("billId", billId);
//				if(isNull(bill)){
//					getInvoiceDataBCLPrepStat.bindDate("processDate",processDate);
//					getInvoiceDataBCLPrepStat.bindDate("finalDate",finalDate);
//
//				}
				getInvoiceDataBCLPrepStat.bindString("fmAmt", FMAMT, "BCL_TYPE");
				getInvoiceDataBCLPrepStat.setAutoclose(false);
				
				billInvoiceDataBCLErrList = getInvoiceDataBCLPrepStat.list();
				logger.debug("billInvoiceDataBCLErrList.size() :" + billInvoiceDataBCLErrList.size());
				
				if (notNull(billInvoiceDataBCLErrList) && billInvoiceDataBCLErrList.size() > 0)
				{
					addInvoiceDataError(billInvoiceDataBCLErrList, messageCat, invDtBclNoRecMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(invDtBclNoRecMsgNbr.intValue()).getMessageText());
				}
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataBCL method, Error -", e);
			}
			finally
			{
				if (notNull(getInvoiceDataBCLPrepStat))
				{
					getInvoiceDataBCLPrepStat.close();
					getInvoiceDataBCLPrepStat = null;
				}
			}
		}
		
		
		/**
		 * checkInvoiceDataLnRate() method checks all applicable bill segments exist in invoice data line Rate
		 * @param billId
		 */
//		private void checkInvoiceDataLnRate(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkInvoiceDataLnRate(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkInvoiceDataLnRate");
			
			StringBuilder getInvoiceDataLnRateStrBuilder = new StringBuilder();
			PreparedStatement getInvoiceDataLnRatePrepStat = null;
			List<SQLResultRow> billInvoiceDataLnRateErrList;
			
			try
			{
				getInvoiceDataLnRateStrBuilder.append(" SELECT LN.BILL_ID, LN.BSEG_ID ");
				getInvoiceDataLnRateStrBuilder.append(" FROM CM_INVOICE_DATA_LN LN, CI_PRICEITEM_CHAR PRCH ");
				getInvoiceDataLnRateStrBuilder.append(" WHERE LN.BILL_ID =:billId ");
//				if(isNull(bill)){
//					getInvoiceDataLnRateStrBuilder.append(" AND LN.ILM_DT >= :processDate AND LN.ILM_DT < :finalDate");
//				}
				
				getInvoiceDataLnRateStrBuilder.append(" AND LN.PRICE_CATEGORY = PRCH.PRICEITEM_CD ");
				getInvoiceDataLnRateStrBuilder.append(" AND PRCH.CHAR_TYPE_CD = :rateFlgCharType ");
				getInvoiceDataLnRateStrBuilder.append(" AND PRCH.CHAR_VAL = :charVal ");
				
				getInvoiceDataLnRateStrBuilder.append(" AND NOT EXISTS ");
				getInvoiceDataLnRateStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_LN_RATE RATE WHERE ");
//				if(isNull(bill)){
//					getInvoiceDataLnRateStrBuilder.append(" RATE.ILM_DT >= :processDate AND RATE.ILM_DT < :finalDate AND");
//				}
				getInvoiceDataLnRateStrBuilder.append(" RATE.BSEG_ID=LN.BSEG_ID) ");
				
				getInvoiceDataLnRatePrepStat = createPreparedStatement(getInvoiceDataLnRateStrBuilder.toString(), "get Invoice Data Ln Rate");
				getInvoiceDataLnRatePrepStat.bindId("billId", billId);
				getInvoiceDataLnRatePrepStat.bindString("rateFlgCharType", RATE_TYPE_FLAG, "CHAR_TYPE_CD");
				getInvoiceDataLnRatePrepStat.bindString("charVal", YES, "CHAR_VAL");
//				if(isNull(bill)){
//					getInvoiceDataLnRatePrepStat.bindDate("processDate",processDate);
//					getInvoiceDataLnRatePrepStat.bindDate("finalDate",finalDate);
//
//				}
				getInvoiceDataLnRatePrepStat.setAutoclose(false);
				
				billInvoiceDataLnRateErrList = getInvoiceDataLnRatePrepStat.list();
				logger.debug("billInvoiceDataLnRateErrList.size() :" + billInvoiceDataLnRateErrList.size());
				
				if (notNull(billInvoiceDataLnRateErrList) && billInvoiceDataLnRateErrList.size() > 0)
				{
					addInvoiceDataError(billInvoiceDataLnRateErrList, messageCat, invDtLnRateNoRecMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(invDtLnRateNoRecMsgNbr.intValue()).getMessageText());
				}
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataLnRate method, Error -", e);
			}
			finally
			{
				if (notNull(getInvoiceDataLnRatePrepStat))
				{
					getInvoiceDataLnRatePrepStat.close();
					getInvoiceDataLnRatePrepStat = null;
				}
			}
		}
		
		
		/**
		 * checkInvoiceDataLnSQ() method checks all applicable bill segments exist in invoice data line Service Quantity
		 * @param billId
		 */
//		private void checkInvoiceDataLnSQ(Bill_Id billId, Bill bill, Date processDate, Date finalDate)
		private void checkInvoiceDataLnSQ(Bill_Id billId)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkInvoiceDataLnSQ");
			
			StringBuilder getInvoiceDataLnSQStrBuilder = new StringBuilder();
			PreparedStatement getInvoiceDataLnSQPrepStat = null;
			List<SQLResultRow> billInvoiceDataLnSQErrList;
			
			try
			{
				getInvoiceDataLnSQStrBuilder.append(" SELECT LN.BILL_ID, LN.BSEG_ID ");
				getInvoiceDataLnSQStrBuilder.append(" FROM CM_INVOICE_DATA_LN LN ");
				getInvoiceDataLnSQStrBuilder.append(" WHERE LN.BILL_ID =:billId ");
//				if(isNull(bill)){
//					getInvoiceDataLnSQStrBuilder.append(" AND LN.ILM_DT >= :processDate AND LN.ILM_DT < :finalDate");
//				}
				getInvoiceDataLnSQStrBuilder.append(" AND NOT EXISTS ");
				getInvoiceDataLnSQStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_LN_SVC_QTY SQ WHERE ");
//				if(isNull(bill)){
//					getInvoiceDataLnSQStrBuilder.append(" SQ.ILM_DT >= :processDate AND SQ.ILM_DT < :finalDate AND ");
//				}
				
//				getInvoiceDataLnSQStrBuilder.append(" SQ.SQI_CD = :transactionVolume AND ");
				getInvoiceDataLnSQStrBuilder.append(" SQ.SQI_CD IN (:transactionVolume, :number) AND ");
				
				getInvoiceDataLnSQStrBuilder.append(" SQ.BSEG_ID=LN.BSEG_ID)  ");
				
				getInvoiceDataLnSQPrepStat = createPreparedStatement(getInvoiceDataLnSQStrBuilder.toString(), "get Invoice Data Ln SQ");
				getInvoiceDataLnSQPrepStat.bindId("billId", billId);
//				if(isNull(bill)){
//					getInvoiceDataLnSQPrepStat.bindDate("processDate",processDate);
//					getInvoiceDataLnSQPrepStat.bindDate("finalDate",finalDate);
//
//				}
				getInvoiceDataLnSQPrepStat.bindString("transactionVolume", TRANSACTION_VOLUME, "SQI_CD");
				getInvoiceDataLnSQPrepStat.bindString("number", SQI_NUMBER, "SQI_CD");

				getInvoiceDataLnSQPrepStat.setAutoclose(false);
				
				billInvoiceDataLnSQErrList = getInvoiceDataLnSQPrepStat.list();
				logger.debug("billInvoiceDataLnSQErrList.size() :" + billInvoiceDataLnSQErrList.size());
				
				if (notNull(billInvoiceDataLnSQErrList) && billInvoiceDataLnSQErrList.size() > 0)
				{
					addInvoiceDataError(billInvoiceDataLnSQErrList, messageCat, invDtLnSvcQtNoRecMsgNbr, CustomMessageRepository.dataIntegrityErrMessage(invDtLnSvcQtNoRecMsgNbr.intValue()).getMessageText());
				}
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataLnSQ method, Error -", e);
			}
			finally
			{
				if (notNull(getInvoiceDataLnSQPrepStat))
				{
					getInvoiceDataLnSQPrepStat.close();
					getInvoiceDataLnSQPrepStat = null;
				}
			}
		}
		
		
//		private void checkLinePriceCategory(Bill_Id billId, Bill bill,Date processDate, Date finalDate)
		private void checkLinePriceCategory(Bill_Id billId)
		{	
			logger.debug("Inside CmInvoiceDataIntegrityInterface checkLinePriceCategory");
			StringBuilder getInvoiceDataPriceCategoryLn = new StringBuilder();
			PreparedStatement getInvoiceDataLnStat = null;
			List<SQLResultRow> billInvoiceDataLnErrList;
			
			try
			{
				getInvoiceDataPriceCategoryLn.append(" SELECT LN.BILL_ID, LN.BSEG_ID,LN.PRICE_CATEGORY ");
				getInvoiceDataPriceCategoryLn.append(" FROM CM_INVOICE_DATA_LN LN ");
				getInvoiceDataPriceCategoryLn.append(" WHERE LN.BILL_ID =:billId ");
//				if(isNull(bill)){
//					getInvoiceDataPriceCategoryLn.append(" AND LN.ILM_DT >= :processDate AND LN.ILM_DT < :finalDate");
//				}
				getInvoiceDataPriceCategoryLn.append(" AND EXISTS ");
				getInvoiceDataPriceCategoryLn.append(" (SELECT 1 FROM CM_CHARGE_TYPE_MAP B ");
				getInvoiceDataPriceCategoryLn.append(" WHERE TRIM(LN.PRICE_CATEGORY)=B.PARENT_CHARGE_TYPE)  ");
				
				getInvoiceDataLnStat = createPreparedStatement(getInvoiceDataPriceCategoryLn.toString(), "get Invoice Data Ln Price Category");
				getInvoiceDataLnStat.bindId("billId", billId);
//				if(isNull(bill)){
//					getInvoiceDataLnStat.bindDate("processDate",processDate);
//					getInvoiceDataLnStat.bindDate("finalDate",finalDate);
//
//				}
				getInvoiceDataLnStat.setAutoclose(false);
				
				billInvoiceDataLnErrList = getInvoiceDataLnStat.list();
				logger.debug("billInvoiceDataLnErrList.size() :" + billInvoiceDataLnErrList.size());
				
				if (notNull(billInvoiceDataLnErrList) && billInvoiceDataLnErrList.size() > 0)
				{
					isLineClaculations = true;
					addInvoiceDataError(billInvoiceDataLnErrList, messageCat, invDtLnPriceCategoryNoRecMsgNbr, "");
					isLineClaculations = false;
				}
			}
			catch (Exception e)
			{
				logger.error("Inside invoice data interface, checkInvoiceDataLnSQ method, Error -", e);
			}
			finally
			{
				if (notNull(getInvoiceDataLnStat))
				{
					getInvoiceDataLnStat.close();
					getInvoiceDataLnStat = null;
				}
			}
		}
		
		
		/**
		 * addInvoiceDataError() method adds all the billIds and bsegIds in CM_INV_DATA_ERR table also adds the biiiId to billErrSet
		 * @param errList
		 * @param messageCat
		 * @param messageNbr
		 * @param messageInfo
		 */
		public void addInvoiceDataError(List<SQLResultRow> errList, BigInteger messageCat, BigInteger messageNbr, String messageInfo)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface addInvoiceDataError");
			
			String billId;
			String bSegId;
			StringBuilder insertInvDataErrStrBuilder;
			PreparedStatement insertInvDataErrPrepStat = null;
			String priceCategory;
			Boolean errFlag = true;
			String partyId;

			logger.debug("addInvoiceDataError errList.size():" + errList.size());
			
			for (SQLResultRow billBseg : errList)
			{
				insertInvDataErrStrBuilder = new StringBuilder();
				billId = billBseg.getString("BILL_ID");
				bSegId = billBseg.getString("BSEG_ID");
				
				if (isLineClaculations)
				{
					priceCategory = billBseg.getString("PRICE_CATEGORY");
					messageInfo = CustomMessageRepository.dataIntegrityErrMessage(messageNbr.intValue(), priceCategory).getMessageText();
				}

				if (isMinChargeCalculations) {
					partyId = billBseg.getString("PARTY_ID");
					messageInfo = CustomMessageRepository.dataIntegrityErrMessage(messageNbr.intValue(), partyId).getMessageText();
					if(billBseg.getBigDecimal("DIFF").compareTo(BigDecimal.ZERO) != 0){
						errFlag = true;
					}
					else{
						errFlag = false;
					}
				}

				if(errFlag){
					insertInvDataErrStrBuilder.append(" INSERT INTO CM_INV_DATA_ERR (BILL_ID, BSEG_ID, MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
					insertInvDataErrStrBuilder.append(" VALUES (:billId, :bSegId, :msgCatNbr, :msgNbr, :errInfo) ");

					insertInvDataErrPrepStat = createPreparedStatement(insertInvDataErrStrBuilder.toString(), "Insert into CM_INV_DATA_ERR");
					insertInvDataErrPrepStat.bindString("billId", billId, "BILL_ID");
					insertInvDataErrPrepStat.bindString("bSegId", bSegId, "BSEG_ID");
					insertInvDataErrPrepStat.bindBigInteger("msgCatNbr", messageCat);
					insertInvDataErrPrepStat.bindBigInteger("msgNbr", messageNbr);
					insertInvDataErrPrepStat.bindString("errInfo", messageInfo, "Error Info");

					insertInvDataErrPrepStat.executeUpdate();

					billErrSet.add(billId);
				}

			}
		}
		
		
		/**
		 *  getBillList() method selects the Bill Ids between lowBillId and highBillId.
		 * @param invData
		 * @return getBillList
		 */
		private List<SQLResultRow> getBillList(InvData_Id invData)
		{
			logger.debug("Inside CmInvoiceDataIntegrityInterface getBillList");
			
			String invLowBillId = null;
			String invHighBillId = null;
			StringBuilder getBillListStrBuilder = new StringBuilder();
			PreparedStatement getBillListPrepStat = null;
			List<SQLResultRow> billList = null;
			Bill bill_Id = getParameters().getBillId();
			
			try 
			{
				invLowBillId = invData.getInvLowBillId();
				invHighBillId = invData.getInvHighBillId();
				
				getBillListStrBuilder.append(" SELECT BILL_ID FROM CM_INVOICE_DATA INVD WHERE ");
				getBillListStrBuilder.append(" BILL_ID BETWEEN :lowBillId AND :highBillId AND CR_NOTE_FR_BILL_ID is null");
				
				if (isNull(bill_Id)) // ilmdate not required when bill_id is there
				{
					getBillListStrBuilder.append(" AND ILM_DT >= :processDate AND ILM_DT < :finalDate ");
				}
				getBillListStrBuilder.append(" AND EXTRACT_FLG = :yes ");
				getBillListStrBuilder.append(" AND NOT EXISTS ");
				getBillListStrBuilder.append(" (SELECT 1 FROM CM_INV_DATA_EXCP EXP ");
				getBillListStrBuilder.append(" WHERE EXP.BILL_ID = INVD.BILL_ID) ");
				
				getBillListPrepStat = createPreparedStatement(getBillListStrBuilder.toString(), "get Bill List");
				
				if (isNull(bill_Id)) // ilmdate not required when bill_id is there
				{
					getBillListPrepStat.bindDate("processDate", getProcessDateTime().getDate());
					getBillListPrepStat.bindDate("finalDate", getProcessDateTime().getDate().addDays(1));
				}
				
				getBillListPrepStat.bindId("lowBillId", new Bill_Id(invLowBillId));
				getBillListPrepStat.bindId("highBillId", new Bill_Id(invHighBillId));
				getBillListPrepStat.bindLookup("yes", SwitchFlagLookup.constants.YES_FIELD);
				getBillListPrepStat.setAutoclose(false);
				
				billList = getBillListPrepStat.list();
				logger.debug("getBillList.size() :" + billList.size());
			}
			catch (Exception e)
			{
				logger.error("Inside getBillList method, Error is :", e);
			}
			finally
			{
				if (notNull(getBillListPrepStat))
				{
					getBillListPrepStat.close();
					getBillListPrepStat = null;
				}
			}
			logger.debug("End CmInvoiceDataIntegrityInterface getBillList");
			return billList;
			
		}

		public boolean isLineClaculations() {
			return isLineClaculations;
		}

		public void setLineClaculations(boolean isLineClaculations) {
			this.isLineClaculations = isLineClaculations;
		}

		public boolean isMinChargeCalculations() {
			return isMinChargeCalculations;
		}

		public void setMinChargeCalculations(boolean isMinChargeCalculations) {
			this.isMinChargeCalculations = isMinChargeCalculations;
		}

	}
	
	
	public static final class InvData_Id implements Id
	{
		private static final long serialVersionUID = 1L;
		private String invLowBillId;
		private String invHighBillId;

		public InvData_Id(String invLowBillId, String invHighBillId)
		{
			setInvLowBillId(invLowBillId);
			setInvHighBillId(invHighBillId);
		}

		public boolean isNull()
		{
			return false;
		}

		public void appendContents(StringBuilder arg0){
		}

		public static long getSerialVersionUID()
		{
			return serialVersionUID;
		}

		public String getInvLowBillId()
		{
			return invLowBillId;
		}

		public void setInvLowBillId(String invLowBillId)
		{
			this.invLowBillId = invLowBillId;
		}

		public String getInvHighBillId()
		{
			return invHighBillId;
		}

		public void setInvHighBillId(String invHighBillId)
		{
			this.invHighBillId = invHighBillId;
		}

	}



}
