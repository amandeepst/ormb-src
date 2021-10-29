/*******************************************************************************
 * FileName                   : MerchantLedgerAccountInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Oct 11, 2017
 * Version Number             : 0.3
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             11-Oct-2017         Vienna Rom    PAM-15298 Initial version
 0.2	  NAP-24276		 20-Mar-2018		 RIA		   OUTBOUND CM ILM: CM_MLEGR(Add ILM fields)
 0.3	  NAP-31484		 06-Aug-2018		 Vienna Rom    Added group by sub-ledger to fix unique constraint
 0.4	  NAP-57540		 06-Jan-2020		 Vikalp        Tuned insert query for performance
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.DateTime;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.domain.customerinfo.person.Person_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author vrom
 *
@BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = type, type = string)
 *            , @BatchJobSoftParameter (name = chunkSize, required = true, type = integer)})
 */
public class MerchantLedgerAccountInterface extends
		MerchantLedgerAccountInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(MerchantLedgerAccountInterface.class);
	
	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		
		List<ThreadWorkUnit> threadWorkUnitList = getMerchantLedgerAccountData();

		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}
	
	/**
	 * getMerchantLedgerAccountData() method selects Person Ids for processing by this Interface.
	 * 
	 * @return List low/high ids 
	 */
	private List<ThreadWorkUnit> getMerchantLedgerAccountData() {
		
		//Retrieve batch parameters
		BigInteger chunkSize = getParameters().getChunkSize();

		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		String lowPersonId = "";
		String highPersonId = "";
		MerchantLedgerAccountData_Id merchantLedgerAccountData = null;		

		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		DateTime ilmDate=getSystemDateTime();

		/* Chunking query
		 * Subquery gets all person ids.
		 * From these person ids, the min and max ids of each chunk (work unit) are determined based on the row number.
		 */
		try {
			stringBuilder.append("WITH TBL AS (SELECT PER_ID FROM CI_PER ORDER BY PER_ID) ");
			stringBuilder.append("SELECT THREAD_NUM, MIN(PER_ID) AS LOW_PER_ID, MAX(PER_ID) AS HIGH_PER_ID ");
			stringBuilder.append("FROM (SELECT PER_ID, CEIL(ROWNUM/:chunkSize) AS THREAD_NUM FROM TBL) ");
			stringBuilder.append("GROUP BY THREAD_NUM ");
			stringBuilder.append("ORDER BY 1 ");

			preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
			preparedStatement.bindBigInteger("chunkSize", chunkSize);
			preparedStatement.setAutoclose(false);

			for (SQLResultRow sqlRow : preparedStatement.list()) {
				lowPersonId = sqlRow.getString("LOW_PER_ID");
				highPersonId = sqlRow.getString("HIGH_PER_ID");
				merchantLedgerAccountData = new MerchantLedgerAccountData_Id(lowPersonId, highPersonId);

				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(merchantLedgerAccountData);
				threadworkUnit.addSupplementalData("ilmDate",ilmDate);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				merchantLedgerAccountData = null;
			}

		} catch (ThreadAbortedException e) {
			logger.error("Inside getMerchantLedgerAccountData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getMerchantLedgerAccountData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}	

		return threadWorkUnitList;
	}

	public Class<MerchantLedgerAccountInterfaceWorker> getThreadWorkerClass() {
		return MerchantLedgerAccountInterfaceWorker.class;
	}

	public static class MerchantLedgerAccountInterfaceWorker extends
			MerchantLedgerAccountInterfaceWorker_Gen {
		
		private InvoiceDataInterfaceLookUp invoiceDataInterfaceLookUp = new InvoiceDataInterfaceLookUp();

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			
			MerchantLedgerAccountData_Id merchantLedgerAccountData = (MerchantLedgerAccountData_Id) unit.getPrimaryId();
			String lowPersonId = merchantLedgerAccountData.getLowPersonId();
			String highPersonId = merchantLedgerAccountData.getHighPersonId();
			DateTime ilmDate= (DateTime) unit.getSupplementallData("ilmDate");
			String type = getParameters().getType();
			String postingType = null;
			if ("B".equalsIgnoreCase(type)) {
				postingType ="BILLING";
			}
			else if ("P".equalsIgnoreCase(type)) {
				postingType ="PAYMENT";
			}
			//NAP-24276 -  OUTBOUND CM ILM: CM_MLEGR - Start Change			
			
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			
			try{
				stringBuilder.append(" INSERT INTO cm_merch_ledger_acct  ");
				stringBuilder.append(" (per_id_nbr, cis_division, currency_cd, acct_nbr, sa_type_cd, sa_bal, acctbal, "); 
				stringBuilder.append(" upload_dttm, extract_flg, extract_dttm, ilm_dt, ilm_arch_sw, type) ");
				stringBuilder.append(" WITH t1 AS ( ");
				stringBuilder.append(" SELECT acct_id, acct_nbr , sa_type_cd , cis_division, currency_cd, per_id_nbr, SUM(sa_balance) AS sa_balance ");
				stringBuilder.append(" FROM (SELECT an.acct_id, an.acct_nbr , sa.sa_type_cd , sa.cis_division, sa.currency_cd, pi.per_id_nbr, ");
				stringBuilder.append(" nvl((SELECT SUM(F.cur_amt) ");
				stringBuilder.append(" FROM ci_ft F ");
				stringBuilder.append(" WHERE F.sa_id=sa.sa_id ");
				stringBuilder.append(" AND F.freeze_sw = :y ");
				stringBuilder.append(" AND 1 = (CASE WHEN F.ft_type_flg = 'BS' AND NOT EXISTS ");
				stringBuilder.append(" (SELECT 1 FROM ci_bill WHERE bill_id = F.bill_id AND bill_stat_flg = 'C') THEN 0 ELSE 1 END) ");
				stringBuilder.append(" ), 0) AS sa_balance ");
				stringBuilder.append(" FROM ci_acct_per AP, ");
				stringBuilder.append(" ci_sa sa, ");
				stringBuilder.append(" ci_acct_nbr an, ");
				stringBuilder.append(" ci_per_id pi ");
				stringBuilder.append(" WHERE AP.acct_id=an.acct_id ");
				stringBuilder.append(" AND an.acct_id=sa.acct_id ");
				stringBuilder.append(" AND pi.per_id = AP.per_id ");
				stringBuilder.append(" AND pi.id_type_cd = :exprtyId ");
				stringBuilder.append(" AND pi.per_id BETWEEN :lowId AND :highId ");
				stringBuilder.append(" AND an.acct_nbr_type_cd = :acctType) ");
				stringBuilder.append(" GROUP BY acct_id, acct_nbr , sa_type_cd , cis_division, currency_cd, per_id_nbr), ");
				stringBuilder.append(" t2 AS ( ");
				stringBuilder.append(" SELECT t1.acct_id, ");
				stringBuilder.append(" SUM(t1.sa_balance) AS acct_balance ");
				stringBuilder.append(" FROM t1 ");
				stringBuilder.append(" GROUP BY t1.acct_id) ");
				stringBuilder.append(" SELECT t1.per_id_nbr, ");
				stringBuilder.append(" t1.cis_division, ");
				stringBuilder.append(" t1.currency_cd, ");
				stringBuilder.append(" t1.acct_nbr, ");
				stringBuilder.append(" t1.sa_type_cd, ");
				stringBuilder.append(" SUM(t1.sa_balance) AS sa_balance, ");
				stringBuilder.append(" SUM(t2.acct_balance) AS acct_balance, ");
				stringBuilder.append(" :systime AS upload_dttm, ");
				stringBuilder.append(" :y AS extract_flg, ");
				stringBuilder.append(" '' AS extract_dttm, ");
				stringBuilder.append(" :systime AS ilm_dt, ");
				stringBuilder.append(" :y AS ilm_arch_sw, ");
				stringBuilder.append(" :postingType AS TYPE ");
				stringBuilder.append(" FROM t1, t2 ");
				stringBuilder.append(" WHERE t1.acct_id = t2.acct_id ");
				stringBuilder.append(" GROUP BY ");
				stringBuilder.append(" t1.per_id_nbr, ");
				stringBuilder.append(" t1.cis_division, ");
				stringBuilder.append(" t1.currency_cd, ");
				stringBuilder.append(" t1.acct_nbr, ");
				stringBuilder.append(" t1.sa_type_cd, ");
				stringBuilder.append(" :systime, ");
				stringBuilder.append(" :y, ");
				stringBuilder.append(" '', ");
				stringBuilder.append(" :systime, ");
				stringBuilder.append(" :y, ");
				stringBuilder.append(":postingType ");
				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindId("lowId", new Person_Id(lowPersonId));
				preparedStatement.bindId("highId", new Person_Id(highPersonId)); 			
				preparedStatement.bindString("postingType", postingType, "TYPE");
				preparedStatement.bindString("acctType", invoiceDataInterfaceLookUp.getAccountType(), "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("exprtyId", invoiceDataInterfaceLookUp.getExternalPartyId(), "ID_TYPE_CD");
				preparedStatement.bindString("y", "Y", "FREEZE_SW");
				preparedStatement.bindDateTime("systime", ilmDate);
				preparedStatement.execute();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			//NAP-24276 -  OUTBOUND CM ILM: CM_MLEGR - End Change
			
			return true;
		}

	}
	
	public static final class MerchantLedgerAccountData_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String lowPersonId;

		private String highPersonId;

		public MerchantLedgerAccountData_Id(String lowPersonId, String highPersonId) {
			setLowPersonId(lowPersonId);
			setHighPersonId(highPersonId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getHighPersonId() {
			return highPersonId;
		}

		public void setHighPersonId(String highPersonId) {
			this.highPersonId = highPersonId;
		}

		public String getLowPersonId() {
			return lowPersonId;
		}

		public void setLowPersonId(String lowPersonId) {
			this.lowPersonId = lowPersonId;
		}
	}

}
