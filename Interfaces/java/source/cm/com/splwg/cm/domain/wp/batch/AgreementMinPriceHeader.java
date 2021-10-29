/*******************************************************************************
* FileName                   : AgreementMinPriceHeader.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Apr 04, 2016 
* Version Number             : 0.3
* Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Apr 04, 2016        Preeti Tiwari        Implemented to assign dummy Minimum charge price assignment over Billing Merchant.
0.2      NA             May 04, 2016        Preeti Tiwari        Update to fix SQL warnings..
0.3		 NA             Mar 28, 2017        Ankur Jain           PAM-11753:Fixed performance issue
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

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
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.common.LoggedException;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import com.splwg.ccb.domain.admin.timeOfUse.TimeOfUse_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_DTO;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_Id;
import com.splwg.ccb.domain.pricing.pricecomp.PriceComp_DTO;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.ccb.domain.pricing.ratecomponent.RcMap_Id;
import com.splwg.ccb.domain.rate.rateSchedule.RateSchedule_Id;

/**
 * @author Preeti
 * @BatchJob (multiThreaded = true, rerunnable = false,modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 */

public class AgreementMinPriceHeader extends AgreementMinPriceHeader_Gen {

	static final Logger logger = LoggerFactory.getLogger(AgreementMinPriceHeader.class);

	private AgreementPriceHeaderLookUps agreementPriceHeaderLookUps = null;

	public JobWork getJobWork() {
		logger.debug("Inside get Job Work Method ");
		agreementPriceHeaderLookUps = new AgreementPriceHeaderLookUps();
		List<ThreadWorkUnit> threadWorkUnitList;
		threadWorkUnitList = getPerIdData();  
		agreementPriceHeaderLookUps = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	private List<ThreadWorkUnit> getPerIdData() {
		PreparedStatement preparedStatement = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		ThreadWorkUnit threadWorkUnit = null;
		PerIdNbr_Id peridnbrId = null;
		String cisDivision = null;
		String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
		StringBuilder stringBuilder = new StringBuilder();
		
		try {
			
				stringBuilder.append(" SELECT acctper2.per_id, hdr.cis_division AS cis_division ");
				stringBuilder.append(" FROM ci_acct_per acctper2, ci_sa sa2, ci_sa_char sachar, ci_sa sa1 , ci_acct_nbr nbr, ");
				stringBuilder.append(" ci_acct_per acctper1, ci_per_id perid, cm_price_hdr hdr ");
				stringBuilder.append(" WHERE hdr.priceitem_cd = 'MINCHRGP' ");
				stringBuilder.append(" AND hdr.bo_status_cd = :selectBoStatus ");
				if (!CommonUtils.CheckNull(txnSourceCode).equals("")) {
				stringBuilder.append(" AND hdr.TXN_SOURCE_CD = :txnSourceCode ");
				}
				stringBuilder.append(" AND hdr.ilm_dt > :sysdte ");
				stringBuilder.append(" AND hdr.per_id_nbr = perid.per_id_nbr ");
				stringBuilder.append(" AND perid.per_id = acctper1.per_id ");
				stringBuilder.append(" AND acctper1.acct_id = nbr.acct_id ");
				stringBuilder.append(" AND nbr.acct_nbr_type_cd = :acctType ");
				stringBuilder.append(" AND nbr.acct_nbr = 'CHRG' ");
				stringBuilder.append(" AND sa1.acct_id = nbr.acct_id ");
				stringBuilder.append(" AND sa1.sa_type_cd = 'CHRG' ");
				stringBuilder.append(" AND sachar.srch_char_val = sa1.sa_id ");
				stringBuilder.append(" AND sachar.char_type_cd = :c1Safcd ");
				stringBuilder.append(" AND sachar.sa_id = sa2.sa_id ");
				stringBuilder.append(" AND sa2.sa_status_flg IN ('20', '30', '40', '50') ");
				stringBuilder.append(" AND sa2.acct_id = acctper2.acct_id ");
				stringBuilder.append(" AND EXISTS (SELECT 1 FROM ci_acct_nbr WHERE acct_id = acctper1.acct_id AND acct_nbr_type_cd = :c1FAno) ");
				stringBuilder.append(" GROUP BY acctper2.per_id, hdr.cis_division ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus", agreementPriceHeaderLookUps.getCompleted(), "BO_STATUS_CD");
				preparedStatement.bindString("acctType", "ACCTTYPE", "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("c1FAno", "C1_F_ANO", "ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("c1Safcd", "C1_SAFCD", "CHAR_TYPE_CD");
				preparedStatement.bindDate("sysdte", getSystemDateTime().getDate().addDays(-1));
				if (!CommonUtils.CheckNull(txnSourceCode).equals("")) {
				preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
				}
				preparedStatement.setAutoclose(false);
							
			for (SQLResultRow resultSet : preparedStatement.list()) {
				String perIdNbr = resultSet.getString("PER_ID");	
				//Multi Division Changes
				cisDivision = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));
				peridnbrId=new PerIdNbr_Id(perIdNbr);
				threadWorkUnit = new ThreadWorkUnit();
				threadWorkUnit.setPrimaryId(peridnbrId);
				threadWorkUnit.addSupplementalData("cisDivision", cisDivision);
				threadWorkUnitList.add(threadWorkUnit);
			}
		}  catch (Exception e) {
			logger.error("Inside catch block- ", e);
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

	public Class<AgreementMinPriceHeader_Worker> getThreadWorkerClass() {
		return AgreementMinPriceHeader_Worker.class;
	}

	public static class AgreementMinPriceHeader_Worker extends
	AgreementMinPriceHeaderWorker_Gen {

		private AgreementPriceHeaderLookUps agreementPriceHeaderLookUps = null;

		private String getPartyUID = "SELECT PA.PARTY_UID FROM CI_PARTY PA WHERE PA.PARTY_ID = :per_id_nbr";
		
		//Multi Division Changes
		private String getDivPartyUID = "SELECT PA.PARTY_UID FROM CI_PARTY PA, CI_PER PER WHERE PER.PER_ID=PA.PARTY_ID "
				+ "AND PA.PARTY_ID = :per_id_nbr AND PER.CIS_DIVISION= :cisDivision";

		private String fetchPriceListId = "SELECT PRICELIST_ID FROM CI_PRICELIST WHERE PL_GLOBAL_SW=:switch";		

		private String checkPriceItem = "SELECT PRICE_ASGN_ID,RS_CD,TOU_CD,PRICE_CURRENCY_CD,PRICE_STATUS_FLAG,BUS_OBJ_CD,"
				+ " PA_TYPE_FLAG,PRINT_IF_ZERO_SW,IGNORE_SW,DO_NOT_AGG_SW, SCHEDULE_CD, START_DT, END_DT FROM CI_PRICEASGN WHERE "
				+ " OWNER_ID = :pricelist_id AND PRICEITEM_CD =:priceitem_cd AND PA_OWNER_TYPE_FLG = 'PLST'"
				+ " ORDER BY START_DT ASC";

		private String checkPriceAsgn = "SELECT PRICE_ASGN_ID,START_DT,END_DT FROM (SELECT * FROM CI_PRICEASGN WHERE OWNER_ID =:owner_id"
				+ " AND PA_OWNER_TYPE_FLG = 'PRTY' AND PRICEITEM_CD =:priceitem_cd "
				+ " ORDER BY START_DT DESC) WHERE ROWNUM < 2";		

		private String fetchRcMapId = "SELECT RC.RC_MAP_ID, RC.RC_SEQ, RCH.CHAR_VAL FROM CI_RC_MAP RC, CI_RC_CHAR RCH WHERE RC.RS_CD = RCH.RS_CD"
				+ " AND RCH.RS_CD =:rs_cd  AND RC.RC_SEQ = RCH.RC_SEQ AND RCH.CHAR_TYPE_CD ='RATE_TP'";

		private String getPriceAsgnId = "SELECT PRICE_ASGN_ID ,RS_CD FROM CI_PRICEASGN WHERE OWNER_ID = :pricelist_id "
				+ " AND PRICEITEM_CD =:priceitem_cd AND START_DT = to_date(:start_dt,'YYYY-MM-DD')"
				+ " AND END_DT = to_date(:end_dt,'YYYY-MM-DD') AND PA_OWNER_TYPE_FLG = 'PRTY'";

		private String getPriceAsgnId1 = "SELECT PRICE_ASGN_ID ,RS_CD FROM CI_PRICEASGN WHERE OWNER_ID = :pricelist_id "
				+ " AND PRICEITEM_CD =:priceitem_cd AND START_DT = to_date(:start_dt,'YYYY-MM-DD')"
				+ " AND END_DT IS NULL AND PA_OWNER_TYPE_FLG = 'PRTY'";
		
		String globalPriceListId = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		@Override
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork method");
			if (agreementPriceHeaderLookUps == null) {
				agreementPriceHeaderLookUps = new AgreementPriceHeaderLookUps();
			}
			
			PreparedStatement stmtCheckPriceList = null;
				
				try{
					stmtCheckPriceList = createPreparedStatement(fetchPriceListId,"");
					stmtCheckPriceList.bindString("switch", "Y", "PL_GLOBAL_SW");
					stmtCheckPriceList.setAutoclose(false);
					if (!stmtCheckPriceList.list().isEmpty()) {
						for (SQLResultRow sqlCheckPriceList : stmtCheckPriceList.list()) {
							globalPriceListId = sqlCheckPriceList.getString("PRICELIST_ID");
						}
					} else {
						globalPriceListId = null;
					}
					
				}
				catch (LoggedException e) {
					logger.error("Inside catch block -", e);
				}
				finally
				{

					if (stmtCheckPriceList != null) {
						stmtCheckPriceList.close();
					}
				}
			super.initializeThreadWork(arg0);
		}

		@SuppressWarnings("deprecation")
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {			
			PreparedStatement stmtGetPriceAsgn = null;
			PreparedStatement stmtCheckPricing = null;
			PreparedStatement stmtLine = null;
			PreparedStatement stmtCheckPartyId = null;
			PreparedStatement stmtCheckPriceAsgn = null;			
			PreparedStatement stmtCurrencyCheck = null;	

			PerIdNbr_Id perIdNbrId = (PerIdNbr_Id) unit.getPrimaryId();
			String cisDivision = (String)unit.getSupplementallData("cisDivision");//Multi Division Changes
			logger.debug("Processing of Person record - "+ perIdNbrId.getPerIdNbr());
			
			try {
				boolean isDeltaDataExist = false;
				String priceAsgnId = null;
				String touCd = null;
				String paTypeFlg = null;
				String ignoreSw = null;
				String priceStatusFlg = null;
				String printIfZero = null;
				String startDt = "";
				String endDt = "";
				String deltaPartyUid = null;

				String rateSchedule = "CHRGMPC".trim();
				String priceItemCd = "MINCHRGP".trim();
				String deltaPerIdNbr = perIdNbrId.getPerIdNbr().trim();
				String currency = "";					

				//***********************************Check the currency*********************************//
				
				stmtCurrencyCheck = createPreparedStatement("select A.CURRENCY_CD from CI_ACCT A, CI_ACCT_NBR B, CI_ACCT_PER C" +
						" where A.ACCT_ID=B.ACCT_ID AND B.ACCT_NBR_TYPE_CD='ACCTTYPE' AND B.ACCT_NBR='CHRG'" +
					    " AND A.ACCT_ID=C.ACCT_ID and C.PER_ID=:deltaPerIdNbr","");
				stmtCurrencyCheck.bindString("deltaPerIdNbr", deltaPerIdNbr, "PER_ID");
				stmtCurrencyCheck.setAutoclose(false);

				if (!stmtCurrencyCheck.list().isEmpty()) {
					for (SQLResultRow stmtCurrencyCheckList : stmtCurrencyCheck.list()) {
						currency = stmtCurrencyCheckList.getString("CURRENCY_CD");
						
					}
				}
				//***********************************Check Global Price list*********************************//
				
				if (CommonUtils.CheckNull(globalPriceListId).equals("")) {	
					logger.debug("No Global Price list Exists ");
					
				}
				

				//***********************************Check Pricing for the Product in Global Price List*********************************//
				stmtCheckPricing = createPreparedStatement(checkPriceItem,"");
				stmtCheckPricing.bindString("pricelist_id", globalPriceListId, "OWNER_ID");
				stmtCheckPricing.bindString("priceitem_cd", priceItemCd, "PRICEITEM_CD");
				stmtCheckPricing.setAutoclose(false);

				for (SQLResultRow sqlCheckPricing : stmtCheckPricing.list()) {

					touCd = sqlCheckPricing.getString("TOU_CD");
					paTypeFlg = sqlCheckPricing.getString("PA_TYPE_FLAG");
					ignoreSw = sqlCheckPricing.getString("IGNORE_SW");
					priceStatusFlg = sqlCheckPricing.getString("PRICE_STATUS_FLAG");
					printIfZero = sqlCheckPricing.getString("PRINT_IF_ZERO_SW");
					startDt = String.valueOf(sqlCheckPricing.getDate("START_DT"));
					endDt = String.valueOf(sqlCheckPricing.getDate("END_DT"));
				}	

				//Multi Division Changes
				//***********************************Check the Party UID*********************************//	
				if (!isBlankOrNull(cisDivision)){
					stmtCheckPartyId = createPreparedStatement(getDivPartyUID,"");
					stmtCheckPartyId.bindString("per_id_nbr", deltaPerIdNbr, "PER_ID");	
					stmtCheckPartyId.bindString("cisDivision", cisDivision, "CIS_DIVISION");
					stmtCheckPartyId.setAutoclose(false);
				}
				else{
					stmtCheckPartyId = null;
					stmtCheckPartyId = createPreparedStatement(getPartyUID,"");
					stmtCheckPartyId.bindString("per_id_nbr", deltaPerIdNbr, "PER_ID");		
					stmtCheckPartyId.setAutoclose(false);
				}
				
				for (SQLResultRow sqlCheckPartyId : stmtCheckPartyId.list()) {
					deltaPartyUid = sqlCheckPartyId.getString("PARTY_UID");
				

									
					//Multi Division Changes
				//*********************************************************Retrieve latest Price assignment details*********************
				
				stmtCheckPriceAsgn = createPreparedStatement(checkPriceAsgn,"");
				stmtCheckPriceAsgn.bindString("owner_id", deltaPartyUid,"OWNER_ID");
				stmtCheckPriceAsgn.bindString("priceitem_cd", priceItemCd,"PRICEITEM_CD");
				stmtCheckPriceAsgn.setAutoclose(false);

				//***********************************Check Price assignment with same start date and end date*********************************//
				isDeltaDataExist = checkDeltaData(deltaPartyUid,priceItemCd,startDt,endDt);

				//***********************************Create or Update Scenario*********************************//
				if (!isDeltaDataExist) {
					if (stmtCheckPriceAsgn.list().isEmpty()) {
						//***********************************Create scenario*********************************//
						//***********************************Normal Pricing*********************************//
						priceAssignment(deltaPartyUid,rateSchedule, priceItemCd,touCd, paTypeFlg, ignoreSw,priceStatusFlg,
								currency,printIfZero, "N", "DAILY",startDt, endDt);					

						//***********************************Retrieve New Regular Price Assignment ID*********************************//
						if (CommonUtils.CheckNull(endDt).equals("")) {										
							stmtGetPriceAsgn = createPreparedStatement(getPriceAsgnId1,"");
							stmtGetPriceAsgn.bindString("pricelist_id",deltaPartyUid,"OWNER_ID");
							stmtGetPriceAsgn.bindString("priceitem_cd",priceItemCd,"PRICEITEM_CD");
							stmtGetPriceAsgn.bindString("start_dt",startDt,"START_DT");
						} else {
							stmtGetPriceAsgn = createPreparedStatement(getPriceAsgnId,"");
							stmtGetPriceAsgn.bindString("pricelist_id",deltaPartyUid,"OWNER_ID");
							stmtGetPriceAsgn.bindString("priceitem_cd",priceItemCd,"PRICEITEM_CD");
							stmtGetPriceAsgn.bindString("start_dt",startDt,"START_DT");
							stmtGetPriceAsgn.bindString("end_dt",endDt,"END_DT");
						}
						stmtGetPriceAsgn.setAutoclose(false);

						for (SQLResultRow sqlGetPriceAsgn : stmtGetPriceAsgn.list()) {
							priceAsgnId = sqlGetPriceAsgn.getString("PRICE_ASGN_ID");
						}

						//***********************************Price component assignment*********************************//
						stmtLine = createPreparedStatement(fetchRcMapId,"");
						stmtLine.bindString("rs_cd", rateSchedule.trim(),"RS_CD");
						stmtLine.setAutoclose(false);

						//***********************************Assign price component*********************************//
						//***********************************Run "for" loop to process each line record*********************************//
						for (SQLResultRow sqlLine : stmtLine.list()) {										

							//***********************************Assign variables*********************************//
							BigDecimal lineValueAmount = BigDecimal.ZERO;
							String lineRateType = sqlLine.getString("CHAR_VAL").trim();
							String rcMapValue = sqlLine.getString("RC_MAP_ID");
							//***********************************Call Price Component method*********************************//
							priceComponent(priceAsgnId,lineValueAmount,rcMapValue,lineRateType);
							}//for loop	
						}
					}//if same start and end date	
				}
			}  catch (Exception e) {
				logger.error(" Inside catch block-", e);
			} finally {
				if (stmtGetPriceAsgn != null) {
					stmtGetPriceAsgn.close();
				}
				if (stmtCheckPartyId != null) {
					stmtCheckPartyId.close();
				}
				if (stmtGetPriceAsgn != null) {
					stmtGetPriceAsgn.close();
				}
				if (stmtLine != null) {
					stmtLine.close();
				}
				if (stmtCheckPriceAsgn != null) {
					stmtCheckPriceAsgn.close();
				}
				if (stmtCheckPricing != null) {
					stmtCheckPricing.close();
				}
				if (stmtCheckPartyId != null) {
					stmtCheckPartyId.close();
				}
				if (stmtCurrencyCheck != null) {
					stmtCurrencyCheck.close();
				}
			}

			return true;
		}

		/***********
		 * Price Assignment logic
		 * @return 
		 *
		 */
		private String priceAssignment(String deltaPartyUid, String rateSchedule,
				String priceItemCd, String touCd, String paTypeFlg, String ignoreSw,
				String priceStatusFlg, String currency, String printIfZero, String doNotAggSw,
				String scheduleCode, String startDt, String endDt) {

			PriceAsgn_DTO priceAsgn_DTO=null;

			try {
				//******************CI_PRICEASGN*******************//
				priceAsgn_DTO=new PriceAsgn_DTO();
				priceAsgn_DTO.setOwnerId(deltaPartyUid);
				priceAsgn_DTO.setRateScheduleId(new RateSchedule_Id(rateSchedule));
				priceAsgn_DTO.setPaOwnerTypeFlag("PRTY");
				priceAsgn_DTO.setPriceItemCodeId(new PriceItem_Id(priceItemCd));

				if (!CommonUtils.CheckNull(touCd).equals("")) {
					priceAsgn_DTO.setTimeOfUseId(new TimeOfUse_Id(touCd));
				}		
				if (!CommonUtils.CheckNull(paTypeFlg).equals("")) {
					priceAsgn_DTO.setPaTypeFlag(paTypeFlg);
				}	
				if (!CommonUtils.CheckNull(ignoreSw).equals("")) {
					priceAsgn_DTO.setIgnoreSw("N");
				}	
				if (!CommonUtils.CheckNull(priceStatusFlg).equals("")) {
					priceAsgn_DTO.setPriceStatusFlag(priceStatusFlg);
				}
				if (!CommonUtils.CheckNull(currency).equals("")) {
					priceAsgn_DTO.setPriceCurrencyCode(currency);
				}	
				if (!CommonUtils.CheckNull(printIfZero).equals("")) {
					priceAsgn_DTO.setPrintIfZeroSwitch(printIfZero);
				}
				if (!CommonUtils.CheckNull(doNotAggSw).equals("")) {
					priceAsgn_DTO.setDoNotAggSw(doNotAggSw);
				}	
				if (!CommonUtils.CheckNull(scheduleCode).equals("")) {
					priceAsgn_DTO.setScheduleCode(scheduleCode);
				}				
				priceAsgn_DTO.setTxnDailyRatingCrt("AGTR");

				if (!CommonUtils.CheckNull(startDt).equals("")) {
					String startDateStringArray[] = new String[3];
					startDateStringArray = startDt.split("-", 50);
					priceAsgn_DTO.setStartDate(new Date(Integer.parseInt(startDateStringArray[0]),
							Integer.parseInt(startDateStringArray[1]),
							Integer.parseInt(startDateStringArray[2])));
				}
				if (!CommonUtils.CheckNull(endDt).equals("")) {
					String endDateStringArray[] = new String[3];
					endDateStringArray = endDt.split("-", 50);
					priceAsgn_DTO.setEndDate(new Date(Integer.parseInt(endDateStringArray[0]),
							Integer.parseInt(endDateStringArray[1]),
							Integer.parseInt(endDateStringArray[2])));
				}

				priceAsgn_DTO.newEntity();
			}  catch (Exception e) {
				logger.error("Inside  catch block-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());

				String errorMessageNumber = errorMessage.substring(errorMessage
						.indexOf("Number:") + 8, errorMessage
						.indexOf("Call Sequence:"));

				String errorMessageCategory = errorMessage.substring(
						errorMessage.indexOf("Category:") + 10, errorMessage
						.indexOf("Number"));

				if (errorMessage.contains("Text: ")
						&& errorMessage.contains("Description: ")) {
					errorMessage = errorMessage.substring(errorMessage
							.indexOf("Text:  "), errorMessage
							.indexOf("Description:  "));
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
			return "true";
		}
		/***********
		 * Price Component logic
		 * @param priceItemCd 
		 * @param lineRateType 
		 * @return 
		 * 
		 */
		private String priceComponent(String priceAsgnId,
				BigDecimal lineValueAmount, String rcMapValue, String lineRateType) {
			PriceComp_DTO priceComp_DTO=null;
			try {				
				priceComp_DTO=new PriceComp_DTO();
				priceComp_DTO.setPriceAsgnId(new PriceAsgn_Id(priceAsgnId));
				priceComp_DTO.setValueAmt(lineValueAmount);
				priceComp_DTO.setRcMapId(new RcMap_Id(rcMapValue));
				priceComp_DTO.newEntity();
			} catch (Exception e) {
				logger.error("Inside catch block-", e);
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
			return "true";			
		}

		public void finalizeThreadWork() throws ThreadAbortedException,RunAbortedException {

			logger.debug("Inside finalizeThreadWork() method");			
			super.finalizeThreadWork();
		}

		@SuppressWarnings("deprecation")
		private boolean checkDeltaData(String deltaPartyUid, String priceItemCd, String startDt, String endDt) {

			String checkDeltaQuery = "SELECT COUNT(PRICE_ASGN_ID) AS COUNT FROM CI_PRICEASGN WHERE OWNER_ID =:deltaPartyUid  AND "
					+ "START_DT = to_date(:start_dt,'YYYY-MM-DD') AND END_DT = to_date(:end_dt,'YYYY-MM-DD') "
					+ "AND PRICEITEM_CD =:priceItem_cd AND PA_OWNER_TYPE_FLG = 'PRTY'";

			String checkDeltaQuery1 = "SELECT COUNT(PRICE_ASGN_ID) AS COUNT FROM CI_PRICEASGN WHERE OWNER_ID =:deltaPartyUid  AND "
					+ "START_DT = to_date(:start_dt,'YYYY-MM-DD') AND END_DT IS NULL "
					+ "AND PRICEITEM_CD =:priceItem_cd AND PA_OWNER_TYPE_FLG = 'PRTY'";

			boolean checkDelta = false;
			int count = 0;
			PreparedStatement pstmt = null;
			try {
				if (endDt.equalsIgnoreCase("null")) {

					pstmt = createPreparedStatement(checkDeltaQuery1,"");
					pstmt.bindString("deltaPartyUid", deltaPartyUid,"OWNER_ID");
					pstmt.bindString("priceItem_cd", priceItemCd,"PRICEITEM_CD");
					pstmt.bindString("start_dt", startDt, "START_DT");
				} else {

					pstmt = createPreparedStatement(checkDeltaQuery,"");
					pstmt.bindString("deltaPartyUid", deltaPartyUid,"OWNER_ID");
					pstmt.bindString("priceItem_cd", priceItemCd,"PRICEITEM_CD");
					pstmt.bindString("start_dt", startDt, "START_DT");
					pstmt.bindString("end_dt", endDt, "END_DT");
				}
				pstmt.setAutoclose(false);
				for (SQLResultRow sqlRow : pstmt.list()) {
					count = sqlRow.getInteger("COUNT").intValue();
				}
				if (count > 0) {
					checkDelta = true;
				}
			}  catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (pstmt != null) {
					pstmt.close();
				}
			}
			return checkDelta;
		}	
	}

	public static final class PerIdNbr_Id implements Id {

		private static final long serialVersionUID = 1L;
		private String perIdNbr;

		public PerIdNbr_Id(String perIdNbr) {
			setPerIdNbr(perIdNbr);
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}		

		public boolean isNull() {
			return false;
		}
		public void appendContents(StringBuilder arg0) {
			// appendContents

		}

	}
}
