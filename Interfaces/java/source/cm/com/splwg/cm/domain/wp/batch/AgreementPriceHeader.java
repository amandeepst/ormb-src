/*******************************************************************************
 * FileName                   : AgreementPriceHeader.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015 
 * Version Number             : 1.9
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name        | Nature of Change
0.1      NA             Mar 24, 2015        Gaurav Sood          Implemented all requirements for CD1.
0.2      NA             Apr 22, 2015        Preeti Tiwari        Fix for Update scenarios and Currency check.
0.3      NA             Apr 28, 2015        Preeti Tiwari        Implemented Tiered Pricing and all the requirements for CD2.
0.4      NA				Jun 29, 2015		Gaurav Sood			 Defect Fix PAM-2105.
0.5      NA             Jul 31, 2015        Abhishek Paliwal     BO replaced by DTO.
0.6      NA             Sep 15, 2015        Preeti Tiwari        CT Defect fix ORMBDEV-131.
0.7      NA             Sep 16, 2015        Preeti Tiwari        Implemented changes to the tiering logic as per the CD2 requirements.
0.8      NA             Apr 07, 2016        Sunaina Raina        Updated as per Oracle Code review.
0.9      NA             Apr 22, 2016        Preeti Tiwari        Updated to set Pending status in execute work unit.
1.0      NA             May 04, 2016        Preeti Tiwari        Removed extra loggers and fixed sql warnings.
1.1      NA             Jan 01, 2017        Preeti Tiwari        PAM-10360 (Update rates when end date changed).
1.2      NA             Jan 23, 2017        Ankur Jain           Changes for performance improvement
1.3      NA             May 17, 2017        Ankur Jain           NAP-16306 fixed
1.4      NA             Dec 19, 2017        Ankur Jain           NAP-21127 fix
1.5      NA             Jan 31, 2018        Ankur Jain           NAP-15587 merchant pricing view
1.6		 NAP-24089      Mar 23, 2018		Rakesh Ranjan		 NAP-24086 Included ILM_ARCH_SW to be updated to Y for completed status.
1.7      NA             May 27, 2018        Ankur Jain           NAP-27745 ,NAP-27758 & NAP-27719 Fix
1.8		 NA				Jul 12, 2018	 	RIA					 NAP-30468 Delete from C1_PRICECOMP_L
1.9		 NA				Jul 31, 2018	 	RIA					 NAP-30332 Rate schedule feature config check
2.0      NA             Aug 14, 2018        RIA                  NAP-31445 Performance improvement for APRI
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.EntityId;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationInfo;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationOptionInfo;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfigurationOptionsCache;
import com.splwg.base.domain.common.featureConfiguration.FeatureConfiguration_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.domain.admin.timeOfUse.TimeOfUse_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_DTO;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentChar_DTO;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentChar_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentParm;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentParm_DTO;
import com.splwg.ccb.domain.pricing.priceassign.PriceAssignmentParm_Id;
import com.splwg.ccb.domain.pricing.pricecomp.PriceCompTier_DTO;
import com.splwg.ccb.domain.pricing.pricecomp.PriceCompTier_Id;
import com.splwg.ccb.domain.pricing.pricecomp.PriceComp_DTO;
import com.splwg.ccb.domain.pricing.pricecomp.PriceComp_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.ccb.domain.pricing.priceparm.PriceParm_Id;
import com.splwg.ccb.domain.pricing.pricingcriteria.PriceCriteria_Id;
import com.splwg.ccb.domain.pricing.ratecomponent.RcMap_Id;
import com.splwg.ccb.domain.rate.rateSchedule.RateSchedule_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.common.LoggedException;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 * @BatchJob (rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = chunkSize, required = true, type = integer)
 *            , @BatchJobSoftParameter (name = txnSourceCode, type = string)})
 */

public class AgreementPriceHeader extends AgreementPriceHeader_Gen {

	static final Logger logger = LoggerFactory.getLogger(AgreementPriceHeader.class);

	private AgreementPriceHeaderLookUps agreementPriceHeaderLookUps = null;

	public JobWork getJobWork() {
		logger.debug("Inside get Job Work Method ");
		agreementPriceHeaderLookUps = new AgreementPriceHeaderLookUps();
		List<ThreadWorkUnit> threadWorkUnitList = getPerIdData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "+ threadWorkUnitList.size());
		agreementPriceHeaderLookUps = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	private List<ThreadWorkUnit> getPerIdData() {
		PreparedStatement preparedStatement = null;
		PerIdNbr_Id peridnbrId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
		StringBuilder stringBuilder = new StringBuilder();
		String perIdNbr = null;
		String priceItem = null;
		int chunkSize = getParameters().getChunkSize().intValue();
		String lowHdrId = null;
		String highHdrId = null;
		AgreementPrice_Id aggPriceId = null;
		
		try {
			stringBuilder.append(" WITH TBL AS (SELECT PRICE_HDR_ID FROM CM_PRICE_HDR " );
			stringBuilder.append(" WHERE BO_STATUS_CD = :selectBoStatus  " );

			if (notBlank(txnSourceCode)) {
				stringBuilder.append(" and TXN_SOURCE_CD=:txnSourceCode");
				stringBuilder.append(" ORDER BY PRICE_HDR_ID) SELECT THREAD_NUM, MIN(PRICE_HDR_ID) AS LOW_HDR_ID, ");
				stringBuilder.append(" MAX(PRICE_HDR_ID) AS HIGH_HDR_ID FROM (SELECT PRICE_HDR_ID, ");
				stringBuilder.append(" CEIL((ROWNUM)/:chunkSize) AS THREAD_NUM FROM TBL) GROUP BY THREAD_NUM ORDER BY 1 ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus", agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
				preparedStatement.bindBigInteger("chunkSize", new BigInteger(String.valueOf(chunkSize)));
			}else{
				stringBuilder.append(" ORDER BY PRICE_HDR_ID) SELECT THREAD_NUM, MIN(PRICE_HDR_ID) AS LOW_HDR_ID, ");
				stringBuilder.append(" MAX(PRICE_HDR_ID) AS HIGH_HDR_ID FROM (SELECT PRICE_HDR_ID, ");
				stringBuilder.append(" CEIL((ROWNUM)/:chunkSize) AS THREAD_NUM FROM TBL) GROUP BY THREAD_NUM ORDER BY 1 ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus", agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindBigInteger("chunkSize", new BigInteger(String.valueOf(chunkSize)));
			}
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				lowHdrId = resultSet.getString("LOW_HDR_ID");
				highHdrId = resultSet.getString("HIGH_HDR_ID");

				aggPriceId=new AgreementPrice_Id(lowHdrId,highHdrId);
				
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(aggPriceId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				aggPriceId = null;
			}
		}  
		
		catch (Exception e) {
			logger.error("Inside catch block-", e);
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

	public Class<AgreementPriceHeader_Worker> getThreadWorkerClass() {
		return AgreementPriceHeader_Worker.class;
	}

	public static class AgreementPriceHeader_Worker extends
	AgreementPriceHeaderWorker_Gen {

		private AgreementPriceHeaderLookUps agreementPriceHeaderLookUps = null;

		private ArrayList<ArrayList<String>> updateAgreementStatusList = new ArrayList<ArrayList<String>>();

		private ArrayList<String> eachAgreementStatusList = null;
		
		String globalPriceListId = null;
		String priceAsgnId = null;

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}


		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork method");
	
			PreparedStatement stmtCheckPriceList = null;
			if (agreementPriceHeaderLookUps == null) {
				agreementPriceHeaderLookUps = new AgreementPriceHeaderLookUps();
			}
			StringBuilder fetchPriceListId = new StringBuilder();
			fetchPriceListId.append("SELECT PRICELIST_ID FROM CI_PRICELIST WHERE PL_GLOBAL_SW=:switch");
			try{
				
				stmtCheckPriceList = createPreparedStatement(fetchPriceListId.toString(),"");
				stmtCheckPriceList.bindString("switch", "Y", "PL_GLOBAL_SW");
				stmtCheckPriceList.setAutoclose(false);

				if (notNull(stmtCheckPriceList.firstRow())) {
					
					globalPriceListId = stmtCheckPriceList.firstRow().getString("PRICELIST_ID");
				} 
				
			}
			catch (LoggedException e) {
				logger.error("Inside catch block-", e);
			}
			finally
			{

				if (stmtCheckPriceList != null) {
					stmtCheckPriceList.close();
				}
			}
			
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {	
			
		
			
			StringBuilder getPartyUID = new StringBuilder();
			getPartyUID.append("SELECT PA.PARTY_UID FROM CI_PARTY PA, CI_PER_ID PE WHERE PA.PARTY_ID = PE.PER_ID");
			getPartyUID.append(" AND PE.PER_ID_NBR =:per_id_nbr AND PE.ID_TYPE_CD = :exprtyId AND PA.PARTY_TYPE_FLG= :pers "); //TODO set division parameter
			
			
			
			//Multi Division Changes
			StringBuilder getDivPartyUID = new StringBuilder();
			getDivPartyUID.append("SELECT PA.PARTY_UID FROM CI_PARTY PA, CI_PER_ID PE , CI_PER PER WHERE PA.PARTY_ID = PE.PER_ID ");
			getDivPartyUID.append("AND PER.PER_ID = PE.PER_ID AND PE.PER_ID_NBR =:per_id_nbr ");
			getDivPartyUID.append("AND PE.ID_TYPE_CD = :exprtyId AND PA.PARTY_TYPE_FLG= :pers AND PER.CIS_DIVISION = :division ");
			
			
			StringBuilder checkPriceItem = new StringBuilder();
			checkPriceItem.append("SELECT TOU_CD,PRICE_STATUS_FLAG,");
			checkPriceItem.append(" PA_TYPE_FLAG,PRINT_IF_ZERO_SW,IGNORE_SW FROM CI_PRICEASGN WHERE ");
			checkPriceItem.append(" OWNER_ID = :pricelist_id AND PRICEITEM_CD =:priceitem_cd AND PA_OWNER_TYPE_FLG = :plst");
			checkPriceItem.append(" ORDER BY START_DT ASC");
			
			StringBuilder checkTierLines = new StringBuilder();
			checkTierLines.append("SELECT 1 FROM CM_PRICE_TIER" );
			checkTierLines.append(" WHERE PRICE_HDR_ID =:txn_header_id ");
			
			StringBuilder getDeltaLine = new StringBuilder();
			getDeltaLine.append("SELECT TXN_TIER_ID, RATE_TYPE, VALUE_AMT FROM CM_PRICE_LN WHERE PRICE_HDR_ID =:price_hdr_id");
			
			StringBuilder checkDupRate = new StringBuilder();
			checkDupRate.append("SELECT count(*) AS COUNT FROM CM_PRICE_LN WHERE (PRICE_HDR_ID, nvl(TXN_TIER_ID,0), RATE_TYPE) ");
			checkDupRate.append( "IN (SELECT PRICE_HDR_ID,nvl(TXN_TIER_ID,0), RATE_TYPE FROM CM_PRICE_LN ");
			checkDupRate.append( "GROUP BY PRICE_HDR_ID, TXN_TIER_ID, RATE_TYPE ");
			checkDupRate.append( "HAVING COUNT(RATE_TYPE) > 1) AND PRICE_HDR_ID =:price_hdr_id ");
			
			StringBuilder fetchRcMapId = new StringBuilder();
			fetchRcMapId.append("SELECT RC.RC_MAP_ID FROM CI_RC_MAP RC, CI_RC_CHAR RCH WHERE RC.RS_CD = RCH.RS_CD");
			fetchRcMapId.append( " AND RCH.RS_CD =:rs_cd  AND RC.RC_SEQ = RCH.RC_SEQ AND RCH.CHAR_TYPE_CD = :rateTp");
			fetchRcMapId.append( " AND RCH.CHAR_VAL =:char_val");
			
			StringBuilder getTierLines = new StringBuilder();
			getTierLines.append("SELECT PRICE_HDR_ID, TXN_TIER_ID, PRICECRITERIA_CD, LOWER_LIMIT, UPPER_LIMIT FROM CM_PRICE_TIER" );
			getTierLines.append(" WHERE PRICE_HDR_ID =:txn_header_id AND TXN_TIER_ID =:txn_tier_id");
			
			StringBuilder checkCreateOrUpd = new StringBuilder();
			checkCreateOrUpd.append("SELECT PC.PRICE_ASGN_ID FROM CI_PRICEASGN_CHAR PC,CI_PRICEASGN PS WHERE PC.SRCH_CHAR_VAL=:srchCharVal AND PC.PRICE_ASGN_ID=PS.PRICE_ASGN_ID");
			checkCreateOrUpd.append(" AND PS.OWNER_ID=:deltaPartyUid ORDER BY PC.EFFDT DESC");  //TODO  CI_PARTY AND CI_PER fetch
			
			//Multi Division Changes
			StringBuilder checkCreateOrUpdDiv = new StringBuilder();
			checkCreateOrUpdDiv.append("SELECT PC.PRICE_ASGN_ID FROM CI_PRICEASGN_CHAR PC,CI_PRICEASGN PS, CI_PARTY PR, CI_PER PER ");
			checkCreateOrUpdDiv.append(" WHERE PC.SRCH_CHAR_VAL=:srchCharVal AND PC.PRICE_ASGN_ID=PS.PRICE_ASGN_ID AND PS.OWNER_ID = PR.PARTY_UID AND PR.PARTY_ID = PER.PER_ID ");
			checkCreateOrUpdDiv.append(" AND PER.CIS_DIVISION = :division AND PS.OWNER_ID=:deltaPartyUid ORDER BY PC.EFFDT DESC ");
			
			StringBuilder updPriceAsgn = new StringBuilder();
			updPriceAsgn.append("UPDATE CI_PRICEASGN SET END_DT = to_date(trim(:end_dt),'YYYY-MM-DD'),PRICE_STATUS_FLAG=:priceStatusFlg WHERE PRICE_ASGN_ID =:price_asgn_id");
			
			
			String tou_cd = null;
			String paTypeFlg = null;
			String ignoreSw = null;
			String priceStatusFlg = null;
			String printIfZero = null;
			String startDt = "";
			String endDt = "";
			String deltaPartyUid = null;
		
			String txn_header = "";
			String rateSchedule = "";
			String priceItemCd = "";
			String deltaPerIdNbr = "";
			String currency = "";
			BigDecimal lineValueAmount = null;
			String lineRateType = "";
			String transactionTierId = "";
			String rcMapValue="";
			String priceCriteriaCd = "";
			BigDecimal lowerLimit = BigDecimal.ZERO;
			BigDecimal upperLimit = BigDecimal.ZERO;
			
			String rowId = "";
			String canFlag = "";
			String division = "";  //Multi Division Changes
			String prodParm1 = "";
			String prodParm2 = "";
			String prodParm3 = "";
			String prodParm4 = "";
			String prodParm5 = "";
			String prodParm6 = "";
			String prodParm7 = "";
			String prodParm8 = "";

			
			Boolean tierFlag=false; 
			Date effectiveDt = null;
	
			PreparedStatement stmtCheckPricing = null;
			PreparedStatement stmtLine = null;
			PreparedStatement stmtCheckPartyId = null;
			PreparedStatement stmtCheckPriceAsgn = null;
			PreparedStatement stmtUpdatePriceAsgn = null;
			PreparedStatement stmtCheckDupRate = null;
			PreparedStatement stmtGetTierLines = null;
			PreparedStatement stmtCheckTierLines = null;
			PreparedStatement stmtFetchRcId = null;
			PreparedStatement stmtCheckCreateOrUpd = null;
			String returnStatus="";
		
			String messageCategoryNumber = agreementPriceHeaderLookUps.getMessageCatNum();
			String messageNumber = agreementPriceHeaderLookUps.getMessageNum();
		
			AgreementPrice_Id aggPriceId = (AgreementPrice_Id) unit.getPrimaryId();
			List<InboundAgreementFeedInterface_Id> dataList = getDeltaStagingData(aggPriceId);
		    InboundAgreementFeedInterface_Id inboundAgreementInterfaceId=null;
		
			for (int countAgrmnt = 0; countAgrmnt < dataList.size(); countAgrmnt++) {
				
				removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
				setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));//Required to nullify the effect of database transactions in case of error scenario
		
				inboundAgreementInterfaceId = dataList.get(countAgrmnt);
				logger.debug("Processing of Transaction Header Id - " + inboundAgreementInterfaceId.getTransactionId());
				try {
					setPendingStatus(inboundAgreementInterfaceId);
					
					txn_header = inboundAgreementInterfaceId.getTransactionId();
					effectiveDt = inboundAgreementInterfaceId.getStartDate();
					startDt = CommonUtils.CheckNull(String.valueOf(effectiveDt).trim());
					endDt = inboundAgreementInterfaceId.getEndDate().trim();
					rateSchedule = inboundAgreementInterfaceId.getRateSchedule().trim();
					priceItemCd = inboundAgreementInterfaceId.getPriceitem_cd().trim();
					deltaPerIdNbr = inboundAgreementInterfaceId.getPerIdNbr().trim();
					currency = inboundAgreementInterfaceId.getCurrency_cd().trim();	
					rowId = inboundAgreementInterfaceId.getRowId();
					canFlag = inboundAgreementInterfaceId.getCanFlag();
					division = inboundAgreementInterfaceId.getDivision(); //Multi Division Changes
					prodParm1=inboundAgreementInterfaceId.getProdParm1();
					prodParm2=inboundAgreementInterfaceId.getProdParm2();
					prodParm3=inboundAgreementInterfaceId.getProdParm3();
					prodParm4=inboundAgreementInterfaceId.getProdParm4();
					prodParm5=inboundAgreementInterfaceId.getProdParm5();
					prodParm6=inboundAgreementInterfaceId.getProdParm6();
					prodParm7=inboundAgreementInterfaceId.getProdParm7();
					prodParm8=inboundAgreementInterfaceId.getProdParm8();


					//***********************************Check the currency*********************************//
		
					if(isNull(new Currency_Id(currency).getEntity())) {
						return logError(txn_header, deltaPerIdNbr, String.valueOf(CustomMessages.MESSAGE_CATEGORY), String.valueOf(CustomMessages.AGREEMENT_7NO_CURRENCY_MATCH),"");
					}
		
					//***********************************Check the Party UID*********************************//
					//stmtCheckPartyId = createPreparedStatement(getPartyUID.toString(),""); //TODO change for if else
					 
					//Multi Division Changes
					if(!isBlankOrNull(division)){
						stmtCheckPartyId = createPreparedStatement(getDivPartyUID.toString(),"");
						stmtCheckPartyId.bindString("per_id_nbr", deltaPerIdNbr, "PER_ID_NBR");
						stmtCheckPartyId.bindString("exprtyId", agreementPriceHeaderLookUps.getExprtyId(), "ID_TYPE_CD");		
						stmtCheckPartyId.bindString("pers", agreementPriceHeaderLookUps.getPers(), "PARTY_TYPE_FLG");
						stmtCheckPartyId.bindString("division", division, "CIS_DIVISION");
						stmtCheckPartyId.setAutoclose(false);
					}
					else{
						stmtCheckPartyId = null;
						stmtCheckPartyId = createPreparedStatement(getPartyUID.toString(),"");
						stmtCheckPartyId.bindString("per_id_nbr", deltaPerIdNbr, "PER_ID_NBR");
						stmtCheckPartyId.bindString("exprtyId", agreementPriceHeaderLookUps.getExprtyId(), "ID_TYPE_CD");		
						stmtCheckPartyId.bindString("pers", agreementPriceHeaderLookUps.getPers(), "PARTY_TYPE_FLG");	
						stmtCheckPartyId.setAutoclose(false);
					}
					
					
					if (stmtCheckPartyId.list().size()>0) {
					
						//***********************************Run "for" loop to assign Pricing to all the merchants with same Per ID Number*********************************//
						int partyUidRunningNum = 1;
						for (SQLResultRow sqlCheckPartyId : stmtCheckPartyId.list()) {
							
							deltaPartyUid = sqlCheckPartyId.getString("PARTY_UID");
							
							if (priceItemCd.equalsIgnoreCase("RECRTAX")){							
							//***********************Set Price Assignment for Tax product****************************//
							priceAssignmentForTax(deltaPartyUid,startDt);	
							
							}else{		
							//***********************************Check Global Price list*********************************//
		
							if(isNull(globalPriceListId))
							{
								return logError(txn_header, deltaPerIdNbr, String.valueOf(CustomMessages.MESSAGE_CATEGORY), String.valueOf(CustomMessages.AGREEMENT_6NO_PRICE_ASSIGNMENT),"");
							}
		
							//***********************************Check Price Item*********************************//
							if(isNull(new PriceItem_Id(priceItemCd).getEntity())) {
								return logError(txn_header, deltaPerIdNbr, String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_6NO_PRICE_ASSIGNMENT),"");
							}						
		
							//***********************************Check Pricing for the Product in Global Price List*********************************//
							stmtCheckPricing = createPreparedStatement(checkPriceItem.toString(),"");
							stmtCheckPricing.bindString("pricelist_id", globalPriceListId, "OWNER_ID");
							stmtCheckPricing.bindString("priceitem_cd", priceItemCd, "PRICEITEM_CD");
							stmtCheckPricing.bindString("plst", agreementPriceHeaderLookUps.getPlst(), "PA_OWNER_TYPE_FLG");
							stmtCheckPricing.setAutoclose(false);
		
							if (isNull(stmtCheckPricing.firstRow())) {
								return logError(inboundAgreementInterfaceId.getTransactionId(),deltaPerIdNbr,
										String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_9PRICEITEM_NOT_PRICELIST),"");
							}						
							
							SQLResultRow sqlCheckPricing=stmtCheckPricing.firstRow();
							tou_cd = sqlCheckPricing.getString("TOU_CD");
							paTypeFlg = sqlCheckPricing.getString("PA_TYPE_FLAG");
							ignoreSw = sqlCheckPricing.getString("IGNORE_SW");
							if("Y".equalsIgnoreCase(canFlag))
							{
								priceStatusFlg = "INAC";
							}
							else
							{
								priceStatusFlg = sqlCheckPricing.getString("PRICE_STATUS_FLAG");
							}
							printIfZero = sqlCheckPricing.getString("PRINT_IF_ZERO_SW");
		
							//*********************************************************Compare start date with end date*********************							
							if (notBlank(endDt)) {
								if (startDt.compareTo(endDt) > 0) {
									return logError(txn_header,deltaPerIdNbr,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_5STRT_DT_GRTR_END),"");
								}
							}
		
							
							//***********************************Check Tiering pricing flag*********************************//
							stmtCheckTierLines = createPreparedStatement(checkTierLines.toString(),"");
							stmtCheckTierLines.bindString("txn_header_id", txn_header, "PRICE_HDR_ID");
							stmtCheckTierLines.setAutoclose(false);

							if (notNull(stmtCheckTierLines.firstRow())) {
								tierFlag=true;
							} 
							
							//***********************************Create or Update Scenario*********************************//
							//stmtCheckCreateOrUpd = createPreparedStatement(checkCreateOrUpd.toString(),"");  //TODO if else statement for CIS_DIVISION check //SAGA
							
							//Multi Division Changes
							if (!isBlankOrNull(division)){
								stmtCheckCreateOrUpd = createPreparedStatement(checkCreateOrUpdDiv.toString(),"");
								stmtCheckCreateOrUpd.bindString("srchCharVal", rowId, "SRCH_CHAR_VAL");
								stmtCheckCreateOrUpd.bindString("deltaPartyUid",deltaPartyUid,"OWNER_ID");
								stmtCheckCreateOrUpd.bindString("division",division,"CIS_DIVISION");
								stmtCheckCreateOrUpd.setAutoclose(false);
							}
							else{
								stmtCheckCreateOrUpd = null;
								stmtCheckCreateOrUpd = createPreparedStatement(checkCreateOrUpd.toString(),"");
								stmtCheckCreateOrUpd.bindString("srchCharVal", rowId, "SRCH_CHAR_VAL");
								stmtCheckCreateOrUpd.bindString("deltaPartyUid",deltaPartyUid,"OWNER_ID");
								stmtCheckCreateOrUpd.setAutoclose(false);
							}
							
							
//							stmtCheckCreateOrUpd.bindString("srchCharVal", rowId, "SRCH_CHAR_VAL");
//							stmtCheckCreateOrUpd.bindString("deltaPartyUid",deltaPartyUid,"OWNER_ID");
//							stmtCheckCreateOrUpd.setAutoclose(false);						

							if(notNull(stmtCheckCreateOrUpd.firstRow())) {
								//update existing pricing
								priceAsgnId = stmtCheckCreateOrUpd.firstRow().getString("PRICE_ASGN_ID");
								stmtUpdatePriceAsgn = createPreparedStatement(updPriceAsgn.toString(),"");
								stmtUpdatePriceAsgn.bindString("end_dt",endDt, "END_DT");
								stmtUpdatePriceAsgn.bindString("price_asgn_id",priceAsgnId,"PRICE_ASGN_ID");
								stmtUpdatePriceAsgn.bindString("priceStatusFlg",priceStatusFlg,"PRICE_STATUS_FLAG");//priceStatusFlg
								stmtUpdatePriceAsgn.executeUpdate();	
								
							} else {
								//create new pricing
								returnStatus = priceAssignment(deltaPartyUid,rateSchedule, priceItemCd,tou_cd, paTypeFlg, ignoreSw,priceStatusFlg,
										currency,printIfZero, "N", "DAILY",startDt, endDt, txn_header, tierFlag);
								
								//Create New Pricing Parameter for having Parameter level pricing-
								
								Map<String,String> map=new HashMap<String,String>();
								map.put("TTYPE", prodParm1);
								map.put("CTYPE", prodParm2);
								map.put("PTYPE", prodParm3);
								map.put("ATYPE", prodParm4);
								map.put("JTYPE", prodParm5);
								map.put("STYPE", prodParm6);
								map.put("AUTYPE", prodParm7);
								map.put("ARTYPE", prodParm8);
								
								BigInteger priorityNum=BigInteger.ONE;
								for (Map.Entry<String, String> entry : map.entrySet()){									 
									returnStatus=priceAssignParam(priceAsgnId,entry.getKey(),entry.getValue(),priorityNum);
									priorityNum=priorityNum.add(BigInteger.ONE);
								}
								
								checkingAndCallingLogError(txn_header,deltaPerIdNbr, returnStatus);
								returnStatus = priceAssignmentChar(effectiveDt, rowId);
								checkingAndCallingLogError(txn_header,deltaPerIdNbr, returnStatus);
							}
							
							// RIA: Skip price component create logic for rate schedules in feature config
							FeatureConfiguration_Id featureConfigId = new FeatureConfiguration_Id(agreementPriceHeaderLookUps.getFeatureConfig());
							FeatureConfigurationInfo featureConfigInfo = FeatureConfigurationOptionsCache.getFeatureConfiguration(featureConfigId);
							List<FeatureConfigurationOptionInfo> rateSchList = featureConfigInfo.getOptionsFor(agreementPriceHeaderLookUps.getFeatureConfigLookup());
							boolean isRSPresentInFeatureConfig = false;
							for(FeatureConfigurationOptionInfo rateSch: rateSchList) {
								if(rateSch.getValue().equalsIgnoreCase(rateSchedule)) {
									isRSPresentInFeatureConfig = true;
								}
							}
							if(!isRSPresentInFeatureConfig) {
							//***********************************Delete Previous Price component records for same start and end date*********************************//							
								deletePreviousPricecomp(priceAsgnId);
							
								//***********************************Price component assignment*********************************//
								stmtLine = createPreparedStatement(getDeltaLine.toString(),"");
								stmtLine.bindString("price_hdr_id", txn_header,"PRICE_HDR_ID");
								stmtLine.setAutoclose(false);
		
								if (!(stmtLine.list().size() > 0)) {
									//***********************************Line data missing*********************************//
									return logError(txn_header,deltaPerIdNbr,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_19DataMissingFromLineTable),"");									
								}else{
									//***********************************Assign price component*********************************//
		
									//***********************************Update Line Records in Pending status*********************************//
									if (partyUidRunningNum == 1) {
										updateDeltaLine(txn_header);
									}								
		
									//***********************************Check duplicate rates while considering tiered pricing*********************************//		
									stmtCheckDupRate = createPreparedStatement(checkDupRate.toString(),"");
									stmtCheckDupRate.bindString("price_hdr_id",txn_header, "PRICE_HDR_ID");
									stmtCheckDupRate.setAutoclose(false);
		
									SQLResultRow stmtCheckDupRateResultRow = stmtCheckDupRate.firstRow();
									if(notNull(stmtCheckDupRateResultRow) && stmtCheckDupRateResultRow.getInteger("COUNT").intValue() > 1) {
										return logError(txn_header,deltaPerIdNbr,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_18MULTIPLE_TF_RATE),"");
									}
		
		
									//***********************************Run "for" loop to process each line record*********************************//
									for (SQLResultRow sqlLine : stmtLine.list()) {										
		
										//***********************************Assign variables*********************************//
										lineValueAmount = sqlLine.getBigDecimal("VALUE_AMT");
										lineRateType = sqlLine.getString("RATE_TYPE").trim();
										transactionTierId = CommonUtils.CheckNull((sqlLine.getString("TXN_TIER_ID")));										
		
										rcMapValue="";
		
										//***********************************Retrieve RC Map ID for the given rate type and regular rate schedule*********************************//
										stmtFetchRcId = createPreparedStatement(fetchRcMapId.toString(),"");	
										stmtFetchRcId.bindString("char_val", lineRateType.trim(), "CHAR_VAL");
										stmtFetchRcId.bindString("rs_cd", rateSchedule.trim(),"RS_CD");
										stmtFetchRcId.bindString("rateTp", agreementPriceHeaderLookUps.getRateType(),"CHAR_TYPE_CD");
										stmtFetchRcId.setAutoclose(false);
		
										if (notNull(stmtFetchRcId.firstRow())) {											
											rcMapValue = stmtFetchRcId.firstRow().getString("RC_MAP_ID");												
										}else{
											return logError(txn_header,deltaPerIdNbr,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_8INVALID_RATE_TYPE),"");
										}
										if (stmtFetchRcId != null) {
											stmtFetchRcId.close();
										}										
		
										//***********************************Retrieve Tiered staging data*********************************//
										priceCriteriaCd = "";
										lowerLimit = BigDecimal.ZERO;
										upperLimit = BigDecimal.ZERO;
		
										if (notBlank(transactionTierId)) {
											stmtGetTierLines = createPreparedStatement(getTierLines.toString(),"");
											stmtGetTierLines.bindString("txn_header_id",txn_header, "PRICE_HDR_ID");
											stmtGetTierLines.bindString("txn_tier_id",transactionTierId, "TXN_TIER_ID");
											stmtGetTierLines.setAutoclose(false);
											if (isNull(stmtGetTierLines.firstRow())) {
												return logError(txn_header,deltaPerIdNbr,String.valueOf(CustomMessages.MESSAGE_CATEGORY),String.valueOf(CustomMessages.AGREEMENT_20DataMissingFromTierTable),"");																													
											}else{
												priceCriteriaCd = CommonUtils.CheckNull(stmtGetTierLines.firstRow().getString("PRICECRITERIA_CD"));
												lowerLimit =stmtGetTierLines.firstRow().getBigDecimal("LOWER_LIMIT");
												upperLimit = stmtGetTierLines.firstRow().getBigDecimal("UPPER_LIMIT");
											}													
										}	
										if (stmtGetTierLines != null) {
											stmtGetTierLines.close();
										}
		
										//***********************************Call Price Component method*********************************//
										returnStatus = priceComponent(priceAsgnId,lineValueAmount,rcMapValue,priceCriteriaCd,lowerLimit,
												upperLimit,txn_header,transactionTierId,priceItemCd,lineRateType);	
										checkingAndCallingLogError(txn_header,deltaPerIdNbr, returnStatus); 
									}//for loop	
								}//else-line data
							}
						//	}//if same start date
							//}//if same start and end date
							partyUidRunningNum++;
							}//else tax product
						}//per id for loop	
					}else {
						return logError(txn_header, deltaPerIdNbr, String.valueOf(CustomMessages.MESSAGE_CATEGORY), String.valueOf(CustomMessages.AGREEMENT_1NO_PARTY),"");
					}
		
					//***********************************Mark staging table record into completed status*********************************//
					//***********************************Also in case of Price assignment existing with same start date and end date*********************************//
					updateDeltaStaging(inboundAgreementInterfaceId.getTransactionId(),agreementPriceHeaderLookUps.getCompleted(),
							messageCategoryNumber,messageNumber, " ");
		
					updateDeltaLine(txn_header,agreementPriceHeaderLookUps.getCompleted(),messageCategoryNumber,messageNumber," ");					
		
				} 
				catch (Exception e) {
					logger.error("Inside catch block-", e);
				} finally {
					if (stmtCheckPartyId != null) {
						stmtCheckPartyId.close();
					}
					if (stmtGetTierLines != null) {
						stmtGetTierLines.close();
					}
					if (stmtFetchRcId != null) {
						stmtFetchRcId.close();
					}
					if (stmtCheckDupRate != null) {
						stmtCheckDupRate.close();
					}
					if (stmtLine != null) {
						stmtLine.close();
					}
					if (stmtCheckTierLines != null) {
						stmtCheckTierLines.close();
					}
					if (stmtUpdatePriceAsgn != null) {
						stmtUpdatePriceAsgn.close();
					}
					if (stmtCheckPriceAsgn != null) {
						stmtCheckPriceAsgn.close();
					}
					if (stmtCheckPricing != null) {
						stmtCheckPricing.close();
					}
					if (stmtCheckCreateOrUpd != null) {
						stmtCheckCreateOrUpd.close();
					}
					
				}
			}//for
			return true;
		}


		private String priceAssignParam(String priceAsgnId, String key,
				String value, BigInteger priorityNum) {
			// TODO Auto-generated method stub
			
			PriceParm_Id priceParmId=new PriceParm_Id(key);
			
			if(notBlank(value)){
				PriceAssignmentParm_DTO priceParamDTO=null;
				EntityId<PriceAssignmentParm> priceAssignId=new PriceAssignmentParm_Id(priceParmId.getEntity(),new PriceAsgn_Id(priceAsgnId).getEntity()); 
				try{
					priceParamDTO=new PriceAssignmentParm_DTO();
					priceParamDTO.setId(priceAssignId);
					priceParamDTO.setOverridePriorityNum(priorityNum);
					priceParamDTO.setPriceParmVal(value);
					priceParamDTO.setVersion(1);
					priceParamDTO.newEntity();
				}
				catch(Exception e){
				logger.error("Inside Catch Block:"+e);				
				}				
			}
						
			return "true";
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
		
		/***********
		 * Tax Price Assignment logic
		 * @param startDt 
		 * @return 
		 *
		 */
		private void priceAssignmentForTax(String deltaPartyUid, String startDt) {
			//*******************Determine owner Id******************//			
			
			PriceAsgn_DTO priceAsgn_DTO=null;
			try {
				//******************CI_PRICEASGN*******************//
				priceAsgn_DTO=new PriceAsgn_DTO();
				priceAsgn_DTO.setOwnerId(deltaPartyUid);
				priceAsgn_DTO.setRateScheduleId(new RateSchedule_Id("TAX"));
				priceAsgn_DTO.setPaOwnerTypeFlag("PRTY");
				priceAsgn_DTO.setPriceItemCodeId(new PriceItem_Id("RECRTAX"));				
				priceAsgn_DTO.setPaTypeFlag("POST");
				priceAsgn_DTO.setIgnoreSw("N");
				priceAsgn_DTO.setPriceStatusFlag("ACTV");
				priceAsgn_DTO.setPriceCurrencyCode("GBP");
				priceAsgn_DTO.setPrintIfZeroSwitch("Y");
				priceAsgn_DTO.setDoNotAggSw("N");
				priceAsgn_DTO.setScheduleCode("DAILY");
				priceAsgn_DTO.setTxnDailyRatingCrt("AGTR");
				final String[] effectiveDateStringArray1 = startDt.toString().split("-", 50);
				final Date effective_date=new Date(
						Integer.parseInt(effectiveDateStringArray1[0]),
						Integer.parseInt(effectiveDateStringArray1[1]),
						Integer.parseInt(effectiveDateStringArray1[2]));
				priceAsgn_DTO.setStartDate(effective_date);
				priceAsgn_DTO.newEntity();				
			}  catch (Exception e) {
				logger.error("Inside catch block-", e);
			}
		}

		/***********
		 * Price Assignment logic
		 * @return 
		 *
		 */
		private String priceAssignment(String deltaPartyUid, String rateSchedule,
				String priceItemCd, String tou_cd, String paTypeFlg, String ignoreSw,
				String priceStatusFlg, String currency,String printIfZero, String doNotAggSw,
				String scheduleCode, String startDt, String endDt, String txn_header,
				boolean productParmFlg) {

			PriceAsgn_DTO priceAsgn_DTO=null;

			try {
				//******************CI_PRICEASGN*******************//
				priceAsgn_DTO=new PriceAsgn_DTO();
				priceAsgn_DTO.setOwnerId(deltaPartyUid);
				priceAsgn_DTO.setRateScheduleId(new RateSchedule_Id(rateSchedule));
				priceAsgn_DTO.setPaOwnerTypeFlag(agreementPriceHeaderLookUps.getParty().trim());
				priceAsgn_DTO.setPriceItemCodeId(new PriceItem_Id(priceItemCd));

				if (notBlank((tou_cd))) {
					priceAsgn_DTO.setTimeOfUseId(new TimeOfUse_Id(tou_cd));
				}		
				if (notBlank((paTypeFlg))) {
					priceAsgn_DTO.setPaTypeFlag(paTypeFlg);
				}	
				if (notBlank((ignoreSw))) {
					priceAsgn_DTO.setIgnoreSw("N");
				}	
				if (notBlank((priceStatusFlg))) {
					priceAsgn_DTO.setPriceStatusFlag(priceStatusFlg);
				}
				if (notBlank((currency))) {
					priceAsgn_DTO.setPriceCurrencyCode(currency);
				}	
				if (notBlank((printIfZero))) {
					priceAsgn_DTO.setPrintIfZeroSwitch(printIfZero);
				}
				if (notBlank((doNotAggSw))) {
					priceAsgn_DTO.setDoNotAggSw(doNotAggSw);
				}	
				if (notBlank((scheduleCode))) {
					priceAsgn_DTO.setScheduleCode(scheduleCode);
				}
				if (productParmFlg) {
					priceAsgn_DTO.setTxnDailyRatingCrt("DNRT");
				}else{
					priceAsgn_DTO.setTxnDailyRatingCrt("AGTR");
				}

				if (notBlank(startDt)) {
					String startDateStringArray[] = new String[3];
					startDateStringArray = startDt.split("-", 50);
					priceAsgn_DTO.setStartDate(new Date(Integer.parseInt(startDateStringArray[0]),
							Integer.parseInt(startDateStringArray[1]),
							Integer.parseInt(startDateStringArray[2])));
				}
				if (notBlank(endDt)) {
					String endDateStringArray[] = new String[3];
					endDateStringArray = endDt.split("-", 50);
					priceAsgn_DTO.setEndDate(new Date(Integer.parseInt(endDateStringArray[0]),
							Integer.parseInt(endDateStringArray[1]),
							Integer.parseInt(endDateStringArray[2])));
				}

				PriceAsgn priceAsgn = priceAsgn_DTO.newEntity();
				priceAsgnId = priceAsgn.getId().getIdValue();

			}  catch (Exception e) {
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
		
		/***********
		 * Price Assignment Characteristics logic
		 * @return 
		 *
		 */
		private String priceAssignmentChar(Date effectiveDt, String rowId) 
		{
			PriceAssignmentChar_DTO priceAssignmentChar_DTO = null;
			try {
				//******************CI_PRICEASGN_CHAR*******************//
				priceAssignmentChar_DTO=new PriceAssignmentChar_DTO();
				priceAssignmentChar_DTO.setId(new PriceAssignmentChar_Id(new PriceAsgn_Id(priceAsgnId),new CharacteristicType_Id("ROWID"),effectiveDt));
				priceAssignmentChar_DTO.setAdhocCharacteristicValue(rowId);
				priceAssignmentChar_DTO.newEntity();
				
			}  catch (Exception e) {
				logger.error("Inside catch block-", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());

				String errorMessageNumber = errorMessage.substring(errorMessage.indexOf("Number:") + 8, errorMessage.indexOf("Call Sequence:"));
				String errorMessageCategory = errorMessage.substring(errorMessage.indexOf("Category:") + 10, errorMessage.indexOf("Number"));

				if (errorMessage.contains("Text:") && errorMessage.contains("Description:")) {
					errorMessage = errorMessage.substring(errorMessage.indexOf("Text:"), errorMessage.indexOf("Description:"));
				}
				
				if (errorMessage.length() > 250) {
					errorMessage = errorMessage.substring(0, 250);
				}
				else {
					errorMessage = errorMessage.substring(0, errorMessage.length());
				}
				
				return "false" + "~" + errorMessageCategory + "~"+ errorMessageNumber + "~" + errorMessage;
			}
			return "true";
		}
		
		private boolean checkingAndCallingLogError(String txn_header,String deltaPerIdNbr,String returnStatus){
			if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {
				String[] returnStatusArray = returnStatus.split("~");
				returnStatusArray[3] = returnStatusArray[3].replace("Text:", "");				
				return logError(txn_header,deltaPerIdNbr,returnStatusArray[1].trim(), returnStatusArray[2].trim(),returnStatusArray[3].trim());
			} 
			return true;
		}


		/***********
		 * deletePreviousPricecomp logic
		 * @param priceAsgnId 
		 * @return 
		 * 
		 */
		private void deletePreviousPricecomp (String priceAsgnId) {			
			PreparedStatement preparedStatement = null;
			try {
				preparedStatement = createPreparedStatement("DELETE FROM CI_PRICECOMP_TIER" +
						" WHERE PRICECOMP_ID IN (SELECT PRICECOMP_ID FROM CI_PRICECOMP WHERE PRICE_ASGN_ID=:priceAsgnId)","");
				preparedStatement.bindString("priceAsgnId", priceAsgnId, "PRICE_ASGN_ID");
				preparedStatement.executeUpdate();
			}
			catch (LoggedException e) {
				logger.error("Inside catch block-", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}	
			try {
				preparedStatement = createPreparedStatement("DELETE FROM CI_PRICECOMP_K" +
						" WHERE PRICECOMP_ID IN (SELECT PRICECOMP_ID FROM CI_PRICECOMP WHERE PRICE_ASGN_ID=:priceAsgnId)","");
				preparedStatement.bindString("priceAsgnId", priceAsgnId, "PRICE_ASGN_ID");
				preparedStatement.executeUpdate();
			} catch (LoggedException e) {
				logger.error("Inside catch block-", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			//RIA:NAP-30468 Delete from C1_PRICECOMP_L
			try {
				preparedStatement = createPreparedStatement("DELETE FROM C1_PRICECOMP_L" +
						" WHERE PRICECOMP_ID IN (SELECT PRICECOMP_ID FROM CI_PRICECOMP WHERE PRICE_ASGN_ID=:priceAsgnId)","");
				preparedStatement.bindString("priceAsgnId", priceAsgnId, "PRICE_ASGN_ID");
				preparedStatement.executeUpdate();
			} catch (LoggedException e) {
				logger.error("Inside catch block-", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			try {
				preparedStatement = createPreparedStatement("DELETE FROM CI_PRICECOMP" +
						" WHERE PRICE_ASGN_ID=:priceAsgnId","");
				preparedStatement.bindString("priceAsgnId", priceAsgnId, "PRICE_ASGN_ID");
				preparedStatement.executeUpdate();
			} catch (LoggedException e) {
				logger.error("Inside catch block-", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
		/***********
		 * Price Component logic
		 * @param priceItemCd 
		 * @param lineRateType 
		 * @return 
		 * 
		 */
		private String priceComponent(String priceAsgnId,
				BigDecimal lineValueAmount, String rcMapValue, String priceCriteriaCd,BigDecimal lowerLimit,
				BigDecimal upperLimit, String txn_header, String transactionTierId, String priceItemCd, String lineRateType) {
			PriceComp_DTO priceComp_DTO=null;
			PriceCompTier_DTO priceCompTier_DTO=null;
			PreparedStatement preparedStatement = null;
			String priceCriteria="";
			try {				
				priceComp_DTO=new PriceComp_DTO();
				priceCompTier_DTO=new PriceCompTier_DTO();
				priceComp_DTO.setPriceAsgnId(new PriceAsgn_Id(priceAsgnId));
				priceComp_DTO.setValueAmt(lineValueAmount);
				priceComp_DTO.setRcMapId(new RcMap_Id(rcMapValue));
				priceComp_DTO.newEntity();
				if (notBlank((transactionTierId))) {

					preparedStatement = createPreparedStatement("select A.PRICEITEM_REL_TYPE_FLG from CI_PRICEITEM_REL A, CI_PRICECRITERIA B" +
							" where A.PRICEITEM_CHLD_CD=:priceCriteriaCd AND A.PRICEITEM_REL_TYPE_FLG=B.PRICECRITERIA_CD AND B.SQI_CD=:lineRateType","");
					preparedStatement.bindString("priceCriteriaCd", priceCriteriaCd, "PRICEITEM_CHLD_CD");
					preparedStatement.bindString("lineRateType",lineRateType, "SQI_CD");
					preparedStatement.setAutoclose(false);

					for (SQLResultRow resultSet : preparedStatement.list()) {
						priceCriteria = CommonUtils.CheckNull(resultSet.getString("PRICEITEM_REL_TYPE_FLG"));
					}					

					priceCompTier_DTO.setId(new PriceCompTier_Id(new PriceComp_Id(priceComp_DTO.getId().getIdValue()), new BigInteger("1")));
					priceCompTier_DTO.setPriceItemCodeId(new PriceItem_Id(priceItemCd));
					priceCompTier_DTO.setPriceCriteriaId(new PriceCriteria_Id(priceCriteria));
					priceCompTier_DTO.setLowerLimit(lowerLimit);
					priceCompTier_DTO.setUpperLimit(upperLimit);
					priceCompTier_DTO.newEntity();

				}
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
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			return "true";			
		}

		private void updateDeltaLine(String transactionHeaderId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("UPDATE CM_PRICE_LN SET BO_STATUS_CD =:newBoStatus," );
				stringBuilder.append(" STATUS_UPD_DTTM = SYSTIMESTAMP WHERE PRICE_HDR_ID =:price_hdr_id");
				stringBuilder.append( " AND BO_STATUS_CD =:selectBoStatus1" );

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("newBoStatus",agreementPriceHeaderLookUps.getPending(), "BO_STATUS_CD");
				preparedStatement.bindString("price_hdr_id",transactionHeaderId, "PRICE_HDR_ID");
				preparedStatement.bindString("selectBoStatus1",agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
				preparedStatement.executeUpdate();
			}  
			catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}

		private void updateDeltaLine(String transactionHeaderId, String status,
				String messageCategoryNumber, String messageNumber,
				String errorDescription) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				if (CommonUtils.CheckNull(errorDescription).trim().length() > 250) {
					errorDescription = errorDescription.substring(0, 250);
				}
				stringBuilder.append("UPDATE CM_PRICE_LN SET BO_STATUS_CD =:status," );
				stringBuilder.append(" STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber," );
				stringBuilder.append(" ERROR_INFO =:errorDescription WHERE  PRICE_HDR_ID =:headerId");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status", status, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory",messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", messageNumber,"MESSAGE_NBR");
				preparedStatement.bindString("errorDescription",errorDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", transactionHeaderId,"PRICE_HDR_ID");
				preparedStatement.executeUpdate();
			}  
			catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}

		private boolean logError(String transactionHeaderId, String messageKey, String messageCategory, String messageNumber, String message) {

			String completeErrorMessage = "";
			if (!notBlank(message)) {
				completeErrorMessage = CustomMessageRepository
						.agreementPriceError(messageNumber).getMessageText();
			}else{
				completeErrorMessage=message;
			}

			eachAgreementStatusList = new ArrayList<String>();
			eachAgreementStatusList.add(0, transactionHeaderId);
			eachAgreementStatusList.add(1, agreementPriceHeaderLookUps.getError());
			eachAgreementStatusList.add(2, String.valueOf(messageCategory));
			eachAgreementStatusList.add(3, String.valueOf(messageNumber));
			eachAgreementStatusList.add(4, completeErrorMessage);
			updateAgreementStatusList.add(eachAgreementStatusList);
			eachAgreementStatusList = null;

			// Roll back
			rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
			addError(CustomMessageRepository.agreementPriceError(messageNumber));
			return false; // intentionally kept false as roll back has to occur here
		}
		/**
		 * setPendingStatus sets record being processed into Pending state.
		 * @param aInboundMerchantInterfaceId
		 */
		private void setPendingStatus(InboundAgreementFeedInterface_Id aInboundAgreementFeedInterfaceId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			//Set records that would be processed as PENDING
			//Update only effective dated records for processing.
			try {
				stringBuilder = new StringBuilder();
				stringBuilder.append("UPDATE CM_PRICE_HDR SET BO_STATUS_CD =:newBoStatus, ");
				stringBuilder.append("STATUS_UPD_DTTM = SYSTIMESTAMP ");
				stringBuilder.append("WHERE BO_STATUS_CD =:selectBoStatus1 ");
				stringBuilder.append("and PRICE_HDR_ID=:txnHeaderId ");
				//stringBuilder.append("and trim(PRICE_HDR_ID)=trim(:txnHeaderId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("txnHeaderId", aInboundAgreementFeedInterfaceId.getTransactionId(), "PRICE_HDR_ID");
				preparedStatement.bindString("newBoStatus",	agreementPriceHeaderLookUps.getPending(), "BO_STATUS_CD");
				preparedStatement.bindString("selectBoStatus1",agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
				preparedStatement.executeUpdate();
				
			}
			catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}

		public void finalizeThreadWork() throws ThreadAbortedException,RunAbortedException {

			logger.debug("Inside finalizeThreadWork() method");

			//*********************Logic to update erroneous records************************//
			if (updateAgreementStatusList.size() > 0) {
				Iterator<ArrayList<String>> updateAccountStatusItr = updateAgreementStatusList.iterator();
				updateAgreementStatusList = null;
				ArrayList<String> rowList = null;
				while (updateAccountStatusItr.hasNext()) {

					rowList = (ArrayList<String>) updateAccountStatusItr.next();
					updateDeltaStaging(String.valueOf(rowList.get(0)),
							String.valueOf(rowList.get(1)),
							String.valueOf(rowList.get(2)),
							String.valueOf(rowList.get(3)),
							String.valueOf(rowList.get(4)));
					updateDeltaLine(String.valueOf(rowList.get(0)),
							String.valueOf(rowList.get(1)),
							String.valueOf(rowList.get(2)),
							String.valueOf(rowList.get(3)),
							String.valueOf(rowList.get(4)));
					rowList = null;
				}
				updateAccountStatusItr = null;
			}
			super.finalizeThreadWork();
		}

		private void updateDeltaStaging(String transactionHeaderId,String status, String messageCategoryNumber,
				String messageNumber, String errorDescription) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			try {
				if (CommonUtils.CheckNull(errorDescription).trim().length() > 250) {
					errorDescription = errorDescription.substring(0, 250);
				}
				stringBuilder.append("UPDATE CM_PRICE_HDR SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription " );
				if(agreementPriceHeaderLookUps.getCompleted().equalsIgnoreCase(status)){
					stringBuilder.append(",ILM_ARCH_SW ='Y' ");
				}
				stringBuilder.append(" WHERE  PRICE_HDR_ID =:headerId");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status", status, "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", messageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("messageNumber", messageNumber,"MESSAGE_NBR");
				preparedStatement.bindString("errorDescription",errorDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", transactionHeaderId, "PRICE_HDR_ID");
				
				preparedStatement.executeUpdate();

			}
			catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
		
		//Added Logic to retrieve Product Parameters
		private List<InboundAgreementFeedInterface_Id> getDeltaStagingData(AgreementPrice_Id aggPriceId) {

			InboundAgreementFeedInterface_Id inboundAgreementInterface = null;
			List<InboundAgreementFeedInterface_Id> deltaList = new ArrayList<InboundAgreementFeedInterface_Id>();
			PreparedStatement preparedStatement = null;
			String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
			StringBuilder stringBuilder = new StringBuilder();

			
			try {//TODO division column fetch
				stringBuilder.append("SELECT PRICE_HDR_ID, PER_ID_NBR," );
				stringBuilder.append(" PRICEITEM_CD, CURRENCY_CD, START_DT, END_DT, RATE_SCHEDULE,ROW_ID,CAN_FLG, CIS_DIVISION ");  //Multi Division Changes
				stringBuilder.append(" FROM CM_PRICE_HDR ");
				//stringBuilder.append(" WHERE BO_STATUS_CD =:selectBOStatus AND PER_ID_NBR = :perIdNbr and TRIM(PRICEITEM_CD)=TRIM(:priceitem)" );
				//stringBuilder.append(" WHERE BO_STATUS_CD =:selectBOStatus AND PER_ID_NBR = :perIdNbr and PRICEITEM_CD=:priceitem" );
				stringBuilder.append(" WHERE BO_STATUS_CD =:selectBOStatus AND PRICE_HDR_ID BETWEEN :lowHdrId and :highHdrId" );
				

				if (notBlank((txnSourceCode))) {
					stringBuilder.append(" and TXN_SOURCE_CD=:txnSourceCode " );
					stringBuilder.append(" ORDER BY START_DT ASC");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("selectBOStatus",	agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
					//preparedStatement.bindString("perIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					//preparedStatement.bindString("priceitem", perIdNbrId.getPriceItem(), "PRODUCT1");
					preparedStatement.bindString("lowHdrId", aggPriceId.getLowHdrId(), "PRICE_HDR_ID");
					preparedStatement.bindString("highHdrId", aggPriceId.getHighHdrId(), "PRICE_HDR_ID");
					preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");

				}else{
					stringBuilder.append(" ORDER BY START_DT ASC");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("selectBOStatus",	agreementPriceHeaderLookUps.getUpload(), "BO_STATUS_CD");
					//preparedStatement.bindString("perIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					//preparedStatement.bindString("priceitem", perIdNbrId.getPriceItem(), "PRODUCT1");
					preparedStatement.bindString("lowHdrId", aggPriceId.getLowHdrId(), "PRICE_HDR_ID");
					preparedStatement.bindString("highHdrId", aggPriceId.getHighHdrId(), "PRICE_HDR_ID");
				}
				preparedStatement.setAutoclose(false);
				
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String transactionId = resultSet.getString("PRICE_HDR_ID");
					String perIdNbr = resultSet.getString("PER_ID_NBR");
					String priceItemCd = resultSet.getString("PRICEITEM_CD");
					Date startDate = resultSet.getDate("START_DT");
					String endDate = CommonUtils.CheckNull(String.valueOf(resultSet.getDate("END_DT")).trim());
					String currency = resultSet.getString("CURRENCY_CD").trim();
					String rateSchedule = resultSet.getString("RATE_SCHEDULE").trim();
					String rowId = CommonUtils.CheckNull(resultSet.getString("ROW_ID")).trim();
					String canFlag = CommonUtils.CheckNull(resultSet.getString("CAN_FLG")).trim();
					String division = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));  //Multi Division Changes
					SQLResultRow result=getProuctParameters(priceItemCd);

					String prodParm1="";
					String prodParm2="";
					String prodParm3="";
					String prodParm4="";
					String prodParm5="";
					String prodParm6="";
					String prodParm7="";
					String prodParm8="";
					
					if(notNull(result)){
						
						prodParm1 = CommonUtils.CheckNull(result.getString("PROD_PARM1"));
						
						prodParm2 = CommonUtils.CheckNull(result.getString("PROD_PARM2"));
						
						prodParm3 = CommonUtils.CheckNull(result.getString("PROD_PARM3"));
						
						prodParm4 = CommonUtils.CheckNull(result.getString("PROD_PARM4"));
						
						prodParm5 = CommonUtils.CheckNull(result.getString("PROD_PARM5"));
						
						prodParm6 = CommonUtils.CheckNull(result.getString("PROD_PARM6"));
						
						prodParm7 = CommonUtils.CheckNull(result.getString("PROD_PARM7"));
						
						prodParm8 = CommonUtils.CheckNull(result.getString("PROD_PARM8"));


						
						if(notNull(result.getString("PARENT_CHARGE_TYPE"))){
							priceItemCd=result.getString("PARENT_CHARGE_TYPE");
							}
					}									
					
					inboundAgreementInterface = new InboundAgreementFeedInterface_Id(transactionId, perIdNbr,
							startDate, endDate, priceItemCd, currency, rateSchedule,rowId,canFlag,division,prodParm1,prodParm2,
							prodParm3,prodParm4,prodParm5,prodParm6,prodParm7,prodParm8);  //Multi Division Changes

					deltaList.add(inboundAgreementInterface);
					resultSet = null;
					result=null;
					inboundAgreementInterface = null;
				}
			}  
			catch (Exception e) {
				logger.error("Inside catch block-", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			
			return deltaList;
		}


		private SQLResultRow getProuctParameters(String priceItemCd) {
			// TODO Auto-generated method stub
			PreparedStatement ps=null;
			StringBuilder sb=new StringBuilder();
			SQLResultRow result=null;
			
			try{
/*				if(priceItemCd.startsWith("P") && !(priceItemCd.endsWith("A01"))){
					sb.append("SELECT DISTINCT PARENT_CHARGE_TYPE,TTYPE PROD_PARM1,CTYPE PROD_PARM2,PTYPE PROD_PARM3,AUTH_EVENT PROD_PARM4,JTYPE PROD_PARM5,PAYMENT_SCHEME PROD_PARM6,");
					sb.append("AUTH_TRANSACTION_TYPE PROD_PARM7,INTERNAL_AUTH_RESPONSE PROD_PARM8 FROM CM_CHARGE_TYPE_MAP WHERE CHILD_CHARGE_TYPE=:priceItemCd");
				}
				else if(priceItemCd.startsWith("P") && priceItemCd.endsWith("A01")){
					sb.append("SELECT DISTINCT PARENT_CHARGE_TYPE,TTYPE PROD_PARM1,CTYPE PROD_PARM2,PTYPE PROD_PARM3,AUTH_EVENT PROD_PARM4,JTYPE PROD_PARM5,PAYMENT_SCHEME PROD_PARM6, ");
					sb.append("AUTH_TRANSACTION_TYPE PROD_PARM7,INTERNAL_AUTH_RESPONSE PROD_PARM8 FROM CM_CHARGE_TYPE_MAP WHERE CHILD_CHARGE_TYPE=:priceItemCd");
				}
				else if(priceItemCd.startsWith("A") || priceItemCd.startsWith("NA") || priceItemCd.startsWith("F")){
					sb.append("SELECT DISTINCT PARENT_CHARGE_TYPE,TTYPE PROD_PARM1,CTYPE PROD_PARM2,PTYPE PROD_PARM3,AUTH_EVENT PROD_PARM4,JTYPE PROD_PARM5,PAYMENT_SCHEME PROD_PARM6, ");
					sb.append("AUTH_TRANSACTION_TYPE PROD_PARM7,INTERNAL_AUTH_RESPONSE PROD_PARM8 FROM CM_CHARGE_TYPE_MAP WHERE CHILD_CHARGE_TYPE=:priceItemCd");
				}
				
				else{*/
				
					sb.append("SELECT DISTINCT PARENT_CHARGE_TYPE,TTYPE PROD_PARM1,CTYPE PROD_PARM2,PTYPE PROD_PARM3,AUTH_EVENT PROD_PARM4,JTYPE PROD_PARM5,PAYMENT_SCHEME PROD_PARM6, ");
					sb.append("AUTH_TRANSACTION_TYPE PROD_PARM7,INTERNAL_AUTH_RESPONSE PROD_PARM8 FROM CM_CHARGE_TYPE_MAP WHERE CHILD_CHARGE_TYPE=:priceItemCd");
				//}
				
				ps=createPreparedStatement(sb.toString(),"");
				ps.bindString("priceItemCd", priceItemCd, "CHILD_CHARGE_TYPE");
				result=ps.firstRow();			
				
			}
			catch(Exception e){
				logger.error("Error In execution to fetch parameters*************"+e);
			}
			finally{
				if(ps != null){
					ps.close();
					ps=null;
				}
			}
					
			return result;
			
		}
	}

	public static final class PerIdNbr_Id implements Id {

		private static final long serialVersionUID = 1L;
		private String perIdNbr;
		private String priceItem;

		public PerIdNbr_Id(String perIdNbr,String priceItem) {
			setPerIdNbr(perIdNbr);
			setPriceItem(priceItem);
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public String getPriceItem() {
			return priceItem;
		}		

		public void setPriceItem(String priceItem) {
			this.priceItem = priceItem;
		}		

		public boolean isNull() {
			return false;
		}
		public void appendContents(StringBuilder arg0) {

		}

	}

	public static final class InboundAgreementFeedInterface_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String transactionId;

		private String perIdNbr;

		private Date startDate;

		private String endDate;

		private String sequenceNumber;

		private String priceitem_cd;

		private String currency_cd;

		private String rateSchedule;
		
		public String getProdParm1() {
			return prodParm1;
		}

		public void setProdParm1(String prodParm1) {
			this.prodParm1 = prodParm1;
		}

		public String getProdParm2() {
			return prodParm2;
		}

		public void setProdParm2(String prodParm2) {
			this.prodParm2 = prodParm2;
		}

		public String getProdParm3() {
			return prodParm3;
		}

		public void setProdParm3(String prodParm3) {
			this.prodParm3 = prodParm3;
		}

		public String getProdParm4() {
			return prodParm4;
		}

		public void setProdParm4(String prodParm4) {
			this.prodParm4 = prodParm4;
		}

		public String getProdParm5() {
			return prodParm5;
		}

		public void setProdParm5(String prodParm5) {
			this.prodParm5 = prodParm5;
		}

		public String getProdParm6() {
			return prodParm6;
		}

		public void setProdParm6(String prodParm6) {
			this.prodParm6 = prodParm6;
		}

		private String rowId ;
		
		private String canFlag;
		private String division; //Multi Division Changes
		private String prodParm1;
		private String prodParm2;
		private String prodParm3;
		private String prodParm4;
		private String prodParm5;
		private String prodParm6;
		private String prodParm7;
		private String prodParm8;
		

		/**
		 * @return the prodParm7
		 */
		public String getProdParm7() {
			return prodParm7;
		}

		/**
		 * @param prodParm7 the prodParm7 to set
		 */
		public void setProdParm7(String prodParm7) {
			this.prodParm7 = prodParm7;
		}

		/**
		 * @return the prodParm8
		 */
		public String getProdParm8() {
			return prodParm8;
		}

		/**
		 * @param prodParm8 the prodParm8 to set
		 */
		public void setProdParm8(String prodParm8) {
			this.prodParm8 = prodParm8;
		}

		public InboundAgreementFeedInterface_Id(String transactionId,
				String perIdNbr, Date startDate, String endDate,
				String priceitem_CD, String currency_CD,String rateSchedule,String rowId,String canFlag,String division, String prodParm1, String prodParm2, String prodParm3, String prodParm4, String prodParm5, String prodParm6, String prodParm7, String prodParm8 ) {
			setTransactionId(transactionId);
			setPerIdNbr(perIdNbr);
			setStartDate(startDate);
			setEndDate(endDate);
			setPriceitem_cd(priceitem_CD);
			setCurrency_cd(currency_CD);
			setRateSchedule(rateSchedule);
			setRowId(rowId);
			setCanFlag(canFlag);
			setDivision(division);  //Multi Division Changes
			setProdParm1(prodParm1);
			setProdParm2(prodParm2);
			setProdParm3(prodParm3);
			setProdParm4(prodParm4);
			setProdParm5(prodParm5);
			setProdParm6(prodParm6);
			setProdParm7(prodParm7);
			setProdParm8(prodParm8);
		}

		public String getEndDate() {
			return endDate;
		}

		public void setEndDate(String endDate) {
			this.endDate = endDate;
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public String getSequenceNumber() {
			return sequenceNumber;
		}

		public void setSequenceNumber(String sequenceNumber) {
			this.sequenceNumber = sequenceNumber;
		}

		public Date getStartDate() {
			return startDate;
		}

		public void setStartDate(Date startDate) {
			this.startDate = startDate;
		}

		public String getTransactionId() {
			return transactionId;
		}

		public void setTransactionId(String transactionId) {
			this.transactionId = transactionId;
		}

		public String getCurrency_cd() {
			return currency_cd;
		}

		public void setCurrency_cd(String currency_cd) {
			this.currency_cd = currency_cd;
		}

		public String getPriceitem_cd() {
			return priceitem_cd;
		}

		public void setPriceitem_cd(String priceitem_cd) {
			this.priceitem_cd = priceitem_cd;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getRateSchedule() {
			return rateSchedule;
		}

		public void setRateSchedule(String rateSchedule) {
			this.rateSchedule = rateSchedule;
		}
		
		public String getRowId() {
			return rowId;
		}

		public void setRowId(String rowId) {
			this.rowId = rowId;
		}
		
		public String getCanFlag() {
			return canFlag;
		}

		public void setCanFlag(String canFlag) {
			this.canFlag = canFlag;
		}
		
		
		//Multi Division Changes
		public String getDivision() {
			return division;
		}
		public void setDivision(String division) {
			this.division = division;
		}
	}
	
	public static final class AgreementPrice_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String lowHdrId;
		private String highHdrId;

		public AgreementPrice_Id(String lowHdrId, String highHdrId) {
			setLowHdrId(lowHdrId);
			setHighHdrId(highHdrId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getLowHdrId() {
			return lowHdrId;
		}

		public void setLowHdrId(String lowHdrId) {
			this.lowHdrId = lowHdrId;
		}

		public String getHighHdrId() {
			return highHdrId;
		}

		public void setHighHdrId(String highHdrId) {
			this.highHdrId = highHdrId;
		}


	}
}