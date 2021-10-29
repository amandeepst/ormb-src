/*******************************************************************************
* FileName                   : InboundBillableChargeUpload.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Feb 26, 2015
* Version Number             : 1.9
* Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             26-Feb-2015         Sunaina              Implemented all requirement as in Billable Charge Upload Technical Design v0.04.
0.2      NA             28-Sep-2015         Sunaina              Updated Code for fixing Sonar Defects/recurring charges/replacement of BO with DTO/enabling it to handle any contract type.
0.3     NA                        May 17, 2016         Sunaina                       NAP-6121 fix.
0.4      NA             Sep 18, 2016        Preeti               Non Zero Balance Migration-Adhoc Billable charges.
0.5     NA                        Feb 10, 2017        Ankur                Implemented Change 1,Change 2 and Change 3 as per billable charge upload interface TDD.
0.6      NA             Feb 22, 2017        Preeti               Creation of billable charges as line charge.
0.7      NA             Feb 27, 2017        Ankur                Update of billable charges as line charge.
0.8      NA             Mar 13, 2017        Preeti               Handling of update scenarios.
0.9           NA             Apr 24, 2017        Ankur              PAM-12403 fix
1.0           NA             May 25, 2017        Ankur              PAM-12281 fix
1.1           NA             Jan 25, 2018        Preeti                   NAP-22450/NAP-22451: BCHG Staging mapping
1.2           NA             Feb 01, 2018        Ankur              NAP-15587 merchant pricing view
1.3           NAP-24118           Mar 16, 2018         RIA                    Add tables to Billed Billable Charges MO. Remove ILM_DT & ILM_ARCH_SW from CM_BCHG_ATTRIBUTES_MAP.  
1.4      PAM-15782         Apr 17, 2018         Nitika          PAM-15782 - Fix for MIGCHRG2 financial adjustment
1.5           NAP-24108           May 23, 2018         Nitika Sharma        Included ILM_ARCH_SW to be updated to Y for completed status.
1.6            NA                        Jun 11, 2018         RIA                                Prepared Statement close 
1.7         NAP-29464             Jul 03, 2018         RIA                               Bill after date = latest date amongst start date and bill after date    
1.8      NAP-29844         Jul 06, 2018         Nitika                             Defaulting CHARGE_AMT from CM_BCHG_STG to 0.  
1.9      NAP-32005      Aug 30 ,2018            Prerna                     Debt Migration Changes
2.0      NAP-36853      Feb 6 ,2019            Rajesh                     Change to cater for start date issue
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
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillableChargeStatusLookup;
import com.splwg.ccb.api.lookup.RecurringFlgLookup;
import com.splwg.ccb.domain.admin.billPeriod.BillPeriod_Id;
import com.splwg.ccb.domain.admin.chargeType.ChargeType_Id;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
* @author rainas403
*
@BatchJob (multiThreaded = true, rerunnable = false, modules = {"demo"})
*/

public class InboundBillableChargeUpload extends
InboundBillableChargeUpload_Gen {

       public static final Logger logger = LoggerFactory.getLogger(InboundBillableChargeUpload.class);

       private InboundBillableChargeUploadLookUp inboundBillableChargeUploadLookUp = null;

       public InboundBillableChargeUpload() {
       }

       @Override
       public JobWork getJobWork() {
              logger.debug("Inside getJobWork");

              //Initialize Lookup that stores various constants used by this interface.
              inboundBillableChargeUploadLookUp = new InboundBillableChargeUploadLookUp();      

              try {
                     validateData();
              } catch (Exception e){
                     logger.error("Exception in validating data ", e);
              }

              PerIdNbrRecId perIdNbrRecId = null;
              ThreadWorkUnit threadworkUnit = null;
              List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
              List<PerIdNbrRecId> stagingDataList = getPerIdNbrRecs();             
              int rowsForProcessing = stagingDataList.size();
              logger.debug("No of rows selected for processing are - " + rowsForProcessing);

              for (int i=0; i < rowsForProcessing; i++) {
                     perIdNbrRecId = stagingDataList.get(i);
                     threadworkUnit = new ThreadWorkUnit();
                     threadworkUnit.setPrimaryId(perIdNbrRecId);
                     threadWorkUnitList.add(threadworkUnit);
                     threadworkUnit = null;
              }
              stagingDataList = null;
              inboundBillableChargeUploadLookUp = null;
              return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

       }

       //     *********************** getPerIdNbrRecs Method******************************

       /**
       * getPerIdNbrRecs() method selects distinct PER_ID_NBR and PRICEITEM_CD combination from CM_BCHG_STG staging table.
       * 
        * @return List PerIdNbrRecId
       */
       private List<PerIdNbrRecId> getPerIdNbrRecs() {
              logger.debug("Inside getPerIdNbrRecs() method");
              
              logger.debug("inboundBillableChargeUploadLookUp.getUpldStatus() :" + inboundBillableChargeUploadLookUp.getUpldStatus());
              
              PreparedStatement preparedStatement = null;
              PerIdNbrRecId peridNbrRecId = null;
              List<PerIdNbrRecId> rowsForProcessingList = new ArrayList<PerIdNbrRecId>();
              StringBuilder stringBuilder = new StringBuilder();

              String perIdNbr = "";
              String priceItemCode = "";
              String cisDivision = "";

              try {
                     stringBuilder.append("SELECT DISTINCT PER_ID_NBR, PRICEITEM_CD, CIS_DIVISION FROM CM_BCHG_STG ");
                     stringBuilder.append("WHERE BO_STATUS_CD = :selectBoStatus1 ");
                     preparedStatement = createPreparedStatement( stringBuilder.toString(), "");
                     preparedStatement.bindString("selectBoStatus1", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.setAutoclose(false);

                     logger.debug("preparedStatement.list().size() :" + preparedStatement.list().size());
                     
                     for (SQLResultRow resultSet : preparedStatement.list()) {
                           perIdNbr = resultSet.getString("PER_ID_NBR");
                           priceItemCode = resultSet.getString("PRICEITEM_CD");
                           cisDivision = resultSet.getString("CIS_DIVISION");
                           peridNbrRecId = new PerIdNbrRecId(perIdNbr, priceItemCode, cisDivision);
                           rowsForProcessingList.add(peridNbrRecId);
                           resultSet = null;
                           peridNbrRecId = null;
                     }
              } catch (Exception e) {
                     logger.error("Exception in getPerIdNbrRecs" , e);
                     throw new RunAbortedException(CustomMessageRepository
                                  .exceptionInExecution(e.getMessage()));
              } finally {
                     if (preparedStatement != null) {
                           preparedStatement.close();
                           preparedStatement = null;
                     }
              }
              return rowsForProcessingList;
       }

       /**
       * validateData() method will validate the records for Billable Charge Upload Program.
       * 
        * @param inputTable
       */

       private void validateData() {
              logger.debug("Inside Valdidate Data");

              PreparedStatement preparedStatement = null;
              String messageCatNbr = String.valueOf(CustomMessages.MESSAGE_CATEGORY);
              StringBuilder stringBuilder = new StringBuilder();

              try {
                     stringBuilder.append(" UPDATE CM_BCHG_STG SET BO_STATUS_CD = :error, " );
                     stringBuilder.append(" UPLOAD_DTTM = SYSTIMESTAMP, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                     stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
                     stringBuilder.append(" WHERE (TRIM(SA_TYPE_CD) IS NULL OR " );
                     stringBuilder.append(" SA_TYPE_CD NOT IN (SELECT SA_TYPE_CD FROM CI_SA_TYPE)) AND BO_STATUS_CD = :uploadStatus ");

                     preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                     preparedStatement.bindString("uploadStatus", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("error", inboundBillableChargeUploadLookUp.getErrorStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
                     preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.SA_TYPE_CD_NOT_FOUND)), "MESSAGE_NBR");
                     preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.SA_TYPE_CD_NOT_FOUND), "ERROR_INFO");
                     preparedStatement.executeUpdate();

                     stringBuilder = new StringBuilder();
                     stringBuilder.append(" UPDATE CM_BCHG_STG SET BO_STATUS_CD = :error, " );
                     stringBuilder.append(" UPLOAD_DTTM = SYSTIMESTAMP, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                     stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
                     stringBuilder.append(" WHERE (TRIM(PRICEITEM_CD) IS NULL " );
                     stringBuilder.append(" OR TRIM(PRICEITEM_CD) NOT IN (SELECT TRIM(PRICEITEM_CD) FROM CI_PRICEITEM)) " );
                     stringBuilder.append(" AND BO_STATUS_CD = :uploadStatus ");

                     preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                     preparedStatement.bindString("uploadStatus", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("error", inboundBillableChargeUploadLookUp.getErrorStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
                     preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.PRICEITEM_CD_NOT_FOUND)), "MESSAGE_NBR");
                     preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.PRICEITEM_CD_NOT_FOUND), "ERROR_INFO");
                     preparedStatement.executeUpdate();

                     stringBuilder = new StringBuilder();
                     stringBuilder.append(" UPDATE CM_BCHG_STG SET BO_STATUS_CD = :error, " );
                     stringBuilder.append(" UPLOAD_DTTM = SYSTIMESTAMP, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                     stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
                     stringBuilder.append(" WHERE (TRIM(CURRENCY_CD) IS NULL "  );
                     stringBuilder.append(" OR CURRENCY_CD NOT IN(SELECT CURRENCY_CD FROM CI_CURRENCY_CD)) " );
                     stringBuilder.append(" AND BO_STATUS_CD = :uploadStatus ");

                     preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                     preparedStatement.bindString("uploadStatus", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("error", inboundBillableChargeUploadLookUp.getErrorStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
                     preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.CHG_AMT_AND_CRCY_CD_NOT_FOUND)), "MESSAGE_NBR");
                     preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.CHG_AMT_AND_CRCY_CD_NOT_FOUND), "ERROR_INFO");
                     preparedStatement.executeUpdate();

                     stringBuilder = new StringBuilder();
                     stringBuilder.append(" UPDATE CM_BCHG_STG SET BO_STATUS_CD = :error, " );
                     stringBuilder.append(" UPLOAD_DTTM = SYSTIMESTAMP, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                     stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
                     stringBuilder.append(" WHERE (TRIM(BILL_PERIOD_CD) IS NULL " );
                     stringBuilder.append(" OR BILL_PERIOD_CD NOT IN(SELECT BILL_PERIOD_CD FROM CI_BILL_PERIOD)) " );
                     stringBuilder.append(" AND SA_TYPE_CD = :recur " );
                     stringBuilder.append(" AND BO_STATUS_CD = :uploadStatus ");

                     preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                     preparedStatement.bindString("uploadStatus", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("error", inboundBillableChargeUploadLookUp.getErrorStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("recur", inboundBillableChargeUploadLookUp.getRecur(), "SA_TYPE_CD");
                     preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
                     preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.BILL_PERIOD_CD_NOT_FOUND)), "MESSAGE_NBR");
                     preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.BILL_PERIOD_CD_NOT_FOUND), "ERROR_INFO");
                     preparedStatement.executeUpdate();

                     stringBuilder = new StringBuilder();
                     stringBuilder.append(" UPDATE CM_BCHG_STG SET BO_STATUS_CD = :error, " );
                     stringBuilder.append(" UPLOAD_DTTM = SYSTIMESTAMP, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                     stringBuilder.append(" MESSAGE_CAT_NBR = :msgCatNbr, MESSAGE_NBR = :msgNbr, ERROR_INFO = :errorInfo " );
                     stringBuilder.append(" WHERE (TRIM(CHG_TYPE_CD) IS NULL " );
                     stringBuilder.append(" OR CHG_TYPE_CD NOT IN(SELECT CHG_TYPE_CD FROM CI_CHG_TYPE)) " );
                     stringBuilder.append(" AND SA_TYPE_CD = :recur " );
                     stringBuilder.append(" AND BO_STATUS_CD = :uploadStatus ");

                     preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                     preparedStatement.bindString("uploadStatus", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                     preparedStatement.bindString("error", "ERROR", "BO_STATUS_CD");
                     preparedStatement.bindString("recur", inboundBillableChargeUploadLookUp.getRecur(), "SA_TYPE_CD");
                     preparedStatement.bindString("msgCatNbr", messageCatNbr, "MESSAGE_CAT_NBR");
                     preparedStatement.bindString("msgNbr", CommonUtils.CheckNull(String.valueOf(CustomMessages.CHG_TYPE_CD_NOT_FOUND)), "MESSAGE_NBR");
                     preparedStatement.bindString("errorInfo", getErrorDescription(CustomMessages.CHG_TYPE_CD_NOT_FOUND), "ERROR_INFO");
                     preparedStatement.executeUpdate();

              } catch (Exception e) {
                     logger.error("Exception in validateData()", e);
                     throw new RunAbortedException(CustomMessageRepository
                                  .exceptionInExecution(e.getMessage()));
              } finally {
                     if (preparedStatement != null) {
                           preparedStatement.close();
                           preparedStatement = null;
                     }
              }

       }

       //Extracting error info from Application server
       public static String getErrorDescription(int messageNumber) {
              String errorInfo = " ";
              errorInfo = CustomMessageRepository.billCycleError(CommonUtils.CheckNull(String.valueOf(messageNumber))).getMessageText();
              if (errorInfo.contains("Text:")
                           && errorInfo.contains("Description:")) {
                     errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
                                  errorInfo.indexOf("Description:"));
              }
              return errorInfo;
       }


       public Class<InboundBillableChargeUploadWorker> getThreadWorkerClass() {
              return InboundBillableChargeUploadWorker.class;
       }

       public static class InboundBillableChargeUploadWorker extends
       InboundBillableChargeUploadWorker_Gen {


              String saId = "";
              String billableChargeId = "";
              Date recurIlmDt = new Date(1900, 01, 01);                            
              private ArrayList<ArrayList<String>> updateCustomerStatusList = new ArrayList<ArrayList<String>>();
              private ArrayList<String> eachCustomerStatusList = null;
              private InboundBillableChargeUploadLookUp inboundBillableChargeUploadLookUp = null;
              public static final String FAST_PAY_VAL= "FAST_PAY_VAL";
              public static final String CASE_IDENTIFIER= "CASE_IDENTIFIER";
              public static final String PAY_NARRATIVE= "PAY_NARRATIVE";
              public static final String REL_RESERVE_FLG= "REL_RESERVE_FLG";
              public static final String REL_WAF_FLG= "REL_WAF_FLG";
              //            Default constructor
              public InboundBillableChargeUploadWorker() {
              }

              /**
              * initializeThreadWork() method contains logic that is invoked once by
              * the framework per thread.
              */
              @Override
              public void initializeThreadWork(boolean arg0)
                           throws ThreadAbortedException, RunAbortedException {
                     logger.debug("Inside initializeThreadWork()");
                     if(inboundBillableChargeUploadLookUp == null) {
                           inboundBillableChargeUploadLookUp = new InboundBillableChargeUploadLookUp();
                     }
              }

              /**
              * ThreadExecutionStrategy() method contains commit strategy of the
              * interface.
              */
              public ThreadExecutionStrategy createExecutionStrategy() {
                     logger.debug("Inside createExecutionStrategy() method");
                     return new StandardCommitStrategy(this);
              }

              
              public boolean executeWorkUnit(ThreadWorkUnit unit)
                           throws ThreadAbortedException, RunAbortedException {

                     logger.debug("Inside executeWorkUnit()");
                     try {
                           PerIdNbrRecId perIdNbrRecId = (PerIdNbrRecId) unit.getPrimaryId();
                           String skipRowsFlag = "false"; //This flag will be used to determine whether the rows should be skipped or not
                           //as per the status of earlier rows processed with same PER_ID_NBR.

                           List<InboundBillableChargeUpload_Id> billableChargeuploadId = getBillableChargeStagingData(perIdNbrRecId);

                           for (int merchantCount=0; merchantCount<billableChargeuploadId.size(); merchantCount++) {
                        	   	InboundBillableChargeUpload_Id bchgId = billableChargeuploadId.get(merchantCount);


                                  removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
                                  setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));//Required to nullify the effect of database transactions in case of error scenario


                                  logger.debug("Transaction Header Id - " + bchgId.getTxnHeaderId());
                                  logger.debug("Person record with Per Id Nbr- " + bchgId.getPerIdNbr());     

                                  boolean isValid = checkExistingEventId(bchgId);

                                  if(!isValid){
                                         return !isValid;
                                  }

                                  //Check merchant details for example, person id exists or not, account id exists or not and so on
                                  boolean validationFlag = validateMerchantDetails(bchgId);

                                  // If validation has failed, then exit processing
                                  if (!validationFlag) {                                                      
                                         return validationFlag;
                                  }

                                  // ****************** Billable Charge Creation / Update ******************
                                  String returnStatus = createOrUpdateBillableCharge(bchgId);

                                  if (CommonUtils.CheckNull(returnStatus).trim().startsWith("false")) {
                                         String[] returnStatusArray = returnStatus.split("~");
                                         returnStatusArray[1] = returnStatusArray[1].replace("Text:", "");
                                         skipRowsFlag = "true";

                                         return logError(bchgId.getTxnHeaderId(), 
                                                       inboundBillableChargeUploadLookUp.getErrorStatus(),
                                                       returnStatusArray[2].trim(), returnStatusArray[3].trim(), 
                                                       returnStatusArray[1].trim(), skipRowsFlag, 
                                                       bchgId.getPerIdNbr(),
                                                       bchgId.getCisDivision(),
                                                       bchgId.getSaTypeCode(),
                                                       bchgId.getPriceItemCode());
                                  } else {
                                         updateBillableChargeStaging(bchgId.getTxnHeaderId(), 
                                                       inboundBillableChargeUploadLookUp.getCompStatus(),
                                                       "0", "0", " ", skipRowsFlag, 
                                                       bchgId.getPerIdNbr(),
                                                       bchgId.getCisDivision(),
                                                       bchgId.getSaTypeCode(),
                                                       bchgId.getPriceItemCode());
                                  }      
                           } // end of for loop
                           billableChargeuploadId = null;
                     } catch (Exception e) {
                           logger.error("Exception in executeWorkUnit: ", e);
                     }
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

              //***********************getBillableChargeStagingData Method******************************

              /**
              * getBillableChargeStagingData() method selects data from CM_BCHG_STG staging table.
              * 
               * @return List BillableChargeUpload_Id
              */
              private List<InboundBillableChargeUpload_Id> getBillableChargeStagingData (PerIdNbrRecId perIdNbrRecId) {
                     logger.debug("Inside getBillableChargeStagingData() method");
                     PreparedStatement preparedStatement = null;
                     InboundBillableChargeUpload_Id billableChargeUploadId = null;
                     List<InboundBillableChargeUpload_Id> rowsForProcessingList = new ArrayList<InboundBillableChargeUpload_Id>();
                     StringBuilder stringBuilder = new StringBuilder();

                     Date startDate = null;
                     Date endDate = null;
                     Date billAfterDate = null;
                     Date debtDate = null;
                     String txnHeaderId = "";
                     String perIdNbr = "";
                     String cisDivision = "";
                     String saTypeCode = "";
                     String billPeriodCode = "";
                     String priceItemCode = "";
                     String sqiCode = "";
                     String serviceQty = "";
                     String chargeTypeCode = "";
                     String adhocSwitch = "";
                     String chargeAmt = "";
                     String currencyCode = "";
                     String sqiCodeRecr = "";
                     String sqiCodeRecrRate = "";
                     String recrIdentifier = "";
                     String isIndividualFlag="";

                     String fastPayVal="";
                     String caseIdentifier="";
                     String payNarrative="";
                     String relReserveFlg="";
                     String relWafFlg="";
                     String eventId="";
                     String canFlag="";
                     String sourceType="";
                     String sourceId="";

                     try {
                           stringBuilder.append(" SELECT TXN_HEADER_ID, PER_ID_NBR, CIS_DIVISION, SA_TYPE_CD, ");
                           stringBuilder.append(" START_DT, END_DT, BILL_PERIOD_CD, PRICEITEM_CD, SQI_CD, SVC_QTY, CHG_TYPE_CD, BILL_AFTER_DT, ");
                           stringBuilder.append(" SQI_CD_RECR, RECR_RATE, RECR_IDFR, ");
                           stringBuilder.append(" FAST_PAY_VAL, CASE_IDENTIFIER, PAY_NARRATIVE, REL_RESERVE_FLG, REL_WAF_FLG,EVENT_ID, ");
                           stringBuilder.append(" ADHOC_SW, CHARGE_AMT, CURRENCY_CD ,IS_IND_FLG,CAN_FLG,DEBT_DT,SOURCE_TYPE,SOURCE_ID FROM CM_BCHG_STG ");
                           stringBuilder.append(" WHERE BO_STATUS_CD= :selectBoStatus1 ");
                           stringBuilder.append(" AND PER_ID_NBR = :personIdNbr ");
                           stringBuilder.append(" AND CIS_DIVISION = :division ");
                           stringBuilder.append(" AND PRICEITEM_CD = :priceItemCode ");
                           stringBuilder.append(" ORDER BY TXN_HEADER_ID ");

                           preparedStatement = createPreparedStatement(stringBuilder.toString() , "");
                           preparedStatement.bindString("selectBoStatus1", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                           preparedStatement.bindString("personIdNbr", perIdNbrRecId.getPerIdNbr(), "PER_ID_NBR");
                           preparedStatement.bindString("division", perIdNbrRecId.getCisDivision(), "CIS_DIVISION");
                           preparedStatement.bindString("priceItemCode", perIdNbrRecId.getPriceItemCode(), "PRODUCT1");
                           preparedStatement.setAutoclose(false);

                           for (SQLResultRow resultSet : preparedStatement.list()) {
                                  txnHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID"));
                                  perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR"));
                                  cisDivision = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));
                                  saTypeCode = CommonUtils.CheckNull(resultSet.getString("SA_TYPE_CD"));
                                  startDate= resultSet.getDate("START_DT");
                                  endDate= resultSet.getDate("END_DT");
                                  billPeriodCode = CommonUtils.CheckNull(resultSet.getString("BILL_PERIOD_CD"));
                                  priceItemCode = CommonUtils.CheckNull(resultSet.getString("PRICEITEM_CD"));
                                  sqiCode = CommonUtils.CheckNull(resultSet.getString("SQI_CD"));
                                  serviceQty = CommonUtils.CheckNull(String.valueOf(resultSet.getInteger("SVC_QTY")));
                                  chargeTypeCode = CommonUtils.CheckNull(resultSet.getString("CHG_TYPE_CD"));
                                  billAfterDate= resultSet.getDate("BILL_AFTER_DT");
                                  adhocSwitch = CommonUtils.CheckNull(resultSet.getString("ADHOC_SW")).trim();
                                  sourceType = CommonUtils.CheckNull(resultSet.getString("SOURCE_TYPE"));
                                  sourceId = CommonUtils.CheckNull(resultSet.getString("SOURCE_ID"));
                                  //NAP-29844 : Start Changes

                                  if (isNull(resultSet.getBigDecimal("CHARGE_AMT"))){
                                         chargeAmt = CommonUtils.CheckNull(String.valueOf(BigDecimal.ZERO));
                                  }else 
                                  {
                                         chargeAmt = CommonUtils.CheckNull(String.valueOf(resultSet.getBigDecimal("CHARGE_AMT")));
                                  }

                                  //NAP-29844 : End Changes
                                  currencyCode = CommonUtils.CheckNull(resultSet.getString("CURRENCY_CD")).trim();

                                  sqiCodeRecr = CommonUtils.CheckNull(resultSet.getString("SQI_CD_RECR")).trim();
                                  sqiCodeRecrRate = CommonUtils.CheckNull(String.valueOf(resultSet.getBigDecimal("RECR_RATE")));
                                  recrIdentifier = CommonUtils.CheckNull(resultSet.getString("RECR_IDFR")).trim();
                                  isIndividualFlag = CommonUtils.CheckNull(resultSet.getString("IS_IND_FLG")).trim();

                                  fastPayVal = CommonUtils.CheckNull(resultSet.getString(FAST_PAY_VAL)).trim();
                                  caseIdentifier = CommonUtils.CheckNull(resultSet.getString(CASE_IDENTIFIER)).trim();
                                  payNarrative = CommonUtils.CheckNull(resultSet.getString(PAY_NARRATIVE)).trim();
                                  relReserveFlg = CommonUtils.CheckNull(resultSet.getString(REL_RESERVE_FLG)).trim();
                                  relWafFlg = CommonUtils.CheckNull(resultSet.getString(REL_WAF_FLG)).trim();
                                  eventId = CommonUtils.CheckNull(resultSet.getString("EVENT_ID")).trim();

                                  canFlag = CommonUtils.CheckNull(resultSet.getString("CAN_FLG")).trim();
                                  debtDate = resultSet.getDate("DEBT_DT");

                                  billableChargeUploadId = new InboundBillableChargeUpload_Id(
                                                txnHeaderId, perIdNbr, cisDivision,fastPayVal,caseIdentifier,payNarrative,relReserveFlg,relWafFlg,eventId,
                                                saTypeCode, startDate, endDate, billPeriodCode,
                                                priceItemCode, sqiCode, serviceQty, chargeTypeCode,
                                                billAfterDate, adhocSwitch, chargeAmt, currencyCode, sqiCodeRecr, sqiCodeRecrRate, recrIdentifier ,
                                                isIndividualFlag, canFlag, debtDate, sourceType, sourceId);

                                  rowsForProcessingList.add(billableChargeUploadId);
                                  resultSet = null;
                                  billableChargeUploadId = null;
                           }
                     } catch (Exception e) {
                           logger.error("Exception occurred in getBillableChargeStagingData()" , e);
                           throw new RunAbortedException(CustomMessageRepository
                                         .exceptionInExecution(e.getMessage()));
                     } finally {
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }
                     try {
                           stringBuilder = new StringBuilder();
                           stringBuilder.append("UPDATE CM_BCHG_STG SET BO_STATUS_CD =:newBoStatus, STATUS_UPD_DTTM = SYSTIMESTAMP ");
                           stringBuilder.append("WHERE BO_STATUS_CD = :selectBoStatus1 AND PER_ID_NBR = :personIdNbr ");
                           stringBuilder.append("AND CIS_DIVISION = :cisDivision AND PRICEITEM_CD = :priceItem ");

                           preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
                           preparedStatement.bindString("personIdNbr", perIdNbrRecId.getPerIdNbr(), "PER_ID_NBR");
                           preparedStatement.bindString("priceItem", perIdNbrRecId.getPriceItemCode(), "PRODUCT1");
                           preparedStatement.bindString("cisDivision", perIdNbrRecId.getCisDivision(), "CIS_DIVISION");
                           preparedStatement.bindString("newBoStatus", inboundBillableChargeUploadLookUp.getPendStatus(), "BO_STATUS_CD");
                           preparedStatement.bindString("selectBoStatus1", inboundBillableChargeUploadLookUp.getUpldStatus(), "BO_STATUS_CD");
                           preparedStatement.executeUpdate();

                     } catch (Exception e) {
                           logger.error("Exception occurred in getBillableChargeStagingData()" , e);
                           throw new RunAbortedException(CustomMessageRepository
                                         .exceptionInExecution(e.getMessage()));
                     } finally {
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }
                     return rowsForProcessingList;
              }

              /**
              * validateMerchantDetails() method Checks merchant details in RMB like Person Id, Account Id, Contract Id etc.
              * 
               * @param InboundBillableChargeUpload_Id
              * @return
              * @throws RunAbortedException
              */
              private boolean validateMerchantDetails (InboundBillableChargeUpload_Id billableChargeuploadId) {

                     StringBuilder stringBuilder = new StringBuilder();
                     PreparedStatement preparedStatement = null;
                     String personId = "";
                     String accountId = "";
                     String accountType = "";                 

                     if((billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur().trim()) ||
                                  (billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getChrg().trim())))){
                           accountType=inboundBillableChargeUploadLookUp.getChrg().trim();
                     } else if((billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getStaticReserve().trim()) ||
                                  (billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getDynamicReserve().trim())) ||
                                  (billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getFund().trim())) ||
                                  (billableChargeuploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getWaf().trim())))) {
                           accountType = inboundBillableChargeUploadLookUp.getFund().trim();
                     } else {
                           accountType = billableChargeuploadId.getSaTypeCode().trim();
                     }
                     logger.debug("Account type is - "+accountType);
                     String acctNbr = billableChargeuploadId.getPerIdNbr().trim().concat("_")
                                  .concat(accountType).trim().concat("_")
                                  .concat(billableChargeuploadId.getCurrencyCode());

                     try {
                           /********* Determining Person Id ******** */
                           stringBuilder.append("SELECT PERID.PER_ID FROM CI_PER_ID PERID, CI_PER PER " );
                           stringBuilder.append(" WHERE PERID.ID_TYPE_CD =:idTypeCode AND PERID.PER_ID_NBR = :custNumber " );
                           stringBuilder.append(" AND PER.CIS_DIVISION = :division AND PERID.PER_ID = PER.PER_ID");

                           preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
                           preparedStatement.bindString("custNumber", billableChargeuploadId.getPerIdNbr().trim(), "PER_ID_NBR");
                           preparedStatement.bindString("idTypeCode", inboundBillableChargeUploadLookUp.getIdTypeCode().trim(), "ID_TYPE_CD");
                           preparedStatement.bindString("division", billableChargeuploadId.getCisDivision().trim(), "CIS_DIVISION");
                           preparedStatement.setAutoclose(false);

                           if (notNull(preparedStatement.firstRow())) {
                                  personId = preparedStatement.firstRow().getString("PER_ID");
                           } else {
                                  logger.debug("No Person exists");

                                  logError(billableChargeuploadId.getTxnHeaderId(), 
                                                inboundBillableChargeUploadLookUp.getErrorStatus(),
                                                String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
                                                String.valueOf(CustomMessages.PER_ID_NOT_FOUND), 
                                                getErrorDescription(String.valueOf(CustomMessages.PER_ID_NOT_FOUND)), 
                                                "true", billableChargeuploadId.getPerIdNbr(),
                                                billableChargeuploadId.getCisDivision(),
                                                billableChargeuploadId.getSaTypeCode(),
                                                billableChargeuploadId.getPriceItemCode());
                           }

                           /*****closed preparedStatement in case of not null*****/

                           preparedStatement.close();
                           preparedStatement = null;


                           /********* Determining Account Id ******** */
                           stringBuilder = new StringBuilder();
                           stringBuilder.append("SELECT ACCTPER.ACCT_ID " );
                           stringBuilder.append(" FROM CI_ACCT_PER ACCTPER, CI_ACCT_NBR ACCTNBR, CI_ACCT ACCT " );
                           stringBuilder.append(" WHERE ACCTPER.PER_ID = :personId " );
                           stringBuilder.append(" AND ACCTPER.ACCT_ID = ACCT.ACCT_ID ");
                           stringBuilder.append(" AND ACCTNBR.ACCT_ID = ACCT.ACCT_ID " );
                           stringBuilder.append(" AND ACCTNBR.ACCT_NBR_TYPE_CD = 'C1_F_ANO' " );
                           stringBuilder.append(" AND ACCTNBR.ACCT_NBR = :accountNumber " );
                           stringBuilder.append(" AND ACCT.CIS_DIVISION = :division ");

                           preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
                           preparedStatement.bindString("personId", personId, "PER_ID");
                           preparedStatement.bindString("accountNumber",acctNbr, "ACCT_NBR");
                           preparedStatement.bindString("division", billableChargeuploadId.getCisDivision().trim(), "CIS_DIVISION");
                           preparedStatement.setAutoclose(false);

                           if (notNull(preparedStatement.firstRow())) {
                                  accountId = preparedStatement.firstRow().getString("ACCT_ID");
                           } else {
                                  logger.debug("No Account exists for the person");
                                  logError(billableChargeuploadId.getTxnHeaderId(), 
                                                inboundBillableChargeUploadLookUp.getErrorStatus(),
                                                String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
                                                String.valueOf(CustomMessages.ACCOUNT_ID_NOT_FOUND), 
                                                getErrorDescription(String.valueOf(CustomMessages.ACCOUNT_ID_NOT_FOUND)), 
                                                "true", billableChargeuploadId.getPerIdNbr(),
                                                billableChargeuploadId.getCisDivision(),
                                                billableChargeuploadId.getSaTypeCode(),
                                                billableChargeuploadId.getPriceItemCode());
                           }

                           preparedStatement.close();
                           preparedStatement = null;


                           /********* Determining Contract Id ******** */
                           stringBuilder = new StringBuilder();
                           stringBuilder.append("SELECT DISTINCT B.SA_ID FROM CI_SA A, CI_SA_CHAR B " );
                           stringBuilder.append(" WHERE A.SA_ID=B.SA_ID AND B.CHAR_TYPE_CD= :saIdChar " );
                           stringBuilder.append(" AND A.ACCT_ID= :accountId AND A.SA_TYPE_CD= :saTypeCd AND A.SA_STATUS_FLG IN ('20','30') " );
                           stringBuilder.append(" AND A.CIS_DIVISION = :division ");

                           preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                           preparedStatement.bindString("accountId", accountId, "ACCT_ID");
                           preparedStatement.bindString("saTypeCd", billableChargeuploadId.getSaTypeCode().trim(), "SA_TYPE_CD");
                           preparedStatement.bindString("division", billableChargeuploadId.getCisDivision().trim(), "CIS_DIVISION");
                           preparedStatement.bindString("saIdChar", "SA_ID", "CHAR_TYPE_CD");
                           preparedStatement.setAutoclose(false);

                           if(notNull(preparedStatement.firstRow())){
                                  saId = preparedStatement.firstRow().getString("SA_ID");
                           } else {
                                  logger.debug("No Contract exists for the person's charging account");
                                  logError(billableChargeuploadId.getTxnHeaderId(), 
                                                inboundBillableChargeUploadLookUp.getErrorStatus(),
                                                String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
                                                String.valueOf(CustomMessages.SA_ID_NOT_FOUND), 
                                                getErrorDescription(String.valueOf(CustomMessages.SA_ID_NOT_FOUND)), 
                                                "true", billableChargeuploadId.getPerIdNbr(),
                                                billableChargeuploadId.getCisDivision(),
                                                billableChargeuploadId.getSaTypeCode(),
                                                billableChargeuploadId.getPriceItemCode());
                           }

                     } catch(Exception e) {
                           logger.error("Exception occurred in validateMerchantDetails()" , e);
                           return false;
                     } finally {
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }
                     return true;
              }             

              /**
              * createOrUpdateBillableCharge() method Creates or Updates billable charge.
              * 
               * @param InboundBillableChargeUpload_Id
              * @return
              * @throws RunAbortedException
              */
              private String createOrUpdateBillableCharge(
                           InboundBillableChargeUpload_Id billableChargeUploadId) {
                     logger.debug("Inside createOrUpdateBillableCharge()");

                     StringBuilder stringBuilder = new StringBuilder();
                     PreparedStatement preparedStatement = null;
                     PreparedStatement adhocChrgPreparedStmt = null;
                     PreparedStatement dupBillChgAggPreparedStmt=null;
                     PreparedStatement dupBillChgAggPreparedStmt1=null;
                     String statusOfBillableChgProcessing = "";

                     try {                      
                           if (billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur())) {                                 
                                  stringBuilder.append("SELECT BILLABLE_CHG_ID FROM CM_BILL_CHG_CHAR where SRCH_CHAR_VAL=:srchCharVal");
                                  preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                                  preparedStatement.bindString("srchCharVal", billableChargeUploadId.getRecrIdentifier(), "SRCH_CHAR_VAL");
                                  preparedStatement.setAutoclose(false);                                      

                                  if(notNull(preparedStatement.firstRow())) {
                                         //Update if billable charge already exists with the given identifier.   
                                         billableChargeId = preparedStatement.firstRow().getString("BILLABLE_CHG_ID");
                                         statusOfBillableChgProcessing=updateBillableChargeEntity(billableChargeUploadId);
                                         if(statusOfBillableChgProcessing.startsWith("false")) {
                                                return statusOfBillableChgProcessing;
                                         }
                                  } else {
                                         logger.debug("New Billable Charge Id to be created ");
                                         statusOfBillableChgProcessing=createBillableChargeEntity(billableChargeUploadId);
                                         if(statusOfBillableChgProcessing.startsWith("false")) {
                                                return statusOfBillableChgProcessing;
                                         }
                                  }
                           }else {                                  
                                  statusOfBillableChgProcessing=createBillableChargeEntity(billableChargeUploadId);
                                  if(statusOfBillableChgProcessing.startsWith("false")) {
                                         return statusOfBillableChgProcessing;
                                  }
                           }                    

                     } catch (Exception e) {
                           logger.error("Inside catch block of createOrUpdateBillableCharge() method-", e);
                           String errorMessage = CommonUtils.CheckNull(e.getMessage());
                           Map<String, String> errorMsg = new HashMap<String, String>();
                           errorMsg = errorList(errorMessage);
                           return "false" + "~" + errorMsg.get("Text") + "~"
                           + errorMsg.get("Category") + "~"
                           + errorMsg.get("Number");
                     }      finally {
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                           if(adhocChrgPreparedStmt!=null){
                                  adhocChrgPreparedStmt.close();
                                  adhocChrgPreparedStmt=null;
                           }
                           if(dupBillChgAggPreparedStmt!=null){
                                  dupBillChgAggPreparedStmt.close();
                                  dupBillChgAggPreparedStmt=null;
                           }
                           if(dupBillChgAggPreparedStmt1!=null){
                                  dupBillChgAggPreparedStmt1.close();
                                  dupBillChgAggPreparedStmt1=null;
                           }

                     }
                     return "true";
              }

              /**
              * createBillableChargeEntity() method will create a new
              * Billable charge on the contract of Input Merchant.
              * 
               * @param InboundBillableChargeUpload_Id
              * @return
              */

              
              private String createBillableChargeEntity(InboundBillableChargeUpload_Id billableChargeUploadId){

                     logger.debug("Inside createBillableChargeEntity()");
                     billableChargeId = "";
                    
                     PreparedStatement preparedStatement = null;
                     StringBuilder stringBuilder = new StringBuilder();
                    try {
                           BigDecimal numberOfTerminals=new BigDecimal(billableChargeUploadId.getServiceQty());
                           BigDecimal recurringChargeRate=null;
                           BigDecimal billableChargeStgChargeAmount=new BigDecimal(billableChargeUploadId.getChargeAmt());
                           BigDecimal billableChargeLineChargeAmount=null;
                                                 
                           /*********** Function generating billable_chg_id *************/ 
                          
                             String saId1 = new ServiceAgreement_Id(saId).getIdValue();                              
                             billableChargeId= funcBillableChrgId(saId1);
                             
                            /*********** CM_BILL_CHG table insertion *************/   
                             
                           
                           stringBuilder=new StringBuilder();
                           stringBuilder.append("Insert INTO CM_BILL_CHG (BILLABLE_CHG_ID, SA_ID, START_DT, END_DT, DESCR_ON_BILL, PRICEITEM_CD,  VERSION,");
                           stringBuilder.append("CHG_TYPE_CD, RECURRING_FLG, BILL_PERIOD_CD, POLICY_INVOICE_FREQ_CD, START_TM, END_TM,  ");
                           stringBuilder.append("TOU_CD, PRICE_ASGN_ID, PA_ACCT_ID, PA_PER_ID, PA_PRICELIST_ID, FEED_SOURCE_FLG, PRICEITEM_PARM_GRP_ID, CRE_DT, ");
                           stringBuilder.append("GRP_REF_VAL, ADHOC_BILL_SW, AGG_PARM_GRP_ID, ILM_DT, ILM_ARCH_SW, BILLABLE_CHG_STAT, BILL_AFTER_DT) ");
                           stringBuilder.append(" values (:billableChgId, :saId, :startDt, :endDT, :descronBill, :pritemCd, :version, " );
                           stringBuilder.append(" :chrgTypeCd, :recurringFlg, :billPeriodCd,' ', :startTm, :endTm, ' ', ' ', ' ', ");
                           stringBuilder.append(" ' ', ' ',' ', '0', TRUNC(SYSDATE), ' ', ");
                           stringBuilder.append(" :adhocBillSw, '0', :ilmDt , 'N', :billableChgStat,:billAfterDt )");
                           preparedStatement = createPreparedStatement(stringBuilder.toString(),"");  
                           preparedStatement.bindString("billableChgId",billableChargeId, "BILLABLE_CHG_ID" );
                           preparedStatement.bindString("saId",saId1, "SA_ID" );
                           preparedStatement.bindDate("startDt", billableChargeUploadId.getStartDate());                      
                           preparedStatement.bindDate("endDT", billableChargeUploadId.getEndDate());
                           preparedStatement.bindString("descronBill", inboundBillableChargeUploadLookUp.getDescr(),"DESCR_ON_BILL");
                           preparedStatement.bindId("pritemCd", new PriceItem_Id(billableChargeUploadId.getPriceItemCode()));
                           preparedStatement.bindDate("startTm", recurIlmDt);
                           preparedStatement.bindDate("endTm", recurIlmDt);
                           preparedStatement.bindString("adhocBillSw" ,billableChargeUploadId.getAdhocSwitch(),"ADHOC_BILL_SW");
                           preparedStatement.bindBigInteger("version", BigInteger.ONE);                        
                          
                            if("Y".equalsIgnoreCase(billableChargeUploadId.getCanFlag()))
                           {                                 
                                  preparedStatement.bindLookup("billableChgStat", BillableChargeStatusLookup.constants.CANCELED);
                           }
                           else
                           {                               
                                  preparedStatement.bindLookup("billableChgStat", BillableChargeStatusLookup.constants.BILLABLE);
                                  
                           } 
                            if(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur()))
                            {                                   
                              preparedStatement.bindDate("ilmDt", recurIlmDt);
                              preparedStatement.bindId("chrgTypeCd", new ChargeType_Id(billableChargeUploadId.getChargeTypeCode()));                              
                              preparedStatement.bindLookup("recurringFlg", RecurringFlgLookup.constants.BILL_PERIOD);
                              preparedStatement.bindId("billPeriodCd", new BillPeriod_Id(billableChargeUploadId.getBillPeriodCode()));
                              if(notNull(billableChargeUploadId.getBillAfterDate()))
                              {          // RIA: Bill after date = latest date amongst start date and bill after date (NAP-29464)
                                         if(billableChargeUploadId.getBillAfterDate().isSameOrAfter(billableChargeUploadId.getStartDate()))
                                              preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getBillAfterDate());
                                         
                                         else                                              
                                            preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getStartDate());
                              }
                              else
                              {
                                preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getBillAfterDate());   
                              }
                             
                                 
                                  logger.debug("When sa type code is recurring");                                
                                  recurringChargeRate=new BigDecimal(billableChargeUploadId.getSqiCodeRecrRate());                                 
                                  billableChargeLineChargeAmount=numberOfTerminals.multiply(recurringChargeRate);
                           }else
                           {
                                  checkIfBillAfterDateIsNull(billableChargeUploadId);
                                  logger.debug("When sa type code is non-recurring");
                                  preparedStatement.bindDate("ilmDt", getSystemDateTime().getDate()); 
                                  preparedStatement.bindId("chrgTypeCd", new ChargeType_Id(billableChargeUploadId.getChargeTypeCode()));                                
                                  preparedStatement.bindString("recurringFlg","","RECURRING_FLG");                                 
                                  preparedStatement.bindId("billPeriodCd", new BillPeriod_Id(billableChargeUploadId.getBillPeriodCode()));                                 
                                  preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getBillAfterDate());                                
                                  billableChargeLineChargeAmount=numberOfTerminals.multiply(billableChargeStgChargeAmount);
                           }
                            
                               preparedStatement.executeUpdate();
                               if(notNull(preparedStatement))
                               {
                                 preparedStatement.close();
                                 preparedStatement=null;
                               }
                               
                             
                               /*********** CM_B_CHG_LINE table insertion *************/ 
                               
                            funInsertingIntoCiBchgLn(billableChargeUploadId,billableChargeLineChargeAmount);   
                           
                            BillableCharge_Id billChgId= new BillableCharge_Id(billableChargeId);                            
                           
                           /*********** CM_B_LN_CHAR table insertion *************/                         
                            ciBLnCharInsertForDntrert(billableChargeUploadId,billableChargeId);
                           

                           ///**********************SQI inserted as char values*********//
                          insertIntoCiBLncharCForNumberOrTxnVol(billableChargeUploadId,billableChargeId);
                           
                           ciBLnCharInsertForCharTypeCd(billableChargeUploadId,billableChargeId);                        
                           
                           if(billableChargeUploadId.getAdhocSwitch().trim().equals("Y"))
                           {
                                /*********** CM_BILL_CHG table update for Adhoc Bill Switch *************/                                 
                                    updateCiBillableChg(billableChargeUploadId);
                      
                                    /*********** CM_BILL_CHG_CHAR table insertion fro offCycleChartype*************/
                                   insertCibillChgCharForOffCycleCharType(billableChargeId);
                           }

                           //PAM-15782 - START fetch adhoc char val
                            getAdhocCharVal(billableChargeUploadId);

                           //PAM-15782 - END

                           //Insert into CM_BCHG_ATTRIBUTES_MAP table
                            if(!(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur()))) 
                            {
                                   insertIntoCmBchgAtrributeMap(billableChargeUploadId);                                 
                            }                          

                           logger.debug("Billable Charge Id - "+ billChgId.getIdValue() + "updated");
                     } catch (Exception e) {
                           logger.error("Exception in createBillableChargeEntity ", e);
                           String errorMessage = CommonUtils.CheckNull(e.getMessage());
                           Map<String, String> errorMsg = new HashMap<>();
                           errorMsg = errorList(errorMessage);
                           return "false" + "~" + errorMsg.get("Text") + "~"
                           + errorMsg.get("Category") + "~"
                           + errorMsg.get("Number");

                     } finally { 
                    	  if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }
                     return "true";
              }
              
              public void checkIfBillAfterDateIsNull(InboundBillableChargeUpload_Id billableChargeUploadId)
              {
              
                if(billableChargeUploadId.getBillAfterDate()==null)
                {
                  logger.debug("Bill After is Null");

                  logError(billableChargeUploadId.getTxnHeaderId(), 
                                inboundBillableChargeUploadLookUp.getErrorStatus(),
                                String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
                                String.valueOf(CustomMessages.BILL_AFTER_DT_NOT_NULL), 
                                getErrorDescription(String.valueOf(CustomMessages.BILL_AFTER_DT_NOT_NULL)), 
                                "true", billableChargeUploadId.getPerIdNbr(),
                                billableChargeUploadId.getCisDivision(),
                                billableChargeUploadId.getSaTypeCode(),
                                billableChargeUploadId.getPriceItemCode());
                  
                }
              }
              
              public void ciBLnCharInsertForCharTypeCd( InboundBillableChargeUpload_Id billableChargeUploadId,String billableChargeId)
              {
                
               StringBuilder stringBuilder=null;
               PreparedStatement preparedStatement= null;              
              try
              {
                stringBuilder = null;
                stringBuilder = new StringBuilder();
                stringBuilder.append(" insert into CM_B_LN_CHAR (BILLABLE_CHG_ID,LINE_SEQ,CHAR_TYPE_CD,CHAR_VAL,ADHOC_CHAR_VAL,VERSION, ILM_DT) ");
                stringBuilder.append(" values(:billableChgId,:lineSeq , :charTypeCd, :charVal,:adhocCharVal,:version,:ilmDt) ");
                preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID");                
                preparedStatement.bindBigInteger("lineSeq",BigInteger.ONE);
                preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
                preparedStatement.bindBigInteger("version",BigInteger.ONE); 
               if(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur())) {
                 ///**********************BCL TYPE needs to be different for recurring*********//
            	 logger.debug("New Characteristic to be created is = "+billableChargeUploadId.getRecrIdentifier());
                 preparedStatement.bindString("charVal", "PI_RECUR"  ,"CHAR_VAL");
                 preparedStatement.bindString("charTypeCd",inboundBillableChargeUploadLookUp.getBclTypeCharTypeCode(),"CHAR_TYPE_CD"); 
                 preparedStatement.bindString("adhocCharVal"," ","ADHOC_CHAR_VAL");
                 preparedStatement.executeUpdate();  
                 
                 /***********CM_B_LN_CHAR table insertion for RECRRATE*********************/
                 insertIntoCIBLnCharForRecurRate(billableChargeUploadId,billableChargeId);
                 
                 /***********CM_BILL_CHG_CHAR table insertion for Recridfy(RECR)*********************/
                  insertCibillChgCharForRecridfy(billableChargeId,billableChargeUploadId);
                 
          }
           else
           {
                 preparedStatement.bindString("charTypeCd",inboundBillableChargeUploadLookUp.getBclTypeCharTypeCode(),"CHAR_TYPE_CD"); 
                 preparedStatement.bindString("charVal", inboundBillableChargeUploadLookUp.getMscPi(),"CHAR_VAL");
                 preparedStatement.bindString("adhocCharVal"," ","ADHOC_CHAR_VAL");
                 preparedStatement.executeUpdate();  
                
           }
            }
              catch (Exception e){
                logger.error("Exception while inserting  rows in CM_B_LN_CHAR ", e);
            } finally {
                if (preparedStatement != null) {
                       preparedStatement.close();
                       preparedStatement = null;
                }
            }

              }

              
              public void getAdhocCharVal(InboundBillableChargeUpload_Id billableChargeUploadId)
              {
                
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;                
                stringBuilder = new StringBuilder();
                stringBuilder.append("select adhoc_char_val from CI_PRICEITEM_CHAR " );
                stringBuilder.append("where char_type_cd='NON_ZERO' and PRICEITEM_CD=RPAD(:priceitemCode,30,' ') " );
                preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("priceitemCode", billableChargeUploadId.getPriceItemCode().trim(), "PRICEITEM_CD");               
                preparedStatement.setAutoclose(false);
                if(notNull(preparedStatement.firstRow()))
                {
                  /*********** CM_BILL_CHG_CHAR table insertion for NON_ZERO *************/
                  insertCibillChgCharForNonZero(billableChargeUploadId);                             
                }
              }
              public BigDecimal getGranularityHashValue(
                  InboundBillableChargeUpload_Id billableChargeUploadId) {
      StringBuilder fetchHashValue = new StringBuilder();
      PreparedStatement preparedStatement = null;
      BigDecimal graNularityHashValue = BigDecimal.ZERO;
      try {
        fetchHashValue.append(" WITH tbl AS ( ");
        fetchHashValue.append("     SELECT ");
        fetchHashValue.append("     TRIM(:adhocSw) AS adhoc_sw,  ");
        fetchHashValue.append("     TRIM(:payNarrative)  AS pay_narrative, ");
        fetchHashValue.append("     TRIM(:relReserveflg) AS rel_res_flg, ");
        fetchHashValue.append("     TRIM(:relWafFlg) AS rel_waf_flg, ");
        fetchHashValue.append("     TRIM(:fastPayVal) AS fast_pay_val, ");
        fetchHashValue
            .append(
                "     (DECODE(NVL(TRIM(:relReserveflg),'N'),'Y', NVL(TRIM(:caseIdentifier),'N'), ");
        fetchHashValue
            .append(
                "			DECODE(NVL(TRIM(:relWafFlg),'N'), 'Y',NVL(TRIM(:caseIdentifier),'N'),'N'))) ");
        fetchHashValue.append("     AS case_id FROM dual ");
        fetchHashValue.append("     )  ");
        fetchHashValue.append(" SELECT CASE WHEN ");
        fetchHashValue.append("   ( adhoc_sw='N'  OR adhoc_sw IS NULL ) ");
        fetchHashValue.append("    AND ( pay_narrative='N'   OR pay_narrative IS NULL ) ");
        fetchHashValue.append("    AND ( rel_waf_flg='N'  OR rel_waf_flg IS NULL ) ");
        fetchHashValue.append("    AND ( fast_pay_val='N' OR fast_pay_val IS NULL ) ");
        fetchHashValue.append("    AND ( rel_res_flg='N'  OR rel_res_flg IS NULL ) ");
        fetchHashValue.append("    AND case_id ='N' ");
        fetchHashValue.append("  THEN 0 ");
        fetchHashValue.append(
            "	   ELSE ORA_HASH(adhoc_sw||pay_narrative||rel_res_flg||rel_waf_flg||fast_pay_val||case_id ) ");
        fetchHashValue.append("	END granularity_hash FROM tbl ");

        preparedStatement = createPreparedStatement(fetchHashValue.toString(), "");
        preparedStatement
            .bindString("adhocSw", billableChargeUploadId.getAdhocSwitch(), "STATUS_COD");
        preparedStatement
            .bindString("fastPayVal", billableChargeUploadId.getFastPayVal(), FAST_PAY_VAL);
        preparedStatement.bindString("caseIdentifier", billableChargeUploadId.getCaseIdentifier(),
            CASE_IDENTIFIER);
        preparedStatement
            .bindString("payNarrative", billableChargeUploadId.getPayNarrative(), PAY_NARRATIVE);
        preparedStatement.bindString("relReserveflg", billableChargeUploadId.getRelReserveFlg(),
            REL_RESERVE_FLG);
        preparedStatement
            .bindString("relWafFlg", billableChargeUploadId.getRelWafFlg(), REL_WAF_FLG);

        if (notNull(preparedStatement.firstRow())) {
          String hashValue = String.valueOf(preparedStatement.firstRow().getBigDecimal("GRANULARITY_HASH"));
          graNularityHashValue = (hashValue != null ) ? new BigDecimal(hashValue) :
                   BigDecimal.ZERO;
        }
      } catch (Exception e) {
        logger.error("Exception while generating hash value  ", e);
      } finally {
          if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
          }
      }
      return graNularityHashValue; }

      public void insertIntoCmBchgAtrributeMap(
                  InboundBillableChargeUpload_Id billableChargeUploadId) {
      StringBuilder stringBuilder = null;
      PreparedStatement preparedStatement = null;
      BigDecimal granularityHashValue = getGranularityHashValue(billableChargeUploadId);
      try {
        stringBuilder = null;
        stringBuilder = new StringBuilder();
        stringBuilder.append("Insert into CM_BCHG_ATTRIBUTES_MAP ");
        stringBuilder.append(
            "(BILLABLE_CHG_ID, PRICEITEM_CD, SVC_QTY, ADHOC_SW, CHARGE_AMT, FAST_PAY_VAL, ");
        stringBuilder.append(
            "CASE_IDENTIFIER, PAY_NARRATIVE, IS_IND_FLG, REL_RESERVE_FLG, REL_WAF_FLG, EVENT_ID, ");
        stringBuilder.append("DEBT_DT ,SOURCE_TYPE,SOURCE_ID, GRANULARITY_HASH) values  ");
        stringBuilder.append(
            "(:billableChgId,TRIM(:priceitemCode),TRIM(:svcQty),TRIM(:adhocSw),TRIM(:chargeAmt),TRIM(:fastPayVal), ");
        stringBuilder.append(
            "TRIM(:caseIdentifier),TRIM(:payNarrative),TRIM(:isIndFlg),TRIM(:relReserveflg),TRIM(:relWafFlg), ");
        stringBuilder
            .append("TRIM(:eventId),:debtDate,TRIM(:srcType),TRIM(:srcId),:granularityHash ) ");
        preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
        preparedStatement.bindString("billableChgId", billableChargeId, "BILLABLE_CHG_ID");
        preparedStatement
            .bindString("priceitemCode", billableChargeUploadId.getPriceItemCode(), "PRICEITEM_CD");
        preparedStatement.bindString("svcQty", billableChargeUploadId.getServiceQty(), "SVC_QTY");
        preparedStatement
            .bindString("adhocSw", billableChargeUploadId.getAdhocSwitch(), "STATUS_COD");
        preparedStatement
            .bindBigDecimal("chargeAmt", new BigDecimal(billableChargeUploadId.getChargeAmt()));
        preparedStatement
            .bindString("fastPayVal", billableChargeUploadId.getFastPayVal(), FAST_PAY_VAL);
        preparedStatement.bindString("caseIdentifier", billableChargeUploadId.getCaseIdentifier(),
            CASE_IDENTIFIER);
        preparedStatement
            .bindString("payNarrative", billableChargeUploadId.getPayNarrative(), PAY_NARRATIVE);
        preparedStatement
            .bindString("isIndFlg", billableChargeUploadId.getIsIndividualFlag(), "IS_IND_FLG");
        preparedStatement.bindString("relReserveflg", billableChargeUploadId.getRelReserveFlg(),
            REL_RESERVE_FLG);
        preparedStatement
            .bindString("relWafFlg", billableChargeUploadId.getRelWafFlg(), REL_WAF_FLG);
        preparedStatement.bindString("eventId", billableChargeUploadId.getEventId(), "EVENT_ID");
        preparedStatement.bindDate("debtDate", billableChargeUploadId.getDebtDate());
        preparedStatement
            .bindString("srcType", billableChargeUploadId.getSourceType(), "SOURCE_TYPE");
        preparedStatement.bindString("srcId", billableChargeUploadId.getSourceId(), "SOURCE_ID");
        preparedStatement.bindBigDecimal("granularityHash", granularityHashValue);

        preparedStatement.executeUpdate();
      } catch (Exception e) {
          logger.error("Exception while updating rows in CM_BCHG_ATTRIBUTES_MAP ", e);
      } finally {
        if (preparedStatement != null) {
          preparedStatement.close();
          preparedStatement = null;
          }
      } }

              public void updateCiBillableChg(InboundBillableChargeUpload_Id billableChargeUploadId)
              {
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;
                try
                {
                stringBuilder = new StringBuilder();
                stringBuilder.append("UPDATE CM_BILL_CHG SET BILL_AFTER_DT=:billAfterDt, GRP_REF_VAL=:grpRefVal ");
                stringBuilder.append(" WHERE BILLABLE_CHG_ID =:billableChgId");
                preparedStatement=createPreparedStatement(stringBuilder.toString(),"");  
                preparedStatement.bindString("billableChgId",billableChargeId , "BILLABLE_CHG_ID");
                if(notNull(billableChargeUploadId.getBillAfterDate())){
                           // RIA: Bill after date = latest date amongst start date and bill after date (NAP-29464)
                           if(billableChargeUploadId.getBillAfterDate().isSameOrAfter(billableChargeUploadId.getStartDate()))
                                 
                                  preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getBillAfterDate());
                           else
                                 
                             preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getStartDate());
                    } 
                else
                {
                  preparedStatement.bindDate("billAfterDt", billableChargeUploadId.getStartDate());                  
                }
                    preparedStatement.bindBigInteger("grpRefVal", new BigInteger("891581313662"));
                    preparedStatement.executeUpdate();
                }
                catch (Exception e){
                  logger.error("Exception while updating rows in CM_BILL_CHG ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
              }
              public void insertCibillChgCharForNonZero(InboundBillableChargeUpload_Id billableChargeUploadId)
              {
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;
                try
                {
                stringBuilder = new StringBuilder();                                  
                stringBuilder.append("Insert into CM_BILL_CHG_CHAR " );
                stringBuilder.append(" (BILLABLE_CHG_ID, CHAR_TYPE_CD, EFFDT, CHAR_VAL, ADHOC_CHAR_VAL, VERSION, CHAR_VAL_FK1, CHAR_VAL_FK2, ");
                stringBuilder.append(" CHAR_VAL_FK3, CHAR_VAL_FK4, CHAR_VAL_FK5, SRCH_CHAR_VAL, ILM_DT ) values ");
                stringBuilder.append(" (:billableChgId, :charTypeCd, trunc(SYSDATE), :charVal, :adhocCharVal, :version, ");
                stringBuilder.append("  ' ', ' ', ' ', ' ', ' ', :srchCharVal, :ilmDt )");
                preparedStatement=createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID"); 
                preparedStatement.bindBigInteger("version", BigInteger.ONE);   
                preparedStatement.bindString("charVal", " ","CHAR_VAL");
                preparedStatement.bindString("charTypeCd","NON_ZERO", "CHAR_TYPE_CD");
                preparedStatement.bindDate("adhocCharVal",billableChargeUploadId.getStartDate());
                preparedStatement.bindDate("srchCharVal",billableChargeUploadId.getStartDate());
                preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
                preparedStatement.executeUpdate();   
                
                }
                catch (Exception e){
                  logger.error("Exception while inserting  rows in CM_BILL_CHG_CHAR ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
              }
              
              public void insertCibillChgCharForOffCycleCharType(String billableChargeId)
              {
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;
                try
                {
                stringBuilder = new StringBuilder();                                  
                stringBuilder.append("Insert into CM_BILL_CHG_CHAR " );
                stringBuilder.append(" (BILLABLE_CHG_ID, CHAR_TYPE_CD, EFFDT, CHAR_VAL, ADHOC_CHAR_VAL, VERSION, CHAR_VAL_FK1, CHAR_VAL_FK2, ");
                stringBuilder.append(" CHAR_VAL_FK3, CHAR_VAL_FK4, CHAR_VAL_FK5, SRCH_CHAR_VAL, ILM_DT) values ");
                stringBuilder.append(" (:billableChgId, :charTypeCd, trunc(SYSDATE), :charVal, :adhocCharVal, :version, ");
                stringBuilder.append("  ' ', ' ', ' ', ' ', ' ', :srchCharVal, :ilmDt )");
                preparedStatement=createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID"); 
                preparedStatement.bindString("charTypeCd",inboundBillableChargeUploadLookUp.getOffCycleCharTypeCode(),"CHAR_TYPE_CD");
                preparedStatement.bindBigInteger("version",BigInteger.ONE);   
                preparedStatement.bindString("charVal",  inboundBillableChargeUploadLookUp.getCharVal(),"CHAR_VAL");
                preparedStatement.bindString("charTypeCd", inboundBillableChargeUploadLookUp.getOffCycleCharTypeCode(), "CHAR_TYPE_CD");
                preparedStatement.bindString("adhocCharVal"," ","ADHOC_CHAR_VAL");
                preparedStatement.bindString("srchCharVal", inboundBillableChargeUploadLookUp.getCharVal(), "SRCH_CHAR_VAL");
                preparedStatement.bindDate("ilmDt",getSystemDateTime().getDate());
                preparedStatement.executeUpdate(); 
               
                }  
                catch (Exception e){
                  logger.error("Exception while inserting  rows in CM_BILL_CHG_CHAR ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
              }
              public void ciBLnCharInsertForDntrert( InboundBillableChargeUpload_Id billableChargeUploadId,String billableChargeId)
              {
               
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement= null;
               
              try
              {
                stringBuilder = null;
                stringBuilder = new StringBuilder();
                stringBuilder.append(" insert into CM_B_LN_CHAR (BILLABLE_CHG_ID,LINE_SEQ,CHAR_TYPE_CD,CHAR_VAL,ADHOC_CHAR_VAL,VERSION, ILM_DT) ");
                stringBuilder.append(" values(:billableChgId,:lineSeq , :charTypeCd, :charVal,:adhocCharVal,:version, :ilmDt) ");
                preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID");                
               preparedStatement.bindBigInteger("lineSeq",BigInteger.ONE);
               preparedStatement.bindString("charTypeCd",inboundBillableChargeUploadLookUp.getDonotRerateCharTypeCode(),"CHAR_TYPE_CD");                             
               preparedStatement.bindString("charVal",  inboundBillableChargeUploadLookUp.getCharVal(),"CHAR_VAL");
               preparedStatement.bindString("adhocCharVal", " ","ADHOC_CHAR_VAL");
               preparedStatement.bindBigInteger("version",BigInteger.ONE); 
               preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
               preparedStatement.executeUpdate();  
               
              }
              catch (Exception e){
                logger.error("Exception while inserting  rows in CM_B_LN_CHAR ", e);
            } finally {
                if (preparedStatement != null) {
                       preparedStatement.close();
                       preparedStatement = null;
                }
            }

              }
              
              public void insertCibillChgCharForRecridfy(String billableChargeId,InboundBillableChargeUpload_Id billableChargeUploadId)
              {
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;
                try
                {
                stringBuilder = new StringBuilder();                                  
                stringBuilder.append("Insert into CM_BILL_CHG_CHAR " );
                stringBuilder.append(" (BILLABLE_CHG_ID, CHAR_TYPE_CD, EFFDT, CHAR_VAL, ADHOC_CHAR_VAL, VERSION, CHAR_VAL_FK1, CHAR_VAL_FK2, ");
                stringBuilder.append(" CHAR_VAL_FK3, CHAR_VAL_FK4, CHAR_VAL_FK5, SRCH_CHAR_VAL, ILM_DT) values ");
                stringBuilder.append(" (:billableChgId, :charTypeCd, trunc(SYSDATE), :charVal, :adhocCharVal, :version, ");
                stringBuilder.append("  ' ', ' ', ' ', ' ', ' ', :srchCharVal, :ilmDt )");
                preparedStatement=createPreparedStatement(stringBuilder.toString(),"");
                preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID"); 
                preparedStatement.bindBigInteger("version",BigInteger.ONE);   
                preparedStatement.bindString("charVal",  " ","CHAR_VAL");
                preparedStatement.bindString("charTypeCd", "RECRIDFY", "CHAR_TYPE_CD");
                preparedStatement.bindString("adhocCharVal",billableChargeUploadId.getRecrIdentifier(),"ADHOC_CHAR_VAL");
                preparedStatement.bindString("srchCharVal", billableChargeUploadId.getRecrIdentifier(), "SRCH_CHAR_VAL");
                preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
                preparedStatement.executeUpdate();  
              
                }
                catch (Exception e){
                  logger.error("Exception while inserting  rows in CM_BILL_CHG_CHAR ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
              }
              
             public void  funInsertingIntoCiBchgLn(InboundBillableChargeUpload_Id billableChargeUploadId, BigDecimal billableChargeLineChargeAmount)
              {
               StringBuilder stringBuilder=null;
               PreparedStatement preparedStatement=null;
               try
               {
               stringBuilder=new StringBuilder();
               stringBuilder.append("Insert into CM_B_CHG_LINE " );
               stringBuilder.append(" (BILLABLE_CHG_ID,LINE_SEQ,DESCR_ON_BILL,CHARGE_AMT,CURRENCY_CD,SHOW_ON_BILL_SW, " );
               stringBuilder.append(" APP_IN_SUMM_SW,DST_ID,VERSION,MEMO_SW,AGG_PARM_GRP_ID,PRECS_CHARGE_AMT,ILM_DT) values " );
               stringBuilder.append(" (:billableChgId, 1, :descronBill, :billableChargeLineChargeAmount, :currency, 'Y', " );
               stringBuilder.append(" 'N', :dstId, 1, 'N', 1, :precisionChargeAmount, :ilmDt) ");
               preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
               preparedStatement.bindString("billableChgId", billableChargeId, "BILLABLE_CHG_ID");
               preparedStatement.bindString("descronBill", inboundBillableChargeUploadLookUp.getDescr(),"DESCR_ON_BILL");
               preparedStatement.bindString("currency", billableChargeUploadId.getCurrencyCode(), "CURRENCY_CD");
               preparedStatement.bindBigDecimal("billableChargeLineChargeAmount", billableChargeLineChargeAmount.setScale(2, BigDecimal.ROUND_HALF_UP));
               preparedStatement.bindString("dstId", inboundBillableChargeUploadLookUp.getDstId(), "DST_ID");
               preparedStatement.bindBigDecimal("precisionChargeAmount", billableChargeLineChargeAmount.setScale(5, BigDecimal.ROUND_HALF_UP));
               preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
               preparedStatement.executeUpdate(); 
              }
               catch (Exception e){
                 logger.error("Exception while inserting  rows in CM_B_CHG_LINE ", e);
             } finally {
                 if (preparedStatement != null) {
                        preparedStatement.close();
                        preparedStatement = null;
                 }
             }
                
              }
             
             public void insertIntoCIBLnCharForRecurRate(InboundBillableChargeUpload_Id billableChargeUploadId,String billableChargeId )
             {
            	  StringBuilder stringBuilder=null;
                  PreparedStatement preparedStatement= null;                 
                  try
                  {
                  stringBuilder = null;
                  stringBuilder = new StringBuilder();
                  stringBuilder.append(" insert into CM_B_LN_CHAR (BILLABLE_CHG_ID,LINE_SEQ,CHAR_TYPE_CD,CHAR_VAL,ADHOC_CHAR_VAL,VERSION, ILM_DT) ");
                  stringBuilder.append(" values(:billableChgId,:lineSeq , :charTypeCd, :charVal,:adhocCharVal,:version,:ilmDt) ");
                  preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                  preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID");                
                  preparedStatement.bindBigInteger("lineSeq",BigInteger.ONE);
                  preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
                  preparedStatement.bindBigInteger("version",BigInteger.ONE); 
                 if(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur())) 
                 {                  
                   logger.debug("New Characteristic to be created is = "+billableChargeUploadId.getRecrIdentifier());
                   preparedStatement.bindString("charVal"," " ,"CHAR_VAL");
                   preparedStatement.bindString("charTypeCd",billableChargeUploadId.getSqiCodeRecr(),"CHAR_TYPE_CD"); 
                   preparedStatement.bindString("adhocCharVal",billableChargeUploadId.getSqiCodeRecrRate(),"ADHOC_CHAR_VAL");
                   preparedStatement.executeUpdate();  
                  }
         
               }
                catch (Exception e){
                  logger.error("Exception while inserting  rows in CM_B_LN_CHAR For RECRRATE ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
             }
             
             
             public void insertIntoCiBLncharCForNumberOrTxnVol(InboundBillableChargeUpload_Id billableChargeUploadId,String billableChargeId )
             {
            	  StringBuilder stringBuilder=null;
                  PreparedStatement preparedStatement= null; 
                  try
                  {
                  stringBuilder = null;
                  stringBuilder = new StringBuilder();
                  stringBuilder.append(" insert into CM_B_LN_CHAR (BILLABLE_CHG_ID,LINE_SEQ,CHAR_TYPE_CD,CHAR_VAL,ADHOC_CHAR_VAL,VERSION, ILM_DT) ");
                  stringBuilder.append(" values(:billableChgId,:lineSeq , :charTypeCd, :charVal,:adhocCharVal,:version,:ilmDt) ");
                  preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                  preparedStatement.bindString("billableChgId" ,billableChargeId, "BILLABLE_CHG_ID");                
                  preparedStatement.bindBigInteger("lineSeq",BigInteger.ONE);
                  preparedStatement.bindDate("ilmDt", getProcessDateTime().getDate());
                  preparedStatement.bindBigInteger("version",BigInteger.ONE); 
                 if(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur())) 
                 {                  
                   logger.debug("New Characteristic to be created is = "+billableChargeUploadId.getRecrIdentifier());
                   preparedStatement.bindString("charVal"," " ,"CHAR_VAL");
                   preparedStatement.bindString("charTypeCd",billableChargeUploadId.getSqiCode(),"CHAR_TYPE_CD"); 
                   preparedStatement.bindString("adhocCharVal",billableChargeUploadId.getServiceQty(),"ADHOC_CHAR_VAL");
                   preparedStatement.executeUpdate();  
                   
                 }
             else
             {
            	  preparedStatement.bindString("charVal"," " ,"CHAR_VAL");
                  preparedStatement.bindString("charTypeCd",billableChargeUploadId.getSqiCode(),"CHAR_TYPE_CD"); 
                  preparedStatement.bindString("adhocCharVal",billableChargeUploadId.getServiceQty(),"ADHOC_CHAR_VAL");
                  preparedStatement.executeUpdate();  
               }
               }
                catch (Exception e){
                  logger.error("Exception while inserting  rows in CM_B_LN_CHAR ", e);
              } finally {
                  if (preparedStatement != null) {
                         preparedStatement.close();
                         preparedStatement = null;
                  }
              }
             }

             public String funcBillableChrgId(String saId) 
              {
                StringBuilder stringBuilder=null;
                PreparedStatement preparedStatement=null;
                String billableChargeId=null;
              
                try
                {
                  SQLResultRow row=null; 
                 stringBuilder=new StringBuilder();
                 stringBuilder.append("SELECT BCU_GENERATE_BILLABLE_CHG_ID(:saId) as BILLABLE_CHG_ID FROM dual ");
                 preparedStatement = createPreparedStatement(stringBuilder.toString(),""); 
                 preparedStatement.bindString("saId",saId, "SA_ID" );                             
                 preparedStatement.setAutoclose(false);            
                  row = preparedStatement.firstRow();                               
                  if(notNull(row)) 
                  {
                     billableChargeId = row.getString("BILLABLE_CHG_ID");      
                     
                    
                  }
                }
                  catch (Exception e){
                    logger.error("Exception while selecting  rows for same BILLABLE_CHG_ID", e);
                } finally {
                    if (preparedStatement != null) {
                           preparedStatement.close();
                           preparedStatement = null;
                    }
                }
                return billableChargeId;
              }
              /**
              * updateBillableChargeEntity() method will update the existing 
               * Billable charge on the contract of Input Merchant.
              * 
               * @param InboundBillableChargeUpload_Id
              * @return
              */
              private String updateBillableChargeEntity(InboundBillableChargeUpload_Id billableChargeUploadId){
                     //Update scenario: Only required for recurring charges

                     logger.debug("Inside updateBillableChargeEntity()");                    
                     PreparedStatement preparedStatement = null;  

                     BillableCharge_Id bchgId = null;
                     Date billChgStartDt = null;
                
                     try {
                           /*********** CM_BILL_CHG table update *************/
                    	   bchgId = new BillableCharge_Id(billableChargeId);
                    	   billChgStartDt = bchgId.getEntity().getStartDate();
                    	   String saId1 = new ServiceAgreement_Id(saId).getIdValue(); 
                           
                    	   updateCmBillChg(billableChargeUploadId,billChgStartDt,saId1);

                          
                           updateCiBLnCharForAdhocCharVal(billableChargeUploadId);
                           
     
                           BigDecimal numberOfTerminals=new BigDecimal(billableChargeUploadId.getServiceQty());
                           BigDecimal recurringChargeRate=new BigDecimal(billableChargeUploadId.getSqiCodeRecrRate());      
                           BigDecimal billableChargeLineChargeAmount=numberOfTerminals.multiply(recurringChargeRate);

                           StringBuilder stringBuilder = new StringBuilder();
                           stringBuilder.append("UPDATE CM_B_CHG_LINE SET CHARGE_AMT=:billableChargeLineChargeAmount, ");
                           stringBuilder.append("PRECS_CHARGE_AMT=:precisionChargeAmount ");
                           stringBuilder.append("WHERE BILLABLE_CHG_ID=:billableChgId");
                           preparedStatement=createPreparedStatement(stringBuilder.toString(),"");
                           preparedStatement.bindBigDecimal("billableChargeLineChargeAmount", billableChargeLineChargeAmount.setScale(2, BigDecimal.ROUND_HALF_UP));
                           preparedStatement.bindBigDecimal("precisionChargeAmount", billableChargeLineChargeAmount.setScale(5, BigDecimal.ROUND_HALF_UP));
                           preparedStatement.bindString("billableChgId", billableChargeId, "BILLABLE_CHG_ID");
                           preparedStatement.executeUpdate();

                           logger.debug("Billable Charge Id - "+ billableChargeId + "updated");

                     } catch (Exception e) {
                           logger.error("Exception in updateBillableChargeEntity ", e);
                           String errorMessage = CommonUtils.CheckNull(e.getMessage());
                           Map<String, String> errorMsg = new HashMap<String, String>();
                           errorMsg = errorList(errorMessage);
                           return "false" + "~" + errorMsg.get("Text") + "~"
                           + errorMsg.get("Category") + "~"
                           + errorMsg.get("Number");

                     } finally {                          
                           
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }
                     return "true";
              }
              
              
              public void updateCiBLnCharForAdhocCharVal(InboundBillableChargeUpload_Id billableChargeUploadId)
              { 
            	  PreparedStatement preparedStatement = null;
                  StringBuilder stringBuilder = new StringBuilder(); 
              
        	     try {
            	                  
                    stringBuilder.append("UPDATE CM_B_LN_CHAR SET ADHOC_CHAR_VAL=:adhocCharVal ");                           
                    stringBuilder.append("WHERE BILLABLE_CHG_ID=:billableChgId and CHAR_TYPE_CD=:charTypeCd");
                    preparedStatement=createPreparedStatement(stringBuilder.toString(),""); 
                    preparedStatement.bindString("billableChgId", billableChargeId, "BILLABLE_CHG_ID");
                    if(billableChargeUploadId.getSaTypeCode().trim().equals(inboundBillableChargeUploadLookUp.getRecur()))
                    {
                    	  preparedStatement.bindString("charTypeCd", billableChargeUploadId.getSqiCodeRecr(), "CHAR_TYPE_CD");
                          preparedStatement.bindString("adhocCharVal" , billableChargeUploadId.getSqiCodeRecrRate(),"ADHOC_CHAR_VAL");
                      }
                    else
                    {
                    	 preparedStatement.bindString("charTypeCd", billableChargeUploadId.getSqiCode(), "CHAR_TYPE_CD");
                         preparedStatement.bindString("adhocCharVal" , billableChargeUploadId.getServiceQty(),"ADHOC_CHAR_VAL");
                       
                    }
                    preparedStatement.executeUpdate();
                    logger.debug("Billable Charge Id - "+ billableChargeId + "updated");
                     }
                catch (Exception e) {
                 logger.error("Exception in updateCiBLnCharForSqiCode ", e);
                 
                    } 
        	     finally 
        	     {              
                   if (preparedStatement != null) {
                        preparedStatement.close();
                        preparedStatement = null;
                    }
                 }
        	     
              }
              
              public void updateCmBillChg(InboundBillableChargeUpload_Id billableChargeuploadId,Date billChgStartDt,String saId )
              {
            	  
            	  PreparedStatement preparedStatement = null;
                  StringBuilder stringBuilder = new StringBuilder(); 
             
            	  try {
                   stringBuilder.append(" UPDATE CM_BILL_CHG set  SA_ID=:saId,  END_DT=:endDt, CHG_TYPE_CD=:chrgTypeCd, "); 
                   stringBuilder.append(" RECURRING_FLG=:recurringFlg, BILL_PERIOD_CD=:billPeriodCd, PRICEITEM_CD=:pritemCd, BILLABLE_CHG_STAT=:billableChgStat"); 
                   stringBuilder.append(" where BILLABLE_CHG_ID=:billableChargeId ");
                   preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                   preparedStatement.bindString("billableChargeId",billableChargeId, "BILLABLE_CHG_ID");
                   preparedStatement.bindString("saId", saId, "SA_ID");
                    // NAP-36853 Start
                   if (billableChargeuploadId.getEndDate().isBefore(billChgStartDt))
                    {
              	        //billChg_DTO.setEndDate(billChgStartDt);
                        preparedStatement.bindDate("endDt", billChgStartDt);
                      }
                  else
                     {
                         // billChg_DTO.setEndDate(billableChargeUploadId.getEndDate());
                         preparedStatement.bindDate("endDt", billableChargeuploadId.getEndDate());
                      }
                    // NAP-36853 End              
                    preparedStatement.bindId("pritemCd", new PriceItem_Id(billableChargeuploadId.getPriceItemCode()));
                    preparedStatement.bindId("chrgTypeCd", new ChargeType_Id(billableChargeuploadId.getChargeTypeCode()));                              
                    preparedStatement.bindLookup("recurringFlg", RecurringFlgLookup.constants.BILL_PERIOD);
                    preparedStatement.bindId("billPeriodCd", new BillPeriod_Id(billableChargeuploadId.getBillPeriodCode()));                                         
                                        
                
                // NAP-36853 Start
              if((billableChargeuploadId.getEndDate().isBefore(billChgStartDt)) || ("Y".equalsIgnoreCase(billableChargeuploadId.getCanFlag())))
                {
                   // billChg_DTO.setBillableChargeStatus(BillableChargeStatusLookup.constants.CANCELED);  
                   preparedStatement.bindLookup("billableChgStat", BillableChargeStatusLookup.constants.CANCELED);
                }
                else
                {
                    //billChg_DTO.setBillableChargeStatus(BillableChargeStatusLookup.constants.BILLABLE); 
                    preparedStatement.bindLookup("billableChgStat", BillableChargeStatusLookup.constants.BILLABLE);
                }
              
             preparedStatement.executeUpdate();
            
         } 
            catch (Exception e) {
             logger.error("Error in update CM_BILL_CHG");

             } 
             finally {
             if (preparedStatement != null) {
                    preparedStatement.close();
                    preparedStatement = null;
             }
       }
              }
              


              /**
              * updateBillableChargeStaging() method will update the CM_BCHG_STG
              * staging table with the processing status
              * 
               * @param txnHeaderId
              * @param billableChargeId
              * @param status
              * @param messageCategoryNumber
              * @param messageNumber
              * @param errorDescription
              */
              private void updateBillableChargeStaging(String aTransactionHeaderId, String aStatus,
                           String aMessageCategory, String aMessageNumber,String aErrorMessage,
                           String skipRemainingRows, String aPerIdNbr, String aCisDivision, 
                           String aSaTypeCode, String aPriceItem) {

                     logger.debug("Inside updateBillableChargeStaging()");
                     PreparedStatement preparedStatement = null;
                     StringBuilder stringBuilder = new StringBuilder();
                     if (aErrorMessage.length() > 255) {
                           aErrorMessage = aErrorMessage.substring(0, 249);
                     }
                     if (aStatus.trim().equals(inboundBillableChargeUploadLookUp.getErrorStatus().trim())) {
                           billableChargeId = " ";
                     }
                     logger.debug("Status is - "+aStatus);
                     try {
                           stringBuilder.append("UPDATE CM_BCHG_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
                           stringBuilder.append("MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription, ");
                           stringBuilder.append("BILLABLE_CHG_ID=:billableChargeId ");
                           // NAP- 24086 -- START

                           if (inboundBillableChargeUploadLookUp.getCompStatus().equalsIgnoreCase(aStatus)){
                                  stringBuilder.append(",ILM_ARCH_SW = 'Y' ");
                           }

                           // NAP- 24086 -- END 

                           stringBuilder.append("WHERE TXN_HEADER_ID = :headerId  ");
                           stringBuilder.append("AND PER_ID_NBR = :perIdNbr AND CIS_DIVISION = :cisDivision ");
                           stringBuilder.append("AND SA_TYPE_CD = :saTypeCode AND PRICEITEM_CD = :priceItem ");
                           stringBuilder.append("AND BO_STATUS_CD = :status1 ");

                           preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
                           preparedStatement.bindString("status", aStatus.trim(), "BO_STATUS_CD");
                           preparedStatement.bindString("status1", inboundBillableChargeUploadLookUp.getPendStatus().trim(), "BO_STATUS_CD");
                           preparedStatement.bindBigInteger("messageCategory", new BigInteger(aMessageCategory.trim()));
                           preparedStatement.bindBigInteger("messageNumber", new BigInteger(aMessageNumber.trim()));
                           preparedStatement.bindString("errorDescription", CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
                           preparedStatement.bindString("billableChargeId",billableChargeId, "BILLABLE_CHG_ID");
                           preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
                           preparedStatement.bindString("cisDivision", aCisDivision, "CIS_DIVISION");
                           preparedStatement.bindString("perIdNbr", aPerIdNbr, "PER_ID_NBR");
                           preparedStatement.bindString("saTypeCode", aSaTypeCode, "SA_TYPE_CD");
                           preparedStatement.bindString("priceItem", aPriceItem, "PRODUCT1");
                           preparedStatement.executeUpdate();
                     } catch (Exception e) {
                           logger.error("Error in updateBillableChargeStaging");

                     } finally {
                           if (preparedStatement != null) {
                                  preparedStatement.close();
                                  preparedStatement = null;
                           }
                     }

                     //This logic is required to update those rows, with corresponding error messages, which were skipped for processing through execute work unit
                     //since one row with same parent and child merchant combination already has got failed
                     if (CommonUtils.CheckNull(skipRemainingRows).trim().startsWith("true")){
                           logger.debug("updateBillableChargeStaging() for other rows"); 
                           try {
                                  aMessageCategory ="0";
                                  aMessageNumber="0";
                                  aErrorMessage = "Row couldn't be processed: One row is already in error for same Person Id number.";
                                  stringBuilder = new StringBuilder();
                                  stringBuilder.append("UPDATE CM_BCHG_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
                                  stringBuilder.append(" MESSAGE_CAT_NBR = :messageCategory, MESSAGE_NBR = :messageNumber, ERROR_INFO = :errorDescription " );
                                  stringBuilder.append(" WHERE (TXN_HEADER_ID >:headerId) " );
                                  stringBuilder.append(" AND PER_ID_NBR = :perIdNbr " );
                                  stringBuilder.append(" AND PRICEITEM_CD = :priceItem ");
                                  stringBuilder.append(" AND BO_STATUS_CD = :status1 ");
                                  stringBuilder.append(" AND CIS_DIVISION = :cisDivision ");

                                  preparedStatement = createPreparedStatement(stringBuilder.toString(),"" );
                                  preparedStatement.bindString("status1", inboundBillableChargeUploadLookUp.getPendStatus().trim(), "BO_STATUS_CD");
                                  preparedStatement.bindString("status", inboundBillableChargeUploadLookUp.getUpldStatus().trim(), "BO_STATUS_CD");
                                  preparedStatement.bindString("messageCategory", aMessageCategory, "MESSAGE_CAT_NBR");
                                  preparedStatement.bindString("messageNumber", aMessageNumber, "MESSAGE_NBR");
                                  preparedStatement.bindString("errorDescription", aErrorMessage, "ERROR_INFO");
                                  preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
                                  preparedStatement.bindString("perIdNbr", aPerIdNbr, "PER_ID_NBR");
                                  preparedStatement.bindString("cisDivision", aCisDivision, "CIS_DIVISION");
                                  preparedStatement.bindString("priceItem", aPriceItem, "PRODUCT1");
                                  preparedStatement.executeUpdate();

                           } catch (Exception e){
                                  logger.error("Exception while updating other rows for same PER_ID_NBR", e);
                           } finally {
                                  if (preparedStatement != null) {
                                         preparedStatement.close();
                                         preparedStatement = null;
                                  }
                           }
                     }
              }

              private boolean checkExistingEventId(InboundBillableChargeUpload_Id billableChargeuploadId){

                     String eventId = billableChargeuploadId.getEventId();
                     StringBuilder stringBuilder = new StringBuilder();
                     PreparedStatement preparedStatement = null;
                     stringBuilder.append("SELECT count(1) as COUNT from (SELECT BILLABLE_CHG_ID " );
                     stringBuilder.append(" FROM CM_BCHG_ATTRIBUTES_MAP where EVENT_ID=:eventId" );
                     stringBuilder.append(" UNION " );
                     stringBuilder.append(" SELECT BILLABLE_CHG_ID FROM CM_BCHG_STG ");
                     stringBuilder.append(" where TXN_HEADER_ID!=:txnHeaderId " );
                     stringBuilder.append(" and EVENT_ID=:eventId) " );
                     

                     preparedStatement = createPreparedStatement(stringBuilder.toString(), "Query to fetch Count from query");
                     preparedStatement.bindString("eventId",eventId.trim(), "");
                     preparedStatement.bindString("txnHeaderId",billableChargeuploadId.getTxnHeaderId(), "TXN_HEADER_ID");
                     
                     preparedStatement.setAutoclose(false);
                     
                     BigInteger abc = preparedStatement.firstRow().getInteger("COUNT");
                     try{
                    	 
                     if (!abc.equals(BigInteger.ZERO)) {
                           return logError(billableChargeuploadId.getTxnHeaderId(), 
                                         inboundBillableChargeUploadLookUp.getErrorStatus(),
                                         String.valueOf(CustomMessages.MESSAGE_CATEGORY), 
                                         String.valueOf(CustomMessages.EVENT_ID_PRESENT), 
                                         getErrorDescription(String.valueOf(CustomMessages.EVENT_ID_PRESENT)), 
                                         "true", billableChargeuploadId.getPerIdNbr(),
                                         billableChargeuploadId.getCisDivision(),
                                         billableChargeuploadId.getSaTypeCode(),
                                         billableChargeuploadId.getPriceItemCode());


                     }

}
						catch (Exception e){
						    logger.error("Exception while updating other rows for same PER_ID_NBR", e);
						} finally {
						    if (preparedStatement != null) {
						           preparedStatement.close();
						           preparedStatement = null;
						    }
						}

                   

                     return true;

              }

              public Map<String, String> errorList(String errorMessage) {
                     Map<String, String> errorMap = new HashMap<String, String>();
                     String errorMessageNumber = "";
                     String errorMessageCategory = "";
                     if (errorMessage.contains("Number:")) {
                           errorMessageNumber = errorMessage.substring(errorMessage
                                         .indexOf("Number:") + 8, errorMessage
                                         .indexOf("Call Sequence:"));
                           errorMap.put("Number", errorMessageNumber);
                     }
                     if (errorMessage.contains("Category:")) {
                           errorMessageCategory = errorMessage.substring(errorMessage
                                         .indexOf("Category:") + 10, errorMessage
                                         .indexOf("Number"));
                           errorMap.put("Category", errorMessageCategory);
                     }
                     if (errorMessage.contains("Text:")
                                  && errorMessage.contains("Description:")) {
                           errorMessage = errorMessage
                                         .substring(errorMessage.indexOf("Text:"), errorMessage
                                                       .indexOf("Description:"));
                     }
                     if (errorMessage.length() > 250) {
                           errorMessage = errorMessage.substring(0, 250);
                           errorMap.put("Text", errorMessage);
                     } else {
                           errorMessage = errorMessage.substring(0, errorMessage.length());
                           errorMap.put("Text", errorMessage);
                     }
                     return errorMap;
              }      

              /**
              * logError() method stores the error information in the List and does rollback of all the database transaction of this unit.
              * 
               * @param aTransactionHeaderId
              * @param aMessageCategory
              * @param aMessageNumber
              * @param aErrorMessage
              * @param skipRemainingRows
              * @param aPerIdNbr
              * @return
              */           
              private boolean logError(String aTransactionHeaderId, String aStatus,
                           String aMessageCategory, String aMessageNumber,String aErrorMessage,
                           String skipRemainingRows, String aPerIdNbr, String aCisDivision, 
                           String aSaTypeCode, String aPriceItem) {
                     logger.debug("Inside logError method");
                     eachCustomerStatusList = new ArrayList<>();
                     eachCustomerStatusList.add(0, aTransactionHeaderId);
                     eachCustomerStatusList.add(1, inboundBillableChargeUploadLookUp.getErrorStatus());
                     eachCustomerStatusList.add(2, aMessageCategory);
                     eachCustomerStatusList.add(3, aMessageNumber);
                     eachCustomerStatusList.add(4, aErrorMessage);
                     eachCustomerStatusList.add(5, skipRemainingRows);
                     eachCustomerStatusList.add(6, aPerIdNbr);
                     eachCustomerStatusList.add(7, aCisDivision);
                     eachCustomerStatusList.add(8, aSaTypeCode);
                     eachCustomerStatusList.add(9, aPriceItem);
                     updateCustomerStatusList.add(eachCustomerStatusList);
                     eachCustomerStatusList = null;

                     //Excepted to do rollback
                     rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));
                     if (aMessageCategory.trim().equals(String.valueOf(CustomMessages.MESSAGE_CATEGORY))) {
                           addError(CustomMessageRepository.billCycleError(aMessageNumber));
                     }
                     return false; // intentionally kept false as rollback has to occur here
              }

              /**
              * getErrorDescription() method selects error message description from ORMB message catalog.
              * 
               * @return errorInfo
              */
              public static String getErrorDescription(String messageNumber) {
                     String errorInfo = " ";
                     errorInfo = CustomMessageRepository.merchantError(
                                  messageNumber).getMessageText();
                     if (errorInfo.contains("Text:")
                                  && errorInfo.contains("Description:")) {
                           errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
                                         errorInfo.indexOf("Description:"));
                     }
                     return errorInfo;
              }
             

              /**
              * finalizeThreadWork() execute by the batch program once per thread
              * after processing all units.
              */  
              @Override
              public void finalizeThreadWork() throws ThreadAbortedException,
              RunAbortedException {
                     logger.debug("Inside finalizeThreadWork() method");
                     //Logic to update erroneous records
                     if (updateCustomerStatusList.size() > 0) {
                           Iterator<ArrayList<String>> updateBillableChargeStatusItr = updateCustomerStatusList.iterator();
                           updateCustomerStatusList = null;
                           ArrayList<String> rowList = null;
                           while (updateBillableChargeStatusItr.hasNext()) {
                                  rowList = (ArrayList<String>) updateBillableChargeStatusItr.next();
                                  updateBillableChargeStaging(String.valueOf(rowList.get(0)),
                                                String.valueOf(rowList.get(1)), 
                                                String.valueOf(rowList.get(2)), 
                                                String.valueOf(rowList.get(3)), 
                                                String.valueOf(rowList.get(4)), 
                                                String.valueOf(rowList.get(5)), 
                                                String.valueOf(rowList.get(6)), 
                                                String.valueOf(rowList.get(7)), 
                                                String.valueOf(rowList.get(8)), 
                                                String.valueOf(rowList.get(9)));
                                  rowList = null;
                           }
                           updateBillableChargeStatusItr = null;
                     }
                     inboundBillableChargeUploadLookUp = null;
                     super.finalizeThreadWork();
              }
       } //End of Worker Class

       public static final class PerIdNbrRecId implements Id {

              private static final long serialVersionUID = 1L;

              private String perIdNbr;
              private String priceItemCode;
              private String cisDivision;

              public PerIdNbrRecId(String perIdNbr, String priceItemCode, String cisDivision) {
                     setPerIdNbr(perIdNbr);
                     setPriceItemCode(priceItemCode);
                     setCisDivision(cisDivision);
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

              public String getPriceItemCode() {
                     return priceItemCode;
              }

              public void setPriceItemCode(String priceItemCode) {
                     this.priceItemCode = priceItemCode;
              }

              public String getCisDivision() {
                     return cisDivision;
              }

              public void setCisDivision(String cisDivision) {
                     this.cisDivision = cisDivision;
              }
       }

       public static final class InboundBillableChargeUpload_Id implements Id {

              private static final long serialVersionUID = 1L;
              private String txnHeaderId;
              private String perIdNbr;
              private String cisDivision;
              private String saTypeCode;
              private Date startDate;
              private Date endDate;
              private String billPeriodCode;
              private String priceItemCode;
              private String sqiCode;
              private String serviceQty;
              private String chargeTypeCode;
              private Date billAfterDate;
              private String adhocSwitch;
              private String chargeAmt;
              private String currencyCode;
              private String sqiCodeRecr;
              private String sqiCodeRecrRate;
              private String recrIdentifier;
              private String isIndividualFlag;

              private String fastPayVal="";
              private String caseIdentifier="";
              private String payNarrative="";
              private String relReserveFlg="";
              private String relWafFlg="";
              private String eventId="";
              private String canFlag="";
              private Date debtDate;
              private String sourceType;
              private String sourceId;

              public InboundBillableChargeUpload_Id(String txnHeaderId,
                           String perIdNbr, String cisDivision, String fastPayVal,String caseIdentifier,
                           String payNarrative,String relReserveFlg,String relWafFlg,String eventId,
                           String saTypeCode, com.splwg.base.api.datatypes.Date startDate, 
                           com.splwg.base.api.datatypes.Date endDate,
                           String billPeriodCode, String priceItemCode, String sqiCode,
                           String serviceQty, String chargeTypeCode, 
                           com.splwg.base.api.datatypes.Date billAfterDate,
                           String adhocSwitch, String chargeAmt, String currencyCode, String sqiCodeRecr, 
                           String sqiCodeRecrRate, String recrIdentifier ,String isIndividualFlag ,String canFlag, Date debtDate, String sourceType,String sourceId) {
                     setTxnHeaderId(txnHeaderId);
                     setPerIdNbr(perIdNbr);
                     setCisDivision(cisDivision);
                     setSaTypeCode(saTypeCode);
                     setStartDate(startDate);
                     setEndDate(endDate);
                     setBillPeriodCode(billPeriodCode);
                     setPriceItemCode(priceItemCode);
                     setSqiCode(sqiCode);
                     setServiceQty(serviceQty);
                     setChargeTypeCode(chargeTypeCode);
                     setBillAfterDate(billAfterDate);
                     setAdhocSwitch(adhocSwitch);
                     setChargeAmt(chargeAmt);
                     setCurrencyCode(currencyCode);
                     setSqiCodeRecr(sqiCodeRecr);
                     setSqiCodeRecrRate(sqiCodeRecrRate);
                     setRecrIdentifier(recrIdentifier);
                     setIsIndividualFlag(isIndividualFlag);

                     setFastPayVal(fastPayVal);
                     setCaseIdentifier(caseIdentifier);
                     setPayNarrative(payNarrative);
                     setRelReserveFlg(relReserveFlg);
                     setRelWafFlg(relWafFlg);
                     setEventId(eventId);
                     setCanFlag(canFlag);
                     setDebtDate(debtDate);
                     setSourceType(sourceType);
                     setSourceId(sourceId);
              }

              public boolean isNull() {
                     return false;
              }

              public void appendContents(StringBuilder arg0) {
              }

              public String getFastPayVal() {
                     return fastPayVal;
              }

              public void setFastPayVal(String fastPayVal) {
                     this.fastPayVal = fastPayVal;
              }
              
              

              public String getSourceType() {
				return sourceType;
			}

			public void setSourceType(String sourceType) {
				this.sourceType = sourceType;
			}

			public String getSourceId() {
				return sourceId;
			}

			public void setSourceId(String sourceId) {
				this.sourceId = sourceId;
			}

			public String getCaseIdentifier() {
                     return caseIdentifier;
              }

              public void setCaseIdentifier(String caseIdentifier) {
                     this.caseIdentifier = caseIdentifier;
              }

              public String getPayNarrative() {
                     return payNarrative;
              }

              public void setPayNarrative(String payNarrative) {
                     this.payNarrative = payNarrative;
              }

              public String getRelReserveFlg() {
                     return relReserveFlg;
              }

              public void setRelReserveFlg(String relReserveFlg) {
                     this.relReserveFlg = relReserveFlg;
              }

              public String getRelWafFlg() {
                     return relWafFlg;
              }

              public void setRelWafFlg(String relWafFlg) {
                     this.relWafFlg = relWafFlg;
              }

              public String getEventId() {
                     return eventId;
              }

              public void setEventId(String eventId) {
                     this.eventId = eventId;
              }

              public String getAdhocSwitch() {
                     return adhocSwitch;
              }

              public void setAdhocSwitch(String adhocSwitch) {
                     this.adhocSwitch = adhocSwitch;
              }

              public String getBillPeriodCode() {
                     return billPeriodCode;
              }

              public void setBillPeriodCode(String billPeriodCode) {
                     this.billPeriodCode = billPeriodCode;
              }

              public String getChargeAmt() {
                     return chargeAmt;
              }

              public void setChargeAmt(String chargeAmt) {
                     this.chargeAmt = chargeAmt;
              }

              public String getChargeTypeCode() {
                     return chargeTypeCode;
              }

              public void setChargeTypeCode(String chargeTypeCode) {
                     this.chargeTypeCode = chargeTypeCode;
              }

              public String getCurrencyCode() {
                     return currencyCode;
              }

              public void setCurrencyCode(String currencyCode) {
                     this.currencyCode = currencyCode;
              }

              public String getPerIdNbr() {
                     return perIdNbr;
              }

              public void setPerIdNbr(String perIdNbr) {
                     this.perIdNbr = perIdNbr;
              }

              public String getPriceItemCode() {
                     return priceItemCode;
              }

              public void setPriceItemCode(String priceItemCode) {
                     this.priceItemCode = priceItemCode;
              }

              public String getSaTypeCode() {
                     return saTypeCode;
              }

              public void setSaTypeCode(String saTypeCode) {
                     this.saTypeCode = saTypeCode;
              }

              public String getServiceQty() {
                     return serviceQty;
              }

              public void setServiceQty(String serviceQty) {
                     this.serviceQty = serviceQty;
              }

              public String getSqiCode() {
                     return sqiCode;
              }

              public void setSqiCode(String sqiCode) {
                     this.sqiCode = sqiCode;
              }

              public String getTxnHeaderId() {
                     return txnHeaderId;
              }

              public void setTxnHeaderId(String txnHeaderId) {
                     this.txnHeaderId = txnHeaderId;
              }

              public String getCisDivision() {
                     return cisDivision;
              }

              public void setCisDivision(String cisDivision) {
                     this.cisDivision = cisDivision;
              }

              public com.splwg.base.api.datatypes.Date getBillAfterDate() {
                     return billAfterDate;
              }

              public void setBillAfterDate(com.splwg.base.api.datatypes.Date billAfterDate) {
                     this.billAfterDate = billAfterDate;
              }

              public com.splwg.base.api.datatypes.Date getEndDate() {
                     return endDate;
              }

              public void setEndDate(com.splwg.base.api.datatypes.Date endDate) {
                     this.endDate = endDate;
              }

              public com.splwg.base.api.datatypes.Date getStartDate() {
                     return startDate;
              }

              public void setStartDate(com.splwg.base.api.datatypes.Date startDate) {
                     this.startDate = startDate;
              }

              public String getSqiCodeRecr() {
                     return sqiCodeRecr;
              }

              public void setSqiCodeRecr(String sqiCodeRecr) {
                     this.sqiCodeRecr = sqiCodeRecr;
              }

              public String getSqiCodeRecrRate() {
                     return sqiCodeRecrRate;
              }

              public void setSqiCodeRecrRate(String sqiCodeRecrRate) {
                     this.sqiCodeRecrRate = sqiCodeRecrRate;
              }

              public String getRecrIdentifier() {
                     return recrIdentifier;
              }

              public void setRecrIdentifier(String recrIdentifier) {
                     this.recrIdentifier = recrIdentifier;
              }

              public String getIsIndividualFlag() {
                     return isIndividualFlag;
              }

              public void setIsIndividualFlag(String isIndividualFlag) {
                     this.isIndividualFlag = isIndividualFlag;
              }

              public String getCanFlag() {
                     return canFlag;
              }

              public void setCanFlag(String canFlag) {
                     this.canFlag = canFlag;
              }

              public Date getDebtDate() {
                     return debtDate;
              }

              public void setDebtDate(Date debtDate) {
                     this.debtDate = debtDate;
              }
       }
}
