/*******************************************************************************
 * FileName                   : TaxPreCalculation_Impl
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Feb 09, 2017
 * Version Number             : 1.3
 * Revision History           :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Feb 09, 2017        Preeti        Pre-Tax calculation algorithm implementation.
0.2      NA             Mar 09, 2017        Preeti        Performance review updates.
0.3      NA             Apr 18, 2017        Vienna Rom    Performance and sonar issues.
0.4      NA             Jun 09, 2017        Vienna Rom    Removed logger.debug and used StringBuilder.
0.5      NA             Aug 30, 2017        Preeti        Adding Post Tax Calculation logic and removing fix to tax precision.
0.6      NA             Feb 19, 2017        Preeti        PAM-17408 Rounding on the basis of price assignment ID.
0.7      NA             Mar 27, 2018        Vienna Rom    Running Totals implementation
0.8      NA             Jun 18, 2018        Ankur         NAP-26251 Fix
0.9      NA             Jul 31, 2018        RIA           NAP-31068 RUN_TOT char for adhoc charge fix
1.0      NA             Sep 06, 2018        Vienna Rom    Reverted running totals changes 0.7 and 0.9
1.1      NA             Sep 18, 2018        Vienna Rom    Added bill completion day check
1.2      NA             Sep 29, 2018        RIA           NAP-31865 round by currency exponent
1.3      NA             Oct 05, 2018        RIA           NAP-34569 Exclude FX Income 
 *******************************************************************************/

package com.splwg.cm.domain.wp.algorithm;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.serviceAgreementType.SaTypePostProcessingAlgorithmSpot;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 * 
 * @AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (name = TaxDistributionCode, required = true, type = string)})
 */
public class TaxPreCalculation_Impl extends TaxPreCalculation_Gen
    implements SaTypePostProcessingAlgorithmSpot {

  private static final Logger logger = LoggerFactory.getLogger(TaxPreCalculation_Impl.class);
  private static final String EXCEPTION_IN_UPDATE_FT = "Exception in updateBsAmounts()";
  private static final String EXCEPTION_IN_SELECT_DST_ID = "Exception in select DST_ID";
  private static final String EXCEPTION_IN_FTGL_INSERT = "Exception in createFtGlDstLine()";
  private static final String EXCEPTION_MESSAGE = "Exception-";
  private static final CharacteristicType_Id runningTotalCharTypeId =
      new CharacteristicType_Id("RUN_TOT");
  private static final CharacteristicType_Id bclCharType = new CharacteristicType_Id("BCL-TYPE");
  private static final CharacteristicType_Id runningTotalTimestampCharacteristic =
      new CharacteristicType_Id("CM_RTTC");
  private static final String CHRG = "CHRG";
  private String parameterBillid;
  private Bill_Id billId;
  private String billCycCode;
  private Bool freezeCmpSw;

  /**
   * Main processing.
   */
  public void invoke() {

    billId = new Bill_Id(parameterBillid);
    String time = null;
    boolean bsegExists = false;

    // Add running total for the day
    bsegExists = getBillSegments(billId);
    if (bsegExists) {
      time = getTimestamp(billId);
      addRunningTotal(time, billId);
      updateTimestampChar(billId);

      // Correct amounts if regular bill for completion or re-bill/ad-hoc
      // bill
      if ((notBlank(billCycCode) && freezeCmpSw.isTrue()) || isBlankOrNull(billCycCode)) {
        createCharForRecurCharges();
        correctBillAmounts();
      }
    }
  }

  

   private void updateTimestampChar(Bill_Id billId) {

    PreparedStatement preparedStatement1 = null;
    StringBuilder sb;

    try {
      preparedStatement1 = null;
      sb = new StringBuilder();
      sb.append("UPDATE CI_BILL_CHAR ");
      sb.append("SET ADHOC_CHAR_VAL= :processDate, ");
      sb.append("SRCH_CHAR_VAL= :processDate ");
      sb.append("WHERE BILL_ID=:billId AND ");
      sb.append("CHAR_TYPE_CD=:runningTotalTimestamp ");

      preparedStatement1 = createPreparedStatement(sb.toString(), "");
      preparedStatement1.bindId("billId", billId);
      preparedStatement1.bindId("runningTotalTimestamp", runningTotalTimestampCharacteristic);
      // RUN_TOT changes to stamp System Date instead of Batch Business Date
      preparedStatement1.bindDateTime("processDate", getSystemDateTime());
      preparedStatement1.setAutoclose(false);
      preparedStatement1.executeUpdate();

    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement1 != null) {
        preparedStatement1.close();
      }
    }

  }

  /**
   * Add/Update Running Total bill characteristic per product and price assignment combination for
   * bill segments created for the day
   */
  private void addRunningTotal(String time, Bill_Id billId) {

    BigInteger maxSeqNum = null;
    SQLResultRow billCharRow;
    List<SQLResultRow> prodList = null;


    // Get max seq num of bill's chars
    maxSeqNum = getMaxSeqNum(billId);

    prodList = getDataForChildProduct(time, billId);

    if (notNull(prodList)) {

      for (SQLResultRow resultSet : prodList) {
        if (notBlank(resultSet.getString("PRICEITEM_CD"))) {

          if (maxSeqNum.equals(BigInteger.ZERO)) {

            maxSeqNum = BigInteger.ONE;

            insertOrUpdateRunningTotalChar(maxSeqNum, resultSet,"INSERT");

          } else {
            // Query for existing RUN_TOT entry
            billCharRow = getExistingRunningTotalChar(resultSet);
            // If char exists, update it. Else, insert a new char.

            maxSeqNum = maxSeqNum.add(BigInteger.ONE);
            getDataIfCharExists(billCharRow, maxSeqNum, resultSet);

          }
        }
      }
    }
  }

  private SQLResultRow getExistingRunningTotalChar(SQLResultRow resultSet) {
    PreparedStatement preparedStatement1 = null;
    SQLResultRow row = null;
    StringBuilder sb;

    try {
      sb = new StringBuilder();
      sb.append("SELECT C.SEQ_NUM, TO_NUMBER(C.ADHOC_CHAR_VAL) AS RUNNINGTOTAL ");
      sb.append("FROM CI_BILL_CHAR C ");
      sb.append("WHERE C.BILL_ID=:billId ");
      sb.append("AND C.CHAR_TYPE_CD=:runningTotalCharTypeId ");
      sb.append("AND C.SRCH_CHAR_VAL=:priceItemCode ");
      sb.append("AND C.CHAR_VAL_FK1=:priceAsgnId ");
      sb.append("AND C.CHAR_VAL_FK4=:dstId ");
      sb.append("AND C.CHAR_VAL_FK5=:settLevelGranularity ");
      preparedStatement1 = createPreparedStatement(sb.toString(), "");
      preparedStatement1.bindId("billId", billId);
      preparedStatement1.bindId("runningTotalCharTypeId", runningTotalCharTypeId);
      preparedStatement1.bindString("priceItemCode", resultSet.getString("PRICEITEM_CD"),
          "SRCH_CHAR_VAL");
      preparedStatement1.bindString("priceAsgnId",
          isBlankOrNull(resultSet.getString("PRICE_ASGN_ID")) ? " "
              : resultSet.getString("PRICE_ASGN_ID"),
          "CHAR_VAL_FK1");
      preparedStatement1.bindString("dstId", resultSet.getString("DST_ID"), "CHAR_VAL_FK4");
      preparedStatement1.bindString("settLevelGranularity",
          resultSet.getString("SETT_LEVEL_GRANULARITY"), "CHAR_VAL_FK5");



      preparedStatement1.setAutoclose(false);
      row = preparedStatement1.firstRow();

    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement1 != null) {
        preparedStatement1.close();
        preparedStatement1 = null;
      }
    }

    return row;
  }

  public List<SQLResultRow> getDataForChildProduct(String time, Bill_Id billId) {
    StringBuilder sb = null;
    PreparedStatement preparedStatement = null;
    List<SQLResultRow> prodList = null;
    try {

      sb = new StringBuilder();
      sb.append("WITH TBL AS ( SELECT NVL((SELECT MAP.CHILD_PRODUCT ");
      sb.append(
          "FROM CM_TXN_ATTRIBUTES_MAP MAP WHERE BILLABLE_CHG_ID = C.BILLABLE_CHG_ID),C.PRICEITEM_CD) AS PRICEITEM_cD,  ");
      sb.append(
          "NVL((SELECT MAP.SETT_LEVEL_GRANULARITY FROM CM_TXN_ATTRIBUTES_MAP MAP WHERE BILLABLE_CHG_ID = C.BILLABLE_CHG_ID ");
      sb.append(
          "AND NOT EXISTS(SELECT 1 FROM CI_SA WHERE SA_ID=C.SA_ID AND SA_TYPE_CD=:saTypeCd)),0) SETT_LEVEL_GRANULARITY, ");
      sb.append("C.PRICE_aSGN_ID , D.CHARGE_AMT AS PRODTOTAL, D.PRECS_CHARGE_AMT AS CHARGEAMT,  ");
      sb.append("B.BSEG_ID AS BSEG_ID, D.DST_ID AS DST_ID  ");
      sb.append("FROM CI_BSEG_CALC A, CI_BSEG B, CI_BILL_CHG C, CI_B_CHG_LINE D, CI_B_LN_CHAR E ");
      sb.append("WHERE A.BSEG_ID = B.BSEG_ID ");
      sb.append("AND A.BILLABLE_CHG_ID = C.BILLABLE_CHG_ID ");
      sb.append("AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID ");
      sb.append("AND D.BILLABLE_CHG_ID = E.BILLABLE_CHG_ID ");
      sb.append("AND D.LINE_SEQ = E.LINE_SEQ ");
      sb.append("AND B.BILL_ID = :billId ");
      sb.append("AND ( C.RECURRING_FLG = ' ' OR C.RECURRING_FLG IS NULL) ");
      if (notNull(time)) {
        sb.append("AND B.CRE_DTTM > :time ");
      }
      sb.append("AND E.CHAR_TYPE_CD = :bclCharType) SELECT ");
      

      sb.append(" /*+ ");
      sb.append("      BEGIN_OUTLINE_DATA ");
      sb.append("      IGNORE_OPTIM_EMBEDDED_HINTS ");
      sb.append("      OPTIMIZER_FEATURES_ENABLE('10.1.0.5') ");
      sb.append("      DB_VERSION('12.1.0.2') ");
      sb.append("      OPT_PARAM('_fix_control' '20243268:1 16732417:1 26664361:7') ");
      sb.append("      ALL_ROWS ");
      sb.append("      OUTLINE_LEAF(@\"SEL$3\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$4\") ");
      sb.append("      OUTLINE_LEAF(@\"SET$1\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$2\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$8\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$6\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$7\") ");
      sb.append("      OUTLINE_LEAF(@\"SET$2\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$5\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$193D1E52\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$A66D0F29\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$FEC66756\") ");
      sb.append("      OUTLINE_LEAF(@\"SET$61667F90\") ");
      sb.append("      OLD_PUSH_PRED(@\"SEL$1\" \"C\"@\"SEL$1\" (\"VWM_BILLABLE_CHARGE\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      OUTLINE_LEAF(@\"SEL$3461DC8E\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$42DA16AB\") ");
      sb.append("      OUTLINE_LEAF(@\"SET$6071500D\") ");
      sb.append("      OLD_PUSH_PRED(@\"SEL$1\" \"D\"@\"SEL$1\" (\"CM_B_CHG_LINE\".\"BILLABLE_CHG_ID\" \"CM_B_CHG_LINE\".\"LINE_SEQ\")) ");
      sb.append("      OUTLINE_LEAF(@\"SEL$354EC8B6\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$620EB3CF\") ");
      sb.append("      OUTLINE_LEAF(@\"SET$013C4B19\") ");
      sb.append("      OLD_PUSH_PRED(@\"SEL$1\" \"E\"@\"SEL$1\" (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\" \"CM_B_LN_CHAR\".\"LINE_SEQ\") (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\"  ");
      sb.append("              \"CM_B_LN_CHAR\".\"LINE_SEQ\")) ");
      sb.append("      OUTLINE_LEAF(@\"SEL$1\") ");
      sb.append("      OUTLINE_LEAF(@\"SEL$16\") ");
      sb.append("      OUTLINE(@\"SEL$9\") ");
      sb.append("      OUTLINE(@\"SEL$10\") ");
      sb.append("      OUTLINE(@\"SEL$11\") ");
      sb.append("      OUTLINE(@\"SET$3\") ");
      sb.append("      OUTLINE(@\"SEL$1\") ");
      sb.append("      OUTLINE(@\"SEL$12\") ");
      sb.append("      OUTLINE(@\"SEL$13\") ");
      sb.append("      OUTLINE(@\"SET$4\") ");
      sb.append("      OUTLINE(@\"SEL$14\") ");
      sb.append("      OUTLINE(@\"SEL$15\") ");
      sb.append("      OUTLINE(@\"SET$5\") ");
      sb.append("      NO_ACCESS(@\"SEL$16\" \"TBL\"@\"SEL$16\") ");
      sb.append("      INDEX_RS_ASC(@\"SEL$1\" \"B\"@\"SEL$1\" (\"CI_BSEG\".\"BILL_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$1\" \"A\"@\"SEL$1\" (\"CI_BSEG_CALC\".\"BSEG_ID\" \"CI_BSEG_CALC\".\"HEADER_SEQ\")) ");
      sb.append("      NO_ACCESS(@\"SEL$1\" \"C\"@\"SEL$1\") ");
      sb.append("      NO_ACCESS(@\"SEL$1\" \"D\"@\"SEL$1\") ");
      sb.append("      NO_ACCESS(@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("      LEADING(@\"SEL$1\" \"B\"@\"SEL$1\" \"A\"@\"SEL$1\" \"C\"@\"SEL$1\" \"D\"@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("      USE_NL(@\"SEL$1\" \"A\"@\"SEL$1\") ");
      sb.append("      USE_NL(@\"SEL$1\" \"C\"@\"SEL$1\") ");
      sb.append("      USE_NL(@\"SEL$1\" \"D\"@\"SEL$1\") ");
      sb.append("      USE_NL(@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("      INDEX(@\"SEL$620EB3CF\" \"CM_B_LN_CHAR\"@\"SEL$15\" (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\" \"CM_B_LN_CHAR\".\"LINE_SEQ\"  ");
      sb.append("              \"CM_B_LN_CHAR\".\"CHAR_TYPE_CD\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$354EC8B6\" \"VWM_BILLABLE_CHARGE_LINE_CHAR\"@\"SEL$14\" (\"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"BILLABLE_CHG_ID\"  ");
      sb.append("              \"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"LINE_SEQ\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$42DA16AB\" \"CM_B_CHG_LINE\"@\"SEL$13\" (\"CM_B_CHG_LINE\".\"BILLABLE_CHG_ID\" \"CM_B_CHG_LINE\".\"LINE_SEQ\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$3461DC8E\" \"VWM_BILLABLE_CHARGE_LINE\"@\"SEL$12\" (\"VWM_BILLABLE_CHARGE_LINE\".\"BILLABLE_CHG_ID\"  ");
      sb.append("              \"VWM_BILLABLE_CHARGE_LINE\".\"LINE_SEQ\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$FEC66756\" \"VWM_BILLABLE_CHARGE\"@\"SEL$11\" (\"VWM_BILLABLE_CHARGE\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$A66D0F29\" \"MIG_BILL_CHG\"@\"SEL$10\" (\"MIG_BILL_CHG\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$193D1E52\" \"CM_BILL_CHG\"@\"SEL$9\" (\"CM_BILL_CHG\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      NO_ACCESS(@\"SEL$5\" \"MAP\"@\"SEL$5\") ");
      sb.append("      NO_ACCESS(@\"SEL$2\" \"MAP\"@\"SEL$2\") ");
      sb.append("      INDEX_RS_ASC(@\"SEL$4\" \"MIG_TXN_ATTRIBUTES_MAP\"@\"SEL$4\" (\"MIG_TXN_ATTRIBUTES_MAP\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$3\" \"VWM_BILLABLE_CHARGE_ATTRIBUTES\"@\"SEL$3\" (\"VWM_BILLABLE_CHARGE_ATTRIBUTES\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$7\" \"MIG_TXN_ATTRIBUTES_MAP\"@\"SEL$7\" (\"MIG_TXN_ATTRIBUTES_MAP\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$6\" \"VWM_BILLABLE_CHARGE_ATTRIBUTES\"@\"SEL$6\" (\"VWM_BILLABLE_CHARGE_ATTRIBUTES\".\"BILLABLE_CHG_ID\")) ");
      sb.append("      INDEX_RS_ASC(@\"SEL$8\" \"CI_SA\"@\"SEL$8\" (\"CI_SA\".\"SA_ID\")) ");
      sb.append("      END_OUTLINE_DATA ");
      sb.append("  */ ");
      
      sb.append("PRICEITEM_CD, PRICE_ASGN_ID, SUM(PRODTOTAL) AS BSEGTOTAL , ");
      sb.append(
          "SUM(CHARGEAMT) AS PRODTOTAL, MAX(BSEG_ID) AS BSEG_ID, DST_ID, SETT_LEVEL_GRANULARITY  FROM TBL ");
      sb.append("GROUP BY PRICEITEM_CD, PRICE_ASGN_ID, DST_ID , SETT_LEVEL_GRANULARITY ");

      preparedStatement = createPreparedStatement(sb.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("bclCharType", bclCharType);
      preparedStatement.bindString("saTypeCd", CHRG, "SA_TYPE_CD");

      if (notNull(time)) {
        preparedStatement.bindString("time", time, "CRE_DTTM");
      }
      preparedStatement.setAutoclose(false);
      prodList = preparedStatement.list();


    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }
    return prodList;

  }

  public void getDataIfCharExists(SQLResultRow billCharRow, BigInteger maxSeqNum,
      SQLResultRow resultSet) {

    if (billCharRow != null) {
      // Update existing running total
      insertOrUpdateRunningTotalChar(billCharRow.getInteger("SEQ_NUM"), resultSet,"UPDATE");
    } else {
      insertOrUpdateRunningTotalChar(maxSeqNum, resultSet,"INSERT");
    }
  }

  public BigInteger getMaxSeqNum(Bill_Id billId) {
    PreparedStatement preparedStatement = null;
    StringBuilder sb;
    BigInteger maxSeqNum = null;
    try {
      sb = new StringBuilder();
      sb.append("SELECT MAX(SEQ_NUM) AS MAXSEQ FROM CI_BILL_CHAR ");
      sb.append("WHERE BILL_ID=:billId AND CHAR_TYPE_CD=:runningTotalCharTypeId ");
      preparedStatement = createPreparedStatement(sb.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("runningTotalCharTypeId", runningTotalCharTypeId);
      preparedStatement.setAutoclose(false);
      SQLResultRow billCharRow = preparedStatement.firstRow();
      if (notNull(billCharRow)) {
        maxSeqNum = billCharRow.getInteger("MAXSEQ");
      }
      if (isNull(maxSeqNum)) {
        maxSeqNum = BigInteger.ZERO;
      }
    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }
    return maxSeqNum;
  }

  
  private void insertOrUpdateRunningTotalChar(BigInteger seqNum, SQLResultRow resultSet,String commandName) {

	logger.debug(" Inside insertOrUpdateRunningTotalChar "+" seqNum - " +seqNum +" ***** Bill Id - " +billId +" ****PRICEITEM_CD - " + resultSet.getString("PRICEITEM_CD"));
	
    PreparedStatement preparedStatement1 = null;
    StringBuilder sb;

    try {
      preparedStatement1 = null;
      sb = new StringBuilder();
      if(commandName.equals("UPDATE")) {
      sb.append("UPDATE CI_BILL_CHAR ");
      sb.append("SET ADHOC_CHAR_VAL=TO_NUMBER(ADHOC_CHAR_VAL)+:productPriceTotal, ");
      sb.append("CHAR_VAL_FK2=TO_NUMBER(CHAR_VAL_FK2)+:bsegTotal ");
      sb.append("WHERE BILL_ID=:billId AND SEQ_NUM=:seqNum ");
      sb.append("AND CHAR_TYPE_CD=:runningTotalCharTypeId ");
      sb.append("AND CHAR_VAL_FK1=:priceAsgnId ");
      sb.append("AND SRCH_CHAR_VAL=:priceItemCode ");
      sb.append("AND CHAR_VAL_FK4=:dstId ");
      preparedStatement1 = createPreparedStatement(sb.toString(), "");
      }
      else
      {
        sb.append(
            "Insert into CI_BILL_CHAR (BILL_ID,CHAR_TYPE_CD,SEQ_NUM,VERSION,CHAR_VAL,ADHOC_CHAR_VAL, ");
        sb.append("CHAR_VAL_FK1,CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4,CHAR_VAL_FK5,SRCH_CHAR_VAL) ");
        sb.append(
            "values (:billId,:runningTotalCharTypeId,:seqNum,1,' ',:productPriceTotal,:priceAsgnId,:bsegTotal,:bsegId,:dstId,:settLevelGranularity,:priceItemCode) ");
        
        preparedStatement1 = createPreparedStatement(sb.toString(), "");
        preparedStatement1.bindString("bsegId",resultSet.getString("BSEG_ID"), "CHAR_VAL_FK2");
        preparedStatement1.bindString("settLevelGranularity", resultSet.getString("SETT_LEVEL_GRANULARITY"), "CHAR_VAL_FK5");
      }
      
      preparedStatement1.bindId("billId", billId);
      preparedStatement1.bindId("runningTotalCharTypeId", runningTotalCharTypeId);
      preparedStatement1.bindBigInteger("seqNum", seqNum);
      preparedStatement1.bindBigDecimal("productPriceTotal",
          isBlankOrNull(resultSet.getString("PRODTOTAL")) ? BigDecimal.ZERO
              : resultSet.getBigDecimal("PRODTOTAL"));
      preparedStatement1.bindString("priceItemCode", resultSet.getString("PRICEITEM_CD"),
          "SRCH_CHAR_VAL");
      preparedStatement1.bindString("priceAsgnId",
          isBlankOrNull(resultSet.getString("PRICE_ASGN_ID")) ? " "
              : resultSet.getString("PRICE_ASGN_ID"),
          "CHAR_VAL_FK1");
      preparedStatement1.bindBigDecimal("bsegTotal",
          isBlankOrNull(resultSet.getString("BSEGTOTAL")) ? BigDecimal.ZERO
              : resultSet.getBigDecimal("BSEGTOTAL"));
      preparedStatement1.bindString("dstId", resultSet.getString("DST_ID"), "CHAR_VAL_FK4");
      preparedStatement1.setAutoclose(false);
      preparedStatement1.executeUpdate();

    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement1 != null) {
        preparedStatement1.close();
      }
    }

  }

  
  private void createCharForRecurCharges() {

    String priceItemCode = null;
    BigInteger maxSeqNum = null;


    PreparedStatement preparedStatement = null;
    SQLResultRow billCharRow;
    List<SQLResultRow> prodList = null;
    StringBuilder sb;

    // Get max seq num of bill's chars
    try {
      sb = new StringBuilder();
      sb.append("SELECT MAX(SEQ_NUM) AS MAXSEQ FROM CI_BILL_CHAR ");
      sb.append("WHERE BILL_ID=:billId AND CHAR_TYPE_CD=:runningTotalCharTypeId ");
      preparedStatement = createPreparedStatement(sb.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("runningTotalCharTypeId", runningTotalCharTypeId);
      preparedStatement.setAutoclose(false);
      billCharRow = preparedStatement.firstRow();
      if (billCharRow != null) {
        maxSeqNum = billCharRow.getInteger("MAXSEQ");
      }
    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }

    try {
      preparedStatement = null;
      sb = new StringBuilder();
      sb.append("WITH TBL AS ( SELECT NVL((SELECT MAP.CHILD_PRODUCT ");
      sb.append(
          "FROM CM_TXN_ATTRIBUTES_MAP MAP WHERE BILLABLE_CHG_ID = C.BILLABLE_CHG_ID),C.PRICEITEM_CD) AS PRICEITEM_cD,  ");
      sb.append(
          "NVL((SELECT MAP.SETT_LEVEL_GRANULARITY FROM CM_TXN_ATTRIBUTES_MAP MAP WHERE BILLABLE_CHG_ID = C.BILLABLE_CHG_ID),0) SETT_LEVEL_GRANULARITY, ");
      sb.append(" NVL((SELECT LN.ADHOC_CHAR_VAL FROM CI_B_LN_CHAR LN WHERE BILLABLE_CHG_ID = A.BILLABLE_CHG_ID AND CHAR_TYPE_CD='RECRRATE'),' ') PRICE_ASGN_ID, ");
      sb.append(" D.CHARGE_AMT AS PRODTOTAL, D.PRECS_CHARGE_AMT AS CHARGEAMT, ");
      sb.append(" B.BSEG_ID AS BSEG_ID, D.DST_ID AS DST_ID ");
      sb.append("FROM CI_BSEG_CALC A, CI_BSEG B, CI_BILL_CHG C, CI_B_CHG_LINE D, CI_B_LN_CHAR E ");
      sb.append("WHERE A.BSEG_ID = B.BSEG_ID ");
      sb.append("AND A.BILLABLE_CHG_ID = C.BILLABLE_CHG_ID ");
      sb.append("AND C.BILLABLE_CHG_ID = D.BILLABLE_CHG_ID ");
      sb.append("AND D.BILLABLE_CHG_ID = E.BILLABLE_CHG_ID ");
      sb.append("AND D.LINE_SEQ = E.LINE_SEQ ");
      sb.append("AND B.BILL_ID = :billId ");
      sb.append("AND C.RECURRING_FLG > ' ' AND C.RECURRING_FLG IS NOT NULL ");
      sb.append("AND E.CHAR_TYPE_CD = :bclCharType) SELECT ");
      
      sb.append(" /*+ ");
      sb.append("       BEGIN_OUTLINE_DATA ");
      sb.append("       IGNORE_OPTIM_EMBEDDED_HINTS ");
      sb.append("       OPTIMIZER_FEATURES_ENABLE('10.1.0.5') ");
      sb.append("       DB_VERSION('12.1.0.2') ");
      sb.append("       OPT_PARAM('_fix_control' '20243268:1 16732417:1 26664361:7') ");
      sb.append("       ALL_ROWS ");
      sb.append("       OUTLINE_LEAF(@\"SEL$3\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$4\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$1\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$2\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$6\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$7\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$2\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$5\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$9\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$10\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$3\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$8\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$FEC66756\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$3461DC8E\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$42DA16AB\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$6071500D\") ");
      sb.append("       OLD_PUSH_PRED(@\"SEL$1\" \"C\"@\"SEL$1\" (\"VWM_BILLABLE_CHARGE\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       OUTLINE_LEAF(@\"SEL$354EC8B6\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$620EB3CF\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$013C4B19\") ");
      sb.append("       OLD_PUSH_PRED(@\"SEL$1\" \"D\"@\"SEL$1\" (\"CM_B_CHG_LINE\".\"BILLABLE_CHG_ID\" \"CM_B_CHG_LINE\".\"LINE_SEQ\")) ");
      sb.append("       OUTLINE_LEAF(@\"SEL$A9CAA6A0\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$FE9B0E79\") ");
      sb.append("       OUTLINE_LEAF(@\"SET$CA1A952B\") ");
      sb.append("       OLD_PUSH_PRED(@\"SEL$1\" \"E\"@\"SEL$1\" (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\" \"CM_B_LN_CHAR\".\"LINE_SEQ\") (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\"  ");
      sb.append("               \"CM_B_LN_CHAR\".\"LINE_SEQ\")) ");
      sb.append("       OUTLINE_LEAF(@\"SEL$1\") ");
      sb.append("       OUTLINE_LEAF(@\"SEL$18\") ");
      sb.append("       OUTLINE(@\"SEL$11\") ");
      sb.append("       OUTLINE(@\"SEL$12\") ");
      sb.append("       OUTLINE(@\"SEL$13\") ");
      sb.append("       OUTLINE(@\"SET$4\") ");
      sb.append("       OUTLINE(@\"SEL$1\") ");
      sb.append("       OUTLINE(@\"SEL$14\") ");
      sb.append("       OUTLINE(@\"SEL$15\") ");
      sb.append("       OUTLINE(@\"SET$5\") ");
      sb.append("       OUTLINE(@\"SEL$16\") ");
      sb.append("       OUTLINE(@\"SEL$17\") ");
      sb.append("       OUTLINE(@\"SET$6\") ");
      sb.append("       NO_ACCESS(@\"SEL$18\" \"TBL\"@\"SEL$18\") ");
      sb.append("       INDEX_RS_ASC(@\"SEL$1\" \"B\"@\"SEL$1\" (\"CI_BSEG\".\"BILL_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$1\" \"A\"@\"SEL$1\" (\"CI_BSEG_CALC\".\"BSEG_ID\" \"CI_BSEG_CALC\".\"HEADER_SEQ\")) ");
      sb.append("       NO_ACCESS(@\"SEL$1\" \"C\"@\"SEL$1\") ");
      sb.append("       NO_ACCESS(@\"SEL$1\" \"D\"@\"SEL$1\") ");
      sb.append("       NO_ACCESS(@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("       LEADING(@\"SEL$1\" \"B\"@\"SEL$1\" \"A\"@\"SEL$1\" \"C\"@\"SEL$1\" \"D\"@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("       USE_NL(@\"SEL$1\" \"A\"@\"SEL$1\") ");
      sb.append("       USE_NL(@\"SEL$1\" \"C\"@\"SEL$1\") ");
      sb.append("       USE_NL(@\"SEL$1\" \"D\"@\"SEL$1\") ");
      sb.append("       USE_NL(@\"SEL$1\" \"E\"@\"SEL$1\") ");
      sb.append("       INDEX(@\"SEL$FE9B0E79\" \"CM_B_LN_CHAR\"@\"SEL$17\" (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\" \"CM_B_LN_CHAR\".\"LINE_SEQ\"  ");
      sb.append("               \"CM_B_LN_CHAR\".\"CHAR_TYPE_CD\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$A9CAA6A0\" \"VWM_BILLABLE_CHARGE_LINE_CHAR\"@\"SEL$16\" (\"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"BILLABLE_CHG_ID\"  ");
      sb.append("               \"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"LINE_SEQ\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$620EB3CF\" \"CM_B_CHG_LINE\"@\"SEL$15\" (\"CM_B_CHG_LINE\".\"BILLABLE_CHG_ID\" \"CM_B_CHG_LINE\".\"LINE_SEQ\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$354EC8B6\" \"VWM_BILLABLE_CHARGE_LINE\"@\"SEL$14\" (\"VWM_BILLABLE_CHARGE_LINE\".\"BILLABLE_CHG_ID\"  ");
      sb.append("               \"VWM_BILLABLE_CHARGE_LINE\".\"LINE_SEQ\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$42DA16AB\" \"VWM_BILLABLE_CHARGE\"@\"SEL$13\" (\"VWM_BILLABLE_CHARGE\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$3461DC8E\" \"MIG_BILL_CHG\"@\"SEL$12\" (\"MIG_BILL_CHG\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$FEC66756\" \"CM_BILL_CHG\"@\"SEL$11\" (\"CM_BILL_CHG\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       NO_ACCESS(@\"SEL$8\" \"LN\"@\"SEL$8\") ");
      sb.append("       NO_ACCESS(@\"SEL$5\" \"MAP\"@\"SEL$5\") ");
      sb.append("       NO_ACCESS(@\"SEL$2\" \"MAP\"@\"SEL$2\") ");
      sb.append("       INDEX_RS_ASC(@\"SEL$4\" \"MIG_TXN_ATTRIBUTES_MAP\"@\"SEL$4\" (\"MIG_TXN_ATTRIBUTES_MAP\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$3\" \"VWM_BILLABLE_CHARGE_ATTRIBUTES\"@\"SEL$3\" (\"VWM_BILLABLE_CHARGE_ATTRIBUTES\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$7\" \"MIG_TXN_ATTRIBUTES_MAP\"@\"SEL$7\" (\"MIG_TXN_ATTRIBUTES_MAP\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$6\" \"VWM_BILLABLE_CHARGE_ATTRIBUTES\"@\"SEL$6\" (\"VWM_BILLABLE_CHARGE_ATTRIBUTES\".\"BILLABLE_CHG_ID\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$10\" \"CM_B_LN_CHAR\"@\"SEL$10\" (\"CM_B_LN_CHAR\".\"BILLABLE_CHG_ID\" \"CM_B_LN_CHAR\".\"LINE_SEQ\"  ");
      sb.append("               \"CM_B_LN_CHAR\".\"CHAR_TYPE_CD\")) ");
      sb.append("       INDEX_RS_ASC(@\"SEL$9\" \"VWM_BILLABLE_CHARGE_LINE_CHAR\"@\"SEL$9\" (\"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"BILLABLE_CHG_ID\"  ");
      sb.append("               \"VWM_BILLABLE_CHARGE_LINE_CHAR\".\"LINE_SEQ\")) ");
      sb.append("       END_OUTLINE_DATA ");
      sb.append("   */ ");
      sb.append("PRICEITEM_CD, PRICE_ASGN_ID, SUM(PRODTOTAL) AS BSEGTOTAL , ");
      sb.append(
          "SUM(CHARGEAMT) AS PRODTOTAL, MAX(BSEG_ID) AS BSEG_ID, DST_ID, SETT_LEVEL_GRANULARITY FROM TBL ");
      sb.append("GROUP BY PRICEITEM_CD, PRICE_ASGN_ID, DST_ID, SETT_LEVEL_GRANULARITY  ");

      preparedStatement = createPreparedStatement(sb.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("bclCharType", bclCharType);
      preparedStatement.setAutoclose(false);
      prodList = preparedStatement.list();

    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }

    if (prodList != null) {

      for (SQLResultRow resultSet : prodList) {
    	  priceItemCode = resultSet.getString("PRICEITEM_CD");
        if (maxSeqNum == null) {
          maxSeqNum = BigInteger.ONE;
        }
        else {
           maxSeqNum = maxSeqNum.add(BigInteger.ONE);
        }
        if (notBlank(priceItemCode)) {
          insertOrUpdateRunningTotalChar(maxSeqNum, resultSet,"INSERT");
         }
      }
    }
  }

  /**
   * Correct the amounts on the bill segments and FTs by retrieving the precise difference between
   * the actual and billed amounts
   */
  private void correctBillAmounts() {

    BigDecimal diffAmt = BigDecimal.ZERO;
    BigInteger currencyExponent =
        billId.getEntity().getAccount().getCurrency().getDecimalPositions();
    BigDecimal runningTotalAmt = BigDecimal.ZERO;
    BigDecimal bsegTotalAmt = BigDecimal.ZERO;
    String bsegId = null;
    String dstId = "";
    List<SQLResultRow> runningTotalList = null;

    // ***********************Retrieve list of products charged on bill and
    // one bill segment id against each***********//
    PreparedStatement preparedStatement = null;
    StringBuilder sb;
    try {
      sb = new StringBuilder();
      sb.append("SELECT ROUND(C.ADHOC_CHAR_VAL, :currencyExponent) AS RUNNINGTOTAL, ");
      sb.append(
          "ROUND(TO_NUMBER(C.CHAR_VAL_FK2), :currencyExponent) AS BSEGTOTAL, C.CHAR_VAL_FK3 AS BSEG_ID, C.CHAR_VAL_FK4 AS DST_ID ");
      sb.append("FROM CI_BILL_CHAR C ");
      sb.append("WHERE C.BILL_ID = :billId ");
      sb.append("AND C.CHAR_TYPE_CD = :runningTotalCharTypeId ");
      preparedStatement = createPreparedStatement(sb.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("runningTotalCharTypeId", runningTotalCharTypeId);
      preparedStatement.bindBigInteger("currencyExponent", currencyExponent);
      preparedStatement.setAutoclose(false);
      runningTotalList = preparedStatement.list();

    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }
    if (notNull(runningTotalList)) {
      for (SQLResultRow resultSet : runningTotalList) {
        diffAmt = BigDecimal.ZERO;
        bsegId = null;
        runningTotalAmt = resultSet.getBigDecimal("RUNNINGTOTAL");
        bsegTotalAmt = resultSet.getBigDecimal("BSEGTOTAL");
        bsegId = resultSet.getString("BSEG_ID");
        dstId = resultSet.getString("DST_ID");
        // ********************determine precision difference per
        // product*************************//
        diffAmt = runningTotalAmt.subtract(bsegTotalAmt);
        if (diffAmt.compareTo(BigDecimal.ZERO) != 0) {
          updateBsAmounts(bsegId, diffAmt, dstId);
        }

      }
      // end of for loop
    }
  }

  private String getTimestamp(Bill_Id billId) {
    PreparedStatement preparedStatement = null;
    StringBuilder sb2;

    String time = null;

    try {
      preparedStatement = null;

      sb2 = new StringBuilder();
      sb2.append("select ADHOC_CHAR_VAL from CI_BILL_CHAR ");
      sb2.append("where bill_id=:billId and CHAR_TYPE_CD=:runningTotalTimestamp ");
      preparedStatement = createPreparedStatement(sb2.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.bindId("runningTotalTimestamp", runningTotalTimestampCharacteristic);
      preparedStatement.setAutoclose(false);
      SQLResultRow timeStamp = preparedStatement.firstRow();

      if (notNull(timeStamp)) {

        time = timeStamp.getString("ADHOC_CHAR_VAL");

      }
    }

    catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }

    if (isNull(time)) {

      try {
        preparedStatement = null;
        sb2 = new StringBuilder();

        sb2.append("Insert into CI_BILL_CHAR (BILL_ID,CHAR_TYPE_CD,SEQ_NUM, ");
        sb2.append("VERSION,CHAR_VAL,ADHOC_CHAR_VAL,CHAR_VAL_FK1, ");
        sb2.append("CHAR_VAL_FK2,CHAR_VAL_FK3,CHAR_VAL_FK4, ");
        sb2.append("CHAR_VAL_FK5,SRCH_CHAR_VAL) values (:billId, ");
        sb2.append(":runningTotalTimestamp,1,1,'                ', ");
        sb2.append("' ',' ',' ',' ',' ',' ',' ') ");

        preparedStatement = createPreparedStatement(sb2.toString(), "");
        preparedStatement.bindId("billId", billId);
        preparedStatement.bindId("runningTotalTimestamp", runningTotalTimestampCharacteristic);
        preparedStatement.setAutoclose(false);
        preparedStatement.executeUpdate();
      }

      catch (Exception e) {
        logger.error(EXCEPTION_MESSAGE, e);
      } finally {
        if (preparedStatement != null) {
          preparedStatement.close();
          preparedStatement = null;
        }
      }

    }

    return time;

  }

  private boolean getBillSegments(Bill_Id billId) {
    PreparedStatement preparedStatement = null;
    StringBuilder sb2 = null;
    boolean bsegExists = false;
    try {
      preparedStatement = null;
      sb2 = new StringBuilder();

      sb2.append(" SELECT 1 AS COUNT FROM CI_BSEG ");
      sb2.append(" WHERE BILL_ID = :billId ");

      preparedStatement = createPreparedStatement(sb2.toString(), "");
      preparedStatement.bindId("billId", billId);
      preparedStatement.setAutoclose(false);
      SQLResultRow bsegCount = preparedStatement.firstRow();

      if (notNull(bsegCount)) {
        bsegExists = true;
      }
    }

    catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (preparedStatement != null) {
        preparedStatement.close();
        preparedStatement = null;
      }
    }
    return bsegExists;
  }

  /**
   * @param billSegId
   * @param totalAmt
   * @param dstId
   */
  private void updateBsAmounts(String billSegId, BigDecimal totalAmt, String dstId) {

    FinancialTransaction_Id ftId = null;
    String saTypeDstId = null;
    BillSegment_Id bsegId = null;
    PreparedStatement ftQuery = null;
    PreparedStatement ftUpdate = null;
    PreparedStatement ftGLUpdate = null;
    PreparedStatement updateBsCalc = null;
    PreparedStatement updateBsCalcLn = null;
    bsegId = new BillSegment_Id(billSegId.trim());
    try {
      StringBuilder sb = new StringBuilder();
      sb.append(
          "select ft.FT_ID, st.DST_ID from ci_ft ft, ci_sa sa, ci_sa_type st where sibling_id=:bsegId ");
      sb.append(
          "and ft_type_flg = :ftTypeFlg and st.sa_type_cd = sa.sa_type_cd and ft.sa_id = sa.sa_id ");
      sb.append("and st.cis_division = ft.cis_division ");
      ftQuery = createPreparedStatement(sb.toString(), "");
      ftQuery.bindId("bsegId", bsegId);
      ftQuery.bindLookup("ftTypeFlg", FinancialTransactionTypeLookup.constants.BILL_SEGMENT);
      ftQuery.setAutoclose(false);
      SQLResultRow ftRow = ftQuery.firstRow();
      if (notNull(ftRow)) {
        ftId = (FinancialTransaction_Id) ftRow.getId("FT_ID", FinancialTransaction.class);
        saTypeDstId = ftRow.getString("DST_ID");
      }
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_UPDATE_FT, e);
    } finally {
      if (ftQuery != null) {
        ftQuery.close();
      }
    }
    // to get the DST_id from ci_ft_gl
    getDistIdFromFtgl(ftId, saTypeDstId, saTypeDstId);
    try {
      StringBuilder sb = new StringBuilder();
      sb.append("UPDATE CI_BSEG_CALC_LN SET CALC_AMT=CALC_AMT+(:totalAmt) ");
      sb.append("WHERE BSEG_ID =:bsegId AND DST_ID=:dstId AND ROWNUM=1 ");
      updateBsCalcLn = createPreparedStatement(sb.toString(), "");
      updateBsCalcLn.bindId("bsegId", bsegId);
      updateBsCalcLn.bindBigDecimal("totalAmt", totalAmt);
      updateBsCalcLn.bindString("dstId", dstId.trim(), "DST_ID");
      updateBsCalcLn.setAutoclose(false);
      updateBsCalcLn.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_MESSAGE, e);
    } finally {
      if (updateBsCalcLn != null) {
        updateBsCalcLn.close();
      }
    }
    try {
      updateBsCalc = createPreparedStatement(
          "UPDATE CI_BSEG_CALC SET CALC_AMT=CALC_AMT+(:totalAmt) " + "WHERE BSEG_ID =:bsegId ", "");
      updateBsCalc.bindId("bsegId", bsegId);
      updateBsCalc.bindBigDecimal("totalAmt", totalAmt);
      updateBsCalc.setAutoclose(false);
      updateBsCalc.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_UPDATE_FT, e);
    } finally {
      if (updateBsCalc != null) {
        updateBsCalc.close();
      }
    }
    try {
      ftUpdate = createPreparedStatement("UPDATE CI_ft SET CUR_AMT=CUR_AMT+(:totalAmt), "
          + "TOT_AMT=TOT_AMT+(:totalAmt) WHERE FT_ID=:ftId", "");
      ftUpdate.bindId("ftId", ftId);
      ftUpdate.bindBigDecimal("totalAmt", totalAmt);
      ftUpdate.setAutoclose(false);
      ftUpdate.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_UPDATE_FT, e);
    } finally {
      if (ftUpdate != null) {
        ftUpdate.close();
      }
    }
    try {
      ftGLUpdate = createPreparedStatement(
          "UPDATE CI_FT_GL SET AMOUNT=AMOUNT+(:totalAmt) " + "WHERE FT_ID=:ftId and TOT_AMT_SW='Y'",
          "");
      ftGLUpdate.bindId("ftId", ftId);
      ftGLUpdate.bindBigDecimal("totalAmt", totalAmt);
      ftGLUpdate.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_UPDATE_FT, e);
    } finally {
      if (ftGLUpdate != null) {
        ftGLUpdate.close();
      }
    }
    try {
      ftGLUpdate = createPreparedStatement("UPDATE CI_FT_GL SET AMOUNT=AMOUNT-(:totalAmt) "
          + "WHERE FT_ID=:ftId and TOT_AMT_SW!='Y' AND DST_ID!=:dstId AND rownum=1 ", "");
      ftGLUpdate.bindId("ftId", ftId);
      ftGLUpdate.bindBigDecimal("totalAmt", totalAmt);
      ftGLUpdate.bindString("dstId", getTaxDistributionCode().trim(), "DST_ID");
      ftGLUpdate.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_UPDATE_FT, e);
    } finally {
      if (ftGLUpdate != null) {
        ftGLUpdate.close();
      }
    }
  }

  public void getDistIdFromFtgl(FinancialTransaction_Id ftId, String saTypeDstId, String dstId) {
    PreparedStatement ftQuery = null;
    List<SQLResultRow> resultList = null;
    try {

      List<String> distIdList = new ArrayList<>();
      ftQuery = createPreparedStatement("SELECT DST_ID FROM CI_FT_GL WHERE FT_ID = :ftId ", "");
      ftQuery.bindId("ftId", ftId);
      ftQuery.setAutoclose(false);
      resultList = ftQuery.list();
      if (resultList.isEmpty()) {
        createFtGlDstLineDROrCR(ftId, saTypeDstId,"Y");//'Y' for DR(debit)
        createFtGlDstLineDROrCR(ftId, dstId,"N");
      } else {
        for (SQLResultRow rs : resultList) {
          distIdList.add(rs.get("DST_ID").toString().trim());
        }
        if (!distIdList.contains(saTypeDstId.trim())) {
          createFtGlDstLineDROrCR(ftId, saTypeDstId,"Y");
        }
        if (!distIdList.contains(dstId.trim())) {
          createFtGlDstLineDROrCR(ftId, dstId,"N");
        }
      }
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_SELECT_DST_ID, e);
    } finally {
      if (ftQuery != null) {
        ftQuery.close();
      }
    }


  }

  /**
   * @param ftId
   * @param dstId
   */
  private void createFtGlDstLineDROrCR(FinancialTransaction_Id ftId, String dstId, String modeOfTxn) {
    PreparedStatement ftGlInsert = null;

    try {
      StringBuilder sb = new StringBuilder();
      sb.append(" Insert into CI_FT_GL (FT_ID,GL_SEQ_NBR,DST_ID,CHAR_TYPE_CD,AMOUNT,CHAR_VAL, ");
      sb.append(
          " TOT_AMT_SW,VERSION,STATISTIC_AMOUNT,GL_ACCT,VALIDATE_SW,GLA_VAL_DT,FT_GL_CATEGORY,GLAT_ID,SEQNO) values (");
      sb.append(
          " :ftId, (SELECT NVL(MAX(GL_SEQ_NBR),0)+1 FROM CI_FT_GL WHERE FT_ID = :ftId), :dstId, ' ', 0, ' ',");
      sb.append(":modeOfTxn, 1, 0, ' ', ' ', null, ' ', ' ',0)");
      ftGlInsert = createPreparedStatement(sb.toString(), "");
      ftGlInsert.bindId("ftId", ftId);
      ftGlInsert.bindString("dstId", dstId, "DST_ID");
      ftGlInsert.bindString("modeOfTxn", modeOfTxn, "TOT_AMT_SW");
      ftGlInsert.executeUpdate();
    } catch (Exception e) {
      logger.error(EXCEPTION_IN_FTGL_INSERT, e);
    } finally {
      if (ftGlInsert != null) {
        ftGlInsert.close();
      }
    }
  }

  @Override
  public String getAccountId() {
    return null;
  }

  @Override
  public void setAccountId(String paramString) {
    // empty
  }

  @Override
  public String getBillCycCode() {
    return billCycCode;
  }

  @Override
  public void setBillCycCode(String paramString) {
    billCycCode = paramString;
  }

  public String getBillId() {
    return parameterBillid;
  }

  public void setBillId(String parameterBillId) {
    parameterBillid = parameterBillId;
  }

  @Override
  public String getCisDivision() {
    return null;
  }

  @Override
  public void setCisDivision(String paramString) {
    // empty
  }

  @Override
  public Date getEndDate() {
    return null;
  }

  @Override
  public void setEndDate(Date arg0) {
    // empty
  }

  @Override
  public String getSaId() {
    return null;
  }

  @Override
  public void setSaId(String paramString) {
    // empty
  }

  @Override
  public String getSaTypeCode() {
    // empty
    return null;
  }

  @Override
  public void setSaTypeCode(String paramString) {
    // empty
  }

  @Override
  public Date getStartDate() {
    return null;
  }

  @Override
  public void setStartDate(Date arg0) {
    // empty
  }

  @Override
  public Bool getSkipSw() {
    return null;
  }

  @Override
  public void setSkipSw(Bool paramBool) {
    // empty
  }

  @Override
  public String getBsbsxAccountId() {
    // empty
    return null;
  }

  @Override
  public void setBsbsxAccountId(String paramString) {
    // empty
  }

  @Override
  public Date getAccountingDate() {
    // empty
    return null;
  }

  @Override
  public void setAccountingDate(Date paramDate) {
    // empty
  }

  @Override
  public Bool getAllowEstSw() {
    return null;
  }

  @Override
  public void setAllowEstSw(Bool paramBool) {
    // empty
  }

  @Override
  public Bool getAsgnSeqNbrSw() {
    // empty..
    return null;
  }

  @Override
  public void setAsgnSeqNbrSw(Bool paramBool) {
    // empty
  }

  @Override
  public String getBsbsxBillCycCode() {
    // empty
    return null;
  }

  @Override
  public void setBsbsxBillCycCode(String paramString) {
    // empty
  }

  @Override
  public Date getBillDate() {
    // empty
    return null;
  }

  @Override
  public void setBillDate(Date paramDate) {
    // empty
  }

  public String getBsbsxBillId() {
    return null;
  }

  public void setBsbsxBillId(String paramString) {
    // empty
  }

  @Override
  public Bool getBillOptSw() {
    return null;
  }

  @Override
  public void setBillOptSw(Bool paramBool) {
    // empty
  }

  @Override
  public Date getCutOffDate() {
    return null;
  }

  @Override
  public void setCutOffDate(Date paramDate) {
    // empty
  }

  @Override
  public Date getBsbsxEndDate() {
    // empty
    return null;
  }

  @Override
  public void setBsbsxEndDate(Date paramDate) {
    // empty
  }

  @Override
  public Date getEstDate() {
    return null;
  }

  @Override
  public void setEstDate(Date paramDate) {
    // empty
  }

  @Override
  public Bool getFreezeCmpSw() {
    return freezeCmpSw;
  }

  @Override
  public void setFreezeCmpSw(Bool paramBool) {
    freezeCmpSw = paramBool;
  }

  @Override
  public String getBsbsxSaId() {
    return null;
  }

  @Override
  public void setBsbsxSaId(String paramString) {
    // empty
  }

  @Override
  public Date getBsbsxStartDate() {
    return null;
  }

  @Override
  public void setBsbsxStartDate(Date paramDate) {
    // empty
  }

  @Override
  public void setUpdateSw(String paramString) {
    // empty
  }

  @Override
  public String getUpdateSw() {
    return null;
  }

  @Override
  public void setBillCompleteSw(Bool paramBool) {
    // empty
  }

  @Override
  public Bool getBillCompleteSw() {
    return null;
  }

  @Override
  public void setMidTrxCommitSw(Bool paramBool) {
    // empty
  }

  @Override
  public Bool getMidTrxCommitSw() {
    return null;
  }

  public void setBillGenType(String paramString) {
    // empty
  }

  public String getBillGenType() {
    // empty
    return null;
  }

  public void setTrialBillId(String paramString) {
    // empty
  }

  public String getTrialBillId() {
    return null;
  }

  public void setBatchRunNo(BigInteger paramBigInteger) {
    // empty
  }

  public BigInteger getBatchRunNo() {
    return null;
  }



}
