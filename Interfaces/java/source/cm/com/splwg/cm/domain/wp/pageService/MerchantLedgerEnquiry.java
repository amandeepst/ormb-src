/*******************************************************************************
 * FileName                   : MerchantLedgerEnquirys.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : JAN 14, 2015
 * Version Number             : 0.1
 * Revision History     		 : 1.0
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.2      NA             14-JAN-2016         Abhishek Paliwal  Implemented all requirement as in TS version 1.0.
0.3	     NAP-24120 		10-Mar-2018			RIA				 Changed data type of ALT_BILL_ID from decimal to String
0.4      NAP--28224     31-OCT-2018         RIA              Change to show bills based on start date and end date.
0.5      NAP--28224     05-FEB-2019         RIA              Performance Enhancement/ Removal of Strored Procedure
 *******************************************************************************/

package com.splwg.cm.domain.wp.pageService;

import java.util.ArrayList;
import java.util.List;

import com.ibm.icu.util.Calendar;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.base.api.service.DataElement;
import com.splwg.base.api.service.ItemList;
import com.splwg.base.api.service.PageHeader;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.StandardMessages;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.common.StringUtilities;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author paliwala708
 *
 @QueryPage (program = CMMERCHLDGENQ, service = CMMERCHLDGENQ,
 *      body = @DataElement (contents = { @DataField (name = PER_ID_NBR)
 *                  , @ListField (name = Division, owner = CM)
 *                  , @DataField (name = STATUS)
 *                  , @DataField (name = ERROR_MSG)}),
 *      header = { @DataField (name = PER_ID_NBR)
 *            , @DataField (name = CIS_DIVISION)
 *            , @DataField (name = CURRENCY_CD)
 *            , @DataField (name = ACCT_NBR)
 *            , @DataField (name = DESCR)
 *            , @DataField (name = BILL_STAT_FLG)
 *            , @DataField (name = START_DATE)
 *            , @DataField (name = END_DATE)
 *            , @DataField (name = NUM1)},
 *      headerFields = { @DataField (name = PER_ID_NBR)
 *            , @DataField (name = CIS_DIVISION)
 *            , @DataField (name = CURRENCY_CD)
 *            , @DataField (name = ACCT_NBR)
 *            , @DataField (name = DESCR)
 *            , @DataField (name = BILL_STAT_FLG)
 *            , @DataField (name = START_DATE)
 *            , @DataField (name = END_DATE)
 *            , @DataField (name = NUM1)},
 *      lists = { @List (name = Division, size = 100,
 *                  body = @DataElement (contents = { @DataField (name = CIS_DIVISION)
 *                              , @ListField (name = Currency)}))
 *            , @List (name = Currency, size = 100,
 *                  body = @DataElement (contents = { @ListField (name = MerchantLdgAcct)}))
 *            , @List (name = MerchantLdgAcct, size = 100,
 *                  body = @DataElement (contents = { @DataField (name = CUR_AMT)
 *                              , @ListField (name = Account)
 *                              , @ListField (name = FinancialDocument)}))
 *            , @List (name = Account, size = 100,
 *                  body = @DataElement (contents = { @DataField (name = ACCT_ID)
 *                              , @DataField (name = DESCR)
 *                              , @ListField (name = Contract, owner = CM, property = String)}))
 *            , @List (name = Contract, size = 100,
 *                  body = @DataElement (contents = { @DataField (name = SA_ID)
 *                              , @DataField (name = SA_TYPE_CD)
 *                              , @DataField (name = CUR_AMT)
 *                              , @DataField (name = CURRENCY_CD)}))
 *            , @List (name = FinancialDocument, size = 100,
 *                  body = @DataElement (contents = { @DataField (name = BILL_ID)
 *                  			, @DataField (name = SA_TYPE_CD)
 *                              , @DataField (name = CUR_AMT)
 *                              , @DataField (name = CURRENCY_CD)
 *                              , @DataField (name = CIS_DIVISION)
 *                              , @DataField (name = START_DT)
 *                              , @DataField (name = END_DT)
 *                              , @DataField (name = DUE_DT)
 *                              , @DataField (name = BILL_DT)
 *                              , @DataField (name = ALT_BILL_ID)
 *                              , @DataField (name = LINE_ID)
 *                              , @DataField (name = LINE_AMT)
 *                              , @DataField (name = BANK_ENTRY_EVENT_ID)
 *                              , @DataField (name = UNPAID_AMT)
 *                              , @DataField (name = BANKING_ENTRY_STATUS)
 *                              , @DataField (name = NUM1)}))}
 *                              , modules={ })
 */
public class MerchantLedgerEnquiry extends MerchantLedgerEnquiry_Gen {
	Logger logger = LoggerFactory.getLogger(MerchantLedgerEnquiry.class);
	DataElement root = new DataElement();


	private StringBuilder merchntQUERY = new StringBuilder(" SELECT ANBR.ACCT_ID AS ACCOUNT_ID, PER.CIS_DIVISION AS CIS_DIVISION, ACCT.CURRENCY_CD AS CCY,  " +
			" SA.SA_ID AS CONTRACT_ID, ANBR.ACCT_NBR AS ACCOUNT_TYPE, SA.SA_TYPE_CD "+
			" FROM CI_PER_ID PI, CI_PER PER, CI_ACCT_PER APER, CI_ACCT_NBR ANBR, CI_ACCT ACCT, CI_SA SA, CI_SA_CHAR SACHAR "+
			" WHERE PI.PER_ID = APER.PER_ID " +
			" AND PI.PER_ID = PER.PER_ID " +
			" AND APER.ACCT_ID = ANBR.ACCT_ID " +
			" AND ANBR.ACCT_ID = ACCT.ACCT_ID " +
			" AND SACHAR.SA_ID = SA.SA_ID " +
			" AND ACCT.ACCT_ID = SA.ACCT_ID " +
			" AND PER.CIS_DIVISION = NVL(TRIM(:division), PER.CIS_DIVISION) " +
			" AND ANBR.ACCT_NBR = NVL(:acctNbr, ANBR.ACCT_NBR) " +
			" AND ACCT.CURRENCY_CD = NVL(TRIM(:currencyCd), ACCT.CURRENCY_CD) " +
			" AND ANBR.ACCT_NBR_TYPE_CD = 'ACCTTYPE' " +
			" AND SACHAR.CHAR_TYPE_CD = 'SA_ID' " +
			" AND TRIM(SA.SA_TYPE_CD) = TRIM(NVL(TRIM(:saTypeCd), SA.SA_TYPE_CD))" +
			" AND PI.PER_ID_NBR = :perIdNbr ");


	/**
	 * The read() method will take the input as Per Id Number and returns the Merchant
	 * Balances details for that perIdNumber
	 * @param PageHeader
	 * @return DataElement
	 * @exception ApplicationError
	 */
	@Override
	protected DataElement read(PageHeader header) throws ApplicationError {
		Calendar cl=Calendar.getInstance();

		String inputPerIdNbr = getInputHeader().getString(STRUCTURE.HEADER.PER_ID_NBR);
		String inputCisDivision = CommonUtils.CheckNull(getInputHeader().getString(STRUCTURE.HEADER.CIS_DIVISION));
		String inputCurrencyCd = CommonUtils.CheckNull(getInputHeader().getString(STRUCTURE.HEADER.CURRENCY_CD));
		String inputLedgerAccount = CommonUtils.CheckNull(getInputHeader().getString(STRUCTURE.HEADER.ACCT_NBR));
		String inputSubLedgerAccount = CommonUtils.CheckNull(getInputHeader().getString(STRUCTURE.HEADER.DESCR));
		BillStatusLookup inputBillStatFlag = getInputHeader().get(STRUCTURE.HEADER.BILL_STAT_FLG);
		Date inputStartDate = (getInputHeader().get(STRUCTURE.HEADER.START_DATE)==null?new Date(1950, 12, 31):getInputHeader().get(STRUCTURE.HEADER.START_DATE));
		Date inputEndDate = (getInputHeader().get(STRUCTURE.HEADER.END_DATE)==null?new  Date(cl.get(Calendar.YEAR),cl.get(Calendar.MONTH)+1,cl.get(Calendar.DAY_OF_MONTH)):getInputHeader().get(STRUCTURE.HEADER.END_DATE));
		String maxRecString = getInputHeader().getString(STRUCTURE.HEADER.NUM1);
		logger.info("maxRecString = "+maxRecString);
		int inputMaxRec = maxRecString==null||maxRecString.equalsIgnoreCase("")?-1:Integer.parseInt(maxRecString);

		logger.info(" Search String - inputPerIdNbr :" + inputPerIdNbr + " inputCisDivision :" + inputCisDivision +" inputCurrencyCd :" + inputCurrencyCd +
				" inputLedgerAccount :" + inputLedgerAccount + " inputSubLedgerAccount :" + inputSubLedgerAccount + " inputBillStatFlag :" + inputBillStatFlag +
				" inputStartDate :" + inputStartDate + " inputEndDate :" + inputEndDate + " inputMaxRec : "+ inputMaxRec);

		if (StringUtilities.isEmptyOrNull(inputPerIdNbr)) {
			logger.info(" Search String is Missing ");
			addError(StandardMessages.fieldMissing(MerchantLedgerEnquiry_Gen.STRUCTURE.PER_ID_NBR));
		}

		PreparedStatement pst = null, 
		pstFin = null;
		pst = createPreparedStatement(merchntQUERY.toString(),"");
		pst.bindString("perIdNbr", inputPerIdNbr, "PER_ID_NBR");

		pst.bindString("division", inputCisDivision, "CIS_DIVISION");
		pst.bindString("currencyCd", inputCurrencyCd, "CURRENCY_CD");
		pst.bindString("acctNbr", inputLedgerAccount, "ACCT_NBR");
		pst.bindString("saTypeCd", inputSubLedgerAccount, "SA_TYPE_CD");


		String contractId = null;

		String accountType = null;
		String ccy = null;
		String contractType = null;
		Money amount = null;
		Money amountRecr = null;
		String billId=null;
		String lineId=null;
		String cisDiv=null;
		Date dueDate = null;
		String bankEntryEventId = null;
		String altBillId = null;
		Date billDate = null;
		Money lineAmount = null;
		Money unpaidAmount = null;
		Money curAmount = null;
		Money amountAcct = null;
		int count = 1;
		String saTypeCd = null;
		String merchantAcctId=null;

		ItemList<DataElement> divisionList =root.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Division.name);

		ItemList<DataElement> currencyList =null; 
		ItemList<DataElement> merchantLdgAcctList = null ;
		ItemList<DataElement> accountList = null;
		ItemList<DataElement> contractList = null;
		ItemList<DataElement> financialDocumentList = null;
		ArrayList<String> merchantAcctIdList = new ArrayList<String>();

		logger.info(pst);
		List<SQLResultRow> merchantAcctList = pst.list();
		try{
			if(merchantAcctList.isEmpty()){
				root.put(STRUCTURE.STATUS, "Error");
				root.put(STRUCTURE.ERROR_MSG, "Matching merchant entity could not be identified");
			}

			boolean isFinancialqueryExec = false;
			for (SQLResultRow rs : merchantAcctList) {

				PreparedStatement preparedStatement = null;

				try {
					StringBuilder sb = new StringBuilder();


					sb.append(" SELECT SUM(CUR_AMT) AS AMOUNT FROM CI_FT FT, CI_SA SA, CI_SA_CHAR SACHAR ");
					sb.append(" WHERE SA.SA_ID=FT.SA_ID ");
					sb.append(" AND SA.SA_ID=SACHAR.SA_ID AND SA.ACCT_ID =  :acctID ");
					sb.append(" AND SACHAR.CHAR_TYPE_CD='SA_ID' ");


					preparedStatement = createPreparedStatement(sb.toString(),"");
					preparedStatement.bindString("acctID", rs.getString("ACCOUNT_ID"), "ACCT_ID");
					preparedStatement.setAutoclose(false);

					SQLResultRow amountFtDetails = preparedStatement.firstRow();
					amountAcct = amountFtDetails.getMoney("AMOUNT");
				}

				catch (Exception e) {
					logger.error("Exception in query" , e);
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
				try {
					StringBuilder sb = new StringBuilder();
					sb.append("select sum(CUR_AMT) AS AMOUNT from CI_FT where SA_ID =:contractId and REDUNDANT_SW='N' ");

					preparedStatement = createPreparedStatement(sb.toString(),"");
					preparedStatement.bindString("contractId", rs.getString("CONTRACT_ID"), "SA_ID");
					preparedStatement.setAutoclose(false);

					SQLResultRow amountFtDetails = preparedStatement.firstRow();
					amount = amountFtDetails.getMoney("AMOUNT");
				}

				catch (Exception e) {
					logger.error("Exception in query" , e);
				} finally {
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
				root.put(STRUCTURE.PER_ID_NBR, inputPerIdNbr);
				root.put(STRUCTURE.STATUS, "Success");

				boolean isDivisionChanged = false;
				if(cisDiv == null || !cisDiv.equalsIgnoreCase(rs.getString("CIS_DIVISION"))){
					cisDiv = rs.getString("CIS_DIVISION");
					DataElement parmElemDivision = divisionList.newDataElement();
					parmElemDivision.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Division.CIS_DIVISION, cisDiv);
					currencyList = parmElemDivision.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Currency.name);
					isDivisionChanged = true;
				}

				boolean isCurrencyChanged = false;
				if(isDivisionChanged){
					DataElement parmElemCurrency = currencyList.newDataElement();
					merchantLdgAcctList = parmElemCurrency.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_MerchantLdgAcct.name);
					isCurrencyChanged = true;
				}

				boolean isMerchLdgrAcctTypeChanged = false;
				if(accountType == null || !accountType.equalsIgnoreCase(rs.getString("ACCOUNT_TYPE")) || isCurrencyChanged){
					accountType = rs.getString("ACCOUNT_TYPE");

					DataElement parmElemMerchLdgAcct = merchantLdgAcctList.newDataElement();
					parmElemMerchLdgAcct.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_MerchantLdgAcct.CUR_AMT, amountAcct);  // Account level amount
					accountList = parmElemMerchLdgAcct.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Account.name);
					financialDocumentList = parmElemMerchLdgAcct.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.name); 
					isMerchLdgrAcctTypeChanged = true;
				}

				boolean isAccountParentChanged = false;
				if(merchantAcctId == null || !merchantAcctId.equalsIgnoreCase(rs.getString("ACCOUNT_ID")) || isMerchLdgrAcctTypeChanged){
					merchantAcctId = rs.getString("ACCOUNT_ID");
					ccy = rs.getString("CCY");
					DataElement parmElemAcctId = accountList.newDataElement();
					parmElemAcctId.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Account.ACCT_ID, merchantAcctId);
					parmElemAcctId.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Account.DESCR, accountType);//SA_TYPE_CD  //Acct_type
					contractList = parmElemAcctId.newList(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Contract.name);
					isAccountParentChanged = true;
				}

				if("RECR".equalsIgnoreCase(rs.getString("SA_TYPE_CD").trim())){
					amountRecr = amount;
				}
				else if(contractId == null || !contractId.equalsIgnoreCase(rs.getString("CONTRACT_ID")) || isAccountParentChanged){
					contractId = rs.getString("CONTRACT_ID");
					contractType = rs.getString("SA_TYPE_CD");

					if("CHRG".equalsIgnoreCase(contractType.trim())){
						if(amount!=null && amountRecr!=null){
							amount = amount.add(amountRecr);
						}
						else if(amountRecr!=null){
							amount = amountRecr;
						}
					}
					DataElement parmElemContract = contractList.newDataElement();
					parmElemContract.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Contract.SA_ID, contractId);
					parmElemContract.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Contract.SA_TYPE_CD, rs.getString("SA_TYPE_CD"));
					parmElemContract.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Contract.CUR_AMT, amount);
					parmElemContract.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_Contract.CURRENCY_CD, ccy);
				}

				if(isMerchLdgrAcctTypeChanged){
					isFinancialqueryExec = true;
				}

				if(!merchantAcctIdList.contains(merchantAcctId)){
					merchantAcctIdList.add(merchantAcctId);


					try{
						StringBuilder sb = new StringBuilder();
						pst = null; 
						sb.append("SELECT DISTINCT D1.BILL_ID AS BILL_ID, D1.ALT_BILL_ID AS ALT_BILL_ID, D1.BILL_DT AS BILL_DT, DTL.LINE_ID AS LINE_ID ");
						sb.append("FROM CM_BILL_PAYMENT_DTL DTL, CI_BILL D1 , CM_BILL_DUE_DT DUEDT ");
						sb.append("WHERE DTL.BILL_ID = D1.BILL_ID ");
						sb.append("AND DTL.BILL_ID = DUEDT.BILL_ID ");
						sb.append("AND DTL.LINE_ID = DUEDT.LINE_ID ");
						sb.append("AND D1.ACCT_ID = :merchantAcctId AND D1.BILL_STAT_FLG='C' ");
						sb.append("AND DUEDT.IS_MERCH_BALANCED = 'N' ");
						sb.append("AND DTL.UNPAID_AMT <>0  ");
						sb.append("AND D1.BILL_DT BETWEEN :startDt AND :endDt ");
						sb.append("AND DTL.OVERPAID is null ORDER BY D1.BILL_ID ");

						pst = createPreparedStatement(sb.toString(),"");
						pst.bindString("merchantAcctId", merchantAcctId, " ");
						pst.bindDate("startDt", inputStartDate);
						pst.bindDate("endDt", inputEndDate);
						List<SQLResultRow> billDtl = pst.list();

						for (SQLResultRow billDetails : billDtl){

							billId = billDetails.getString("BILL_ID");
							lineId = billDetails.getString("LINE_ID");
							billDate = billDetails.getDate("BILL_DT");
							altBillId = billDetails.getString("ALT_BILL_ID");

							try{
								StringBuilder stringBuilder = new StringBuilder();
								pst = null;

								stringBuilder.append("SELECT DTL.LINE_AMT AS LINE_AMT, DTL.UNPAID_AMT AS UNPAID_AMT, BCHAR.ADHOC_CHAR_VAL as CUR_AMT, ");
								stringBuilder.append("DUEDT.BANK_ENTRY_EVENT_ID AS BANK_ENTRY, DUEDT.DUE_DT AS DUE_DT ");
								stringBuilder.append("FROM CM_BILL_PAYMENT_DTL DTL,  CI_BILL_CHAR BCHAR , CM_BILL_DUE_DT DUEDT ");
								stringBuilder.append("WHERE  DTL.BILL_ID=DUEDT.BILL_ID ");
								stringBuilder.append("AND  DTL.LINE_ID=DUEDT.LINE_ID ");
								stringBuilder.append("AND BCHAR.BILL_ID = DTL.BILL_ID  ");
								stringBuilder.append("AND DTL.BILL_ID = :billId AND DTL.LINE_ID = :lineId ");
								stringBuilder.append("AND BCHAR.CHAR_TYPE_CD = 'BILL_AMT' ");
								stringBuilder.append("ORDER BY CASE WHEN BANKING_ENTRY_STATUS = 'OVERDUE' THEN 1 ELSE "); 
								stringBuilder.append("DTL.PAY_DTL_ID END DESC, DUEDT.PAY_DT DESC");
								
								pst = createPreparedStatement(stringBuilder.toString(),"MerchantQuery");
								pst.bindString("billId", billId, "BILL_ID");
								pst.bindString("lineId", lineId, "LINE_ID");

								SQLResultRow amountdetails = pst.firstRow();
								lineAmount =  amountdetails.getMoney("LINE_AMT");
								unpaidAmount = amountdetails.getMoney("UNPAID_AMT");
								curAmount = new Money(amountdetails.getString("CUR_AMT"));
								bankEntryEventId = amountdetails.getString("BANK_ENTRY");
								dueDate = amountdetails.getDate("DUE_DT");
							}

							catch (Exception e) {
								logger.error("Exception in query" , e);
							} finally {
								if (pst != null) {
									pst.close();
									pst = null;
								}
							}

							try{
								StringBuilder stringBuild = new StringBuilder();
								pst = null;

								stringBuild.append("SELECT SA.SA_TYPE_CD AS SA_TYPE_CD FROM CI_BILL_SA BSA, CI_SA SA ");
								stringBuild.append("WHERE BSA.SA_ID = SA.SA_ID ");
								stringBuild.append("AND BSA.BILL_ID =:billId  ");

								pst = createPreparedStatement(stringBuild.toString(),"MerchantQuery");
								pst.bindString("billId", billId, "BILL_ID");

								SQLResultRow contractDetails = pst.firstRow();

								saTypeCd = contractDetails.getString("SA_TYPE_CD");
							}

							catch (Exception e) {
								logger.error("Exception in query" , e);
							} finally {
								if (pst != null) {
									pst.close();
									pst = null;
								}
							}
							
							if(notNull(saTypeCd)){
								if("RECR".equalsIgnoreCase(saTypeCd.trim())){
									saTypeCd = "CHRG";
								}
							}

							//Financial Document				
							DataElement parmElemFinancialDocument = financialDocumentList.newDataElement();
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.BILL_ID, billId);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.LINE_ID, lineId);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.DUE_DT, dueDate);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.BANK_ENTRY_EVENT_ID, bankEntryEventId);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.ALT_BILL_ID, isBlankOrNull(altBillId)?"0":altBillId); 
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.BILL_DT, billDate);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.LINE_AMT, lineAmount);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.UNPAID_AMT, unpaidAmount);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.CUR_AMT, curAmount);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.SA_TYPE_CD, saTypeCd);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.CURRENCY_CD, ccy);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.CIS_DIVISION, cisDiv);
							parmElemFinancialDocument.put(MerchantLedgerEnquiry_Gen.STRUCTURE.list_FinancialDocument.NUM1, String.valueOf(count));
						}

					}


					catch (Exception e) {
						logger.error("Exception in query" , e);
					} finally {
						if (pst != null) {
							pst.close();
							pst = null;
						}
					}
				}
			}
		}
		catch (Exception e) {
			logger.error("Exception in query" , e);
		}

		finally{
			if(pst != null){
				pst.close();
			}
			if(pstFin != null){
				pstFin.close();
			}
		}
		logger.info("The response in root is = " +root);
		return root;
	}

}