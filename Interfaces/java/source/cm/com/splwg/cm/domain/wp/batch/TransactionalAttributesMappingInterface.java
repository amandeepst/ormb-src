/*******************************************************************************
 * FileName                   : TransactionalAttributesMappingInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Jan 17, 2018
 * Version Number             : 0.8
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Jan 17, 2018		Preeti		 Initial version.
0.2		 NAP-24118		Mar 16, 2018		RIA			 Add tables to Billed Billable Charges MO. Remove ILM_DT & ILM_ARCH_SW from CM_TXN_ATTRIBUTES_MAP.
0.3		 NA 			May 18, 2018		Amandeep     Added Columns for other UDF_CHAR's to accommodate changes for Pricing Structure Change
0.4      NAP-30741      Aug 27, 2018        RIA          Added column SETT_LEVEL_GRANULARITY.
0.5 	 NAP-33633 		Oct 15, 2018		Amandeep	 L0 Product Mapped to CM_TXN_ATTRIBUTES_MAP
0.6		 NAP-36061		Nov 05, 2018		Amandeep	 Company Account Id mapped in Sett Granulaity
0.7      NAP-36537      Dec 04, 2018        RIA          Add parent_acct_id to CM_TXN_ATTRIBUTES_MAP
0.8      NAP-40040      Jan 28, 2019        Vikalp       Add price_asgn_id to CM_TXN_ATTRIBUTES_MAP
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ibm.icu.math.BigDecimal;
import com.splwg.base.api.batch.CommitEveryUnitStrategy;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;
import com.splwg.ccb.api.lookup.FeedSrcFlgLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.billing.billableCharge.BillableCharge_Id;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;

/**
 * @author Preeti
 *
   @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"})
 */
public class TransactionalAttributesMappingInterface extends
TransactionalAttributesMappingInterface_Gen {

	public static final Logger logger = LoggerFactory.getLogger(TransactionalAttributesMappingInterface.class);
	public static final String BLANK="";
	public static final String TTYPE="TTYPE";
	public static final String CTYPE="CTYPE";
	public static final String JTYPE="JTYPE";
	public static final String PTYPE="PTYPE";
	public static final String AUTYPE="AUTYPE";
	public static final String ARTYPE="ARTYPE";
	public static final String STYPE="STYPE";
	public static final String ATYPE="ATYPE";
	public static final String AUTH_EVENT = "AUTH_EVENT";
	public static final String PAYMENT_SCHEME = "PAYMENT_SCHEME";
	public static final String AUTH_TRANSACTION_TYPE = "AUTH_TRANSACTION_TYPE";
	public static final String INTERNAL_AUTH_RESPONSE = "INTERNAL_AUTH_RESPONSE";
	public static final String PARENT_CHARGE_TYPE = "PARENT_CHARGE_TYPE";
	public static final String CHILD_CHARGE_TYPE = "CHILD_CHARGE_TYPE";
	public static final String PARM_STR = "PARM_STR";
	public static final String EQUALS = "=";
	public static final String DELIMITER = "~";
	public static final String BLANK_SPACE = " ";
	public static final String BILLABLE_CHG_ID = "BILLABLE_CHG_ID";
	public static final String TXN_CALC_ID = "TXN_CALC_ID";
	public static final String UDF_CHAR_11 = "UDF_CHAR_11";
	public static final String UDF_CHAR_12 = "UDF_CHAR_12";
	public static final String UDF_CHAR_15 = "UDF_CHAR_15";
	public static final String UDF_CHAR_25 = "UDF_CHAR_25";
	public static final String CHILD_PRODUCT = "CHILD_PRODUCT";
	public static final String ACCT_ID = "ACCT_ID";
	public static final String PRICE_ASGN_ID = "PRICE_ASGN_ID";
	public static final String WIN_END_DT = "WIN_END_DT";
	public static final String UDF_NBR_19 = "UDF_NBR_19";
	public static final String TEXT = "Text:";
	public static final String DESCR = "Description:";
	public static final String ROLLBACK = "Rollback";
	public static final String REF_CHAR = "C1_SAFCD";

	// Default constructor
	public TransactionalAttributesMappingInterface() {
	}	

	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {	
		
		List<ThreadWorkUnit> threadWorkUnitList = getBillableCharges();
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	// *********************** getBillableCharges
	// Method******************************

	/**
	 * getBillableCharges() method
	 * selects set of BILLABLE_CHG_ID from CI_BILL_CHG table created for the day via TFM.
	 * 
	 * @return List BillableChgId
	 */
	private List<ThreadWorkUnit> getBillableCharges() {
		PreparedStatement preparedStatement = null;
		BillableCharges_Id billableChargesId = null;
		
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();

		try {			
			preparedStatement = createPreparedStatement("SELECT A.BILLABLE_CHG_ID FROM CI_BILL_CHG A "
					+ "WHERE A.FEED_SOURCE_FLG =:feedSourceFlg " 
					+ "AND A.CRE_DT =:processDate " 
					+ "AND NOT EXISTS (SELECT B.BILLABLE_CHG_ID FROM CM_TXN_ATTRIBUTES_MAP B " 
					+ "WHERE A.BILLABLE_CHG_ID=B.BILLABLE_CHG_ID)","");
			preparedStatement.bindLookup("feedSourceFlg",FeedSrcFlgLookup.constants.TFM);
			preparedStatement.bindDate("processDate", getProcessDateTime().getDate());
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				String billableChgId = CommonUtils.CheckNull(resultSet.getString(BILLABLE_CHG_ID));
				
				billableChargesId = new BillableCharges_Id(billableChgId);				
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(billableChargesId);
				threadWorkUnitList.add(threadworkUnit);
				
				threadworkUnit = null;
				billableChargesId = null;
			}
		} catch (Exception e) {
			logger.error("Inside catch block of getBillableCharges() method-", e);
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

	public Class<TransactionalAttributesMappingInterfaceWorker> getThreadWorkerClass() {
		return TransactionalAttributesMappingInterfaceWorker.class;
	}

	public static class TransactionalAttributesMappingInterfaceWorker extends
	TransactionalAttributesMappingInterfaceWorker_Gen {

		private String billableChgId = BLANK;	
		private Map<String,String> parmMap = new HashMap<String,String>();

		// Default constructor
		public TransactionalAttributesMappingInterfaceWorker() {
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once per
		 * thread by the framework.
		 */
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			super.initializeThreadWork(arg0);
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			return new CommitEveryUnitStrategy(this);
		}

		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * each row of processing. The selected row for processing is read
		 * (comes as input) and then processed further.
		 */

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			BillableCharges_Id billableChargesId = (BillableCharges_Id) unit.getPrimaryId();	
			billableChgId=billableChargesId.getBillableChgId();

				removeSavepoint(ROLLBACK.concat(getParameters().getThreadCount().toString()));					
				setSavePoint(ROLLBACK.concat(getParameters().getThreadCount().toString()));//Required to nullify the effect of database transactions in case of error scenario
				
				try {					
					// ****************** Create Transaction Attributes Mapping  ******************
					createTxnAttribtesMapping();
				} catch (Exception e) {
					logger.error("Exception in executeWorkUnit: " + e);
					throw new RunAbortedException(CustomMessageRepository
							.exceptionInExecution(e.getMessage()));
				} 
			return true;
		}	

		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */

		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			super.finalizeThreadWork();
		}

		/**
		 * createTxnAttribtesMapping() method 
		 */
		private void createTxnAttribtesMapping() {	
			
			String txnCalcId=BLANK;
			String udfChar11=BLANK;
			String udfChar12=BLANK;
			String udfChar15=BLANK;
			String udfChar25=BLANK;			
			String udfChar1=BLANK;
			String udfChar3=BLANK;
			String udfChar6=BLANK;
			String udfChar7=BLANK;
			String udfChar8=BLANK;
			String udfChar13=BLANK;
			String udfChar14=BLANK;
			String udfChar20=BLANK;
			String acctId = BLANK;
			String childProduct = BLANK;
			String priceAsgnId = BLANK;
			Date winEndDt = null;
			BigDecimal udfNbr19 =BigDecimal.ZERO;
			
			//Added Logic- NAP-36061
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();			
			try {
				stringBuilder.append(" SELECT A.TXN_CALC_ID, B.UDF_CHAR_11, ");
				stringBuilder.append(" B.UDF_CHAR_12, B.UDF_CHAR_15, B.UDF_CHAR_25, B.UDF_NBR_19, C.ACCT_ID, D.WIN_END_DT, BCHG.PRICE_ASGN_ID ");
				stringBuilder.append(" FROM CI_TXN_DTL_PRITM A, CI_TXN_DETAIL B,CI_BILL_CHG BCHG ,CI_ACCT C,CI_SA SA,CI_BILL_CYC_SCH D ");
				stringBuilder.append(" WHERE A.TXN_DETAIL_ID=B.TXN_DETAIL_ID ");
				stringBuilder.append(" AND SA.ACCT_ID = C.ACCT_ID ");
				stringBuilder.append(" AND D.BILL_CYC_CD = C.BILL_CYC_CD ");
				stringBuilder.append(" AND A.BILLABLE_CHG_ID = BCHG.BILLABLE_CHG_ID ");
				stringBuilder.append(" AND SA.SA_ID=NVL((SELECT SA.SA_ID FROM CI_SA_CHAR SACHAR, CI_SA SA WHERE SA.SA_ID = SACHAR.SA_ID AND ");
				stringBuilder.append(" CHAR_TYPE_CD=:saChar AND SRCH_CHAR_VAL=BCHG.SA_ID and SA.SA_STATUS_FLG IN (:active, :pendingStop)),BCHG.SA_ID) ");
				stringBuilder.append(" AND A.BILLABLE_CHG_ID=:billableChgId ");
				stringBuilder.append(" AND B.CURR_SYS_PRCS_DT - 1 BETWEEN D.WIN_START_DT AND D.WIN_END_DT ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),BLANK);
				preparedStatement.bindString("billableChgId", billableChgId.trim(), BILLABLE_CHG_ID);
				preparedStatement.bindId("saChar", new CharacteristicType_Id(REF_CHAR));
				preparedStatement.bindLookup("active", ServiceAgreementStatusLookup.constants.ACTIVE);
				preparedStatement.bindLookup("pendingStop", ServiceAgreementStatusLookup.constants.PENDING_STOP);

				preparedStatement.setAutoclose(false);				
				if(notNull(preparedStatement.firstRow())){
					txnCalcId = CommonUtils.CheckNull( preparedStatement.firstRow().getString(TXN_CALC_ID));
					udfChar11 = CommonUtils.CheckNull(preparedStatement.firstRow().getString(UDF_CHAR_11));
					udfChar12 = CommonUtils.CheckNull(preparedStatement.firstRow().getString(UDF_CHAR_12));
					udfChar15 = CommonUtils.CheckNull(preparedStatement.firstRow().getString(UDF_CHAR_15));
					udfChar25 = CommonUtils.CheckNull(preparedStatement.firstRow().getString(UDF_CHAR_25));
					udfNbr19 = CommonUtils.CheckNullNumber(preparedStatement.firstRow().getBigDecimal((UDF_NBR_19)));
					acctId = CommonUtils.CheckNull(preparedStatement.firstRow().getString(ACCT_ID));
					priceAsgnId = CommonUtils.CheckNull(preparedStatement.firstRow().getString(PRICE_ASGN_ID));
					winEndDt = preparedStatement.firstRow().getDate(WIN_END_DT);
					
				}
				
				
			} catch (Exception e) {
				logger.error("Exception occurred in createTxnAttribtesMapping()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
								
				BillableCharge_Id billchgId=new BillableCharge_Id(billableChgId);
				BigInteger parmGrpId =   billchgId.getEntity().getPriceItemParmGroupId();
				PriceItem_Id priceItemCode = billchgId.getEntity().getPriceItemCodeId();
				logger.debug("****************Price Item Code for Billable Charge***************"+priceItemCode);
				
				parmMap.clear();
				getParameterString(parmGrpId);
				
				udfChar1 = CommonUtils.convertNullToEmptyString(parmMap.get(TTYPE));
				udfChar3 = CommonUtils.convertNullToEmptyString(parmMap.get(CTYPE));
				udfChar6 = CommonUtils.convertNullToEmptyString(parmMap.get(PTYPE));
				udfChar7 = CommonUtils.convertNullToEmptyString(parmMap.get(ATYPE));
				udfChar8 = CommonUtils.convertNullToEmptyString(parmMap.get(JTYPE));
				udfChar13 = CommonUtils.convertNullToEmptyString(parmMap.get(STYPE));
				udfChar14 = CommonUtils.convertNullToEmptyString(parmMap.get(ARTYPE));
				udfChar20 = CommonUtils.convertNullToEmptyString(parmMap.get(AUTYPE));
				
				childProduct=fetchChildProduct(udfChar1,udfChar3,udfChar6,udfChar7,udfChar8,udfChar13,udfChar14,udfChar20,priceItemCode);
				logger.debug("************Child Product mapped- L0******************"+childProduct);
						
				stringBuilder = new StringBuilder();
				try{
				
				stringBuilder.append(" INSERT INTO CM_TXN_ATTRIBUTES_MAP ");
				//NAP-24118 - Add tables to Billed Billable Charges MO - Start Change
//				stringBuilder.append("(BILLABLE_CHG_ID,TXN_CALC_ID,UDF_CHAR_11,UDF_CHAR_12,UDF_CHAR_15,UDF_CHAR_25,ILM_DT,ILM_ARCH_SW) ");
//				stringBuilder.append("values (:billableChgId,:txnCalcId,trim(:udfChar11),trim(:udfChar12),TRIM(:udfChar15),TRIM(:udfChar25),SYSDATE,'Y')");
				
				// RIA: NAP- 30741: Added SETT_LEVEL_GRANULARITY column in CM_TXN_ATTRIBUTES_MAP
				stringBuilder.append(" (BILLABLE_CHG_ID,TXN_CALC_ID,UDF_CHAR_11,UDF_CHAR_12,UDF_CHAR_15,UDF_CHAR_25,UDF_NBR_19, SETT_LEVEL_GRANULARITY, CHILD_PRODUCT, PARENT_ACCT_ID, PRICE_ASGN_ID) ");
				stringBuilder.append(" values (:billableChgId,:txnCalcId,trim(:udfChar11),trim(:udfChar12),TRIM(:udfChar15),TRIM(:udfChar25), ");
				stringBuilder.append(" :udfNbr19,SUBSTR(concat(CONCAT(TO_CHAR(:winEndDt, 'YYMMDD'), :acctId), ORA_HASH(CONCAT(TRIM(:udfChar11), TRIM(:udfChar12)))), 1, 30), :childProduct, :acctId, :priceAsgnId) ");
				//NAP-24118 - Add tables to Billed Billable Charges MO - End Change
				preparedStatement = createPreparedStatement(stringBuilder.toString(),BLANK);
				preparedStatement.bindString("billableChgId", billableChgId, BILLABLE_CHG_ID);	
				preparedStatement.bindString("txnCalcId", txnCalcId, TXN_CALC_ID);	
				preparedStatement.bindString("udfChar11", udfChar11, UDF_CHAR_11);	
				preparedStatement.bindString("udfChar12", udfChar12, UDF_CHAR_12);	
				preparedStatement.bindString("udfChar15", udfChar15, UDF_CHAR_15);	
				preparedStatement.bindString("udfChar25", udfChar25, UDF_CHAR_25);
				preparedStatement.bindString("priceAsgnId", priceAsgnId, PRICE_ASGN_ID);
				preparedStatement.bindBigDecimal("udfNbr19", udfNbr19);
				preparedStatement.bindString("childProduct", childProduct, CHILD_PRODUCT);

				preparedStatement.bindString("acctId", acctId, ACCT_ID);
				preparedStatement.bindDate("winEndDt", winEndDt);
				preparedStatement.setAutoclose(false);				

				preparedStatement.executeUpdate();
			
				
			} catch (Exception e) {
				logger.error("Exception occurred in createTxnAttribtesMapping()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
		}
		
		private String fetchChildProduct(String udfChar1, String udfChar3,
				String udfChar6, String udfChar7, String udfChar8,
				String udfChar13, String udfChar14, String udfChar20,
				PriceItem_Id priceItemCode) {
			// TODO Auto-generated method stub
			StringBuilder sb=new StringBuilder();
			PreparedStatement ps=null;
			String childPriceItem=priceItemCode.getTrimmedValue();

			
			sb.append("SELECT M.CHILD_CHARGE_TYPE FROM CM_CHARGE_TYPE_MAP M");
			sb.append(" WHERE M.TTYPE = :udfchar1");
			sb.append(" AND M.CTYPE = :udfChar3");
			sb.append(" AND M.PTYPE = :udfChar6");
			sb.append(" AND M.AUTH_EVENT = :udfChar7"); 
			sb.append(" AND M.JTYPE = :udfChar8"); 
			sb.append(" AND M.PAYMENT_SCHEME = :udfChar13"); 
			sb.append(" AND M.AUTH_TRANSACTION_TYPE = :udfChar20"); 
			sb.append(" AND M.INTERNAL_AUTH_RESPONSE = :udfChar14"); 
			sb.append(" AND M.PARENT_CHARGE_TYPE = :priceItemCode"); 
			sb.append(" AND ROWNUM=1"); 
			
			try{
				ps=createPreparedStatement(sb.toString(),BLANK);
				ps.bindString("udfchar1", udfChar1, TTYPE);
				ps.bindString("udfChar3", udfChar3, CTYPE);
				ps.bindString("udfChar6", udfChar6, PTYPE);
				ps.bindString("udfChar7", udfChar7, AUTH_EVENT);
				ps.bindString("udfChar8", udfChar8, JTYPE);
				ps.bindString("udfChar13", udfChar13, PAYMENT_SCHEME);
				ps.bindString("udfChar20", udfChar20, AUTH_TRANSACTION_TYPE);
				ps.bindString("udfChar14", udfChar14, INTERNAL_AUTH_RESPONSE);
				ps.bindString("priceItemCode", priceItemCode.getTrimmedValue(),PARENT_CHARGE_TYPE);
				ps.setAutoclose(false);
				if(notNull(ps.firstRow())){
					childPriceItem=ps.firstRow().getString(CHILD_CHARGE_TYPE);
					logger.debug("****************Child Price Item Code*******"+childPriceItem);
				}				
			}
			catch(Exception e){
				logger.error("*****Exception in fetching Payment Scheme*****"+e);
			}
			finally{
				if(ps != null){
					ps.close();
					ps=null;
				}
			}
			
			
			return childPriceItem;
		}
		
		private void getParameterString(BigInteger parmGrpId) {
			// TODO Auto-generated method stub
			
			StringBuilder sb=new StringBuilder();
			PreparedStatement ps=null;
			String parmStr=null;
			String splitFirstValue=null;
			//Map<String,String> outputMap = new HashMap<String,String>();

			String splitSeconSValue=null;

			try{
				sb.append("SELECT PARM_STR FROM CI_PRICEITEM_PARM_GRP_K WHERE PRICEITEM_PARM_GRP_ID = :parmGrpId");
				ps=createPreparedStatement(sb.toString(),BLANK);
				ps.bindBigInteger("parmGrpId", parmGrpId);
				ps.setAutoclose(false);	
				if(notNull(ps.firstRow())){					
					parmStr=ps.firstRow().getString(PARM_STR);
				}
				
				if(notNull(parmStr)){
					String[] str=parmStr.split(DELIMITER);
					
					for(String parameterString : str){
						splitFirstValue = parameterString.substring(0, parameterString.indexOf(EQUALS));
						
						splitSeconSValue = parameterString.substring(parameterString.indexOf(EQUALS) + 1 ,parameterString.length());
						parmMap.put(splitFirstValue, splitSeconSValue);	
					}
				}
				
			}
			catch(Exception e){
				logger.error("Exception occurred in createTxnAttribtesMapping()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			}
			finally{
				if(notNull(ps)){
					ps.close();
					ps=null;
				}
			}
			
			//return parmMap;
		}

		public static String getErrorDescription(String messageNumber) {
			String errorInfo = BLANK_SPACE;
			errorInfo = CustomMessageRepository.billCycleError(messageNumber).getMessageText();
			if (errorInfo.contains(TEXT)
					&& errorInfo.contains(DESCR)) {
				errorInfo = errorInfo.substring(errorInfo.indexOf(TEXT),
						errorInfo.indexOf(DESCR));
			}
			return errorInfo;
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
			// In case error occurs, roll back all changes for the current transaction and log error.
			FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}			
	}// end worker

	public static final class BillableCharges_Id implements Id {
		private static final long serialVersionUID = 1L;
	
		private String billableChgId;

		public BillableCharges_Id(String billableChgId) {
			setBillableChgId(billableChgId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public String getBillableChgId() {
			return billableChgId;
		}

		public void setBillableChgId(String billableChgId) {
			this.billableChgId = billableChgId;
		}		
	} // end Id class
}