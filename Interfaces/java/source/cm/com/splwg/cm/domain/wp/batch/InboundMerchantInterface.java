/*******************************************************************************
 * FileName                   : InboundMerchantInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 2.7
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name       | Nature of Change
0.1		 NA				Mar 24, 2015		Sunaina		  		Implemented all the requirements for CD1. 
0.2		 NA				Apr 22, 2015		Sunaina		  		Fix for records with same Per Id Number but different divisions.
0.3		 NA				Jun 12, 2015		Sunaina		  		Implemented Requirements for CD2 (End Date change).
0.4		 NA				Jun 15, 2015		Preeti		  		Implemented Requirements for CD2 (Tiered/WAF).
0.5		 NA				Aug 27, 2015		Sunaina		  		Changes for End date.
0.6		 NA				Sept 9, 2015		Sunaina		  		Changes for Characteristics.
0.7		 NA				Sept 21, 2015		Abhishek Paliwal	BO replaced by DTO.
0.8      NA             Oct  02, 2015       Sunaina             Changed execution strategy to Standard Commit Strategy.
0.9		 NA				Feb 8, 2016			Sunaina				Updated for PAM-4182.
1.0 	 NA				Apr 5, 2016			Sunaina				Updated as per Oracle code Review.
1.0 	 NA				Apr 22, 2016		Sunaina				Updated for PAM-5551 and PAM-5490.
1.1 	 NA				May 04, 2016		Preeti Tiwari		Updated to resolve SQL warnings.
1.2 	 NA				Sept 8, 2016		Sunaina				PAM-8602, NAP-6118 resolution.
1.3 	 NA				Jan 17, 2017		Ankur Jain			Changes for performance improvement.
1.4 	 NA				Feb 27, 2017		Vienna Rom			Fixed to eliminate bill cycle sched top sql execution. 
1.5 	 NA				Mar 15, 2017		Vienna Rom			Added unique constraint workaround on account/SA creation.
1.6 	 NA				Mar 24, 2017		Vienna Rom			Allow merchants with pricing and no billing.
1.7 	 NA				Apr 04, 2017		Vienna Rom			PAM-11999 Fixed creation of VAT per id nbr for existing merchant.
1.8 	 NA				May 05, 2017		Vienna Rom			PAM-12711 Removed final on for loop variable.
1.9      NA	            Jun 07, 2017		Ankur  Jain         NAP-14404 fix.
2.0      NA	            Aug 31, 2017		Preeti              Tax price assignment for Merchant.
2.1      NA	            Jan 25, 2018		Ankur Jain          PAM-17077 & PAM-17079 Fix.
2.2      NA	            Feb 05, 2018		Preeti              NAP-22634 Remove default creation logic for reserve contracts.
2.3		 NAP-24121	    Mar 10, 2018	    RIA					Enter details in CI_ACCT_PER_ROUTING table as well.Added DISTINCT in SQL of getCharData().
2.4		 NAP-24085		Apr 10, 2018		Rakesh Ranjan		Update ILM_ARCH_SW coulmn to Y for records marked completed.
2.5		 NAP-27867		Jun 08, 2018		RIA					Handle bills on bill cycle change 
2.6	   	 NA			 	Jun 11, 2018		RIA		   			Prepared Statement close
2.7	   	 NAP-31443	 	JuL 08, 2018		RIA		   			Performance changes
2.8      NAP-36897      Dec 05, 2018        RIA                 Added check for active contracts to be fetched.  
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.DateFormat;
import com.splwg.base.api.datatypes.DateFormatParseException;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.country.Country;
import com.splwg.base.domain.common.country.Country_Id;
import com.splwg.base.domain.common.currency.Currency_Id;
import com.splwg.base.domain.common.language.Language_Id;
import com.splwg.base.domain.security.accessGroup.AccessGroup_Id;
import com.splwg.base.support.context.FrameworkSession;
import com.splwg.base.support.context.SessionHolder;
import com.splwg.ccb.api.lookup.BillFormatLookup;
import com.splwg.ccb.api.lookup.BillingAddressSourceLookup;
import com.splwg.ccb.api.lookup.CustomerReadLookup;
import com.splwg.ccb.api.lookup.LifeSupportSensitiveLoadLookup;
import com.splwg.ccb.api.lookup.NameTypeLookup;
import com.splwg.ccb.api.lookup.PersonOrBusinessLookup;
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.admin.accountRelationshipType.AccountRelationshipType_Id;
import com.splwg.ccb.domain.admin.billCycle.BillCycle_Id;
import com.splwg.ccb.domain.admin.billRouteType.BillRouteType_Id;
import com.splwg.ccb.domain.admin.cisDivision.CisDivision_Id;
import com.splwg.ccb.domain.admin.collectionClass.CollectionClass_Id;
import com.splwg.ccb.domain.admin.customerClass.CustomerClass_Id;
import com.splwg.ccb.domain.admin.idType.IdType_Id;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType_Id;
import com.splwg.ccb.domain.admin.serviceAgreementType.ServiceAgreementType_Id;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.BillCharacteristic;
import com.splwg.ccb.domain.billing.bill.BillCharacteristic_DTO;
import com.splwg.ccb.domain.billing.bill.BillCharacteristic_Id;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristic;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristics;
import com.splwg.ccb.domain.customerinfo.account.AccountNumber;
import com.splwg.ccb.domain.customerinfo.account.AccountNumber_DTO;
import com.splwg.ccb.domain.customerinfo.account.AccountNumber_Id;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson_DTO;
import com.splwg.ccb.domain.customerinfo.account.AccountPerson_Id;
import com.splwg.ccb.domain.customerinfo.account.Account_DTO;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.person.Person;
import com.splwg.ccb.domain.customerinfo.person.PersonCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.person.PersonCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.person.PersonId;
import com.splwg.ccb.domain.customerinfo.person.PersonId_DTO;
import com.splwg.ccb.domain.customerinfo.person.PersonId_Id;
import com.splwg.ccb.domain.customerinfo.person.PersonName;
import com.splwg.ccb.domain.customerinfo.person.PersonName_DTO;
import com.splwg.ccb.domain.customerinfo.person.PersonName_Id;
import com.splwg.ccb.domain.customerinfo.person.Person_DTO;
import com.splwg.ccb.domain.customerinfo.person.Person_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreementCharacteristic_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.ccb.domain.pricing.priceassign.PriceAsgn_DTO;
import com.splwg.ccb.domain.pricing.priceitem.PriceItem_Id;
import com.splwg.ccb.domain.rate.rateSchedule.RateSchedule_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.common.ApplicationError;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * InboundMerchantInterface program creates / updates Merchant, Account & Contracts.
 * 
 * @author Sunaina
 *
  @BatchJob (multiThreaded = true, rerunnable = false,
 *      modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = txnSourceCode, required = false, type = string)})
 */
public class InboundMerchantInterface extends InboundMerchantInterface_Gen {

	public static final Logger logger = LoggerFactory
			.getLogger(InboundMerchantInterface.class);

	private InboundMerchantInterfaceLookUp merchantInterfaceLookup = null;
	public static final String ACTIVE =  "active";
	public static final String PENDIGSTOP = "pendingStop";
	public static final String  CISDIVISION = "cisDivision";
	public static final Integer  TWO = 2;
	public static final String  EXCEPTIONOCCURED = "Exception occurred in updateStagingStatus()";

	public InboundMerchantInterface() {
	  //empty
	}

	/**
	 * getJobWork() method selects data for processing by Merchants Interface. 
	 * The source of data is selected from CM_MERCH_STG table and then passed to 
	 * the executeWorkUnit for further processing by framework.
	 */
	@Override
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork() method");
		
		//Initialize Lookup that stores various constants used by this interface.
		merchantInterfaceLookup = new InboundMerchantInterfaceLookUp();		

		final List<ThreadWorkUnit> threadWorkUnitList = getPerIdNbrData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "
				+ threadWorkUnitList.size());
		merchantInterfaceLookup = null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	//	*********************** getPerIdNbrData Method******************************

	/**
	 * getPerIdNbrData() method selects distinct PER_ID_NBRs from CM_MERCH_STG staging table.
	 * 
	 * @return List Per_Id_Nbr_Id
	 */
	private List<ThreadWorkUnit> getPerIdNbrData() {
		logger.debug("Inside getPerIdNbrData() method");
		PreparedStatement preparedStatement = null;
		Per_Id_Nbr_Id peridnbrId = null;
		List<Per_Id_Nbr_Id> rowsForProcessingList = new ArrayList<>();
		String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();
		StringBuilder stringBuilder = new StringBuilder();

		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		String perIdNbr = "";
		String cisDivision = "";

		stringBuilder.append("SELECT DISTINCT STG.PER_ID_NBR, STG.CIS_DIVISION FROM CM_MERCH_STG STG ");
		stringBuilder.append(" WHERE STG.BO_STATUS_CD = :selectBoStatus1 AND (STG.EFFDT<=SYSDATE OR STG.EFFDT IS NULL) " );
		stringBuilder.append(" AND NOT EXISTS(SELECT 1 FROM CM_INV_GRP_END_STG WHERE BO_STATUS_CD= :selectBoStatus1 and end_dt > SYSDATE");
		stringBuilder.append(" AND PER_ID_NBR=STG.PER_ID_NBR AND CIS_DIVISION=STG.CIS_DIVISION) ");
		try {
			if (notBlank(txnSourceCode)) {
				stringBuilder.append(" and TXN_SOURCE_CD=:txnSourceCode");				
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus1", merchantInterfaceLookup.getUpload(), "BO_STATUS_CD");
				preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
			} else {
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("selectBoStatus1", merchantInterfaceLookup.getUpload(), "BO_STATUS_CD");
			}
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				perIdNbr = resultSet.getString("PER_ID_NBR");
				cisDivision = resultSet.getString("CIS_DIVISION");
				peridnbrId = new Per_Id_Nbr_Id(perIdNbr,cisDivision);
				rowsForProcessingList.add(peridnbrId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(peridnbrId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				peridnbrId = null;
			}
		} catch (Exception e) {
			logger.error("Exception in getPerIdNbrData" , e);
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

	public Class<InboundMerchantInterfaceWorker> getThreadWorkerClass() {
		return InboundMerchantInterfaceWorker.class;
	}

	public static class InboundMerchantInterfaceWorker extends InboundMerchantInterfaceWorker_Gen {

		private String personId = "";
		private String accountId = "";
		private String contractId = "";
		private boolean isExistingAccount=false;
		private boolean isExistingPerson=false;

		private ArrayList<ArrayList<String>> updateCustomerStatusList = new ArrayList<ArrayList<String>>();
		private ArrayList<String> eachCustomerStatusList = null;
		private ArrayList<String> charOrAcctList = new ArrayList<>();
		private ArrayList<List<String>> fundAccountTypeList = new ArrayList<List<String>>();
		private ArrayList<List<String>> chrgAccountTypeList = new ArrayList<List<String>>();
		private ArrayList<List<String>> chbkAccountTypeList = new ArrayList<List<String>>();
		private ArrayList<List<String>> crwdAccountTypeList = new ArrayList<List<String>>();

		
		private InboundMerchantInterfaceLookUp merchantInterfaceLookup = null;
		
		private static final String SQL_ERROR_CODE_999999993 = "SQL Error code 999999993";
		
		public InboundMerchantInterfaceWorker() {
		  //empty
		}

		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		@Override
		public void initializeThreadWork(boolean arg0)
				throws ThreadAbortedException, RunAbortedException {
			logger.debug("Inside initializeThreadWork() method");
			//Retrieving the lookup again to support multiple threadpool
			if(merchantInterfaceLookup == null) {
				merchantInterfaceLookup = new InboundMerchantInterfaceLookUp();
			}
		}

		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.debug("Inside createExecutionStrategy() method");
			return new StandardCommitStrategy(this);
		}		
		
		private  boolean errorLogger(String entityCreateOrUpdateOrEndDateStatus,InboundMerchantInterface_Id merchantInterfaceId) {
			
			if (CommonUtils.CheckNull(entityCreateOrUpdateOrEndDateStatus).trim().startsWith("false")) {
				String skipRowsFlag = "true";
				Map<String, String> personOrAccountOrContractError = new HashMap<String, String>();
				personOrAccountOrContractError = errorHandler(entityCreateOrUpdateOrEndDateStatus);

				return logError(merchantInterfaceId.getTransactionHeaderId(),
						personOrAccountOrContractError.get("actualErrorMessageCategory"), 
						personOrAccountOrContractError.get("messageNumberLocal"), 
						personOrAccountOrContractError.get("actualErrorMessageNumber"), 
						personOrAccountOrContractError.get("status"), skipRowsFlag,
						merchantInterfaceId.getPerIdNbr(),
						merchantInterfaceId.getCisDivision());
			}
			
			return true;
		}
		
		private boolean fundingAccountCreationOrUpdate(InboundMerchantInterface_Id merchantInterfaceId) {
			String accountCreateOrUpdateStatus ="";
			String contractCreateOrUpdateStatus="";
			String skipRowsFlag = "false";
			
			for(int listId =0; listId< fundAccountTypeList.size(); listId++) {

				accountCreateOrUpdateStatus = createOrUpdateAccount(
						merchantInterfaceId, 
						merchantInterfaceLookup.getFundingAccountType(),
						merchantInterfaceId.getPerIdNbr().concat(
								merchantInterfaceLookup.getFundingAccountNumber())
								.concat(fundAccountTypeList.get(listId).get(3)),
								fundAccountTypeList.get(listId).get(3),
								fundAccountTypeList.get(listId).get(2),
								fundAccountTypeList.get(listId).get(4),
								fundAccountTypeList.get(listId).get(5));

				if(!errorLogger(accountCreateOrUpdateStatus,merchantInterfaceId))
					return false;

				logger.debug("fundingAccountId record Added / Updated - " + accountId);
				accountCreateOrUpdateStatus = "";

				//					******************* Funding Contract Creation / Update *****************************************
				//Funding Contract Creation / Update

				contractCreateOrUpdateStatus = createOrUpdateContract(
						merchantInterfaceId, 
						merchantInterfaceLookup.getFundingAccountType(),
						fundAccountTypeList.get(listId).get(5), 
						fundAccountTypeList.get(listId).get(6));

				if(!errorLogger(contractCreateOrUpdateStatus,merchantInterfaceId))
					return false;

				logger.debug("fundingContractId record Added / Updated - " + contractId);
				contractCreateOrUpdateStatus = "";							
			}
			return true;
		}
		
		private boolean chargingAccountCreationOrUpdate(InboundMerchantInterface_Id merchantInterfaceId) {
			String accountCreateOrUpdateStatus ="";
			String contractCreateOrUpdateStatus="";
			String skipRowsFlag = "false";
			
			for(int listId =0; listId< chrgAccountTypeList.size(); listId++) {
				accountCreateOrUpdateStatus = createOrUpdateAccount(
						merchantInterfaceId, 
						merchantInterfaceLookup.getChargingAccountType(),
						merchantInterfaceId.getPerIdNbr().concat(
								merchantInterfaceLookup.getChargingAccountNumber())
								.concat(chrgAccountTypeList.get(listId).get(3)),
								chrgAccountTypeList.get(listId).get(3),
								chrgAccountTypeList.get(listId).get(2),
								chrgAccountTypeList.get(listId).get(4),
								chrgAccountTypeList.get(listId).get(5));

				if(!errorLogger(accountCreateOrUpdateStatus,merchantInterfaceId))
					return false;
				logger.debug("ChargingAccountId record Added / Updated - " + accountId);
				accountCreateOrUpdateStatus = null;

				//				*******************		Charging Contract Creation / Update *****************************************
				// Charging Contract Creation / Update
				contractCreateOrUpdateStatus = createOrUpdateContract(
						merchantInterfaceId, 
						merchantInterfaceLookup.getChargingAccountType(),
						chrgAccountTypeList.get(listId).get(5),
						chrgAccountTypeList.get(listId).get(6));

				if(!errorLogger(contractCreateOrUpdateStatus,merchantInterfaceId))
					return false;
				logger.debug("chargingContractId record Added / Updated - " + contractId);
				contractCreateOrUpdateStatus = null;

				//				*******************		Recurring Charges Contract Creation / Update *****************************************
				// Recurring Charges Contract Creation / Update

				contractCreateOrUpdateStatus = createOrUpdateContract(
						merchantInterfaceId, 
						merchantInterfaceLookup.getRecurringChargesContractType(),
						chrgAccountTypeList.get(listId).get(5), 
						chrgAccountTypeList.get(listId).get(6));

				
				if(!errorLogger(contractCreateOrUpdateStatus,merchantInterfaceId))
					return false;

				logger.debug("recurringChargesContractId record Added / Updated - " + contractId);
				contractCreateOrUpdateStatus = null;
			}
			
			return true;
		}
		private boolean chargeBackAccountCreationOrUpdate(InboundMerchantInterface_Id merchantInterfaceId) {
			String accountCreateOrUpdateStatus ="";
			String contractCreateOrUpdateStatus="";
			String skipRowsFlag = "false";
			
			for(int listId =0; listId< chbkAccountTypeList.size(); listId++) {

				accountCreateOrUpdateStatus = createOrUpdateAccount(merchantInterfaceId,
						merchantInterfaceLookup.getChargebackAccountType(),
						merchantInterfaceId.getPerIdNbr().concat(
								merchantInterfaceLookup.getChargebackAccountNumber())
								.concat(chbkAccountTypeList.get(listId).get(3)),
								chbkAccountTypeList.get(listId).get(3),
								chbkAccountTypeList.get(listId).get(2),
								chbkAccountTypeList.get(listId).get(4),
								chbkAccountTypeList.get(listId).get(5));
				
				if(!errorLogger(accountCreateOrUpdateStatus,merchantInterfaceId))
					return false;

				logger.debug("chargeBackAccountId record Added / Updated - " + accountId);
				accountCreateOrUpdateStatus = null;

				//					*******************		Charge back Contract Creation / Update *****************************************
				// Charge back Contract Creation / Update

				contractCreateOrUpdateStatus = createOrUpdateContract(
						merchantInterfaceId, 
						merchantInterfaceLookup.getChargebackAccountType(),
						chbkAccountTypeList.get(listId).get(5), 
						chbkAccountTypeList.get(listId).get(6));

				if(!errorLogger(contractCreateOrUpdateStatus,merchantInterfaceId))
					return false;

				logger.debug("chargeBackContractId record Added / Updated - " + contractId);
				contractCreateOrUpdateStatus = null;
			}
			
			return true;
		}
		private boolean cardRewardAccountCreationOrUpdate(InboundMerchantInterface_Id merchantInterfaceId) {
			String accountCreateOrUpdateStatus ="";
			String contractCreateOrUpdateStatus="";
			String skipRowsFlag = "false";
			
			for(int listId =0; listId< crwdAccountTypeList.size(); listId++) {

				accountCreateOrUpdateStatus = createOrUpdateAccount(merchantInterfaceId,
						merchantInterfaceLookup.getCardRewardAccountType(),
						merchantInterfaceId.getPerIdNbr().concat(
								merchantInterfaceLookup.getCardRewardAccountNumber())
								.concat(crwdAccountTypeList.get(listId).get(3)),
								crwdAccountTypeList.get(listId).get(3),
								crwdAccountTypeList.get(listId).get(2),
								crwdAccountTypeList.get(listId).get(4),
								crwdAccountTypeList.get(listId).get(5));

				
				if(!errorLogger(accountCreateOrUpdateStatus,merchantInterfaceId))
					return false;
				logger.debug("cardRewardAccountId record Added / Updated - "
						+ accountId);
				accountCreateOrUpdateStatus = "";

				//					*******************		CardReward Contract Creation / Update *****************************************
				//Card Reward Contract Creation / Update
				contractCreateOrUpdateStatus = createOrUpdateContract(
						merchantInterfaceId, 
						merchantInterfaceLookup.getCardRewardAccountType(),
						crwdAccountTypeList.get(listId).get(5), 
						crwdAccountTypeList.get(listId).get(6));

				if(!errorLogger(contractCreateOrUpdateStatus,merchantInterfaceId))
					return false;
				logger.debug("cardRewardContractId record Added / Updated - "
						+ contractId);
				contractCreateOrUpdateStatus = "";
			}
			
			return true;
		}
		private boolean creatOorUpdateDifferentAccounts(InboundMerchantInterface_Id merchantInterfaceId) {
			if(!fundingAccountCreationOrUpdate(merchantInterfaceId))
				return false;
			if(!chargingAccountCreationOrUpdate(merchantInterfaceId))
				return false;
			if(!chargeBackAccountCreationOrUpdate(merchantInterfaceId))
				return false;
			if(!cardRewardAccountCreationOrUpdate(merchantInterfaceId)) 
				return false;
			
			return true;
		}
		private String getMessageDescription(boolean isPricingOnly,InboundMerchantInterface_Id merchantInterfaceId) {
			if(isPricingOnly)
				return checkIfMerchantAccountStagingDataExist(merchantInterfaceId);
			
			return " ";
		}
		private boolean methodToExecuteAccountCreationOrUpdation(InboundMerchantInterface_Id merchantInterfaceId,String acctStgRetrievalFlag) {
			
			String skipRowsFlag="false";
			/*******************		Check for Transaction Group Type Data required for Account Creation / Update *****************************************/				
			if(acctStgRetrievalFlag.startsWith("false")) {

				skipRowsFlag = "true";
				return logError(merchantInterfaceId.getTransactionHeaderId(), 
						merchantInterfaceLookup.getErrorMessageCategory(), 
						String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE), 
						String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE),
						getErrorDescription(String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE)), 
						skipRowsFlag, merchantInterfaceId.getPerIdNbr(),
						merchantInterfaceId.getCisDivision());
			}
			boolean iscreatOorUpdateDifferentAccounts = creatOorUpdateDifferentAccounts(merchantInterfaceId);
			if(!iscreatOorUpdateDifferentAccounts)
				return false;


			String contractEndDateStatus = contractEndDateUpdate(merchantInterfaceId);

					
			if(!errorLogger(contractEndDateStatus,merchantInterfaceId))
				return false;
			logger.debug("Person record Added / Updated - " + personId);
			contractEndDateStatus = "";
			return true;

		
		}
		/**
		 * executeWorkUnit() method contains business logic that is executed for
		 * every row of processing. The selected row for processing is read
		 * (comes as input) and then processed further to create / update
		 * Person, Account & Contract records.
		 */
		@Override
		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			final Per_Id_Nbr_Id perIdNbrId = (Per_Id_Nbr_Id) unit.getPrimaryId();
			
			String skipRowsFlag = "false"; //This flag will be used to determine whether the rows should be skipped or not
			//as per the status of earlier rows processed with same PER_ID_NBR.
			boolean checkStateFlag = false;
			String messageCategoryNumber = "";
			String messageNumber = "";
			String messageDescription = "";
			String accountCreateOrUpdateStatus = "";
			String contractCreateOrUpdateStatus = "";
			String contractEndDateStatus = "";
			String acctStgRetrievalFlag = "";

			List<InboundMerchantInterface_Id> inboundMerchantInterfaceId = getMerchantStagingData(perIdNbrId);

			for(InboundMerchantInterface_Id merchantInterfaceId : inboundMerchantInterfaceId) {

				logger.debug("Transaction Header Id - " + merchantInterfaceId.getTransactionHeaderId());
				logger.debug("Person record with Per Id Nbr- " + merchantInterfaceId.getPerIdNbr());	

				messageCategoryNumber = "0";
				messageNumber = "0";
				messageDescription = " ";
				accountCreateOrUpdateStatus = "";
				contractCreateOrUpdateStatus = "";
				contractEndDateStatus = "";
				
				// Merchant is for pricing only if PER_OR_BUS_FLG='G'
				boolean isPricingOnly = merchantInterfaceId.getPerOrBusFlg().trim().equalsIgnoreCase("G");
				messageDescription = getMessageDescription(isPricingOnly,merchantInterfaceId);
				if(!isPricingOnly) {
					acctStgRetrievalFlag=getMerchantAccountStagingData(merchantInterfaceId);
				}
				
				checkStateFlag = checkState(merchantInterfaceId);
				logger.debug("checkStateFlag"+ checkStateFlag);


				removeSavepoint("Rollback".concat(getBatchThreadNumber().toString()));
				setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));


				//			 			Merchant Record Processing
				//			*******************		Person Creation / Updation *****************************************
				try {
					
					String personCreateOrUpdateStatus = createOrUpdatePerson(
							merchantInterfaceId, checkStateFlag, isPricingOnly);
					
					if(!errorLogger(personCreateOrUpdateStatus,merchantInterfaceId))
						return false;
					
					logger.debug("Person record Added / Updated - " + personId);
					personCreateOrUpdateStatus = "";
					
					if(!isPricingOnly && !methodToExecuteAccountCreationOrUpdation(merchantInterfaceId,acctStgRetrievalFlag)) {
						
						
							return false;
						/*******************		Check for Transaction Group Type Data required for Account Creation / Update *****************************************/				


					}
					
					// Update status of row in CM_MERCH_STG
					updateStagingStatus(merchantInterfaceId.getTransactionHeaderId(),
							merchantInterfaceLookup.getCompleted(),
							messageCategoryNumber, messageNumber, messageNumber, messageDescription, skipRowsFlag, 
							merchantInterfaceId.getPerIdNbr(), merchantInterfaceId.getCisDivision());
				} catch (Exception e) {
					logger.error("Exception in executeWorkUnit: " , e);
				}

				// Clear all the lists			
				fundAccountTypeList.clear();
				chrgAccountTypeList.clear();
				chbkAccountTypeList.clear();
				crwdAccountTypeList.clear();
			}
			
			inboundMerchantInterfaceId = null;
			return true;
		}

		/**
		 * checkIfMerchantAccountStagingDataExist() method checks CM_ACCT_STG records for the transaction header id
		 * 
		 * @param merchantInterfaceId
		 * @return String messageDescription
		 */
		private String checkIfMerchantAccountStagingDataExist(final InboundMerchantInterface_Id merchantInterfaceId) {
			String messageDescription = " ";
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder =  new StringBuilder();

			try {
				stringBuilder.append(" SELECT 1 FROM CM_ACCT_STG WHERE TXN_HEADER_ID =:headerId" );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("headerId", merchantInterfaceId.getTransactionHeaderId(), "TXN_HEADER_ID");
				preparedStatement.setAutoclose(false);
				if(notNull(preparedStatement.firstRow())){
					messageDescription = "Pricing Only merchant. Accounts in staging will not be created/updated.";
				}
			} catch (Exception e) {
				logger.error("Exception occurred in getMerchantAccountStagingData()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return messageDescription;
		}

		protected final void removeSavepoint(final String savePointName)
		{
			final FrameworkSession session = (FrameworkSession)SessionHolder.getSession();
			if (session.hasActiveSavepointWithName(savePointName)) {
				session.removeSavepoint(savePointName);
			}
		}
		protected final void setSavePoint(final String savePointName){
			// Create save point before any change is done for the current transaction.
			final FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.setSavepoint(savePointName);
		}

		protected final void rollbackToSavePoint(final String savePointName){
			// In case error occurs, rollback all changes for the current transaction and log error.
			final FrameworkSession session = (FrameworkSession) SessionHolder.getSession();
			session.rollbackToSavepoint(savePointName);
		}

		//			***********************checkState Method******************************

		/**
		 * checkState() method checks if state needs to be set for ORMB table.
		 * 
		 * @return List InboundMerchantInterface_Id
		 */
		private boolean checkState(final InboundMerchantInterface_Id mrchId) {
			boolean stateFlag = false;

			try {
				if(notBlank(mrchId.getState())){
					final Country country = new Country_Id(mrchId.getCountry()).getEntity();
					if(notNull(country) && !country.getStates().isEmpty()) {
						stateFlag = true;
					}
				}

			} catch(Exception e) {
				logger.error("Exception in state retrieval");
			}
			return stateFlag;
		}

		//		***********************getMerchantStagingData Method******************************

		/**
		 * getMerchantStagingData() method selects data from CM_MERCH_STG staging table.
		 * 
		 * @return List InboundMerchantInterface_Id
		 */
		private List<InboundMerchantInterface_Id> getMerchantStagingData(final Per_Id_Nbr_Id perIdNbrId) {
			logger.debug("Inside getMerchantstaging() method");
			PreparedStatement preparedStatement = null;
			InboundMerchantInterface_Id inboundMerchantInterfaceId = null;
			final List<InboundMerchantInterface_Id> rowsForProcessingList = new ArrayList<>();
			final String txnSourceCode = CommonUtils.CheckNull(getParameters().getTxnSourceCode()).trim();	
			StringBuilder stringBuilder = new StringBuilder();

			try {
				stringBuilder.append("SELECT TXN_HEADER_ID, EFFDT, PER_OR_BUS_FLG, CIS_DIVISION, PER_ID_NBR, ENTITY_NAME, ");
				stringBuilder.append(" VAT_REG_NBR, COUNTRY, STATE, ADD_MONTHS(END_DT,6) AS END_DT FROM CM_MERCH_STG ");
				stringBuilder.append(" WHERE BO_STATUS_CD = :selectBoStatus1 AND (EFFDT<=SYSDATE OR EFFDT IS NULL) ");
				stringBuilder.append(" AND PER_ID_NBR = :personIdNbr ");
				stringBuilder.append(" AND CIS_DIVISION = :division");
				if (notBlank(txnSourceCode)) {
					stringBuilder.append(" AND TXN_SOURCE_CD= :txnSourceCode ");
					stringBuilder.append(" ORDER BY TXN_HEADER_ID");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
					preparedStatement.bindString("selectBoStatus1", merchantInterfaceLookup.getUpload(), "BO_STATUS_CD");
					preparedStatement.bindString("personIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					preparedStatement.bindString("division", perIdNbrId.getCisDivision(), "CIS_DIVISION");
				} else {
					stringBuilder.append(" ORDER BY TXN_HEADER_ID");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("selectBoStatus1", merchantInterfaceLookup.getUpload(), "BO_STATUS_CD");
					preparedStatement.bindString("personIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					preparedStatement.bindString("division", perIdNbrId.getCisDivision(), "CIS_DIVISION");
				}
				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String transactionHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID"));
					String effectiveDate = CommonUtils.CheckNull(String.valueOf(resultSet.getDate("EFFDT")));
					String cisDivision = CommonUtils.CheckNull(resultSet.getString("CIS_DIVISION"));
					String perOrBusFlg = CommonUtils.CheckNull(resultSet.getString("PER_OR_BUS_FLG"));
					String perIdNbr = CommonUtils.CheckNull(resultSet.getString("PER_ID_NBR"));
					String entityName = CommonUtils.CheckNull(resultSet.getString("ENTITY_NAME"));
					String vatRegNbr = CommonUtils.CheckNull(resultSet.getString("VAT_REG_NBR"));
					String country = CommonUtils.CheckNull(resultSet.getString("COUNTRY"));
					String state = CommonUtils.CheckNull(resultSet.getString("STATE"));
					String endDtMerchant = CommonUtils.CheckNull(String.valueOf(resultSet.getDate("END_DT")));

					inboundMerchantInterfaceId = new InboundMerchantInterface_Id(
							transactionHeaderId, effectiveDate, perOrBusFlg, cisDivision,
							perIdNbr, entityName, vatRegNbr, country, state, endDtMerchant);


					rowsForProcessingList.add(inboundMerchantInterfaceId);
					resultSet = null;
					inboundMerchantInterfaceId = null;
				}

				stringBuilder = null;
				stringBuilder = new StringBuilder();
			} catch (Exception e) {
				logger.error("Exception occurred in getMerchantStagingData()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			stringBuilder = null;
			stringBuilder = new StringBuilder();
			//Update only future dated records with corresponding error information.
			try {

				stringBuilder.append("UPDATE CM_MERCH_STG SET ERROR_INFO = 'Future dated record: Row will get processed once it becomes effective.' ");
				stringBuilder.append(" WHERE (EFFDT>SYSDATE) ");
				stringBuilder.append(" AND PER_ID_NBR = :personIdNbr ");
				stringBuilder.append(" AND CIS_DIVISION = :division");
				
				if (notBlank(txnSourceCode)) {
					stringBuilder.append(" AND TXN_SOURCE_CD=:txnSourceCode");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("txnSourceCode", txnSourceCode.trim(), "TXN_SOURCE_CD");
					preparedStatement.bindString("personIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					preparedStatement.bindString("division", perIdNbrId.getCisDivision(), "CIS_DIVISION");
					preparedStatement.executeUpdate();
					
				}else{
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("personIdNbr", perIdNbrId.getPerIdNbr(), "PER_ID_NBR");
					preparedStatement.bindString("division", perIdNbrId.getCisDivision(), "CIS_DIVISION");
					preparedStatement.executeUpdate(); 
					
				}

			} catch (Exception e) {
				logger.error("Exception occurred in getMerchantStagingData()" , e);
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
		 * getMerchantAccountStagingData retrieves the account information for input Header Id.
		 * @param aInboundMerchantInterfaceId
		 */
		private String getMerchantAccountStagingData(
				final InboundMerchantInterface_Id aInboundMerchantInterfaceId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			logger.debug(" Inside getMerchantAccountStagingData() for Transaction Header Id "+aInboundMerchantInterfaceId.getTransactionHeaderId().trim());

			try {
				stringBuilder.append(" SELECT 1 FROM CM_ACCT_STG" );
				stringBuilder.append(" WHERE TXN_HEADER_ID =:headerId" );
				stringBuilder.append(" GROUP BY TXN_HEADER_ID,TRAN_GRP_TYPE,CUST_CL_CD,CURRENCY_CD,BILL_CYC_CD,SETUP_DT,END_DT HAVING COUNT(TXN_HEADER_ID)>1 " );
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("headerId", aInboundMerchantInterfaceId.getTransactionHeaderId().trim(), "TXN_HEADER_ID");
				
				preparedStatement.setAutoclose(false);
				if(notNull(preparedStatement.firstRow())){
					return "false" + "~"
							+ getErrorDescription(String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE))
							+ "~"
							+ merchantInterfaceLookup.getErrorMessageCategory()
							+ "~" + String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE);
				}
				preparedStatement.close();
								
				stringBuilder = new StringBuilder();
				
				stringBuilder.append("SELECT TXN_HEADER_ID, TRAN_GRP_TYPE, " );
				stringBuilder.append(" CUST_CL_CD, CURRENCY_CD, " );
				stringBuilder.append(" BILL_CYC_CD, SETUP_DT, ADD_MONTHS(END_DT,6) AS END_DT FROM CM_ACCT_STG " );
				stringBuilder.append(" WHERE TXN_HEADER_ID = :headerId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("headerId", aInboundMerchantInterfaceId.getTransactionHeaderId().trim(), "TXN_HEADER_ID");
				preparedStatement.setAutoclose(false);


					for (SQLResultRow resultSet : preparedStatement.list()) {
						String transactionHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID")).trim();
						String transactionGroupType = CommonUtils.CheckNull(resultSet.getString("TRAN_GRP_TYPE")).trim();
						String customerClassCode = CommonUtils.CheckNull(resultSet.getString("CUST_CL_CD")).trim();	
						String currencyCode = CommonUtils.CheckNull(resultSet.getString("CURRENCY_CD")).trim();	
						String billCycleCode = CommonUtils.CheckNull(resultSet.getString("BILL_CYC_CD")).trim();	
						String setupDate = CommonUtils.CheckNull(String.valueOf(resultSet.getDate("SETUP_DT"))).trim();	
						String endDate = CommonUtils.CheckNull(String.valueOf(resultSet.getDate("END_DT"))).trim();

						if (transactionGroupType.equalsIgnoreCase(merchantInterfaceLookup.getFundingAccountType())) {
							// Populate Funding Account Type List
							charOrAcctList = new ArrayList<String>();
							charOrAcctList.add(0, transactionHeaderId);
							charOrAcctList.add(1, transactionGroupType);
							charOrAcctList.add(2, customerClassCode);
							charOrAcctList.add(3, currencyCode);
							charOrAcctList.add(4, billCycleCode);
							charOrAcctList.add(5, setupDate);
							charOrAcctList.add(6, endDate);
							fundAccountTypeList.add(charOrAcctList);
							charOrAcctList = null;
						} else if (transactionGroupType.equalsIgnoreCase(merchantInterfaceLookup.getChargingAccountType())) {
							//	Populate Charging Account Type List
							charOrAcctList = new ArrayList<String>();
							charOrAcctList.add(0, transactionHeaderId);
							charOrAcctList.add(1, transactionGroupType);
							charOrAcctList.add(2, customerClassCode);
							charOrAcctList.add(3, currencyCode);
							charOrAcctList.add(4, billCycleCode);
							charOrAcctList.add(5, setupDate);						
							charOrAcctList.add(6, endDate);
							chrgAccountTypeList.add(charOrAcctList);
							charOrAcctList = null;
						} else if (transactionGroupType.equalsIgnoreCase(merchantInterfaceLookup.getChargebackAccountType())) {
							//	Populate Chargeback Account Type List
							charOrAcctList = new ArrayList<String>();
							charOrAcctList.add(0, transactionHeaderId);
							charOrAcctList.add(1, transactionGroupType);
							charOrAcctList.add(2, customerClassCode);
							charOrAcctList.add(3, currencyCode);
							charOrAcctList.add(4, billCycleCode);
							charOrAcctList.add(5, setupDate);
							charOrAcctList.add(6, endDate);
							chbkAccountTypeList.add(charOrAcctList);
							charOrAcctList = null;
						} else if (transactionGroupType.equalsIgnoreCase(merchantInterfaceLookup.getCardRewardAccountType())) {
							//	Populate Card reward Account Type List
							charOrAcctList = new ArrayList<String>();
							charOrAcctList.add(0, transactionHeaderId);
							charOrAcctList.add(1, transactionGroupType);
							charOrAcctList.add(2, customerClassCode);
							charOrAcctList.add(3, currencyCode);
							charOrAcctList.add(4, billCycleCode);
							charOrAcctList.add(5, setupDate);
							charOrAcctList.add(6, endDate);
							crwdAccountTypeList.add(charOrAcctList);
							charOrAcctList = null;
						} else {
							return "false" + "~"
									+ getErrorDescription(String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE))
									+ "~"
									+ merchantInterfaceLookup.getErrorMessageCategory()
									+ "~" + String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE);
						}
					}
				if(fundAccountTypeList.isEmpty() && chrgAccountTypeList.isEmpty() && chbkAccountTypeList.isEmpty() && crwdAccountTypeList.isEmpty()) {
					return "false" + "~"
							+ getErrorDescription(String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE))
							+ "~"
							+ merchantInterfaceLookup.getErrorMessageCategory()
							+ "~" + String.valueOf(CustomMessages.INVALID_TRANSACTION_GROUP_TYPE);
				}

			} catch (Exception e) {
				logger.error("Exception occurred in getMerchantAccountStagingData()" , e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			return "true";
		}

		/**This Map Interface is used to retrieve the actualErrorMessageNumber, actualErrorMessageCategory,messageKey and messageNumberLocal
		 *using CreateOrUpdateStatus for Person, Account or Contract 
		 *
		 * @param status
		 * 
		 */		
		public Map<String, String> errorHandler(String status) {
			final Map<String, String> errorMap = new HashMap<>();
			String actualErrorMessageNumber = status.substring(status.lastIndexOf('~') + 1, status.length()).trim();
			final String message = status.substring(status.indexOf('~') + 1, status.lastIndexOf('~'));
			String actualErrorMessageCategory = message.substring(message.indexOf('~') + 1).trim();
			String messageNumberLocal = "";
			status = message.substring(0, message.lastIndexOf('~')).replace("Text:", "");

			if ( (!(notBlank(actualErrorMessageNumber)))
					|| (!(notBlank(actualErrorMessageCategory))) ) {
				actualErrorMessageNumber = "0";
				actualErrorMessageCategory = merchantInterfaceLookup.getErrorMessageCategory();
			}

			if (!(notBlank(messageNumberLocal))) {
				messageNumberLocal = "0";
			}
			errorMap.put("actualErrorMessageNumber", actualErrorMessageNumber);
			errorMap.put("actualErrorMessageCategory", actualErrorMessageCategory);
			errorMap.put("messageNumberLocal", messageNumberLocal);
			errorMap.put("status", status);
			return errorMap;
		}


		/**
		 * finalizeThreadWork() is execute by the batch program once per thread after processing all units.
		 */
		@Override
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.debug("Inside finalizeThreadWork() method");
			// Logic to update erroneous records

			if (updateCustomerStatusList.size() > 0) {
				Iterator<ArrayList<String>> updateAccountStatusItr = updateCustomerStatusList
						.iterator();
				updateCustomerStatusList = null;
				ArrayList<String> rowList = null;
				while (updateAccountStatusItr.hasNext()) {
					rowList = (ArrayList<String>) updateAccountStatusItr.next();
					updateStagingStatus(String.valueOf(rowList.get(0)), 
							String.valueOf(rowList.get(1)), 
							String.valueOf(rowList.get(2)), 
							String.valueOf(rowList.get(3)), 
							String.valueOf(rowList.get(4)), 
							String.valueOf(rowList.get(5)),
							String.valueOf(rowList.get(6)), 
							String.valueOf(rowList.get(7)), 
							String.valueOf(rowList.get(8)));
					rowList = null;
				}
				updateAccountStatusItr = null;
			}

			super.finalizeThreadWork();
		}
		
		private String deletePersonCharacteristics(ArrayList<List<String>> aPersonCharList,PersonCharacteristic_DTO personCharacteristic_DTO, Person_Id person_Id ) {
			StringBuilder stringBuilder=null;
			PreparedStatement preparedStatement = null;
			for (int i = 0; i < aPersonCharList.size(); i++) {
				try{
					stringBuilder = new StringBuilder();
					//Deleting characteristic if existing for any characteristic type code irrespective of effective date
					//RIA: reverse order of filters in query
					stringBuilder.append("DELETE FROM CI_PER_CHAR WHERE PER_ID = :personId AND CHAR_TYPE_CD = :charTypeCode ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("personId", personId, "PER_ID");
					preparedStatement.bindString("charTypeCode", aPersonCharList.get(i).get(2), "CHAR_TYPE_CD");
					preparedStatement.executeUpdate();
					preparedStatement.setAutoclose(false);
					
					String effectiveDateString =aPersonCharList.get(i).get(1);
					personCharacteristic_DTO.setAdhocCharacteristicValue(aPersonCharList.get(i).get(3));
					if(notBlank(effectiveDateString)) {
						String[] effectiveDateStringArray = effectiveDateString.split("-", 50);
						Date d1=new Date(
								Integer.parseInt(effectiveDateStringArray[0]),
								Integer.parseInt(effectiveDateStringArray[1]),
								Integer.parseInt(effectiveDateStringArray[2]));
						personCharacteristic_DTO.setId(new PersonCharacteristic_Id(person_Id,new CharacteristicType_Id(aPersonCharList.get(i).get(2)),d1));

					}else{
						personCharacteristic_DTO.setId(new PersonCharacteristic_Id(person_Id,new CharacteristicType_Id(aPersonCharList.get(i).get(2)),new Date(0,0,0)));
					}
					personCharacteristic_DTO.newEntity();
				} catch (Exception e) {
					logger.error("Exception in createOrUpdatePerson() deleting person characteristics",e);
					String errorMessage = CommonUtils.CheckNull(e.getMessage());
					Map<String, String> errorMsg = new HashMap<>();
					errorMsg = errorList(errorMessage);
					return "false" + "~" + errorMsg.get("Text") + "~"
					+ errorMsg.get("Category") + "~"
					+ errorMsg.get("Number");
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}
			return "";
		}
		
		private void setNewPersonCharacteristics(ArrayList<List<String>> aPersonCharList,PersonCharacteristic_DTO personCharacteristic_DTO,Person_DTO personDto) {
			for (int i = 0; i < aPersonCharList.size(); i++) {
				personCharacteristic_DTO.setAdhocCharacteristicValue(aPersonCharList.get(i).get(3));
				String effectiveDateString = String.valueOf(aPersonCharList.get(i).get(1));
				if(notBlank(effectiveDateString)) {
					String[] effectiveDateStringArray = effectiveDateString.split("-", 50);
					Date d1=new Date(
							Integer.parseInt(effectiveDateStringArray[0]),
							Integer.parseInt(effectiveDateStringArray[1]),
							Integer.parseInt(effectiveDateStringArray[2]));
					personCharacteristic_DTO.setId(new PersonCharacteristic_Id(new Person_Id(personDto.getId().getIdValue()),new CharacteristicType_Id(aPersonCharList.get(i).get(2)),d1));

				}else{
					personCharacteristic_DTO.setId(new PersonCharacteristic_Id(new Person_Id(personDto.getId().getIdValue()),new CharacteristicType_Id(aPersonCharList.get(i).get(2)),new Date(0,0,0)));
				}
				personCharacteristic_DTO.newEntity();
			}
		}
		
		private void createUpdateOrDeleteVetRegNbr(String vatRegNbr,Person_Id personIdObj,String idTypeCode2,String personIdstring ) {
			StringBuilder stringBuilder=null;
			PreparedStatement preparedStatement = null;
			
			if (notBlank(vatRegNbr)) {
			  
				PersonId_Id personId_Id2 = new PersonId_Id(personIdObj, new IdType_Id(idTypeCode2));
				PersonId personId2 = personId_Id2.getEntity();
				if(notNull(personId2)) {
					PersonId_DTO personIdDto2 = personId2.getDTO();
					personIdDto2.setPersonIdNumber(vatRegNbr);
					personIdDto2.setIsPrimaryId(Bool.FALSE);
					personId2.setDTO(personIdDto2);
				}else {
					PersonId_DTO personIdDto2 = createDTO(PersonId.class);
					personIdDto2.setId(personId_Id2);
					personIdDto2.setPersonIdNumber(vatRegNbr);
					personIdDto2.setIsPrimaryId(Bool.FALSE);
					personIdDto2.newEntity();
				}
			}else {
			  
				// remove vatRgenbr
				try {
					stringBuilder = new StringBuilder();
					stringBuilder.append("SELECT PER_ID_NBR FROM CI_PER_ID ");
					stringBuilder.append(" WHERE ID_TYPE_CD =:idTypeCode2 AND PER_ID =:personId ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
					preparedStatement.bindString("idTypeCode2", idTypeCode2, "ID_TYPE_CD");
					preparedStatement.bindString("personId", personIdstring, "PER_ID");
					preparedStatement.setAutoclose(false);
					SQLResultRow vatRegNbrRow = preparedStatement.firstRow();
					if (notNull(vatRegNbrRow)) {
						stringBuilder = null;
						if (preparedStatement != null  ) {
							preparedStatement.close();
							preparedStatement = null;
						}
						
						stringBuilder = new StringBuilder();
						stringBuilder.append("DELETE  FROM CI_PER_ID ");
						stringBuilder.append(" WHERE ID_TYPE_CD =:idTypeCode2 AND PER_ID =:personId ");
						preparedStatement = createPreparedStatement(stringBuilder.toString(), "");
						preparedStatement.bindString("idTypeCode2", idTypeCode2, "ID_TYPE_CD");
						preparedStatement.bindString("personId", personIdstring, "PER_ID");
						preparedStatement.executeUpdate();
						preparedStatement.setAutoclose(false);

					}
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}
		}
		 private String valueOfState(boolean checkStateFlg,InboundMerchantInterface_Id aInboundMerchantInterfaceId) {
			 return (checkStateFlg)?aInboundMerchantInterfaceId.getState():"";
		 }
		/**
		 * createOrUpdatePerson() method Creates a new Person or Updates an existing person record.
		 * 
		 * @param aInboundMerchantInterfaceId
		 * @param checkStateFlag
		 * @param isPricingOnly
		 * @return
		 * @throws RunAbortedException
		 */

		private String createOrUpdatePerson(
				final InboundMerchantInterface_Id aInboundMerchantInterfaceId, final boolean checkStateFlag, boolean isPricingOnly) {
			
			logger.debug("Inside createOrUpdateMerchant() method");
			
			StringBuilder stringBuilder = new StringBuilder();

			String cisDivision = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getCisDivision()).trim();
			String perIdNbr = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getPerIdNbr()).trim();
			String vatRegNbr = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getVatRegNbr()).trim();
			String entityName = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getEntityName()).trim();
			String idTypeCode2 = merchantInterfaceLookup.getIdTypeCd2();
			String idTypeCode = merchantInterfaceLookup.getIdTypeCd();
			String accessGroup = merchantInterfaceLookup.getAccessGroup();
			personId = "";
			PreparedStatement preparedStatement = null;
			
			//Pricing Only merchant has person type lookup set to Group, for all other merchants it is set to Business
			PersonOrBusinessLookup perOrBusFlg = (isPricingOnly ? PersonOrBusinessLookup.constants.GROUP : PersonOrBusinessLookup.constants.BUSINESS);
			
			startChanges();
			
			Person_DTO personDto=new Person_DTO();
			PersonId_DTO personIdDto=new PersonId_DTO();
			PersonCharacteristic_DTO personCharacteristic_DTO=new PersonCharacteristic_DTO();
			PersonName_DTO personName_DTO=new PersonName_DTO();
			ArrayList<List<String>> aPersonCharList = new ArrayList<List<String>>();
			
			aPersonCharList = getCharData(aInboundMerchantInterfaceId.getTransactionHeaderId(), merchantInterfaceLookup.getPerson(), " ");
			try {
				// Checks if Person record exists in the system
				stringBuilder.append(" SELECT PERID.PER_ID FROM CI_PER_ID PERID, CI_PER PER " );
				stringBuilder.append(" WHERE PERID.ID_TYPE_CD =:idTypeCode AND PERID.PER_ID_NBR =:custNumber " );
				stringBuilder.append(" AND PER.CIS_DIVISION = :division AND PERID.PER_ID = PER.PER_ID");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("idTypeCode", idTypeCode, "ID_TYPE_CD");
				preparedStatement.bindString("custNumber", perIdNbr, "PER_ID_NBR");
				preparedStatement.bindString("division", cisDivision, "CIS_DIVISION");
				boolean newPersonFlag = false;
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if(notNull(row)) {
					personId = row.getString("PER_ID");
					isExistingPerson=true;
				} else {
					newPersonFlag = true;
					isExistingPerson=false;
					 
				}
				preparedStatement.close();
				preparedStatement= null;
				
				if (!newPersonFlag) {


					Person_Id person_Id=new Person_Id(personId);
					Person person=person_Id.getEntity();
					personDto=person.getDTO();
					personDto.setPersonOrBusiness(perOrBusFlg);
					personDto.setLanguageId(new Language_Id("ENG"));
					personDto.setLifeSupportSensitiveLoad( LifeSupportSensitiveLoadLookup.constants.NONE);
					personDto.setAccessGroupId(new AccessGroup_Id(accessGroup));
					personDto.setDivision(cisDivision);
					personDto.setAddress1(merchantInterfaceLookup.getAddress1());

					
					personDto.setState(valueOfState(checkStateFlag,aInboundMerchantInterfaceId));
					personDto.setCountry(aInboundMerchantInterfaceId.getCountry());
					person.setDTO(personDto);

					//values for CI_PER_NAME Table for old Person

					PersonName_Id personName_Id=new PersonName_Id(person_Id,new BigInteger("1"));
					PersonName personName=personName_Id.getEntity();
					personName_DTO=personName.getDTO();
					personName_DTO.setEntityName(entityName);
					personName_DTO.setNameType(NameTypeLookup.constants.PRIMARY);
					personName_DTO.setUppercaseEntityName(entityName);
					personName.setDTO(personName_DTO);
					//deletePersonCharacteristics if exist
					String errorMsg =deletePersonCharacteristics(aPersonCharList, personCharacteristic_DTO,person_Id);
					if(!errorMsg.isEmpty())
						return errorMsg;
					String personIdstring =personId;
					
					//values for CI_PER_ID Table for old person


					PersonId_Id personId_Id=new PersonId_Id(person_Id, new IdType_Id(idTypeCode));
					PersonId personId=personId_Id.getEntity();
					personIdDto=personId.getDTO();
					personIdDto.setPersonIdNumber(perIdNbr);
					personIdDto.setIsPrimaryId(Bool.TRUE);
					personId.setDTO(personIdDto);

					createUpdateOrDeleteVetRegNbr(vatRegNbr,person_Id,idTypeCode2,personIdstring);

				}else{

					//values for CI_PER Table for New Person

					personDto.setPersonOrBusiness(perOrBusFlg);
					personDto.setLanguageId(new Language_Id("ENG"));
					personDto.setLifeSupportSensitiveLoad( LifeSupportSensitiveLoadLookup.constants.NONE);
					personDto.setAccessGroupId(new AccessGroup_Id(accessGroup));
					personDto.setDivision(cisDivision);
					personDto.setAddress1(merchantInterfaceLookup.getAddress1());
					
					
					
					personDto.setState(valueOfState(checkStateFlag,aInboundMerchantInterfaceId));
					personDto.setCountry(aInboundMerchantInterfaceId.getCountry());
					personDto.newEntity();

					personName_DTO.setId(new PersonName_Id(new Person_Id(personDto.getId().getIdValue()),new BigInteger("1")));
					personName_DTO.setEntityName(entityName);
					personName_DTO.setNameType(NameTypeLookup.constants.PRIMARY);
					personName_DTO.setUppercaseEntityName(entityName);
					personName_DTO.newEntity();
					//values for CI_PER_CHAR Table for New Person
					if (aPersonCharList.isEmpty()) {
						return "false"
								+ "~"
								+ getErrorDescription(String.valueOf(CustomMessages.CHARACTERISTICS_MISSING))
								+ "~"
								+ merchantInterfaceLookup.getErrorMessageCategory()
								+ "~"
								+ String.valueOf(CustomMessages.CHARACTERISTICS_MISSING);
					} 

					setNewPersonCharacteristics(aPersonCharList,personCharacteristic_DTO,personDto);

					//values for CI_PER_ID Table for New Person
					personIdDto.setId(new PersonId_Id(new Person_Id(personDto.getId().getIdValue()), new IdType_Id(idTypeCode)));
					personIdDto.setPersonIdNumber(perIdNbr);
					personIdDto.setIsPrimaryId(Bool.TRUE);
					personIdDto.newEntity();

					if (notBlank(vatRegNbr)) {
						personIdDto.setId(new PersonId_Id(new Person_Id(personDto.getId().getIdValue()), new IdType_Id(idTypeCode2)));
						personIdDto.setPersonIdNumber(vatRegNbr);
						personIdDto.setIsPrimaryId(Bool.FALSE);
						personIdDto.newEntity();
					}
					personId=personDto.getId().getIdValue();
					
					//***********************Set Price Assignment for Tax product****************************//
					priceAssignment(personId,aInboundMerchantInterfaceId);							

				}

			} catch (Exception e) {
				logger.error("Exception in createOrUpdatePerson()",e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			saveChanges();
			
			return "true";
		
		}
		
		/***********
		 * Tax Price Assignment logic
		 * @param aInboundMerchantInterfaceId 
		 * @return 
		 *
		 */
		private void priceAssignment(String partyId, InboundMerchantInterface_Id aInboundMerchantInterfaceId) {
			//*******************Determine owner Id******************//
			String deltaPartyUid="";
			PreparedStatement preparedStatement = null;		
			try {
				preparedStatement = createPreparedStatement("SELECT PA.PARTY_UID FROM CI_PARTY PA " +
						"WHERE PA.PARTY_ID = :partyId AND PA.PARTY_TYPE_FLG= 'PERS' ","");
				preparedStatement.bindString("partyId", partyId, "PARTY_ID");
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				if (notNull(row)) {
					deltaPartyUid = row.getString("PARTY_UID");
				}	
			} catch (Exception e) {
				logger.error("Exception in invoke()", e);
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}	
			
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
				final String[] effectiveDateStringArray1 = aInboundMerchantInterfaceId.getEffectiveDate().split("-", 50);
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
		private String  updateAccountChar(ArrayList<List<String>> accountCharList,Account account){
			AccountCharacteristics accountChars = account.getCharacteristics();
			String effectiveDateString;
			Date effectiveDate;
			AccountCharacteristic_DTO accountCharDto;
			CharacteristicType_Id charTypeId;
			PreparedStatement preparedStatement = null;	
			StringBuilder stringBuilder =null;
			for (int i = 0; i < accountCharList.size(); i++) {
				try{
					stringBuilder = new StringBuilder();
					//Deleting characteristic if existing for any characteristic type code irrespective of effective date
					// RIA: reverse order of filters in query
					stringBuilder.append("DELETE FROM CI_ACCT_CHAR WHERE ACCT_ID = :acctId AND CHAR_TYPE_CD = :charTypeCode ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("acctId", accountId, "ACCT_ID");
					preparedStatement.bindString("charTypeCode", accountCharList.get(i).get(2), "CHAR_TYPE_CD");
					preparedStatement.executeUpdate();
					preparedStatement.setAutoclose(false);
					
					effectiveDateString = accountCharList.get(i).get(1);
					effectiveDate = null;
					if(notBlank(effectiveDateString)) {
						effectiveDate = Date.fromString(effectiveDateString.trim(), new DateFormat("yyyy-MM-dd"));
					}

					//Check if char type exists as an account characteristic
					charTypeId = new CharacteristicType_Id(accountCharList.get(i).get(2));

					//Add new characteristic
					accountCharDto = accountChars.newChildDTO();
					accountCharDto.setCharacteristicValue(accountCharList.get(i).get(3));
					accountChars.add(accountCharDto, charTypeId, effectiveDate);
				} catch (Exception e) {
					logger.error("Exception in createOrUpdatePerson() deleting person characteristics",e);
					String errorMessage = CommonUtils.CheckNull(e.getMessage());
					Map<String, String> errorMsg = new HashMap<String, String>();
					errorMsg = errorList(errorMessage);
					return "false" + "~" + errorMsg.get("Text") + "~"
					+ errorMsg.get("Category") + "~"
					+ errorMsg.get("Number");
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}
			return "";
		}
		private void createAccountChar(ArrayList<List<String>> accountCharList,Account_Id newAccountId ) throws DateFormatParseException {
			AccountCharacteristic_DTO accountCharDto;
			String effectiveDateString;
			Date effectiveDate;
			for (int i = 0; i < accountCharList.size(); i++) {		
				effectiveDateString = accountCharList.get(i).get(1);
				effectiveDate = null;
				if(notBlank(effectiveDateString)) {
					effectiveDate = Date.fromString(effectiveDateString.trim(), new DateFormat("yyyy-MM-dd"));
				}
				accountCharDto = createDTO(AccountCharacteristic.class);
				accountCharDto.setId(new AccountCharacteristic_Id(new CharacteristicType_Id(accountCharList.get(i).get(2)), newAccountId, effectiveDate));
				accountCharDto.setCharacteristicValue(accountCharList.get(i).get(3));
				accountCharDto.newEntity();
			}
		}
		
		private SQLResultRow fetchAcctId(String accountNumberForAccountType,String cisDivision) {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement accountPreparedStatement = null;
			SQLResultRow resultRow=null;
			

			if(isExistingPerson)
			{
				// Check if Account record exists in the system
				stringBuilder.append("SELECT ACCTPER.ACCT_ID " );
				stringBuilder.append(" FROM CI_ACCT_PER ACCTPER, CI_ACCT_NBR ACCTNBR, CI_ACCT ACCT " );
				stringBuilder.append(" WHERE ACCTPER.PER_ID = :personId " );
				stringBuilder.append(" AND ACCTPER.ACCT_ID = ACCT.ACCT_ID " );
				stringBuilder.append(" AND ACCTNBR.ACCT_ID = ACCT.ACCT_ID " );
				stringBuilder.append(" AND ACCTNBR.ACCT_NBR_TYPE_CD = :accountIdentifier " );
				stringBuilder.append(" AND ACCTNBR.ACCT_NBR = :accountNumberForAccountType " );
				stringBuilder.append(" AND ACCT.CIS_DIVISION = :division ");
				accountPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");

				accountPreparedStatement.bindString("personId", personId, "PER_ID");
				accountPreparedStatement.bindString( "accountIdentifier", merchantInterfaceLookup.getExternalAccountIdentifier(), "ACCT_NBR_TYPE_CD");
				accountPreparedStatement.bindString("accountNumberForAccountType", accountNumberForAccountType, "ACCT_NBR");
				accountPreparedStatement.bindString("division", cisDivision, "CIS_DIVISION");
				accountPreparedStatement.setAutoclose(false);

				resultRow = accountPreparedStatement.firstRow();
				accountPreparedStatement.close();
			}
			return resultRow;

		}
		/**
		 * createOrUpdateAccount() method creates a new Account record or updates an existing Account record.
		 *
		 * @param aInboundMerchantInterfaceId
		 * @param getSetUpDate
		 * @param aAccountType
		 * @param accountNumberForAccountType
		 * @param getCurrencyCode
		 * @param getCustClassCode
		 * @param getBillCycleCode
		 * @return
		 */
		private String createOrUpdateAccount(InboundMerchantInterface_Id aInboundMerchantInterfaceId,
				String aAccountType, String accountNumberForAccountType, String getCurrencyCode,
				String getCustClassCode, String getBillCycleCode,String getSetUpDate) {

			String setupDate = CommonUtils.CheckNull(getSetUpDate).trim();
			
			if(isBlankOrNull(setupDate)) {
				return "false"
						+ "~"
						+ getErrorDescription(String.valueOf(CustomMessages.FIELDS_MISSING))
						+ "~"
						+ merchantInterfaceLookup.getErrorMessageCategory()
						+ "~" +String.valueOf(CustomMessages.FIELDS_MISSING);
			}
			
			String cisDivision = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getCisDivision()).trim();
			String currencyCode = CommonUtils.CheckNull(getCurrencyCode).trim();
			String custClassCode = CommonUtils.CheckNull(getCustClassCode).trim();
			String billCycleCode = CommonUtils.CheckNull(getBillCycleCode).trim();
			
			PreparedStatement preparedStatement = null;
			ArrayList<List<String>> accountCharList = getCharData(aInboundMerchantInterfaceId.getTransactionHeaderId(), merchantInterfaceLookup.getAccount(), aAccountType);

			accountId = "";
			
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement accountPreparedStatement = null;
			SQLResultRow resultRow=null;
			
			startChanges();
			
			try {
				resultRow = fetchAcctId(accountNumberForAccountType,cisDivision);
				//If an existing account is found, update it
				if(resultRow!=null && notNull(resultRow)) {
					
					isExistingAccount=true;
					Account_Id existingAccountId = (Account_Id)resultRow.getId("ACCT_ID", Account.class);
					accountId = existingAccountId.getTrimmedValue();

					Date accountSetupDate = null;
					try {
						accountSetupDate = Date.fromString(setupDate.trim(), new DateFormat("yyyy-MM-dd"));
					}
					catch(DateFormatParseException e) {
						logger.error("Exception in date format change",e);
						return "false"
						+ "~"
						+ getErrorDescription(String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION))
						+ "~"
						+ merchantInterfaceLookup.getErrorMessageCategory()
						+ "~" +String.valueOf(CustomMessages.RUN_TIME_ERROR_IN_EXECUTION);
					}
					Account account = existingAccountId.getEntity();
					
					//Get old bill cycle code
					String oldBillCycleCode = account.getBillCycleId().getTrimmedValue();
					
					Account_DTO accountDto = account.getDTO();
					accountDto.setBillCycleId(new BillCycle_Id(billCycleCode));
					accountDto.setSetUpDate(accountSetupDate);
					accountDto.setCurrencyId(new Currency_Id(currencyCode));
					accountDto.setDivisionId(new CisDivision_Id(cisDivision));
					accountDto.setCollectionClassId(new CollectionClass_Id(merchantInterfaceLookup.getCollectionClass()));
					accountDto.setCustomerClassId(new CustomerClass_Id(custClassCode));
					accountDto.setAccessGroupId(new AccessGroup_Id(merchantInterfaceLookup.getAccessGroup()));
					accountDto.setBusinessObjectDataArea(" ");
					account.setDTO(accountDto);
					
					//Handle bills if bill cycle has changed
					if(oldBillCycleCode.compareToIgnoreCase(billCycleCode) != 0) {
						handleBillsOnBillCycleChange(account.getId());
					}

					//Update account characteristics
					String errorMsg = updateAccountChar(accountCharList, account);
					if(!errorMsg.isEmpty())
						return errorMsg;
					
				}
				//Else, add new account
				else {
					//Create account
					
					isExistingAccount=false;
					Date accountSetupDate = Date.fromString(setupDate, new DateFormat("yyyy-MM-dd"));
					Account_DTO accountDto = createDTO(Account.class);
					accountDto.setSetUpDate(accountSetupDate);
					accountDto.setCurrencyId(new Currency_Id(currencyCode));
					accountDto.setDivisionId(new CisDivision_Id(cisDivision));
					accountDto.setCollectionClassId(new CollectionClass_Id(merchantInterfaceLookup.getCollectionClass()));
					accountDto.setCustomerClassId(new CustomerClass_Id(custClassCode));
					accountDto.setAccessGroupId(new AccessGroup_Id(merchantInterfaceLookup.getAccessGroup()));
					Account account = addAccountEntity(accountDto);
					Account_Id newAccountId = account.getId();
					accountId = newAccountId.getTrimmedValue();

					//Create account characteristics
					AccountCharacteristic_DTO accountCharDto;
					String effectiveDateString;
					Date effectiveDate;
					createAccountChar(accountCharList,newAccountId);
					//Create IGA characteristic with effective date the same as account setup date
					accountCharDto = createDTO(AccountCharacteristic.class);
					accountCharDto.setId(new AccountCharacteristic_Id(new CharacteristicType_Id(merchantInterfaceLookup.getIgaCharacteristic()), newAccountId, accountSetupDate));
					accountCharDto.setCharacteristicValue(merchantInterfaceLookup.getIgaCharacteristicLabel());
					accountCharDto.newEntity();

					//Create account person
					AccountPerson_DTO accountPersonDto = createDTO(AccountPerson.class);
					accountPersonDto.setBillFormat(BillFormatLookup.constants.DETAILED);
					accountPersonDto.setAccountRelationshipTypeId(new AccountRelationshipType_Id(merchantInterfaceLookup.getAccountRelationshipType()));
					accountPersonDto.setIsMainCustomer(Bool.TRUE);
					accountPersonDto.setIsFinanciallyResponsible(Bool.TRUE);
					accountPersonDto.setReceivesNotification(Bool.TRUE);
					accountPersonDto.setShouldReceiveCopyOfBill(Bool.TRUE);
					accountPersonDto.setBillRouteTypeId(new BillRouteType_Id( merchantInterfaceLookup.getBillRouteType()));
					accountPersonDto.setNumberOfBillCopies(new BigInteger(merchantInterfaceLookup.getBillCopies()));
					accountPersonDto.setBillAddressSource(BillingAddressSourceLookup.constants.PERSON);
					accountPersonDto.setId(new AccountPerson_Id(new Person_Id(personId), newAccountId));
					AccountPerson accountPerson = accountPersonDto.newEntity();
					
					//NAP-24121 : RIA: Create entry in CI_ACCT_PER_ROUTING table also.
					
					
					//DTO has product bug so using insert statement for now.
					
					StringBuilder sb = new StringBuilder();
					sb.append(" Insert into CI_ACCT_PER_ROUTING (ACCT_ID,PER_ID,BILL_RTE_TYPE_CD,SEQ_NUM,RECEIVE_COPY_SW,BILL_FORMAT_FLG,NBR_BILL_COPIES,CUST_PO_ID,NOTIFY_SW,BILL_ADDR_SRCE_FLG,ADDRESS_ID,VERSION) ");
					sb.append(" values (:acct,:per ,:billRteType,1,:rcvCopy,:billFormat,:nbrCopy,' ',:notifySw,:billAddressSrc,' ',1) ");
					PreparedStatement ps = createPreparedStatement(sb.toString(),"accountPersonRouting");
					ps.bindId("acct", newAccountId);
					ps.bindId("per", new Person_Id(personId));
					ps.bindId("billRteType", new BillRouteType_Id( merchantInterfaceLookup.getBillRouteType()));
					ps.bindBoolean("rcvCopy", Bool.TRUE);
					ps.bindLookup("billFormat", BillFormatLookup.constants.DETAILED);
					ps.bindBigInteger("nbrCopy", new BigInteger(merchantInterfaceLookup.getBillCopies()));
					ps.bindBoolean("notifySw", Bool.TRUE);
					ps.bindLookup("billAddressSrc", BillingAddressSourceLookup.constants.PERSON);
					ps.execute();
					if(notNull(ps)){
						ps.close();
						ps=null;
					}
					
					//Create external account identifier - primary
					AccountNumber_DTO accountNumberDto = createDTO(AccountNumber.class);
					accountNumberDto.setId(new AccountNumber_Id(new AccountNumberType_Id(merchantInterfaceLookup.getExternalAccountIdentifier()),newAccountId));
					accountNumberDto.setIsPrimaryId(Bool.TRUE);
					accountNumberDto.setAccountNumber(accountNumberForAccountType);
					accountNumberDto.newEntity();

					//Create internal account identifier
					accountNumberDto = createDTO(AccountNumber.class);
					accountNumberDto.setId(new AccountNumber_Id(new AccountNumberType_Id(merchantInterfaceLookup.getInternalAccountIdentifier()), newAccountId));
					accountNumberDto.setIsPrimaryId(Bool.FALSE);
					accountNumberDto.setAccountNumber(aAccountType);
					accountNumberDto.newEntity();
					
					// Set bill cycle on account
                    stringBuilder = new StringBuilder();
                    stringBuilder.append("UPDATE CI_ACCT SET BILL_CYC_CD =:billCycleCd WHERE ACCT_ID=:acctId");
                    preparedStatement = createPreparedStatement(stringBuilder.toString(),"Set account bill cycle");
                    preparedStatement.bindId("billCycleCd", new BillCycle_Id(billCycleCode));
                    preparedStatement.bindId("acctId", newAccountId);
                    preparedStatement.executeUpdate();
				}

			} catch (Exception e) {
				logger.error("Exception in account creation",e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			} finally {
				stringBuilder = null;
				if (accountPreparedStatement != null) {
					accountPreparedStatement.close();
					accountPreparedStatement = null;
				}
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			saveChanges();
			
			return "true";
		}
		
		/**
		 * When bill cycle is changed, update ADHOC_BILL_SW='Y' on completed regular bills belonging to the current 
		 * window of the new bill cycle. Also, delete pending regular bill if existing with no frozen bill segment.
		 * @param accountId
		 */
		private void handleBillsOnBillCycleChange(Account_Id accountId) {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			
			//To avoid multiple completed bills being picked up during billing, set ADHOC_BILL_SW='Y'
			//on completed regular bill belong to the new bill cycle's current window
			try {
				stringBuilder.append("UPDATE CISADM.CI_BILL U SET U.ADHOC_BILL_SW='Y' ");
				stringBuilder.append("WHERE EXISTS ( SELECT 1 ");
				stringBuilder.append("FROM CISADM.CI_BILL B, CISADM.CI_ACCT A, CISADM.CI_BILL_CYC_SCH S ");
				stringBuilder.append("WHERE A.ACCT_ID=B.ACCT_ID ");
				stringBuilder.append("AND B.ACCT_ID=:acctId ");
				stringBuilder.append("AND (A.BILL_CYC_CD<>B.BILL_CYC_CD AND B.BILL_CYC_CD<>' ') ");
				stringBuilder.append("AND S.BILL_CYC_CD=A.BILL_CYC_CD ");
				stringBuilder.append("AND TRUNC(SYSDATE-1) BETWEEN S.WIN_START_DT AND S.WIN_END_DT ");
				stringBuilder.append("AND B.WIN_START_DT BETWEEN S.WIN_START_DT AND S.WIN_END_DT ");
				stringBuilder.append("AND B.BILL_STAT_FLG='C' AND B.ADHOC_BILL_SW='N' ");
				stringBuilder.append("AND U.BILL_ID=B.BILL_ID)");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindId("acctId", accountId);
				preparedStatement.executeUpdate();
				
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
			
			//Check if a pending regular bill exists and mark it for delete
			stringBuilder = new StringBuilder();
			preparedStatement = null;
			try {
				stringBuilder.append("SELECT BILL_ID FROM CI_BILL B ");
				stringBuilder.append("WHERE B.ACCT_ID=:acctId ");
				stringBuilder.append("AND B.BILL_CYC_CD<>' ' ");
				stringBuilder.append("AND B.BILL_STAT_FLG='P' ");
				stringBuilder.append("AND B.ADHOC_BILL_SW='N' ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindId("acctId", accountId);
				preparedStatement.setAutoclose(false);
				SQLResultRow row = preparedStatement.firstRow();
				
				//Add a CM_BCYCH char on the existing pending bill to mark it for delete
				if(notNull(row)) {
					Bill_Id billId = (Bill_Id)row.getId("BILL_ID", Bill.class);
					
					CharacteristicType_Id deletePendingBillCharTypeId = new CharacteristicType_Id("CM_BCYCH");
					
					//Remove existing bill char
					PreparedStatement deleteBillCharPrepStatement = null;
					row = null;
					try {
						stringBuilder = new StringBuilder();
						stringBuilder.append("DELETE FROM CI_BILL_CHAR ");
						stringBuilder.append("WHERE BILL_ID=:billId AND CHAR_TYPE_CD=:charType ");
						deleteBillCharPrepStatement = createPreparedStatement(stringBuilder.toString(),"");
						deleteBillCharPrepStatement.bindId("billId", billId);
						deleteBillCharPrepStatement.bindId("charType", deletePendingBillCharTypeId);
						deleteBillCharPrepStatement.executeUpdate();
						
					} finally {
						if(deleteBillCharPrepStatement!=null) {
							deleteBillCharPrepStatement.close();
						}
					}

					//Add bill char
					BillCharacteristic_DTO billCharDto = createDTO(BillCharacteristic.class);
					billCharDto.setId(new BillCharacteristic_Id(deletePendingBillCharTypeId, billId, BigInteger.TEN));
					billCharDto.setCharacteristicValue("Y");
					billCharDto.newEntity();
				}
			} finally {
				if (preparedStatement != null) {
					preparedStatement.close();
				}
			}
		}
		
		/**
		 * Handles the creation of the account
		 * @param accountDto
		 * @return Account
		 */
		private Account addAccountEntity(Account_DTO accountDto) {
			Account account = null;
			try{
				account = accountDto.newEntity();
			}
			catch(ApplicationError e){
				if(e.getMessage().contains(SQL_ERROR_CODE_999999993)){
					logger.warn("Caught unique constraint violation in creating an account", e);
					account = accountDto.newEntity();
				}
				else{
					throw e;
				}
			}
			return account;
		}
		
		private String deleteContractChar(ArrayList<List<String>> saCharList,ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO,ServiceAgreement_Id serviceAgreement_Id) {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement preparedStatement = null;
			for (int i =0; i<saCharList.size(); i++) {
				try{
					
//							values for CI_SA_CHAR with char type code as sa_id
					stringBuilder = new StringBuilder();
					//Deleting characteristic if existing for any characteristic type code irrespective of effective date
					// RIA: reverse order of filters in query
					stringBuilder.append("DELETE FROM CI_SA_CHAR WHERE SA_ID = :saId AND CHAR_TYPE_CD = :charTypeCode ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("saId", contractId, "SA_ID");
					preparedStatement.bindString("charTypeCode", saCharList.get(i).get(2), "CHAR_TYPE_CD");
					preparedStatement.executeUpdate();
					preparedStatement.setAutoclose(false);
					
					serviceAgreementCharacteristic_DTO.setAdhocCharacteristicValue(saCharList.get(i).get(3));
					if(notBlank(saCharList.get(i).get(2))) {
						String[] effectiveDateStringArray1 = saCharList.get(i).get(1).split("-", 50);
						Date effective_date=new Date(
								Integer.parseInt(effectiveDateStringArray1[0]),
								Integer.parseInt(effectiveDateStringArray1[1]),
								Integer.parseInt(effectiveDateStringArray1[2]));
						serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_Id,new CharacteristicType_Id(saCharList.get(i).get(2)),effective_date));
					}else{
						serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_Id,new CharacteristicType_Id(saCharList.get(i).get(2)),null));
					}
					serviceAgreementCharacteristic_DTO.newEntity();
				}catch (Exception e) {
					logger.error("Exception in contract creation ", e);
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
			}
			return "";
		}
		private void setDatesforCISAChar(ServiceAgreement_DTO serviceAgreement_DTO,String getEndAcctDt,String setupDate) {
			if(notBlank(getEndAcctDt)){
				serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);
				String [] startDateStringArray2 = new String[3];
				startDateStringArray2 = setupDate.split("-", 50);

				serviceAgreement_DTO.setStartDate(new Date(Integer.parseInt(startDateStringArray2[0]), 
						Integer.parseInt(startDateStringArray2[1]), 
						Integer.parseInt(startDateStringArray2[2])));

				String [] endDateStringArray = new String[3];
				endDateStringArray = getEndAcctDt.split("-", 50);
				serviceAgreement_DTO.setEndDate(new Date(Integer.parseInt(endDateStringArray[0]), 
						Integer.parseInt(endDateStringArray[1]), 
						Integer.parseInt(endDateStringArray[2])));

			} else {

				serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
				String [] startDateStringArray3 = new String[3];
				startDateStringArray3 = setupDate.split("-", 50);
				serviceAgreement_DTO.setStartDate(new Date(Integer.parseInt(startDateStringArray3[0]), 
						Integer.parseInt(startDateStringArray3[1]), 
						Integer.parseInt(startDateStringArray3[2])));
				serviceAgreement_DTO.setEndDate(null);

			}
		}
		private void setValuesForCISACharForContracts(ArrayList<List<String>> saCharList,ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO,
				ServiceAgreement_DTO serviceAgreement_DTO)  {

			for (int i =0; i<saCharList.size(); i++) {
				serviceAgreementCharacteristic_DTO.setAdhocCharacteristicValue(saCharList.get(i).get(3));
				if(notBlank(saCharList.get(i).get(2))) {
					String[] effectiveDateStringArray1 = saCharList.get(i).get(1).split("-", 50);
					Date effective_date=new Date(
							Integer.parseInt(effectiveDateStringArray1[0]),
							Integer.parseInt(effectiveDateStringArray1[1]),
							Integer.parseInt(effectiveDateStringArray1[2]));
					serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(saCharList.get(i).get(2)),effective_date));
				}else{
					serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(saCharList.get(i).get(2)),null));
				}
				serviceAgreementCharacteristic_DTO.newEntity();
			}
		}
		private void setIdforCISAChar(String setupDate,ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO,
				ServiceAgreement_Id serviceAgreement_Id) {
			if(notBlank(setupDate)) {
				String[] effectiveDateStringArray = setupDate.split("-", 50);
				Date effective_date=new Date(Integer.parseInt(effectiveDateStringArray[0]),
						Integer.parseInt(effectiveDateStringArray[1]),
						Integer.parseInt(effectiveDateStringArray[2]));
				serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_Id,new CharacteristicType_Id(merchantInterfaceLookup.getSaCharacteristic()),effective_date));
			}else{
				serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_Id,new CharacteristicType_Id(merchantInterfaceLookup.getSaCharacteristic()),null));
			}
		}
		
		/**
		 * createOrUpdateContract() method creates a new contract record or updates an existing contract record.
		 * 
		 * @param aInboundMerchantInterfaceId
		 * @param getSetUpDate
		 * @param contractTypeFlg
		 * @return
		 */

		private String createOrUpdateContract(InboundMerchantInterface_Id aInboundMerchantInterfaceId,
				String contractTypeFlg, String getSetUpDate, String getEndAcctDt) {
			
			String cisDivision = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getCisDivision()).trim();
			String setupDate = CommonUtils.CheckNull(getSetUpDate).trim();

			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement contractPreparedStatement = null;
			PreparedStatement preparedStatement = null;
			SQLResultRow resultRow=null;
			contractId = "";
			ServiceAgreement_DTO serviceAgreement_DTO=new ServiceAgreement_DTO();
			ServiceAgreementCharacteristic_DTO serviceAgreementCharacteristic_DTO=new ServiceAgreementCharacteristic_DTO();
			ArrayList<List<String>> saCharList = new ArrayList<List<String>>();

			saCharList = getCharData(aInboundMerchantInterfaceId.getTransactionHeaderId(), merchantInterfaceLookup.getSa(), contractTypeFlg);

			startChanges();
			
			try {
				if(isExistingAccount)
				{
				stringBuilder.append("SELECT SA_ID FROM CI_SA A WHERE A.ACCT_ID=:accountId ");
				stringBuilder.append(" AND A.SA_TYPE_CD=:saTypeCd AND A.CIS_DIVISION = :division AND A.SA_STATUS_FLG in (:active, :pendingStop) " );
				stringBuilder.append(" AND NOT EXISTS (SELECT SA_ID FROM CI_SA_CHAR B WHERE B.CHAR_TYPE_CD = :charTypeCd AND A.SA_ID = B.SA_ID ) ");
				contractPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				contractPreparedStatement.bindString("accountId", accountId, "ACCT_ID");
				contractPreparedStatement.bindString("saTypeCd", contractTypeFlg, "SA_TYPE_CD");
				contractPreparedStatement.bindString("division", cisDivision, "CIS_DIVISION");
				contractPreparedStatement.bindString("charTypeCd", "C1_SAFCD", "CHAR_TYPE_CD");
				contractPreparedStatement.bindLookup(ACTIVE, ServiceAgreementStatusLookup.constants.ACTIVE);
				contractPreparedStatement.bindLookup(PENDIGSTOP, ServiceAgreementStatusLookup.constants.PENDING_STOP);
				contractPreparedStatement.setAutoclose(false);
				
                resultRow =  contractPreparedStatement.firstRow();
                
				if (resultRow!=null && notNull(resultRow)) {
				contractId = resultRow.getString("SA_ID");
				
					ServiceAgreement_Id serviceAgreement_Id=new ServiceAgreement_Id(contractId);
					ServiceAgreement serviceAgreement=serviceAgreement_Id.getEntity();
					serviceAgreement_DTO=serviceAgreement.getDTO();
					

					//values for CI_SA
					serviceAgreement_DTO.setAccountId(new Account_Id(accountId));
					setDatesforCISAChar(serviceAgreement_DTO,getEndAcctDt,setupDate);
					serviceAgreement_DTO.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisDivision),contractTypeFlg));
					serviceAgreement_DTO.setCustomerRead(CustomerReadLookup.constants.NO);
					serviceAgreement.setDTO(serviceAgreement_DTO);

					//					values for CI_SA_CHAR with char_type_cd as SA_ID
					
//					values for CI_SA_CHAR with char type code as sa_id
					stringBuilder = new StringBuilder();
					//Deleting characteristic if existing for any characteristic type code irrespective of effective date
					// RIA: reverse order of filters in query
					stringBuilder.append("DELETE FROM CI_SA_CHAR WHERE SA_ID = :saId AND CHAR_TYPE_CD = :charTypeCode ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindString("saId", contractId, "SA_ID");
					preparedStatement.bindString("charTypeCode", merchantInterfaceLookup.getSaCharacteristic(), "CHAR_TYPE_CD");
					preparedStatement.executeUpdate();
					preparedStatement.setAutoclose(false);

					preparedStatement.close();
					preparedStatement= null;
					
						serviceAgreementCharacteristic_DTO.setAdhocCharacteristicValue(contractId);
						setIdforCISAChar(setupDate,serviceAgreementCharacteristic_DTO,serviceAgreement_Id);
						serviceAgreementCharacteristic_DTO.newEntity();	

						//values for CI_SA_CHAR for Contract				
						String errorMsg = deleteContractChar(saCharList,serviceAgreementCharacteristic_DTO,serviceAgreement_Id);
						if(!errorMsg.isEmpty())
							return errorMsg;
}
				else {
					return "false"
							+ "~"
							+ getErrorDescription(String.valueOf(CustomMessages.ACTIVE_CONTRACT_MISSING))
							+ "~"
							+ merchantInterfaceLookup.getErrorMessageCategory()
							+ "~"
							+ String.valueOf(CustomMessages.ACTIVE_CONTRACT_MISSING);
				}
				}else{
					//values for CI_SA
					String [] startDateStringArray = new String[3];
					startDateStringArray = setupDate.split("-", 50);
					Date start_date= new Date(Integer.parseInt(startDateStringArray[0]), Integer.parseInt(startDateStringArray[1]), Integer.parseInt(startDateStringArray[2]));
					serviceAgreement_DTO.setAccountId(new Account_Id(accountId));
					serviceAgreement_DTO.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
					serviceAgreement_DTO.setStartDate(start_date);
					serviceAgreement_DTO.setServiceAgreementTypeId(new ServiceAgreementType_Id(new CisDivision_Id(cisDivision),contractTypeFlg));
					serviceAgreement_DTO.setCustomerRead(CustomerReadLookup.constants.NO);
					addSaEntity(serviceAgreement_DTO);
					contractId=serviceAgreement_DTO.getEntity().getId().getIdValue();

					//values for CI_SA_CHAR 
					serviceAgreementCharacteristic_DTO.setAdhocCharacteristicValue(contractId);
					if(notBlank(setupDate)) {
						String[] effectiveDateStringArray = setupDate.split("-", 50);
						Date effective_date=new Date(Integer.parseInt(effectiveDateStringArray[0]),
								Integer.parseInt(effectiveDateStringArray[1]),
								Integer.parseInt(effectiveDateStringArray[2]));
						serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(merchantInterfaceLookup.getSaCharacteristic()),effective_date));
					}else{
						serviceAgreementCharacteristic_DTO.setId(new ServiceAgreementCharacteristic_Id(serviceAgreement_DTO.getId(),new CharacteristicType_Id(merchantInterfaceLookup.getSaCharacteristic()),null));
					}
					serviceAgreementCharacteristic_DTO.newEntity();	

					//values for CI_SA_CHAR for Contract				
					setValuesForCISACharForContracts(saCharList,serviceAgreementCharacteristic_DTO,serviceAgreement_DTO);
					contractId=serviceAgreement_DTO.getEntity().getId().getIdValue();
				}

			}catch (Exception e) {
				logger.error("Exception in contract creation ", e);
				String errorMessage = CommonUtils.CheckNull(e.getMessage());
				Map<String, String> errorMsg = new HashMap<String, String>();
				errorMsg = errorList(errorMessage);
				return "false" + "~" + errorMsg.get("Text") + "~"
				+ errorMsg.get("Category") + "~"
				+ errorMsg.get("Number");
			} finally {
				stringBuilder = null;
				if (contractPreparedStatement != null) {
					contractPreparedStatement.close();
					contractPreparedStatement = null;
				}
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}
			
			saveChanges();
			
			return "true";
		}
		//
		/**
		 * Handles the creation of contract
		 * @param serviceAgreement_DTO
		 */
		private void addSaEntity(ServiceAgreement_DTO serviceAgreement_DTO) {
			try{
				serviceAgreement_DTO.newEntity();
			}
			catch(ApplicationError e){
				if(e.getMessage().contains(SQL_ERROR_CODE_999999993)){
					logger.warn("Caught unique constraint violation in creating a contract", e);
					serviceAgreement_DTO.newEntity();
				}
				else{
					throw e;
				}
			}
		}
		
		/**
		 * contractEndDateUpdate() method creates a new contract record or updates an existing contract record.
		 * 
		 * @param aInboundMerchantInterfaceId
		 * @return
		 */
		private String contractEndDateUpdate(InboundMerchantInterface_Id aInboundMerchantInterfaceId) {

			logger.debug("Inside contractEndDateUpdate method for person id as " + personId);

			String cisDivision = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getCisDivision()).trim();
			String endDate = CommonUtils.CheckNull(aInboundMerchantInterfaceId.getEndDtMerchant()).trim();
			contractId = "";

			startChanges();

			if (notBlank(endDate)) {
				String  [] endDateStringArray = new String[3];
				endDateStringArray = endDate.split("-", 50);
				Date endDt = new Date(Integer
						.parseInt(endDateStringArray[0]), Integer
						.parseInt(endDateStringArray[1]), Integer
						.parseInt(endDateStringArray[TWO]));
				PreparedStatement preparedStatement = null;
				StringBuilder stringBuilder = new StringBuilder();
				try {
					stringBuilder.append("UPDATE CI_SA SET SA_STATUS_FLG = :pendingStop, END_DT = :endDate ");
					stringBuilder.append(" WHERE ACCT_ID IN (SELECT ACCT_ID FROM CI_ACCT_PER WHERE PER_ID = :personId ");
					stringBuilder.append(" AND ACCT_REL_TYPE_CD = :relTypeCd) AND CIS_DIVISION = :cisDivision AND SA_STATUS_FLG = :active ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindLookup(PENDIGSTOP, ServiceAgreementStatusLookup.constants.PENDING_STOP);
					preparedStatement.bindLookup(ACTIVE, ServiceAgreementStatusLookup.constants.ACTIVE);
					preparedStatement.bindString("relTypeCd", merchantInterfaceLookup.getAccountRelationshipType(), "ACCT_REL_TYPE_CD");
					preparedStatement.bindString(CISDIVISION, cisDivision, "CIS_DIVISION");
					preparedStatement.bindString("personId", personId,	"PER_ID");
					preparedStatement.bindDate("endDate", endDt);
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error(EXCEPTIONOCCURED , e);
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			} else {
				PreparedStatement preparedStatement = null;
				StringBuilder stringBuilder = new StringBuilder();
				try {
					stringBuilder.append("UPDATE CI_SA SET SA_STATUS_FLG = :active WHERE ACCT_ID IN ");
					stringBuilder.append(" (SELECT ACCT_ID FROM CI_ACCT_PER WHERE PER_ID = :personId ");
					stringBuilder.append(" AND ACCT_REL_TYPE_CD = :relTypeCd) AND CIS_DIVISION = :cisDivision AND SA_STATUS_FLG = :pendingStop ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
					preparedStatement.bindLookup(PENDIGSTOP, ServiceAgreementStatusLookup.constants.PENDING_STOP);
					preparedStatement.bindLookup(ACTIVE, ServiceAgreementStatusLookup.constants.ACTIVE);
					preparedStatement.bindString("relTypeCd", merchantInterfaceLookup.getAccountRelationshipType(), "ACCT_REL_TYPE_CD");
					preparedStatement.bindString(CISDIVISION, cisDivision, "CIS_DIVISION");
					preparedStatement.bindString("personId", personId,	"PER_ID");
					preparedStatement.executeUpdate();
				} catch (Exception e) {
					logger.error(EXCEPTIONOCCURED , e);
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			}

			saveChanges();

			return "true";
		}

		/**
		 * logError() method stores the error information in the List and does rollback of all the database transaction of this unit.
		 * 
		 * @param aTransactionHeaderId
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aActualErrorMessageNumber
		 * @param aErrorMessage
		 * @param skipRemainingRows
		 * @return
		 */

		private boolean logError(String aTransactionHeaderId, String aMessageCategory, String aMessageNumber,
				String aActualErrorMessageNumber, String aErrorMessage, String skipRemainingRows, String aPerIdNbr, String aCisDivision) {
			logger.debug("Inside logError method");

			eachCustomerStatusList = new ArrayList<String>();
			eachCustomerStatusList.add(0, aTransactionHeaderId);
			eachCustomerStatusList.add(1, merchantInterfaceLookup.getError());
			eachCustomerStatusList.add(2, aMessageCategory);
			eachCustomerStatusList.add(3, aMessageNumber);
			eachCustomerStatusList.add(4, aActualErrorMessageNumber);
			eachCustomerStatusList.add(5, aErrorMessage);
			eachCustomerStatusList.add(6, skipRemainingRows);
			eachCustomerStatusList.add(7, aPerIdNbr);
			eachCustomerStatusList.add(8, aCisDivision);
			updateCustomerStatusList.add(eachCustomerStatusList);
			eachCustomerStatusList = null;


			//Clear all the lists
			fundAccountTypeList.clear();
			chrgAccountTypeList.clear();
			chbkAccountTypeList.clear();
			crwdAccountTypeList.clear();

			//Rolling back to SavePoint in order to reverse database transactions
			rollbackToSavePoint("Rollback".concat(getBatchThreadNumber().toString()));

			// Does rollback for this unit and the code exits from
			// executeWorkUnit for this unit

			if((aMessageCategory.equals(merchantInterfaceLookup.getErrorMessageCategory())&& !aActualErrorMessageNumber.trim().equals("0"))) {
				addError(CustomMessageRepository.merchantError(
						aActualErrorMessageNumber));
			}

			return false; // intentionally kept false as rollback has to occur here
		}

		/**
		 * updateStagingStatus() method updates the CM_MERCH_STG table with processing status.
		 * 
		 * @param aTransactionHeaderId
		 * @param aStatus
		 * @param aMessageCategoryNumber
		 * @param aPerIdNbr
		 * @param aMessageNumber
		 * @param aErrorDescription
		 * @param aCisDivision
		 */
		private void updateStagingStatus(final String aTransactionHeaderId,
				String aStatus, String aMessageCategoryNumber,
				String aMessageNumber, String aActualErrorMessageNumber,
				String aErrorDescription, String skipRemainingRows,
				String aPerIdNbr, String aCisDivision) {
			logger.debug("Inside updateStagingStatus() method");

			if (aActualErrorMessageNumber.trim().equals("0")) {
				aActualErrorMessageNumber = aMessageNumber;
			}

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			try {
				if (CommonUtils.CheckNull(aErrorDescription).trim().length() > 250) {
					aErrorDescription = aErrorDescription.substring(0, 250);
				}
				stringBuilder.append("UPDATE CM_MERCH_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, ");
				stringBuilder.append(" MESSAGE_CAT_NBR =:messageCategory, MESSAGE_NBR =:actualErrorMessageNumber, ERROR_INFO =:errorDescription ");
				if(merchantInterfaceLookup.getCompleted().equalsIgnoreCase(aStatus)){
					stringBuilder.append(",ILM_ARCH_SW ='Y' ");
				}
				stringBuilder.append(" WHERE TXN_HEADER_ID = :headerId " );

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("status", aStatus.trim(), "BO_STATUS_CD");
				preparedStatement.bindString("messageCategory", aMessageCategoryNumber, "MESSAGE_CAT_NBR");
				preparedStatement.bindString("actualErrorMessageNumber", aActualErrorMessageNumber, "MESSAGE_NBR");
				preparedStatement.bindString("errorDescription", aErrorDescription, "ERROR_INFO");
				preparedStatement.bindString("headerId", aTransactionHeaderId.trim(), "TXN_HEADER_ID");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error(EXCEPTIONOCCURED , e);

			} finally {
				stringBuilder = null;
				if (preparedStatement != null) {
					preparedStatement.close();
					preparedStatement = null;
				}
			}

			//			This logic is required to update those rows, with corresponding error messages, which were skipped for processing through execute work unit
			//since one row with same parent and child merchant combination already has got failed
			if (CommonUtils.CheckNull(skipRemainingRows)
					.trim().startsWith("true")){
				logger.debug("updateStagingStatus For updation of Other Rows with same PER_ID_NBR method");			
				try {
					aMessageCategoryNumber="0";
					aMessageNumber="0";
					aErrorDescription="Row couldn't be processed: One row is already in error for same PER_ID_NBR.";

					stringBuilder = new StringBuilder();
					stringBuilder.append("UPDATE CM_MERCH_STG SET BO_STATUS_CD =:status, STATUS_UPD_DTTM = SYSTIMESTAMP, " );
					stringBuilder.append(" MESSAGE_CAT_NBR = :messageCategory, MESSAGE_NBR = :actualErrorMessageNumber, ERROR_INFO = :errorDescription " );
					stringBuilder.append(" WHERE ((TXN_HEADER_ID >:headerId)) ");
					stringBuilder.append(" AND PER_ID_NBR=:perIdNbr AND CIS_DIVISION = :cisDivision ");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");						
					preparedStatement.bindString("status", merchantInterfaceLookup.getUpload().trim(), "BO_STATUS_CD");
					preparedStatement.bindString("messageCategory", aMessageCategoryNumber, "MESSAGE_CAT_NBR");
					preparedStatement.bindString("actualErrorMessageNumber", aMessageNumber, "MESSAGE_NBR");
					preparedStatement.bindString("errorDescription", aErrorDescription, "ERROR_INFO");
					preparedStatement.bindString("headerId", aTransactionHeaderId, "TXN_HEADER_ID");
					preparedStatement.bindString("perIdNbr", aPerIdNbr, "PER_ID_NBR");
					preparedStatement.bindString(CISDIVISION, aCisDivision, "CIS_DIVISION");

					final int rowsUpdated=preparedStatement.executeUpdate();
					logger.debug("Rows updated"+rowsUpdated);
				} catch (Exception e) {
					logger.error(EXCEPTIONOCCURED , e);
				} finally {
					stringBuilder = null;
					if (preparedStatement != null) {
						preparedStatement.close();
						preparedStatement = null;
					}
				}
			} //End of if loop
		}// end method
		/**
		 * Map Interface is used to retrieve the Error Message Text, Error
		 * Message Number and Error message Category from Application Error
		 * Messages
		 * 
		 * 
		 */
		public Map<String, String> errorList(String aErrorMessage) {
			Map<String, String> errorMap = new HashMap<String, String>();
			String errorMessageNumber = "";
			String errorMessageCategory = "";
			if (aErrorMessage.contains("Number:")) {
				errorMessageNumber = aErrorMessage.substring(aErrorMessage.indexOf("Number:") + 8, 
						aErrorMessage.indexOf("Call Sequence:"));
				errorMap.put("Number", errorMessageNumber);
			}
			if (aErrorMessage.contains("Category:")) {
				errorMessageCategory = aErrorMessage.substring(aErrorMessage.indexOf("Category:") + 10, 
						aErrorMessage.indexOf("Number"));
				errorMap.put("Category", errorMessageCategory);
			}
			if (aErrorMessage.contains("Text:")
					&& aErrorMessage.contains("Description:")) {
				aErrorMessage = aErrorMessage.substring(aErrorMessage.indexOf("Text:"), 
						aErrorMessage.indexOf("Description:"));
			}
			if (aErrorMessage.length() > 250) {
				aErrorMessage = aErrorMessage.substring(0, 250);
				errorMap.put("Text", aErrorMessage);
			} else {
				aErrorMessage = aErrorMessage.substring(0, aErrorMessage.length());
				errorMap.put("Text", aErrorMessage);
			}
			return errorMap;
		}

		/**
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 * 
		 * @return errorInfo
		 */
		public static String getErrorDescription(final String messageNumber) {
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
		 * getErrorDescription() method selects error message description from ORMB message catalog.
		 * 
		 * @return errorInfo
		 */

		public ArrayList<List<String>> getCharData(String txnHeaderId, String charEntity, String entityCode) {
			logger.debug("Inside getCharData()");
			
			ArrayList<List<String>> charList = new ArrayList<List<String>>();
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			String transactionHeaderId = "";
			String charEffectiveDate = "";
			String charTypeCode = "";
			String charValue = "";
			stringBuilder.append(" SELECT CHARM.TXN_HEADER_ID, CHARM.EFFDT, CHARM.CHAR_TYPE_CD, " );
			stringBuilder.append(" CHARM.CHAR_VAL FROM CM_MERCH_CHAR CHARM, CM_MERCH_CHAR_TMP TMP " );
			stringBuilder.append(" WHERE CHARM.TXN_HEADER_ID = :txnHeaderId " );
			stringBuilder.append(" AND CHARM.CHAR_TYPE_CD = TMP.CHAR_TYPE_CD " );
			stringBuilder.append(" AND TMP.CHAR_ENTITY = :charEntity ");
			// RIA: effective date = max(effective date) from CM_MERCH_CHAR
			stringBuilder.append(" AND CHARM.EFFDT = (SELECT MAX(A.EFFDT) FROM CM_MERCH_CHAR A WHERE A.TXN_HEADER_ID = CHARM.TXN_HEADER_ID AND A.CHAR_TYPE_CD = CHARM.CHAR_TYPE_CD) ");

			if(!(notBlank(entityCode))){

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("txnHeaderId", txnHeaderId.trim(), "TXN_HEADER_ID");
				preparedStatement.bindString("charEntity", charEntity.trim(), "ENTITY_TYPE");//CHAR_ENTITY
				
			} else {
				stringBuilder.append(" AND TMP.ENTITY_CD = :entityCode");

				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("txnHeaderId", txnHeaderId.trim(), "TXN_HEADER_ID");
				preparedStatement.bindString("charEntity", charEntity.trim(), "ENTITY_TYPE");//CHAR_ENTITY
				preparedStatement.bindString("entityCode", entityCode.trim(), "CHAR_TYPE_CD");//ENTITY_CD
			
			}
			preparedStatement.setAutoclose(false);
			//RIA: Compare with previous values. Do not process same set of records again.
			String prevCharTypeCode = "";
			for (SQLResultRow resultSet : preparedStatement.list()) {
				transactionHeaderId = CommonUtils.CheckNull(resultSet.getString("TXN_HEADER_ID")).trim();
				charEffectiveDate = CommonUtils.CheckNull(String.valueOf((resultSet.getDate("EFFDT")))).trim();
				charTypeCode = CommonUtils.CheckNull(resultSet.getString("CHAR_TYPE_CD")).trim();
				charValue = CommonUtils.CheckNull(resultSet.getString("CHAR_VAL")).trim();
				
				if(!prevCharTypeCode.equalsIgnoreCase(charTypeCode)) {
					charOrAcctList = new ArrayList<String>();
					charOrAcctList.add(0, transactionHeaderId);
					charOrAcctList.add(1, charEffectiveDate);
					charOrAcctList.add(2, charTypeCode);
					charOrAcctList.add(3, charValue);
					charList.add(charOrAcctList);
					charOrAcctList = null;
					prevCharTypeCode = charTypeCode;
				}
			}
			preparedStatement.close();
			preparedStatement = null;
			return charList;
		}

	} // end worker class


	public static final class Per_Id_Nbr_Id implements Id {

		private static final long serialVersionUID = 1L;

		private String perIdNbr;
		private String cisDivision;

		public Per_Id_Nbr_Id(final String perIdNbr, final String cisDivision) {
			setPerIdNbr(perIdNbr);
			setCisDivision(cisDivision);
		}

		public String getPerIdNbr() {
			return perIdNbr;
		}

		public void setPerIdNbr(final String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		  //empty meethod
		}

		public String getCisDivision() {
			return cisDivision;
		}

		public void setCisDivision(final String cisDivision) {
			this.cisDivision = cisDivision;
		}

	}

	public static final class InboundMerchantInterface_Id implements Id {

		private static final long serialVersionUID = 1L;
		private String transactionHeaderId;
		private String effectiveDate;
		private String perOrBusFlg;
		private String cisDivision;
		private String perIdNbr;
		private String entityName;
		private String vatRegNbr;
		private String country;
		private String state;
		private String endDtMerchant;

		public InboundMerchantInterface_Id(String transactionHeaderId,
				String effectiveDate,String perOrBusFlg,String cisDivision,
				String perIdNbr, String entityName, String vatRegNbr,
				String country, String state, String endDtMerchant) {
			setTransactionHeaderId(transactionHeaderId);
			setEffectiveDate(effectiveDate);
			setPerOrBusFlg(perOrBusFlg);
			setCisDivision(cisDivision);
			setPerIdNbr(perIdNbr);
			setEntityName(entityName);
			setVatRegNbr(vatRegNbr);
			setCountry(country);
			setState(state);
			setEndDtMerchant(endDtMerchant);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		  //empty method
		}


		private String getEntityName() {
			return entityName;
		}

		private void setEntityName(final String entityName) {
			this.entityName = entityName;
		}

		private String getPerIdNbr() {
			return perIdNbr;
		}

		private void setPerIdNbr(final String perIdNbr) {
			this.perIdNbr = perIdNbr;
		}

		public String getPerOrBusFlg() {
			return perOrBusFlg;
		}

		public void setPerOrBusFlg(final String perOrBusFlg) {
			this.perOrBusFlg = perOrBusFlg;
		}

		public String getTransactionHeaderId() {
			return transactionHeaderId;
		}

		public void setTransactionHeaderId(final String transactionHeaderId) {
			this.transactionHeaderId = transactionHeaderId;
		}

		public String getVatRegNbr() {
			return vatRegNbr;
		}

		public void setVatRegNbr(final String vatRegNbr) {
			this.vatRegNbr = vatRegNbr;
		}

		public String getCountry() {
			return country;
		}

		public void setCountry(final String country) {
			this.country = country;
		}

		public String getState() {
			return state;
		}

		public void setState(final String state) {
			this.state = state;
		}

		public String getCisDivision() {
			return cisDivision;
		}

		public void setCisDivision(final String cisDivision) {
			this.cisDivision = cisDivision;
		}

		public String getEffectiveDate() {
			return effectiveDate;
		}

		public void setEffectiveDate(final String effectiveDate) {
			this.effectiveDate = effectiveDate;
		}
		public String getEndDtMerchant() {
			return endDtMerchant;
		}

		public void setEndDtMerchant(final String endDtMerchant) {
			this.endDtMerchant = endDtMerchant;
		}		
	} // end Id class
}
