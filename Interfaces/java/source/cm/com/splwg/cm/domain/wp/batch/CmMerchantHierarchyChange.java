/*******************************************************************************
 * FileName                   : CmMerchantHierarchyChange.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 14, 2019
 * Version Number             : 0.1
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             14-Mar-2019         Amandeep      Implemented all requirement as per Design.
 
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
import com.splwg.ccb.api.lookup.ServiceAgreementStatusLookup;
import com.splwg.ccb.domain.admin.billCycle.BillCycle_Id;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.customerinfo.account.AccountCharacteristics;
import com.splwg.ccb.domain.customerinfo.account.Account_DTO;
import com.splwg.ccb.domain.customerinfo.account.Account_Id;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_DTO;
import com.splwg.ccb.domain.customerinfo.serviceAgreement.ServiceAgreement_Id;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 *
 * @BatchJob (modules = { "demo"})
 */
public class CmMerchantHierarchyChange extends CmMerchantHierarchyChange_Gen {

	public static final Logger logger = LoggerFactory
			.getLogger(CmMerchantHierarchyChange.class);

	 InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();

	@Override
	public JobWork getJobWork() {
		logger.debug("Inside getJobWork() method");
		inboundAccountHierarchyLookUps.setLookUpConstants();
		final List<ThreadWorkUnit> threadWorkUnitList = getPerIdNbrData();
		logger.debug("No of rows selected for processing in getJobWork() method are - "
				+ threadWorkUnitList.size());
		
		inboundAccountHierarchyLookUps=null;
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);

	}

	private List<ThreadWorkUnit> getPerIdNbrData() {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		MerchantProcessingHierDataId merchantProcessingDataId = null;
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<>();
		List<MerchantProcessingHierDataId> rowsForProcessingList = new ArrayList<>();
		String perIdNbr = "";
		String personDivision = "";
		final String errorInfo = "Inside catch block of getMerchantHierarchyData() method-";

		try {
			stringBuilder
					.append("SELECT PER_ID_NBR, CIS_DIVISION FROM CM_INV_GRP_END_STG ");
			stringBuilder
					.append(" WHERE BO_STATUS_CD = :selectBoStatus1 AND (END_DT <= :processDate)");

			// Pick only effective dated records for processing.

			preparedStatement = createPreparedStatement(
					stringBuilder.toString(), "");
			preparedStatement.bindString("selectBoStatus1",
					inboundAccountHierarchyLookUps.getUpload(), "BO_STATUS_CD");
			preparedStatement
					.bindDate("processDate", getSystemDateTime().getDate().addDays(-1));

			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				perIdNbr = CommonUtils.CheckNull(resultSet
						.getString("PER_ID_NBR"));
				personDivision = CommonUtils.CheckNull(resultSet
						.getString("CIS_DIVISION"));

				merchantProcessingDataId = new MerchantProcessingHierDataId(
						perIdNbr, personDivision);
				rowsForProcessingList.add(merchantProcessingDataId);
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(merchantProcessingDataId);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				resultSet = null;
				merchantProcessingDataId = null;
			}
		} catch (Exception e) {
			logger.error(errorInfo, e);
			throw new RunAbortedException(
					CustomMessageRepository.exceptionInExecution(e.getMessage()));
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return threadWorkUnitList;
	}

	public Class<CmMerchantHierarchyChangeWorker> getThreadWorkerClass() {
		return CmMerchantHierarchyChangeWorker.class;
	}

	public static class CmMerchantHierarchyChangeWorker extends
			CmMerchantHierarchyChangeWorker_Gen {
		
		private InboundAccountHierarchyLookUps inboundAccountHierarchyLookUps = new InboundAccountHierarchyLookUps();
		
		public void initializeThreadWork(
				boolean initializationPreviouslySuccessful)
				throws ThreadAbortedException, RunAbortedException {
			
			inboundAccountHierarchyLookUps.setLookUpConstants();
			
		}

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}
		

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {

			MerchantProcessingHierDataId merchantProcessingDataId = (MerchantProcessingHierDataId) unit
					.getPrimaryId();
			List<MerchHierarchyEndDatedRecordsId> merchHierExistingRecords = getMerchantHierEndDatedRecords(merchantProcessingDataId);
			String skipRowsFlag = "false";
			String childPersonIdNumber = merchantProcessingDataId
					.getChildMerchant();
			String division = merchantProcessingDataId.getPersonDivision();

			for (int merchantCount = 0; (merchantCount < merchHierExistingRecords
					.size())
					&& (CommonUtils.CheckNull(skipRowsFlag).trim()
							.startsWith(skipRowsFlag)); merchantCount++) {

				hierarchyChange(merchHierExistingRecords, merchantCount,
						childPersonIdNumber, division, skipRowsFlag);
			}

			return true;
		}

		public boolean hierarchyChange(
				List<MerchHierarchyEndDatedRecordsId> merchHierExistingRecords,
				int merchantCount, String childPersonIdNumber, String division,
				String skipRowsFlag) {

			Map<String, List<SQLResultRow>> memberAcctMap = new HashMap<>();
			String memberAccountId = "";
			String status = "";

			removeSavepoint("Rollback"
					.concat(getBatchThreadNumber().toString()));

			setSavePoint("Rollback".concat(getBatchThreadNumber().toString()));

			setRecStatus(merchHierExistingRecords.get(merchantCount),
					inboundAccountHierarchyLookUps.getPending(),
					inboundAccountHierarchyLookUps.getUpload(), "0", "0", " ");

			// member Account Id
			String hierarchyType = merchHierExistingRecords.get(merchantCount)
					.getHierType();
			String currency = merchHierExistingRecords.get(merchantCount)
					.getCurrencyCode();
			List<SQLResultRow> memberlist = null;
			List<SQLResultRow> inactiveMemberlist = null;
			// Fetch Member Details
			String memberAccountNumber = childPersonIdNumber.concat(
					"_" + hierarchyType + "_").concat(currency);
			String acctNbrTypeCd = null;

			if (notNull(memberAcctMap) && !memberAcctMap.isEmpty()
					&& memberAcctMap.containsKey(memberAccountNumber)) {
				memberlist = memberAcctMap.get(memberAccountNumber);
				memberAccountId = checkMemberAcct(memberlist, memberAccountId);

			} else {
				inactiveMemberlist = fetchInactiveAccountId(
						memberAccountNumber, division);
				if (!inactiveMemberlist.isEmpty()) {
					acctNbrTypeCd = "INACTIVE";
					errorOut(acctNbrTypeCd, merchantCount,
							merchHierExistingRecords, skipRowsFlag);
					return false;
				} else {
					memberlist = fetchAccountId(memberAccountNumber, division);
					if (!memberlist.isEmpty()) {
						for (SQLResultRow rs : memberlist) {
							memberAccountId = rs.getString("ACCT_ID");
						}
					} else {
						errorOut(acctNbrTypeCd, merchantCount,
								merchHierExistingRecords, skipRowsFlag);
						return false;
					}
				}
				memberAcctMap.put(memberAccountNumber, memberlist);
			}
			Date endDate = getSystemDateTime().getDate();
			status = hierarchyChange(memberAccountId, endDate);
			insertErrorLogs(status, skipRowsFlag, merchHierExistingRecords,
					merchantCount);
			// Required to nullify the effect of database transactions in
			// case of error
			// scenario
			return true;
		}
		
		public String updateSa(String memberContract, String masterContract, Date endDate) {
			PreparedStatement preparedStatement = null; 
			String skipRowsFlag = "false";
			String status="true";			

			try{
			 	ServiceAgreement_DTO serviceAgreementDTO=new ServiceAgreement_DTO();
				// updating CI_SA
				ServiceAgreement_DTO serviceAgreementDTO2= new ServiceAgreement_DTO();
				serviceAgreementDTO.setId(new ServiceAgreement_Id(masterContract)); 
				serviceAgreementDTO2=serviceAgreementDTO.getEntity().getDTO();
				if (notNull(endDate) && endDate.isAfter(getProcessDateTime().getDate())) {
					serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.PENDING_STOP);
	
				} 
				else if(notNull(endDate) && endDate.isSameOrBefore(getProcessDateTime().getDate())){
					serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.CANCELED);
				}
				else {
					serviceAgreementDTO2.setStatus(ServiceAgreementStatusLookup.constants.ACTIVE);
				}
				serviceAgreementDTO2.setEndDate(endDate);
				serviceAgreementDTO.getEntity().setDTO(serviceAgreementDTO2);		
				
			//updating CI_SA_CHAR
				if(notNull(endDate) && endDate.isSameOrBefore(getProcessDateTime().getDate())){
					StringBuilder saCharUpdate = new StringBuilder();
					saCharUpdate.append("UPDATE CI_SA_CHAR SET CHAR_TYPE_CD=:fkCanRef ");
					saCharUpdate.append("WHERE SA_ID=:masterSaId AND CHAR_TYPE_CD=:fkRef AND SRCH_CHAR_VAL=:memberSaId");
					preparedStatement =createPreparedStatement(saCharUpdate.toString(),"");
					preparedStatement.bindString("masterSaId",serviceAgreementDTO.getId().getIdValue(),"SA_ID");
					preparedStatement.bindString("memberSaId",memberContract,"SA_ID");
					preparedStatement.bindString("fkRef",inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(),"CHAR_TYPE_CD");
					preparedStatement.bindString("fkCanRef",inboundAccountHierarchyLookUps.getFkRefCancelInvoicingCharacteristic(),"CHAR_TYPE_CD");
					preparedStatement.executeUpdate();
				}
			}
			
			catch (Exception e) {
				logger.error("Error in updating Master Contract"
						+ e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.MASTER_SA_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.MASTER_SA_NOT_UPDATED;
			} finally {
				closeConnection(preparedStatement);
			}
			
			//Updating Master Account Char as The Hierarchy is end Dated.
			status=updateAccountChar(masterContract);
			
			return status;
	 }

		private String updateAccountChar(String masterContract) {

			String skipRowsFlag = "false";

			ServiceAgreement_Id saId=new ServiceAgreement_Id(masterContract);
			PreparedStatement ps=null;
			PreparedStatement preparedStatement = null;

			try{
				if(saId.getEntity() != null){
					Account masterAcct=saId.getEntity().getAccount();
					if(masterAcct != null){											
						StringBuilder query=new StringBuilder();
						query.append("SELECT SA.SA_ID FROM  CI_SA SA,CI_SA_CHAR SCHAR WHERE SCHAR.SA_ID=SA.SA_ID AND SA.ACCT_ID=:acctId AND ");
						query.append("SA.SA_STATUS_FLG IN (:active,:pendStop) AND SCHAR.CHAR_TYPE_CD=:saRefChar");					
						ps=createPreparedStatement(query.toString(),"");
						ps.bindId("acctId",masterAcct.getId());
						ps.bindLookup("active", ServiceAgreementStatusLookup.constants.ACTIVE);
						ps.bindLookup("pendStop", ServiceAgreementStatusLookup.constants.PENDING_STOP);
						ps.bindString("saRefChar", inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(), "CHAR_TYPE_CD");
						ps.setAutoclose(false);
						SQLResultRow rs=ps.firstRow();
						
						if(rs == null){
							StringBuilder queryDetail=new StringBuilder();
							queryDetail.append("UPDATE CI_ACCT_CHAR SET CHAR_VAL=:charVal WHERE ACCT_ID=:acctId AND CHAR_TYPE_CD=:igaChar");
							preparedStatement=createPreparedStatement(queryDetail.toString(), "");
							preparedStatement.bindId("acctId", masterAcct.getId());
							preparedStatement.bindString("charVal", "N", "CHAR_VAL");
							preparedStatement.bindString("igaChar", inboundAccountHierarchyLookUps.getIgaCharacteristic(), "CHAR_TYPE_CD");
							preparedStatement.setAutoclose(false);
							preparedStatement.executeUpdate();
						}
					}		
				}
			}
			catch (Exception e) {
				logger.error("Error in updating Master Account Characteristics"
						+ e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.MASTER_ACCT_CHAR_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.MASTER_ACCT_CHAR_NOT_UPDATED;
			} finally {
				closeConnection(preparedStatement);
				closeConnection(ps);
			}
			
			return "true";			
		}

		

		/**
		 * @param status
		 * @param skipRowsFlag
		 * @param merchHierExistingRecords
		 * @param merchantCount
		 */
		public void insertErrorLogs(String status, String skipRowsFlag,
				List<MerchHierarchyEndDatedRecordsId> merchHierExistingRecords,
				int merchantCount) {
			if (CommonUtils.CheckNull(status).trim().startsWith(skipRowsFlag)) {
				getError(merchHierExistingRecords, status, merchantCount);
			} else {
				setRecStatus(merchHierExistingRecords.get(merchantCount),
						inboundAccountHierarchyLookUps.getCompleted(),
						inboundAccountHierarchyLookUps.getPending(), "0", "0",
						" ");
			}
			logger.debug("Processing of Transaction Header Id - "
					+ merchHierExistingRecords.get(merchantCount)
							.getTransactionHeaderId());
		}

		/**
		 * @param acctNbrTypeCd
		 * @param merchantCount
		 * @param merchHierExistingRecords
		 * @param skipRowsFlag
		 */
		public void errorOut(String acctNbrTypeCd, int merchantCount,
				List<MerchHierarchyEndDatedRecordsId> merchHierExistingRecords,
				String skipRowsFlag) {
			String status = callError(acctNbrTypeCd);
			if (CommonUtils.CheckNull(status).trim().startsWith(skipRowsFlag)) {
				getError(merchHierExistingRecords, status, merchantCount);

			}
		}

		/**
		 * @param merchHierExistingRecords
		 * @param status
		 * @param merchantCount
		 */
		private void getError(
				List<MerchHierarchyEndDatedRecordsId> merchHierExistingRecords,
				String status, int merchantCount) {
			String[] returnStatusArray = status.split("~");
			setRecStatus(merchHierExistingRecords.get(merchantCount),
					inboundAccountHierarchyLookUps.getError(),
					inboundAccountHierarchyLookUps.getPending(),
					returnStatusArray[2].trim(), returnStatusArray[3].trim(),
					returnStatusArray[1].trim());

		}

		/**
		 * @param memberlist
		 * @param memberAccountId
		 * @return
		 */
		private String checkMemberAcct(List<SQLResultRow> memberlist,
				String memberAccountId) {

			if (!memberlist.isEmpty()) {
				for (SQLResultRow rs : memberlist) {
					memberAccountId = rs.getString("ACCT_ID");

				}
			}
			return memberAccountId;

		}

		/**
		 * @param memberAccountId
		 * @param endDate
		 * @return
		 */
		private String hierarchyChange(String memberAccountId, Date endDate) {

			String status = null;			
			PreparedStatement saIds = getSaId(memberAccountId);
			
			//Updating Master Contract
			List<SQLResultRow> resultList=saIds.list();
			
			if (notNull(resultList) && !resultList.isEmpty()) {
				for (SQLResultRow result : resultList) {						
				String masterContract = result.getString("MASTER_SA");
				String memberContract = result.getString("MEMBER_SA");								
				status=updateSa(memberContract,masterContract,endDate);
				}
			}
			
			checkStatus(status);		
					
			//Updating Member Contract
			status=updateMemberContract(memberAccountId,endDate);
			checkStatus(status);
					
			//Updating Outlet Account To INACTIVE
			
			status= updateAcctId(memberAccountId);
			checkStatus(status);

			// Changing RelationShip Type Flag to INACTIVE-
			status = updateAccountPersonRelationship(memberAccountId);
			checkStatus(status);		
			
			// Change ACCT_NBR_TYPE_CD to INACTIVE-
			status = updateAccountNumberTypeCd(memberAccountId);
			checkStatus(status);
								
			return "true";

		}

		private String checkStatus(String status) {
			
			String skipRowsFlag = "false";

			if (CommonUtils.CheckNull(status).trim()
					.startsWith(skipRowsFlag)) {
				return status;
			}
			
			return "true";
		}

		private String updateMemberContract(String memberAccountId, Date endDate) {
			PreparedStatement preparedStatement3 = null;
			StringBuilder stringBuilder = new StringBuilder();
			String skipRowsFlag = "false";

			
			try {
				stringBuilder
						.append("UPDATE CI_SA SA SET SA.SA_STATUS_FLG=:status3, SA.END_DT=:endDt WHERE SA.ACCT_ID=:accountId2 ");
				stringBuilder
						.append(" AND SA_STATUS_FLG IN (:status1,:status2) AND EXISTS (SELECT 1 FROM ");
				stringBuilder
						.append(" CI_ACCT_NBR WHERE ACCT_ID = SA.ACCT_ID AND ACCT_NBR_TYPE_CD=:acctNbrTypeCd)");
				preparedStatement3 = createPreparedStatement(
						stringBuilder.toString(), "");
				preparedStatement3.bindString("accountId2", memberAccountId,
						"ACCT_ID");
				preparedStatement3.bindLookup("status1",
						ServiceAgreementStatusLookup.constants.ACTIVE);
				preparedStatement3.bindLookup("status2",
						ServiceAgreementStatusLookup.constants.PENDING_STOP);
				preparedStatement3.bindLookup("status3",
						ServiceAgreementStatusLookup.constants.CANCELED);
				preparedStatement3.bindDate("endDt", endDate);
				preparedStatement3.bindString("acctNbrTypeCd",
						inboundAccountHierarchyLookUps
								.getExternalAccountIdentifier(),
						"ACCT_NBR_TYPE_CD");
				preparedStatement3.setAutoclose(false);
				preparedStatement3.executeUpdate();
				
			} catch (Exception e) {
				logger.error("Error:- ", e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.SA_STATUS_FLG_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.SA_STATUS_FLG_NOT_UPDATED;

			} finally {

				closeConnection(preparedStatement3);
			}
			return "true";
		}

		public PreparedStatement getSaId(String memberAccountId) {
			StringBuilder stringBuilder = new StringBuilder();
			PreparedStatement contractPreparedStatement = null;
			try {
				stringBuilder.append(" SELECT MA.SA_ID MASTER_SA,MB.SA_ID MEMBER_SA FROM CI_SA MA,CI_SA MB, CI_SA_CHAR B, CI_SA_CHAR C WHERE " );
				stringBuilder.append(" MA.SA_ID=B.SA_ID AND MA.SA_ID=C.SA_ID  AND C.SRCH_CHAR_VAL=MB.SA_ID AND MA.SA_STATUS_FLG=MB.SA_STATUS_FLG ");
				stringBuilder.append(" AND MA.SA_STATUS_FLG IN (:status1,:status2) AND B.CHAR_TYPE_CD=:charTypeCd AND C.CHAR_TYPE_CD=:fkRef " );
				stringBuilder.append(" AND MB.ACCT_ID=:accountId ");
				contractPreparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				contractPreparedStatement.bindString("charTypeCd", inboundAccountHierarchyLookUps.getDateTimeCharacteristic(), "CHAR_TYPE_CD");
				contractPreparedStatement.bindString("fkRef", inboundAccountHierarchyLookUps.getFkRefInvoicingCharacteristic(), "CHAR_TYPE_CD");
				contractPreparedStatement.bindString("accountId", memberAccountId, "ACCT_ID");
				contractPreparedStatement.bindString("status1", inboundAccountHierarchyLookUps.getActive(), "SA_STATUS_FLG");
				contractPreparedStatement.bindString("status2", inboundAccountHierarchyLookUps.getPendingStop(), "SA_STATUS_FLG");
				contractPreparedStatement.setAutoclose(false);
				return contractPreparedStatement;
			}
			finally {
				if(contractPreparedStatement != null) {
					contractPreparedStatement.close();
					contractPreparedStatement= null;
				}
			}
			
		}

		/**
		 * @param memberAccountId
		 * @return 
		 */
		public String updateAcctId(String memberAccountId) {
			String skipRowsFlag = "false";

			try {
			Account_Id acctId = new Account_Id(memberAccountId);
			Account_DTO acctDTO = acctId.getEntity().getDTO();
			acctDTO.setAlertInformation("INACTIVE");
			acctDTO.setBillCycleId(new BillCycle_Id(" "));
			acctId.getEntity().setDTO(acctDTO);		
			
			} catch (Exception e) {
				logger.error("Error:- ", e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.BILL_CYC_CD_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.BILL_CYC_CD_NOT_UPDATED;
			}
			return "true";
		}

		/**
		 * @param memberAccountId
		 * @return
		 */
		private String updateAccountNumberTypeCd(String memberAccountId) {

			PreparedStatement preparedStatement = null;
			StringBuilder sb = new StringBuilder();
			String skipRowsFlag = "false";
			try {
				sb.append("UPDATE CI_ACCT_NBR SET ACCT_NBR_TYPE_CD=:inactive WHERE ACCT_ID=:acctId AND ACCT_NBR_TYPE_CD=:acctNbrTypeCd ");
				preparedStatement = createPreparedStatement(sb.toString(), "");
				preparedStatement.bindString("inactive", "INACTIVE",
						"ACCT_NBR_TYPE_CD");
				preparedStatement.bindString("acctId", memberAccountId,
						"ACCT_ID");
				preparedStatement.bindString("acctNbrTypeCd",
						inboundAccountHierarchyLookUps
								.getExternalAccountIdentifier(),
						"ACCT_NBR_TYPE_CD");
				preparedStatement.setAutoclose(false);

				int row = preparedStatement.executeUpdate();
				if (row == 0) {
					return skipRowsFlag
							+ "~"
							+ getErrorDescription(String
									.valueOf(CustomMessages.ACCT_NBR_TYP_CD_NOT_UPDATED))
							+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
							+ CustomMessages.ACCT_NBR_TYP_CD_NOT_UPDATED;
				}

			} catch (Exception e) {
				logger.error("Error in updating Account Number Type Code " + e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.ACCT_NBR_TYP_CD_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.ACCT_NBR_TYP_CD_NOT_UPDATED;
			} finally {
				closeConnection(preparedStatement);
			}
			return "true";
		}

		/**
		 * getErrorDescription() method selects error message description from
		 * ORMB message catalog.
		 * 
		 * @return errorInfo
		 */
		public static String getErrorDescription(String messageNumber) {
			String errorInfo = null;

			errorInfo = CustomMessageRepository.dataIntegrityErrMessage(
					Integer.parseInt(messageNumber)).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}

		private String callError(String acctNbrTypeCd) {
			String skipRowsFlag = "false";
			if (acctNbrTypeCd == null) {
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.ACCT_MISSING)) + "~"
						+ CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.ACCT_MISSING;
			} else {
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.PERSON_ACCT_INACTIVE))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.PERSON_ACCT_INACTIVE;
			}

		}

		/**
		 * @param memberAccountId
		 * @return
		 */
		private String updateAccountPersonRelationship(String memberAccountId) {
			PreparedStatement preparedStatement = null;
			String skipRowsFlag = "false";
			StringBuilder sb = new StringBuilder();
			try {
				sb.append("UPDATE CI_ACCT_PER SET ACCT_REL_TYPE_CD=:inactive WHERE ACCT_ID=:acctId");
				preparedStatement = createPreparedStatement(sb.toString(), "");
				preparedStatement.bindString("inactive", "INACTIVE",
						"ACCT_REL_TYPE_CD");
				preparedStatement.bindString("acctId", memberAccountId,
						"ACCT_ID");
				preparedStatement.setAutoclose(false);

				int row = preparedStatement.executeUpdate();
				if (row == 0) {
					return skipRowsFlag
							+ "~"
							+ getErrorDescription(String
									.valueOf(CustomMessages.ACCT_PER_REL_NOT_UPDATED))
							+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
							+ CustomMessages.ACCT_PER_REL_NOT_UPDATED;
				}
			} catch (Exception e) {
				logger.error("Error in updating Account Person Relationship"
						+ e);
				return skipRowsFlag
						+ "~"
						+ getErrorDescription(String
								.valueOf(CustomMessages.ACCT_PER_REL_NOT_UPDATED))
						+ "~" + CustomMessages.MESSAGE_CATEGORY + "~"
						+ CustomMessages.ACCT_PER_REL_NOT_UPDATED;
			} finally {
				closeConnection(preparedStatement);
			}
			return "true";
		}

		/**
		 * @param acctNumber
		 * @param division
		 * @return
		 */
		private List<SQLResultRow> fetchAccountId(String acctNumber,
				String division) {
			PreparedStatement preparedStatement = null;
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT AN.ACCT_ID,AP.PER_ID, AN.ACCT_NBR_TYPE_CD FROM CI_ACCT_NBR AN,CI_ACCT_PER AP,CI_PER PR WHERE AN.ACCT_NBR =:acctNbr ");
			sb.append(" AND AN.ACCT_NBR_TYPE_CD IN (:acctNbrTypeCd) ");
			sb.append(" AND AN.ACCT_ID=AP.ACCT_ID AND PR.PER_ID=AP.PER_ID AND PR.CIS_DIVISION=:division");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.bindString("acctNbr", acctNumber, "ACCT_NBR");
			preparedStatement.bindString("acctNbrTypeCd", "C1_F_ANO",
					"ACCT_NBR_TYPE_CD");
			preparedStatement.bindString("division", division, "CIS_DIVISION");

			preparedStatement.setAutoclose(false);
			List<SQLResultRow> list = preparedStatement.list();
			closeConnection(preparedStatement);
			return list;
		}

		/**
		 * @param acctNumber
		 * @param division
		 * @return
		 */
		private List<SQLResultRow> fetchInactiveAccountId(String acctNumber,
				String division) {
			PreparedStatement preparedStatement = null;
			StringBuilder sb = new StringBuilder();
			sb.append(" SELECT AN.ACCT_ID,AP.PER_ID, AN.ACCT_NBR_TYPE_CD FROM CI_ACCT_NBR AN,CI_ACCT_PER AP,CI_PER PR WHERE AN.ACCT_NBR =:acctNbr ");
			sb.append(" AND AN.ACCT_NBR_TYPE_CD IN (:inactive) ");
			sb.append(" AND AN.ACCT_ID=AP.ACCT_ID AND PR.PER_ID=AP.PER_ID AND PR.CIS_DIVISION=:division");
			sb.append(" AND NOT EXISTS ( SELECT 1 FROM CI_ACCT_NBR WHERE AN.ACCT_NBR = ACCT_NBR AND ACCT_NBR_TYPE_CD IN (:acctNbrTypeCd))");
			preparedStatement = createPreparedStatement(sb.toString(), "");
			preparedStatement.bindString("acctNbr", acctNumber, "ACCT_NBR");
			preparedStatement.bindString("inactive", "INACTIVE",
					"ACCT_REL_TYPE_CD");
			preparedStatement.bindString("division", division, "CIS_DIVISION");
			preparedStatement.bindString("acctNbrTypeCd", "C1_F_ANO",
					"ACCT_NBR_TYPE_CD");

			preparedStatement.setAutoclose(false);
			List<SQLResultRow> list = preparedStatement.list();
			closeConnection(preparedStatement);
			return list;
		}

		/**
		 * @param merchHierarchyEndDatedRecordsId
		 * @param newStat
		 * @param oldStat
		 * @param aMessageCategory
		 * @param aMessageNumber
		 * @param aErrorMessage
		 */
		private void setRecStatus(
				MerchHierarchyEndDatedRecordsId merchHierarchyEndDatedRecordsId,
				String newStat, String oldStat, String aMessageCategory,
				String aMessageNumber, String aErrorMessage) {

			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			if (aErrorMessage.length() > 255) {
				aErrorMessage = aErrorMessage.substring(0, 249);
			}
			// Update only effective dated records for processing.
			try {
				stringBuilder
						.append("UPDATE CM_INV_GRP_END_STG SET BO_STATUS_CD =:status1, STATUS_UPD_DTTM = :systimestamp, MESSAGE_CAT_NBR =:messageCategory, ");
				stringBuilder
						.append(" MESSAGE_NBR =:messageNumber, ERROR_INFO =:errorDescription WHERE BO_STATUS_CD =:status2 AND (END_DT<=:sysdate) ");
				stringBuilder.append(" AND TXN_HEADER_ID =:txnHeaderId ");
				preparedStatement = createPreparedStatement(
						stringBuilder.toString(), "");
				preparedStatement.bindString("txnHeaderId",
						merchHierarchyEndDatedRecordsId
								.getTransactionHeaderId(), "TXN_HEADER_ID");
				preparedStatement
						.bindString("status1", newStat, "BO_STATUS_CD");
				preparedStatement
						.bindString("status2", oldStat, "BO_STATUS_CD");
				preparedStatement.bindBigInteger("messageCategory",
						new BigInteger(aMessageCategory));
				preparedStatement.bindBigInteger("messageNumber",
						new BigInteger(aMessageNumber));
				preparedStatement.bindString("errorDescription",
						CommonUtils.CheckNull(aErrorMessage), "ERROR_INFO");
				preparedStatement.bindDate("sysdate", getSystemDateTime()
						.getDate());
				preparedStatement.bindDateTime("systimestamp",
						getSystemDateTime());
				preparedStatement.setAutoclose(false);
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error(
						"Inside catch block of getMerchantHierarchyData() method-",
						e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution(e
								.getMessage()));
			} finally {
				closeConnection(preparedStatement);
			}
		}

		/**
		 * @param preparedStatement
		 */
		private void closeConnection(PreparedStatement preparedStatement) {
			if (preparedStatement != null) {
				preparedStatement.close();
			}
		}

		/**
		 * @param savePointName
		 */
		protected final void removeSavepoint(String savePointName) {
			FrameworkSession session = (FrameworkSession) SessionHolder
					.getSession();
			if (session.hasActiveSavepointWithName(savePointName)) {
				session.removeSavepoint(savePointName);
			}
		}

		/**
		 * @param savePointName
		 */
		protected final void setSavePoint(String savePointName) {
			// Create save point before any change is done for the current
			// transaction.
			FrameworkSession session = (FrameworkSession) SessionHolder
					.getSession();
			session.setSavepoint(savePointName);
		}

		/**
		 * @param savePointName
		 */
		protected final void rollbackToSavePoint(String savePointName) {
			// In case error occurs, rollback all changes for the current
			// transaction and
			// log error.
			FrameworkSession session = (FrameworkSession) SessionHolder
					.getSession();
			session.rollbackToSavepoint(savePointName);
		}

		/**
		 * @param merchantProcessingDataId
		 * @return
		 */
		private List<MerchHierarchyEndDatedRecordsId> getMerchantHierEndDatedRecords(
				MerchantProcessingHierDataId merchantProcessingDataId) {
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();
			MerchHierarchyEndDatedRecordsId inboundMerchantHierInterfaceId = null;
			List<MerchHierarchyEndDatedRecordsId> inboundMerchantHierEndDatedList = new ArrayList<>();

			try {
				stringBuilder = null;
				stringBuilder = new StringBuilder();
				stringBuilder
						.append("SELECT STG.TXN_HEADER_ID, STG.PER_ID_NBR, STG.ACCT_TYPE, STG.CURRENCY_CD, ");
				stringBuilder
						.append(" STG.END_DT FROM CM_INV_GRP_END_STG STG ");
				stringBuilder
						.append(" WHERE STG.BO_STATUS_CD =:boStatus AND STG.PER_ID_NBR = :personIdNbr2 ");
				stringBuilder
						.append(" AND STG.CIS_DIVISION =:personDivision AND STG.END_DT <= :processDate");
				stringBuilder.append(" ORDER BY TXN_HEADER_ID ");

				preparedStatement = createPreparedStatement(
						stringBuilder.toString(), "");
				preparedStatement.bindString("boStatus",
						inboundAccountHierarchyLookUps.getUpload(),
						"BO_STATUS_CD");
				preparedStatement.bindString("personIdNbr2",
						merchantProcessingDataId.getChildMerchant(),
						"PER_ID_NBR");
				preparedStatement.bindString("personDivision",
						merchantProcessingDataId.getPersonDivision(),
						"CIS_DIVISION");
				preparedStatement.bindDate("processDate", getSystemDateTime()
						.getDate().addDays(-1));

				preparedStatement.setAutoclose(false);
				for (SQLResultRow resultSet : preparedStatement.list()) {
					String transactionHeaderId = CommonUtils
							.CheckNull(resultSet.getString("TXN_HEADER_ID"));
					String hierType = CommonUtils.CheckNull(resultSet
							.getString("ACCT_TYPE"));
					Date merchantHierarchyEndDate = resultSet.getDate("END_DT");
					String currencyCode = CommonUtils.CheckNull(resultSet
							.getString("CURRENCY_CD"));

					inboundMerchantHierInterfaceId = new MerchHierarchyEndDatedRecordsId(
							transactionHeaderId, merchantHierarchyEndDate,
							hierType, currencyCode);

					inboundMerchantHierEndDatedList
							.add(inboundMerchantHierInterfaceId);
					resultSet = null;
					inboundMerchantHierInterfaceId = null;
				}
			} catch (Exception e) {
				logger.error(
						"Inside catch block of getMerchantHierarchyData() method-",
						e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution(e
								.getMessage()));
			} finally {
				closeConnection(preparedStatement);
			}
			// update only future dated records with corresponding error
			// message.
			try {
				stringBuilder = null;
				stringBuilder = new StringBuilder();

				stringBuilder
						.append("UPDATE CM_INV_GRP_END_STG SET ERROR_INFO = :errorMsg ");
				stringBuilder.append("WHERE (END_DT>:sysdate) ");
				stringBuilder.append("AND PER_ID_NBR = :personIdNbr ");
				stringBuilder.append("AND CIS_DIVISION =:personDivision");
				preparedStatement = createPreparedStatement(
						stringBuilder.toString(), "");
				preparedStatement.bindString("personIdNbr",
						merchantProcessingDataId.getChildMerchant(),
						"PER_ID_NBR");
				preparedStatement.bindString("personDivision",
						merchantProcessingDataId.getPersonDivision(),
						"CIS_DIVISION");
				preparedStatement.bindDate("sysdate", getSystemDateTime()
						.getDate());
				preparedStatement
						.bindString(
								"errorMsg",
								"Future dated record: Row will get processed once becomes effective.",
								"ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (Exception e) {
				logger.error(
						"Inside catch block of getMerchantHierarchyData() method-",
						e);
				throw new RunAbortedException(
						CustomMessageRepository.exceptionInExecution(e
								.getMessage()));
			} finally {
				closeConnection(preparedStatement);
			}
			return inboundMerchantHierEndDatedList;
		}

		public void finalizeThreadWork() throws ThreadAbortedException,
				RunAbortedException {

			super.finalizeThreadWork();
		}

	}

	public static final class MerchantProcessingHierDataId implements Id {

		private static final long serialVersionUID = 1L;

		private String childMerchant;

		private String personDivision;

		public MerchantProcessingHierDataId(String childMerchant,
				String personDivision) {
			setChildMerchant(childMerchant);
			setPersonDivision(personDivision);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
			// appendContents
		}

		public String getPersonDivision() {
			return personDivision;
		}

		public void setPersonDivision(String personDivision) {
			this.personDivision = personDivision;
		}

		public String getChildMerchant() {
			return childMerchant;
		}

		public void setChildMerchant(String childMerchant) {
			this.childMerchant = childMerchant;
		}
	}

	public static final class MerchHierarchyEndDatedRecordsId implements Id {

		private static final long serialVersionUID = 1L;

		private String transactionHeaderId;

		private Date merchantHierarchyEndDate;

		private String hierType;
		private String currencyCode;

		public MerchHierarchyEndDatedRecordsId(String transactionHeaderId,
				Date merchantHierarchyEndDate, String hierType,
				String currencyCode) {
			setTransactionHeaderId(transactionHeaderId);
			setMerchantHierarchyEndDate(merchantHierarchyEndDate);
			setHierType(hierType);
			setCurrencyCode(currencyCode);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
			// appendContents
		}

		public String getHierType() {
			return hierType;
		}

		public void setHierType(String hierType) {
			this.hierType = hierType;
		}

		public String getCurrencyCode() {
			return currencyCode;
		}

		public void setCurrencyCode(String currencyCode) {
			this.currencyCode = currencyCode;
		}

		public String getTransactionHeaderId() {
			return transactionHeaderId;
		}

		public void setTransactionHeaderId(String transactionHeaderId) {
			this.transactionHeaderId = transactionHeaderId;
		}

		public void setMerchantHierarchyEndDate(Date merchantHierarchyEndDate) {
			this.merchantHierarchyEndDate = merchantHierarchyEndDate;
		}

		public Date getMerchantHierarchyEndDate() {
			return merchantHierarchyEndDate;
		}

	}
}
