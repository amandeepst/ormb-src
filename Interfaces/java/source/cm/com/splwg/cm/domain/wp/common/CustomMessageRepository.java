package com.splwg.cm.domain.wp.common;

import com.splwg.base.domain.common.message.MessageParameters;
import com.splwg.shared.common.ServerMessage;

public class CustomMessageRepository extends CustomMessages {
	private static CustomMessageRepository instance;

	// ~ Methods
	// ----------------------------------------------------------------------------------------------

	static CustomMessageRepository getInstance() {
		if (instance == null) {
			instance = new CustomMessageRepository();
		}
		return instance;
	}

	public static ServerMessage exceptionInExecution(String exceptionMessage) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(exceptionMessage);
		return repo.getMessage(RUN_TIME_ERROR_IN_EXECUTION, messageParms);
	}
	 
	// Merchants Interface Error
	
	public static ServerMessage merchantError(String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}
	
	// INT 034 Account Bill cycle update Error
	
		public static ServerMessage accountBillCycleUpdateError(String messageNumber) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters messageParms = new MessageParameters();
			return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
		}
		
	public static ServerMessage exceptionInUnitProcessing(
			String transactionHeaderId, String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(RUN_TIME_ERROR_WHILE_PROCESSING_UNIT,
				messageParms);
	}
	
	public static ServerMessage getActiveSAAlreadyExistsError(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(ACTIVE_SA_EXISTS, messageParms);
	}

	public static ServerMessage getAcctIsInactiveError(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(ACCT_NOT_ACTIVE, messageParms);
	}

	
	// Payment Cancellation messages
	public static ServerMessage paymentCancellationError(
			String transactionHeaderId, String extRefNo, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(extRefNo);
		messageParms.addRawString(message);
		return repo.getMessage(PAYMENT_GENERIC, messageParms);
	}

	public static ServerMessage paymentRejectionUpldStgNoDataFound(
			String transactionHeaderId, String extRefNo) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(extRefNo);
		return repo.getMessage(PAYMENT_UPLD_STG_NO_DATA_FOUND, messageParms);
	}

	public static ServerMessage paymentRejectionUpldNoMatch(
			String transactionHeaderId, String extRefNo) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(extRefNo);
		return repo.getMessage(PAYMENT_UPLD_NO_MATCH_STG_VALUES, messageParms);
	}

	public static ServerMessage paymentRejectionUpldNoBillId(
			String transactionHeaderId, String paymentEventId, String extRefNo) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(paymentEventId);
		messageParms.addRawString(extRefNo);
		return repo.getMessage(PAYMENT_UPLD_NO_PAYMENT_ID, messageParms);
	}
	
	public static ServerMessage paymentRejectioninvalidCancelReason(String transactionHeaderId) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		return repo.getMessage(PAYMENT_INVALID_CANCEL_REASON, messageParms);
	}
	
	//	Invoice Data Interface
	public static ServerMessage invoiceData(String billId,
			String billSegmentId, String headerSequence, String fundCurrency, 
			String sequenceNbr, int messageCategory, int messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		return repo.getMessage(messageNumber);
	}


	// Correction Notes messages
	public static ServerMessage correctionNotesError(
			String transactionHeaderId, String billId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(billId);
		messageParms.addRawString(message);
		return repo.getMessage(CORRECTION_NOTE_GENERIC, messageParms);
	}

	public static ServerMessage correctionNotesBillNotFoundError(
			String transactionHeaderId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(message);
		return repo.getMessage(CORRECTION_NOTE_NO_BILL, messageParms);
	}

//	 Merchants Hierarchy Interface
	public static ServerMessage personHierError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(PERSON_HIER_FAILED, messageParms);
	}

	public static ServerMessage personIdCheckError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(PERSONID__CHECK_FAILED, messageParms);
	}
	
	public static ServerMessage personIdBothCheckError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(BOTH_PERSONID__CHECK_FAILED, messageParms);
	}
	
	public static ServerMessage personIdParentCheckError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(PARENT_PERSONID__CHECK_FAILED, messageParms);
	}

	public static ServerMessage personIdChildCheckError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(CHILD_PERSONID__CHECK_FAILED, messageParms);
	}
	
	public static ServerMessage personHierCheckError(
			String transactionHeaderId, String transactionDetailId,
			String personRelationshipType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personRelationshipType);
		messageParms.addRawString(message);
		return repo.getMessage(PERSON_HIER_CHECK_FAILED, messageParms);
	}

	public static ServerMessage sameHierCheckError(String transactionHeaderId,
			String transactionDetailId, String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(SAME_HIER_CHECK_FAILED, messageParms);
	}

	public static ServerMessage diffDivisionCheckError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNumber2, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNumber2);
		messageParms.addRawString(message);
		return repo.getMessage(DIFF_DIVISION_CHECK_FAILED, messageParms);
	}

	//  Account Hierarchy Interface
	public static ServerMessage personNotFoundError(String transactionHeaderId,
			String personIdNbr, String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(PERSON_NOT_FOUND, messageParms);
	}

	public static ServerMessage personHierarchyError(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(PERSON_HIERARCHY_DOESNT_EXIST, messageParms);
	}

	public static ServerMessage masterAccountValidationError(
			String transactionHeaderId, String personIdNbr, String accountType,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(accountType);
		messageParms.addRawString(message);
		return repo.getMessage(MASTER_ACCOUNT_DOESNT_EXIST, messageParms);
	}

	public static ServerMessage memberAccountValidationError(
			String transactionHeaderId, String personIdNbr, String accountType,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(accountType);
		messageParms.addRawString(message);
		return repo.getMessage(CHILD_ACCOUNT_DOESNT_EXIST, messageParms);
	}

	public static ServerMessage masterAccountUpdateError(
			String transactionHeaderId, String personIdNbr, String accountType,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(accountType);
		messageParms.addRawString(message);
		return repo.getMessage(MASTER_ACCOUNT_UPDATE_FAILED, messageParms);
	}

	public static ServerMessage accountHierarchyCreationError(
			String transactionHeaderId, String personIdNbr, String accountType,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(accountType);
		messageParms.addRawString(message);
		return repo.getMessage(MASTER_CONTRACT_CREATION_FAILED, messageParms);
	}

	public static ServerMessage accountHierarchyUpdateError(
			String transactionHeaderId, String accountId, String accountType,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(accountId);
		messageParms.addRawString(accountType);
		messageParms.addRawString(message);
		return repo.getMessage(MASTER_CONTRACT_UPDATION_FAILED, messageParms);
	}

	public static ServerMessage masterMasterAssociationError(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(MASTER_MASTER_ASSOCIATION_FAILED, messageParms);
	}

	public static ServerMessage divisionIsDifferentForMasterAndMember(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(DIVISION_IS_DIFFERENT_FOR_MASTER_AND_MEMBER,
				messageParms);
	}

	public static ServerMessage currencyIsDifferentForMasterAndMember(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(
				CURRENCY_CODES_ARE_DIFFERENT_FOR_MASTER_AND_MEMBER,
				messageParms);
	}

	public static ServerMessage parentAndChildPersonsAreSame(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(PARENT_AND_CHILD_PERSONS_ARE_SAME, messageParms);
	}

	public static ServerMessage divisionIsDifferentForParentAndChildPersons(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(
				DIVISION_IS_DIFFERENT_FOR_MASTER_AND_CHILD_PERSONS,
				messageParms);
	}

	public static ServerMessage currencyCodesAreDifferentForParentAndChildPersons(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(
				CURRENCY_CODES_ARE_DIFFERENT_FOR_MASTER_AND_CHILD_PERSONS,
				messageParms);
	}

	public static ServerMessage currencyCodesOfMasterMerchantAndMasterAccountDonotMatch(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo
		.getMessage(
				CURRENCY_CODES_OF_MASTER_MERCHANT_AND_MASTER_ACCOUNT_DONOT_MATCH,
				messageParms);
	}

	public static ServerMessage currencyCodesOfChildMerchantAndMemberAccountDonotMatch(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo
		.getMessage(
				CURRENCY_CODES_OF_CHILD_MERCHANT_AND_MEMBER_ACCOUNT_DONOT_MATCH,
				messageParms);
	}

	public static ServerMessage invalidHierarchyType(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(INVALID_HIERARCHY_TYPE, messageParms);
	}

	public static ServerMessage perIdNbrsAreSame(String transactionHeaderId,
			String memberAccount, String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(memberAccount);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(PER_ID_NBR_AND_PER_ID_NBR2_ARE_SAME,
				messageParms);
	}

	public static ServerMessage invalidCurrencycodeForMasterPerson(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(INVALID_CURRENCY_CODE_FOR_MASTER_PERSON,
				messageParms);
	}

	public static ServerMessage invalidCurrencyCodeForChildPerson(
			String transactionHeaderId, String personIdNbr,
			String hierarchyType, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(hierarchyType);
		messageParms.addRawString(message);
		return repo.getMessage(INVALID_CURRENCY_CODE_FOR_CHILD_PERSON,
				messageParms);
	}

//	 Payment Requests Interface

	public static ServerMessage getPayReqErrorMessage(
			String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}
		
//ENH010 Tariff Rate Reprice Algorithm
    
    public static ServerMessage invalidProductCodeError(String priceAsgnId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
    	messageParms.addRawString(priceAsgnId);
    	messageParms.addRawString(message);
    	return repo.getMessage(INVALID_PRODUCT_CODE_ERROR, messageParms);
    }
    
    public static ServerMessage invalidPricelistIdError(String priceAsgnId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
    	messageParms.addRawString(priceAsgnId);
    	messageParms.addRawString(message);
    	return repo.getMessage(INVALID_PRICELISTID_ERROR, messageParms);
    }
    
    public static ServerMessage merchantLevelPricingUpdateError(String priceAsgnId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
    	messageParms.addRawString(priceAsgnId);
    	messageParms.addRawString(message);
    	return repo.getMessage(MERCHANT_LEVEL_PRICING_ERROR, messageParms);
    }

	// Transactional Pricing Data Interface
	
	public static ServerMessage eventIdError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(EVENT_ID_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage productCodeError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(PRODUCT_CODE_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage transactionCalculationIdError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(TRANSACTION_CALCULATION_ID_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage sequenceNumberError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(SEQUENCE_NUMBER_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage billCalcLineTypeError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(BILL_CALC_LINE_TYPE_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage calculatedAmountError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(CALCULATED_AMOUNT_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage currencyCodeError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(CURRENCY_CODE_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage billIdError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(BILL_ID_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage acctTypeError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(ACCT_TYPE_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	public static ServerMessage chargingCardProductError(String billId,
			String errorDescription) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billId);
		messageParms.addRawString(errorDescription);
		return repo.getMessage(CHARGING_CARD_PRODUCT_COULD_NOT_BE_DETERMINED,
				messageParms);
	}
	
	// Recurring Charges Interface
	public static ServerMessage billableChargePersonEntityNotFoundError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNunber, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNunber);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_PERSON_NOT_FOUND, messageParms);
	}

	public static ServerMessage billableChargingAccountNotFoundError(
			String transactionHeaderId, String transactionDetailId,
			String personId, String personIdNumber, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personId);
		messageParms.addRawString(personIdNumber);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_CHARGING_ACCOUNT_NOT_FOUND, messageParms);
	}

	public static ServerMessage billableRecurringChargeContractNotFoundError(
			String transactionHeaderId, String transactionDetailId,
			String chargingAccountId, String personIdNbr, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(chargingAccountId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_RECURRING_CONTRACT_NOT_FOUND, messageParms);
	}

	public static ServerMessage billableRecurringChargeEntityCreationError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNbr, String chargingAccountId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(chargingAccountId);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_BILLABLE_CHARGE_CREATION_FAILED,
				messageParms);
	}

	public static ServerMessage billableRecurringChargeEntityUpdateError(
			String transactionHeaderId, String transactionDetailId,
			String billableChargeId, String recurringChargeContractId,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(billableChargeId);
		messageParms.addRawString(recurringChargeContractId);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_BILLABLE_CHARGE_UPDATE_FAILED, messageParms);
	}

	public static ServerMessage billableRecurringChargeNewSVCQuantityUpdateError(
			String transactionHeaderId, String transactionDetailId,
			String billableChargeId, String recurringChargeContractId,
			String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(billableChargeId);
		messageParms.addRawString(recurringChargeContractId);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_BILLABLE_CHARGE_UPDATING_NEW_QTY_FAILED,
				messageParms);
	}

	public static ServerMessage billableChargeInvalidBillPeriodCodeError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNunber, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNunber);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_INVALID_BILL_PERIOD_CD, messageParms);
	}

	public static ServerMessage billableChargeInvalidOriginalFields(
			String transactionHeaderId, String transactionDetailId,
			String personIdNbr, String chargingAccountId, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNbr);
		messageParms.addRawString(chargingAccountId);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_INVALID_ORIGINAL_FIELDS, messageParms);
	}

	public static ServerMessage billableChargeInvalidPriceItemCodeError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNunber, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNunber);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_INVALID_PRICEITEM_CD, messageParms);
	}
	
	public static ServerMessage billableChargeSQICodeUpdateError(
			String transactionHeaderId, String transactionDetailId,
			String personIdNunber, String message) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(transactionDetailId);
		messageParms.addRawString(personIdNunber);
		messageParms.addRawString(message);
		return repo.getMessage(RCI_SQI_CD_UPDATE_FAIL, messageParms);
	}

	public static ServerMessage getPricingErrorMessage(
			String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}

	public static ServerMessage agreementInterfaceError(
			String transactionHeaderId, String perIdNbr, String message, int messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(transactionHeaderId);
		messageParms.addRawString(perIdNbr);
		messageParms.addRawString(message);
		return repo.getMessage(messageNumber, messageParms);
	}

	// Algorithms - ENH01 and ENH03
	public static ServerMessage prodTaxStatNotFound(String priceItem) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(priceItem);
		return repo.getMessage(PROD_TAX_MISSING, messageParms);
	}

	public static ServerMessage perTaxRegNotFound(String person, String party) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(person);
		messageParms.addRawString(party);
		return repo.getMessage(PER_TAXREG_MISSING, messageParms);
	}

	public static ServerMessage billFactNotFound(String billFactor) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(billFactor);
		return repo.getMessage(BF_MISSING, messageParms);
	}

	public static ServerMessage bfTaxStatNotFound(String bf, String taxStatus) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(bf);
		messageParms.addRawString(taxStatus);
		return repo.getMessage(BF_STAT_MISSING, messageParms);
	}

	/*public static ServerMessage inValidObject(String id,String entity) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		messageParms.addRawString(id);
		messageParms.addRawString(entity);
		return repo.getMessage(INVALID_OBJECT, messageParms);
	}

	public static ServerMessage BillFactorDateNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(BILL_FACT_DATE_MISSING, messageParms);
	}

	public static ServerMessage billChargeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(BILL_CHARG_NOT_FOUND, messageParms);
	}*/

/*	public static ServerMessage bcChargeTypeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(BC_CHRG_TYPE_MISSING, messageParms);
	}
*/
	public static ServerMessage priceItemCdDescNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(PI_CD_DESC_MISSING, messageParms);
	}

	public static ServerMessage bsCalcTaxRgmeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(TAX_RGME_CALC_DESC_NOT_FOUND, messageParms);
	}

	public static ServerMessage dsCdDescNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DS_CD_DESC_MISSING, messageParms);
	}

	public static ServerMessage bankAcctNumNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(BANK_ACCT_NUM_MISSING, messageParms);
	}

	public static ServerMessage adjCdDescNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(ADJ_CD_DESC_MISSING, messageParms);
	}
	
	public static ServerMessage bcPerIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(BC_PERID_NOT_FOUND, messageParms);
	}



	// ENH009 Minimum charge Calculation
	public static ServerMessage minChargeCharTypenotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(MC_TYPE_NOT_FOUND, messageParms);
	}
	
	public static ServerMessage prodNotFound(){
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(PROD_NOT_FOUND, messageParms);
		
	}
	

	//ENH0011 Create Debt Case
	public static ServerMessage caseTypeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CASE_TYPE_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningCcIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_CC_ID_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningCcTypeCodeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_CC_TYPE_CD_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningCcDttmNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_CC_DTTM_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningPerIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_PER_ID_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningPerIdNbrNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_PER_ID_NBR_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningAccountIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_ACCT_ID_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningAccountTypeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_ACCTTYPE_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningBillIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_BILL_ID_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningBillDateNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_BILL_DT_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningDueDateNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_DUE_DT_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningBillAmountNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_BILL_AMT_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningOutstandingAmountNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_OUTSTANDING_AMT_NOT_FOUND, messageParms);
	}
	
	//	INT035 Dunning_Letters Interface
	public static ServerMessage dunningCurrencyCodeNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(DUNNING_CURRENCY_CD_NOT_FOUND, messageParms);
	}
	
	
	//	Bill Cycle Upload Interface
		
		public static ServerMessage billCycleError(String messageNumber) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			return repo.getMessage(Integer.parseInt(messageNumber));
		}
	
//		Bill Period Upload Interface
		
			public static ServerMessage billPeriodError(String messageNumber) {
				CustomMessageRepository repo = CustomMessageRepository.getInstance();
				return repo.getMessage(Integer.parseInt(messageNumber));
			}


	//	Header Polling Job
		
		public static ServerMessage headerPolling(int messageNumber) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			return repo.getMessage(messageNumber);
		}

		//ENH001 Tax Determination Algorithm
		//	ENH001 Tax Amount Determination
		public static ServerMessage taxStatValueNotFound(String messageNumber, String taxStatusCharVal) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters messageParms = new MessageParameters();
			messageParms.addRawString(taxStatusCharVal);
			return repo.getMessage(TAX_STAT_OPTION_VAL_NOT_FOUND, messageParms);
		}

		public static ServerMessage billDateNotFound() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters messageParms = new MessageParameters();
			return repo.getMessage(BILL_DATE_NOT_FOUND, messageParms);
		}
	
//	Reserve Algorithm
	public static ServerMessage acctIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(ACCT_ID_NOT_FOUND, messageParms);
	}
	public static ServerMessage fundSaIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(FUND_SA_ID_NOT_FOUND, messageParms);
	}
	public static ServerMessage reserveSaIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(RESERVE_SA_ID_NOT_FOUND, messageParms);
	}
	public static ServerMessage ctbd1NotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CTBD1_NOT_FOUND, messageParms);
	}
	public static ServerMessage ctbd2NotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CTBD2_NOT_FOUND, messageParms);
	}
	public static ServerMessage ctbd3NotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CTBD3_NOT_FOUND, messageParms);
	}
	public static ServerMessage ctbd4NotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CTBD4_NOT_FOUND, messageParms);
	}
	public static ServerMessage ctbd5NotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CTBD5_NOT_FOUND, messageParms);
	}
	public static ServerMessage calcAmtNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(CALC_AMT_NOT_FOUND, messageParms);
	}
//	Event Price Interface
	public static ServerMessage getEventPriceErrorMessage(
			String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}
//	Price Type Interface
	public static ServerMessage getPriceTypeErrorMessage(
			String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}
//	Accounting Summary Data Interface
	public static ServerMessage getAccountingErrorMessage(
			String messageNumber) {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
	}

// WAF Algorithm 
	public static ServerMessage WAFContractNotCreated() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(WAF_CONTRACT_NOT_CREATED, messageParms);
	}
	
	public static ServerMessage accountIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(ACCTID_NOT_FOUND, messageParms);
	}
	public static ServerMessage fundingSaIdNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(FUND_SAID_NOT_FOUND, messageParms);
	}
	
//	 ADJ COntract Determination Algorithm 
	public static ServerMessage ADJContractNotFound() {
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		MessageParameters messageParms = new MessageParameters();
		return repo.getMessage(ADJ_CONTRACT_NOT_FOUND, messageParms);
	}
	
	//Agreements Interface
		public static ServerMessage agreementPriceError(String messageNumber) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters messageParms = new MessageParameters();
			return repo.getMessage(Integer.parseInt(messageNumber), messageParms);
		}
		
	// Payment Request Interface
		public static ServerMessage missingBillDate() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(BILL_DT_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingAcctType() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(ACCOUNT_TYPE_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingDivision() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(DIVISION_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingExternalPartyId() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(EXTERNAL_PARTY_ID_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingPayType() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(PAY_TYPE_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingAmount() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(AMOUNT_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage missingCurrencyCd() {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();        
			return repo.getMessage(CURRENCY_COULD_NOT_BE_DETERMINED, parms);
		}
		
		public static ServerMessage batchParameterMissing(String parmName, String batchCd) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();
			parms.addRawString(parmName);
			parms.addRawString(batchCd);
			return repo.getMessage(BATCH_PARM_MISSING, parms);
		}
		
		public static ServerMessage errorInsertingInTable(String tableName) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();
			parms.addRawString(tableName);
			return repo.getMessage(ERROR_INSERTING_IN_TABLE, parms);
		}
			
		public static ServerMessage unableToCreatePaymentRequest(String billId) {
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();
			parms.addRawString(billId);
			return repo.getMessage(UNABLE_TO_CREATE_PAY_REQ, parms);
		}
		// Start Invoice Data Integrity
		public static ServerMessage dataIntegrityErrMessage(int messageNbr)
		{
		CustomMessageRepository repo = CustomMessageRepository.getInstance();
		return repo.getMessage(messageNbr);
		}
		
		public static ServerMessage dataIntegrityErrMessage(int messageNbr, String priceCategory)
		{
			CustomMessageRepository repo = CustomMessageRepository.getInstance();
			MessageParameters parms = new MessageParameters();
			parms.addRawString(priceCategory);
			return repo.getMessage(messageNbr, parms);
		}
		// End Invoice Data Integrity

}
