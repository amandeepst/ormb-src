/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: PersistentClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/PersistentClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.entity;


import java.math.BigInteger;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.support.api.FrameworkBusinessEntity;
import com.splwg.base.support.api.PersistenceStrategy;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.base.api.DataTransferObject;

import com.splwg.base.support.context.FrameworkSession;

/**
  * Generated persistent class for CmFtGlAslAccountingId
  *
  * @author Generated by splDev.artifactGenerator
  */
public class CmFtGlAslAccountingId_Per
                 extends CmFtGlAslAccountingId_Impl
                 implements com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId,
                            FrameworkBusinessEntity<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId>
{ 
    /* simple persistent fields */
    private String glAccount;
    private Date accountingDate;
    private String counterparty;
    private String businessUnit;
    private String costCenter;
    private String intercompany;
    private String scheme;
    private FinancialTransactionTypeLookup financialTransactionType;
    private String batchControl;
    private BigInteger batchNumber;
    private String accountNumber;
    private Money amount;
    /* FK properties */
    /* optional FK properties */
    private com.splwg.base.domain.common.currency.Currency_Id currencyId;
    private com.splwg.base.domain.common.currency.Currency _opt_currency;
   /* child collections */

    //~ Methods ------------------------------------------------------------------------------------------

    /**
     * Default constructor.
     */
    public CmFtGlAslAccountingId_Per () {
       storePersistentFreshnessInfo(this);
    }

    /* the prime key */
    /**
      * Get the id property
      *
      * @return the id
      */
        @Override
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id getId() {
        return (com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id) getPrivateId();
    }

    public void setId(com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id newId) {
        setPrivateId(newId);
    }

    /* simple persistent fields */
    /**
      * Get the glAccount property
      *
      * @return glAccount
      */
    @Override
    public String getGlAccount() {
        verifyFreshState();
        return glAccount;
    }
    public void setGlAccount(String value) {
        this.glAccount = value;
    }

    /**
      * Get the accountingDate property
      *
      * @return accountingDate
      */
    @Override
    public Date getAccountingDate() {
        verifyFreshState();
        return accountingDate;
    }
    public void setAccountingDate(Date value) {
        this.accountingDate = value;
    }

    /**
      * Get the counterparty property
      *
      * @return counterparty
      */
    @Override
    public String getCounterparty() {
        verifyFreshState();
        return counterparty;
    }
    public void setCounterparty(String value) {
        this.counterparty = value;
    }

    /**
      * Get the businessUnit property
      *
      * @return businessUnit
      */
    @Override
    public String getBusinessUnit() {
        verifyFreshState();
        return businessUnit;
    }
    public void setBusinessUnit(String value) {
        this.businessUnit = value;
    }

    /**
      * Get the costCenter property
      *
      * @return costCenter
      */
    @Override
    public String getCostCenter() {
        verifyFreshState();
        return costCenter;
    }
    public void setCostCenter(String value) {
        this.costCenter = value;
    }

    /**
      * Get the intercompany property
      *
      * @return intercompany
      */
    @Override
    public String getIntercompany() {
        verifyFreshState();
        return intercompany;
    }
    public void setIntercompany(String value) {
        this.intercompany = value;
    }

    /**
      * Get the scheme property
      *
      * @return scheme
      */
    @Override
    public String getScheme() {
        verifyFreshState();
        return scheme;
    }
    public void setScheme(String value) {
        this.scheme = value;
    }

    /**
      * Get the financialTransactionType property
      *
      * @return financialTransactionType
      */
    @Override
    public FinancialTransactionTypeLookup getFinancialTransactionType() {
        verifyFreshState();
        return financialTransactionType;
    }
    public void setFinancialTransactionType(FinancialTransactionTypeLookup value) {
        this.financialTransactionType = value;
    }

    /**
      * Get the batchControl property
      *
      * @return batchControl
      */
    @Override
    public String getBatchControl() {
        verifyFreshState();
        return batchControl;
    }
    public void setBatchControl(String value) {
        this.batchControl = value;
    }

    /**
      * Get the batchNumber property
      *
      * @return batchNumber
      */
    @Override
    public BigInteger getBatchNumber() {
        verifyFreshState();
        return batchNumber;
    }
    public void setBatchNumber(BigInteger value) {
        this.batchNumber = value;
    }

    /**
      * Get the accountNumber property
      *
      * @return accountNumber
      */
    @Override
    public String getAccountNumber() {
        verifyFreshState();
        return accountNumber;
    }
    public void setAccountNumber(String value) {
        this.accountNumber = value;
    }

    /**
      * Get the amount property
      *
      * @return amount
      */
    @Override
    public Money getAmount() {
        verifyFreshState();
        return amount;
    }
    public void setAmount(Money value) {
        this.amount = value;
    }


    /* FK properties */

    /* optional FK properties */

    /* Methods and properties for currencyId */

    /**
      * Get the currencyId property
      *
      * @return currencyId the value
      */
    @Override
    public com.splwg.base.domain.common.currency.Currency_Id getCurrencyId() {
        verifyFreshState();
        return currencyId;
    }

    /**
      * Set the currencyId property.
      *
      * @param currencyId the value
      */
    public void setCurrencyId(com.splwg.base.domain.common.currency.Currency_Id newId) {
        if (newId != null && newId.isNull()) {
            this.currencyId = null;
        } else {
            this.currencyId = newId;
        }
        _opt_currency = null;
    }
    /**
      * Get the optional FK _opt_currency with field(s):
       *    currency
      *
      * @return the optional FK entity _opt_currency
      */
    @Override      
    public com.splwg.base.domain.common.currency.Currency fetchCurrency() {
        verifyFreshState();
        if (_opt_currency == null && currencyId!=null) {
            _opt_currency =
                                      currencyId.getEntity();
        }
        return _opt_currency;
    }


    /**
    * Set this object's fields from its corresponding data transfer object.
    * Accept a generic DTO, cast to my specific type.  Useful for framework code.
    *
    * @param dto the data transfer object
    */
    @Override
    public void setDTO(DataTransferObject<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> dto) {
        setDTOEtc(dto);
    }

    /**
      * Get the Data transfer object with this business entity's values
      *
      * @return a Data transfer object based upon this entity
      */
    @Override
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO getDTO() {
        verifyFreshState();
        CmFtGlAslAccountingId_DTO dto = new CmFtGlAslAccountingId_DTO();
        dto.setId(getId());
    /* Simple fields */
        dto.setGlAccount(this.glAccount);
        dto.setAccountingDate(this.accountingDate);
        dto.setCounterparty(this.counterparty);
        dto.setBusinessUnit(this.businessUnit);
        dto.setCostCenter(this.costCenter);
        dto.setIntercompany(this.intercompany);
        dto.setScheme(this.scheme);
        dto.setFinancialTransactionType(this.financialTransactionType);
        dto.setBatchControl(this.batchControl);
        dto.setBatchNumber(this.batchNumber);
        dto.setAccountNumber(this.accountNumber);
        dto.setAmount(this.amount);
        // version
        dto.setVersion(getVersion());

    /* Optional FKs */
        dto.setCurrencyId(this.getCurrencyId());

    /* Many-to-ones */


        return dto;
    }
    /**
      * @see com.splwg.base.api.BusinessEntity#entityName()
      */
    @Override
    public String entityName() {
        return "cmFtGlAslAccountingId";
    }




     /**
     * @see  com.splwg.base.api.PersistentEntity#delete()
     */
    @Override
    public void delete() {
        executeDelete();
    }

    /**
     * @see com.splwg.base.support.api.FrameworkBusinessEntity
     */
    @Override
    public DataTransferObject<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> setAbstractDTOValues(DataTransferObject<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> newDto) {
        com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO dto = (com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO) newDto;
        Id id = getPrivateId();
        boolean beingAdded = id == null || id.isNull();

        verifyFreshState();

        // set the oldDTO value used for change handler processing
        com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO oldDTO = null;
        FrameworkSession frameworkSession = (FrameworkSession) getSession();
        if (!beingAdded){
            // this is a change
            if (! frameworkSession.isChangeHandlingDisabled()) {
                oldDTO = getDTO();
            }
        }
        PersistenceStrategy persistenceStrategy = getPersistenceStrategy();
        privateSetAbstractDTOValuesMiddle(beingAdded, com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id.NULL, dto, persistenceStrategy);

        Id proposedId = dto.getId();
        if (persistenceStrategy.shouldSetInternalValuesFromSetDto()){
            id = proposedId == null || proposedId.isNull() ? null : proposedId;
            setPrivateId(id);


            /* Simple fields */
            this.glAccount = dto.getGlAccount();
            this.accountingDate = dto.getAccountingDate();
            this.counterparty = dto.getCounterparty();
            this.businessUnit = dto.getBusinessUnit();
            this.costCenter = dto.getCostCenter();
            this.intercompany = dto.getIntercompany();
            this.scheme = dto.getScheme();
            this.financialTransactionType = dto.getFinancialTransactionType();
            this.batchControl = dto.getBatchControl();
            this.batchNumber = dto.getBatchNumber();
            this.accountNumber = dto.getAccountNumber();
            this.amount = dto.getAmount();

            /* Optional FKs */
            // this.currencyId = dto.getCurrencyId();
            setCurrencyId(dto.getCurrencyId());

        /* Many-to-ones */

            // version
            touchVersion(dto.getVersion());
        }
        return oldDTO;
    }

}
