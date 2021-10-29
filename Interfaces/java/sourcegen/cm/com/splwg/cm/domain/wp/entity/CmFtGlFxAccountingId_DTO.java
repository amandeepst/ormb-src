/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: DTOClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/DTOClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.entity;

import com.splwg.base.api.datatypes.Date;
import java.math.BigInteger;


import com.splwg.base.api.Versioned;

import com.splwg.base.support.impl.AbstractDataTransferObject;

/**
 * Data transfer object for CmFtGlFxAccountingId
 *
 * @author Generated by splDev.artifactGenerator
 */
public class CmFtGlFxAccountingId_DTO extends AbstractDataTransferObject<CmFtGlFxAccountingId> implements Versioned {

    /* version */
    private long version;
    /* persistent fields */
    private String glAccount = "";
    private Date accountingDate = null;
    private String fundCurrency = "";
    private String binSettlementCurrency = "";
    private String counterparty = "";
    private String businessUnit = "";
    private String batchControl = "";
    private BigInteger batchNumber = BigInteger.ZERO;
    /* Many-to-ones and optional FKs*/
    /**
      * Create a new CmFtGlFxAccountingId_DTO
      */
    public CmFtGlFxAccountingId_DTO() {
      // empty constructor
    }

    @Override
    public String entityName() {
        return "cmFtGlFxAccountingId";
    }

    /**
      * Get the id property
      *
      * @return the id
      */
    @Override
    public com.splwg.cm.domain.wp.entity.CmFtGlFxAccountingId_Id getId() {
        return (com.splwg.cm.domain.wp.entity.CmFtGlFxAccountingId_Id) getAbstractId();
    }
    /* version */
    /**
      * Get the version
      *
      * @return the version
      */
    @Override
    public long getVersion() {
        return version;
    }
    /**
      * Set the version
      *
      * @param newVersion the new version
      */
    public void setVersion(long newVersion) {
        version = newVersion;
    }

    /* persistent fields */
    /**
      * Get the glAccount property
      *
      * @return glAccount
      */
    public String getGlAccount() {
        return glAccount;
    }
    /**
      * Set the glAccount property
      * The string will be truncated to fit the field's size of 254
      *
      * @param value the new value
      */
    public void setGlAccount(String value) {
        this.glAccount = modifyVarCharString("glAccount", value, 254 );
    }
    /**
      * Get the accountingDate property
      *
      * @return accountingDate
      */
    public Date getAccountingDate() {
        return accountingDate;
    }
    /**
      * Set the accountingDate property
      *
      * @param value the new value
      */
    public void setAccountingDate(Date value) {
        this.accountingDate = value;
    }
    /**
      * Get the fundCurrency property
      *
      * @return fundCurrency
      */
    public String getFundCurrency() {
        return fundCurrency;
    }
    /**
      * Set the fundCurrency property
      *
      * @param value the new value
      */
    public void setFundCurrency(String value) {
        this.fundCurrency = value;
    }
    /**
      * Get the binSettlementCurrency property
      *
      * @return binSettlementCurrency
      */
    public String getBinSettlementCurrency() {
        return binSettlementCurrency;
    }
    /**
      * Set the binSettlementCurrency property
      *
      * @param value the new value
      */
    public void setBinSettlementCurrency(String value) {
        this.binSettlementCurrency = value;
    }
    /**
      * Get the counterparty property
      *
      * @return counterparty
      */
    public String getCounterparty() {
        return counterparty;
    }
    /**
      * Set the counterparty property
      *
      * @param value the new value
      */
    public void setCounterparty(String value) {
        this.counterparty = value;
    }
    /**
      * Get the businessUnit property
      *
      * @return businessUnit
      */
    public String getBusinessUnit() {
        return businessUnit;
    }
    /**
      * Set the businessUnit property
      *
      * @param value the new value
      */
    public void setBusinessUnit(String value) {
        this.businessUnit = value;
    }
    /**
      * Get the batchControl property
      *
      * @return batchControl
      */
    public String getBatchControl() {
        return batchControl;
    }
    /**
      * Set the batchControl property
      *
      * @param value the new value
      */
    public void setBatchControl(String value) {
        this.batchControl = value;
    }
    /**
      * Get the batchNumber property
      *
      * @return batchNumber
      */
    public BigInteger getBatchNumber() {
        return batchNumber;
    }
    /**
      * Set the batchNumber property
      *
      * @param value the new value
      */
    public void setBatchNumber(BigInteger value) {
        this.batchNumber = value;
    }

    /* Many-to-ones and optional FKs*/


    /* canonical methods */

    @Override
    public CmFtGlFxAccountingId newEntity() {
         return abstractNewEntity();
    }
}
