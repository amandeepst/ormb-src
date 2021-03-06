/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: BaseClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/BaseClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.entity; 


import java.math.BigInteger;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.datatypes.Money;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.base.api.DataTransferObject;


/**
  * Generated base class for CmFtGlAslAccountingId
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class CmFtGlAslAccountingId_Gen
                 extends com.splwg.base.support.api.AbstractBusinessEntity<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId>
                 {                                  
    
    /**
     * Default constructor.
     */
    public CmFtGlAslAccountingId_Gen () {
       // empty
    }
    
    /**
     * Get the persistent instance of the entity
     *
     */
    protected CmFtGlAslAccountingId thisEntity(){
       return (CmFtGlAslAccountingId)this;
    }

    /* the prime key */
    /**
      * Get the id property
      *
      * @return the id
      */
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id getId() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");
    }

    /* simple persistent fields */
    /**
      * Get the glAccount property
      *
      * @return glAccount
      */
    public String getGlAccount() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the accountingDate property
      *
      * @return accountingDate
      */
    public Date getAccountingDate() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the counterparty property
      *
      * @return counterparty
      */
    public String getCounterparty() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the businessUnit property
      *
      * @return businessUnit
      */
    public String getBusinessUnit() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the costCenter property
      *
      * @return costCenter
      */
    public String getCostCenter() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the intercompany property
      *
      * @return intercompany
      */
    public String getIntercompany() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the scheme property
      *
      * @return scheme
      */
    public String getScheme() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the financialTransactionType property
      *
      * @return financialTransactionType
      */
    public FinancialTransactionTypeLookup getFinancialTransactionType() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the batchControl property
      *
      * @return batchControl
      */
    public String getBatchControl() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the batchNumber property
      *
      * @return batchNumber
      */
    public BigInteger getBatchNumber() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the accountNumber property
      *
      * @return accountNumber
      */
    public String getAccountNumber() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }
    /**
      * Get the amount property
      *
      * @return amount
      */
    public Money getAmount() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }

    /* FK properties */

    /* optional FK properties */

    /* Methods and properties for currencyId */

    /**
      * Get the currencyId property
      *
      * @return currencyId the value
      */
    public com.splwg.base.domain.common.currency.Currency_Id getCurrencyId() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }

    /**
      * Get the optional FK _opt_currency with field(s):
       *    currency
      *
      * @return the optional FK entity _opt_currency
      */
    public com.splwg.base.domain.common.currency.Currency fetchCurrency() { 
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }

    /**
      * Modify this object's fields from its corresponding data transfer object
      *
      * @param lowestDto the (lowest casted) data transfer object to set from
      */
    public void setDTO(com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO lowestDto) {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }

    /**
      * Get the Data transfer object with this business entity's values
      *
      * @return a Data transfer object based upon this entity
      */
    @Override
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO getDTO() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }




    /**
    * Set this object's fields from its corresponding data transfer object.
    * Accept a generic DTO, cast to my specific type.  Useful for framework code.
    *
    * @param dto the data transfer object
    */
    @Override
    public void setDTO(DataTransferObject<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> dto) {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }

     /**
     * @see  com.splwg.base.api.PersistentEntity#delete()
     */
    public void delete() {
        throw new UnsupportedOperationException("This method should be overridden by generated persistence code");    
    }


}
