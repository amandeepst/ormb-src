/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: Interface.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/Interface.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */





package com.splwg.cm.domain.wp.entity;



import com.splwg.base.api.Property;
import com.splwg.base.support.changehandlers.PropertyHolder;
import com.splwg.base.support.api.SinglePropertyStaticMetadata;

import com.splwg.base.api.changehandling.*;

import com.splwg.base.api.SimpleKeyedBusinessEntity;

import com.splwg.base.api.SessionManagedEntity;



import com.splwg.base.api.Versioned;

/**
  * Interface to CmFtGlAslAccountingId
  *
  * @author Generated by splDev.artifactGenerator
  */
public interface CmFtGlAslAccountingId 
   extends SimpleKeyedBusinessEntity<CmFtGlAslAccountingId> , SessionManagedEntity, Versioned 
{


    /* the prime key */
    /**
      * Get the id property
      *
      * @return the id
      */
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id getId();


    /* simple persistent fields */
    /**
      * Get the glAccount property
      *
      * @return glAccount
      */
    public java.lang.String getGlAccount();
    /**
      * Get the accountingDate property
      *
      * @return accountingDate
      */
    public com.splwg.base.api.datatypes.Date getAccountingDate();
    /**
      * Get the counterparty property
      *
      * @return counterparty
      */
    public java.lang.String getCounterparty();
    /**
      * Get the businessUnit property
      *
      * @return businessUnit
      */
    public java.lang.String getBusinessUnit();
    /**
      * Get the costCenter property
      *
      * @return costCenter
      */
    public java.lang.String getCostCenter();
    /**
      * Get the intercompany property
      *
      * @return intercompany
      */
    public java.lang.String getIntercompany();
    /**
      * Get the scheme property
      *
      * @return scheme
      */
    public java.lang.String getScheme();
    /**
      * Get the financialTransactionType property
      *
      * @return financialTransactionType
      */
    public com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup getFinancialTransactionType();
    /**
      * Get the batchControl property
      *
      * @return batchControl
      */
    public java.lang.String getBatchControl();
    /**
      * Get the batchNumber property
      *
      * @return batchNumber
      */
    public java.math.BigInteger getBatchNumber();
    /**
      * Get the accountNumber property
      *
      * @return accountNumber
      */
    public java.lang.String getAccountNumber();
    /**
      * Get the amount property
      *
      * @return amount
      */
    public com.splwg.base.api.datatypes.Money getAmount();

    /* FK properties */

    /* optional FK properties */

    /* Methods and properties for currencyId */

    /**
      * Get the currencyId property
      *
      * @return currencyId the value
      */
    public com.splwg.base.domain.common.currency.Currency_Id getCurrencyId();
    /**
      * Get the optional FK _opt_currency with field(s):
       *    currency
      *
      * @return the optional FK entity _opt_currency
      */
    public  com.splwg.base.domain.common.currency.Currency fetchCurrency();

    /* Collection accessors */

    /**
      * Get the Data transfer object with this business entity's values
      *
      * @return a Data transfer object based upon this entity
      */
    @Override
    public com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_DTO getDTO();



   
   /**
    * The properties of CmFtGlAslAccountingId
    *
    */
    public static EntityProperties properties = new EntityProperties();
    
   /**
    * The {@link PropertyHolder} class containing the entities properties
    *
    */
    public static class EntityProperties extends PropertyHolder<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> {
    
        //~ Constructors -------------------------------------------------------------------------------------
        
        public EntityProperties(Property<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> referencingProperty) {
            super(referencingProperty);
        }

        private EntityProperties() {
            super(null);
        }

        /**
         * @returns Return the entity name
         */
        @Override
        public String getEntityName() {
            return "cmFtGlAslAccountingId";
        }
            
        @Override    
        public final Property[] getAllProperties(){
            return ALL_PROPERTIES.clone();
        }
    
        /** The glAccount property */
        public final StringProperty glAccount
            = new StringProperty(new SinglePropertyStaticMetadata(this, "glAccount", "CM_FT_GL_ASL_ACCTING_ID       ", "GL_ACCT                       ", false, 254, 0));
        /** The accountingDate property */
        public final DateProperty accountingDate
            = new DateProperty(new SinglePropertyStaticMetadata(this, "accountingDate", "CM_FT_GL_ASL_ACCTING_ID       ", "ACCOUNTING_DT                 ", false, 10, 0));
        /** The counterparty property */
        public final StringProperty counterparty
            = new StringProperty(new SinglePropertyStaticMetadata(this, "counterparty", "CM_FT_GL_ASL_ACCTING_ID       ", "COUNTERPARTY                  ", false, 12, 0));
        /** The businessUnit property */
        public final StringProperty businessUnit
            = new StringProperty(new SinglePropertyStaticMetadata(this, "businessUnit", "CM_FT_GL_ASL_ACCTING_ID       ", "BUSINESS_UNIT                 ", false, 20, 0));
        /** The costCenter property */
        public final StringProperty costCenter
            = new StringProperty(new SinglePropertyStaticMetadata(this, "costCenter", "CM_FT_GL_ASL_ACCTING_ID       ", "COST_CENTRE                   ", false, 2, 0));
        /** The intercompany property */
        public final StringProperty intercompany
            = new StringProperty(new SinglePropertyStaticMetadata(this, "intercompany", "CM_FT_GL_ASL_ACCTING_ID       ", "INTERCOMPANY                  ", false, 12, 0));
        /** The scheme property */
        public final StringProperty scheme
            = new StringProperty(new SinglePropertyStaticMetadata(this, "scheme", "CM_FT_GL_ASL_ACCTING_ID       ", "SCHEME                        ", false, 16, 0));
        /** The financialTransactionType property */
        public final LookupProperty<com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup> financialTransactionType
            = new LookupProperty<com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup>(new SinglePropertyStaticMetadata(this, "financialTransactionType", "CM_FT_GL_ASL_ACCTING_ID       ", "FT_TYPE_FLG                   ", false, 2, 0));
        /** The batchControl property */
        public final StringProperty batchControl
            = new StringProperty(new SinglePropertyStaticMetadata(this, "batchControl", "CM_FT_GL_ASL_ACCTING_ID       ", "BATCH_CD                      ", false, 8, 0));
        /** The batchNumber property */
        public final IntegerProperty batchNumber
            = new IntegerProperty(new SinglePropertyStaticMetadata(this, "batchNumber", "CM_FT_GL_ASL_ACCTING_ID       ", "BATCH_NBR                     ", false, 10, 0));
        /** The accountNumber property */
        public final StringProperty accountNumber
            = new StringProperty(new SinglePropertyStaticMetadata(this, "accountNumber", "CM_FT_GL_ASL_ACCTING_ID       ", "ACCT_NBR                      ", false, 30, 0));
        /** The amount property */
        public final MoneyProperty amount
            = new MoneyProperty(new SinglePropertyStaticMetadata(this, "amount", "CM_FT_GL_ASL_ACCTING_ID       ", "AMOUNT                        ", false, 15, 2));
        /** The currencyId property */
        public final com.splwg.base.domain.common.currency.Currency.EntityProperties.ReferenceProperty currencyId
            = new com.splwg.base.domain.common.currency.Currency.EntityProperties.ReferenceProperty(this, "currencyId","CM_FT_GL_ASL_ACCTING_ID       ", "CM_ASLACTFK ", "CM  ", false, true);

        /** The ID property */
        public final IdProperty<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId> id = new IdProperty<com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId>(this, "CM_FT_GL_ASL_ACCTING_ID       ", "CM_ASLACTPK ", "CM  ");
       

        /** The version property */
        public final VersionProperty version = new VersionProperty(this, "CM_FT_GL_ASL_ACCTING_ID       ");
            
        // References to parent entities PropertyHolders    
        /** Reference to the com.splwg.base.domain.common.currency.Currency entity via the currencyId property */
        public final com.splwg.base.domain.common.currency.Currency.EntityProperties lookOnCurrency(){
            return new com.splwg.base.domain.common.currency.Currency.EntityProperties(currencyId);
        }
    
        /** An array of all the properties */
        private final Property[] ALL_PROPERTIES = {
           glAccount,accountingDate,counterparty,businessUnit,costCenter,intercompany,scheme,financialTransactionType,batchControl,batchNumber,accountNumber,amount,currencyId,               id
               ,version
};


        public static class ReferenceProperty
            extends ReferencePropertyValue {

            //~ Instance fields ------------------------------------------------------------------------------

            private ReferencePropertyValue oldValue;

            //~ Constructors ---------------------------------------------------------------------------------

            public ReferenceProperty(PropertyHolder holder, String propertyName, String tableName,
                                         String dbName, String owner,
                                         boolean isInPK,
                                         boolean isOptional) {
                super(holder, propertyName, tableName, dbName, owner, isInPK, isOptional);
            }

            //~ Methods --------------------------------------------------------------------------------------

            public ReferencePropertyValue getOldValue() {
                if (oldValue == null) {
                    oldValue = new ReferencePropertyValue(getPropertyHolder(),
                                                              getPropertyName(),
                                                              getTableName(),
                                                              getDbName(), getOwner(),
                                                              isPKForeignKeyProperty(),
                                                              isOptionalFKProperty());
                }
                return oldValue;
            }


            /**
             * @see com.splwg.base.api.changehandling.OldValueContainingProperty#wasChanged()
             */
            public Condition wasChanged() {
                 return isEqualTo(getOldValue()).not();
            }

            /**
             * @see com.splwg.base.api.changehandling.PropertyValue#isOldVersion()
             */
            @Override
            public boolean isOldVersion() {
                return false;
            }

            /**
             * @see com.splwg.base.api.changehandling.PropertyValue#isOldVersion()
             */
            @Override
            public boolean isNewVersion() {
                return true;
            }
            
        }

        public static class ReferencePropertyValue
            extends ManyToOnePropertyValue<CmFtGlAslAccountingId> {

            //~ Constructors ---------------------------------------------------------------------------------

            public ReferencePropertyValue(PropertyHolder holder, String propertyName, String tableName,
                                              String dbName, String owner,
                                              boolean isInPK,
                                              boolean isOptional) {
                super(holder, propertyName, tableName, dbName, owner, isInPK, isOptional);
            }

            //~ Methods --------------------------------------------------------------------------------------

            public Condition isEqualTo(ReferencePropertyValue other) {
                return privateEqualTo(other);
            }

            public Condition isEqualTo(com.splwg.cm.domain.wp.entity.CmFtGlAslAccountingId_Id id) {
                return privateEqualTo(id);
            }

            @Override
            public String getReferencedTableName() {
                return "CM_FT_GL_ASL_ACCTING_ID";
            }
        }
   	
    }
}
