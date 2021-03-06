/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: SingleFieldIdClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/SingleFieldIdClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.entity;

import com.splwg.base.api.datatypes.StringId;

import java.util.Map;


/**
  * Generated Id class for CmFtGlFxAccountingId
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public class CmFtGlFxAccountingId_Id extends StringId<CmFtGlFxAccountingId> {
    public static final int FIELD_SIZE = 12;
    public static final CmFtGlFxAccountingId_Id NULL = new CmFtGlFxAccountingId_Id (
         ""
    );

    /* public constructor */
    /**
     * Creates a new CmFtGlFxAccountingId_Id object.
     *
      * @param accountingId the accountingId
     */
    public CmFtGlFxAccountingId_Id(
         String accountingId
      ) {
      super(
        accountingId
            );
    }

    /**
      * Create a new CmFtGlFxAccountingId_Id object from the passed map of field/value pairs:
      *            Key           Type
      *     accountingId = a String
      *
      * @param valuePairs a Map of key/value pairs
      */
    public CmFtGlFxAccountingId_Id(Map valuePairs) {
        super(valuePairs);
    }

    /**
     * @see com.splwg.base.api.datatypes.SingleFieldId#getFieldName()
     */
    @Override
    public String getFieldName() {
         return "ACCOUNTING_ID";
    }

    /**
     * @see com.splwg.base.api.datatypes.EntityId#getEntityName()
     */
    @Override
    public String getEntityName() {
        return "cmFtGlFxAccountingId";
    }

    /**
      * @see com.splwg.base.api.datatypes.StringId#fieldSize()
      */
    @Override      
    public int fieldSize() {
        return FIELD_SIZE;
    }

    @Override      
    protected Class getUserTypeClass() {
        return com.splwg.base.support.hibernatetypes.StringIdType.class;
    }

    


    @Override
    	public Class<CmFtGlFxAccountingId> getEntityInterface() {
	        return CmFtGlFxAccountingId.class;    
    }
}
