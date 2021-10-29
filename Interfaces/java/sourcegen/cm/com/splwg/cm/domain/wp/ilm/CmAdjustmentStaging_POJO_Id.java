/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: HibernatePOJOSingleFieldIdClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/HibernatePOJOSingleFieldIdClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.ilm;

import java.math.BigInteger;
import com.splwg.base.api.datatypes.HibernatePOJOIntegerId;

/**
  * Generated Id class for CmAdjustmentStaging
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public class CmAdjustmentStaging_POJO_Id extends HibernatePOJOIntegerId {
    public static final int FIELD_SIZE = 12;

    public CmAdjustmentStaging_POJO_Id(
         BigInteger adjustmentUploadId
      ) {
      super(
        adjustmentUploadId
            );
    }

    public String getFieldName() {
         return "ADJ_STG_UP_ID";
    }

    @Override
    public int fieldSize() {
        return FIELD_SIZE;
    }

    
    public String getEntityName() {
        return "cmAdjustmentStaging";
    }

    @Override
    public String getSimpleIdClassName() {
        return "CmAdjustmentStaging_Id";
    }
        
    
}