/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: HibernatePOJOIdUserType.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/HibernatePOJOIdUserType.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.entity;


import com.splwg.base.api.datatypes.HibernatePOJOStringId;
import com.splwg.base.support.hibernatetypes.HibernatePOJOStringIdType;

public class CmFtGlFxAccountingId_POJO_IdType extends HibernatePOJOStringIdType {

    @Override
    public Class returnedClass() {
        return CmFtGlFxAccountingId_POJO_Id.class;
    }
    
    @Override
    protected HibernatePOJOStringId getId(String value) {
        return new CmFtGlFxAccountingId_POJO_Id(value);
    }
    

}
