/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: AlgorithmComponentGenClass.vm
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/AlgorithmComponentGenClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.algorithm;

import com.splwg.base.support.algorithm.AbstractAlgorithmComponent;

/**
  * Generated super class for the cmDetermineContractAlgComp algorithm component
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class CmDetermineContractAlgComp_Gen extends AbstractAlgorithmComponent
         implements CmDetermineContractAlgComp
{

    public final String getCharTypeCd () {
	    String result = getSoftParameter(0);
		if (result == null) reportRequiredParameter("charTypeCd",0);
	    return result;
    }

    protected final void validateParameterTypes() {
        getCharTypeCd();

    }
   

}