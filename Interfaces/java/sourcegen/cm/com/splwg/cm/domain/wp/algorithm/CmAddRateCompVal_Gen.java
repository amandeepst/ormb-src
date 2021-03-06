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
  * Generated super class for the cmAddRateCompVal algorithm component
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class CmAddRateCompVal_Gen extends AbstractAlgorithmComponent
         implements CmAddRateCompVal
{

    public final java.math.BigInteger getRcSequence1 () {
	    java.math.BigInteger result = getIntegerSoftParameter(0);
		if (result == null) reportRequiredParameter("rcSequence1",0);
	    return result;
    }

    public final java.math.BigInteger getRcSequence2 () {
	    java.math.BigInteger result = getIntegerSoftParameter(1);
		if (result == null) reportRequiredParameter("rcSequence2",1);
	    return result;
    }

    public final java.math.BigInteger getRoundToDecimal () {
	    java.math.BigInteger result = getIntegerSoftParameter(2);
		if (result == null) reportRequiredParameter("roundToDecimal",2);
	    return result;
    }

    protected final void validateParameterTypes() {
        getRcSequence1();
        getRcSequence2();
        getRoundToDecimal();

    }
   

}
