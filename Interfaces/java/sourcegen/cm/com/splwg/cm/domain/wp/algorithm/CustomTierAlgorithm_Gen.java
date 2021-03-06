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
  * Generated super class for the customTierAlgorithm algorithm component
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class CustomTierAlgorithm_Gen extends AbstractAlgorithmComponent
         implements CustomTierAlgorithm
{

    public final com.splwg.base.api.datatypes.Bool getIsErrorNoPrice () {
	    com.splwg.base.api.datatypes.Bool result = getBooleanSoftParameter(0);
		if (result == null) reportRequiredParameter("isErrorNoPrice",0);
	    return result;
    }

    public final String getTieringCondition () {
	    String result = getSoftParameter(1);
		if (result == null) reportRequiredParameter("tieringCondition",1);
	    return result;
    }

    public final String getPriceAssignidCharTypeCode () {
	    String result = getSoftParameter(2);
		if (result == null) reportRequiredParameter("priceAssignidCharTypeCode",2);
	    return result;
    }

    public final String getPersonCharTypeCode () {
	    String result = getSoftParameter(3);
		if (result == null) reportRequiredParameter("personCharTypeCode",3);
	    return result;
    }

    public final String getAccountCharTypeCode () {
	    String result = getSoftParameter(4);
		if (result == null) reportRequiredParameter("accountCharTypeCode",4);
	    return result;
    }

    public final String getPriceInformationRequired () {
	    String result = getSoftParameter(5);
		if (result == null) reportRequiredParameter("priceInformationRequired",5);
	    return result;
    }

    public final String getPriceCompIdCharTypeCode () {
	    String result = getSoftParameter(6);
		if (result == null) reportRequiredParameter("priceCompIdCharTypeCode",6);
	    return result;
    }

    public final String getRateValueCharTypeCode () {
	    String result = getSoftParameter(7);
		if (result == null) reportRequiredParameter("rateValueCharTypeCode",7);
	    return result;
    }

    public final String getNoTieringQuantity () {
	    String result = getSoftParameter(8);
		if (result == null) reportRequiredParameter("noTieringQuantity",8);
	    return result;
    }

    public final String getAggSrvQtyCharTypeCode () {
	    String result = getSoftParameter(9);
		if (result == null) reportRequiredParameter("aggSrvQtyCharTypeCode",9);
	    return result;
    }

    public final String getAcctExclusionCharTypeCode () {
	    String result = getSoftParameter(10);
		if (result == null) reportRequiredParameter("acctExclusionCharTypeCode",10);
	    return result;
    }

    public final String getProductUomRelFlag () {
	    String result = getSoftParameter(11);
		if (result == null) reportRequiredParameter("productUomRelFlag",11);
	    return result;
    }

    public final String getStackingReq () {
	    String result = getSoftParameter(12);
		if (result == null) reportRequiredParameter("stackingReq",12);
	    return result;
    }

    public final String getHierarchyAlgoCd () {
	    String result = getSoftParameter(13);
		if (result == null) reportRequiredParameter("hierarchyAlgoCd",13);
	    return result;
    }

    public final String getSrvQtyCharTypeCode () {
	    String result = getSoftParameter(14);
	    return result;
    }

    protected final void validateParameterTypes() {
        getIsErrorNoPrice();
        getTieringCondition();
        getPriceAssignidCharTypeCode();
        getPersonCharTypeCode();
        getAccountCharTypeCode();
        getPriceInformationRequired();
        getPriceCompIdCharTypeCode();
        getRateValueCharTypeCode();
        getNoTieringQuantity();
        getAggSrvQtyCharTypeCode();
        getAcctExclusionCharTypeCode();
        getProductUomRelFlag();
        getStackingReq();
        getHierarchyAlgoCd();
        getSrvQtyCharTypeCode();

    }
   

}
