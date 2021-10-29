/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: ComponentInterface.VM
 * $File: //FW/4.0.1/Code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/ComponentInterface.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.algorithm;

import com.splwg.base.api.BusinessComponent;
import com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCodeGlAccountConstructionAlgorithmSpot; 

/**
  * Interface for the cmGLAccountConstruction component
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public interface CmGLAccountConstruction extends BusinessComponent
                 , GeneralLedgerDistributionCodeGlAccountConstructionAlgorithmSpot 
                  {


    /**
      * 
      */
     
     void invoke() ;

    /**
      * 
      */
     
     java.lang.String getGlAccount() ;

    /**
      * 
      */
     
     void setFinancialTransaction(com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction arg0) ;

    /**
      * 
      */
     
     void setGlDistribution(com.splwg.ccb.domain.admin.generalLedgerDistributionCode.GeneralLedgerDistributionCode arg0) ;

    /**
      * 
      */
     
     void setGlSequenceNumber(java.math.BigInteger arg0) ;

    /**
      * 
      */
     
     void setTrialFinancialTransaction(com.splwg.ccb.domain.billing.trialBilling.TrialFinancialTransaction arg0) ;

}
