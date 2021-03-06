/**
 * Generated by com.splwg.tools.artifactgen.ArtifactGenerator
 * Template: BatchJobGenClass.vm
 * $File: //FW/4.0.1/code/modules/tools/source/java/com/splwg/tools/artifactgen/templates/BatchJobGenClass.vm $
 * $DateTime: 2009/12/17 11:38:59 $
 * $Revision: #1 $
 */
package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.batch.AbstractBatchJob;
import com.splwg.base.api.batch.AbstractThreadWorker;
import com.splwg.base.api.batch.StandardJobParameters;
import com.splwg.base.api.batch.SubmissionParameters;
import com.splwg.base.domain.common.characteristicType.CharacteristicType_Id;
import com.splwg.base.domain.common.characteristicType.CharacteristicType;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType_Id;
import com.splwg.ccb.domain.admin.idType.accountIdType.AccountNumberType;


/**
  * Generated super class for the cmAssignGlAccount batch job
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class CmAssignGlAccount_Gen extends AbstractBatchJob
{

   //~ Instance fields --------------------------------------------------------------------------------------

    private JobParameters jobParameters;

    //~ Methods ----------------------------------------------------------------------------------------------

	    @Override
    public final void setSubmissionParameters(SubmissionParameters submissionParameters) {
        super.setSubmissionParameters(submissionParameters);
        jobParameters = new JobParameters(submissionParameters);
    }

    @Override
    protected final JobParameters getParameters() {
        return jobParameters;
    }

    //~ Inner Classes ----------------------------------------------------------------------------------------

    protected static class JobParameters
        extends StandardJobParameters {

        //~ Constructors -------------------------------------------------------------------------------------

        private JobParameters(SubmissionParameters parms) {
            super(parms);
        }
        
        //~ Methods ------------------------------------------------------------------------------------------
        
public final CharacteristicType getTaxGLAccountCharType () {
        String idValue = privateGetSoftParameter("taxGLAccountCharType");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType taxGLAccountCharType = id.getEntity();
	        validateSoftEntityExists("taxGLAccountCharType", id, taxGLAccountCharType);
	        return taxGLAccountCharType;
}

public final CharacteristicType getOverrideDistributionCodeCharTy () {
        String idValue = privateGetSoftParameter("overrideDistributionCodeCharTy");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType overrideDistributionCodeCharTy = id.getEntity();
	        validateSoftEntityExists("overrideDistributionCodeCharTy", id, overrideDistributionCodeCharTy);
	        return overrideDistributionCodeCharTy;
}

public final String getIdType () {
        String result = privateGetSoftParameter("idType");
        if (result==null) reportRequiredParameter("idType");
        return result;
}

public final CharacteristicType getCounterPartyCharType () {
        String idValue = privateGetSoftParameter("counterPartyCharType");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType counterPartyCharType = id.getEntity();
	        validateSoftEntityExists("counterPartyCharType", id, counterPartyCharType);
	        return counterPartyCharType;
}

public final CharacteristicType getIntercompanyCharType () {
        String idValue = privateGetSoftParameter("intercompanyCharType");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType intercompanyCharType = id.getEntity();
	        validateSoftEntityExists("intercompanyCharType", id, intercompanyCharType);
	        return intercompanyCharType;
}

public final String getIntercompanyCharValue () {
        String result = privateGetSoftParameter("intercompanyCharValue");
        if (result==null) reportRequiredParameter("intercompanyCharValue");
        return result;
}

public final CharacteristicType getBusinessUnitCharType () {
        String idValue = privateGetSoftParameter("businessUnitCharType");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType businessUnitCharType = id.getEntity();
	        validateSoftEntityExists("businessUnitCharType", id, businessUnitCharType);
	        return businessUnitCharType;
}

public final CharacteristicType getSchemeCharType () {
        String idValue = privateGetSoftParameter("schemeCharType");
	        CharacteristicType_Id id = new CharacteristicType_Id(idValue);

        CharacteristicType schemeCharType = id.getEntity();
	        validateSoftEntityExists("schemeCharType", id, schemeCharType);
	        return schemeCharType;
}

public final AccountNumberType getFundAccountNumberType () {
        String idValue = privateGetSoftParameter("fundAccountNumberType");
	        AccountNumberType_Id id = new AccountNumberType_Id(idValue);

        AccountNumberType fundAccountNumberType = id.getEntity();
	        validateSoftEntityExists("fundAccountNumberType", id, fundAccountNumberType);
	        return fundAccountNumberType;
}

public final String getFundAccountNumber () {
        String result = privateGetSoftParameter("fundAccountNumber");
        if (result==null) reportRequiredParameter("fundAccountNumber");
        return result;
}

public final String getFundGLAccount () {
        String result = privateGetSoftParameter("fundGLAccount");
        if (result==null) reportRequiredParameter("fundGLAccount");
        return result;
}

public final String getPayType () {
        String result = privateGetSoftParameter("payType");
        if (result==null) reportRequiredParameter("payType");
        return result;
}

public final String getNegGlAccount () {
        String result = privateGetSoftParameter("negGlAccount");
        if (result==null) reportRequiredParameter("negGlAccount");
        return result;
}

public final String getDstCdForDebtFund () {
        String result = privateGetSoftParameter("dstCdForDebtFund");
        if (result==null) reportRequiredParameter("dstCdForDebtFund");
        return result;
}

public final String getTaxDistributionCode () {
        String result = privateGetSoftParameter("taxDistributionCode");
        if (result==null) reportRequiredParameter("taxDistributionCode");
        return result;
}

public final String getDivision () {
        String result = privateGetSoftParameter("division");
        return result;
}

public final java.math.BigInteger getNoOfRetries () {
        java.math.BigInteger result = privateGetIntegerSoftParameter("noOfRetries");
        return result;
}


        @Override
       protected final void validateParameterTypes() {
           getTaxGLAccountCharType();
           getOverrideDistributionCodeCharTy();
           getIdType();
           getCounterPartyCharType();
           getIntercompanyCharType();
           getIntercompanyCharValue();
           getBusinessUnitCharType();
           getSchemeCharType();
           getFundAccountNumberType();
           getFundAccountNumber();
           getFundGLAccount();
           getPayType();
           getNegGlAccount();
           getDstCdForDebtFund();
           getTaxDistributionCode();
           getDivision();
           getNoOfRetries();
       }
    }

    public abstract static class CmAssignGlAccountWorker_Gen
        extends AbstractThreadWorker
		            {
        //~ Instance fields ----------------------------------------------------------------------------------

        private ThreadParameters threadParameters;

        //~ Methods ------------------------------------------------------------------------------------------

        protected final ThreadParameters getParameters() {
            return threadParameters;
        }

        @Override
        public final void setSubmissionParameters(SubmissionParameters parms) {
            super.setSubmissionParameters(parms);
            threadParameters = new ThreadParameters(parms);
        }

        //~ Inner Classes ------------------------------------------------------------------------------------

        protected static class ThreadParameters
            extends JobParameters {

            //~ Constructors ---------------------------------------------------------------------------------

            private ThreadParameters(SubmissionParameters parms) {
                super(parms);
            }

        }

    }
}
