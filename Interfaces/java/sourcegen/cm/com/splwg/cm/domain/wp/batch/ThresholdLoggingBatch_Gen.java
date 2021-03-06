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


/**
  * Generated super class for the thresholdLoggingBatch batch job
  *
  * @author Generated by com.splwg.tools.artifactgen.ArtifactGenerator
  */
public abstract class ThresholdLoggingBatch_Gen extends AbstractBatchJob
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
        
public final com.splwg.base.api.datatypes.Date getBillDate () {
        com.splwg.base.api.datatypes.Date result = privateGetDateSoftParameter("billDate");
        return result;
}

public final com.splwg.base.api.datatypes.Date getTxnDt () {
        com.splwg.base.api.datatypes.Date result = privateGetDateSoftParameter("txnDt");
        return result;
}

public final String getBatchType () {
        String result = privateGetSoftParameter("batchType");
        if (result==null) reportRequiredParameter("batchType");
        return result;
}

public final String getThreshold () {
        String result = privateGetSoftParameter("threshold");
        return result;
}

public final String getCategory () {
        String result = privateGetSoftParameter("category");
        return result;
}


        @Override
       protected final void validateParameterTypes() {
           getBillDate();
           getTxnDt();
           getBatchType();
           getThreshold();
           getCategory();
       }
    }

    public abstract static class ThresholdLoggingBatchWorker_Gen
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
