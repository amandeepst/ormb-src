/*
 **********************************************************************
 *                Confidentiality Information:                    
 *                                                                
 * This module is the confidential and proprietary information of 
 * Oracle Corporation; it is not to be copied, reproduced, or         
 * transmitted in any form, by any means, in whole or in part,    
 * nor is it to be used for any purpose other than that for which 
 * it is expressly provided without the written permission of     
 * Oracle Corporation.                                                
 ***********************************************************************
 *                                                                
 * PROGRAM DESCRIPTION:
 * 
 * This algorithm will negate the transaction volume of the transaction 
 * detail passed if the value on UDF_NBR_20 is -1.
 * 
 ***********************************************************************
 *                                                                
 * CHANGE HISTORY:                                                
 *                                                                
 * Date:        by:         Reason:                                     
 * 2016-11-07 	VRom		NAP-9977. Initial Version.
 * 
 ***********************************************************************
 */
package com.splwg.cm.domain.wp.algorithm;

import java.math.BigDecimal;

import com.splwg.ccb.domain.banking.transactionFeed.transactionFeedAgg.algorithm.TransactionProductDerivationPostProcessingAlgorithmSpot;
import com.splwg.ccb.domain.banking.transactionFeed.transactionFeedAgg.vo.TransactionDetailVO;
import com.splwg.cm.domain.wp.algorithm.CmNegateTransactionVolumeAlgComp_Gen;

/**
 * @author vrom
 *
@AlgorithmComponent ()
 */
public class CmNegateTransactionVolumeAlgComp_Impl extends
		CmNegateTransactionVolumeAlgComp_Gen implements
		TransactionProductDerivationPostProcessingAlgorithmSpot {

	private static final BigDecimal NEGATIVE_ONE = BigDecimal.valueOf(-1);
	
	//Hard parameter
	TransactionDetailVO txnDetailVo;

	public void setTransactionDetailVO(TransactionDetailVO paramTransactionDetailVO) {
		txnDetailVo = paramTransactionDetailVO;
	}

	public TransactionDetailVO getTransactionDetailVO() {
		return txnDetailVo;
	}

	public void invoke() {

		//Negate transaction volume if UDF_NBR_20 is -1
		if(txnDetailVo.getUdfNbr20().equals(NEGATIVE_ONE)) {
			txnDetailVo.setTxnVol(txnDetailVo.getTxnVol().negate());
		}
		
	}

}
