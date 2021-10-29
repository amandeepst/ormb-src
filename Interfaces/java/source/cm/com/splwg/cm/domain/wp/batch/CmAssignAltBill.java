/*******************************************************************************
 * FileName                   : CmAssignAltBill.java
 * File Description           : This multi-threaded batch process generates ALT_BILL_ID for all the completed bills in the system.
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Apr 30, 2018
 * Version Number             : 0.1
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Apr 30, 2018          RIA		  Batch to generate alternate bill id as per NAP-26548.
 *******************************************************************************/
package com.splwg.cm.domain.wp.batch;

import java.util.List;

import com.splwg.base.api.Query;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.BillStatusLookup;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_DTO;
import com.splwg.ccb.domain.billing.bill.Bill_Id;

/**
 * @author Administrator
 *
@BatchJob (modules = { "demo"})
 */
public class CmAssignAltBill extends CmAssignAltBill_Gen {

	public JobWork getJobWork() {
		StringBuilder querySb1 = new StringBuilder();
		querySb1.append(" from Bill bill where (bill.seqBillNumber = ' ' OR bill.seqBillNumber is NULL) ");
		querySb1.append(" and bill.billStatus = :compBill ");
		
		Query<QueryResultRow> query = createQuery(querySb1.toString(),"");
		query.bindLookup("compBill", BillStatusLookup.constants.COMPLETE);
		
		query.addResult("billId", "bill.id");
		QueryIterator<QueryResultRow> queryItr = query.iterate();
		
		return createJobWorkForQueryIterator(queryItr, this);

	}
	
	@Override
	public ThreadWorkUnit createWorkUnit(QueryResultRow queryResultRow) {
		// TODO Auto-generated method stub
		ThreadWorkUnit threadWorkUnit = new ThreadWorkUnit();	
		threadWorkUnit.setPrimaryId((Bill_Id) queryResultRow.get("billId"));
		return threadWorkUnit;
	}
	
	public Class<CmAssignAltBillWorker> getThreadWorkerClass() {
		return CmAssignAltBillWorker.class;
	}

	public static class CmAssignAltBillWorker extends CmAssignAltBillWorker_Gen {

		public ThreadExecutionStrategy createExecutionStrategy() {
			// TODO Auto-generated method stub
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
				throws ThreadAbortedException, RunAbortedException {
			/** Start processing bill as work unit **/
			Bill_Id billId = (Bill_Id) unit.getPrimaryId();
			Bill bill = billId.getEntity();
			Bill_DTO bill_Dto = bill.getDTO();
			
			String sql = " SELECT CM_ALT_BILL_SEQ.NEXTVAL as SEQ_NUM FROM DUAL";
			PreparedStatement ps = createPreparedStatement(sql,"");
			ps.setAutoclose(false);
			List<SQLResultRow> result = ps.list();
			
			if(notNull(result) && result.size()>0){
				bill_Dto.setSeqBillNumber(result.get(0).getInteger("SEQ_NUM").toString());
				bill.setDTO(bill_Dto);
			}
			
			if(notNull(ps)){
				result=null;
				ps.close();
			}
			
			return true;
		}

	}

}
