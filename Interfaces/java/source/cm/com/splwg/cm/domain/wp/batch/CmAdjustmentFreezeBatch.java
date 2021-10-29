package com.splwg.cm.domain.wp.batch;

/***********************************************************
 * This program will select all the freezable adjustments 
 * and freeze them with process date.
 * Author: RIA         Date: 06-Sep-2018
 ************************************************************/


import java.util.ArrayList;
import java.util.List;

import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.AdjustmentStatusLookup;
import com.splwg.ccb.domain.adjustment.adjustment.Adjustment;
import com.splwg.ccb.domain.adjustment.adjustment.Adjustment_Id;

/**
 * @author RIA
 *
@BatchJob (modules = { "demo"})
 */

public class CmAdjustmentFreezeBatch extends CmAdjustmentFreezeBatch_Gen {

	public JobWork getJobWork() {

		List<ThreadWorkUnit> unitList= new ArrayList<ThreadWorkUnit>();
		
		//Fetch freezable adjustments from adjustment table.
		
		String sql = " SELECT ADJ_ID FROM CI_ADJ WHERE ADJ_STATUS_FLG=:freezable ";
		PreparedStatement ps = createPreparedStatement(sql,this.getClass().getName());
		ps.setAutoclose(false);
		ps.bindLookup("freezable",AdjustmentStatusLookup.constants.FREEZABLE);
		List<SQLResultRow> result = ps.list();
		
		if(notNull(result)&& result.size()>0){
			for(SQLResultRow row: result){
					String rowVal= row.getString("ADJ_ID");
					if(!isBlankOrNull(rowVal)){
						
						ThreadWorkUnit unit = new ThreadWorkUnit();
						unit.setPrimaryId(new Adjustment_Id(rowVal.trim()));
						unitList.add(unit);
						unit=null;
						
					}//End-if
				
			}//End-for
		}//End-if
		

		if(notNull(ps)){
			ps.close();
			ps=null;
			result=null;
		}
		
		return createJobWorkForThreadWorkUnitList(unitList);
	}

	public Class<CmAdjustmentFreezeBatchWorker> getThreadWorkerClass() {
		return CmAdjustmentFreezeBatchWorker.class;
	}

	public static class CmAdjustmentFreezeBatchWorker extends	CmAdjustmentFreezeBatchWorker_Gen {

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)	throws ThreadAbortedException, RunAbortedException {
			
			//Freeze adjustment
			Adjustment_Id adjId= (Adjustment_Id) unit.getPrimaryId();
			Adjustment adj = adjId.getEntity();
			
			adj.freeze(getProcessDateTime().getDate());
			
			//Update CI_FT.SHOW_ON_BILL_SW='Y' if print default is True.
//			if(adj.getAdjustmentType().getShouldPrintByDefault().isTrue()){
//				
//				String sql= "UPDATE CI_FT SET SHOW_ON_BILL_SW ='Y' WHERE FT_TYPE_FLG ='AD' AND SIBLING_ID=:adjId  ";
//				PreparedStatement ps = createPreparedStatement(sql,this.getClass().getName());
//				ps.bindString("adjId",adjId.getTrimmedValue(),"SIBLING_ID");
//				ps.setAutoclose(false);
//				ps.executeUpdate();
//				
//				if(notNull(ps)){
//					ps.close();
//					ps=null;
//				}
//			}
			
			
			return true;
		}

	}

}
