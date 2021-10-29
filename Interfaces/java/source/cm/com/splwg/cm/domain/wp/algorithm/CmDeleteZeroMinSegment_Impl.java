package com.splwg.cm.domain.wp.algorithm;

/***********************************************************
 * Author: RIA
 * Description: This algorithm will find all those bill
 * segments in a bill which have 0 dollar calc line header
 * for minimum charge product and then delete those bill
 * segments including calc lines and FT details.
 * No tax information will be deleted.
 * 
 * FileName                   : CmDeleteZeroMinSegment_Impl.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : July 16, 2018
 * Version Number             : 0.2
 * Revision History           :
 VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
 0.1      NA             16-Jul-2018         RIA  		       Implemented all requirement as per Design.
 0.2      NAP-37745      14-Dec-2018         Somya Sikka       Updated to exclude overpay adjustments while checking for adjustments before deleting a bill.
 ************************************************************/


import java.util.List;

import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.datatypes.Date;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.api.lookup.AdjustmentStatusLookup;
import com.splwg.ccb.api.lookup.BillCompletionActionLookup;
import com.splwg.ccb.api.lookup.FinancialTransactionTypeLookup;
import com.splwg.ccb.domain.admin.customerClass.CustomerClassPreBillCompletionAlgorithmSpot;
import com.splwg.ccb.domain.billing.bill.Bill;
import com.splwg.ccb.domain.billing.bill.Bill_Id;
import com.splwg.ccb.domain.billing.billSegment.BillSegment;
import com.splwg.ccb.domain.billing.billSegment.BillSegment_Id;
import com.splwg.ccb.domain.customerinfo.account.Account;
import com.splwg.ccb.domain.financial.financialTransaction.FinancialTransaction_Id;
import com.splwg.cm.domain.wp.batch.InboundPaymentsLookUp;
import com.splwg.shared.common.ServerMessage;

/**
 * @author RIA
 *
@AlgorithmComponent (softParameters = { @AlgorithmSoftParameter (entityName = priceItem, name = priceItem, required = true, type = entity)})
 */
public class CmDeleteZeroMinSegment_Impl extends CmDeleteZeroMinSegment_Gen
		implements CustomerClassPreBillCompletionAlgorithmSpot {

	private Bill bill;
	private BillCompletionActionLookup actionLookup;
	private InboundPaymentsLookUp payConfirmationLookup = new InboundPaymentsLookUp(); 
	
	@Override
	public void invoke() {
		//Get post processing 0 dollar segments for product given in input
		StringBuilder sb = new StringBuilder();
		sb.append(" SELECT A.BSEG_ID FROM CI_BSEG A , CI_BSEG_EXT B , CI_BSEG_CALC C ");
		sb.append(" WHERE A.BSEG_ID=B.BSEG_ID AND A.BILL_ID=:bill AND B.BSEG_ID=C.BSEG_ID ");
		sb.append(" AND B.BSEG_TYPE_FLG='POST' AND B.PRICEITEM_CD=:priceitem AND C.CALC_AMT=0 ");
		
		PreparedStatement ps = createPreparedStatement(sb.toString(),this.getClass().getName());
		ps.bindId("bill", bill.getId());
		ps.bindId("priceitem", getPriceItem().getId());
		ps.setAutoclose(false);
		List<SQLResultRow> result = ps.list();
		
		if(notNull(result) && !result.isEmpty()){
			
			for(SQLResultRow row: result){
				
				String billSegment = row.getString("BSEG_ID");
				

				// Deelete FT
				fTData(billSegment);
				//Delete bill segment
				deleteBillSegment(billSegment);
				
			}//end-for
			
		}//end-if
		
		if(notNull(ps)){
			ps.close();
			ps=null;
		}//end-if
		
		StringBuilder ftChk = new StringBuilder();
		ftChk.append(" SELECT B.FT_ID FROM CI_SA A, CI_FT B, CI_ADJ C ");
		ftChk.append(" WHERE A.SA_ID = B.SA_ID ");
		ftChk.append(" AND B.SIBLING_ID = C.ADJ_ID ");
		ftChk.append(" AND B.FT_TYPE_FLG IN (:adj,:adjCancel) ");
		ftChk.append(" AND C.ADJ_STATUS_FLG IN (:adjFreezable,:adjFrozen) ");
		ftChk.append(" AND C.ADJ_TYPE_CD NOT IN (:ovpyChrg, :ovpyFnd, :ovpyChbk) ");
		ftChk.append(" AND A.ACCT_ID = :acctId ");
		ftChk.append(" AND B.SHOW_ON_BILL_SW = 'Y' ");
		ftChk.append(" AND B.BILL_ID = ' ' ");
		
		ps = createPreparedStatement(ftChk.toString(), "Query to fetch Overpayment Adjustments ");
		ps.bindLookup("adjFreezable", AdjustmentStatusLookup.constants.FREEZABLE);
		ps.bindLookup("adjFrozen", AdjustmentStatusLookup.constants.FROZEN);
		ps.bindLookup("adj", FinancialTransactionTypeLookup.constants.ADJUSTMENT);
		ps.bindLookup("adjCancel", FinancialTransactionTypeLookup.constants.ADJUSTMENT_CANCELLATION);
		ps.bindString("ovpyChrg", payConfirmationLookup.getOverpayChrg(), "ADJ_TYPE_CD");
		ps.bindString("ovpyFnd", payConfirmationLookup.getOverpayFund(), "ADJ_TYPE_CD");
		ps.bindString("ovpyChbk", payConfirmationLookup.getOverpayChbk(), "ADJ_TYPE_CD");
		ps.bindId("acctId", bill.getAccount().getId());
		ps.setAutoclose(false);
		List<SQLResultRow> ftResult = ps.list();
		
		if(notNull(ftResult) && !ftResult.isEmpty()) {
			actionLookup=BillCompletionActionLookup.constants.CARRYONWITH_BILL_COMPLETION;
		}		
		else {
			actionLookup = checkIfBillSegmentExist(bill.getId());
		}
		
		if(notNull(ps)){
			ps.close();
			ps=null;
		}//end-if
		
	}//end

	private BillCompletionActionLookup checkIfBillSegmentExist(Bill_Id billId){
		StringBuilder sb = new StringBuilder();
		BillCompletionActionLookup lookup;
		sb.append(" SELECT * FROM CI_BSEG WHERE BILL_ID = :billId");
		PreparedStatement ps = createPreparedStatement(sb.toString(),this.getClass().getName());
		ps.bindId("billId", bill.getId());
		ps.setAutoclose(false);
		List<SQLResultRow> bsegResult = ps.list();
		

		// NAP-31325: Use BillCompletionActionLookup for Deleting Bill
		if(notNull(bsegResult) && bsegResult.isEmpty() && !checkCreditNote(billId)){
			lookup=BillCompletionActionLookup.constants.DELETE_BILL;
		} else {
			lookup=BillCompletionActionLookup.constants.CARRYONWITH_BILL_COMPLETION;
		}
		
		if(notNull(ps)){
			ps.close();
			ps=null;
		}//end-if
		
		
		return lookup;
	}
	
	
	private Boolean checkCreditNote(Bill_Id billId) {
		Bill_Id creditNote=billId.getEntity().getCrNoteFromBillId();
		if (creditNote != null && creditNote.getTrimmedValue() != null){
			return true;
		}
		else {
			return false;
		}	
	}
	/**
	 * This method will delete bill segment including it's FT details.
	 * 
	 * @param billSegment
	 */
	private void deleteBillSegment(String billSegment) {

		String bsegSQL;
		PreparedStatement delBs = null;
		
		// CI_BSEG_CL_CHAR
		bsegSQL = " DELETE FROM CI_BSEG_CL_CHAR WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_CALC_LN
		bsegSQL = " DELETE FROM CI_BSEG_CALC_LN WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_CALC
		bsegSQL = " DELETE FROM CI_BSEG_CALC WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_EXT
		bsegSQL = " DELETE FROM CI_BSEG_EXT WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_SQ
		bsegSQL = " DELETE FROM CI_BSEG_SQ WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_EXCP
		bsegSQL = " DELETE FROM CI_BSEG_EXCP WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG_K
		bsegSQL = " DELETE FROM CI_BSEG_K WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

		bsegSQL = null;
		delBs = null;
		// CI_BSEG
		bsegSQL = " DELETE FROM CI_BSEG WHERE BSEG_ID = :bseg  ";
		delBs = createPreparedStatement(bsegSQL, "");
		delBs.bindId("bseg", new BillSegment_Id(billSegment.trim()));
		delBs.setAutoclose(false);
		delBs.execute();

		if (notNull(delBs)) {
			delBs.close();
			delBs = null;
		}

	}

	private void fTData(String bsegId) {
		String FtId = "SELECT FT_ID FROM CI_FT WHERE FT_TYPE_FLG IN ('BS','BX') AND SIBLING_ID = :bseg";
		PreparedStatement gtFtId = createPreparedStatement(FtId, "");
		gtFtId.bindId("bseg", new BillSegment_Id(bsegId.trim()));
		gtFtId.setAutoclose(false);
		List<SQLResultRow> ftResult = gtFtId.list();

		if (!ftResult.isEmpty()) {
			for (SQLResultRow rw : ftResult) {

				FinancialTransaction_Id ftID = new FinancialTransaction_Id(
						rw.getString("FT_ID"));
				delFt(ftID);
			}
		}
		if (notNull(gtFtId)) {
			gtFtId.close();
			gtFtId = null;
		}
	}

	private void delFt(FinancialTransaction_Id ftId) {
		// FT_GL_EXT
		String ftSQL1 = " DELETE FROM CI_FT_GL_EXT WHERE FT_ID = :ftId ";
		PreparedStatement delftSQL = createPreparedStatement(ftSQL1, "");
		delftSQL.bindId("ftId", ftId);
		delftSQL.setAutoclose(false);
		delftSQL.execute();

		if (notNull(delftSQL)) {
			delftSQL.close();
			delftSQL = null;
		}

		// FT GL
		String ftSQL2 = " DELETE FROM CI_FT_GL WHERE FT_ID = :ftId ";
		delftSQL = createPreparedStatement(ftSQL2, "");
		delftSQL.bindId("ftId", ftId);
		delftSQL.setAutoclose(false);
		delftSQL.execute();

		if (notNull(delftSQL)) {
			delftSQL.close();
			delftSQL = null;
		}

		// FT_K
		String ftSQL3 = " DELETE FROM CI_FT_K WHERE  FT_ID = :ftId";
		delftSQL = createPreparedStatement(ftSQL3, "");
		delftSQL.bindId("ftId", ftId);
		delftSQL.setAutoclose(false);
		delftSQL.execute();

		if (notNull(delftSQL)) {
			delftSQL.close();
			delftSQL = null;
		}

		// FT
		String ftSQL4 = " DELETE FROM CI_FT WHERE FT_ID = :ftId ";
		delftSQL = createPreparedStatement(ftSQL4, "");
		delftSQL.bindId("ftId", ftId);
		delftSQL.setAutoclose(false);
		delftSQL.execute();

		if (notNull(delftSQL)) {
			delftSQL.close();
			delftSQL = null;
		}
	}
	@Override
	public Bool getAllowReopeningOfBills() {
		return null;
	}

	@Override
	public BillCompletionActionLookup getBillCompletionAction() {
		return actionLookup;
	}

	@Override
	public ServerMessage getServerMessage() {
		return null;
	}

	@Override
	public void setAccount(Account arg0) {
	}

	@Override
	public void setAccountingDate(Date arg0) {
	}

	@Override
	public void setAllowReopeningOfBills(Bool arg0) {
		
	}

	@Override
	public void setBill(Bill arg0) {
		this.bill = arg0;
	}

	@Override
	public void setBillDate(Date arg0) {
		
	}

	@Override
	public void setBillSegments(List<BillSegment> arg0) {
		
	}

	@Override
	public void setCreditNoteForBill(Bill arg0) {
		
	}

}
