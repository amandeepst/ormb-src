/*******************************************************************************
 * FileName                   : PriceTypeDataInterface.java
 * Project Name               : WorldPay RMB Implementation
 * Date of Creation           : Mar 24, 2015
 * Version Number             : 0.8
 * Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1      NA             Mar 24, 2015         Preeti       Implemented all requirements for CD1.
0.2      NA             May 21, 2015         Preeti       Fix for Defect PAM-1844.
0.3      NA             Jun 11, 2015         Preeti       Fix for Defect PAM-2012.
0.4      NA             Aug 11, 2015         Preeti       Implemented changes as per NAP3 requirements.
0.5      NA             May 02, 2015         Preeti       Implemented code review changes.
0.6      NA             May 24, 2017         Preeti       PAM-12299 Fix to end date issue for classifier records.
0.7      NA             Jun 22, 2017         Vienna       NAP-15568 Renamed nested Id class
0.8      NA             Jul 13, 2017         Ankur        PAM-14009 Fix
0.9	   	 NA			 	Jun 11, 2018		 RIA		  Prepared Statement close
1.0      NA             Oct 02, 2018         Vikalp       NAP-31952
 *******************************************************************************/

package com.splwg.cm.domain.wp.batch;
import java.util.ArrayList;
import java.util.List;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Id;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.common.CustomMessageRepository;
import com.splwg.cm.domain.wp.common.CustomMessages;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tiwarip404
 *
 @BatchJob (multiThreaded = true, rerunnable = false,
 * modules = {"demo"},
 * softParameters = { @BatchJobSoftParameter (name = customThreadCount, required = true, type = integer)})
 */
public class PriceTypeDataInterface extends PriceTypeDataInterface_Gen {
	
	public static final Logger logger = LoggerFactory.getLogger(PriceTypeDataInterface.class);
	private static final PriceTypeDataInterfaceLookUp priceTypeDataInterfaceLookUp = new PriceTypeDataInterfaceLookUp();
	
	/**
	 * getJobWork() method passes data for processing to the Worker inner class
	 * by the framework.
	 */
	public JobWork getJobWork() {
		logger.info("Inside getJobWork()");
		// To truncate error tables and temporary tables
		//deleteFromPayReqTmpTable(PaymentsRequestInterfaceLookUp.cmPayReqTmp1);
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable1());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable11());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable2());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable3());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable4());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable5());
		deleteFromPriceTypeTmpTable(priceTypeDataInterfaceLookUp.getTempTable6());//error table
		logger.info("Rows deleted from temporary and error tables");
		
		List<ThreadWorkUnit> threadWorkUnitList = getPriceTypeData();
		int rowsForProcessing = threadWorkUnitList.size();
		logger.info("No of rows selected for processing are - "+ rowsForProcessing);
		
		return createJobWorkForThreadWorkUnitList(threadWorkUnitList);
	}

	/**
	 * deleteFromPriceTypeTmpTable() method will delete from the table provided as
	 * input.
	 * 
	 * @param inputPriceTypeTmpTable
	 */
	@SuppressWarnings("deprecation")
	private void deleteFromPriceTypeTmpTable(String inputPriceTypeTmpTable) {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("DELETE FROM "+ inputPriceTypeTmpTable);
			preparedStatement.execute();
		} catch (ThreadAbortedException e) {
			logger.error("Inside deleteFromPriceTypeTmpTable() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside deleteFromPriceTypeTmpTable() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}
	private void nonPriceData() {
		PreparedStatement preparedStatement = null;
		StringBuilder stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_5 (PRICE_CTG_ID, DESCR, STATUS_CD) ");
			stringBuilder.append("SELECT A.CHAR_VAL, A.DESCR, :initialStatus ");
			stringBuilder.append("FROM CI_CHAR_VAL_L A ");
			stringBuilder.append("WHERE A.CHAR_TYPE_CD = :bclTypeCode AND A.LANGUAGE_CD = :languageCode ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.bindString("languageCode",priceTypeDataInterfaceLookUp.getLanguageCode(), "LANGUAGE_CD");
			preparedStatement.bindString("bclTypeCode",priceTypeDataInterfaceLookUp.getBclTypeCode(), "CHAR_TYPE_CD");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}	
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("UPDATE CM_PRICE_CATEGORY A SET A.VALID_TO=SYSTIMESTAMP ");
			stringBuilder.append("where A.PRICE_CTG_ID NOT IN ");
			stringBuilder.append("(SELECT B.PRICE_CTG_ID FROM CM_PRICE_TYPE_TMP_5 B) AND A.VALID_TO IS NULL ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows updated in table CM_PRICE_CATEGORY -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("MERGE INTO CM_PRICE_CATEGORY T ");
			stringBuilder.append("USING (SELECT PRICE_CTG_ID, DESCR FROM CM_PRICE_TYPE_TMP_5) S ON (T.PRICE_CTG_ID=S.PRICE_CTG_ID) ");
			stringBuilder.append("WHEN NOT MATCHED THEN INSERT (PRICE_CTG_ID, DESCR, VALID_FROM, VALID_TO) values (S.PRICE_CTG_ID, S.DESCR, SYSTIMESTAMP, NULL) ");
			stringBuilder.append("WHEN MATCHED THEN UPDATE SET T.DESCR=S.DESCR,VALID_FROM=SYSTIMESTAMP where T.DESCR<>S.DESCR ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows inserted and merged into table CM_PRICE_CATEGORY -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_11 (PRICETYPE_ID, DESCR, STATUS_CD) ");
			stringBuilder.append("SELECT DISTINCT X.PRICETYPE_ID, A.DESCR, :initialStatus "); 
			stringBuilder.append("FROM CM_PRICE_TYPE_TMP_1 X, CI_PRICEITEM_L A ");
			stringBuilder.append("WHERE TRIM(A.PRICEITEM_CD(+))=TRIM(X.PRICETYPE_ID) ");
			stringBuilder.append("AND A.LANGUAGE_CD(+) = :languageCode ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("languageCode",priceTypeDataInterfaceLookUp.getLanguageCode(), "LANGUAGE_CD");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_2 (PRICETYPE_ID, CLASS_GRP_ID, CLASS_ID, EFFDT, STATUS_CD) ");
			stringBuilder.append("SELECT TRIM(A.PRICEITEM_CD), A.CHAR_TYPE_CD, A.CHAR_VAL, A.EFFDT, :initialStatus "); 
			stringBuilder.append("FROM CI_PRICEITEM_CHAR A WHERE A.EFFDT <= SYSDATE ");
			stringBuilder.append("AND TRIM(A.PRICEITEM_CD) IN (SELECT TRIM(X.PRICETYPE_ID) FROM CM_PRICE_TYPE_TMP_1 X ");
			stringBuilder.append("WHERE TRIM(A.PRICEITEM_CD)=TRIM(X.PRICETYPE_ID) AND X.STATUS_CD=:initialStatus) ");
			stringBuilder.append("and A.CHAR_VAL is not null and A.CHAR_VAL!=' ' ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_2 (PRICETYPE_ID, CLASS_GRP_ID, CLASS_ID, EFFDT, STATUS_CD) ");
			stringBuilder.append("SELECT TRIM(A.PRICEITEM_CD), A.CHAR_TYPE_CD, A.ADHOC_CHAR_VAL, A.EFFDT, 'A' "); 
			stringBuilder.append("FROM CI_PRICEITEM_CHAR A WHERE A.EFFDT <= SYSDATE ");
			stringBuilder.append("AND TRIM(A.PRICEITEM_CD) IN (SELECT TRIM(X.PRICETYPE_ID) FROM CM_PRICE_TYPE_TMP_1 X ");
			stringBuilder.append("WHERE TRIM(A.PRICEITEM_CD)=TRIM(X.PRICETYPE_ID) AND X.STATUS_CD=:initialStatus) ");
			stringBuilder.append("and (A.CHAR_VAL is null or A.CHAR_VAL=' ') ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_3 (CLASS_GRP_ID, DESCR, STATUS_CD) ");
			stringBuilder.append("SELECT DISTINCT A.CHAR_TYPE_CD, C.DESCR, :initialStatus ");
			stringBuilder.append("FROM CI_CHAR_TYPE A, CI_CHAR_ENTITY B, CI_CHAR_TYPE_L C ");
			stringBuilder.append("WHERE B.CHAR_ENTITY_FLG(+) = :charEntityCode ");
			stringBuilder.append("AND B.CHAR_TYPE_CD(+) = A.CHAR_TYPE_CD ");
			stringBuilder.append("AND C.CHAR_TYPE_CD(+) = B.CHAR_TYPE_CD ");
			stringBuilder.append("AND C.LANGUAGE_CD(+) = :languageCode ");
			stringBuilder.append("AND A.CHAR_TYPE_CD IN (SELECT X.CLASS_GRP_ID FROM CM_PRICE_TYPE_TMP_2 X ");
			stringBuilder.append("WHERE A.CHAR_TYPE_CD=X.CLASS_GRP_ID ");
			stringBuilder.append("AND (X.STATUS_CD=:initialStatus OR X.STATUS_CD='A') AND X.CLASS_ID is not null AND X.CLASS_ID!=' ') ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("languageCode",priceTypeDataInterfaceLookUp.getLanguageCode(), "LANGUAGE_CD");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.bindString("charEntityCode",priceTypeDataInterfaceLookUp.getCharEntityCode(), "CHAR_ENTITY_FLG");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("INSERT INTO CM_PRICE_TYPE_TMP_4 (CLASS_ID, DESCR, STATUS_CD) ");
			stringBuilder.append("SELECT DISTINCT TMP.CLASS_ID, D.DESCR, :initialStatus ");
			stringBuilder.append("FROM CM_PRICE_TYPE_TMP_2 TMP, CI_CHAR_VAL_L D, CM_PRICE_TYPE_TMP_1 X ");
			stringBuilder.append("WHERE TMP.PRICETYPE_ID=X.PRICETYPE_ID ");
			stringBuilder.append("AND TMP.CLASS_ID = D.CHAR_VAL(+) ");
			stringBuilder.append("AND TRIM(TMP.CLASS_GRP_ID) = TRIM(D.CHAR_TYPE_CD) ");
			stringBuilder.append("AND D.LANGUAGE_CD(+) = :languageCode ");
			stringBuilder.append("AND X.STATUS_CD=:initialStatus AND TMP.CLASS_ID is not null AND TMP.CLASS_ID!=' ' ");
			stringBuilder.append("AND TMP.STATUS_CD!='A' ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			preparedStatement.bindString("languageCode",priceTypeDataInterfaceLookUp.getLanguageCode(), "LANGUAGE_CD");
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			preparedStatement.executeUpdate();
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}	
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("UPDATE CM_PRICE_TYPE C SET C.VALID_TO=SYSTIMESTAMP ");
			stringBuilder.append("where C.PRICETYPE_ID NOT IN ");
			stringBuilder.append("(SELECT DISTINCT A.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_11 A, CM_PRICE_TYPE_TMP_1 B ");
			stringBuilder.append("WHERE A.PRICETYPE_ID=B.PRICETYPE_ID) AND  C.VALID_TO IS NULL");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows updated in table CM_PRICE_TYPE -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		
		try {
			stringBuilder.append("UPDATE CM_CLASS_GRP C SET C.VALID_TO=SYSTIMESTAMP ");
			stringBuilder.append("where C.CLASS_GRP_ID NOT IN ");
			stringBuilder.append("(SELECT DISTINCT A.CLASS_GRP_ID ");
			stringBuilder.append("FROM CM_PRICE_TYPE_TMP_3 A, CM_PRICE_TYPE_TMP_1 B, CM_PRICE_TYPE_TMP_2 C ");
			stringBuilder.append("WHERE C.PRICETYPE_ID=B.PRICETYPE_ID ");
			stringBuilder.append("AND A.CLASS_GRP_ID=C.CLASS_GRP_ID) AND C.VALID_TO IS NULL");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows updated in table CM_CLASS_GRP -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		//This needs to be commented or changed to update latest descriptions
		try {
			stringBuilder.append("UPDATE CM_PRICE_TYP_CLASS C SET C.VALID_TO=SYSTIMESTAMP ");
			stringBuilder.append("where C.CLASS_ID NOT IN ");
			stringBuilder.append("(SELECT DISTINCT A.CLASS_ID ");
			stringBuilder.append("FROM CM_PRICE_TYPE_TMP_4 A, CM_PRICE_TYPE_TMP_2 C ");
			stringBuilder.append("WHERE A.CLASS_ID    =C.CLASS_ID ");
			stringBuilder.append("AND C.PRICETYPE_ID IN (SELECT B.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_1 B ");
			stringBuilder.append("WHERE C.PRICETYPE_ID=B.PRICETYPE_ID)) and C.DESCR!=' ' AND C.VALID_TO IS NULL ");
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows updated in table CM_PRICE_TYP_CLASS -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		stringBuilder = null;
		stringBuilder = new StringBuilder();
		try {
			stringBuilder.append("MERGE INTO CM_CLASS_GRP T ");
			stringBuilder.append("USING (SELECT DISTINCT A.CLASS_GRP_ID,A.DESCR ");
			stringBuilder.append("FROM CM_PRICE_TYPE_TMP_3 A, CM_PRICE_TYPE_TMP_1 B, CM_PRICE_TYPE_TMP_2 C ");
			stringBuilder.append("WHERE C.PRICETYPE_ID=B.PRICETYPE_ID ");
			stringBuilder.append("AND A.CLASS_GRP_ID=C.CLASS_GRP_ID AND A.DESCR is not null AND A.DESCR!=' ') S ON (T.CLASS_GRP_ID=S.CLASS_GRP_ID) ");
			stringBuilder.append("WHEN NOT MATCHED THEN INSERT (CLASS_GRP_ID, DESCR, VALID_FROM, VALID_TO) ");
			stringBuilder.append("values (S.CLASS_GRP_ID, S.DESCR, SYSTIMESTAMP, NULL) ");
			stringBuilder.append("WHEN MATCHED THEN UPDATE SET T.DESCR=S.DESCR,VALID_FROM=SYSTIMESTAMP where T.DESCR<>S.DESCR ");	
			preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
			int count = preparedStatement.executeUpdate();
			logger.info("Rows inserted and merged into table CM_CLASS_GRP -" + count);
		} catch (ThreadAbortedException e) {
			logger.error("Inside nonPriceData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside nonPriceData() method, Error -", e);
		}
		if (preparedStatement != null) {
			preparedStatement.close();
			preparedStatement = null;
		}
		
	}

	// *********************** getPriceTypeData Method******************************

	/**
	 * getPriceTypeData() method selects Price Type IDs for processing by this Interface.
	 * 
	 * @return List Price_Type_Id
	 */
	private List<ThreadWorkUnit> getPriceTypeData() {
		logger.info("Inside getPriceTypeData() method");
		PreparedStatement preparedStatement = null;		
		String lowPriceTypeId = "";
		String highPriceTypeId = "";
		int totalPriceTypeRecords = 0;
		int totalPriceTypeThreads = getParameters().getCustomThreadCount().intValue();
		int rowsInEachPriceTypeThread = 0;
		int remainingPriceTypeRows = 0;
		PriceTypeData_Id priceTypeData = null;
		//***********************
		ThreadWorkUnit threadworkUnit = null;
		List<ThreadWorkUnit> threadWorkUnitList = new ArrayList<ThreadWorkUnit>();
		//***********************
		try {
			preparedStatement = createPreparedStatement("INSERT INTO CM_PRICE_TYPE_TMP_1 (PRICETYPE_ID,"
					+ " STATUS_CD"
					+ " )"
					+ " SELECT TRIM(A.PRICEITEM_CD) AS PRICETYPE_ID,"
					+ " :initialStatus AS STATUS_CD" 
					+ " FROM CI_PRICEITEM A" 
					+ " ORDER BY A.PRICEITEM_CD","");			
			preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
			totalPriceTypeRecords = preparedStatement.executeUpdate();
			logger.info("No of rows selected for processing are - "+ totalPriceTypeRecords);
		} catch (ThreadAbortedException e) {
			logger.error("Inside getPriceTypeData() method, Error -", e);
			throw new RunAbortedException(CustomMessageRepository
					.exceptionInExecution(e.getMessage()));
		} catch (Exception e) {
			logger.error("Inside getPriceTypeData() method, Error -", e);
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}	
		
		//*******************
		
				nonPriceData();
				
				//*************************
				
		if (totalPriceTypeRecords < totalPriceTypeThreads) {
			rowsInEachPriceTypeThread = totalPriceTypeRecords;
		} else {
			rowsInEachPriceTypeThread = totalPriceTypeRecords / totalPriceTypeThreads;
		}

		remainingPriceTypeRows = totalPriceTypeRecords
		- (rowsInEachPriceTypeThread * totalPriceTypeThreads);

		logger.info("Count = " + totalPriceTypeRecords);
		logger.info("Threads = " + totalPriceTypeThreads);
		logger.info("rowsInEachThread = " + rowsInEachPriceTypeThread);
		logger.info("remainingRows =  " + remainingPriceTypeRows);

		for (int i = 1; i <= totalPriceTypeRecords; i = i + rowsInEachPriceTypeThread, remainingPriceTypeRows--) {
			int fromPriceTypeRowNum = i;
			int toPriceTypeRowNum = i + rowsInEachPriceTypeThread - 1;
			if (remainingPriceTypeRows > 0) {
				toPriceTypeRowNum++;
				i++;
			}
			try {
				preparedStatement = createPreparedStatement("SELECT PRICETYPE_ID FROM (SELECT ROWNUM R, A.PRICETYPE_ID FROM"
						+ " (SELECT PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_1 ORDER BY PRICETYPE_ID ASC) A)"
						+ " WHERE R = :firstRowId OR R = :lastRowId ORDER BY R","");
				preparedStatement.bindString("firstRowId", String.valueOf(fromPriceTypeRowNum), "R");
				preparedStatement.bindString("lastRowId", String.valueOf(toPriceTypeRowNum), "R");
				preparedStatement.setAutoclose(false);
				if (preparedStatement.list().size() == 2) {
					SQLResultRow resultSet1 = preparedStatement.list().get(0);
					lowPriceTypeId = resultSet1.getString("PRICETYPE_ID");
					SQLResultRow resultSet2 = preparedStatement.list().get(1);
					highPriceTypeId = resultSet2.getString("PRICETYPE_ID");
				}
				if (preparedStatement.list().size() == 1) {
					SQLResultRow resultSet1 = preparedStatement.list().get(0);
					lowPriceTypeId = resultSet1.getString("PRICETYPE_ID");
					highPriceTypeId = resultSet1.getString("PRICETYPE_ID");
				}
				priceTypeData = new PriceTypeData_Id(lowPriceTypeId, highPriceTypeId);
				//*************************
				threadworkUnit = new ThreadWorkUnit();
				threadworkUnit.setPrimaryId(priceTypeData);
				threadWorkUnitList.add(threadworkUnit);
				threadworkUnit = null;
				priceTypeData = null;
				//*************************				
			} catch (ThreadAbortedException e) {
				logger.error("Inside getPriceTypeData() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside getPriceTypeData() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
		return threadWorkUnitList;
	}

	public Class<PriceTypeDataInterfaceWorker> getThreadWorkerClass() {
		return PriceTypeDataInterfaceWorker.class;
	}

	public static class PriceTypeDataInterfaceWorker extends
	PriceTypeDataInterfaceWorker_Gen {		
		
		/**
		 * initializeThreadWork() method contains logic that is invoked once by
		 * the framework per thread.
		 */
		public void initializeThreadWork(boolean arg0)
		throws ThreadAbortedException, RunAbortedException {
			logger.info("Inside initializeThreadWork() method for batch thread number: "+ getBatchThreadNumber());
		}
		
		/**
		 * ThreadExecutionStrategy() method contains commit strategy of the
		 * interface.
		 */
		public ThreadExecutionStrategy createExecutionStrategy() {
			logger.info("Inside createExecutionStrategy() method");
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit)
		throws ThreadAbortedException, RunAbortedException {			
			logger.info("Inside executeWorkUnit() for thread number - "+ getBatchThreadNumber());	
			PreparedStatement preparedStatement = null;
			StringBuilder stringBuilder = new StringBuilder();

			PriceTypeData_Id priceTypeData = (PriceTypeData_Id) unit.getPrimaryId();
			String lowPriceTypeId = priceTypeData.getLowPriceTypeId();
			String highPriceTypeId = priceTypeData.getHighPriceTypeId();

			logger.info("priceTypeId1 = " + lowPriceTypeId);
			logger.info("priceTypeId2 = " + highPriceTypeId);

			// -------------------------- CM_PRICE_TYPE AND CM_PRICE_TYPE_ERR
			// VALIDATIONS --------------------------------
			
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_11 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus ");
				stringBuilder.append("where (Y.DESCR is null OR Y.DESCR =' ') ");
				stringBuilder.append("and Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR = :msgNbr2301, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2301 ");
				stringBuilder.append("where Y.PRICETYPE_ID IN (SELECT X.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_11 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID AND X.STATUS_CD=:errorStatus) ");
				stringBuilder.append("AND Y.STATUS_CD=:initialStatus AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2301", String.valueOf(CustomMessages.PRICETYPE_DESCR).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2301", getPriceTypeErrorDescription(String.valueOf(CustomMessages.PRICETYPE_DESCR)).trim(), "ERROR_INFO");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
				
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_2 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus ");
				stringBuilder.append("where (Y.CLASS_ID is null OR Y.CLASS_ID=' ') ");
				stringBuilder.append("AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR = :msgNbr2302, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2302 ");
				stringBuilder.append("where Y.PRICETYPE_ID IN (SELECT X.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID AND X.STATUS_CD=:errorStatus ");
				stringBuilder.append("AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2302", String.valueOf(CustomMessages.CLASS_PTM).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2302", getPriceTypeErrorDescription(String.valueOf(CustomMessages.CLASS_PTM)).trim(), "ERROR_INFO");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
				
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR = :msgNbr2303, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2303 ");
				stringBuilder.append("where Y.PRICETYPE_ID IN (SELECT X.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID AND X.CLASS_GRP_ID ");
				stringBuilder.append("NOT IN (SELECT Z.CLASS_GRP_ID FROM CM_PRICE_TYPE_TMP_3 Z WHERE Z.CLASS_GRP_ID=X.CLASS_GRP_ID ");
				stringBuilder.append(") AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				stringBuilder.append("AND Y.STATUS_CD=:initialStatus ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2303", String.valueOf(CustomMessages.CLASS_GRP_CG).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2303", getPriceTypeErrorDescription(String.valueOf(CustomMessages.CLASS_GRP_CG)).trim(), "ERROR_INFO");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_3 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus ");
				stringBuilder.append("where (Y.DESCR is null OR Y.DESCR =' ') ");
				stringBuilder.append("and Y.CLASS_GRP_ID IN (SELECT X.CLASS_GRP_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.CLASS_GRP_ID=X.CLASS_GRP_ID AND X.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR = :msgNbr2304, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2304 ");
				stringBuilder.append("where Y.PRICETYPE_ID IN (SELECT X.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID AND X.CLASS_GRP_ID ");
				stringBuilder.append("IN (SELECT Z.CLASS_GRP_ID FROM CM_PRICE_TYPE_TMP_3 Z WHERE Z.CLASS_GRP_ID=X.CLASS_GRP_ID ");
				stringBuilder.append("AND Z.STATUS_CD=:errorStatus) ");
				stringBuilder.append("AND Y.STATUS_CD=:initialStatus AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2304", String.valueOf(CustomMessages.CLASS_GRP_CG_DESCR).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2304", getPriceTypeErrorDescription(String.valueOf(CustomMessages.CLASS_GRP_CG_DESCR)).trim(), "ERROR_INFO");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("DELETE FROM CM_CLASS_GRP Y ");
				stringBuilder.append("WHERE Y.CLASS_GRP_ID IN (SELECT X.CLASS_GRP_ID FROM CM_PRICE_TYPE_TMP_3 X ");
				stringBuilder.append("WHERE Y.CLASS_GRP_ID=X.CLASS_GRP_ID AND X.STATUS_CD =:errorStatus) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
							
			try {
				stringBuilder.append("UPDATE CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("SET Y.STATUS_CD       =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR       = :msgNbr2305, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2305 ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID IN ");
				stringBuilder.append("(SELECT X.PRICETYPE_ID ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID ");
				stringBuilder.append("AND X.CLASS_ID NOT IN ");
				stringBuilder.append("(SELECT Z.CLASS_ID ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_4 Z ");
				stringBuilder.append("WHERE Z.CLASS_ID=X.CLASS_ID) ");
				stringBuilder.append("AND Y.STATUS_CD=:initialStatus and X.STATUS_CD!='A' ");
				stringBuilder.append("AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId AND :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2305", String.valueOf(CustomMessages.CLASS_PTC_DESCR).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2305", getPriceTypeErrorDescription(String.valueOf(CustomMessages.CLASS_PTC_DESCR)).trim(), "ERROR_INFO");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
						
			try {
				stringBuilder.append("UPDATE CM_PRICE_TYPE_TMP_4 A ");
				stringBuilder.append("SET A.STATUS_CD=:errorStatus ");
				stringBuilder.append("WHERE A.CLASS_ID=(SELECT B.CLASS_ID FROM CM_PRICE_TYPE_TMP_4 B ");
				stringBuilder.append("WHERE A.CLASS_ID=B.CLASS_ID ");						
				stringBuilder.append("and A.CLASS_ID IN (SELECT X.CLASS_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE A.CLASS_ID=X.CLASS_ID AND X.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				stringBuilder.append("group by B.CLASS_ID having count(*)>1) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("UPDATE CM_PRICE_TYPE_TMP_4 Y ");
				stringBuilder.append("SET Y.STATUS_CD       =:errorStatus ");
				stringBuilder.append("WHERE (Y.DESCR is null OR Y.DESCR=' ') ");
				stringBuilder.append("and Y.CLASS_ID IN (SELECT X.CLASS_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.CLASS_ID=X.CLASS_ID AND X.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("update CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("set Y.STATUS_CD =:errorStatus, ");
				stringBuilder.append("Y.MESSAGE_NBR = :msgNbr2305, ");
				stringBuilder.append("Y.ERROR_INFO  = :msg2305 ");
				stringBuilder.append("where Y.PRICETYPE_ID IN (SELECT X.PRICETYPE_ID FROM CM_PRICE_TYPE_TMP_2 X ");
				stringBuilder.append("WHERE Y.PRICETYPE_ID=X.PRICETYPE_ID AND X.CLASS_ID ");
				stringBuilder.append("IN (SELECT Z.CLASS_ID FROM CM_PRICE_TYPE_TMP_4 Z WHERE Z.CLASS_ID=X.CLASS_ID ");
				stringBuilder.append("AND Z.STATUS_CD=:errorStatus) ");
				stringBuilder.append("AND Y.STATUS_CD=:initialStatus AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId) ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				preparedStatement.bindString("msgNbr2305", String.valueOf(CustomMessages.CLASS_PTC_DESCR).trim(), "MESSAGE_NBR");
				preparedStatement.bindString("msg2305", getPriceTypeErrorDescription(String.valueOf(CustomMessages.CLASS_PTC_DESCR)).trim(), "ERROR_INFO");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
										
			try {
				stringBuilder.append("UPDATE CM_PRICE_TYPE_TMP_1 Y ");
				stringBuilder.append("SET Y.STATUS_CD =:successStatus ");
				stringBuilder.append("where Y.STATUS_CD=:initialStatus AND Y.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("initialStatus",priceTypeDataInterfaceLookUp.getInitStatus(), "STATUS_COD");
				preparedStatement.bindString("successStatus", priceTypeDataInterfaceLookUp.getSuccessStatus(), "STATUS_COD");
				preparedStatement.executeUpdate();
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append("MERGE INTO CM_PRICE_TYPE T ");
				stringBuilder.append("USING (SELECT DISTINCT A.PRICETYPE_ID, A.DESCR ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_11 A, CM_PRICE_TYPE_TMP_1 B ");
				stringBuilder.append("WHERE A.PRICETYPE_ID=B.PRICETYPE_ID ");
				stringBuilder.append("AND B.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				stringBuilder.append("AND B.STATUS_CD=:successStatus) S ON (T.PRICETYPE_ID=S.PRICETYPE_ID) ");
				stringBuilder.append("WHEN NOT MATCHED THEN INSERT (PRICETYPE_ID, ");
				stringBuilder.append("DESCR, VALID_FROM, VALID_TO) values (S.PRICETYPE_ID, S.DESCR, SYSTIMESTAMP, NULL) ");
				stringBuilder.append("WHEN MATCHED THEN UPDATE SET T.DESCR=S.DESCR,VALID_FROM=SYSTIMESTAMP where T.DESCR<>S.DESCR ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("successStatus", priceTypeDataInterfaceLookUp.getSuccessStatus(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted and merged into table CM_PRICE_TYPE -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append("MERGE INTO CM_PRICE_TYP_CLASS T ");
				stringBuilder.append("USING (SELECT DISTINCT A.PRICETYPE_ID,A.CLASS_GRP_ID,A.CLASS_ID,C.DESCR ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_2 A, CM_PRICE_TYPE_TMP_1 B, CM_PRICE_TYPE_TMP_4 C ");
				stringBuilder.append("WHERE A.PRICETYPE_ID=B.PRICETYPE_ID ");
				stringBuilder.append("AND B.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				stringBuilder.append("AND B.STATUS_CD=:successStatus ");
				stringBuilder.append("AND A.CLASS_ID    =C.CLASS_ID) S ");
				stringBuilder.append("ON (T.PRICETYPE_ID=S.PRICETYPE_ID ");
				stringBuilder.append("AND T.CLASS_GRP_ID=S.CLASS_GRP_ID ");
				stringBuilder.append("AND T.CLASS_ID=S.CLASS_ID) ");
				stringBuilder.append("WHEN NOT MATCHED THEN INSERT (PRICETYPE_ID, ");
				stringBuilder.append("CLASS_GRP_ID, CLASS_ID, DESCR, VALID_FROM, VALID_TO) ");
				stringBuilder.append("values (S.PRICETYPE_ID, S.CLASS_GRP_ID, S.CLASS_ID, S.DESCR, SYSTIMESTAMP, NULL) ");
				stringBuilder.append("WHEN MATCHED THEN UPDATE SET T.DESCR=S.DESCR,VALID_FROM=SYSTIMESTAMP,VALID_TO=NULL ");
				stringBuilder.append("where T.CLASS_ID<>S.CLASS_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("successStatus", priceTypeDataInterfaceLookUp.getSuccessStatus(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted and merged into table CM_PRICE_TYP_CLASS -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append("MERGE INTO CM_PRICE_TYP_CLASS T ");
				stringBuilder.append("USING (SELECT DISTINCT A.PRICETYPE_ID,A.CLASS_GRP_ID,A.CLASS_ID,C.DESCR ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_2 A, CM_PRICE_TYPE_TMP_1 B, CM_PRICE_TYPE_TMP_4 C ");
				stringBuilder.append("WHERE A.PRICETYPE_ID=B.PRICETYPE_ID ");
				stringBuilder.append("AND B.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				stringBuilder.append("AND B.STATUS_CD=:successStatus ");
				stringBuilder.append("AND A.CLASS_ID    =C.CLASS_ID) S ");
				stringBuilder.append("ON (T.PRICETYPE_ID=S.PRICETYPE_ID ");
				stringBuilder.append("AND T.CLASS_GRP_ID=S.CLASS_GRP_ID) ");
				stringBuilder.append("WHEN NOT MATCHED THEN INSERT (PRICETYPE_ID, ");
				stringBuilder.append("CLASS_GRP_ID, CLASS_ID, DESCR, VALID_FROM, VALID_TO) ");
				stringBuilder.append("values (S.PRICETYPE_ID, S.CLASS_GRP_ID, S.CLASS_ID, S.DESCR, SYSTIMESTAMP, NULL) ");
				stringBuilder.append("WHEN MATCHED THEN UPDATE SET T.DESCR=S.DESCR,VALID_FROM=SYSTIMESTAMP,VALID_TO=NULL ");
				stringBuilder.append("where T.CLASS_ID=S.CLASS_ID AND T.DESCR<>S.DESCR");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("successStatus", priceTypeDataInterfaceLookUp.getSuccessStatus(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted and merged into table CM_PRICE_TYP_CLASS -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			try {
				stringBuilder.append("MERGE INTO CM_PRICE_TYP_CLASS T ");
				stringBuilder.append("USING (SELECT DISTINCT A.PRICETYPE_ID,A.CLASS_GRP_ID,A.CLASS_ID ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_2 A, CM_PRICE_TYPE_TMP_1 B ");
				stringBuilder.append("WHERE A.PRICETYPE_ID=B.PRICETYPE_ID ");
				stringBuilder.append("AND B.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				stringBuilder.append("AND B.STATUS_CD=:successStatus) S ");
				stringBuilder.append("ON (T.PRICETYPE_ID=S.PRICETYPE_ID ");
				stringBuilder.append("AND T.CLASS_GRP_ID=S.CLASS_GRP_ID ");
				stringBuilder.append("AND T.CLASS_ID=S.CLASS_ID) ");
				stringBuilder.append("WHEN NOT MATCHED THEN INSERT (PRICETYPE_ID, ");
				stringBuilder.append("CLASS_GRP_ID, CLASS_ID, DESCR, VALID_FROM, VALID_TO) ");
				stringBuilder.append("values (S.PRICETYPE_ID, S.CLASS_GRP_ID, S.CLASS_ID, ' ', SYSTIMESTAMP, NULL) ");
				stringBuilder.append("WHEN MATCHED THEN UPDATE SET VALID_FROM=SYSTIMESTAMP ");
				stringBuilder.append("where T.CLASS_ID<>S.CLASS_ID ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("successStatus", priceTypeDataInterfaceLookUp.getSuccessStatus(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted and merged into table CM_PRICE_TYP_CLASS -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append(" UPDATE CM_PRICE_TYP_CLASS CLS SET CLS.VALID_TO = SYSDATE-1 ");
				stringBuilder.append(" WHERE TRIM(CLS.CLASS_ID) NOT IN  ");
				stringBuilder.append(" (SELECT TRIM(CHAR_VAL) FROM CI_PRICEITEM_CHAR WHERE TRIM(CLS.CLASS_GRP_ID) = TRIM(CHAR_TYPE_CD) ");
				stringBuilder.append(" AND TRIM(CLS.PRICETYPE_ID) = TRIM(PRICEITEM_CD)) AND CLS.VALID_TO IS NULL");
					preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows updated into table CM_PRICE_TYP_CLASS -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			stringBuilder = null;
			stringBuilder = new StringBuilder();
			
			try {
				stringBuilder.append("INSERT INTO CM_PRICE_TYPE_ERR (PRICETYPE_ID, ");
				stringBuilder.append("MESSAGE_CAT_NBR, MESSAGE_NBR, ERROR_INFO) ");
				stringBuilder.append("SELECT A.PRICETYPE_ID, 90000, A.MESSAGE_NBR, A.ERROR_INFO ");
				stringBuilder.append("FROM CM_PRICE_TYPE_TMP_1 A ");
				stringBuilder.append("WHERE A.STATUS_CD=:errorStatus ");
				stringBuilder.append("AND A.PRICETYPE_ID BETWEEN :lowPriceTypeId and :highPriceTypeId ");
				preparedStatement = createPreparedStatement(stringBuilder.toString(),"");
				preparedStatement.bindString("lowPriceTypeId", lowPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("highPriceTypeId", highPriceTypeId, "PRICETYPE_ID");
				preparedStatement.bindString("errorStatus", priceTypeDataInterfaceLookUp.getErrorStatus(), "STATUS_COD");
				int count = preparedStatement.executeUpdate();
				logger.info("Rows inserted into table CM_PRICE_TYPE_ERR -" + count);
			} catch (ThreadAbortedException e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
				throw new RunAbortedException(CustomMessageRepository
						.exceptionInExecution(e.getMessage()));
			} catch (Exception e) {
				logger.error("Inside executeWorkUnit() method, Error -", e);
			}
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
			
			return true;
		}

		public static String getPriceTypeErrorDescription(String messageNumber) {
			String errorInfo = " ";
			errorInfo = CustomMessageRepository.getPriceTypeErrorMessage(
					messageNumber).getMessageText();
			if (errorInfo.contains("Text:")
					&& errorInfo.contains("Description:")) {
				errorInfo = errorInfo.substring(errorInfo.indexOf("Text:"),
						errorInfo.indexOf("Description:"));
			}
			return errorInfo;
		}
		
		/**
		 * finalizeThreadWork() execute by the batch program once per thread
		 * after processing all units.
		 */
		public void finalizeThreadWork() throws ThreadAbortedException,
		RunAbortedException {
			logger.info("Inside finalizeThreadWork() method");
		}
	}
	public static final class PriceTypeData_Id implements Id {
		private static final long serialVersionUID = 1L;
		private String lowPriceTypeId;
		private String highPriceTypeId;

		public PriceTypeData_Id(String lowPriceTypeId, String highPriceTypeId) {
			setLowPriceTypeId(lowPriceTypeId);
			setHighPriceTypeId(highPriceTypeId);
		}

		public boolean isNull() {
			return false;
		}

		public void appendContents(StringBuilder arg0) {
		}

		public static long getSerialVersionUID() {
			return serialVersionUID;
		}

		public String getHighPriceTypeId() {
			return highPriceTypeId;
		}

		public void setHighPriceTypeId(String highPriceTypeId) {
			this.highPriceTypeId = highPriceTypeId;
		}

		public String getLowPriceTypeId() {
			return lowPriceTypeId;
		}

		public void setLowPriceTypeId(String lowPayBillId) {
			this.lowPriceTypeId = lowPayBillId;
		}
	}
}