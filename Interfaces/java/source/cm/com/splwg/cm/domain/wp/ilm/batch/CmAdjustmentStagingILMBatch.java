package com.splwg.cm.domain.wp.ilm.batch;

import java.math.BigInteger;
import java.util.List;

import org.dom4j.Document;
import org.dom4j.Element;

import com.splwg.base.api.Query;
import com.splwg.base.api.QueryIterator;
import com.splwg.base.api.QueryResultRow;
import com.splwg.base.api.batch.JobWork;
import com.splwg.base.api.batch.RunAbortedException;
import com.splwg.base.api.batch.StandardCommitStrategy;
import com.splwg.base.api.batch.ThreadAbortedException;
import com.splwg.base.api.batch.ThreadExecutionStrategy;
import com.splwg.base.api.batch.ThreadWorkUnit;
import com.splwg.base.api.datatypes.Bool;
import com.splwg.base.api.lookup.MaintenanceObjectOptionTypeLookup;
import com.splwg.base.domain.StandardMessages;
import com.splwg.base.domain.common.algorithm.AlgorithmComponentCache;
import com.splwg.base.domain.common.algorithm.Algorithm_Id;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObjectAlgorithm_Id;
import com.splwg.base.domain.common.maintenanceObject.MaintenanceObject_Id;
import com.splwg.base.domain.common.masterConfiguration.MasterConfigurationHelper;
import com.splwg.base.domain.ilm.genericalgorithm.ILMEligibilitySpot;
import com.splwg.base.support.schema.MaintenanceObjectInfo;
import com.splwg.base.support.schema.MaintenanceObjectInfoCache;
import com.splwg.cm.domain.wp.ilm.CmAdjustmentStaging_Id;
import com.splwg.shared.common.Dom4JHelper;
import com.splwg.shared.common.LoggedException;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author RIA
 *
@BatchJob (modules = { "demo"},
 *      softParameters = { @BatchJobSoftParameter (name = maintenanceObject, required = true, type = string)})
 */
public class CmAdjustmentStagingILMBatch extends CmAdjustmentStagingILMBatch_Gen {

	public static final Logger logger = LoggerFactory.getLogger(CmAdjustmentStagingILMBatch_Gen.class);

	private MaintenanceObjectInfo moInfo;
	private MaintenanceObject_Id moId;

	public JobWork getJobWork() {
		logger.debug("CmAdjustmentStagingILMBatch :: getJobWork() method :: START");

		moId = new MaintenanceObject_Id(getParameters().getMaintenanceObject());
		moInfo = MaintenanceObjectInfoCache.getMaintenanceObjectInfo(moId);
		String retentionString = moInfo.getOption(MaintenanceObjectOptionTypeLookup.constants.RETENTION_PERIOD);
		BigInteger retentionDays;
		if (notNull(retentionString)) {
			retentionDays = BigInteger.valueOf(Long.parseLong(retentionString));
		} else {
			retentionDays = getMasterConfigRetentionPeriod();
		}
		if (isNull(retentionDays)) {
			logger.error("The retention period was not defined on the MO or master config");
			throw new RunAbortedException(StandardMessages.retentionPeriodNotDefined());
		}
		
		StringBuilder sb = new StringBuilder();
		sb.append(" FROM CmAdjustmentStaging adjStg ");
		sb.append(" WHERE adjStg.isEligibleForArchiving = :ilmArchiveSw ");
		sb.append(" AND adjStg.ilmDate < :ilmDate ");
		sb.append(" AND adjStg.status = 'COMPLETED' ");

		Query<QueryResultRow> query = createQuery(sb.toString(), "");
		query.bindBoolean("ilmArchiveSw", Bool.FALSE);
		query.bindDate("ilmDate", getSystemDateTime().getDate().addDays(-retentionDays.intValue()));

		query.addResult("cmAdjStgId", "adjStg.id");
		QueryIterator<QueryResultRow> queryIterator = query.iterate();

		logger.debug("CmAdjustmentStagingILMBatch :: getJobWork() method :: END");
		return createJobWorkForQueryIterator(queryIterator, this);
	}

	/**
	 * Retention period in days from Master Configuration
	 * @return retentionPeriod
	 */
	private BigInteger getMasterConfigRetentionPeriod() {
		BigInteger retentionPeriod = null;
		String resultXML;
		try {
			resultXML = MasterConfigurationHelper.Factory.newInstance().getEntityById("F1-ILMMSConfig").getMasterConfigData();
		}
		catch (NullPointerException e) {
			return null;
		}
		try
		{
			resultXML = "<tempRoot>" + resultXML + "</tempRoot>";

			Document doc = Dom4JHelper.parseText(resultXML);
			Element element = doc.getRootElement();
			List<Element> mappingList = Dom4JHelper.getSameNameSpacedElements(element, "generalMasterConfiguration");
			if (mappingList.size() > 0) {
				element = (Element)mappingList.get(0);
			}
			mappingList = Dom4JHelper.getSameNameSpacedElements(element, "defaultRetentionPeriod");
			if (mappingList.size() > 0)
			{
				element = (Element)mappingList.get(0);
				retentionPeriod = BigInteger.valueOf(Long.parseLong(element.getTextTrim()));
			}
		}
		catch (Exception e) {
			throw LoggedException.wrap(logger, "Error retrieving retention period from master config", e);
		}
		return retentionPeriod;
	}

	@Override
	public ThreadWorkUnit createWorkUnit(QueryResultRow queryResultRow) {
		ThreadWorkUnit threadWorkUnit = new ThreadWorkUnit();
		CmAdjustmentStaging_Id cmAdjStgId = (CmAdjustmentStaging_Id) queryResultRow.get("cmAdjStgId");
		threadWorkUnit.setPrimaryId(cmAdjStgId);
		return threadWorkUnit;
	}

	public Class<CmAdjustmentStagingILMBatchWorker> getThreadWorkerClass() {
		return CmAdjustmentStagingILMBatchWorker.class;
	}

	public static class CmAdjustmentStagingILMBatchWorker extends
	CmAdjustmentStagingILMBatchWorker_Gen {

		public ThreadExecutionStrategy createExecutionStrategy() {
			return new StandardCommitStrategy(this);
		}

		public boolean executeWorkUnit(ThreadWorkUnit unit) throws ThreadAbortedException, RunAbortedException {
			logger.debug("CmAdjustmentStagingILMBatch :: executeWorkUnit() method :: START");

			CmAdjustmentStaging_Id cmAcctStgId = (CmAdjustmentStaging_Id) unit.getPrimaryId();

			MaintenanceObject_Id moId = new MaintenanceObject_Id(getParameters().getMaintenanceObject());
			MaintenanceObjectInfo moInfo = MaintenanceObjectInfoCache.getMaintenanceObjectInfo(moId);
			List<MaintenanceObjectAlgorithm_Id> algList = moInfo.getArchivingEligibilityAlgorithmIds();
			for (MaintenanceObjectAlgorithm_Id algID : algList)
			{
				Algorithm_Id algId = algID.getAlgorithmId();
				ILMEligibilitySpot alg = (ILMEligibilitySpot)AlgorithmComponentCache.getAlgorithmComponent(algId, ILMEligibilitySpot.class);

				alg.setMaintenanceObject(moInfo.getMaintenanceObject());
				alg.setBusinessEntity(cmAcctStgId.getEntity());
				alg.setCutOffDate(getProcessDateTime().getDate());
				alg.invoke();
			}

			logger.debug("CmAdjustmentStagingILMBatch :: executeWorkUnit() method :: END");
			return true;
		}
	}
}
