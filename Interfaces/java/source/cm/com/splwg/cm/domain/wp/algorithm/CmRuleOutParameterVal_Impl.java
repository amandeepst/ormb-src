package com.splwg.cm.domain.wp.algorithm;

import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.ccb.domain.admin.rulesengine.RuleParameters;
import com.splwg.ccb.domain.admin.rulesengine.rule.Rule;
import com.splwg.ccb.domain.admin.rulesengine.rule.RuleCriteriaFieldAlgorithmSpot;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author tutejaa105
 *
@AlgorithmComponent ()
 */
public class CmRuleOutParameterVal_Impl extends CmRuleOutParameterVal_Gen
		implements RuleCriteriaFieldAlgorithmSpot {
	
	private RuleParameters ruleParam;
	private String criteriaFieldName;
	private String criteriaFieldValue;
	private static final Logger logger= LoggerFactory.getLogger(CmRuleOutParameterVal_Impl.class);	
	private static ConcurrentHashMap<String, List<String>> map=new ConcurrentHashMap<String, List<String>>();
	
	private String udfChar1=null;
	private String udfChar3=null;
	private String udfChar6=null;
	private String udfChar7=null;
	private String udfChar8=null;
	private String outputParm=null;
	private String udfChar21=null;
	private String udfChar22=null;
	private String udfChar23=null;
	private String udfChar24=null;
	private String udfChar26=null;
	private String udfChar27=null;

	private static final String PARM_VAL_CONST = "PARM_VAL";
	private static final String PARM_VAL_STR_CONST = "PARM_VAL_STR";
	private static final String UDF_CHAR_1_CONST = "UDF_CHAR_1";
	private static final String UDF_CHAR_3_CONST = "UDF_CHAR_3";
	private static final String UDF_CHAR_6_CONST = "UDF_CHAR_6";
	private static final String UDF_CHAR_8_CONST = "UDF_CHAR_8";

	@Override
	public void invoke() {		
		
		logger.debug("**************Map*********"+map);

			if(map.isEmpty()){
				getParametersValue();
			}

		logger.debug("******Values *********"+ruleParam.get("TXN_DETAIL_ID")+"*****************"+ruleParam.get(UDF_CHAR_1_CONST));
				
		if(notNull(ruleParam.get(UDF_CHAR_1_CONST))){
			udfChar1=(ruleParam.get(UDF_CHAR_1_CONST)).toString().trim();

		}
		if(notNull(ruleParam.get(UDF_CHAR_3_CONST))){
			udfChar3=(ruleParam.get(UDF_CHAR_3_CONST)).toString().trim();
		}
		
		if(notNull(ruleParam.get(UDF_CHAR_6_CONST))){
			udfChar6=(ruleParam.get(UDF_CHAR_6_CONST)).toString().trim();
		}
		if(notNull(ruleParam.get("UDF_CHAR_7"))){
			udfChar7=(ruleParam.get("UDF_CHAR_7")).toString().trim();

		}
		if(notNull(ruleParam.get(UDF_CHAR_8_CONST))){
			udfChar8=(ruleParam.get(UDF_CHAR_8_CONST)).toString().trim();
		}
					
		List<String> mapList=null;

		for (Entry<String, List<String>> entry : map.entrySet())
		{
			mapList = entry.getValue();
			if(mapList != null){
				updateTxnChar(mapList, entry.getKey());
			}
		}
		setCriteriaFieldName("CM_UPDATE_FLG");
		setCriteriaFieldValue("Y");		
	}
	
	
	private void updateTxnChar(List<String> attributesList, String key) {
		
		logger.debug("*******************Inserting into transactions Char********************");		
		
		for(int i=0; i < attributesList.size(); i++)
		{
			String codeStr=attributesList.get(i);
			String codeAr[]= codeStr.split("~");
			logger.debug("******************code STr************"+codeAr[1].toString());
		
			if(notNull(udfChar1) && udfChar1.equals(codeAr[0].toString())){
				setUdfChar21Or27(codeAr);
			}
			else if(notNull(udfChar3) && udfChar3.equals(codeAr[0].toString())){
				
				udfChar22=codeAr[1].toString();
				logger.debug("*****************UDF_CHAR_22**************"+udfChar22);				
				ruleParam.put("UDF_CHAR_22", udfChar22);

			}
			else if(notNull(udfChar6) && udfChar6.equals(codeAr[0].toString())){
				udfChar23=codeAr[1].toString();
				udfChar6=udfChar23;
				logger.debug("*****************UDF_CHAR_6**************"+udfChar23);				
			}
			
			else if(notNull(udfChar7) && udfChar7.equals(codeAr[0].toString())){
				udfChar24=codeAr[1].toString();
				logger.debug("*****************UDF_CHAR_24**************"+udfChar24);				
				ruleParam.put("UDF_CHAR_24", udfChar24);

			}
			else if(notNull(udfChar8) && udfChar8.equals(codeAr[0].toString())){
				udfChar26=codeAr[1].toString();
				logger.debug("*****************UDF_CHAR_26**************"+udfChar26);				
				ruleParam.put("UDF_CHAR_26", udfChar26);
			}
			
			ruleParam.put("UDF_CHAR_23", udfChar6);	
		}		
	}

	private void setUdfChar21Or27(String[] codeAr) {
		if(notNull((udfChar21))){
			udfChar27=codeAr[1].toString();
			logger.debug("*****************UDF_CHAR_27**************"+udfChar27);
			ruleParam.put("UDF_CHAR_27", udfChar27);
		}else{
			udfChar21=codeAr[1].toString();
			logger.debug("*****************UDF_CHAR_21**************"+udfChar21);
			ruleParam.put("UDF_CHAR_21", udfChar21);
		}
	}

	private synchronized void getParametersValue() {

		 PreparedStatement ps=null;

		StringBuilder sb = new StringBuilder();
		sb.append("SELECT OUTPUT_PARM, PARM_VAL, PARM_VAL_STR, PRODUCT FROM CM_CHG_MAP ORDER BY PRODUCT DESC");

			ps=createPreparedStatement(sb.toString(),"");
			logger.debug("************Output Parameters******************");
			ps.setAutoclose(false);
			List<SQLResultRow> resultList=ps.list();
			if(notNull(resultList) && resultList.size()>0) {
				iterateAndAddParameters(resultList);
				}
			ps.close();
	}

	private void iterateAndAddParameters(List<SQLResultRow> resultList) {
		ArrayList<String> char1List=new ArrayList<String>();
		ArrayList<String> char3List=new ArrayList<String>();
		ArrayList<String> char6List=new ArrayList<String>();
		ArrayList<String> char8List=new ArrayList<String>();

		for(SQLResultRow result:resultList) {
			outputParm = result.getString("OUTPUT_PARM");
			if(outputParm.contains(UDF_CHAR_1_CONST)){
				char1List.add(result.getString(PARM_VAL_CONST).concat("~").concat(result.getString(PARM_VAL_STR_CONST)));
			}
			if(outputParm.contains(UDF_CHAR_3_CONST)){
				char3List.add(result.getString(PARM_VAL_CONST).concat("~").concat(result.getString(PARM_VAL_STR_CONST)));
			}
			if(outputParm.contains(UDF_CHAR_6_CONST)){
				char6List.add(result.getString(PARM_VAL_CONST).concat("~").concat(result.getString(PARM_VAL_STR_CONST)));
			}
			if(outputParm.contains(UDF_CHAR_8_CONST)){
				char8List.add(result.getString(PARM_VAL_CONST).concat("~").concat(result.getString(PARM_VAL_STR_CONST)));
			}
			map.put(UDF_CHAR_1_CONST, char1List);
			map.put(UDF_CHAR_3_CONST, char3List);
			map.put(UDF_CHAR_6_CONST, char6List);
			map.put(UDF_CHAR_8_CONST, char8List);
		}
	}

	public void setRuleParam(RuleParameters ruleParam) {
		this.ruleParam = ruleParam;
	}

	
	public void setCriteriaFieldName(String criteriaFieldName) {
		this.criteriaFieldName = criteriaFieldName;
	}

	
	public void setCriteriaFieldValue(String criteriaFieldValue) {
		this.criteriaFieldValue = criteriaFieldValue;
	}


	@Override
	public String getCriteriaFieldName() {
				
		return criteriaFieldName;
	}

	@Override
	public String getCriteriaFieldValue() {
				
		return criteriaFieldValue;
	}

	@Override
	public void setInputParameters(RuleParameters ruleParam) {
				
		this.ruleParam=ruleParam;
	}

	@Override
	public void setRule(Rule rule) {
		
		//this.rule=rule;

	}

}
