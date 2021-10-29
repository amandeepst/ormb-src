/*******************************************************************************
* FileName                   : CustomCreditNoteInterfaceLookUp.java
* Project Name               : WorldPay RMB Implementation
* Date of Creation           : Jul 7, 2015
* Version Number             : 0.1
* Revision History     :
VerNum | ChangeReqNum | Date Modification | Author Name | Nature of Change
0.1		 NA				Jul 7, 2015		Preeti		 Implemented all the requirements for CD2.	
0.2	   	 NA			 	Jun 11, 2018	RIA		   	 Prepared Statement close
*******************************************************************************/

package com.splwg.cm.domain.wp.batch;

import com.splwg.base.api.GenericBusinessObject;
import com.splwg.base.api.sql.PreparedStatement;
import com.splwg.base.api.sql.SQLResultRow;
import com.splwg.cm.domain.wp.utils.CommonUtils;
import com.splwg.shared.logging.Logger;
import com.splwg.shared.logging.LoggerFactory;

/**
 * @author Preeti
 *
 *This LookUp code will be used by Custom Credit Note Interface to avoid hard codings.
 */
public class CustomCreditNoteInterfaceLookUp extends GenericBusinessObject {
	
	public static final Logger logger = LoggerFactory.getLogger(CustomCreditNoteInterfaceLookUp.class);

	
	public CustomCreditNoteInterfaceLookUp() {
		setLookUpConstants();
	}
	
	private String upload = "";

	private String error = "";

	private String pending = "";

	private String completed = "";
	
	private String type = "";
		
	public void setLookUpConstants() {
		PreparedStatement preparedStatement = null;
		try {
			preparedStatement = createPreparedStatement("SELECT FIELD_VALUE, VALUE_NAME "
					+ "FROM CI_LOOKUP_VAL WHERE FIELD_NAME = 'INT0CR_OPT_TYPE_FLG'","");
			String fieldValue = "";
			String valueName = "";
			preparedStatement.setAutoclose(false);
			for (SQLResultRow resultSet : preparedStatement.list()) {
				fieldValue = CommonUtils.CheckNull(resultSet
						.getString("FIELD_VALUE"));
				valueName = CommonUtils.CheckNull(resultSet
						.getString("VALUE_NAME"));

				if (fieldValue.equals("PEND")) {
					setPending(valueName);
				}
				if (fieldValue.equals("UPLD")) {
					setUpload(valueName);
				}
				if (fieldValue.equals("EROR")) {
					setError(valueName);
				}
				if (fieldValue.equals("COMP")) {
					setCompleted(valueName);
				}	
				if (fieldValue.equals("TYPE")) {
					setType(valueName);
				}	
			}
		} catch (Exception e) {
			logger.info(e.getMessage());
		} finally {
			if (preparedStatement != null) {
				preparedStatement.close();
				preparedStatement = null;
			}
		}
	}

	public String getUpload() {
		return upload;
	}

	public void setUpload(String upload) {
		this.upload = upload;
	}
	
	

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public String getError() {
		return error;
	}

	public void setError(String error) {
		this.error = error;
	}

	public String getPending() {
		return pending;
	}

	public void setPending(String pending) {
		this.pending = pending;
	}

	public String getCompleted() {
		return completed;
	}

	public void setCompleted(String completed) {
		this.completed = completed;
	}

}
