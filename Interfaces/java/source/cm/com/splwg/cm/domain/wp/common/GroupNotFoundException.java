package com.splwg.cm.domain.wp.common;
/**
 * 
 * This Class is being created to throw an Exception to the parent class regarding 
 * User Group Id is not present in the database.
 * 
 * @author IBM_ADMIN
 * 
 *
 */
public class GroupNotFoundException extends Exception{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public GroupNotFoundException() {
		super();
	    }
	
	public GroupNotFoundException(String message) {
		super(message);
	    }
	
	public GroupNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }
	
	public GroupNotFoundException(Throwable cause) {
        super(cause);
    }

}
