package com.splwg.cm.domain.wp.utils;


import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.ibm.icu.math.BigDecimal;

/**
 *  CommonUtils will contain static methods used across various flows. 
 * 
 * @author kshitij
 *
 */
public class CommonUtils {

	//Default constructor
	public CommonUtils() {}

	/**
	 * CheckNull converts the input null to empty string to avoid Null Pointer Exception
	 * @param strValue
	 * @return
	 */
	public static String CheckNull(String strValue) {
		if (strValue != null && !strValue.trim().equalsIgnoreCase("null"))
			return strValue;
		else
			return "";
	}
	
	public static String convertNullToEmptyString(String strValue) {
		if (strValue != null && !strValue.trim().equalsIgnoreCase("null") && !"".equals(strValue.trim()))
			return strValue.trim();
		else
			return " ";
	}
	
	/**
	 * CheckNull converts the input null to "0" string to avoid Null Pointer Exception, while working those string in Big Decimal conversion.
	 * @param strValue
	 * @return
	 */
	public static String CheckNullNumber(String strValue) {
		if (strValue != null && !strValue.trim().equalsIgnoreCase("null") && !"".equals(strValue.trim()))
			return strValue;
		else
			return "0";
	}
	
	
	public static BigDecimal CheckNullNumber(BigDecimal bigDecimalValue) {
		if (bigDecimalValue != null)
			return bigDecimalValue;
		else
			return new BigDecimal(0);
	}

	/**
	 * formatStringDate format the String date from one format to another 
	 * @param aStringDate
	 * @return
	 */
	public static String formatStringDate(String aStringDate,
			String fromDateFormat, String toDateFormat) throws ParseException {
		String formattedDate = null;
		SimpleDateFormat dateFormat = new SimpleDateFormat(fromDateFormat);
		java.util.Date tempDate = new java.util.Date();
			tempDate = dateFormat.parse(aStringDate);
			dateFormat = new SimpleDateFormat(toDateFormat);
			formattedDate = dateFormat.format(tempDate).toUpperCase();
		return formattedDate;
	}
	
/**
	 * rpad adds input characters to the right of a string.
	 * @param str
	 * @param size
	 * @param padChar
	 * @return
	 */
	public static String rpad(String str, int size, String padChar) {
		StringBuffer rpadded = null;
		rpadded = new StringBuffer(str);
		while (rpadded.length() < size) {
			rpadded.append(padChar);
		}
		return rpadded.toString();
	}

	/**
	 * lpad adds input characters to the left of a string.
	 * @param str
	 * @param size
	 * @param padChar
	 * @return
	 */
	public static String lpad(String str, int size, String padChar) {
		StringBuffer lpadded = null;
		lpadded = new StringBuffer();
		while (lpadded.length() < (size - str.length())) {
			lpadded.append(padChar);
		}
		return lpadded + str;
	}
	
	public static String getSignedLPad(String str, int placesBeforeDecimal, boolean hasDecimal, int placesAfterDecimal){
		if(str == null || str.trim().length() ==0 || str.trim().equalsIgnoreCase("null")){
			str = "0";
		}
		str = str.trim();
		String sign = " ";
		if(str.indexOf("-") > -1){
			sign = "-";
			str = str.substring(1);
		}
		String []parts = str.split("\\.");
		String intVal = parts[0];
		intVal = CommonUtils.lpad(intVal, placesBeforeDecimal, "0");
		String deciVal = "";
		if(hasDecimal){
			if(parts.length > 1){
				deciVal = parts[1];
			}
			deciVal = CommonUtils.rpad(deciVal, placesAfterDecimal, "0");	
		}
		return (sign + intVal + "." + deciVal); 
	}
	
	public static boolean containsNoInteger(String anyValue) {
		for (int j = 0; j < anyValue.length(); j++) {
			if (Character.isDigit(anyValue
					.charAt(j))) {
				return false;
			}
		}
		return true;
	}
	//TODO : This is for testing 
	public static double elapsedTimeInQuery(Date start, Date end){
		double time = 0;
		time = end.getTime() - start.getTime();		
		return time/1000;		
	}
}