package com.niceshot.hudi.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author created by chenjun at 2020-10-28 19:17
 */
public class DateUtils {
    public static final String DATE_FORMAT_YYYY_MM_DD_SLASH = "yyyy/MM/dd";
    public static final String DATE_FORMAT_YYYY_MM_DD = "yyyy-MM-dd";
    public static final String DATE_FORMAT_YYYY_MM_DD_hh_mm_ss = "yyyy-MM-dd hh:mm:ss";

    /**
     * @param milliseconds
     * @param format
     * @return
     */
    public static String millisecondsFormat(Long milliseconds, String format) {
        Date d = new Date(milliseconds);
        DateFormat f = new SimpleDateFormat(format);
        return f.format(d);
    }

    public static String dateStringFormat(String dateString ,String fromDateFormat,String toDateFormat) {
        DateFormat fromFormat = new SimpleDateFormat(fromDateFormat);
        Date fromDate;
        try {
            fromDate = fromFormat.parse(dateString);
        } catch (ParseException e) {
            throw new RuntimeException("dateString is:"+dateString+",but specifc fromFormat is:"+fromDateFormat,e);
        }
        DateFormat toFormat = new SimpleDateFormat(toDateFormat);
        return toFormat.format(fromDate);
    }


    public static boolean isValidDateTime(String inDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD_hh_mm_ss);
        try {
            dateFormat.parse(inDate.trim());
        } catch (ParseException pe) {
            return false;
        }
        return true;
    }

    public static boolean isValidDate(String dateStr) {
        SimpleDateFormat dateFormat = new SimpleDateFormat(DATE_FORMAT_YYYY_MM_DD);
        try {
            dateFormat.parse(dateStr.trim());
        } catch (ParseException pe) {
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        System.out.println(isValidDate("2014-01-02"));
        System.out.println(isValidDateTime("2014-01-02 22:01:33:023"));
    }

}
