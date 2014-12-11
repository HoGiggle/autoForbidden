package com.jjhu.hadoop;

import junit.framework.TestCase;
import org.omg.CORBA.INTERNAL;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Created by jjhu on 2014/12/5.
 */
public class JunitTest extends TestCase{
    public void testDate() throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone destTimeZone = TimeZone.getTimeZone("GMT+8");
        String date = sf.format(new Date());
        String firstDate = sf.format(sf.parse(date).getTime() - 30*60*1000);
        //System.out.println(date);
        sf.setTimeZone(destTimeZone);
        System.out.println(date);
        System.out.println(firstDate);
    }
}
