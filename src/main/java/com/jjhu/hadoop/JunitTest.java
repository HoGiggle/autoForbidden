package com.jjhu.hadoop;

import com.jjhu.hadoop.command.AllRulerCommand;
import com.jjhu.hadoop.command.Command;
import com.jjhu.hadoop.dataStructure.DataObject;
import com.jjhu.hadoop.dataStructure.MomentCoin;
import com.jjhu.hadoop.dataStructure.ReturnMsg;
import com.jjhu.hadoop.mapreduce.SimHash;
import com.jjhu.hadoop.ruler.*;
import com.jjhu.hadoop.service.AutoForbidden;
import com.sun.tools.doclets.formats.html.SourceToHTMLConverter;
import junit.framework.TestCase;
import org.apache.commons.lang.StringUtils;

import javax.sound.midi.Soundbank;
import java.math.BigInteger;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * Created by jjhu on 2014/12/5.
 */
public class JunitTest extends TestCase{
    public static String getStringUtils(String s){
        List<Character> list = new ArrayList<>();
        for (char chr : s.toCharArray())
            list.add(chr);
        return StringUtils.join(list," ");
    }
    public void testDate() throws ParseException {
        SimpleDateFormat sf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        TimeZone destTimeZone = TimeZone.getTimeZone("GMT+8");
        Date nowDate = new Date();
        String date = sf.format(new Date());
        sf.setTimeZone(destTimeZone);
        String firstDate = sf.format(nowDate);

        System.out.println(date);
        System.out.println(firstDate);
    }

    public void testChar(){
        String msg = "2014-12-21 00:00:00@1@6@50@0@9@2@1@9@2014-12-18 19:24:30";
        String []items = msg.split("\\@");

       /* String msg = "2014-12-21 00:00:00|1";
        String []items = msg.split("\\|");*/
        for (String s : items)
            System.out.println(s);
    }

    public void testMath(){
        String s = "hello1heihei";
        s = getStringUtils(s);
        SimHash hash1 = new SimHash(s, 64);
        System.out.println(hash1.getIntSimHash() + "  " + hash1.getIntSimHash().bitCount());
        System.out.println(hash1.getStrSimHash());

        s = "loheihlhei1";
        s = getStringUtils(s);
        SimHash hash2 = new SimHash(s, 64);
        System.out.println(hash2.getIntSimHash() + "  " + hash2.getIntSimHash().bitLength());
        System.out.println(hash2.getStrSimHash());

        s = "hello34heihei";
        s = getStringUtils(s);
        SimHash hash3 = new SimHash(s, 64);
        System.out.println(hash3.getIntSimHash() + "  " + hash3.getIntSimHash().bitCount());
        System.out.println(hash3.getStrSimHash());

        s = "hello89heihei";
        s = getStringUtils(s);
        SimHash hash4 = new SimHash(s, 64);
        System.out.println(hash4.getIntSimHash() + "  " + hash4.getIntSimHash().bitCount());
        System.out.println(hash4.getStrSimHash());

        System.out.println("============================");
        int dis = hash1.getDistance(hash1.getStrSimHash(),hash2.getStrSimHash());
        System.out.println(hash1.hammingDistance(hash2) + " "+ dis);
        int dis2 = hash1.getDistance(hash1.getStrSimHash(),hash3.getStrSimHash());
        System.out.println(hash1.hammingDistance(hash3) + " " + dis2);
        int dis3 = hash1.getDistance(hash1.getStrSimHash(),hash4.getStrSimHash());
        System.out.println(hash1.hammingDistance(hash4) + " " + dis3);
    }

    public void testInt(){
        String s = "helloworld1";
        String s2 = "helloworld123";
        String s3 = "ollehdlrow21";
        float f;
        f = 1 - StringUtils.getLevenshteinDistance(s,s2)*1f / Math.max(s.length(),s2.length());
        System.out.println(f);

        f = 1 - StringUtils.getLevenshteinDistance(s,s3)*1f / Math.max(s.length(),s3.length());
        System.out.println(f);
    }

    public void testBigIntegerMath(){
        BigInteger i1 = new BigInteger("1121");
        BigInteger i2 = new BigInteger("-1121");
        System.out.println(i1.bitCount() + " " + i1.bitLength());
        System.out.println(i2.bitCount() + " " + i2.bitLength());
    }

    public void testCollection(){
        /*List<DataObject> list1 = new ArrayList();
        List<DataObject> list2 = new Vector<>();

        list1.add(new DataObject("hello1"));
        list1.add(new DataObject("hello2"));
        list2.add(new DataObject("hello1"));
        list2.add(new DataObject("hello2"));

        for (DataObject s : list1){
            System.out.println(s.isVisited());
            ;
        }
        for (DataObject s : list1)
            System.out.println(s.isVisited());

        System.out.println("=====================");
        for (DataObject s : list2){
            System.out.println(s.isVisited());
            s.setVisited(true);
        }
        for (DataObject s : list2)
            System.out.println(s.isVisited());*/

        List list = new ArrayList();
        int i = 1;
        String s = "hu";
        list.add(i);
        list.add(s);
        list.add("hello");

        System.out.println(list.get(0));
        System.out.println(list.get(1));
        System.out.println(StringUtils.join(list,"|"));
    }

    public void testSwitch(){
        String s = "hello";
        switch (s){
            case "caishen":
                System.out.println("I'm comming caishen!");
                break;
            case "recharge":
                System.out.println("I'm recharging, call me god!");
                break;
            default:
                System.out.println("sorry, I'm sleeping!");
        }
    }
    /*
    * 	满足连续登录5天以上，没有充过值，游戏局数3局以下，5天内没玩过财神，进行封禁，记为类型0。
	满足连续登录5天以上，充值在10元以内，无点点充值，游戏局数在6局以下，5天内没玩过财神，进行封禁，记为类型1。
	满足连续登录5天以上，充值在10元到50元之间，无点点充值，游戏局数在10局以下，5天内没玩过财神，进行封禁，记为类型2。
	满足连续登录5天以上，充值在50元到200元之间，无点点充值，游戏局数在20局以下，5天内没玩过财神，进行封禁，记为类型3。
	满足连续登录5天以上，充值在200元到500元之间，无点点充值，游戏局数在30局以下，5天内没玩过财神，进行封禁，记为类型4。
	满足连续登录5天以上，充值在10.1元以内，无点点充值，胜利局数小于等于2局，5天内没玩过财神，进行封禁，记为类型5。

    * */

    public void testAutoForbidden(){
        AutoForbidden once = new AutoForbidden();
        List<Ruler> rulers = new ArrayList<>();
        rulers.add(new OneBasicRuler(5,0,0,0,3,5));
        rulers.add(new TwoBasicRuler(5,0,10,0,6,5));
        rulers.add(new ThreeBasicRuler(5,10,50,0,10,5));
        rulers.add(new FourBasicRuler(5,50,200,0,20,5));
        rulers.add(new FiveBasicRuler(5,200,500,0,30,5));
        rulers.add(new SixBasicRuler(5,0,10.1f,0,2,5));

        String msg = "2014-12-21 00:00:00|1|5|9|0|9|2|1|9|2014-12-18 19:24:30";
        Command cmd = new AllRulerCommand(msg,rulers);
        once.setCommand(cmd);
        ReturnMsg result = once.whoShouldForbidden();
        if (result != null)
            System.out.println(result.getUserId() + "  " + result.getType());
        else
            System.out.println("I'm a good man!");
    }
}
