package com.jjhu.hadoop.mapreduce;

import com.jjhu.hadoop.dataStructure.MaxDifferenceCoins;
import com.jjhu.hadoop.dataStructure.MomentCoin;

import java.sql.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * CommonService类提供一些公共方法
 * Created by jjhu on 2014/12/9.
 */
public class CommonService {
    public static Connection getConnection(){
        Connection conn = null;
        Statement st = null;
        String url = "jdbc:mysql://172.16.7.201:3306/accountdb_lhj?" +
                "user=baina&password=123456&useUnicode=true&characterEncoding=UTF8";
        try {
            Class.forName("com.mysql.jdbc.Driver");
            conn = DriverManager.getConnection(url);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            return conn;
        }
    }

    public static ResultSet executeQuery(String sql){
        Connection conn = getConnection();
        Statement st = null;
        ResultSet rt = null;
        try {
            st = conn.createStatement();
            rt = st.executeQuery(sql);
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                st.close();
                conn.close();
            } catch (SQLException e) {
                System.out.println("statement或connection关闭失败!");
                e.printStackTrace();
            } finally {
                return rt;
            }
        }
    }

    public static boolean hasEmail(String sql){
        ResultSet rt = executeQuery(sql);
        try {
            if (rt.next()){
                return true;
            }
        } catch (SQLException e) {
            System.out.println("hasEmail:I have exception!");
            e.printStackTrace();
        } finally {
            return false;
        }
    }

    public static MaxDifferenceCoins maxDifferenceCoins(List<MomentCoin> coinList, long howLongMinute){
        if (coinList == null || coinList.size() == 0 || howLongMinute <= 0)
            return null;
        MomentCoin []coins = (MomentCoin[]) coinList.toArray();
        MomentCoin maxCoin = coins[0];
        MomentCoin minCoin = coins[0];

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String finallyDate;
        long minuteParam = howLongMinute;//多少时间内的金币变化最大值，可作为参数传入，时间单位为分钟
        long minInterval;
        int key = 1;
        try {
            finallyDate = df.format(df.parse(coins[coins.length - 1].getDate()).getTime() - minuteParam*60*1000l);
            for (int i = 0; key > 0; i++) {
                MomentCoin newMaxCoin = coins[i];
                MomentCoin newMinCoin = coins[i];
                for (int j = i + 1; j < coins.length; ++j) {
                    minInterval = Math.abs(df.parse(coins[j].getDate()).getTime() - df.parse(coins[i].getDate()).getTime()) / 1000 * 60l;
                    if (minInterval > minuteParam)
                        break;
                    if (coins[j].getCoins() > newMaxCoin.getCoins())
                        newMaxCoin = coins[j];
                    if (coins[j].getCoins() < newMinCoin.getCoins())
                        newMinCoin = coins[i];
                    if ((newMaxCoin.getCoins() - newMinCoin.getCoins()) > (maxCoin.getCoins() - minCoin.getCoins())){
                        maxCoin = newMaxCoin;
                        minCoin = newMinCoin;
                    }
                    if (newMaxCoin.getCoins() - newMinCoin.getCoins() > 100){
                           //可以在此处记录金币波动超过阀值次数
                    }
                }

                key = finallyDate.compareTo(coins[i].getDate());//最后minuteParam分钟的日志只需要处理一次即可
            }
        } catch (ParseException e) {
            System.out.println("CommonService:maxDifferenceCoins's dateParse is error!");
            e.printStackTrace();
        } finally {
          return new MaxDifferenceCoins(maxCoin,minCoin);
        }
    }

    public static boolean isCoinsExceptionChanged(List<MomentCoin> coinList, int exceptionCoin, long howLongMinute){
        MaxDifferenceCoins coins = maxDifferenceCoins(coinList, howLongMinute);
        if (coins == null)
            return false;

        int absCoinDifference = Math.abs(coins.getMaxCoin().getCoins() - coins.getMinCoin().getCoins());
        if (absCoinDifference >= exceptionCoin)
            return true;
        return false;
    }

    public static int smsRechargeTimes(List<String> rechargeDateList, long howLongMinute){
        if (rechargeDateList == null || rechargeDateList.size() == 0 || howLongMinute <= 0)
            return 0;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        String []dateList = (String[]) rechargeDateList.toArray();
        long minuteParam = howLongMinute;//多长时间内的短信充值次数，可使用参数传入，可以自己定义使用，by you.
        long minInterval;
        int maxCount = 0; //最多充值次数，用于返回
        int count;

        try {
            for (int i = 0; i < dateList.length - 1; i++){
                count = 0;
                for (int j = i + 1; j < dateList.length; j++){
                    minInterval = Math.abs(df.parse(dateList[j]).getTime() - df.parse(dateList[i]).getTime()) / 1000 * 60l;
                    if (minInterval > minuteParam)
                        break;
                    count++;
                }
                if (count > maxCount)
                    maxCount = count;
            }
        } catch (ParseException e){
            System.out.println("CommonService:smsRechargeTimes's dateParse is error!");
            e.printStackTrace();
        } finally {
            return maxCount;
        }
    }

    public static String getCommonIp(Map<String, Integer> map){
        if (map != null && map.size() > 0){
            int maxTimes = 0;
            String commonIp = null;

            Set<Map.Entry<String, Integer>> set = map.entrySet();
            for (Map.Entry<String, Integer> entry : set){
                if (entry.getValue() > maxTimes){
                    maxTimes = entry.getValue();
                    commonIp = entry.getKey();
                }
            }
            return commonIp;
        }else
            return null;
    }
}
