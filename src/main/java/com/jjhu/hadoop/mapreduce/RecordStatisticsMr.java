package com.jjhu.hadoop.mapreduce;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class RecordStatisticsMr {

    
    public static void main(String[] args) {
    }

    public static class RechargeMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        private String fileTag = "Recharge";

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = new String(value.getBytes(), "GBK");
            String[] items = line.split("|");
            int userID = Integer.parseInt(items[5]);
            context.write(new IntWritable(userID), new Text(this.fileTag + "|" + line));
        }
    }

    public static class UserActionMapper
            extends Mapper<LongWritable, Text, IntWritable, Text> {
        private String fileTag = "UserAction";

        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString();
            String[] items = line.split("|");
            int userID = Integer.parseInt(items[5]);
            context.write(new IntWritable(userID), new Text(this.fileTag + "|" + line));
        }
    }

    public static class StatisticsReducer
            extends Reducer<IntWritable, Text, Text, Text> {
        public static Map<String, Map<String, Set<Integer>>> ip_sameUser_map_register = new HashMap<String, Map<String, Set<Integer>>>();
        public static Map<String, Set<Integer>> ip_mailUser_map_register = new HashMap<String, Set<Integer>>();
        public static Map<String, Map<String, Set<Integer>>> ip_sameUser_map_login = new HashMap<String, Map<String, Set<Integer>>>();
        public static Map<String, Set<Integer>> ip_mailUser_map_login = new HashMap<String, Set<Integer>>();
        private final Date NOWDATE = new Date();
        private final int MAX_CAISHEN_INTERVAL = 10000;
        private final String MIN_DATE = "1900-01-01 00:00:00";
        private final long RECHARGE_HOUR_BOUNDS = 12l;//充值时间界值，单位：小时。用处：多少小时内的充值金额
        private SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置为final时区信息没法更新

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            String nowDate = null;
            int userID = key.get();
            int diandianFlag = 0;
            float rechargeAmount = 0.0F;//
            int caishenInterval = MAX_CAISHEN_INTERVAL;
            int caishenFlag = 0;
            String caishenNewestDate = MIN_DATE;

            Text firstMsg = values.iterator().next();
            DATEFORMAT.setTimeZone(TimeZone.getTimeZone("GMT+" + (int) Float.parseFloat(firstMsg.toString().split("|")[2].substring(0, 3))));//获取日志时间所用时区，并设置
            nowDate = DATEFORMAT.format(NOWDATE);

            for (Text msg : values) {
                String[] items = msg.toString().split("|");
                String date_time = items[2].substring(5, items[2].length());//日志发生时间，截去时区
                long hourInterval = 0;
                try {
                    hourInterval = (DATEFORMAT.parse(nowDate).getTime() - DATEFORMAT.parse(date_time).getTime()) / 1000*60*60l;
                } catch (ParseException e) {
                    System.out.println(date_time + " StatisticReducer:hourInterval date_time is error!" );
                    e.printStackTrace();
                }
                if (items[0].equals("Recharge")) {//充值日志reduce
                    if (hourInterval <= RECHARGE_HOUR_BOUNDS)
                        rechargeAmount += Float.parseFloat(items[8]);

                    if (items[9].equals("diandian")) {
                        diandianFlag = 1;
                    }

                    //短信充值次数：由CommonService smsRechargeTimes实现，参数为充值节点list/时间间隔

                } else if (items[0].equals("UserAction")) {//用户行为日志reduce
                    if (items[8].equals("enter_caishen")) {

                        caishenFlag = 1;
                        caishenNewestDate = caishenNewestDate.compareTo(date_time) > 0 ? caishenNewestDate : date_time;
                    } else if (items[8].equals("register")) {
                        String ip = items[9];
                        String userName = "hu";
                        if (ip_sameUser_map_register.containsKey(ip)) {
                            if ((ip_sameUser_map_register.get(ip)).containsKey(userName)) {
                                ip_sameUser_map_register.get(ip).get(userName).add(userID);
                            } else {
                                Set<Integer> userID_set = new HashSet<Integer>();
                                userID_set.add(userID);
                                ip_sameUser_map_register.get(ip).put(userName, userID_set);
                            }
                        } else {
                            Map<String, Set<Integer>> userName_id_map = new HashMap<String, Set<Integer>>();
                            Set<Integer> userID_set = new HashSet<Integer>();
                            userID_set.add(userID);
                            userName_id_map.put(userName, userID_set);
                            ip_sameUser_map_register.put(ip, userName_id_map);
                        }
                        if (ip_mailUser_map_register.containsKey(ip)) {
                            if (CommonService.hasEmail("select user_str from a_acc_account where acc_id = " + userID)) {
                                ip_mailUser_map_register.get(ip).add(userID);
                            }
                        } else if (CommonService.hasEmail("select user_str from a_acc_account where acc_id = " + userID)) {
                            Set<Integer> userID_set = new HashSet<Integer>();
                            userID_set.add(userID);
                            ip_mailUser_map_register.put(ip, userID_set);
                        }
                    } else if (items[8].equals("login")) {// 登录日志处理：同一ip下相同昵称信息加入ip_sameUser_map_login map中；
                                                          // 同一ip登录的已绑定邮箱用户信息加入ip_mailUser_map_login map中。
                        String ip = items[9];
                        String userName = "hu";
                        if (ip_sameUser_map_login.containsKey(ip)) {
                            if ((ip_sameUser_map_login.get(ip)).containsKey(userName)) {
                                ip_sameUser_map_login.get(ip).get(userName).add(userID);
                            } else {
                                Set<Integer> userID_set = new HashSet<Integer>();
                                userID_set.add(userID);
                                ip_sameUser_map_login.get(ip).put(userName, userID_set);
                            }
                        } else {
                            Map<String, Set<Integer>> userName_id_map = new HashMap<String, Set<Integer>>();
                            Set<Integer> userID_set = new HashSet<Integer>();
                            userID_set.add(userID);
                            userName_id_map.put(userName, userID_set);
                            ip_sameUser_map_login.put(ip, userName_id_map);
                        }
                        if (ip_mailUser_map_login.containsKey(ip)) {
                            if (CommonService.hasEmail("select user_str from a_acc_account where acc_id = " + userID)) {
                                ip_mailUser_map_login.get(ip).add(userID);
                            }
                        } else if (CommonService.hasEmail("select user_str from a_acc_account where acc_id = " + userID)) {
                            Set<Integer> userID_set = new HashSet<Integer>();
                            userID_set.add(userID);
                            ip_mailUser_map_login.put(ip, userID_set);
                        }
                    }

                    //金币异常情况：由CommonService isCoinsExceptionChanged 判定，参数为金币节点list/金币异常阀值/时间间隔

                }
            }
            if (caishenFlag == 1) {
                try {
                    caishenInterval = (int) ((DATEFORMAT.parse(nowDate).getTime() - DATEFORMAT.parse(caishenNewestDate).getTime()) / 24*60*60*1000L);
                } catch (ParseException e) {
                    System.out.println(caishenNewestDate + " StatisticReducer:caishenNewestDate dateParse is error!");
                    e.printStackTrace();
                }
            }
            context.write(new Text(nowDate + "|" + userID + "|" + diandianFlag + "|" + rechargeAmount + "|" + caishenInterval + "|" + caishenNewestDate), new Text());
        }
    }
}
