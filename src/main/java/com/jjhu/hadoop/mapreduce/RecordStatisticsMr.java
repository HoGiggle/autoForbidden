package com.jjhu.hadoop.mapreduce;

import com.jjhu.hadoop.dataStructure.LogTag;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

public class RecordStatisticsMr {

    
    public static void main(String[] args) {
    }


    public static class RecordStatisticsMapper extends Mapper<LongWritable, Text, IntWritable, Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = new String(value.getBytes(),"GBK");
            String []items = line.split("|");
            int userID;
            int logTag;
            if (items.length > 15) {//区别于系统日志
                userID = Integer.parseInt(items[11]);
                logTag = -1;
                if (items[3].equals("login"))
                    logTag = LogTag.USERACTION.getValue();
                else if (items[3].equals("pay_for_order"))
                    logTag = LogTag.RECHARGE.getValue();
                else if (items[3].equals("match_begin_per_player_info"))
                    logTag = LogTag.MATCH.getValue();
                else if (items[3].equals("match_end_per_palyer_info"))
                    logTag = LogTag.MATCH.getValue();
                else if (items[3].equals("caishen_bet"))
                    logTag = LogTag.USERACTION.getValue();

                if (logTag > 0)
                    context.write(new IntWritable(userID), new Text(logTag + "|" + line));
            }
        }
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
            /*
            根据日志存储情况：1.每种日志存储在同一文件夹，对应多个mapper
                          2.每天日志全部放在一个文件夹，一个mapper，通过switch catch 聚合为一个类型日志输出到reducer
             */
        }
    }

    public static class StatisticsReducer
            extends Reducer<IntWritable, Text, IntWritable, Text> {
        //全局变量，用于记录所有userid的汇总信息
        public static Map<String, Map<String, Set<Integer>>> ip_sameUser_map_register = new HashMap<>();
        public static Map<String, Set<Integer>> ip_mailUser_map_register = new HashMap<>();
        public static Map<String, Map<String, Set<Integer>>> ip_sameUser_map_login = new HashMap<>();
        public static Map<String, Set<Integer>> ip_mailUser_map_login = new HashMap<>();
        private final Date NOWDATE = new Date();
        private final int MAX_CAISHEN_INTERVAL = 10000;
        private final String MIN_DATE = "1900-01-01 00:00:00";
        private final long RECHARGE_HOUR_BOUNDS = 12l;
        private final long RECHARGE_MINUTE_BOUNDS = 3l;
        private SimpleDateFormat DATEFORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//设置为final时区信息没法更新?

        protected void reduce(IntWritable key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            //reducer 需要打印部分，用于每个userid
            String nowDate = null;
            int userID = key.get();
            int continueloginDays = 0;
            float rechargeAmount = 0.0F;
            int diandianFlag = 0;
            int playedTimes = 0;
            int winTimes = 0;
            int caishenFlag = 0;
            int caishenInterval = MAX_CAISHEN_INTERVAL;//单位：h
            String caishenNewestDate = MIN_DATE;
            int ipSameNameRegisterNum = 0;
            int ipHaveMailLoginNum = 0;
            int smsRechargTimes = 0;
            int ipSmsRechargeExpNum = 0;
            int maxCoinsChange = 0;//单位：w
            float periodRechargeAmount = 0f;

            //每个userid特征中间量
            List<String> smsRechargeDates = new ArrayList<>();
            List result = new ArrayList<>();

            //从日志中取出公共部分
            String []line = values.iterator().next().toString().split("|");
            DATEFORMAT.setTimeZone(TimeZone.getTimeZone("GMT+" + (int) Float.parseFloat(line[1].substring(0, 3))));//获取日志时间所用时区，并设置
            nowDate = DATEFORMAT.format(NOWDATE);
            continueloginDays = Integer.parseInt(line[17]);
            playedTimes = Integer.parseInt(line[12]);
            winTimes = Integer.parseInt(line[13]);
            rechargeAmount = Integer.parseInt(line[16]);

            for (Text msg : values) {
                String[] items = msg.toString().split("|");
                String date_time = items[1].substring(5, items[1].length());

                if (Integer.parseInt(items[0]) == LogTag.RECHARGE.getValue()) {//充值日志处理
                   /*if (items[4].equals("pay_for_order")){
                       long hourInterval = 1000;
                       try {
                           hourInterval = (DATEFORMAT.parse(nowDate).getTime() - DATEFORMAT.parse(date_time).getTime()) / 1000*60*60l;
                       } catch (ParseException e) {
                           System.out.println(date_time + " StatisticReducer:hourInterval date_time is error!" );
                           e.printStackTrace();
                       }
                       if (hourInterval <= RECHARGE_HOUR_BOUNDS)
                           periodRechargeAmount += Float.parseFloat(items[25]);

                       if (items[18].equals("diandian"))
                           diandianFlag = 1;

                       if (items[18].equals("sms"))
                           smsRechargeDates.add(date_time);
                   }*/
                } else if (Integer.parseInt(items[0]) == LogTag.USERACTION.getValue()) {//行为日志处理
                    if (items[4].equals("caishen_bet")){
                        caishenFlag = 1;
                        caishenNewestDate = caishenNewestDate.compareTo(date_time) > 0 ? caishenNewestDate : date_time;
                    }
                    /*if (items[4].equals("enter_caishen")) {//需要完善，得不到玩财神间隔
                        caishenFlag = 1;
                        caishenNewestDate = caishenNewestDate.compareTo(date_time) > 0 ? caishenNewestDate : date_time;

                    } else if (items[4].equals("register")) {
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

                    } else if (items[4].equals("login")) {// 登录日志处理：同一ip下相同昵称信息加入ip_sameUser_map_login map中；
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

                    } else if (items[4].equals("")){
                        //金币异常情况：由CommonService isCoinsExceptionChanged 判定，参数为金币节点list/金币异常阀值/时间间隔
                    }*/
                }
            }

            //每个userid特征信息赋值
            //smsRechargTimes = CommonService.smsRechargeTimes(smsRechargeDates, RECHARGE_MINUTE_BOUNDS);
            if (caishenFlag == 1) {
                try {
                    caishenInterval = (int) ((DATEFORMAT.parse(nowDate).getTime() - DATEFORMAT.parse(caishenNewestDate).getTime()) / 24*60*60*1000L);
                } catch (ParseException e) {
                    System.out.println(caishenNewestDate + " StatisticReducer:caishenNewestDate dateParse is error!");
                    e.printStackTrace();
                }
            }

            //方法内部，输出日志用StringBuilder实现!(StringUtils join)
            result.add(nowDate);
            result.add(userID);
            result.add(continueloginDays);
            result.add(rechargeAmount);
            result.add(diandianFlag);
            result.add(playedTimes);
            result.add(winTimes);
            result.add(caishenFlag);
            result.add(caishenInterval);
            result.add(caishenNewestDate);
            context.write(new IntWritable(userID), new Text(StringUtils.join(result, "|")));
        }
    }
}
