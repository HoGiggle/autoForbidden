package com.jjhu.hadoop.dataStructure;

/**
 * 日志中金币变化节点定义，date为日志中时间，coins为日志中金币值
 * Created by jjhu on 2014/12/10.
 */
public class MomentCoin {
    String date;
    int    coins;

    public MomentCoin(String date, int coins) {
        this.date = date;
        this.coins = coins;
    }

    public String getDate() {
        return date;
    }

    public int getCoins() {
        return coins;
    }

}
