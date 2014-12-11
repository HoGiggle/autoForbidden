package com.jjhu.hadoop.ruler;

/**
 * 一类封号规则，成员变量参照规则定义
 * Created by jjhu on 2014/12/1.
 */
public abstract class OneTypeRuler extends Ruler {
    int loginDay;
    float rechargeAmount;
    int rechargeByDiandian;
    int playTimes;
    int godInterval;

    protected OneTypeRuler(int loginDay, float rechargeAmount, int rechargeByDiandian, int playTimes, int godInterval) {
        this.loginDay = loginDay;
        this.rechargeAmount = rechargeAmount;
        this.rechargeByDiandian = rechargeByDiandian;
        this.playTimes = playTimes;
        this.godInterval = godInterval;
    }

    protected OneTypeRuler() {
    }
}
