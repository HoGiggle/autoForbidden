package com.jjhu.hadoop.ruler;

/**
 * 一类封号规则，成员变量参照规则定义
 * Created by jjhu on 2014/12/1.
 */
public abstract class OneTypeRuler extends Ruler {
    int continueLoginDays;
    float lowRechargeAmount;
    float highRechargeAmount;
    int diandianFlag;
    int playTimes;
    int caishenInterval;

    protected OneTypeRuler(int continueLoginDays, float lowRechargeAmount, float highRechargeAmount, int diandianFlag, int playTimes, int caishenInterval) {
        this.continueLoginDays = continueLoginDays;
        this.lowRechargeAmount = lowRechargeAmount;
        this.highRechargeAmount = highRechargeAmount;
        this.diandianFlag = diandianFlag;
        this.playTimes = playTimes;
        this.caishenInterval = caishenInterval;
    }
}
