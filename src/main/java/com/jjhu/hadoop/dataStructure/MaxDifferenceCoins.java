package com.jjhu.hadoop.dataStructure;

/**
 * 30分钟内最大的金币差值定义，取两个金币节点描述
 * Created by jjhu on 2014/12/10.
 */
public class MaxDifferenceCoins {
    MomentCoin maxCoin;
    MomentCoin minCoin;

    public MaxDifferenceCoins(MomentCoin firstCoin, MomentCoin nextCoin) {
        this.maxCoin = firstCoin;
        this.minCoin = nextCoin;
    }

    public MomentCoin getMaxCoin() {
        return maxCoin;
    }

    public MomentCoin getMinCoin() {
        return minCoin;
    }
}
