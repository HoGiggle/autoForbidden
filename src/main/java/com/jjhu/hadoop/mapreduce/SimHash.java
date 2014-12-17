package com.jjhu.hadoop.mapreduce;

/**
 * Created by jjhu on 2014/12/11.
 */
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class SimHash {

    private String tokens;

    private BigInteger intSimHash;

    private String strSimHash;

    private int hashbits = 64;

    public SimHash(String tokens) {
        this.tokens = tokens;
        this.intSimHash = this.simHash();
    }

    public SimHash(String tokens, int hashbits) {
        this.tokens = tokens;
        this.hashbits = hashbits;
        this.intSimHash = this.simHash();
    }

    public String getTokens() {
        return tokens;
    }

    public BigInteger getIntSimHash() {
        return intSimHash;
    }

    public String getStrSimHash() {
        return strSimHash;
    }

    public int getHashbits() {
        return hashbits;
    }

    public BigInteger simHash() {
        int[] v = new int[this.hashbits];
        StringTokenizer stringTokens = new StringTokenizer(this.tokens);//分词
        while (stringTokens.hasMoreTokens()) {
            String temp = stringTokens.nextToken();
            BigInteger t = this.hash(temp);
            int weights = 0;
            for (int i = 0; i < this.hashbits; i++) {
                BigInteger bitmask = new BigInteger("1").shiftLeft(i);
                //权重都为1，此处进行权重处理及hash累加
                if (t.and(bitmask).signum() != 0) {
                    v[i] += 1;
                } else {
                    v[i] -= 1;
                }
            }
            weights++;
        }
        BigInteger fingerprint = new BigInteger("0");
        StringBuffer simHashBuffer = new StringBuffer();
        for (int i = 0; i < this.hashbits; i++) {
            if (v[i] >= 0) {
                fingerprint = fingerprint.add(new BigInteger("1").shiftLeft(i));
                simHashBuffer.append("1");
            }else{
                simHashBuffer.append("0");
            }
        }
        this.strSimHash = simHashBuffer.toString();
        return fingerprint;
    }

    private BigInteger hash(String source) {
        if (source == null || source.length() == 0) {
            return new BigInteger("0");
        } else {
            char[] sourceArray = source.toCharArray();
            BigInteger x = BigInteger.valueOf(((long) sourceArray[0]) << 7);
            BigInteger m = new BigInteger("1000003");
            BigInteger mask = new BigInteger("2").pow(this.hashbits).subtract(
                    new BigInteger("1"));
            for (char item : sourceArray) {
                BigInteger temp = BigInteger.valueOf((long) item);
                x = x.multiply(m).xor(temp).and(mask);
            }
            x = x.xor(new BigInteger(String.valueOf(source.length())));
            if (x.equals(new BigInteger("-1"))) {
                x = new BigInteger("-2");
            }
            return x;
        }
    }

    public int hammingDistance(SimHash other) {

        BigInteger x = this.intSimHash.xor(other.intSimHash);
        int tot = 0;
        while (x.signum() != 0) {
            tot += 1;
            x = x.and(x.subtract(new BigInteger("1")));
        }
        return tot;
    }

    public int getDistance(String str1, String str2) {
        int distance;
        if (str1.length() != str2.length()) {
            distance = -1;
        } else {
            distance = 0;
            for (int i = 0; i < str1.length(); i++) {
                if (str1.charAt(i) != str2.charAt(i)) {
                    distance++;
                }
            }
        }
        return distance;
    }

    public List<BigInteger> subByDistance(SimHash simHash, int distance){
        int numEach = this.hashbits/(distance+1);
        List<BigInteger> characters = new ArrayList();

        StringBuffer buffer = new StringBuffer();

        int k = 0;
        for( int i = 0; i < this.intSimHash.bitLength(); i++){
            boolean sr = simHash.intSimHash.testBit(i);

            if(sr){
                buffer.append("1");
            }
            else{
                buffer.append("0");
            }

            if( (i+1)%numEach == 0 ){
                BigInteger eachValue = new BigInteger(buffer.toString(),2);
                System.out.println("----" + eachValue );
                buffer.delete(0, buffer.length());
                characters.add(eachValue);
            }
        }

        return characters;
    }

    public static void main(String[] args) {
        String s = "This is a test string for testing";
        SimHash hash1 = new SimHash(s, 64);
        System.out.println(hash1.intSimHash + "  " + hash1.intSimHash.bitLength());

        hash1.subByDistance(hash1, 3);
        s = "This is a test string for testing, This is a test string for testing abcdef";
        SimHash hash2 = new SimHash(s, 64);
        System.out.println(hash2.intSimHash+ "  " + hash2.intSimHash.bitCount());
        hash1.subByDistance(hash2, 3);
        s = "This is a test string for testing als";
        SimHash hash3 = new SimHash(s, 64);
        System.out.println(hash3.intSimHash+ "  " + hash3.intSimHash.bitCount());
        hash1.subByDistance(hash3, 3);
        System.out.println("============================");
        int dis = hash1.getDistance(hash1.strSimHash,hash2.strSimHash);

        System.out.println(hash1.hammingDistance(hash2) + " "+ dis);

        int dis2 = hash1.getDistance(hash1.strSimHash,hash3.strSimHash);

        System.out.println(hash1.hammingDistance(hash3) + " " + dis2);
    }
}
