package com.jjhu.hadoop.mapreduce;

import com.jjhu.hadoop.dataStructure.DataObject;
import org.apache.commons.lang.StringUtils;

import javax.sound.midi.Soundbank;
import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

/**
 * Created by jjhu on 2014/12/16.
 */

public class DBScan {

    double maxEditDistance;    //最大字符串编辑距离(可能会改为相似度)
    int minDensity;            //最小密度

    public DBScan() {
        maxEditDistance = 3;
        minDensity = 20;
    }

    public DBScan(double maxEditDistance, int minDensity) {
        this.maxEditDistance = maxEditDistance;
        this.minDensity = minDensity;
    }

    //由于自己到自己的距离是0,所以自己也是自己的neighbor
    public List<DataObject> getNeighbors(DataObject targetObject, List<DataObject> objects) {
        List<DataObject> neighbors = new ArrayList<>();
        for (DataObject item : objects){
            double distance = StringUtils.getLevenshteinDistance(targetObject.getEmail(), item.getEmail());
            if (distance <= maxEditDistance)
                neighbors.add(item);
        }
        return neighbors;
    }

    public int dbscan(List<DataObject> objects) {
        int clusterID = 0;
        for (DataObject item : objects) {
            if (item.isVisited())
                continue;
            item.setVisited(true);
            List<DataObject> neighbors = getNeighbors(item, objects);
            if (neighbors.size() < minDensity) {
                if (item.getClusterID() <= 0)
                    item.setClusterID(-1);        //clusterID初始为0,表示未分类；分类后设置为一个正数；设置为-1表示噪声。
            } else {
                if (item.getClusterID() <= 0) {
                    clusterID++;
                    expandCluster(item, neighbors, clusterID, objects);
                } else {
                    expandCluster(item, neighbors, item.getClusterID(), objects);
                }
            }
        }
        return clusterID;   //聚类类别数
    }


    private void expandCluster(DataObject targetObject, List<DataObject> neighbors,
                               int clusterID, List<DataObject> objects) {//簇扩展一层
        targetObject.setClusterID(clusterID);
        for (DataObject item : neighbors){
            if (!item.isVisited()) {
                item.setVisited(true);
                List<DataObject> itemNeighbors = getNeighbors(item, objects);
                if (itemNeighbors.size() >= minDensity) {
                    for (DataObject data : itemNeighbors){
                        if (data.getClusterID() <= 0)
                            data.setClusterID(clusterID);
                    }
                }
            }

            if (item.getClusterID() <= 0)       //q不是任何簇的成员
                item.setClusterID(clusterID);
        }
    }

    public static void main(String[] args) {
        DBScan ds = new DBScan(2,5);
        List<DataObject> list = new ArrayList<>();//list必须由vector实现
        String s1 = "hello";
        String s2 = "helloworld";
        String s3 = "hujinjun2009";
        for (int i = 0; i < 5; i++){
           list.add(new DataObject(s1+i));
//           list.add(new DataObject(s2+i));
//           list.add(new DataObject(s3+i));
        }
        list.add(new DataObject("hello999"));
        list.add(new DataObject("hello9999"));
        list.add(new DataObject("hello999999"));
        list.add(new DataObject("hello99"));
        list.add(new DataObject("hel"));

        int count = ds.dbscan(list);
        System.out.println(count);
        for (DataObject data : list){
            if (data.getClusterID() > 0)
                System.out.println(data.getClusterID() + "  " + data.getEmail());
            else {
                System.out.println(data.getClusterID() + "  " + data.getEmail());

            }
        }
    }
}
