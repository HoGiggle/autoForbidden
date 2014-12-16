package com.jjhu.hadoop.mapreduce;

import org.apache.commons.lang.StringUtils;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

/**
 * Created by jjhu on 2014/12/16.
 */
class DataObject {
    private String email;
    private int clusterID;
    private boolean isVisited;

    DataObject(String email) {
        this.email = email;
        clusterID = 0;
        isVisited = false;
    }

    public String getEmail() {
        return email;
    }

    public int getClusterID() {
        return clusterID;
    }

    public boolean isVisited() {
        return isVisited;
    }

    public void setClusterID(int clusterID) {
        this.clusterID = clusterID;
    }

    public void setVisited(boolean isVisited) {
        this.isVisited = isVisited;
    }
}


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
    public Vector<DataObject> getNeighbors(DataObject targetObject, List<DataObject> objects) {
        Vector<DataObject> neighbors = new Vector<DataObject>();
        for (DataObject item : objects){
            int distance = StringUtils.getLevenshteinDistance(targetObject.getEmail(), item.getEmail());
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
            Vector<DataObject> neighbors = getNeighbors(item, objects);
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


    private void expandCluster(DataObject targetObject, Vector<DataObject> neighbors,
                               int clusterID, List<DataObject> objects) {
        targetObject.setClusterID(clusterID);
        for (DataObject item : neighbors){
            if (!item.isVisited()) {
                item.setVisited(true);
                Vector<DataObject> itemNeighbors = getNeighbors(item, objects);
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
     /*   DataSource datasource = new DataSource();
        //maxEditDistance=3,minDensity=4
        datasource.readMatrix(new File("/home/orisun/test/dot.mat"));
        datasource.readRLabel(new File("/home/orisun/test/dot.rlabel"));
        //maxEditDistance=2.5,minDensity=4
//		datasource.readMatrix(new File("/home/orisun/text.normalized.mat"));
//		datasource.readRLabel(new File("/home/orisun/text.rlabel"));
        DBScan ds = new DBScan();
        int clunum = ds.dbscan(datasource.objects);
        datasource.printResult(datasource.objects, clunum);*/


        DBScan ds = new DBScan(3,5);
        List<DataObject> list = new ArrayList<>();
        list.add(new DataObject("helloworld1"));
        list.add(new DataObject("helloworld12"));
        list.add(new DataObject("helloworld34"));
        list.add(new DataObject("helloworld18"));
        list.add(new DataObject("helloworld246"));
        list.add(new DataObject("helloworld243"));
        list.add(new DataObject("helloworld182"));
        list.add(new DataObject("hello"));

        int count = ds.dbscan(list);
        System.out.println(count);
        for (DataObject data : list){
            if (data.getClusterID() > 0)
                System.out.println(data.getClusterID() + "  " + data.getEmail());
        }
    }
}
