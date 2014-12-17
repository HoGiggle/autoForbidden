package com.jjhu.hadoop.dataStructure;

/**
 * Created by jjhu on 2014/12/17.
 */
public class DataObject {
    private String email;
    private int clusterID;
    private boolean isVisited;

   public DataObject(String email) {
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
