package org.zstack.sdk;

public class QueryClusterResult {
    public java.util.List<ClusterInventory> inventories;
    public void setInventories(java.util.List<ClusterInventory> inventories) {
        this.inventories = inventories;
    }
    public java.util.List<ClusterInventory> getInventories() {
        return this.inventories;
    }

    public java.lang.Long total;
    public void setTotal(java.lang.Long total) {
        this.total = total;
    }
    public java.lang.Long getTotal() {
        return this.total;
    }

}
