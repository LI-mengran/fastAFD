package FastAFD.pli;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

public class Pli<T> {
    public List<T> keys;
    boolean isNum;
    List<Cluster> clusters;
    Map<T, Integer> keyToClusterIdMap;
    public Pli(Boolean isNum, List<Cluster> rawClusters, List<T> keys, Map<T, Integer> translator){
        this.clusters = rawClusters;
        this.isNum = isNum;

        this.keys = keys;
        this.keyToClusterIdMap = translator;
    }
    public int size() {
        return keys.size();
    }
    public List<T> getKeys(){
        return keys;
    }
    public boolean isNumType(){
        return isNum;
    }

    public List<Cluster> getClusters() {
        return clusters;
    }

    public <T> Cluster getClusterByKey(T key) {
        Integer clusterId = keyToClusterIdMap.get(key);
        return clusterId != null ? clusters.get(clusterId) : null;
    }
    public int getLastTupleIdByKey(T key){
        Cluster cluster = getClusterByKey(key);
        return cluster.get(cluster.size() - 1);
    }
    public <T> Integer getClusterIdByKey(T key) {
        return keyToClusterIdMap.get(key);
    }
    public int getFirstIndexWhereKeyIsLTE(BigDecimal target) {
        if(keys.get(0) instanceof Double){
            if ((Double) keys.get(0) >= target.doubleValue()) return 0;
        }
        else if(keys.get(0) instanceof Integer){
                if((Integer) keys.get(0) >= target.intValue()) return 0;
            }
        else return -1;
        return getFirstIndexWhereKeyIsLTE(target, 0);

    }


    public int getFirstIndexWhereLengthIsLTE(int length){
        if(length <= 0)return 0;
        int l = 0;
        int r = keys.size();
        while(l < r){
            int m = l + ((r - l) >>> 1);
            if (keys.get(m).toString().length() >= length) r = m;
            else l = m + 1;
        }
        return l;
    }

    public  Integer getFirstIndexWhereKeyIsLTE(BigDecimal target, int l) {
        Integer i = keyToClusterIdMap.get(target);
        if (i != null) return i;

        int r = keys.size();
        while (l < r) {
            int m = l + ((r - l) >>> 1);
            if(keys.get(m) instanceof Integer){
                if ((Integer) keys.get(m) >= target.intValue()) r = m;
                else l = m + 1;
            }
            else if(keys.get(m) instanceof Double){
                if ((Double) keys.get(m) >= target.doubleValue()) r = m;
                else l = m + 1;
            }

        }

        return l;
    }
    public Cluster getCluster(int i) {
        return clusters.get(i);
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < clusters.size(); i++)
                sb.append(keys.get(i) + ": " + clusters.get(i) + "\n");
//        sb.append(Arrays.toString(keys) + "\n");
//        sb.append(keyToClusterIdMap + "\n");

        return sb.toString();
    }

}
