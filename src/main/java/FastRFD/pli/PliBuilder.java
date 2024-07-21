package FastRFD.pli;

import FastRFD.input.ColumnStats;
import FastRFD.input.ParsedColumn;

import java.util.*;

public class PliBuilder {
    private final boolean[] isNum;
    private int longestLength;
    List<Pli<?>> plis = new ArrayList<>();

    public PliBuilder(List<ParsedColumn<?>> pColumns){
        int colCount = pColumns.size();
        isNum = new boolean[colCount];
        for(int col = 0; col < colCount; col++){
            isNum[col] = pColumns.get(col).getType() != String.class;
        }
    }
    public void buildPlis(ArrayList<ColumnStats> columnStats, List<ParsedColumn<?>> pColumns){
        if (pColumns == null || pColumns.size() == 0 || pColumns.get(0).size() == 0 || columnStats == null)return;
        for(int col = 0; col < pColumns.size(); col++){
            if(pColumns.get(col).getType() == String.class) {
                HashSet<String> keySet = new HashSet<>();
                ColumnStats columnStat = new ColumnStats(col,pColumns.get(col).getType());
                columnStats.add(columnStat);
                columnStat.isNum = false;
                ParsedColumn<?> column = pColumns.get(col);
                for (int row = 0; row < column.size(); ++row){
                    String val = pColumns.get(col).getValue(row).toString();
                    int len = val.length();
                    if(longestLength < val.length())longestLength = val.length();
                    if(len > columnStat.longestLength)columnStat.longestLength = len;
                    if(len < columnStat.shortestLength)columnStat.shortestLength = len;
                    keySet.add(val);
                }
                List<String> keys = new ArrayList<>(keySet.stream().toList());
                keys.sort((s1, s2) -> s1.length() - s2.length());


                Map<String, Integer> keyToClusterID = new HashMap<>(); // int (key) -> cluster id
                for (int clusterID = 0; clusterID < keys.size(); clusterID++)
                    keyToClusterID.put(keys.get(clusterID), clusterID);

                List<Cluster> clusters = new ArrayList<>();             // put tuple indexes in clusters
                for (int i = 0; i < keys.size(); i++)
                    clusters.add(new Cluster());

                for (int row = 0; row < pColumns.get(0).size(); ++row)
                    clusters.get(keyToClusterID.get(column.getValue(row).toString())).add(row);

                plis.add(new Pli<>(isNum[col], clusters, keys, keyToClusterID));
            }
            else if (pColumns.get(col).getType() == Integer.class) {
                HashSet<Integer> keySet = new HashSet<>();
                ColumnStats columnStat = new ColumnStats(col,pColumns.get(col).getType());
                columnStats.add(columnStat);
                columnStat.isNum = true;
                ParsedColumn<?> column = pColumns.get(col);
                for (int row = 0; row < column.size(); ++row){
                    Integer val = Integer.parseInt(pColumns.get(col).getValue(row).toString());
                    keySet.add(val);
                    if(val > columnStat.maxNumber) columnStat.maxNumber = val;
                    if(val < columnStat.minNumber) columnStat.minNumber = val;
                }
                List<Integer> keys = new ArrayList<>(keySet.stream().toList());
                Collections.sort(keys);

                Map<Integer, Integer> keyToClusterID = new HashMap<>(); // int (key) -> cluster id
                for (int clusterID = 0; clusterID < keys.size(); clusterID++)
                    keyToClusterID.put(keys.get(clusterID), clusterID);

                List<Cluster> clusters = new ArrayList<>();             // put tuple indexes in clusters
                for (int i = 0; i < keys.size(); i++)
                    clusters.add(new Cluster());

                for (int row = 0; row < pColumns.get(0).size(); ++row)
                    clusters.get(keyToClusterID.get(Integer.parseInt(pColumns.get(col).getValue(row).toString()))).add(row);

                plis.add(new Pli<>(isNum[col], clusters, keys, keyToClusterID));
            }
            else {
                HashSet<Double> keySet = new HashSet<>();
                ColumnStats columnStat = new ColumnStats(col,pColumns.get(col).getType());
                columnStats.add(columnStat);
                columnStat.isNum = true;
                ParsedColumn<?> column = pColumns.get(col);
                for (int row = 0; row < column.size(); ++row){
                    double val = Double.parseDouble(pColumns.get(col).getValue(row).toString());
                    keySet.add(val);
                    if(val > columnStat.maxNumber) columnStat.maxNumber = val;
                    if(val < columnStat.minNumber) columnStat.minNumber = val;
                }
                List<Double> keys = new ArrayList<>(keySet.stream().toList());
                Collections.sort(keys);

                Map<Double, Integer> keyToClusterID = new HashMap<>(); // int (key) -> cluster id
                for (int clusterID = 0; clusterID < keys.size(); clusterID++)
                    keyToClusterID.put(keys.get(clusterID), clusterID);

                List<Cluster> clusters = new ArrayList<>();             // put tuple indexes in clusters
                for (int i = 0; i < keys.size(); i++)
                    clusters.add(new Cluster());

                for (int row = 0; row < pColumns.get(0).size(); ++row)
                    clusters.get(keyToClusterID.get(Double.parseDouble(pColumns.get(col).getValue(row).toString()))).add(row);

                plis.add(new Pli<>(isNum[col], clusters, keys, keyToClusterID));
            }
        }
    }
    public List<Integer> getTuplesByKey(int columnIndex, String key){
        Pli<?> pli = plis.get(columnIndex);
        int clusterId = pli.getClusterIdByKey(key);
        return pli.getClusters().get(clusterId).getRawCluster();
    }

    //only double value
    public List<Integer> getTuplesByKey(int columnIndex, Double key){
        return plis.get(columnIndex).getClusterByKey(key) == null ? null : plis.get(columnIndex).getClusterByKey(key).tuples;
    }

    public List<Integer> getTuplesByKey(int columnIndex, Integer key){
        return plis.get(columnIndex).getClusterByKey(key) == null ? null : plis.get(columnIndex).getClusterByKey(key).tuples;
    }

    public List<Pli<?>> getPlis() {
        return plis;
    }

    public boolean isLastTuple(int columnIndex, String val, int tupleId){
        return tupleId == getTuplesByKey(columnIndex, val).get(getTuplesByKey(columnIndex, val).size() - 1);
    }
    public boolean isLastTuple(int columnIndex, Double val, int tupleId){
        return tupleId == getTuplesByKey(columnIndex, val).get(getTuplesByKey(columnIndex, val).size() - 1);
    }

    public boolean isLastTuple(int columnIndex, Integer val, int tupleId){
        return tupleId == getTuplesByKey(columnIndex, val).get(getTuplesByKey(columnIndex, val).size() - 1);
    }

    public int getLastTuple(int columnIndex, String val){
        return plis.get(columnIndex).getClusterByKey(val).get(plis.get(columnIndex).getClusterByKey(val).size() - 1);
    }

}
