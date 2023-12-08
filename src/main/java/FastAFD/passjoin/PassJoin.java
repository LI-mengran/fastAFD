package FastAFD.passjoin;


import FastAFD.Utils;
import FastAFD.input.ColumnStats;
import FastAFD.pli.Pli;

import java.util.*;

public class PassJoin {
    private List<Integer> maxSimilarityThresold;
    private int numAllColumns;
    List<SimilarityMeasureManager> similarityMeasureManager = new ArrayList<>();
    protected ArrayList<InvertedIndex> invertedIndexByColumn = new ArrayList<>();
    private final ArrayList<ColumnStats> columnStats;


    public PassJoin(List<Integer> maxerrors, ArrayList<ColumnStats> columnStats){
        this.columnStats = columnStats;
        maxSimilarityThresold = maxerrors;
        for(Integer thresold : maxSimilarityThresold)
            this.similarityMeasureManager.add(new SimilarityMeasureManager(thresold,0,false,false));
        for(int index = 0; index < columnStats.size(); index++){
            Utils.substringInfos.add(new HashMap<>());
        }
    }

    public void createIndexs(List<List<String>> allValues, int longestLength){
        numAllColumns = allValues.size();
        for(int i = 0; i < numAllColumns; i++){
            invertedIndexByColumn.add(new InvertedSegmentIndex(similarityMeasureManager.get(i)));
        }
        Utils.editDistanceBuffer = new int[longestLength + 1][longestLength + 1];
        for (int i = 0; i < longestLength; i++) {
            Utils.editDistanceBuffer[0][i] = i;
            Utils.editDistanceBuffer[i][0] = i;
        }
        for(int columnIndex = 0; columnIndex < numAllColumns; columnIndex++){
            InvertedIndex invertedIndex = invertedIndexByColumn.get(columnIndex);
            List<String> values = allValues.get(columnIndex);
            HashSet<Integer> lens = new HashSet<>();
            for(String val : values){
                invertedIndex.addElementsWithLength(new SubstringableString(val), val.length());
                lens.add(val.length());
            }
            for(Integer len : lens){
                invertedIndex.indexByLength(len);
            }
        }
    }



    public HashMap<String, Integer> getSimilaritySet(String queryWord, int columnIndex, int startTupleId, Pli<String> pli){
        HashMap<String,Integer> valuetoSimilar = new HashMap<>();
        ColumnStats columnStats1 = columnStats.get(columnIndex);
        int currentLength = queryWord.length();
        int currEditDistance = similarityMeasureManager.get(columnIndex).getMaximumEditDistanceForLength(currentLength);
        if(queryWord.length() <= currEditDistance + 1){
            for(int currentL = columnStats1.longestLength; currentL >= columnStats1.shortestLength; currentL--) {
                valuetoSimilar.putAll(computeShortValues(queryWord, invertedIndexByColumn.get(columnIndex).getElementsByLength(currentL), currEditDistance,startTupleId,pli));
            }
            return valuetoSimilar;
        }
        InvertedIndex currIndex = invertedIndexByColumn.get(columnIndex);
        for (int index = 0; index <= 2 * currEditDistance; index++) {
            int queryLength = currentLength + getEditDistanceFromIndex(index);
            if (queryLength <= columnStats.get(columnIndex).longestLength && queryLength > 0) {
                if(queryLength <= currEditDistance + 1){
                    valuetoSimilar.putAll(computeShortValues(queryWord, invertedIndexByColumn.get(columnIndex).getElementsByLength(queryLength),currEditDistance,startTupleId,pli));
                    continue;
                }
                valuetoSimilar.putAll(currIndex.existsSimilarReferencedValueForLength(new SubstringableString(queryWord),columnIndex, queryLength, null, startTupleId,pli));
            }
//            }

        }
        return valuetoSimilar;
    }

    private int getEditDistanceFromIndex(int index) {
        if (index == 0) return 0;
        else if (index % 2 == 1) return (index + 1) / 2;
        else return index / -2;
    }
    public void removeValue(String queryWord, int columnIndex, int tupleId){
        invertedIndexByColumn.get(columnIndex).removeValue(queryWord, columnIndex, tupleId);
    }

    private HashMap<String, Integer> computeShortValues(String queryWord, List<SubstringableString> shortValues, int threshould, int startTupleId, Pli<String> pli){
        HashMap<String, Integer> shortResult = new HashMap<>();
        for(var shortValue : shortValues){
//            if(pli.getLastTupleIdByKey(shortValue.toString()) < startTupleId)continue;
            double distance = Utils.calculateEditDistance(queryWord, shortValue.toString());
            if(distance <= threshould)
                shortResult.put(shortValue.toString(), (int) distance);
        }
        return shortResult;
    }
}
