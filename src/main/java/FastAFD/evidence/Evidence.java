package FastAFD.evidence;

import FastAFD.Utils;
import FastAFD.predicates.Predicate;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

public class Evidence {
    List<Predicate> evidence;
    int columnNumber;
    List<Integer> indexByColumn = new ArrayList<>();
    BitSet bitSet;

    int maxPredicateIndex;
    long count = 0;

    public Evidence(List<Predicate> basedPredicates, int maxPredicateIndex){
        this.evidence = basedPredicates;
        this.columnNumber = basedPredicates.size();
        for(int i = 0; i < columnNumber; i++){
            indexByColumn.add((0));
        }
        this.maxPredicateIndex = maxPredicateIndex;
        this.bitSet = Utils.listToBitSet(indexByColumn, maxPredicateIndex);
    }

    public Evidence(List<Integer> predicateIndexes, long count){
        this.indexByColumn = predicateIndexes;
        this.columnNumber = predicateIndexes.size();
        this.bitSet = Utils.listToBitSet(indexByColumn, maxPredicateIndex);
        this.count = count;
    }
    public Evidence copy(){
        Evidence evidence1 = new Evidence(new ArrayList<>(evidence),maxPredicateIndex);
        List<Integer> index1 = new ArrayList<>(getPredicateIndex());
        evidence1.setIndexByColumn(index1);
        evidence1.resetBitset();
        return evidence1;
    }

    public void updatePredicate(int columnIndex, Predicate predicate, int predicateIndex){
        if(columnIndex >= columnNumber) return;
        evidence.set(columnIndex, predicate);
        indexByColumn.set(columnIndex,predicateIndex);
        resetBitset();
    }

    public Predicate getPredicate(int columnIndex){
        return evidence.get(columnIndex);
    }

    public int getPredicateIndex(int columnIndex){
        return indexByColumn.get(columnIndex);
    }
    public List<Integer> getPredicateIndex(){
        return indexByColumn;
    }

    public void setIndexByColumn(List<Integer> indexByColumn) {
        this.indexByColumn = indexByColumn;
    }
    public void resetBitset(){
        this.bitSet = Utils.listToBitSet(indexByColumn, maxPredicateIndex);
    }
    public int getCount(){
        return (int) count;
    }
}
