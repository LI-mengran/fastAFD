package FastRFD.passjoin;

import FastRFD.pli.Pli;
import it.unimi.dsi.fastutil.ints.IntArrayList;

import java.util.*;

// super class of the InvertedIndex for both token-based and edit-based similarity computation
public abstract class InvertedIndex {

    static class SimilarMatches {
        public LinkedList<Match> matches;
        public int segmentIndex;
        public int lengthOfMatches;

        public SimilarMatches(LinkedList<Match> matches, int segmentIndex, int lengthOfMatches) {
            this.matches = matches;
            this.segmentIndex = segmentIndex;
            this.lengthOfMatches = lengthOfMatches;
        }
    }

    static class Match {
        public int matchedId;
        public int startOfMatch;

        public Match(int matchedId, int startOfMatch) {
            this.matchedId = matchedId;
            this.startOfMatch = startOfMatch;
        }
    }

    static class NonDirectMatch {
        public String dependant;
        public String referenced;

        public NonDirectMatch(String dependant, String referenced) {
            this.dependant = dependant;
            this.referenced = referenced;
        }
    }

    // length -> i-th segment -> segment -> elementId
    protected HashMap<Integer, ArrayList<HashMap<SubstringableString, IntArrayList>>> map;
    protected HashMap<Integer, ArrayList<SubstringableString>> length2Values;
    protected SimilarityMeasureManager similarityMeasureManager;

    public void addElementsWithLength(ArrayList<SubstringableString> elements, int length) {
        length2Values.put(length, elements);
    }

    public void addElementsWithLength(SubstringableString element, int length) {
        if(length2Values.containsKey(length))length2Values.get(length).add(element);
        else {
           ArrayList<SubstringableString> newLen = new ArrayList<>();
            newLen.add(element);
            length2Values.put(length, newLen);
        }
    }


    public ArrayList<SubstringableString> getElementsByLength(int length) {
        ArrayList<SubstringableString> values = length2Values.get(length);
        return values == null ? new ArrayList<>(0) : values;
    }

    abstract void indexByLengthImpl(ArrayList<SubstringableString> values, int length);

    public void indexByLength(int length) {
        if (length <= 0) {
            return;
        }

        ArrayList<SubstringableString> values = length2Values.get(length);
        if (values != null && !values.isEmpty()) {
            indexByLengthImpl(values, length);
        }
    }

    public void removeLengthFromIndex(int length) {
        map.remove(length);
        length2Values.remove(length);
    }

    public void clearIndexStructure() {
        map.clear();
    }

    public void clear() {
        map.clear();
        length2Values.clear();
    }

    public boolean containsLength(int length) {
        return map.containsKey(length);
    }

    public abstract boolean existsSimilarReferencedValueForLength(SubstringableString searchString, int queryLength, int dependentColumnIndex, int referencedColumnIndex, HashMap<Integer, HashMap<Integer, LinkedList<NonDirectMatch>>> errors);
    public abstract HashMap<String,Integer> existsSimilarReferencedValueForLength(SubstringableString searchString, int columnIndex, int queryLength, HashMap<Integer, HashMap<Integer, LinkedList<NonDirectMatch>>> errors, int startTupleId, Pli<String> pli);
    public abstract void removeValue(String val,int columnIndex, int tupleId);
}
