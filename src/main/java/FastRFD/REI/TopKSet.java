package FastRFD.REI;

import FastRFD.RFD.RFD;

import java.util.*;

class UtilityPair{
    RFD afd;
    double utility;

    public UtilityPair(RFD afd, double utility){
        this.afd = afd;
        this.utility = utility;
    }

    public RFD getAfd() {
        return afd;
    }

    public double getUtility() {
        return utility;
    }
}

public class TopKSet {
    PriorityQueue<Double> topKSet;
    HashMap<Double, List<RFD>> utility2AFD;
    int maxK;
    int curK;
    int columnIndex;
    public TopKSet(int k, int columnIndex){
        topKSet = new PriorityQueue<>(k + 1);
        this.maxK = k;
        this.curK = 0;
        this.columnIndex = columnIndex;
        utility2AFD = new HashMap<>();
    }

    public double getMin(){
        if(topKSet.size() == 0  || curK < maxK)return -1;
        else return topKSet.peek();
    }

    public void add(RFD afd, double utility){
        curK++;
        if(!topKSet.contains(utility))topKSet.add(utility);
        utility2AFD.computeIfAbsent(utility, k -> new ArrayList<>()).add(afd);
    }

    public void insertWithUpdate(RFD afd, double utility){

        if( curK == maxK && utility < topKSet.peek())return;
        if(!updateTopKSet(afd, utility))return;
        if(topKSet.size() == 0 || curK < maxK){
            if(!topKSet.contains(utility))
                topKSet.offer(utility);
            utility2AFD.computeIfAbsent(utility, k -> new ArrayList<>()).add(afd);
//            updateAFDset(afdset, null, afd);
            curK++;
            return;
        }

        if(utility > topKSet.peek()){
            double min = topKSet.peek();
            deleteElement(min, utility2AFD.get(min).get(0));

            if(!utility2AFD.containsKey(utility))
                topKSet.offer(utility);
            utility2AFD.computeIfAbsent(utility, k -> new ArrayList<>()).add(afd);
            curK++;
        }
//        System.out.println(curK);
    }


    boolean updateTopKSet(RFD afd, double utility){
        HashSet<UtilityPair> removeSet = new HashSet<>();
        for (double curUtility : topKSet) {
            if (curUtility <= utility) {
                for (RFD cand : utility2AFD.get(curUtility)) {
                    if (canCover(cand.getThresholdsIndexes(), afd.getThresholdsIndexes()))
                        removeSet.add(new UtilityPair(cand, curUtility));
//                        deleteElement(curUtility, cand);
                }
            } else {
                for (RFD cand : utility2AFD.get(curUtility)) {
                    if (canCover(afd.getThresholdsIndexes(), cand.getThresholdsIndexes())) {
                        return false;
                    }
                }
            }
        }
        for(var utilityPair : removeSet){
            deleteElement(utilityPair.getUtility(), utilityPair.getAfd());
        }
        return true;
    }

    boolean canCover(List<Integer> target, List<Integer> currentIdx){
        if(target.get(columnIndex) > currentIdx.get(columnIndex))return false;
        for(int i = 0; i < target.size(); i++){
            if(i == columnIndex)continue;
            if(target.get(i) < currentIdx.get(i)) {
                return false;
            }
        }
        return true;
    }

    void deleteElement(double utility, RFD afd){
        curK--;
        if(utility2AFD.get(utility).size() == 1){

            topKSet.remove(utility);
            utility2AFD.remove(utility);
        }
        else {
            utility2AFD.get(utility).remove(afd);
        }
    }

    public boolean containsSubset(List<Integer> afd, int RIndex){

        for (double curUtility : topKSet) {
            for (RFD cand : utility2AFD.get(curUtility)) {
                boolean flag = true;
                if(cand.getRIndex() < RIndex)continue;
                for(int i = 0; i < afd.size(); i++){
                    if(i == columnIndex)continue;
                    if(cand.getThresholdsIndexes().get(i) > afd.get(i)) {
                        flag = false;
                        break;
                    }
                }
                if(flag) return true;
            }
        }
        return false;
    }

    public List<RFD> getAFDs(){
        List<RFD> afds = new ArrayList<>();
        for (double curUtility : topKSet) {
            afds.addAll(utility2AFD.get(curUtility));
        }
        return  afds;
    }

    public PriorityQueue<Double> getTopKSet() {
        return topKSet;
    }

    public  List<RFD> getUtility2AFD(double utility) {
        return utility2AFD.get(utility);
    }
}
