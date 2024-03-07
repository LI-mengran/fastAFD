package FastAFD.AEI;

import FastAFD.AFD.AFD;
import FastAFD.AFD.AFDSet;
import FastAFD.evidence.Evidence;
import FastAFD.evidence.EvidenceSet;
import FastAFD.predicates.PredicatesBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.time.Instant;
import java.util.*;

public class RelaxedEvidenceInversion {
    boolean nog1Error = true;
    private final int columnNumber;
    private final int evidenceNumber;
    private int rowNumber;
    private List<Integer> maxIndexes;
    private EvidenceSet evidenceSet;
    private Evidence [] evidencesArray;
    //Attention that some rIndexes set may be empty
    List<List<RoaringBitmap>> prefixEviSet = new ArrayList<>();
    private List<Long> intevalIds;
    List<AFDSet> AFDSets = new ArrayList<>();
    List<TopKSet> topKSets = new ArrayList<>();
    long miniTime;
    long hitTime;
    long walkTime;

    public RelaxedEvidenceInversion(PredicatesBuilder pBuilder, int rowNumber, EvidenceSet evidenceSet, boolean nog1Error){
        this.columnNumber = pBuilder.getColumnNumber();
        this.rowNumber = rowNumber;
        this.evidenceSet = evidenceSet;
        this.evidenceNumber = evidenceSet.getEvidenceSet().size();
        this.maxIndexes = pBuilder.getMaxPredicateIndexByColumn();
        this.nog1Error = nog1Error;
        for(int i = 0; i < columnNumber; i++){
            AFDSets.add(new AFDSet(i, maxIndexes.get(i)));
        }
    }

    public void buildPrefixEviSet(){
        for(int i = 0; i < maxIndexes.size(); i++){
            prefixEviSet.add(new ArrayList<>());
            for(int j = 0; j < maxIndexes.get(i); j++){
                prefixEviSet.get(i).add(new RoaringBitmap());
            }
        }
        for(int k = 0; k < evidenceSet.getEvidenceSet().size(); k++){
            Evidence evi = evidenceSet.getEvidenceSet().get(k);
            List<Integer> EPI = evi.getPredicateIndex();
            for(int i = 0; i < EPI.size(); i++){
                for(int j = 0; j <= EPI.get(i); j++)
                    prefixEviSet.get(i).get(j).add(k);
            }
        }
    }

    public AFDSet buildAFD(double threshold){
//        if(threshold == 0)return null;
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
            List<Long> targets = generateTargets(sortedEvidences, threshold);
            evidencesArray = flattenAndConvert(sortedEvidences);
            intevalIds = new ArrayList<>();
            for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.add(0L);
            }
            for(var splitEvidences : sortedEvidences){
                Collections.sort(splitEvidences, (o1, o2) -> Long.compare(o1.getCount(), o2.getCount()));
                if(splitEvidences.size() == 0)continue;
                intevalIds.set(splitEvidences.get(0).getPredicateIndex(columnIndex), (long) splitEvidences.size());
            }
            for(int i = 1; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.set(i, intevalIds.get(i - 1) + intevalIds.get(i));
            }

            inverseEvidenceSet(targets,columnIndex);
            Instant start = Instant.now();

            AFDSets.get(columnIndex).minimize();
            Duration duration = Duration.between(start, Instant.now());
            miniTime += duration.toMillis();

            //printout
//            System.out.println("column " + columnIndex + " completed.");
        }
        AFDSet minimal = new AFDSet();
        int totalCount = 0;

        for(var afds : AFDSets){
            for(AFD afd :  afds.getMinimalAFDs()){
                minimal.directlyAdd(afd);
            }
            totalCount += afds.minimalCount();
        }
        System.out.println(totalCount);
        System.out.println("miniteTime: " + miniTime);
        System.out.println("hitTime: " + hitTime);
        System.out.println("walkTime: " + walkTime);
        return minimal;
    }

    public List<TopKSet> buildTopK(double threshold){
//        if(threshold == 0)return null;
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
            List<Long> targets = generateTargets(sortedEvidences, threshold);
            evidencesArray = flattenAndConvert(sortedEvidences);
            intevalIds = new ArrayList<>();
            for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.add(0L);
            }
            for(var splitEvidences : sortedEvidences){
                Collections.sort(splitEvidences, (o1, o2) -> Long.compare(o1.getCount(), o2.getCount()));
                if(splitEvidences.size() == 0)continue;
                intevalIds.set(splitEvidences.get(0).getPredicateIndex(columnIndex), (long) splitEvidences.size());
            }
            for(int i = 1; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.set(i, intevalIds.get(i - 1) + intevalIds.get(i));
            }

            inverseEvidenceSet(targets,columnIndex);
            buildPrefixEviSet();
            Instant start = Instant.now();
                TopKSet topKSet = new TopKSet(50, columnIndex);
                for(var afds : AFDSets.get(columnIndex).getAFDs()){
                    for(var afd : afds){
                        topKSet.insert(afd,getUtility2(afd.getThresholdsIndexes(),afd.getRIndex(),columnIndex));
                    }
                }
                topKSets.add(topKSet);

            Duration duration = Duration.between(start, Instant.now());
            miniTime += duration.toMillis();

            //printout
//            System.out.println("column " + columnIndex + " completed.");
        }
        AFDSet minimal = new AFDSet();
        int totalCount = 0;

            for(var afds : topKSets){
                for(AFD afd : afds.getAFDs()){
                    minimal.directlyAdd(afd);
                    totalCount ++;
                }
            }

        System.out.println(totalCount);
        System.out.println("miniteTime: " + miniTime);
        System.out.println("hitTime: " + hitTime);
        System.out.println("walkTime: " + walkTime);
        return topKSets;
    }

    public List<List<Evidence>> generateSortedEvidences(int columnIndex){
        List<List<Evidence>> sortedEvidence = new ArrayList<>();
        for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
            sortedEvidence.add(new ArrayList<>());
        }
        for(Evidence evi : evidenceSet.getEvidenceSet()){
            if(evi.getPredicateIndex(columnIndex) == maxIndexes.get(columnIndex) - 1)continue;
            sortedEvidence.get(evi.getPredicateIndex(columnIndex)).add(evi);
        }
        return sortedEvidence;
    }

    public List<Long> generateTargets(List<List<Evidence>> sortedEvidences, double threshold){
        List<Long> targets = new ArrayList<>();
        long tpCounts = (long) rowNumber * (rowNumber - 1) / 2;
        long target = tpCounts -  (long) Math.floor(((double) tpCounts * threshold));
//        System.out.println("need to satisfied:" + (tpCounts - target));
        long totalCount = 0;
        for(var evidences : sortedEvidences){
            for(var evidence : evidences)
                totalCount += evidence.getCount();
            targets.add((target + totalCount - tpCounts));

        }
//        System.out.println(targets.get(0) );
        return targets;
    }

    public void inverseEvidenceSet( List<Long> targets, int columnIndex){
        Stack<SearchNode> nodes = new Stack<>();
        List<Integer> list = new ArrayList<>(Collections.nCopies(columnNumber, 0));
        AFDCandidate candidate = new AFDCandidate(list, columnIndex);

        for(int RIndex = 0; RIndex < maxIndexes.get(columnIndex) - 1; RIndex ++){
            if(targets.get(RIndex) <= 0){
                List<Integer> left = new ArrayList<>(candidate.leftThresholdsIndexes);
                left.set(columnIndex, RIndex);
                AFD afd = new AFD(left, columnIndex);
                AFDSets.get(columnIndex).add(afd);
                targets.set(RIndex,  (long) rowNumber * rowNumber / 2);

                if(RIndex == maxIndexes.get(columnIndex) - 2)return;
            }
        }

        List<AFDCandidate> candidates = new ArrayList<>();
        candidates.add(candidate);
        List<Integer> limitThreshold = new ArrayList<>();
        for(int i = 0; i < columnNumber; i++){
            limitThreshold.add(maxIndexes.get(i));
        }
        walk(0,nodes,candidates,limitThreshold,targets,columnIndex);

        while(!nodes.isEmpty()){
            SearchNode nd = nodes.pop();
            if(nd.e >= evidencesArray.length)continue;
            Instant start = Instant.now();
            hit(nd);
            Duration duration = Duration.between(start, Instant.now());
            hitTime += duration.toMillis();
            start = Instant.now();
            if(! isNoUse(nd))
                walk(nd.e + 1, nodes,nd.candidates,nd.limitThresholds,nd.remainCounts,columnIndex);
            duration = Duration.between(start, Instant.now());
            walkTime += duration.toMillis();
        }
    }

    void walk(int e, Stack<SearchNode> nodes,List<AFDCandidate> AFDCandidates, List<Integer> limitThresholds, List<Long> targets, int columnIndex){
//        System.out.println(limitThresholds);
        while (e < evidencesArray.length && !AFDCandidates.isEmpty()) {
            Evidence evi = evidencesArray[e];
            int RStage = evi.getPredicateIndex(columnIndex);
            if(intevalIds.contains((long) e) && e > 0){
                int lastRstage = evidencesArray[e - 1].getPredicateIndex(columnIndex);
                    if(targets.get(lastRstage) > 0 && targets.get(lastRstage) != (long) rowNumber * rowNumber / 2)
                        return;
            }


            List<AFDCandidate> unhitCand = generateUnhitCand(e,AFDCandidates);

            //cover the evidence;
            if(!isOutOfLimit(evi.getPredicateIndex(),limitThresholds,columnIndex)) {
                List<AFDCandidate> copy = new ArrayList<>();
                for(AFDCandidate afdCandidates : AFDCandidates){
                    copy.add(afdCandidates.copy());
                }
//                System.out.println("e:" + e + "AFDCAND   :" + copy.size() +"unhited:" );

                SearchNode node = new SearchNode(e, copy, new ArrayList<>(targets), new ArrayList<>(unhitCand), new ArrayList<>(limitThresholds), columnIndex);
                nodes.add(node);
            }

            if(nog1Error)
                return;
            //unhit evidence
            if(unhitCand.isEmpty())return;
            getAnd(limitThresholds, evi.getPredicateIndex());
            if(isEmpty(limitThresholds))return;
            if(!isApproxCover(e + 1,evi.getPredicateIndex(),RStage,targets.get(RStage),columnIndex))return;

            List<AFDCandidate> newCandidates = new ArrayList<>();
            for(AFDCandidate cand : unhitCand){
                if(!isOutOfLimit(cand.leftThresholdsIndexes, limitThresholds, columnIndex)){
                    newCandidates.add(cand);
                }
                else
                    for(int RIndex = RStage; RIndex < maxIndexes.get(columnIndex) - 1; RIndex ++){
                        if(!AFDSets.get(columnIndex).containsSubset(cand.leftThresholdsIndexes,columnIndex) && isApproxCover(e + 1,cand.leftThresholdsIndexes,RIndex,targets.get(RIndex),columnIndex)){
                            List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                            left.set(columnIndex, RIndex);
                            AFD afd = new AFD(left, columnIndex);
                            AFDSets.get(columnIndex).add(afd);
                        }
                    else break;
                    }
            }

            if(isEmpty(limitThresholds))return;
            if(newCandidates.isEmpty())return;
            e++;
            AFDCandidates = newCandidates;
        }
        //
    }

    void hit(SearchNode nd){
        if(nd.e >= evidencesArray.length)return;
        if(nd.candidates.isEmpty() && nd.unhitCand.isEmpty()) return;
        //deal with the target count

        List<AFDCandidate> candidates = nd.candidates;
        Evidence evi = evidencesArray[nd.e];
        int RStage = evi.getPredicateIndex(nd.columnIndex);

        List<Integer> canCover = new ArrayList<>();
        List<Integer> unCover = new ArrayList<>();
        for(int index = RStage; index < nd.remainCounts.size(); index++){
            if(nd.remainCounts.get(index) == (long) rowNumber * rowNumber / 2)continue;
            if(nd.sub(index, evi.getCount()) <= 0)
                canCover.add(index);
            else unCover.add(index);
        }

        if(canCover.size() > 0){
            for(Integer rightThresholdIndex  : canCover){
                for(var cand : candidates){
                    List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                    left.set(nd.columnIndex, rightThresholdIndex);
                    AFD afd = new AFD(left, nd.columnIndex);
                    AFDSets.get(nd.columnIndex).add((afd));
                }
            }
            if(nog1Error)return;
        }

        for(AFDCandidate invalid : nd.unhitCand){
            List<Integer> left = evi.getPredicateIndex();
            List<Integer> limit = nd.limitThresholds;
            for(int i = 0; i < columnNumber; i++){
                if(i == nd.columnIndex)continue;
                if(left.get(i) < limit.get(i) - 1){
                    for(int rightThresholdIndex : canCover){
                        List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                        afdCand.set(nd.columnIndex, rightThresholdIndex);
                        afdCand.set(i, left.get(i) + 1);
                        if(!AFDSets.get(nd.columnIndex).containsSubset(afdCand,rightThresholdIndex)){
                            AFD afd = new AFD(afdCand,nd.columnIndex);
                            AFDSets.get(nd.columnIndex).add(afd);
                        }
                        nd.remainCounts.set(rightThresholdIndex, (long) rowNumber * rowNumber / 2);
                    }
                    for(int rightThresholdIndex : unCover) {
                        List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                        afdCand.set(i, left.get(i) + 1);
                        if(!listCanCover( afdCand, candidates,nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(afdCand,rightThresholdIndex)){
                            if(!isEmpty(afdCand)){
                                candidates.add(new AFDCandidate(afdCand, nd.columnIndex));
                                break;
                            }
                            else if(isApproxCover(nd.e + 1,afdCand,rightThresholdIndex,nd.remainCounts.get(rightThresholdIndex),nd.columnIndex))
                            {
                                AFD afd = new AFD(afdCand,nd.columnIndex);
                                AFDSets.get(nd.columnIndex).add(afd);
                            }
                            else break;
                        }
                        else break;
                    }
                }
            }
        }

    }

    //find all candidates that can't include the e ( make all value pairs similar )
    List<AFDCandidate> generateUnhitCand(int e, List<AFDCandidate> AFDCandidates){
        List<AFDCandidate> unhited = new ArrayList<>();
        List<Integer> eviIndexes = evidencesArray[e].getPredicateIndex();
        for(var cand : AFDCandidates){
            List<Integer> candIndexes = cand.leftThresholdsIndexes;
            boolean flag = true;
            for(int i = 0; i < columnNumber; i++){
                if(i == cand.rColumnIndex)continue;
                if(eviIndexes.get(i) < candIndexes.get(i)) {
                    flag = false;
                    break;
                }
            }
            if(flag) unhited.add(cand);
        }
        for(var cand : unhited){
            AFDCandidates.remove(cand);
        }
//        System.out.println("e:" + e + "AFDCAND:" + AFDCandidates.size() +"unhited:" + unhited.size());


        return unhited;
    }

    //[limitThreshold]: -1 represents for no limit, with the limitIndex increasing, the demarcation decrease. As an example [-1,0,1,2] can represent for [3,1,0]
    boolean isOutOfLimit(List<Integer> indexes, List<Integer> limitThresholds, int columnIndex){
        for(int i = 0; i < columnNumber; i++){
            if(i == columnIndex)continue;
            if(indexes.get(i) <= limitThresholds.get(i))
                return false;
        }
        return true;
    }

    boolean canCover(List<Integer> target, List<Integer> currentIdx, int columnIndex){
        for(int i = 0; i < columnNumber; i++){
            if(i == columnIndex)continue;
            if(target.get(i) < currentIdx.get(i)) {
                return true;
            }
        }
        return false;
    }

    boolean listCanCover(List<Integer> target, List<AFDCandidate> candidates, int columnIndex){
        for(AFDCandidate afdCandidate : candidates){
            boolean flag = false;
            List<Integer> currentIdx = afdCandidate.leftThresholdsIndexes;
            for(int i = 0; i < columnNumber; i++){
                if(i == columnIndex)continue;
                if(target.get(i) < currentIdx.get(i)) {
                    flag = true;
                    break;
                }
            }
            if(!flag) return true;
        }
        return false;
    }

    private  Evidence[] flattenAndConvert(List<List<Evidence>> nestedList) {
        List<Evidence> flatList = new ArrayList<>();

        for (List<Evidence> sublist : nestedList) {
            flatList.addAll(sublist);
        }

        return flatList.toArray(new Evidence[0]);
    }

    boolean isApproxCover(int e, List<Integer> cand, int RIndex, long target, int columnIndex){

        if(target <= 0) return true;
        if(target == (long) rowNumber * rowNumber / 2) return false;
        for(; e < intevalIds.get(RIndex); e++){
            if(canCover(evidencesArray[e].getPredicateIndex(),cand, columnIndex)){
                target -= evidencesArray[e].getCount();
                if(target <= 0)return true;
            }
        }
        return false;
    }

    public boolean isEmpty(List<Integer> indexes){
        for (Integer index : indexes) {
            if (index != 0) return false;
        }
        return true;
//        return false;
    }

    void getAnd(List<Integer> A, List<Integer> B){
        for(int i = 0; i < A.size(); i++){
            A.set(i, Math.min(A.get(i),B.get(i) + 1));
        }
    }

    boolean isNoUse(SearchNode nd){
        if(nd.e >= evidencesArray.length)return true;
        for(int i = evidencesArray[nd.e].getPredicateIndex(nd.columnIndex); i < nd.remainCounts.size(); i++){
            if(nd.remainCounts.get(i) != (long) rowNumber * rowNumber / 2)return false;
        }
//        System.out.println("out");

        return true;
    }

    double getUtility( List<Integer> curIdx, int Rindex, int columnIndex){
        long support = 0;
        for(int i = 0; i < evidenceSet.getEvidenceSet().size(); i++){
            Evidence evi = evidenceSet.getEvidenceSet().get(i);
            boolean flag = true;
            List<Integer> targetIdx = evi.getPredicateIndex();
            for(int j = 0; j < columnNumber; j++){
                if(j == columnIndex)continue;
                if(targetIdx.get(j) < curIdx.get(j)) {
                    flag = false;
                    break;
                }
            }
            if(flag) support += evi.getCount();
        }

        if(support == 0)return 0;

        int Lindexes = 0;

        for(int i = 0; i < columnNumber; i++){
            if(i == columnIndex)continue;
            Lindexes += curIdx.get(i) + 1;
        }

        return  (Rindex + 1) * Math.log(support) / Lindexes;
    }

    double getUtility2( List<Integer> curIdx, int Rindex, int columnIndex){
//        long support = 0;
//        for(int i = 0; i < evidenceSet.getEvidenceSet().size(); i++){
//            Evidence evi = evidenceSet.getEvidenceSet().get(i);
//            boolean flag = true;
//            List<Integer> targetIdx = evi.getPredicateIndex();
//            for(int j = 0; j < columnNumber; j++){
//                if(j == columnIndex)continue;
//                if(targetIdx.get(j) < curIdx.get(j)) {
//                    flag = false;
//                    break;
//                }
//            }
//            if(flag) support += evi.getCount();
//        }

        long support = 0;
        RoaringBitmap mp = new RoaringBitmap();
        mp.add(0, evidenceSet.getEvidenceSet().size());
        for(int i = 0; i < curIdx.size(); i++){
            if(i == columnIndex)continue;
            mp.and(prefixEviSet.get(i).get(curIdx.get(i)));
        }
        for(int index : mp){
            support += evidenceSet.getEvidenceSet().get(index).getCount();
        }

        if(support == 0)return 0;

        int Lindexes = 0;

        for(int i = 0; i < columnNumber; i++){
            if(i == columnIndex)continue;
            Lindexes += curIdx.get(i) + 1;
        }

        return  (Rindex + 1) * Math.log(support) / Lindexes;
    }

}
