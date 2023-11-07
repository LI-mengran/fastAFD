package FastAFD.AEI;

import FastAFD.AFD.AFD;
import FastAFD.AFD.AFDSet;
import FastAFD.evidence.Evidence;
import FastAFD.evidence.EvidenceSet;
import FastAFD.predicates.PredicatesBuilder;

import java.util.*;
import java.util.stream.Collectors;

public class RelaxedEvidenceInversion {
    private final int columnNumber;
    private final int evidenceNumber;
    private int rowNumber;
    private List<Integer> maxIndexes;
    private EvidenceSet evidenceSet;
    private Evidence [] evidencesArray;
    //Attention that some rIndexes set may be empty
    private List<Integer> intevalIds;
    List<AFDSet> AFDSets = new ArrayList<>();

    public RelaxedEvidenceInversion(PredicatesBuilder pBuilder, int rowNumber, EvidenceSet evidenceSet){
        this.columnNumber = pBuilder.getColumnNumber();
        this.rowNumber = rowNumber;
        this.evidenceSet = evidenceSet;
        this.evidenceNumber = evidenceSet.getEvidenceSet().size();
        this.maxIndexes = pBuilder.getMaxPredicateIndexByColumn();
        for(int i = 0; i < columnNumber; i++){
            AFDSets.add(new AFDSet(i, maxIndexes.get(i)));
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
                intevalIds.add(0);
            }
            for(var splitEvidences : sortedEvidences){
                Collections.sort(splitEvidences, (o1, o2) -> Long.compare(o1.getCount(), o2.getCount()));
                if(splitEvidences.size() == 0)continue;
                intevalIds.set(splitEvidences.get(0).getPredicateIndex(columnIndex), splitEvidences.size());
            }
            for(int i = 1; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.set(i, intevalIds.get(i - 1) + intevalIds.get(i));
            }
            inverseEvidenceSet(targets,columnIndex);
        }
        AFDSet minimal = new AFDSet();
        int totalCount = 0;
        for(var afds : AFDSets){
            List<AFD> Afds = afds.minimize();
//            for(AFD afd : afds.minimize())
//                minimal.directlyAdd(afd);
            totalCount += Afds.size();

        }
        System.out.println(totalCount);
        return minimal;
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
            targets.add((long)(target + totalCount - tpCounts));

        }
        return targets;
    }

    public void inverseEvidenceSet( List<Long> targets, int columnIndex){
        Stack<SearchNode> nodes = new Stack<>();
        List<Integer> list = new ArrayList<>(Collections.nCopies(columnNumber, 0));
        list.set(columnIndex, -1);
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
//            System.out.println(nd.e);
            if(nd.e >= evidencesArray.length)continue;
            hit(nd);
            if(! isNoUse(nd))
                walk(nd.e + 1, nodes,nd.candidates,nd.limitThresholds,nd.remainCounts,columnIndex);
        }
    }

    void walk(int e, Stack<SearchNode> nodes,List<AFDCandidate> AFDCandidates, List<Integer> limitThresholds, List<Long> targets, int columnIndex){
        while (e < evidencesArray.length && !AFDCandidates.isEmpty()) {
            Evidence evi = evidencesArray[e];
            int RStage = evi.getPredicateIndex(columnIndex);

            List<AFDCandidate> unhitCand = generateUnhitCand(e,AFDCandidates, columnIndex);

            //cover the evidence;
            if(!isOutOfLimit(evi.getPredicateIndex(),limitThresholds,columnIndex)) {
                List<AFDCandidate> copy = new ArrayList<>();
                for(AFDCandidate afdCandidates : AFDCandidates){
                    copy.add(afdCandidates.copy());
                }
                SearchNode node = new SearchNode(e, copy, new ArrayList<>(targets), new ArrayList<>(unhitCand), new ArrayList<>(limitThresholds), columnIndex);
                nodes.add(node);
            }

//            SearchNode node = new SearchNode()
            //unhit evidence
            if(unhitCand.isEmpty())return;
            getAnd(limitThresholds, evi.getPredicateIndex());
            if(isEmpty(limitThresholds))return;

            boolean canHit = false;
            for(int rIndex = RStage; rIndex < maxIndexes.get(columnIndex) - 1; rIndex++){
                    if(!isApproxCover(e + 1,evi.getPredicateIndex(),rIndex,targets.get(rIndex), columnIndex)){
                        targets.set(rIndex, (long) rowNumber * rowNumber / 2);
                    }
                    else canHit = true;
            }
            if(!canHit)return;

            List<AFDCandidate> newCandidates = new ArrayList<>();
            for(AFDCandidate cand : unhitCand){
                if(!isOutOfLimit(cand.leftThresholdsIndexes, limitThresholds, columnIndex)){
                    newCandidates.add(cand);
                }
                else
                    for(int RIndex = RStage; RIndex < maxIndexes.get(columnIndex) - 1; RIndex ++)
                        if(!AFDSets.get(columnIndex).containsSubset(cand.leftThresholdsIndexes,columnIndex) && isApproxCover(e + 1,cand.leftThresholdsIndexes,RIndex,targets.get(RIndex),columnIndex)){
                            List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                            left.set(columnIndex, RIndex);
                            AFD afd = new AFD(left, columnIndex);
                            AFDSets.get(columnIndex).add(afd);
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
                        if(!listCanCover( afdCand, candidates, nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(afdCand,rightThresholdIndex)){
                            if(!isEmpty(afdCand)){
                                candidates.add(new AFDCandidate(afdCand, nd.columnIndex));
                            }
                            else if(isApproxCover(nd.e + 1,afdCand,nd.columnIndex,nd.remainCounts.get(rightThresholdIndex), nd.columnIndex))
                            {
                                AFD afd = new AFD(afdCand,nd.columnIndex);
                                AFDSets.get(nd.columnIndex).add(afd);
                            }
                        }
                    }
                }
            }
        }

    }

    //find all candidates that can't include the e ( make all value pairs similar )
    List<AFDCandidate> generateUnhitCand(int e, List<AFDCandidate> AFDCandidates, int columnIndex){
        List<AFDCandidate> unhited = new ArrayList<>();
        List<Integer> eviIndexes = evidencesArray[e].getPredicateIndex();
        for(var cand : AFDCandidates){
            List<Integer> candIndexes = cand.leftThresholdsIndexes;
            boolean flag = true;
            for(int i = 0; i < columnNumber; i++){
                if(i == columnIndex)continue;;
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
            if(i == columnIndex)continue;;
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
        for(int i = 0; i < indexes.size(); i++){
            if(indexes.get(i) != -1)return false;
        }
        return true;
    }

    List<Integer> getAnd(List<Integer> A, List<Integer> B){
        for(int i = 0; i < A.size(); i++){
            A.set(i, Math.min(A.get(i),B.get(i) + 1));
        }
        return A;
    }

    boolean isNoUse(SearchNode nd){
        if(nd.e >= evidencesArray.length)return true;
        for(int i = evidencesArray[nd.e].getPredicateIndex(nd.columnIndex); i < nd.remainCounts.size(); i++){
            if(nd.remainCounts.get(i) != (long) rowNumber * rowNumber / 2)return false;
        }
        return true;
    }

}
