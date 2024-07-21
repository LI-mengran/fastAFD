package FastRFD.REI;

import FastRFD.RFD.RFD;
import FastRFD.RFD.RFDSet;
import FastRFD.evidence.Evidence;
import FastRFD.evidence.EvidenceSet;
import FastRFD.predicates.PredicatesBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Stack;

public class REIwithTopK {

        boolean nog1Error = true;
        private final int columnNumber;
        private int rowNumber;
        private List<Integer> maxIndexes;
        private EvidenceSet evidenceSet;
        private List<Long> originTarget;
        private Evidence[] evidencesArray;
        private Long [] eviprefixCount;
        List<List<RoaringBitmap>> prefixEviSet ;
        //Attention that some rIndexes set may be empty
        private List<Long> intevalIds;
        List<TopKSet> topKSets = new ArrayList<>();
        long miniTime;
        long hitTime;
        long walkTime;
        long addTime;
        int topK;
        boolean cut = true;

        TopKSet topKSet;

        public REIwithTopK(PredicatesBuilder pBuilder, int rowNumber, EvidenceSet evidenceSet, boolean nog1Error, int topK){
            this.columnNumber = pBuilder.getColumnNumber();
            this.rowNumber = rowNumber;
            this.evidenceSet = evidenceSet;
            this.maxIndexes = pBuilder.getMaxPredicateIndexByColumn();
            this.nog1Error = nog1Error;
            this.topK = topK;
            prefixEviSet = new ArrayList<>();
            buildPrefixEviSet();
        }

        public List<TopKSet> buildTopK(double threshold){
            for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
                List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
                intevalIds = new ArrayList<>();
                topKSet = new TopKSet(topK,columnIndex);
                for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
                    intevalIds.add(0L);
                }
                for(var splitEvidences : sortedEvidences){
                    splitEvidences.sort((o1, o2) -> Long.compare(o2.getCount(), o1.getCount()));
                    if(splitEvidences.isEmpty())continue;
                    intevalIds.set(splitEvidences.get(0).getPredicateIndex(columnIndex), (long) splitEvidences.size());
                }
                for(int i = 1; i < maxIndexes.get(columnIndex) - 1; i++){
                    intevalIds.set(i, intevalIds.get(i - 1) + intevalIds.get(i));
                }

                List<Long> targets = generateTargets(sortedEvidences, threshold);
                originTarget = targets;
                evidencesArray = flattenAndConvert(sortedEvidences);
                inverseEvidenceSet(targets,columnIndex);
                topKSets.add(topKSet);
//                System.out.println(topKSet.curK);
            }
            RFDSet minimal = new RFDSet();
            int totalCount = 0;
            for(var afds : topKSets){
                for(RFD afd : afds.getAFDs()){
                    minimal.directlyAdd(afd);
                    totalCount ++;
                }
            }
            //printout
//            for(int i = 0; i < columnNumber; i++){
//                System.out.println(i);
//                for(var afds : minimal.getMinimalAFDs()){
//                    if(afds.getColumnIndex() == i)
//                        System.out.println(afds.getThresholdsIndexes());
//                }
//            }


            System.out.println(totalCount);
            System.out.println("miniteTime: " + miniTime);
            System.out.println("hitTime: " + hitTime);
            System.out.println("walkTime: " + walkTime);
            System.out.println("addTime: " + addTime);
            return topKSets;
        }

         List<List<Evidence>> generateSortedEvidences(int columnIndex){
            List<List<Evidence>> sortedEvidence = new ArrayList<>();
            for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
                sortedEvidence.add(new ArrayList<>());
            }
            for(Evidence evi : evidenceSet.getEvidenceSet()){
                if(evi.getPredicateIndex(columnIndex) == maxIndexes.get(columnIndex) - 1) {
                    continue;
                }
                sortedEvidence.get(evi.getPredicateIndex(columnIndex)).add(evi);
            }
            return sortedEvidence;
        }

        public void buildPrefixEviSet(){
            for(int i = 0; i < columnNumber; i++){
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

        public List<Long> generateTargets(List<List<Evidence>> sortedEvidences, double threshold){
            List<Long> targets = new ArrayList<>();
            List<Long> prefixCount = new ArrayList<>();
            long tpCounts = (long) rowNumber * (rowNumber - 1) / 2;
            long target = tpCounts -  (long) Math.floor(((double) tpCounts * threshold));
//        System.out.println("need to satisfied:" + (tpCounts - target));
            long totalCount = 0;
            for(var evidences : sortedEvidences){
                for(var evidence : evidences){
                    prefixCount.add(totalCount);
                    totalCount += evidence.getCount();
                }
                targets.add((target + totalCount - tpCounts));
                this.eviprefixCount = prefixCount.toArray(new Long[0]);
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
                    RFD afd = new RFD(left, columnIndex);
                    topKSet.insertWithUpdate(afd, targets.size() * columnNumber * Math.log( (double) (rowNumber * rowNumber) / 2));
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

//            int count = 0;
            while(!nodes.isEmpty()){
                SearchNode nd = nodes.pop();
                Instant start = Instant.now();
                hit(nd);
                Duration duration = Duration.between(start, Instant.now());
                hitTime += duration.toMillis();
                start = Instant.now();
                if(! isNoUse(nd)){
//                    count++;
                    walk(nd.e + 1, nodes,nd.candidates,nd.limitThresholds,nd.remainCounts,columnIndex);

                }
                duration = Duration.between(start, Instant.now());
                walkTime += duration.toMillis();
            }
//            System.out.println("walkCount: " + count);
        }

        void walk(int e, Stack<SearchNode> nodes,List<AFDCandidate> AFDCandidates, List<Integer> limitThresholds, List<Long> targets, int columnIndex){
//        System.out.println(limitThresholds);
            while (e < evidencesArray.length && !AFDCandidates.isEmpty()) {
                Evidence evi = evidencesArray[e];
                if(intevalIds.contains((long) e) && e > 0){
                    int lastRstage = evidencesArray[e - 1].getPredicateIndex(columnIndex);
                    if(targets.get(lastRstage) > 0 && targets.get(lastRstage) != (long) rowNumber * rowNumber / 2)
                        return;
                }

                if(targets.get(targets.size() - 1)  < 0 || targets.get(targets.size() - 1) ==  (long) rowNumber * rowNumber / 2)
                    return;


                //简单剪枝
//                List<AFDCandidate> candidates = new ArrayList<>();
//                for(var cand : AFDCandidates){
//                    if(getApproxUtility( cand.leftThresholdsIndexes, targets.get(evi.getPredicateIndex(columnIndex)), targets.size() - 1) >= topKSet.getMin())
//                        candidates.add(cand);
//                }
//                AFDCandidates = candidates;
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
                int lowR = 0;
                while(targets.get(lowR) == (long) rowNumber * rowNumber / 2)lowR++;
                if(eviprefixCount[Math.toIntExact(intevalIds.get(lowR)) - 1] - eviprefixCount[e] < targets.get(lowR) || !isApproxCover(e + 1,limitThresholds ,lowR,targets.get(lowR),columnIndex))
                    return;

//                List<Integer> thre = new ArrayList<>();
//                for(Integer i : limitThresholds)
//                    thre.add(i - 1);
//                if(getUtility( thre,targets.size() - 1, columnIndex) < topKSet.getMin())
//                    return;


                List<AFDCandidate> newCandidates = new ArrayList<>();

                for(AFDCandidate cand : unhitCand){
                    if(isRunOutSearchSpace(cand.leftThresholdsIndexes, limitThresholds, columnIndex)){
                        int maxFitIndex =  maxSatisfied(e + 1, cand.leftThresholdsIndexes, lowR, targets, columnIndex);
                        if(maxFitIndex != -1 && !topKSet.containsSubset(cand.leftThresholdsIndexes,maxFitIndex)){
                            List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                            left.set(columnIndex, maxFitIndex);
                            RFD afd = new RFD(left, columnIndex);
                            Instant start = Instant.now();
                            topKSet.insertWithUpdate(afd,getUtility(left,maxFitIndex,columnIndex));
                            Duration duration = Duration.between(start, Instant.now());
                            addTime += duration.toMillis();

                        }

                    }
                    else
                        newCandidates.add(cand);
                }

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
            int maxCanCover = canCover.isEmpty() ? 0 : canCover.get(canCover.size() - 1) ;


            if(!canCover.isEmpty()){
                for(Integer rightThresholdIndex  : canCover){
                    nd.remainCounts.set(rightThresholdIndex, (long) rowNumber * rowNumber / 2);
                }
                for(var cand : candidates){
                    List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                    left.set(nd.columnIndex, maxCanCover);
                    RFD afd = new RFD(left, nd.columnIndex);
                    Instant start = Instant.now();
                    topKSet.insertWithUpdate(afd, getUtility( left, canCover.get(canCover.size() - 1),nd.columnIndex));
                    Duration duration = Duration.between(start, Instant.now());
                    addTime += duration.toMillis();
                }
            }

            List<AFDCandidate> newResult = new ArrayList<>();
            for(AFDCandidate invalid : nd.unhitCand){
                List<Integer> left = evi.getPredicateIndex();
                List<Integer> limit = nd.limitThresholds;
                for(int i = 0; i < columnNumber; i++){
                    if(i == nd.columnIndex)continue;
                    if(left.get(i) < limit.get(i) - 1){
                        if(!canCover.isEmpty()){
                            List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                            afdCand.set(nd.columnIndex, maxCanCover);
                            afdCand.set(i, left.get(i) + 1);
                            if(!listContainsSubset(afdCand, newResult, nd.columnIndex) && !topKSet.containsSubset(afdCand,maxCanCover)){
                                checkMin(afdCand, newResult, nd.columnIndex);
                                newResult.add(new AFDCandidate(afdCand, nd.columnIndex));
                            }
                        }
                    }
                }
            }
            if(unCover.isEmpty()){
                for(var afdcand : newResult){
                    RFD afd = new RFD(afdcand.leftThresholdsIndexes,nd.columnIndex);
//                Instant start = Instant.now();
//                    AFDSets.get(nd.columnIndex).add(afd);
                    topKSet.insertWithUpdate(afd,getUtility(afd.getThresholdsIndexes(),maxCanCover ,nd.columnIndex));

//                Duration duration = Duration.between(start, Instant.now());
//                addTime += duration.toMillis();
                }
                return;
            }

            for(AFDCandidate invalid : nd.unhitCand){
                List<Integer> left = evi.getPredicateIndex();
                List<Integer> limit = nd.limitThresholds;
                for(int i = 0; i < columnNumber; i++) {
                    if (i == nd.columnIndex) continue;
                    if (left.get(i) < limit.get(i) - 1) {
                        List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                        afdCand.set(i, left.get(i) + 1);
//                        boolean needSearch = false;
//                        if (!listCanCover(afdCand, candidates, nd.columnIndex)) {
////                            for (int rightThresholdIndex : unCover) {
////                                if (!AFDSets.get(nd.columnIndex).containsSubset(afdCand, rightThresholdIndex)) {
//                                    needSearch = true;
//                                    break;
////                                }
////                            }
//                        }
                        if (!listContainsSubset(afdCand, candidates, nd.columnIndex) && !topKSet.containsSubset(afdCand,unCover.get(unCover.size() - 1))) {
//                            if (!runOutSearchSpace(afdCand, nd.limitThresholds, nd.columnIndex)) {
                            checkMin(afdCand, candidates, nd.columnIndex);
                            candidates.add(new AFDCandidate(afdCand, nd.columnIndex));
//                            }
                        }
                    }
                }
            }
            for(AFDCandidate afdCand : candidates){
                if (isRunOutSearchSpace(afdCand.leftThresholdsIndexes, nd.limitThresholds, nd.columnIndex)) {
                    List<Integer> cand = afdCand.leftThresholdsIndexes;
                    int maxFitIndex = maxSatisfied(nd.e + 1, cand, unCover.get(0), nd.remainCounts, nd.columnIndex);
                    if (maxFitIndex != -1 && !listContainsSubset(cand, newResult, nd.columnIndex) && !topKSet.containsSubset(cand,maxFitIndex)) {//
                        cand.set(nd.columnIndex, maxFitIndex);
                        checkMin(cand, newResult, nd.columnIndex);
                        newResult.add(new AFDCandidate(cand, nd.columnIndex));
                    }
                    candidates.remove(afdCand);
                    break;
                }
            }
            for(var afdcand : newResult){
                RFD afd = new RFD(afdcand.leftThresholdsIndexes,nd.columnIndex);
                Instant start = Instant.now();
//                AFDSets.get(nd.columnIndex).add(afd);
                topKSet.insertWithUpdate(afd,getUtility(afd.getThresholdsIndexes(), afd.getRIndex(),nd.columnIndex));
                Duration duration = Duration.between(start, Instant.now());
                addTime += duration.toMillis();
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
            AFDCandidates.removeAll(unhited);


            return unhited;
        }


    int maxSatisfied(int e, List<Integer> cand, int RIndex, List<Long> target, int columnIndex){
//        if(RIndex < target.size() && (target.get(RIndex) <= 0 || target.get(RIndex) == (long) rowNumber * rowNumber / 2)) RIndex++;
        List<Long> tmpTarget = new ArrayList<>(target);
//        if(RIndex == tmpTarget.size())return -1;
        long len = intevalIds.get(intevalIds.size() - 1);
//        System.out.println(RIndex);
        boolean f = isApproxCover(e,cand,RIndex, target.get(0),columnIndex );
        for(; e < len; e++){
            if(canCover(evidencesArray[e].getPredicateIndex(),cand, columnIndex)){
                for(int ind =  evidencesArray[e].getPredicateIndex(columnIndex); ind < target.size(); ind ++){
                    tmpTarget.set(ind, tmpTarget.get(ind) - evidencesArray[e].getCount());
                }
            }
        }
        for(int ind = tmpTarget.size() - 1 ;  ind >= RIndex; ind --){
            if(tmpTarget.get(ind) <= 0) {
                return ind;
            }
        }
        return -1;
    }
        //[limitThreshold]: -1 represents for no limit, with the limitIndex increasing, the demarcation decrease. As an example [-1,0,1,2] can represent for [3,1,0]
        boolean isOutOfLimit(List<Integer> indexes, List<Integer> limitThresholds, int columnIndex){
            for(int i = 0; i < columnNumber; i++){
                if(i == columnIndex)continue;
                if(indexes.get(i) < limitThresholds.get(i) - 1)
                    return false;
            }
            return true;
        }



    boolean isRunOutSearchSpace(List<Integer> indexes, List<Integer> limitThresholds, int columnIndex){
        boolean pFlag = true;
        for(int index = 0; index < columnNumber; index++){
            if(index == columnIndex) continue;
            if(indexes.get(index) < limitThresholds.get(index) - 1){
                pFlag = false;
                break;
            }
        }
        return pFlag;
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

    boolean isSubset(List<Integer> target, List<Integer> currentIdx, int columnIndex){
        for(int i = 0; i < columnNumber; i++){
            if(i == columnIndex)continue;
            if(target.get(i) < currentIdx.get(i)) {
                return false;
            }
        }
        return true;
    }

        private  Evidence[] flattenAndConvert(List<List<Evidence>> nestedList) {
            List<Evidence> flatList = new ArrayList<>();

            for (List<Evidence> sublist : nestedList) {
                flatList.addAll(sublist);
            }

            return flatList.toArray(new Evidence[0]);
        }

    boolean isApproxCover(int e, List<Integer> cand, int RIndex, long target, int columnIndex){

//        System.out.println("target " + target);
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

            return true;
        }

    boolean listContainsSubset(List<Integer> target, List<AFDCandidate> candidates, int columnIndex){
        for(AFDCandidate afdCandidate : candidates){
            List<Integer> currentIdx = afdCandidate.leftThresholdsIndexes;
            if(isSubset(target,currentIdx,columnIndex))return true;
        }
        return false;
    }

    void checkMin(List<Integer> target, List<AFDCandidate> candidates, int columnIndex){
        List<AFDCandidate> removeSet = new ArrayList<>();
        for(AFDCandidate afdCandidate : candidates){
            List<Integer> currentIdx = afdCandidate.leftThresholdsIndexes;
            if(isSubset(currentIdx,target,columnIndex)) removeSet.add(afdCandidate);
        }
        candidates.removeAll(removeSet);
    }

    double getUtility( List<Integer> curIdx, int Rindex, int columnIndex){

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

        return   ((Rindex + 1) /(double) (maxIndexes.get(columnIndex) + 1))  * ((double) support / ((double) (rowNumber * (rowNumber - 1)) / 2)) / Lindexes;
    }

    double getApproxUtility(List<Integer> curIdx, long remainCount, int Rindex){
        long support = (long) rowNumber * (rowNumber - 1) / 2 - originTarget.get(Rindex) + remainCount ;

        int Lindexes = 0;

        for(int i = 0; i < columnNumber; i++){
            Lindexes += curIdx.get(i) + 1;
        }

        return rowNumber * (Rindex + 1) * Math.log(support) / Lindexes;
    }
}
