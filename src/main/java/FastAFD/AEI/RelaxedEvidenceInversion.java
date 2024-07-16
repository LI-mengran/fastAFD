package FastAFD.AEI;

import FastAFD.AFD.RFD;
import FastAFD.AFD.RFDSet;
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
    private int topKk = 10;
    private List<Integer> maxIndexes;
    private EvidenceSet evidenceSet;
    private Evidence [] evidencesArray;
    private Long [] eviprefixCount;
    //Attention that some rIndexes set may be empty
    List<List<RoaringBitmap>> prefixEviSet = new ArrayList<>();

    long tpCounts;

    private List<Long> intevalIds;
    List<RFDSet> AFDSets = new ArrayList<>();
    List<TopKSet> topKSets = new ArrayList<>();
    long miniTime;
    long hitTime;
    long walkTime;
    long addTime;
    boolean isSeperate = false;
    int seperateId;
    long intevalId;

    public RelaxedEvidenceInversion(PredicatesBuilder pBuilder, int rowNumber, EvidenceSet evidenceSet, boolean nog1Error){
        this.columnNumber = pBuilder.getColumnNumber();
        this.rowNumber = rowNumber;
        this.evidenceSet = evidenceSet;
        this.evidenceNumber = evidenceSet.getEvidenceSet().size();
        this.maxIndexes = pBuilder.getMaxPredicateIndexByColumn();
        this.nog1Error = nog1Error;
        for(int i = 0; i < columnNumber; i++){
            AFDSets.add(new RFDSet(i, maxIndexes.get(i)));
        }
    }

    public void buildPrefixEviSet(){
        for(int i = 0; i < maxIndexes.size(); i++){
            prefixEviSet.add(new ArrayList<>());
            for(int j = 0; j < maxIndexes.get(i); j++){
                prefixEviSet.get(i).add(new RoaringBitmap());
            }
        }
        for(int k = 0; k < evidenceNumber; k++){
            Evidence evi = evidenceSet.getEvidenceSet().get(k);
            List<Integer> EPI = evi.getPredicateIndex();
            for(int i = 0; i < EPI.size(); i++){
                for(int j = 0; j <= EPI.get(i); j++)
                    prefixEviSet.get(i).get(j).add(k);
            }
        }
    }

    public RFDSet buildRFD(double threshold){
//        if(threshold == 0)return null;
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
            intevalIds = new ArrayList<>();
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
            evidencesArray = flattenAndConvert(sortedEvidences);
            List<Long> targets = generateTargets(sortedEvidences, threshold);
            inverseEvidenceSet(targets,columnIndex);
            Instant start = Instant.now();

//            AFDSets.get(columnIndex).minimize();
            Duration duration = Duration.between(start, Instant.now());
            miniTime += duration.toMillis();

        }
        RFDSet minimal = new RFDSet();
        int totalCount = 0;

        for(var AFDs : AFDSets){
            for(var afds :  AFDs.getAFDs()){
                for(var afd : afds){
                    minimal.directlyAdd(afd);
                    totalCount++;
                }
            }
//            for(var afd : AFDs.getMinimalAFDs()){
//                minimal.directlyAdd(afd);
//            }
//            totalCount += AFDs.minimalCount();
        }

        //printout
//        for(int i = 0; i < columnNumber; i++){
//            System.out.println(i);
//            for(var afds : minimal.getMinimalAFDs()){
//                if(afds.getColumnIndex() == i)
//                    System.out.println(afds.getThresholdsIndexes());
//            }
//        }

        System.out.println("[RFD number]: " + totalCount);
        System.out.println("miniteTime: " + miniTime);
        System.out.println("hitTime: " + hitTime);
        System.out.println("walkTime: " + walkTime);
//        System.out.println("addTime: " + addTime);
        return minimal;
    }
    public RFDSet buildRFDSep(double threshold){
//        if(threshold == 0)return null;
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
            intevalIds = new ArrayList<>();
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
            evidencesArray = flattenAndConvert(sortedEvidences);
            List<Long> targets = generateTargets(sortedEvidences, threshold);
            for(int i = 0; i < intevalIds.size(); i++){
                seperateId = i;
                intevalId = intevalIds.get(i);
                inverseEvidenceSetSep(targets.get(i),columnIndex);
            }

            Instant start = Instant.now();
            AFDSets.get(columnIndex).minimize();
            Duration duration = Duration.between(start, Instant.now());
            miniTime += duration.toMillis();

        }
        RFDSet minimal = new RFDSet();
        int totalCount = 0;

        for(var AFDs : AFDSets){
//            for(var afds :  AFDs.getAFDs()){
//                for(var afd : afds){
//                    minimal.directlyAdd(afd);
//                    totalCount++;
//                }
//            }
            for(var afd : AFDs.getMinimalAFDs()){
                minimal.directlyAdd(afd);
            }
            totalCount += AFDs.minimalCount();
        }

        //printout
//        for(int i = 0; i < columnNumber; i++){
//            System.out.println(i);
//            for(var afds : minimal.getMinimalAFDs()){
//                if(afds.getColumnIndex() == i)
//                    System.out.println(afds.getThresholdsIndexes());
//            }
//        }

        System.out.println("[RFD number]: " + totalCount);
        System.out.println("miniteTime: " + miniTime);
        System.out.println("hitTime: " + hitTime);
        System.out.println("walkTime: " + walkTime);
        return minimal;
    }
    public List<TopKSet> buildTopK(double threshold, int _k){
        topKk = _k;
//        if(threshold == 0)return null;
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            List<List<Evidence>> sortedEvidences = generateSortedEvidences(columnIndex);
            intevalIds = new ArrayList<>();
            for(int i = 0; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.add(0L);
            }
            for(var splitEvidences : sortedEvidences){
                splitEvidences.sort((o2, o1) -> Long.compare(o1.getCount(), o2.getCount()));
                if(splitEvidences.isEmpty())continue;
                intevalIds.set(splitEvidences.get(0).getPredicateIndex(columnIndex), (long) splitEvidences.size());
            }
            for(int i = 1; i < maxIndexes.get(columnIndex) - 1; i++){
                intevalIds.set(i, intevalIds.get(i - 1) + intevalIds.get(i));
            }

            List<Long> targets = generateTargets(sortedEvidences, threshold);
            evidencesArray = flattenAndConvert(sortedEvidences);
            inverseEvidenceSet(targets,columnIndex);
            buildPrefixEviSet();
            Instant start = Instant.now();
//            AFDSets.get(columnIndex).minimize();
            TopKSet topKSet = new TopKSet(topKk, columnIndex);

            TreeMap<Double, List<RFD>> uMap = new TreeMap<>();
            for(var afds : AFDSets.get(columnIndex).getAFDs()){
                for(var afd : afds){
                    double utility = getUtility2(afd.getThresholdsIndexes(), afd.getRIndex(), columnIndex);
                    uMap.computeIfAbsent(utility, k -> new ArrayList<>()).add(afd);
//                    topKSet.insert(afd,getUtility2(afd.getThresholdsIndexes(),afd.getRIndex(),columnIndex));
                }
            }

            // 从TreeMap的最后一个键开始遍历，直到找到前k大的AFD
            for (Map.Entry<Double, List<RFD>> entry : uMap.descendingMap().entrySet()) {
                for (RFD afd : entry.getValue()) {
                    if (topKSet.curK < topKk) {
                        topKSet.add(afd, entry.getKey());
                    } else {
                        break;
                    }
                }
                if (topKSet.curK >= topKk) {
                    break;
                }
            }
            topKSets.add(topKSet);

            Duration duration = Duration.between(start, Instant.now());
            miniTime += duration.toMillis();

            //printout
//            System.out.println("column " + columnIndex + " completed.");
        }
        RFDSet minimal = new RFDSet();
        int totalCount = 0;

            for(var afds : topKSets){
                for(RFD afd : afds.getAFDs()){
                    minimal.directlyAdd(afd);
                    totalCount ++;
                }
            }

        System.out.println("[RFD number]: " + totalCount);
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
        List<Long> prefixCount = new ArrayList<>();
        tpCounts = (long) rowNumber * (rowNumber - 1) / 2;
        long target = -  (long) Math.floor(((double) tpCounts * threshold));
//        System.out.println("need to satisfied:" + (tpCounts - target));
        long totalCount = 0;
        for(var evidences : sortedEvidences){
            for(var evidence : evidences){
                totalCount += evidence.getCount();
                prefixCount.add(totalCount);
            }
            targets.add((target + totalCount ));
            this.eviprefixCount = prefixCount.toArray(new Long[0]);
        }

//        System.out.println(targets.get(0) );
        return targets;
    }

    public void inverseEvidenceSet( List<Long> targets, int columnIndex){
        Stack<SearchNode> nodes = new Stack<>();
        List<Integer> list = new ArrayList<>(Collections.nCopies(columnNumber, 0));
        AFDCandidate candidate = new AFDCandidate(list, columnIndex);
        for(int RIndex = 0; RIndex < targets.size(); RIndex ++){
            if(targets.get(RIndex) <= 0){
                List<Integer> left = new ArrayList<>(candidate.leftThresholdsIndexes);
                if(!isSeperate)
                    left.set(columnIndex, RIndex);
                else left.set(columnIndex, seperateId);
                RFD afd = new RFD(left, columnIndex);
                AFDSets.get(columnIndex).add(afd);
                targets.set(RIndex,  (long) rowNumber * rowNumber / 2);

                if(RIndex == targets.size() - 1)return;
            }
        }

        List<AFDCandidate> candidates = new ArrayList<>();
        candidates.add(candidate);
        List<Integer> limitThreshold = new ArrayList<>();
        for(int i = 0; i < columnNumber; i++){
            limitThreshold.add(maxIndexes.get(i));
        }
        walk(0,nodes,candidates,limitThreshold,targets,columnIndex);

//        int count = 0;
        while(!nodes.isEmpty()){
            SearchNode nd = nodes.pop();
            Instant start = Instant.now();
            hit(nd);
            Duration duration = Duration.between(start, Instant.now());
            hitTime += duration.toMillis();
            start = Instant.now();
            if(! isNoUse(nd)){
//                count++;
                walk(nd.e + 1, nodes,nd.candidates,nd.limitThresholds,nd.remainCounts,columnIndex);
            }
            duration = Duration.between(start, Instant.now());
            walkTime += duration.toMillis();
        }
//        System.out.println("walkCounts: "+ count);
    }
    public void inverseEvidenceSetSep(Long targets, int columnIndex){
        Stack<SearchNode> nodes = new Stack<>();
        List<Integer> list = new ArrayList<>(Collections.nCopies(columnNumber, 0));
        AFDCandidate candidate = new AFDCandidate(list, columnIndex);
        if(targets <= 0){
            List<Integer> left = new ArrayList<>(candidate.leftThresholdsIndexes);
            left.set(columnIndex, seperateId);
            RFD afd = new RFD(left, columnIndex);
            AFDSets.get(columnIndex).add(afd);
            return;
        }
        List<AFDCandidate> candidates = new ArrayList<>();
        candidates.add(candidate);
        List<Integer> limitThreshold = new ArrayList<>();
        for(int i = 0; i < columnNumber; i++){
            limitThreshold.add(maxIndexes.get(i));
        }

        walkSep(0,nodes,candidates,limitThreshold,targets,columnIndex);

        while(!nodes.isEmpty()){
            SearchNode nd = nodes.pop();
            Instant start = Instant.now();
            hitSep(nd);
            Duration duration = Duration.between(start, Instant.now());
            hitTime += duration.toMillis();
            start = Instant.now();
            walkSep(nd.e + 1, nodes,nd.candidates,nd.limitThresholds,nd.remainCounts.get(0),columnIndex);
            duration = Duration.between(start, Instant.now());
            walkTime += duration.toMillis();
        }
    }

    void walk(int e, Stack<SearchNode> nodes,List<AFDCandidate> AFDCandidates, List<Integer> limitThresholds, List<Long> targets, int columnIndex){
//        System.out.println(seperateId);
        while (e < evidencesArray.length && !AFDCandidates.isEmpty()) {
            Evidence evi = evidencesArray[e];
            if(intevalIds.contains((long) e) && e > 0){
                int lastRstage = evidencesArray[e - 1].getPredicateIndex(columnIndex);
                    if(targets.get(lastRstage) > 0 && targets.get(lastRstage) != (long) rowNumber * rowNumber / 2)
                        return;
            }

            if(targets.get(targets.size() - 1)  < 0 || targets.get(targets.size() - 1) ==  (long) rowNumber * rowNumber / 2)
                return;


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

            List<AFDCandidate> newCandidates = new ArrayList<>();

            for(AFDCandidate cand : unhitCand){
                if(runOutSearchSpace(cand.leftThresholdsIndexes, limitThresholds, columnIndex)){
//                    int maxFitIndex = -1;
//                    for(int RIndex = lowR; RIndex < maxIndexes.get(columnIndex) - 1; RIndex ++){
////                        if(!AFDSets.get(columnIndex).containsSubset(cand.leftThresholdsIndexes, RIndex) ){
//                            if(isApproxCover(e + 1,cand.leftThresholdsIndexes,RIndex,targets.get(RIndex),columnIndex))
//                                maxFitIndex = RIndex;
//                            else break;
////                        }
//                    }
                    int maxFitIndex =  maxSatisfied(e + 1, cand.leftThresholdsIndexes, lowR, targets, columnIndex);
//                    Instant start = Instant.now();
                    if(maxFitIndex != -1 && !AFDSets.get(columnIndex).containsSubset(cand.leftThresholdsIndexes,maxFitIndex)){
                        List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                        left.set(columnIndex, maxFitIndex);
                        RFD afd = new RFD(left, columnIndex);
                        AFDSets.get(columnIndex).add(afd);
//                        Duration duration = Duration.between(start, Instant.now());
//                        addTime += duration.toMillis();
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
    void walkSep(int e, Stack<SearchNode> nodes, List<AFDCandidate> AFDCandidates, List<Integer> limitThresholds, long targets, int columnIndex){
//        System.out.println(seperateId);
        while (e < intevalId && !AFDCandidates.isEmpty()) {
            Evidence evi = evidencesArray[e];
            if(targets < 0)
                return;

            List<AFDCandidate> unhitCand = generateUnhitCand(e,AFDCandidates);

            //cover the evidence;
            if(!isOutOfLimit(evi.getPredicateIndex(),limitThresholds,columnIndex)) {
                List<AFDCandidate> copy = new ArrayList<>();
                for(AFDCandidate afdCandidates : AFDCandidates){
                    copy.add(afdCandidates.copy());
                }
//                System.out.println("e:" + e + "AFDCAND   :" + copy.size() +"unhited:" );

                List<Long> tar = new ArrayList<>();
                tar.add(targets);
                SearchNode node = new SearchNode(e, copy, tar, new ArrayList<>(unhitCand), new ArrayList<>(limitThresholds), columnIndex);
                nodes.add(node);
            }

            //unhit evidence
            if(unhitCand.isEmpty())return;
            getAnd(limitThresholds, evi.getPredicateIndex());

            List<AFDCandidate> newCandidates = new ArrayList<>();

            for(AFDCandidate cand : unhitCand){
                if(runOutSearchSpace(cand.leftThresholdsIndexes, limitThresholds, columnIndex)){
                    if(isApproxCover(e + 1, cand.leftThresholdsIndexes,seperateId,targets,columnIndex) && !AFDSets.get(columnIndex).containsSubset(cand.leftThresholdsIndexes, seperateId )){
                        List<Integer> left = new ArrayList<>(cand.leftThresholdsIndexes);
                        left.set(columnIndex, seperateId);
                        RFD afd = new RFD(left, columnIndex);
//                        Instant start = Instant.now();
                        AFDSets.get(columnIndex).add(afd);
//                        Duration duration = Duration.between(start, Instant.now());
//                        addTime += duration.toMillis();
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
                if(AFDSets.get(nd.columnIndex).containsSubset(cand.leftThresholdsIndexes, maxCanCover))continue;
                List<Integer> afdCand = new ArrayList<>(cand.leftThresholdsIndexes);
                afdCand.set(nd.columnIndex, maxCanCover);
                RFD afd = new RFD(afdCand, nd.columnIndex);
//                Instant start = Instant.now();
                AFDSets.get(nd.columnIndex).add(afd);
//                Duration duration = Duration.between(start, Instant.now());
//                addTime += duration.toMillis();
            }
        }


        List<AFDCandidate> newResult = new ArrayList<>();
        for(AFDCandidate invalid : nd.unhitCand) {
            List<Integer> left = evi.getPredicateIndex();
            List<Integer> limit = nd.limitThresholds;
            for (int i = 0; i < columnNumber; i++) {
                if (i == nd.columnIndex) continue;
                if(left.get(i) < limit.get(i) - 1){
                    if(!canCover.isEmpty()){
                        List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                        afdCand.set(nd.columnIndex, maxCanCover);
//                        System.out.println(seperateId);
                        afdCand.set(i, left.get(i) + 1);
                        if(!listContainsSubset(afdCand, newResult, nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(afdCand, maxCanCover)){// &&
//                            RFD afd = new RFD(afdCand,nd.columnIndex);
                            checkMin(afdCand, newResult, nd.columnIndex);
                            newResult.add(new AFDCandidate(afdCand, nd.columnIndex));
//                            AFDSets.get(nd.columnIndex).add(afd);
                        }
                    }
                }
            }
        }

        if(unCover.isEmpty()){
            for(var afdcand : newResult){
                RFD afd = new RFD(afdcand.leftThresholdsIndexes,nd.columnIndex);
//                Instant start = Instant.now();
                AFDSets.get(nd.columnIndex).add(afd);
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
                        if (!listContainsSubset(afdCand, candidates, nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(afdCand, unCover.get(unCover.size() - 1))) {
//                            if (!runOutSearchSpace(afdCand, nd.limitThresholds, nd.columnIndex)) {
                                checkMin(afdCand, candidates, nd.columnIndex);
                                candidates.add(new AFDCandidate(afdCand, nd.columnIndex));
//                            }
                        }
                    }
                }
        }
        for(AFDCandidate afdCand : candidates){
            if (runOutSearchSpace(afdCand.leftThresholdsIndexes, nd.limitThresholds, nd.columnIndex)) {
                List<Integer> cand = afdCand.leftThresholdsIndexes;
                int maxFitIndex = maxSatisfied(nd.e + 1, cand, unCover.get(0), nd.remainCounts, nd.columnIndex);
                if (maxFitIndex != -1 && !listContainsSubset(cand, newResult, nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(cand,maxFitIndex)) {//
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
            AFDSets.get(nd.columnIndex).add(afd);
            Duration duration = Duration.between(start, Instant.now());
            addTime += duration.toMillis();
        }
    }
    void hitSep(SearchNode nd){
        if(nd.e >= intevalId)return;
        if(nd.candidates.isEmpty() && nd.unhitCand.isEmpty()) return;

        List<AFDCandidate> candidates = nd.candidates;
        Evidence evi = evidencesArray[nd.e];

        Long count = nd.sub(0, evi.getCount());
        if(count <= 0){
            for(var cand : candidates){
//                if(AFDSets.get(nd.columnIndex).containsSubset(cand.leftThresholdsIndexes, seperateId))continue;
                List<Integer> afdCand = new ArrayList<>(cand.leftThresholdsIndexes);
                afdCand.set(nd.columnIndex, seperateId);
                RFD afd = new RFD(afdCand, nd.columnIndex);
                AFDSets.get(nd.columnIndex).add(afd);
            }
            for(AFDCandidate invalid : nd.unhitCand) {
                List<Integer> left = evi.getPredicateIndex();
                List<Integer> limit = nd.limitThresholds;
                for (int i = 0; i < columnNumber; i++) {
                    if (i == nd.columnIndex) continue;
                    if(left.get(i) < limit.get(i) - 1){
                        List<Integer> afdCand = new ArrayList<>(invalid.leftThresholdsIndexes);
                        afdCand.set(nd.columnIndex, seperateId);
                        afdCand.set(i, left.get(i) + 1);
                        if( !AFDSets.get(nd.columnIndex).containsSubset(afdCand,seperateId)){// &&
                        RFD afd = new RFD(afdCand,nd.columnIndex);
                        AFDSets.get(nd.columnIndex).add(afd);
                        }
                    }
                }
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
                    if (!listContainsSubset(afdCand, candidates, nd.columnIndex) && !AFDSets.get(nd.columnIndex).containsSubset(afdCand,seperateId) ) {
                        checkMin(afdCand, candidates, nd.columnIndex);
                        candidates.add(new AFDCandidate(afdCand, nd.columnIndex));
                    }
                }
            }
        }
        for(AFDCandidate afdCand : candidates){
            if (runOutSearchSpace(afdCand.leftThresholdsIndexes, nd.limitThresholds, nd.columnIndex)) {
                List<Integer> cand = afdCand.leftThresholdsIndexes;
                if (isApproxCover(nd.e + 1, cand,seperateId,nd.remainCounts.get(0), nd.columnIndex) ) {
                    cand.set(nd.columnIndex, seperateId);
                    RFD afd = new RFD(cand,nd.columnIndex);
                    AFDSets.get(nd.columnIndex).add(afd);
                }
                candidates.remove(afdCand);
                break;
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
        AFDCandidates.removeAll(unhited);
//        System.out.println("e:" + e + "AFDCAND:" + AFDCandidates.size() +"unhited:" + unhited.size());


        return unhited;
    }

    List<AFDCandidate> minimize(List<AFDCandidate> candidates, int columnIndex){
        List<AFDCandidate>  newCandidates = new ArrayList<>();
        for(var cand : candidates){
            if(!listContainsSubset(cand.leftThresholdsIndexes, newCandidates, columnIndex)){
                checkMin(cand.leftThresholdsIndexes, newCandidates, columnIndex);
                newCandidates.add(cand);
            }
        }
        return newCandidates;
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

    boolean runOutSearchSpace(List<Integer> indexes, List<Integer> limitThresholds, int columnIndex){
        boolean pFlag = true;
        for(int index = 0; index < columnNumber; index++){
            if(index == columnIndex) continue;
            if(indexes.get(index) < limitThresholds.get(index) - 1 ){
                pFlag = false;
                break;
            }
        }
//        if(pFlag)
//            System.out.println("ds");
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

    int maxSatisfied(int e, List<Integer> cand, int RIndex, List<Long> target, int columnIndex){
//        if(RIndex < target.size() && (target.get(RIndex) <= 0 || target.get(RIndex) == (long) rowNumber * rowNumber / 2)) RIndex++;
        List<Long> tmpTarget = new ArrayList<>(target);
//        if(RIndex == tmpTarget.size())return -1;
        long len = intevalIds.get(intevalIds.size() - 1);
//        System.out.println(RIndex);
        boolean f = isApproxCover(e,cand,RIndex, target.get(0),columnIndex );
        for(; e < len; e++){
            if(canCover(evidencesArray[e].getPredicateIndex(),cand, columnIndex)){
                for(int ind = isSeperate ? 0 : evidencesArray[e].getPredicateIndex(columnIndex); ind < target.size(); ind ++){
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

    public boolean isEmpty(List<Integer> indexes){
        for (Integer index : indexes) {
            if (index != 1) return false;
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
        if(nd.candidates.isEmpty())return true;
        for(int i = evidencesArray[nd.e].getPredicateIndex(nd.columnIndex); i < nd.remainCounts.size(); i++){
            if(nd.remainCounts.get(i) != (long) rowNumber * rowNumber / 2)return false;
        }
//        System.out.println("out");

        return true;
    }

    double getUtility( List<Integer> curIdx, int Rindex, int columnIndex){
        long support = 0;
        for(int i = 0; i < evidenceNumber; i++){
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

        long support = 0;
        RoaringBitmap mp = new RoaringBitmap();
        mp.add(0, evidenceNumber);
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
