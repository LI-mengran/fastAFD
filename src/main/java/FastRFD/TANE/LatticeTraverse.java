package FastRFD.TANE;

import FastRFD.evidence.Evidence;
import FastRFD.evidence.EvidenceSet;
import FastRFD.input.Input;
import FastRFD.input.ParsedColumn;
import FastRFD.pli.PliBuilder;
import FastRFD.predicates.PredicatesBuilder;
import it.unimi.dsi.fastutil.Pair;
import org.roaringbitmap.RoaringBitmap;

import java.math.BigDecimal;
import java.util.*;

import static FastRFD.Utils.calculateEditDistance;

public class LatticeTraverse {
    private final int rowNumber;
    private final double threshold;
    private final int columnNumber;

    List<List<RoaringBitmap>> prefixEviSet;
    EvidenceSet evidenceSet;
    PredicatesBuilder pBuilder;
    PliBuilder pliBuilder;

    List<List<Candidate>> RFDSet;
    List<Candidate> checked;
    List<ParsedColumn<?>> pColumns;

    HashMap<Pair<String, String>, Integer> disCache = new HashMap<>();

    boolean useCache = true;

    boolean usePrefix = true;

    public LatticeTraverse(PredicatesBuilder pBuilder, int rowNumber, EvidenceSet evidenceSet, double threshold) {
        this.rowNumber = rowNumber;
        this.evidenceSet = evidenceSet;
        this.pBuilder = pBuilder;
        this.threshold = threshold;
        this.columnNumber = pBuilder.getColumnNumber();
        prefixEviSet = new ArrayList<>();
        buildPrefixEviSet();
    }

    public LatticeTraverse(PredicatesBuilder pBuilder, int rowNumber, PliBuilder pliBuilder, double threshold, Input input, boolean useCache) {
        this.rowNumber = rowNumber;
        this.pBuilder = pBuilder;
        this.pliBuilder = pliBuilder;
        this.threshold = threshold;
        this.columnNumber = pBuilder.getColumnNumber();
        this.pColumns = input.getParsedColumns();
        this.useCache = useCache;
        usePrefix = false;
    }

    public void findRFD() {
        long error = generateThreshold();
        RFDSet = new ArrayList<>();
        for (int i = 0; i < columnNumber; i++) {
            RFDSet.add(new ArrayList<>());

        }
        int totalCount = 0;
        for (int i = 0; i < columnNumber; i++) {
            checked = new ArrayList<>();
            List<Integer> emptyLhs = new ArrayList<>(Collections.nCopies(columnNumber, 0));
            emptyLhs.set(i, pBuilder.getMaxPredicateIndexByColumn().get(i) - 1);
            Candidate root = new Candidate(emptyLhs, pBuilder.getMaxPredicateIndexByColumn().get(i), i);
            List<Candidate> root1 = new ArrayList<>();
            root1.add(root);
            tranverse(error, root1);
            RFDSet.set(i, minimize(RFDSet.get(i),i));
            totalCount += RFDSet.get(i).size();
//            System.out.println(i);
//            for(var cand : RFDSet.get(i)){
//
//                System.out.println(cand.getLhsIndex());
//            }
        }
        System.out.println("Total RFD:" + totalCount);

    }

    public void tranverse(long error, Candidate candidate) {
        checked.add(candidate);
        if(usePrefix){
            if (validate(error, candidate)) {
//            System.out.println(candidate.getLhsIndex());
                RFDSet.get(candidate.getColumnIndex()).add(candidate);
                return;
            }
        }
        else  if (validate2(error, candidate)) {
//            System.out.println(candidate.getLhsIndex());
            RFDSet.get(candidate.getColumnIndex()).add(candidate);
            return;
        }

        if (reachLastRFD(candidate)) return;
        List<Integer> indexes = candidate.getLhsIndex();
        for (int i = 0; i < indexes.size(); i++) {
            if (i == candidate.getColumnIndex()) {
                if(indexes.get(i) <= 1)continue;
                List<Integer> newIndexes = new ArrayList<>(indexes);
                newIndexes.set(i, newIndexes.get(i) - 1);
                Candidate cr = new Candidate(newIndexes, newIndexes.get(candidate.columnIndex), candidate.columnIndex);
                if(!isChecked(cr) && !containsSubset(cr))
                    tranverse(error, cr);
            }
            else{
                if (indexes.get(i) == pBuilder.getMaxPredicateIndexByColumn().get(i) - 1) continue;
                List<Integer> newIndexes = new ArrayList<>(indexes);
                newIndexes.set(i, newIndexes.get(i) + 1);
                Candidate cl = new Candidate(newIndexes, newIndexes.get(candidate.columnIndex), candidate.columnIndex);
                if(!isChecked(cl) && !containsSubset(cl))
                    tranverse(error, cl);
            }
        }

    }

    public void tranverse(long error, List<Candidate> candidates) {
        List<Candidate> toBeRemoved = new ArrayList<>();
        for(var candidate : candidates){
            if(usePrefix){
                if ( validate(error, candidate) ) {
//                    if(!validate2(error,candidate))
////                        if(candidate.getLhsIndex().get(4) != 2)
//                        System.out.println(candidate.columnIndex);
                    RFDSet.get(candidate.getColumnIndex()).add(candidate);
                    toBeRemoved.add(candidate);
                }
                else if (reachLastRFD(candidate)) toBeRemoved.add(candidate);
            }
            else {
                if ( validate2(error, candidate) ) {
                    RFDSet.get(candidate.getColumnIndex()).add(candidate);
                    toBeRemoved.add(candidate);
                }
                else if (reachLastRFD(candidate)) toBeRemoved.add(candidate);
            }

        }

        if(!toBeRemoved.isEmpty())
            candidates.removeAll(toBeRemoved);
        if(candidates.isEmpty()) return;

        List<Candidate> newCandidates = new ArrayList<>();
        for(var candidate : candidates){
            List<Integer> indexes = candidate.getLhsIndex();
            for (int i = 0; i < indexes.size(); i++) {
                if (i == candidate.getColumnIndex()) {
                    if(indexes.get(i) <= 1)continue;
                    List<Integer> newIndexes = new ArrayList<>(indexes);
                    newIndexes.set(i, newIndexes.get(i) - 1);
                    Candidate cr = new Candidate(newIndexes, newIndexes.get(candidate.columnIndex), candidate.columnIndex);
                    if(!containsSubset(cr))
                        newCandidates.add(cr);
                }
                else{
                    if (indexes.get(i) == pBuilder.getMaxPredicateIndexByColumn().get(i) - 1) continue;
                    List<Integer> newIndexes = new ArrayList<>(indexes);
                    newIndexes.set(i, newIndexes.get(i) + 1);
                    Candidate cl = new Candidate(newIndexes, newIndexes.get(candidate.columnIndex), candidate.columnIndex);
                    if(!containsSubset(cl) && ! containsSubset(cl, newCandidates))
                        newCandidates.add(cl);
                }
            }
        }

        tranverse(error, newCandidates);
    }

    public Candidate cup(Candidate cand1, Candidate cand2, int columnIndex){
        List<Integer> indexes1 = cand1.getLhsIndex();
        List<Integer> indexes2 = cand2.getLhsIndex();
        List<Integer> newIndexes = new ArrayList<>(indexes2);
        for(int i = 0; i < indexes1.size(); i++){
            if(i == columnIndex) newIndexes.set(i, Math.min(indexes1.get(i), indexes2.get(i)));
            else newIndexes.set(i, Math.max(indexes1.get(i), indexes2.get(i)));
        }
        return new Candidate(newIndexes, newIndexes.get(columnIndex), columnIndex);

    }
    public boolean isChecked(Candidate candidate){
        for(var check : checked){
            List<Integer> p1 = candidate.getLhsIndex();
            List<Integer> p2 = check.getLhsIndex();
            boolean flag = true;
            for(int index = 0; index < p1.size(); index++){
                if(!Objects.equals(p1.get(index), p2.get(index))) {
                    flag = false;
                    break;
                }
            }
            if(flag)return true;
        }
        return false;
    }


    public boolean containsSubset( Candidate candidate){
        int columnIndex = candidate.columnIndex;
        List<Candidate> CSet = RFDSet.get(columnIndex);
        for(var c : CSet){
            if(candidate.getRIndex() <= c.getRIndex())
                if (canCover(candidate.getLhsIndex(), c.getLhsIndex(), columnIndex)) {
                    return true;
                }
        }
        return false;
    }

    public boolean containsSubset( Candidate candidate, List<Candidate> candidates){
        int columnIndex = candidate.columnIndex;
        for(var c : candidates){
            if(candidate.getRIndex() <= c.getRIndex())
                if (canCover(candidate.getLhsIndex(), c.getLhsIndex(), columnIndex)) {
                    return true;
                }
        }
        return false;
    }
    public boolean reachLastRFD(Candidate candidate) {
        List<Integer> indexes = candidate.getLhsIndex();
        for (int i = 0; i < indexes.size(); i++) {
            if (i == candidate.getColumnIndex()) {
                if (indexes.get(i) != 1) return false;
            }
            if (!Objects.equals(indexes.get(i), pBuilder.getMaxPredicateIndexByColumn().get(i) - 1)) return false;
        }
        return true;
    }

    public void buildPrefixEviSet() {
        List<Integer> maxIndexes = pBuilder.getMaxPredicateIndexByColumn();
        for (int i = 0; i < maxIndexes.size(); i++) {
            prefixEviSet.add(new ArrayList<>());
            for (int j = 0; j < maxIndexes.get(i); j++) {
                prefixEviSet.get(i).add(new RoaringBitmap());
            }
        }
        for (int k = 0; k < evidenceSet.getEvidenceSet().size(); k++) {
            Evidence evi = evidenceSet.getEvidenceSet().get(k);
            List<Integer> EPI = evi.getPredicateIndex();
            for (int i = 0; i < EPI.size(); i++) {
                for (int j = 0; j <= EPI.get(i); j++)
                    prefixEviSet.get(i).get(j).add(k);
            }
        }
    }

    public int generateThreshold() {
        long tpCounts = (long) rowNumber * (rowNumber - 1) / 2;
        return (int) Math.floor(((double) tpCounts * threshold));
    }

    public boolean validate(long error, Candidate candidate) {
        if(candidate.getRIndex() == 0)return false;
        RoaringBitmap lhs = new RoaringBitmap();
        List<Integer> indexes = candidate.getLhsIndex();
        lhs.add(0, evidenceSet.getEvidenceSet().size());
        for (int i = 0; i < indexes.size(); i++) {
            if (i == candidate.getColumnIndex()) continue;
            lhs.and(prefixEviSet.get(i).get(indexes.get(i)));
        }
        lhs.andNot(prefixEviSet.get(candidate.getColumnIndex()).get(indexes.get(candidate.getColumnIndex())));
        long count = 0;
        for(int index : lhs){
            count += evidenceSet.getEvidenceSet().get(index).getCount();
        }
//        if(candidate.columnIndex == 4)
//            System.out.println(count + " for method 1 \n");
        return count <= error;
    }

    public boolean validate2(long error, Candidate candidate){
        int count = 0;
        for(int i = 0; i < rowNumber; i++){
            for(int j = i + 1; j < rowNumber; j++){
                if(pColumns.get(candidate.columnIndex).isNum()){
                    BigDecimal distance = BigDecimal.valueOf(getDoubleValue(pColumns.get(candidate.columnIndex).getValue(i))).subtract(BigDecimal.valueOf( getDoubleValue(pColumns.get(candidate.columnIndex).getValue(j)))).abs();
                    if(distance.doubleValue() <= pBuilder.getDemarcationsByColumn(candidate.columnIndex).get(candidate.getRIndex() - 1)){
                        continue;
                    }
                }
                else{
                    String a = (String) pColumns.get(candidate.columnIndex).getValue(i);
                    String b = (String) pColumns.get(candidate.columnIndex).getValue(j);
                    double maxDis = pBuilder.getDemarcationsByColumn(candidate.columnIndex).get(candidate.getRIndex() - 1).intValue();
                    if(useCache){
                        int dis;
                        if(disCache.containsKey(Pair.of(a,b)) ){
                            dis = disCache.get(Pair.of(a,b));
                        }
                        else if(disCache.containsKey(Pair.of(b,a))){
                            dis = disCache.get(Pair.of(b,a));
                        }
                        else{
                            dis = calculateEditDistance(a,b);
                            disCache.put(Pair.of(a,b), dis);
                        }
//                    if(pColumns.get(candidate.columnIndex).getValue(i).toString().length() == pColumns.get(candidate.columnIndex).getValue(j).toString().length())continue;
//                    System.out.println(calculateEditDistance((String) pColumns.get(candidate.columnIndex).getValue(100), (String) pColumns.get(candidate.columnIndex).getValue(101)));
                        if(dis <= maxDis){
                            continue;
                        }
                    }
                    else {
                        int dis = calculateEditDistance(a,b);
//                    if(pColumns.get(candidate.columnIndex).getValue(i).toString().length() == pColumns.get(candidate.columnIndex).getValue(j).toString().length())continue;
//                    System.out.println(calculateEditDistance((String) pColumns.get(candidate.columnIndex).getValue(100), (String) pColumns.get(candidate.columnIndex).getValue(101)));
                        if(dis <= maxDis){
                            continue;
                        }
                    }
//                    else if(i == 50)
//                        System.out.println("c");
                }
                boolean flag = false;
                for(int k = 0; k < columnNumber; k++){
                    if(k == candidate.columnIndex)continue;
                    if(candidate.getLhsIndex().get(k) == 0)continue;
                    if(pColumns.get(k).isNum()){
                        BigDecimal distance = BigDecimal.valueOf(getDoubleValue(pColumns.get(k).getValue(i))).subtract(BigDecimal.valueOf( getDoubleValue(pColumns.get(k).getValue(j)))).abs();
                        if(distance.doubleValue() > pBuilder.getDemarcationsByColumn(k).get(candidate.getLhsIndex().get(k) - 1)){
                            flag = true;
                            break;
                        }
                    }
                    else {
                        String a = (String) pColumns.get(k).getValue(i);
                        String b = (String) pColumns.get(k).getValue(j);
                        int maxDis = pBuilder.getDemarcationsByColumn(k).get(candidate.getLhsIndex().get(k) - 1).intValue();
                        if(useCache){
                            int dis;
                            if(disCache.containsKey(Pair.of(a,b)) ){
                                dis = disCache.get(Pair.of(a,b));
                            }
                            else if(disCache.containsKey(Pair.of(b,a))){
                                dis = disCache.get(Pair.of(b,a));
                            }
                            else{
                                dis = calculateEditDistance(a,b);
                                disCache.put(Pair.of(a,b), dis);
                            }
//                    if(pColumns.get(candidate.columnIndex).getValue(i).toString().length() == pColumns.get(candidate.columnIndex).getValue(j).toString().length())continue;
//                    System.out.println(calculateEditDistance((String) pColumns.get(candidate.columnIndex).getValue(100), (String) pColumns.get(candidate.columnIndex).getValue(101)));
                            if(dis > maxDis){
                                flag = true;
                                break;
                            }
                        }
                        else{
                            int dis = calculateEditDistance(a, b);
                            if(dis > maxDis){
//                        if(pColumns.get(k).getValue(i).toString().length() != pColumns.get(k).getValue(j).toString().length()){
                                flag = true;
                                break;
                            }
                        }

                    }
                }
                if(!flag) count += 1;
            }
        }
//        if(candidate.columnIndex == 4)
//            System.out.println(count + " for method 2 \n");
        return count <= error;
    }

    public List<Candidate> minimize(List<Candidate> RFDs, int columnIndex) {

//        RFDs.sort(Comparator.comparingInt(Candidate::getRIndex));

        List<Candidate> results = new ArrayList<>();

        for(var cand : RFDs) {
            // Now, 'afds' contains a list of RFD objects in reverse order
            boolean flag = true;
            List<Candidate> removeSet = new ArrayList<>();
            for (Candidate result : results) {
//
                if(cand.getRIndex() <= result.getRIndex()) {
                    if (canCover(cand.getLhsIndex(), result.getLhsIndex(), columnIndex)) {
                        flag = false;
                        break;
                    }
                }
                if(cand.getRIndex() >= result.getRIndex() && canCover(result.getLhsIndex(), cand.getLhsIndex(), columnIndex)) {
                    removeSet.add(result);
                    }
            }
            if (flag) {
                results.add(cand);
                for(var rm : removeSet){
//                    System.out.println(rm.getLhsIndex());
                    results.remove(rm);
                }

            }
        }
        return results;
    }

    boolean canCover(List<Integer> target, List<Integer> currentIdx, int columnIndex){
        for(int i = 0; i < target.size(); i++){
            if(i == columnIndex) {
                continue;
            }
            if(target.get(i) < currentIdx.get(i)) {
                return false;
            }
        }
        return true;
    }

    public double getDoubleValue(Object number){
        if (number instanceof Double)
            return (Double) number;
        else if (number instanceof Integer) {
            return ((Integer) number).doubleValue();
        }
        else return 0;
    }

    public List<List<Candidate>> getRFDSet(){
        return RFDSet;
    }

}
