package FastAFD.evidence;

import FastAFD.Utils;
import FastAFD.input.ColumnStats;
import FastAFD.input.Input;
import FastAFD.input.ParsedColumn;
import FastAFD.passjoin.PassJoin;
import FastAFD.passjoin.SubstringableString;
import FastAFD.pli.Cluster;
import FastAFD.pli.Pli;
import FastAFD.pli.PliBuilder;
import FastAFD.pli.PliShard;
import FastAFD.predicates.Predicate;
import FastAFD.predicates.PredicatesBuilder;
import org.roaringbitmap.RoaringBitmap;

import java.io.*;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static FastAFD.Utils.calculateEditDistance;
import static java.lang.Thread.sleep;


public class EvidenceSetBuilder {
    PredicatesBuilder predicatesBuilder;
    PassJoin passJoin;
    PliBuilder pliBuilder;
    boolean usePassjoin = false;
    int columnNumber;
    int rowNumber;
    RoaringBitmap tableIds;
    int longestLength = 0;
    int shard = 10000;
    final EvidenceSet evidenceSet = new EvidenceSet();
    ArrayList<ColumnStats> stringColumnStats = new ArrayList<>();
    ArrayList<ColumnStats> numberColumnStats = new ArrayList<>();
    List<Integer> stringColumnIndex = new ArrayList<>();
    List<Integer> numberColumnIndex = new ArrayList<>();
    List<UpdateSetCache> updateSetCaches = new ArrayList<>();
    List<ParsedColumn<?>> pColumns;
    List<List<Object>> keySet = new ArrayList<>();
    int computeTime = 0;
    List<Integer> maxDemarcations = new ArrayList<>();

    long shardLength =  10000;

    public EvidenceSetBuilder(PredicatesBuilder predicatesBuilder, PliBuilder pliBuilder, Input input, int shardLength){
        this.predicatesBuilder = predicatesBuilder;
        this.pliBuilder = pliBuilder;
        this.columnNumber = predicatesBuilder.getColumnStats().size();
        stringColumnStats = new ArrayList<>();
        this.tableIds = new RoaringBitmap();
        this.pColumns = input.getParsedColumns();
        this.rowNumber = input.getRowCount();
        this.shardLength = shardLength;
        for(int index = 0; index < columnNumber; index ++){
            ColumnStats columnStat = predicatesBuilder.getColumnStats().get(index);
            if(!columnStat.isNum){
                stringColumnStats.add(columnStat);
                stringColumnIndex.add(index);

                //passjoin
                if(usePassjoin){
                    if(predicatesBuilder.getAllColumnDemarcations().get(index).size() == 0)
                        maxDemarcations.add(predicatesBuilder.getAllColumnDemarcations().get(index).get(0).intValue());
                    else
                        maxDemarcations.add(predicatesBuilder.getAllColumnDemarcations().get(index).get(0).intValue());
                }
                UpdateSetCache<Double> updateSetCache = new UpdateSetCache<>();
                updateSetCaches.add(updateSetCache);
                if(columnStat.getLongestLength() > longestLength)longestLength = columnStat.getLongestLength();
            }
            else{
                numberColumnStats.add(columnStat);
                numberColumnIndex.add(index);

                UpdateSetCache<String> updateSetCache = new UpdateSetCache<>();
                updateSetCaches.add(updateSetCache);
            }
            keySet.add(new ArrayList<>());
            keySet.get(index).addAll(pliBuilder.getPlis().get(index).getKeys());
        }

        Utils.editDistanceBuffer = new int[longestLength + 1][longestLength + 1];
        for (int i = 0; i <= longestLength; i++) {
            Utils.editDistanceBuffer[0][i] = i;
            Utils.editDistanceBuffer[i][0] = i;
        }

        //passjoin
        if(usePassjoin)
            passJoin = new PassJoin(maxDemarcations, stringColumnStats);
    }

    public void testPassjoin(){
        List<List<String>> allValues = new ArrayList<>();
        for (int index : stringColumnIndex) {
            allValues.add((List<String>) pliBuilder.getPlis().get(index).getKeys());
        }
        passJoin.createIndexs(allValues, longestLength);
        int time1 = 0;
        int time2 = 0;
        for (int columnIndex = 0; columnIndex < 11; columnIndex++){
            if(columnIndex == 10)continue;
            int passJoinIndex = stringColumnIndex.indexOf(columnIndex);
            if(passJoinIndex == -1) {
                return ;
            }
            Instant start = Instant.now();

            for(int startTupleId = 0; startTupleId < rowNumber; startTupleId++){
                String val = (String) pColumns.get(columnIndex).getValue(startTupleId);
                HashMap<String, Integer> updateIds = passJoin.getSimilaritySet(val, passJoinIndex, startTupleId, (Pli<String>) pliBuilder.getPlis().get(columnIndex));



//                if(mp.size() != updateIds.size()){
////                    System.out.println(val + ": " + mp.size() +" " + updateIds.size());
//                    for(String s : mp.keySet()){
//                        if(!updateIds.keySet().contains(s)){
//                            System.out.println(val + " " + s + " : " + mp.get(s));
//                        }
//                        else if(mp.get(s) != updateIds.get(s))
//                            System.out.println(s + " : " + mp.get(s) + "-" + updateIds.get(s));
//                    }
//                }
            }
            Duration duration = Duration.between(start, Instant.now());
            System.out.println("pass" + ':' + duration.toMillis());
            time1 += duration.toMillis();


            start = Instant.now();

            HashMap<String, Integer> mp = new HashMap<>();
            for(int startTupleId = 0; startTupleId < rowNumber; startTupleId++) {
                String val = (String) pColumns.get(columnIndex).getValue(startTupleId);
                for (int index = 0; index < rowNumber; index++) {
                    String compareValue = (String) pColumns.get(columnIndex).getValue(index);
                    double distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0, val.length(), new SubstringableString(compareValue), 0, compareValue.length(), maxDemarcations.get(columnIndex).intValue(), Utils.editDistanceBuffer);
                    if(distance <= maxDemarcations.get(columnIndex))
                        mp.put(compareValue, (int) distance);
                }
            }
            duration = Duration.between(start, Instant.now());
            System.out.println("normal" + ':' + duration.toMillis());
            time2 += duration.toMillis();

        }
        System.out.println("pass" + ':' + time1);
        System.out.println("normal" + ':' + time2);

    }

    public void buildDiffSetByPli()  {
//        boolean[] flag = new boolean[5778];
//        AtomicLong count = new AtomicLong();
        for(int shardIndex = 0; shardIndex <= ((long) rowNumber * (rowNumber - 1) / 2 - 1) / shardLength; shardIndex ++){
            List<List<Integer>> eviSet = new ArrayList<>();
            if(shardIndex == ((long) rowNumber * (rowNumber - 1) / 2 - 1) / shardLength){
                for(int i = 0; i < ((long) rowNumber * (rowNumber - 1) / 2 ) % shardLength; i++){
                    List<Integer> tmp = Collections.nCopies(columnNumber, 0);
//            flag[i] = false;
                    eviSet.add(new ArrayList<>(tmp));
                }
            }
            else
                for(int i = 0; i < shardLength; i++){
                    List<Integer> tmp = Collections.nCopies(columnNumber, 0);
    //            flag[i] = false;
                    eviSet.add(new ArrayList<>(tmp));
                }
            for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
                Pli<?> pli = pliBuilder.getPlis().get(columnIndex);
                if(numberColumnIndex.contains(columnIndex)){
                    for(int i = 0; i < pli.size(); i++){
                        List<Integer> inner = pli.getCluster(i).getRawCluster();
                        for(int i1 = 0; i1 < inner.size(); i1++){
                            for(int j1 = i1 + 1; j1 < inner.size(); j1++){
                                if(((long) inner.get(j1) * (inner.get(j1) - 1) / 2 + inner.get(i1)) / shardLength != shardIndex)continue;
                                eviSet.get((int) (((inner.get(j1)% shardLength) * ((inner.get(j1) - 1)% shardLength) / 2 + inner.get(i1) + shardLength) % shardLength)).set(columnIndex, predicatesBuilder.getDemarcationsByColumn(columnIndex).size());
                            }
                        }
                        for(int j = i + 1; j < pli.size(); j++){
                            double val1 = getDoubleValue(pli.getKeys().get(i));
                            double val2 = getDoubleValue(pli.getKeys().get(j));
                            int digit = pColumns.get(columnIndex).getDigit();
                            List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                            BigDecimal distance = new BigDecimal(val1).subtract(BigDecimal.valueOf(val2)).abs().setScale(digit, BigDecimal.ROUND_HALF_UP);
                            if(distance.doubleValue() > demarcations.get(0))continue;
                            for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                if (distance.doubleValue() <= demarcations.get(stage)) {
                                    for(int i1 : pli.getCluster(i).getRawCluster()) 
                                        for(int j1 : pli.getCluster(j).getRawCluster()){
                                            int i2 = i1;
                                            if(i1 > j1){
                                                i2 = j1;
                                                j1 = i1;
                                            }
                                            if( ((j1 % shardLength) * ((j1 - 1) % shardLength) / 2 + i2) / shardLength != shardIndex)continue;
                                            eviSet.get((int) (((j1 % shardLength) * ((j1 - 1) % shardLength) / 2 + i2 + shardLength) % shardLength)).set(columnIndex,stage + 1);
                                        }
                                    break;
                                }
                            }
                        }
                    }
                }
                else{
                    for(int i = 0; i < pli.size(); i++){
                        List<Integer> inner = pli.getCluster(i).getRawCluster();
                        for(int i1 = 0; i1 < inner.size(); i1++){
                            for(int j1 = i1 + 1; j1 < inner.size(); j1++){
                                if(( ((inner.get(j1)% shardLength) * ((inner.get(j1) - 1)% shardLength)) / 2 + inner.get(i1)) / (shardLength) != shardIndex)continue;
                                eviSet.get((int) (((inner.get(j1)% shardLength) * ((inner.get(j1) - 1)% shardLength) / 2 + inner.get(i1) + shardLength) % shardLength)).set(columnIndex, predicatesBuilder.getDemarcationsByColumn(columnIndex).size());
                            }
                        }
                        for(int j = i + 1; j < pli.size(); j++){
                            String val1 = (String) pli.getKeys().get(i);
                            String val2 = (String) pli.getKeys().get(j);
                            List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                            int distance = calculateEditDistance(val1, val2);
                            if(distance > demarcations.get(0))continue;
                            for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                if (distance <= demarcations.get(stage)) {
                                    for(int i1 : pli.getCluster(i).getRawCluster())
                                        for(int j1 : pli.getCluster(j).getRawCluster()){
                                            int i2 = i1;
                                            if(i1 > j1){
                                                i2 = j1;
                                                j1 = i1;
                                            }
                                            if( ((j1 % shardLength) * ((j1 - 1) % shardLength) / 2 + i2) / shardLength != shardIndex)continue;
                                            eviSet.get((int) (((j1 % shardLength) * ((j1 - 1) % shardLength) / 2 + i2 + shardLength) % shardLength)).set(columnIndex,stage + 1);
                                        }
                                    break;
                                }
                            }
                        }
                    }
                }
            }
            HashMap<List<Integer>, Long> map = new HashMap<>();

            eviSet.forEach((k) -> map.merge(k, 1L, Long::sum));
            map.forEach((k, v) -> {
                List<Predicate> evi = new ArrayList<>();
                for(int index = 0; index < k.size(); index++){
                    evi.add(predicatesBuilder.getPredicatesByColumn(index).get(k.get(index)));
                }
                Evidence evidence = new Evidence(evi, k, v, predicatesBuilder.getMaxPredicateIndex());
                evidenceSet.add(evidence, false);
//                count.addAndGet(v);
            });
//            System.out.println(count);


        }
//        for(Evidence evi : evidenceSet.getEvidenceSet()){
//            if(evi == null){
//                System.out.println("csc");
//            }
//        }
//        System.out.println("1");
//        evidenceSet.sort();
    }

    public void buildDiffSetBySepPli(){
        List<PliShard> pliShards = new ArrayList<>();
        for(int i = 0; i <= rowNumber / shardLength; i++){
            pliShards.add(new PliShard(new ArrayList<>(),(int) (i * shardLength), Math.min((int)((i + 1) * shardLength), rowNumber)));
        }
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            Pli<?> pli = pliBuilder.getPlis().get(columnIndex);
            List<List<Cluster>> clusterss = new ArrayList<>();
            if(pColumns.get(columnIndex).getType() == String.class){
                List<List<String>> keyss = new ArrayList<>();
                List<Map<String, Integer>> translators = new ArrayList<>();
                List<HashSet<String>> keySets = new ArrayList<>();
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    clusterss.add(new ArrayList<>());
                    translators.add(new HashMap<String, Integer>());
                    keySets.add(new HashSet<>());
                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        keySets.get(shardIndex).add((String) key);
                    }
                }
                for(int index = 0; index < keySets.size(); index++){
                    keyss.add(new ArrayList<>(keySets.get(index)));
                    List<String> keyset = keyss.get(index);
                    for(int i = 0; i < keyset.size(); i++) {
                        clusterss.get(index).add(new Cluster());
                        translators.get(index).put(keyset.get(i),i);
                    }

                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        clusterss.get(shardIndex).get(translators.get(shardIndex).get((String) key)).add((int) (index - shardIndex * shardLength));
                    }
                }
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    pliShards.get(i).plis.add(new Pli<>(false, clusterss.get(i), keyss.get(i), translators.get(i)));
                }


            }
            else if(pColumns.get(columnIndex).getType() == Double.class){
                List<List<Double>> keyss = new ArrayList<>();
                List<Map<Double, Integer>> translators = new ArrayList<>();
                List<HashSet<Double>> keySets = new ArrayList<>();
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    clusterss.add(new ArrayList<>());
                    translators.add(new HashMap<Double, Integer>());
                    keySets.add(new HashSet<>());
                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        keySets.get(shardIndex).add((Double) key);
                    }
                }
                for(int index = 0; index < keySets.size(); index++){
                    keyss.add(new ArrayList<>(keySets.get(index)));
                    List<Double> keyset = keyss.get(index);
                    for(int i = 0; i < keyset.size(); i++) {
                        clusterss.get(index).add(new Cluster());
                        translators.get(index).put(keyset.get(i),i);
                    }

                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        clusterss.get(shardIndex).get(translators.get(shardIndex).get((Double) key)).add((int) (index - shardIndex * shardLength));
                    }
                }
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    pliShards.get(i).plis.add(new Pli<>(true, clusterss.get(i), keyss.get(i), translators.get(i)));
                }


            }
            else{
                List<List<Integer>> keyss = new ArrayList<>();
                List<Map<Integer, Integer>> translators = new ArrayList<>();
                List<HashSet<Integer>> keySets = new ArrayList<>();
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    clusterss.add(new ArrayList<>());
                    translators.add(new HashMap<Integer, Integer>());
                    keySets.add(new HashSet<>());
                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        keySets.get(shardIndex).add((Integer) key);
                    }
                }
                for(int index = 0; index < keySets.size(); index++){
                    keyss.add(new ArrayList<>(keySets.get(index)));
                    List<Integer> keyset = keyss.get(index);
                    for(int i = 0; i < keyset.size(); i++) {
                        clusterss.get(index).add(new Cluster());
                        translators.get(index).put(keyset.get(i),i);
                    }

                }
                for(Object key : pli.getKeys()){
                    for(int index : pli.getClusterByKey(key).getRawCluster()){
                        int shardIndex = (int) (index / shardLength);
                        clusterss.get(shardIndex).get(translators.get(shardIndex).get((Integer) key)).add((int) (index - shardIndex * shardLength));
                    }
                }
                for(int i = 0; i <= rowNumber / shardLength; i++){
                    pliShards.get(i).plis.add(new Pli<>(true, clusterss.get(i), keyss.get(i), translators.get(i)));
                }


            }
        }
        for(int i = 0; i < pliShards.size(); i++){
            for(int j = i; j < pliShards.size(); j++){
                List<List<Integer>> eviSet = new ArrayList<>();
                if(i == j){
                    for(int k = 0; k < pliShards.get(i).getLength() * (pliShards.get(i).getLength() - 1) / 2; k++){
                        List<Integer> tmp = Collections.nCopies(columnNumber, 0);
                        eviSet.add(new ArrayList<>(tmp));
                    }
                    for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
                        Pli<?> pli = pliShards.get(i).getPlis().get(columnIndex);
                        if(numberColumnIndex.contains(columnIndex)){
                            for(int index1 = 0; index1 < pli.size(); index1++){
                                List<Integer> inner = pli.getCluster(index1).getRawCluster();
                                for(int i1 = 0; i1 < inner.size(); i1++){
                                    for(int j1 = i1 + 1; j1 < inner.size(); j1++){
                                        eviSet.get( (inner.get(j1) * (inner.get(j1) - 1) / 2 + inner.get(i1))).set(columnIndex, predicatesBuilder.getDemarcationsByColumn(columnIndex).size());
                                    }
                                }
                                double val1 = getDoubleValue(pli.getKeys().get(index1));
                                for(int index2 = index1 + 1; index2 < pli.size(); index2++){
                                    double val2 = getDoubleValue(pli.getKeys().get(index2));
                                    int digit = pColumns.get(columnIndex).getDigit();
                                    List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                                    BigDecimal distance = new BigDecimal(val1).subtract(BigDecimal.valueOf(val2)).abs().setScale(digit, BigDecimal.ROUND_HALF_UP);
                                    if(distance.doubleValue() > demarcations.get(0))continue;
                                    for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                        if (distance.doubleValue() <= demarcations.get(stage)) {
                                            for(int i1 : pli.getCluster(index1).getRawCluster())
                                                for(int j1 : pli.getCluster(index2).getRawCluster()){
                                                    int i2 = i1;
                                                    if(i1 > j1){
                                                        i2 = j1;
                                                        j1 = i1;
                                                    }
                                                    eviSet.get( (j1  * (j1 - 1) / 2 + i2)).set(columnIndex,stage + 1);
                                                }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        else{
                            for(int index1 = 0; index1 < pli.size(); index1++){
                                List<Integer> inner = pli.getCluster(index1).getRawCluster();
                                for(int i1 = 0; i1 < inner.size(); i1++){
                                    for(int j1 = i1 + 1; j1 < inner.size(); j1++){
                                        eviSet.get( (inner.get(j1) * (inner.get(j1) - 1) / 2 + inner.get(i1))).set(columnIndex, predicatesBuilder.getDemarcationsByColumn(columnIndex).size());
                                    }
                                }
                                for(int index2  = index1  + 1; index2 < pli.size(); index2++){
                                    String val1 = (String) pli.getKeys().get(index1);
                                    String val2 = (String) pli.getKeys().get(index2);
                                    List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                                    int distance = calculateEditDistance(val1, val2);
                                    if(distance > demarcations.get(0))continue;
                                    for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                        if (distance <= demarcations.get(stage)) {
                                            for(int i1 : pli.getCluster(index1).getRawCluster())
                                                for(int j1 : pli.getCluster(index2).getRawCluster()){
                                                    int i2 = i1;
                                                    if(i1 > j1){
                                                        i2 = j1;
                                                        j1 = i1;
                                                     }
                                                    eviSet.get( j1  * (j1 - 1) / 2 + i2).set(columnIndex,stage + 1);
                                        }
                                            break;
                                    }
                                }
                            }
                        }
                    }
                }
                }
                else{
                    for(int k = 0; k < pliShards.get(i).getLength() * pliShards.get(j).getLength(); k++){
                        List<Integer> tmp = Collections.nCopies(columnNumber, 0);
                        eviSet.add(new ArrayList<>(tmp));
                    }
                    for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
                        if(numberColumnIndex.contains(columnIndex)){
                            Pli<?> pliA = pliShards.get(i).getPlis().get(columnIndex);
                            Pli<?> pliB = pliShards.get(j).getPlis().get(columnIndex);

                            for(int index1 = 0; index1 < pliA.getClusters().size(); index1++){
                                for(int index2 = 0; index2 < pliB.getClusters().size(); index2++){
                                    double val1 = getDoubleValue(pliA.getKeys().get(index1));
                                    double val2 = getDoubleValue(pliB.getKeys().get(index2));
                                    int digit = pColumns.get(columnIndex).getDigit();
                                    List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                                    BigDecimal distance = new BigDecimal(val1).subtract(BigDecimal.valueOf(val2)).abs().setScale(digit, BigDecimal.ROUND_HALF_UP);
                                    if(distance.doubleValue() > demarcations.get(0))continue;
                                    for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                        if (distance.doubleValue() <= demarcations.get(stage)) {
                                            for(int i1 : pliA.getCluster(index1).getRawCluster())
                                                for(int j1 : pliB.getCluster(index2).getRawCluster()){
                                                    eviSet.get(i1 * (pliShards.get(j).getLength()) + j1).set(columnIndex,stage + 1);
                                                }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        else{
                            Pli<?> pliA = pliShards.get(i).getPlis().get(columnIndex);
                            Pli<?> pliB = pliShards.get(j).getPlis().get(columnIndex);


                            for(int index1 = 0; index1 < pliA.getClusters().size(); index1++){
                                for(int index2 = 0; index2 < pliB.getClusters().size(); index2++){
                                    String val1 = (String) pliA.getKeys().get(index1);
                                    String val2 = (String) pliB.getKeys().get(index2);
                                    List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                                    int distance = calculateEditDistance(val1, val2);
                                    if(distance > demarcations.get(0))continue;
                                    for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                                        if (distance <= demarcations.get(stage)) {
                                            for(int i1 : pliA.getCluster(index1).getRawCluster())
                                                for(int j1 : pliB.getCluster(index2).getRawCluster()){
                                                    eviSet.get(i1 * (pliShards.get(j).getLength()) + j1).set(columnIndex,stage + 1);
                                                }
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                HashMap<List<Integer>, Long> map = new HashMap<>();

                eviSet.forEach((k) -> map.merge(k, 1L, Long::sum));
                map.forEach((k, v) -> {
                    List<Predicate> evi = new ArrayList<>();
                    for(int index = 0; index < k.size(); index++){
                        evi.add(predicatesBuilder.getPredicatesByColumn(index).get(k.get(index)));
                    }
                    Evidence evidence = new Evidence(evi, k, v, predicatesBuilder.getMaxPredicateIndex());
                    evidenceSet.add(evidence, false);
//                count.addAndGet(v);
                });
            }
        }
    }

    public void buildEvidenceSet() {
        List<List<String>> allValues = new ArrayList<>();
        EvidenceTable initialEvidenceTable = new EvidenceTable(rowNumber, new Evidence(predicatesBuilder.getBasedPredicates(), predicatesBuilder.getMaxPredicateIndex()));
        for (int index : stringColumnIndex) {
            allValues.add((List<String>) pliBuilder.getPlis().get(index).getKeys());
        }
        if(usePassjoin)
            passJoin.createIndexs(allValues, longestLength);
        List<Integer> rowSequence = new ArrayList<>();
        for (int i = 0; i < rowNumber - 1; i++) {
            rowSequence.add(i);
        }

        List<Boolean> hasLongValues = new ArrayList<>();
        for(ColumnStats columnStats : predicatesBuilder.getColumnStats()){
            if(columnStats.getLongestLength() >= 230)
                hasLongValues.add(true);
            else hasLongValues.add(false);
        }

        AtomicInteger progress = new AtomicInteger(0);
//        rowSequence.parallelStream().forEachOrdered(row ->{
        for (int row = 0; row < rowNumber; row++) {

            List<EvidenceTable> evidenceTables = new ArrayList<>();
            evidenceTables.add(initialEvidenceTable.copy());
            evidenceTables.get(0).removeIds(0, row);

            for (int columnIndex = 0; columnIndex < columnNumber; columnIndex++) {
                if (numberColumnIndex.contains(columnIndex)) {
                    evidenceTables = updateNumberColumn(pColumns, evidenceTables, columnIndex, row + 1);
//                    evidenceTables = updateNumberColumnWithoutPli(pColumns,evidenceTables,columnIndex,row + 1);
                } else
//                    evidenceTables = updateStringColumnWithPassJoin(pColumns, evidenceTables, columnIndex, row + 1);
//                    evidenceTables = updateStringColumnWithoutPli(pColumns, evidenceTables, columnIndex,row + 1);
                    evidenceTables = updateStringColumnWithPli(pColumns, evidenceTables, columnIndex,row + 1, hasLongValues.get(columnIndex));
//                    evidenceTables = updateStringColumnWithPassJoinWithCache(pColumns,evidenceTables,columnIndex,row + 1);
            }
            //the evidenceSet this tuple have;


            for (EvidenceTable evidenceTable : evidenceTables) {
                synchronized (evidenceSet) {
                    if (evidenceSet.containsEvidence(evidenceTable.getEvidence())) {
                        evidenceSet.add(evidenceTable.getEvidence(), evidenceTable.getNumber());
                    } else
                        evidenceSet.addNewEvidence(evidenceTable.getEvidence(), evidenceTable.getNumber());
                }
            }


            int currentProgress = progress.incrementAndGet();
//            if(currentProgress % 100 == 0){
//                double percentage = (double) currentProgress / (rowNumber - 1) * 100;
//                System.out.println("Progress: " + percentage + "%");
//            }

//        });


        }

        for(var evidence : evidenceSet.getEvidenceSet()){
            evidence.count = evidenceSet.getEvidenceNumber(evidence);
        }
//        evidenceSet.sort();
    }

    public List<EvidenceTable> updateNumberColumn( List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId){
        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        UpdateSetCache<Double> updateSetCache = updateSetCaches.get(columnIndex);
        double val = getDoubleValue(pColumns.get(columnIndex).getValue(startTupleId - 1));
        if(updateSetCache.contain(val)){
            updateSet = updateSetCache.getUpdateSet(val);
            if(pColumns.get(columnIndex).getValue(startTupleId - 1) instanceof Integer){
                if(pliBuilder.isLastTuple(columnIndex,(Integer) pColumns.get(columnIndex).getValue(startTupleId - 1),startTupleId - 1))
                    updateSetCache.remove(val);
            }
            else if(pColumns.get(columnIndex).getValue(startTupleId - 1) instanceof Double){
                if(pliBuilder.isLastTuple(columnIndex,val,startTupleId - 1))
                    updateSetCache.remove(val);
            }

        }
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }
        Pli<?> pli = pliBuilder.getPlis().get(columnIndex);
        int digit = pColumns.get(columnIndex).getDigit();

//        int clusterId = pli.getFirstIndexWhereKeyIsLTE(Double.parseDouble(df.format((val - demarcations.get((0))))));
        int clusterId = pli.getFirstIndexWhereKeyIsLTE(new BigDecimal(val).subtract(BigDecimal.valueOf(demarcations.get(0))).setScale(digit, BigDecimal.ROUND_HALF_UP));
        while(clusterId < pli.size()){
//            double distance = Math.abs(Double.parseDouble(df.format(val - getDoubleValue(pli.getKeys().get(clusterId)))));
            BigDecimal distance = new BigDecimal(val).subtract(BigDecimal.valueOf(getDoubleValue(pli.getKeys().get(clusterId)))).abs().setScale(digit, BigDecimal.ROUND_HALF_UP);
            if(distance.doubleValue() > demarcations.get(0))break;
            for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                if (distance.doubleValue() <= demarcations.get(stage)) {
                    List<Integer> ids = pli.getCluster(clusterId).getRawCluster();
//                    for(int index = findNextTupleId(ids, startTupleId - 1); index < ids.size(); index++ )
//                        updateSet.get(stage).add(ids.get(index));

                    for(Integer index : ids)
                        updateSet.get(stage).add(index);
                    break;
                }
            }
            clusterId++;
        }

        List<Integer> thisCluster;
        if(pColumns.get(columnIndex).getValue(startTupleId - 1) instanceof Double){
            thisCluster = pliBuilder.getTuplesByKey(columnIndex, val);
        }
        else {
            thisCluster = pliBuilder.getTuplesByKey(columnIndex,  (Integer) pColumns.get(columnIndex).getValue(startTupleId - 1) );
        }
        if(thisCluster != null && thisCluster.size() > 1){
            updateSetCache.add(val, updateSet);
        }

        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex), predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
        return newEvidenceTables;
    }

    public List<EvidenceTable> updateNumberColumnWithoutPli( List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId){
        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        double val = getDoubleValue(pColumns.get(columnIndex).getValue(startTupleId - 1));
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }

        for(int index = startTupleId; index < rowNumber; index++){
            double val1;
            if(pColumns.get(columnIndex).getValue(index) instanceof Integer)
                val1 = (Integer) pColumns.get(columnIndex).getValue(index) ;
            else if (pColumns.get(columnIndex).getValue(index) instanceof Double) {
                val1 = (Double) pColumns.get(columnIndex).getValue(index) ;
            }
            else return null;
            double distance = Math.abs(val - val1);
            for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
                if(distance <= demarcations.get(stage)){
                    updateSet.get((stage)).add(index);
                    break;
                }
            }
        }



        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex), predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
        return newEvidenceTables;
    }

    public List<EvidenceTable> updateStringColumnWithPassJoin(List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId){
        int passJoinIndex = stringColumnIndex.indexOf(columnIndex);
        if(passJoinIndex == -1) {
            return null;
        }
        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        UpdateSetCache<String> updateSetCache = updateSetCaches.get(columnIndex);
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }
        String val = (String) pColumns.get(columnIndex).getValue(startTupleId - 1);
        if(updateSetCache.contain(val)){
            updateSet = updateSetCache.getUpdateSet(val);
            if(pliBuilder.isLastTuple(columnIndex,val,startTupleId - 1))
                updateSetCache.remove(val);
        }
        else{
//            Instant start = Instant.now();
            HashMap<String, Integer> updateIds = passJoin.getSimilaritySet(val, passJoinIndex,startTupleId,(Pli<String>) pliBuilder.getPlis().get(columnIndex));
//            for(String kkk : updateIds.keySet()){
//                if(updateIds.get(kkk) > maxDemarcations.get(columnIndex)){
//                double distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0, val.length(), new SubstringableString(kkk), 0, kkk.length(), demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//                if(updateIds.get(kkk) != (int)distance)     System.out.println(updateIds.get(kkk) + ": " + distance);
//                }
//            }
//            for(int i = 0; i < rowNumber; i++){
//                String kkk = (String) pColumns.get(columnIndex).getValue(i);
//                double distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0, val.length(), new SubstringableString(kkk), 0, kkk.length(), demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//                if(distance <= maxDemarcations.get(columnIndex)){
//                    if(!updateIds.keySet().contains(kkk)){
//                        System.out.println(val + " | " + kkk + ":" + distance);
//                    }
//                }
//            }

//            Duration duration = Duration.between(start, Instant.now());
//            System.out.println("pass" + val + ':' + duration.toMillis());
//            computeTime += duration.toMillis();

//            start = Instant.now();
//            HashMap<String, Integer> mp = new HashMap<>();
//            for(int index = startTupleId; index < rowNumber; index++) {
//                String compareValue = (String) pColumns.get(columnIndex).getValue(index);
//                double distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0, val.length(), new SubstringableString(compareValue), 0, compareValue.length(), demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//                mp.put(compareValue,(int) distance);
//            }
//            duration = Duration.between(start, Instant.now());
//            System.out.println("normal" + val + ':' + duration.toMillis());


            for(String similarValue : updateIds.keySet()){
                double distance = updateIds.get(similarValue);
                for(int stage = demarcations.size() - 1; stage >= 0; stage--){
                    if(distance <= demarcations.get(stage)){
                        for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex, similarValue))
                            updateSet.get((stage)).add(tupleIndex);
                        break;
                    }
                }
            }


            if(pliBuilder.getTuplesByKey(columnIndex, val).size() > 1){
                for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex,val)){
                    updateSet.get(demarcations.size() - 1).add(tupleIndex);
                }
                updateSetCache.add(val, updateSet);
            }

        }
        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                updateSet.get(predicateIndex - 1).remove(0,startTupleId - 1);
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex),predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
//        passJoin.removeValue(val,columnIndex,startTupleId - 1);
        return newEvidenceTables;
    }

    public void updateCacheByIndex(int columnIndex, int startTupleId){
        if(updateSetCaches.size() < columnIndex)return;
        UpdateSetCache<String> updateSetCache = updateSetCaches.get(columnIndex);


        List<Integer> rowSequence = new ArrayList<>();
        for (int i = 0; i < shard; i++) {
            rowSequence.add(i);
        }


        AtomicInteger progress = new AtomicInteger(0);
//        rowSequence.parallelStream().forEachOrdered(index ->{
        for(int index = 0; index < shard; index++){
            if(startTupleId + index >= rowNumber)return;
            String val = (String) pColumns.get(columnIndex).getValue(startTupleId + index);
            if(!updateSetCache.contain(val)){

                int passJoinIndex = stringColumnIndex.indexOf(columnIndex);
                List<RoaringBitmap> updateSet = new ArrayList<>();
                List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
                HashMap<String, Integer> updateIds = passJoin.getSimilaritySet(val, passJoinIndex,startTupleId,(Pli<String>) pliBuilder.getPlis().get(columnIndex));
                for(int i = 0; i < demarcations.size(); i++){
                    RoaringBitmap map = new RoaringBitmap();
                    updateSet.add(map);
                }


                for(String similarValue : updateIds.keySet()){
                    double distance = updateIds.get(similarValue);
                    for(int stage = demarcations.size() - 1; stage >= 0; stage--){
                        if(distance <= demarcations.get(stage)){
                            for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex, similarValue))
                                updateSet.get((stage)).add(tupleIndex);
                            break;
                        }
                    }
                }

                if(pliBuilder.getTuplesByKey(columnIndex, val).size() > 1){
                    for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex,val)){
                        updateSet.get(demarcations.size() - 1).add(tupleIndex);
                    }
                }
                updateSetCache.add(val, updateSet);

            }
            int currentProgress = progress.incrementAndGet();
            if(currentProgress % 100 == 0){
                double percentage = (double) currentProgress / shard * 100;
                System.out.println(columnIndex + " Update: " + percentage + "%");
            }
//            });

        }
    }

    public List<EvidenceTable> updateStringColumnWithPassJoinWithCache(List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId){
        if((startTupleId - 1) % shard == 0){
            updateCacheByIndex(columnIndex, startTupleId - 1);
        }
        int passJoinIndex = stringColumnIndex.indexOf(columnIndex);
        if(passJoinIndex == -1) {
            return null;
        }
        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        UpdateSetCache<String> updateSetCache = updateSetCaches.get(columnIndex);
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }
        String val = (String) pColumns.get(columnIndex).getValue(startTupleId - 1);
        if(updateSetCache.contain(val)){
            updateSet = updateSetCache.getUpdateSet(val);
            if(pliBuilder.isLastTuple(columnIndex,val,startTupleId - 1))
                updateSetCache.remove(val);
        }
        else{
//            System.out.println("??????");
            HashMap<String, Integer> updateIds = passJoin.getSimilaritySet(val, passJoinIndex,startTupleId,(Pli<String>) pliBuilder.getPlis().get(columnIndex));


            for(String similarValue : updateIds.keySet()){
                double distance = updateIds.get(similarValue);
                for(int stage = demarcations.size() - 1; stage >= 0; stage--){
                    if(distance <= demarcations.get(stage)){
                        for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex, similarValue))
                            updateSet.get((stage)).add(tupleIndex);
                        break;
                    }
                }
            }


            if(pliBuilder.getTuplesByKey(columnIndex, val).size() > 1){
                for(Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex,val)){
                    updateSet.get(demarcations.size() - 1).add(tupleIndex);
                }
                updateSetCache.add(val, updateSet);
            }

        }
        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                updateSet.get(predicateIndex - 1).remove(0,startTupleId - 1);
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex),predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
//        passJoin.removeValue(val,columnIndex,startTupleId - 1);
        return newEvidenceTables;
    }

    public List<EvidenceTable> updateStringColumnWithoutPli(List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId){

        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }
        String val = (String) pColumns.get(columnIndex).getValue(startTupleId - 1);
        for(int index = startTupleId; index < rowNumber; index++){
            String compareValue = (String) pColumns.get(columnIndex).getValue(index);
            double distance = calculateEditDistance(val, (String) pColumns.get(columnIndex).getValue(index));
//            double distance1 = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0,val.length(),new SubstringableString(compareValue),0,compareValue.length(),demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//            if(distance1 != distance){
//                System.out.println(val + ':' + (String) pColumns.get(columnIndex).getValue(index) );
//                System.out.print(distance1);
//                System.out.print(':' );
//                System.out.print( distance);
//                System.out.print('\n');
//            }
            if(distance < 0)continue;
            for(int stage = demarcations.size() - 1; stage >= 0; stage--){
                if(distance < demarcations.get(stage)){
                        updateSet.get((stage)).add(index);
                    break;
                }
            }
        }
        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                updateSet.get(predicateIndex - 1).remove(0,startTupleId - 1);
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex),predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
//        passJoin.removeValue(val,columnIndex,startTupleId - 1);
        return newEvidenceTables;
    }

    public List<EvidenceTable> updateStringColumnWithPli(List<ParsedColumn<?>> pColumns, List<EvidenceTable> evidenceTables, int columnIndex, int startTupleId, boolean hasLongValue){
//        int passJoinIndex = stringColumnIndex.indexOf(columnIndex);
//        if(passJoinIndex == -1) {
//            return null;
//        }

        List<RoaringBitmap> updateSet = new ArrayList<>();
        List<Predicate> predicates = predicatesBuilder.getPredicatesByColumn(columnIndex);
        List<Double> demarcations = predicatesBuilder.getDemarcationsByColumn(columnIndex);
        UpdateSetCache<String> updateSetCache = updateSetCaches.get(columnIndex);
//        Pli<?> pli = pliBuilder.getPlis().get(columnIndex);
        for(int index = 0; index < demarcations.size(); index++){
            RoaringBitmap map = new RoaringBitmap();
            updateSet.add(map);
        }
        String val = (String) pColumns.get(columnIndex).getValue(startTupleId - 1);
        if(updateSetCache.contain(val)){
            updateSet = updateSetCache.getUpdateSet(val);
            if(pliBuilder.isLastTuple(columnIndex,val,startTupleId - 1))
                updateSetCache.remove(val);
        }
        else{
            if(demarcations.size() != 0)
            {
                RoaringBitmap unComputed = new RoaringBitmap();
                unComputed.add(startTupleId, rowNumber);
                for(Integer index :pliBuilder.getTuplesByKey(columnIndex, val)){
                    updateSet.get(demarcations.size() - 1).add(index);
                    unComputed.remove(index);
                }

                for(int index = startTupleId; index < rowNumber; index++){
                    if(!unComputed.contains(index))continue;
                    String compareValue = (String) pColumns.get(columnIndex).getValue(index);
//                    if(Math.abs(compareValue.length() - val.length()) > demarcations.get(0))
//                        continue;
//                    if(!Utils.isWithinEditDistance(new SubstringableString(val), new SubstringableString((String) pColumns.get(columnIndex).getValue(index)), 0,0, Math.max(compareValue.length(), val.length()),demarcations.get(0).intValue(), Utils.editDistanceBuffer,new int[1]))
//                        continue;
//                    double temp = Utils.calculateEditDistanceWithThreshold(new SubstringableString("Milwaukie"), 0,val.length(),new SubstringableString("Annapolis"),0,compareValue.length(),demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//                    Instant start = Instant.now();
//                    double distance = getLevenshteinDistance(val,compareValue);

                    //If the distance is always < max threshold then can't use it
                    double distance;
                    if(Math.abs(val.length() - compareValue.length()) > demarcations.get(0))
                        distance = demarcations.get(0) + 1;
                    else if(demarcations.get(0).intValue() >= val.length() && demarcations.get(0).intValue() >= compareValue.length()){
                        distance = calculateEditDistance(val,compareValue);
                    }
                    else if(hasLongValue && val.length() >= 230 && val.length() == compareValue.length()){
                        distance = calculateEditDistance(val,compareValue);
                    }
                    else{
                        distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0,val.length(),new SubstringableString(compareValue),0,compareValue.length(),demarcations.get(0).intValue(), Utils.editDistanceBuffer);

//                        double distance1 = calculateEditDistance(val,compareValue);
//                        if(distance1 == 0 || distance == 0){
//                            if(distance1 != 0 || distance !=0){
//                                System.out.println(val.length() + ":" + compareValue.length());
////                                System.out.println(val + ":" + compareValue + " ----" + distance1 + ":" + distance);
//                            }
//                        }
                    }

                    if(distance > demarcations.get(0).intValue()){
                        continue;
                    }
//                    if(val.length() != compareValue.length())distance = 1;
//                    else distance = calculateEditDistance(val,compareValue);

                        for(int stage = demarcations.size() - 1; stage >= 0; stage--){
                            if(distance <= demarcations.get(stage)){
//                                List<Integer> ids = pliBuilder.getTuplesByKey(columnIndex, compareValue);
//                                for(int index1 = findNextTupleId(ids, index - 1); index1 < ids.size(); index1++ ){
//                                    updateSet.get((stage)).add(ids.get(index1));
//                                }
                                for(Integer index1 : pliBuilder.getTuplesByKey(columnIndex, compareValue)){
                                    updateSet.get(stage).add(index1);
                                    unComputed.remove(index1);
                                }
                                break;
                            }
                        }
                }
            }




//            Pli<?> pli = pliBuilder.getPlis().get(columnIndex);
//            int clusterId = pli.getFirstIndexWhereLengthIsLTE(val.length() - demarcations.get(0).intValue());
////        int clusterId = 0;
//            while(clusterId < pli.size()){
////                double distance = Math.abs(val - getDoubleValue(pli.getKeys().get(clusterId)));
//                String compareValue = (String)pli.getKeys().get(clusterId);
//                int distance = Utils.calculateEditDistanceWithThreshold(new SubstringableString(val), 0,val.length(),new SubstringableString(compareValue),0,compareValue.length(),demarcations.get(0).intValue(), Utils.editDistanceBuffer);
//                if(compareValue.length() > val.length() + demarcations.get(0))break;
//                for(int stage = demarcations.size() - 1; stage >= 0; stage--) {
//                    if (distance < demarcations.get(stage)) {
//                        for(Integer index : pli.getCluster(clusterId).getRawCluster())
//                            updateSet.get((stage)).add(index);
//                        break;
//                    }
//                }
//                clusterId++;
//            }


            if(pliBuilder.getTuplesByKey(columnIndex, val).size() > 1){
                if(demarcations.size() != 0) {
                    for (Integer tupleIndex : pliBuilder.getTuplesByKey(columnIndex, val)) {
                        updateSet.get(demarcations.size() - 1).add(tupleIndex);
                    }
                    updateSetCache.add(val, updateSet);
                }
            }

//            if(pliBuilder.isLastTuple(columnIndex, val, startTupleId - 1)){
//                keySet.get(columnIndex).remove(val);
//            }

        }
//        Instant start = Instant.now();

        if(!isUpdated(updateSet)) return evidenceTables;
        List<EvidenceTable> newEvidenceTables = new ArrayList<>();
        for(EvidenceTable evidenceTable : evidenceTables){
            for(int predicateIndex = 1; predicateIndex < predicates.size(); predicateIndex++){
                updateSet.get(predicateIndex - 1).remove(0,startTupleId - 1);
                if(updateSet.get(predicateIndex - 1).isEmpty())continue;
                RoaringBitmap table = evidenceTable.table.clone();
                table.and(updateSet.get(predicateIndex - 1));
                if(table.isEmpty())continue;
                EvidenceTable newEvidenceTable = evidenceTable.copy(table);
                evidenceTable.remove(updateSet.get(predicateIndex - 1));
                newEvidenceTable.updateEvidence(columnIndex, predicates.get(predicateIndex),predicateIndex);
                newEvidenceTables.add(newEvidenceTable);
            }
            if(evidenceTable.table.isEmpty())continue;
            newEvidenceTables.add(evidenceTable);
        }
//        Duration duration = Duration.between(start, Instant.now());
//        computeTime += duration.toMillis();
//        passJoin.removeValue(val,columnIndex,startTupleId - 1);
        return newEvidenceTables;
    }

    public boolean isUpdated(List<RoaringBitmap> updateSet){
        for(RoaringBitmap map : updateSet){
            if(!map.isEmpty()) return true;
        }
        return false;
    }

    public void outPut(){

                String fileName = "evidences.txt"; // 指定要写入的文件名

                try {
                    // 打开文件输出流
                    FileWriter fileWriter = new FileWriter(fileName);

                    // 创建一个PrintWriter，以便更容易地写入文本
                    PrintWriter printWriter = new PrintWriter(fileWriter);

                    int totalCount = 0;
                    for(Evidence evidence : evidenceSet.evidenceSet){
                        String evi = "";
                        for(int index = 0; index < columnNumber; index++) {
                            evi += pColumns.get(index).getColumnName();
                            evi += '[';
                            evi += evidence.getPredicate(index).getLowerBound();
                            evi += ',';
                            evi += evidence.getPredicate(index).getHigherBound();
                            evi += "]、";
                        }
                        evi += evidenceSet.evidenceNumber.get(evidence.bitSet);
                        totalCount += evidenceSet.evidenceNumber.get(evidence.bitSet);
                        printWriter.println(evi);
                    }
                    printWriter.println("evidence count:" + evidenceSet.evidenceSet.size());
                    printWriter.println("totalCount " + ": " + totalCount);

                    // 关闭输出流
                    printWriter.close();
                    System.out.println("数据已写入文件：" + fileName);
                } catch (IOException e) {
                    e.printStackTrace();
                }
                System.out.println(computeTime);


    }

    public int getLongestLength() {
        return longestLength;
    }

    public double getDoubleValue(Object number){
        if (number instanceof Double)
            return (Double) number;
        else if (number instanceof Integer) {
            return ((Integer) number).doubleValue();
        }
        else return 0;
    }

    public EvidenceSet getEvidenceSet() {
        return evidenceSet;
    }

    public void readIndex(String fileName) throws FileNotFoundException {

        try  {
            BufferedReader br = new BufferedReader(new FileReader(fileName));
            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");
                List<Integer> integers = new ArrayList<>();
                for (int index = 0; index < values.length - 1; index++) {
                    integers.add(Integer.parseInt(values[index]));
                }
                Evidence evidence = new Evidence(integers, Long.parseLong(values[values.length - 1]));
                evidenceSet.add(evidence,true);
            }
        }  catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static int findNextTupleId(List<Integer> ids, int thisId) {
        int left = 0;
        int right = ids.size() - 1;
        int result = 0;

        while (left <= right) {
            int mid = (right + left) / 2;
            int currentId = ids.get(mid);

            if (currentId >= thisId) {
                result = mid;
                right = mid - 1;
            } else {
                left = mid + 1;
            }
        }

        return result;
    }

    public void indexOutput(String name){
        String fileName = "evidenceSet/evidencesIndex_" + name;

        try {

            FileWriter fileWriter = new FileWriter(fileName);

            PrintWriter printWriter = new PrintWriter(fileWriter);

            for(Evidence evidence : evidenceSet.evidenceSet){
                String evi = "";
                for(int index = 0; index < columnNumber; index++) {
                    evi += evidence.getPredicateIndex(index);
                    evi += ',';
                }
                evi += evidenceSet.evidenceNumber.get(evidence.bitSet) != null ? evidenceSet.evidenceNumber.get(evidence.bitSet) : evidence.getCount();
                printWriter.println(evi);
            }


            printWriter.close();
            System.out.println("数据已写入文件：" + fileName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
