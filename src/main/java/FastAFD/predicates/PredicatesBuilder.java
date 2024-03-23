package FastAFD.predicates;

import FastAFD.AEI.TopKSet;
import FastAFD.AFD.AFD;
import FastAFD.AFD.AFDSet;
import FastAFD.input.ColumnStats;
import FastAFD.input.ParsedColumn;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class PredicatesBuilder {
    int columnNumber;
    int maxStage = 5;
    double maxThreshold = 0;
    List<Predicate> basedPredicates = new ArrayList<>();
    List<List<Predicate>> allColumnPredicates = new ArrayList<>();
    List<List<Double>> allColumnDemarcations = new ArrayList<>();
    List<ColumnStats> columnStats;
    String columnTresholdsFile;

    public PredicatesBuilder(int maxStage, double maxThreshold, String fp){
        this.maxStage = maxStage;
        this.maxThreshold = maxThreshold;
        if(fp != ""){
            columnTresholdsFile = fp;
        }
    }
    public PredicatesBuilder(String fp){
        columnTresholdsFile = fp;
    }

    public void generatePredicates(List<ColumnStats> columnStats) throws IOException {
        this.columnStats = columnStats;
        columnNumber = columnStats.size();
        if(!Objects.equals(columnTresholdsFile, "")){
            buildPredicatesWithFile();
            return;
        }
        for(int columnIndex = 0; columnIndex < columnNumber; columnIndex++){
            ColumnStats columnstat = columnStats.get(columnIndex);
            if(columnstat.isNum){
                buildNumberColumn(columnstat);
//                buildNumberColumnTest(columnstat);

            }
            else
                buildStringColumn(columnstat);
//                buildStringColumnTest(columnstat);
        }
    }

    private void buildNumberColumn(ColumnStats columnStat){
        List<Predicate> columnPredicate = new ArrayList<>();
        List<Double> demarcation = new ArrayList<>();
        double range = (columnStat.maxNumber - columnStat.minNumber) * maxThreshold;
//        double range = 2;
        double inteval = range / maxStage;
        columnPredicate.add(new Predicate(range,-1, columnStat.getColumnIndex()));
        for(int i = 0; i < maxStage; i++){
            columnPredicate.add(new Predicate(range - (i + 1) * inteval, range - i * inteval, columnStat.getColumnIndex()));
            demarcation.add(range - i * inteval);
        }
        if(columnPredicate.get(maxStage).getLowerBound() != 0){
            columnPredicate.get(maxStage).setLowerBound(0);
        }
        basedPredicates.add(columnPredicate.get(0));
        allColumnPredicates.add(columnPredicate);
        allColumnDemarcations.add(demarcation);
    }

    private void buildNumberColumnTest(ColumnStats columnStat){
        List<Predicate> columnPredicate = new ArrayList<>();
        List<Double> demarcation = new ArrayList<>();
        maxStage = 2;
        double range = 1;
        double inteval = range / 2;
        columnPredicate.add(new Predicate(range,-1, columnStat.getColumnIndex()));
        for(int i = 0; i < maxStage; i++){
            columnPredicate.add(new Predicate(range - (i + 1) * inteval, range - i * inteval, columnStat.getColumnIndex()));
            demarcation.add(range - i * inteval);
        }
        if(columnPredicate.get(maxStage).getLowerBound() != 0){
            columnPredicate.get(maxStage).setLowerBound(0);
        }
        basedPredicates.add(columnPredicate.get(0));
        allColumnPredicates.add(columnPredicate);
        allColumnDemarcations.add(demarcation);
    }

    private void buildStringColumn(ColumnStats columnStat){
        int range = (int) (columnStat.longestLength * maxThreshold);
        List<Predicate> columnPredicate = new ArrayList<>();
        List<Double> demarcation = new ArrayList<>();
        if(range <= maxStage){
            columnPredicate.add(new Predicate(range,-1,columnStat.getColumnIndex()));
            for(int i = range; i > 0; i--){
                columnPredicate.add(new Predicate(i - 1, i,columnStat.getColumnIndex()));
                demarcation.add((double) i);
            }
        }
        else{
            int inteval = range / maxStage;
            columnPredicate.add(new Predicate(range,-1,columnStat.getColumnIndex()));
            for(int i = 0; i < maxStage; i++){
                columnPredicate.add(new Predicate(range - (i + 1) * inteval, range - i * inteval,columnStat.getColumnIndex()));
                demarcation.add((double) (range - i * inteval));
            }
            if(columnPredicate.get(maxStage).getLowerBound() != 0){
                columnPredicate.get(maxStage).setLowerBound(0);
            }
        }
        basedPredicates.add(columnPredicate.get(0));
        allColumnPredicates.add(columnPredicate);
        allColumnDemarcations.add(demarcation);
    }

    private void buildStringColumnTest(ColumnStats columnStat) throws IOException {
        int[][] vector = {{0},{0},{0},{0},{0},{0},{0},{0},{0},{0},{0}};;
        int[] dem = vector[columnStat.getColumnIndex()];
        List<Predicate> columnPredicate = new ArrayList<>();
        List<Double> demarcation = new ArrayList<>();
        int index ;
        columnPredicate.add(new Predicate(dem[dem.length - 1],-1,columnStat.getColumnIndex()));
        demarcation.add((double) dem[dem.length - 1]);
        for(index = dem.length - 1; index > 0; index--){
            columnPredicate.add(new Predicate(dem[index - 1], dem[index],columnStat.getColumnIndex()));
            demarcation.add((double) dem[index - 1]);
        }
        columnPredicate.add(new Predicate(0,0,columnStat.getColumnIndex()));
        basedPredicates.add(columnPredicate.get(0));
        allColumnPredicates.add(columnPredicate);
        allColumnDemarcations.add(demarcation);
    }

    public void buildPredicatesWithFile(){
        if(columnTresholdsFile != null) {
            List<List<Double>> tempArray = new ArrayList<>();

            try {
                BufferedReader br = new BufferedReader(new FileReader(columnTresholdsFile));
                String line;

                while ((line = br.readLine()) != null) {
                    if(line.equals("")){
                        tempArray.add(new ArrayList<>());
                        continue;
                    }
                    String[] values = line.trim().split(",");
                    List<Double> row = new ArrayList<>();

                    for (String value : values) {
                        row.add(Double.parseDouble(value.trim()));
                    }

                    tempArray.add(row);

                }

                br.close();
                for(int columnIndex = 0; columnIndex < tempArray.size(); columnIndex++) {
                    List<Double> dem = tempArray.get(columnIndex);
                    List<Predicate> columnPredicate = new ArrayList<>();
                    List<Double> demarcation = new ArrayList<>();
                    int index ;
                    columnPredicate.add(new Predicate(dem.get(dem.size() - 1), -1,columnIndex));
                    demarcation.add( dem.get(dem.size() - 1));
                    for (index = dem.size() - 1; index > 0; index--) {
                        columnPredicate.add(new Predicate(dem.get(index - 1),dem.get(index), columnIndex));
                        demarcation.add( dem.get(index - 1));
                    }
                    columnPredicate.add(new Predicate(0, dem.get(0), columnIndex));
                    basedPredicates.add(columnPredicate.get(0));
                    allColumnPredicates.add(columnPredicate);
                    allColumnDemarcations.add(demarcation);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    public List<ColumnStats> getColumnStats() {
        return columnStats;
    }

    public List<List<Double>> getAllColumnDemarcations() {
        return allColumnDemarcations;
    }

    public List<Predicate> getPredicatesByColumn(int columnIndex){
        return allColumnPredicates.get(columnIndex);
    }

    public int getPredicatesNumberByColumn(Predicate predicate){
        return allColumnPredicates.get(predicate.getColumn()).size();
    }

    public List<Double> getDemarcationsByColumn(int columnIndex){
        return allColumnDemarcations.get(columnIndex);
    }
    public List<Predicate> getBasedPredicates(){
        return basedPredicates;
    }

    public int getMaxPredicateIndex(){
        int maxIndex = 0;
        for(var predicates : allColumnPredicates){
            maxIndex = Math.max(predicates.size(), maxIndex);
        }
        return maxIndex;
    }

    public List<Integer> getMaxPredicateIndexByColumn(){
        List<Integer> indexesNumbers = new ArrayList<>();
        for(int i = 0; i < columnNumber; i++){
            indexesNumbers.add(allColumnPredicates.get(i).size());
        }
        return indexesNumbers;
    }

    public int getColumnNumber() {
        return columnNumber;
    }

    public void printAFD(AFDSet afdSet,  List<ParsedColumn<?>> pColumns){
            for(AFD afd : afdSet.getMinimalAFDs()){
                List<Integer> pIndexes = afd.getThresholdsIndexes();
                boolean flag = false;
                for(int index = 0; index < pIndexes.size(); index++ ){
                    if(index == afd.getColumnIndex())continue;
                    if(pIndexes.get(index) == 0)continue;
                    if(flag)System.out.print(", ");
                    flag = true;
                    System.out.print( '[' + pColumns.get(index).getColumnName() + "(<= " +
                            allColumnPredicates.get(index).get(pIndexes.get(index)).getHigherBound() +")]");
                }
                System.out.print(" --> ");
                System.out.print('[' + pColumns.get(afd.getColumnIndex()).getColumnName() + "(<=" +
                        allColumnPredicates.get(afd.getColumnIndex()).get(pIndexes.get(afd.getColumnIndex()) + 1).getHigherBound() +")]");
                System.out.print('\n');
            }
    }

    public void printTopK(List<TopKSet> topKSets, List<ParsedColumn<?>> pColumns){
        for(TopKSet topKSet : topKSets){
            for(double utility : topKSet.getTopKSet()){
                for(AFD afd : topKSet.getUtility2AFD(utility)){
                    List<Integer> pIndexes = afd.getThresholdsIndexes();
                    boolean flag = false;
                    for(int index = 0; index < pIndexes.size(); index++ ){
                        if(index == afd.getColumnIndex())continue;
                        if(pIndexes.get(index) == 0)continue;
                        if(flag)System.out.print(", ");
                        flag = true;
                        System.out.print( '[' + pColumns.get(index).getColumnName() + "(<=" +
                                allColumnPredicates.get(index).get(pIndexes.get(index)).getHigherBound() +")]");
                    }
                    System.out.print(" --> ");
                    System.out.print('[' + pColumns.get(afd.getColumnIndex()).getColumnName() + "(<=" +
                            allColumnPredicates.get(afd.getColumnIndex()).get(pIndexes.get(afd.getColumnIndex()) + 1).getHigherBound() +")]");
                    System.out.print("  utility: " + utility);
                    System.out.print('\n');
                }
            }
        }

    }
}
