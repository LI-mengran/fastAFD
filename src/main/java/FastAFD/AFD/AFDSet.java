package FastAFD.AFD;

import it.unimi.dsi.fastutil.doubles.AbstractDouble2FloatSortedMap;

import javax.print.attribute.IntegerSyntax;
import java.util.*;

public class AFDSet {
    List<List<AFD>> AFDs;
    int columnIndex;
    List<AFD> minimalAFDs;

    public AFDSet(){
        AFDs = new ArrayList<>();
        AFDs.add(new ArrayList<>());
    }
    public AFDSet(int columnIndex, int maxRIndex){
        AFDs = new ArrayList<>();
        for(int i = 0; i < maxRIndex - 1; i++){
            AFDs.add(new ArrayList<>());
        }
        this.columnIndex = columnIndex;
    }

    public void directlyAdd(AFD afd){
        AFDs.get(0).add(afd);
    }

    //a afd is minimal when the left is maximum and the right is minimum;
    public void add(AFD afd){
        for(int RIndex = afd.thresholdsIndexes.get(afd.columnIndex); RIndex < AFDs.size(); RIndex++){
            List<AFD> removedSet = new ArrayList<>();
            for(AFD fd : AFDs.get(RIndex)) {
                if(fd.thresholdsIndexes.get(columnIndex) > afd.thresholdsIndexes.get(columnIndex))continue;
                if(canCover(fd.thresholdsIndexes,afd.thresholdsIndexes)) removedSet.add(fd);
            }
                AFDs.get(RIndex).removeAll(removedSet);
        }
        AFDs.get(afd.thresholdsIndexes.get(columnIndex)).add(afd);
        //printout
//        if(AFDs.get(afd.thresholdsIndexes.get(columnIndex)).size() % 100 == 0)
//            System.out.println(AFDs.get(afd.thresholdsIndexes.get(columnIndex)).size());
    }

    public boolean containsSubset(List<Integer> LIndexes, int RIndex){
        int total = 0;
        for(int i = 0; i < LIndexes.size(); i++){
            if(i == columnIndex)continue;
            total += LIndexes.get(i);
        }
        for(AFD afd : AFDs.get(RIndex)){
            List<Integer> AIndexed = afd.thresholdsIndexes;
            if(afd.getTotalLIndexes() < total)continue;
            if(canCover(LIndexes, afd.thresholdsIndexes))return true;
        }
        return false;
    }

    public List<AFD> minimize(){
        for(var AFDsByIndex : AFDs){
            AFDsByIndex.sort(Comparator
                    .comparingInt(AFD::getTotalLIndexes));
        }
        List<AFD> results = new ArrayList<>();

        ListIterator<List<AFD>> listIterator = AFDs.listIterator(AFDs.size());

        while (listIterator.hasPrevious()) {
            List<AFD> afds = listIterator.previous();
            // Now, 'afds' contains a list of AFD objects in reverse order
            for (AFD afd : afds) {
                boolean flag = true;
                for (AFD result : results) {
                    if (Objects.equals(afd.thresholdsIndexes.get(columnIndex), result.thresholdsIndexes.get(columnIndex))) {
                        if (canCover(afd.thresholdsIndexes, result.thresholdsIndexes)) {
                            flag = false;
                            break;
                        }
                    } else {
                        if (afd.getTotalLIndexes() >= result.getTotalLIndexes()) {
                            if (canCover(afd.thresholdsIndexes, result.thresholdsIndexes)) {
                                flag = false;
                                break;
                            }
                        }
                    }
                }
                if(flag) results.add(afd);
            }
        }
        AFDs = null;
        minimalAFDs = results;
        return results;
    }


    //current index can cover target and dont need to add target
    boolean canCover(List<Integer> target, List<Integer> currentIdx){
        for(int i = 0; i < target.size(); i++){
            if(i == columnIndex)continue;
            if(target.get(i) < currentIdx.get(i)) {
                return false;
            }
        }
        return true;
    }

    public void show(){
        for(var afd: AFDs.get(0)){
            System.out.println(afd.toString());
        }
    }
    public int minimalCount(){
        return minimalAFDs.size();
    }
}
