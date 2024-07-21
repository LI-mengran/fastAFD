package FastRFD.RFD;

import java.util.*;

public class RFDSet {
    List<List<RFD>> AFDs;
    int columnIndex;
    List<RFD> minimalAFDs;
//    HashMap<List<Integer>, RFD> mp = new HashMap<List<Integer>, RFD>();

    public RFDSet(){
        AFDs = new ArrayList<>();
        minimalAFDs = new ArrayList<>();
        AFDs.add(new ArrayList<>());
    }
    public RFDSet(int columnIndex, int maxRIndex){
        AFDs = new ArrayList<>();
        minimalAFDs = new ArrayList<>();
        for(int i = 0; i < maxRIndex - 1; i++){
            AFDs.add(new ArrayList<>());
        }
        this.columnIndex = columnIndex;
    }

    public void directlyAdd(RFD afd){
        minimalAFDs.add(afd);
    }

    //a afd is minimal when the left is maximum and the right is minimum;
    public void add(RFD afd){
        for(int RIndex = 0; RIndex <= afd.thresholdsIndexes.get(afd.columnIndex); RIndex++){
            List<RFD> removedSet = new ArrayList<>();
            for(RFD fd : AFDs.get(RIndex)) {
                if(lhsEquals(fd.thresholdsIndexes,afd.thresholdsIndexes)){
                    removedSet.add(fd);
                }
            }
                AFDs.get(RIndex).removeAll(removedSet);
        }

//        List<RFD> removedSet = new ArrayList<>();
//        for(RFD fd : AFDs.get(afd.thresholdsIndexes.get(afd.columnIndex))) {
//            if(canCover(fd.thresholdsIndexes,afd.thresholdsIndexes)){
//                removedSet.add(fd);
//            }
//        }
//        AFDs.get(afd.thresholdsIndexes.get(afd.columnIndex)).removeAll(removedSet);

        AFDs.get(afd.thresholdsIndexes.get(columnIndex)).add(afd);
//        List<Integer> mpIndex = new ArrayList<>(afd.getThresholdsIndexes());
//        mpIndex.set(columnIndex, -1);
//        mp.put(mpIndex,afd);

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
        for(int index = RIndex; index < AFDs.size(); index ++){
            for(RFD afd : AFDs.get(index)){
                if(afd.getTotalLIndexes() > total)continue;
                if(canCover(LIndexes, afd.thresholdsIndexes))return true;
            }
        }

        return false;
    }

    public List<RFD> minimize(){
        for(var AFDsByIndex : AFDs){
            AFDsByIndex.sort(Comparator
                    .comparingInt(RFD::getTotalLIndexes));
        }
        List<RFD> results = new ArrayList<>();

        ListIterator<List<RFD>> listIterator = AFDs.listIterator(AFDs.size());

        while (listIterator.hasPrevious()) {
            List<RFD> afds = listIterator.previous();
            // Now, 'afds' contains a list of RFD objects in reverse order
            List<RFD> removeSet = new ArrayList<>();
            for (RFD afd : afds) {
                boolean flag = true;
                for (RFD result : results) {
                    if (Objects.equals(afd.thresholdsIndexes.get(columnIndex), result.thresholdsIndexes.get(columnIndex))) {
                        if (canCover(afd.thresholdsIndexes, result.thresholdsIndexes)) {
                            flag = false;
                            break;
                        }
                        if(canCover(result.thresholdsIndexes, afd.thresholdsIndexes)){
                            removeSet.add(result);
                        }
                    }
                    else {
                        if (afd.getTotalLIndexes() >= result.getTotalLIndexes()) {
                            if (canCover(afd.thresholdsIndexes, result.thresholdsIndexes)) {
                                flag = false;
                                break;
                            }
                        }
                    }
                }
                if(flag) {
                    results.add(afd);
                    results.removeAll(removeSet);
                }
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

    boolean lhsEquals(List<Integer> target, List<Integer> currentIdx){
        for(int i = 0; i < target.size(); i++){
            if(i == columnIndex)continue;
            if(!Objects.equals(target.get(i), currentIdx.get(i))) {
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

    public List<RFD> getMinimalAFDs() {
        return minimalAFDs;
    }

    public List<List<RFD>> getAFDs() {
        return AFDs;
    }


}
