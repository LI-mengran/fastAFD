package FastAFD.AFD;

import java.util.List;

public class AFD {
    List<Integer> thresholdsIndexes;
    Integer columnIndex;


    public AFD(List<Integer> thresholdsIndexes, Integer columnIndex){
        this.thresholdsIndexes = thresholdsIndexes;
        this.columnIndex = columnIndex;
    }

    public int getTotalLIndexes(){
        int total = 0;
        for(int i = 0; i < thresholdsIndexes.size(); i++){
            if(i == columnIndex)continue;
            total += thresholdsIndexes.get(i);
        }
        return total;
    }

    public int getRIndex(){
        return thresholdsIndexes.get(columnIndex);
    }

    public List<Integer> getThresholdsIndexes() {
        return thresholdsIndexes;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }
}
