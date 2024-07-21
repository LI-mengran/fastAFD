package FastRFD.TANE;

import java.util.List;

public class Candidate {
    List<Integer> lhsIndex;
    int columnIndex;
    public Candidate(List<Integer> lhsIndex, Integer RIndex, int columnIndex){
        this.lhsIndex = lhsIndex;
        this.columnIndex = columnIndex;
    }

    public Integer getRIndex() {
        return lhsIndex.get(columnIndex);
    }

    public List<Integer> getLhsIndex() {
        return lhsIndex;
    }

    public int getColumnIndex() {
        return columnIndex;
    }

}
