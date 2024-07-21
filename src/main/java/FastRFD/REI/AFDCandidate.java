package FastRFD.REI;

import java.util.ArrayList;
import java.util.List;

public class AFDCandidate {
    List<Integer> leftThresholdsIndexes;
    int rColumnIndex;

    public AFDCandidate(List<Integer> leftThresholdsIndexes, int rColumnIndex){
        this.leftThresholdsIndexes = leftThresholdsIndexes;
        this.rColumnIndex = rColumnIndex;
    }

    public AFDCandidate copy() {
        List<Integer> copy = new ArrayList<>(leftThresholdsIndexes);
        return new AFDCandidate(copy,rColumnIndex );
    }
}
