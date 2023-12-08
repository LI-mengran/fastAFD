package FastAFD.AEI;

import java.util.ArrayList;
import java.util.List;

public class SearchNode {
    int e;
    int columnIndex;
    List<Long> remainCounts;
    List<AFDCandidate> candidates;
    List<AFDCandidate> unhitCand;

    List<Integer> limitThresholds = new ArrayList<>();

    public SearchNode(int e, List<AFDCandidate> candidates, List<Long> remainCounts, List<AFDCandidate> unhitCand,List<Integer> limitThresholds, int columnIndex){
       this.e = e;
       this.candidates = candidates;
       this.remainCounts = remainCounts;
       this.unhitCand = unhitCand;
       this.limitThresholds = limitThresholds;
       this.columnIndex = columnIndex;
    }

    public Long sub(int rightTresholdIndex, Long count){
        remainCounts.set(rightTresholdIndex, remainCounts.get(rightTresholdIndex) - count);
        return remainCounts.get(rightTresholdIndex);
    }
}
