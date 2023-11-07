package FastAFD.evidence;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;

public class EvidenceSet {
    List<Evidence> evidenceSet = new ArrayList<>();
    HashMap<BitSet, Integer> evidenceNumber = new HashMap<>();
    public EvidenceSet (){

    }

    public void addNewEvidence(Evidence evidence, int number){
        evidenceSet.add(evidence.copy());
        evidenceNumber.put(evidence.bitSet, number);
    }

    public List<Evidence> getEvidenceSet() {
        return evidenceSet;
    }

    public int getEvidenceNumber(Evidence evidence){
        return evidenceNumber.get(evidence.bitSet);
    }

    public boolean containsEvidence(Evidence target){
        return evidenceNumber.containsKey(target.bitSet);
    }
    //may hash by context and not address
    public void add(Evidence target, int number){
        evidenceNumber.put(target.bitSet, evidenceNumber.get(target.bitSet) + number);
    }

    public void add(Evidence evidence){
        evidenceSet.add(evidence);
    }

}
