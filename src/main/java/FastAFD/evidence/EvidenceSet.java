package FastAFD.evidence;

import java.util.*;

public class EvidenceSet {
    List<Evidence> evidenceSet = new ArrayList<>();
    HashMap<BitSet, Long> evidenceNumber = new HashMap<>();
    public EvidenceSet (){

    }

    public void addNewEvidence(Evidence evidence, long number){
        evidenceSet.add(evidence.copy());
        evidenceNumber.put(evidence.bitSet, number);
    }

    public List<Evidence> getEvidenceSet() {
        return evidenceSet;
    }

    public long getEvidenceNumber(Evidence evidence){
        return evidenceNumber.get(evidence.bitSet);
    }

    public boolean containsEvidence(Evidence target){
        return evidenceNumber.containsKey(target.bitSet);
    }
    //may hash by context and not address
    public void add(Evidence target, long number){
        evidenceNumber.put(target.bitSet, evidenceNumber.get(target.bitSet) + number);
    }

    public void add(Evidence evidence){
        evidenceSet.add(evidence);
    }

    public void sort(){
        Collections.sort(evidenceSet, Comparator.comparing(Evidence::getCount).reversed());
    }

}
