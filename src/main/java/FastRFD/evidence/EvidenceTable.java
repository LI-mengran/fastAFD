package FastRFD.evidence;

import FastRFD.predicates.Predicate;
import org.roaringbitmap.RoaringBitmap;

public class EvidenceTable {
    RoaringBitmap table = new RoaringBitmap();
    Evidence evidence;
    public EvidenceTable(int rowNumber,Evidence basedEvidence){
        this.table.add(0, (long) rowNumber);
        this.evidence = basedEvidence;
    }
    public EvidenceTable(RoaringBitmap mp,Evidence basedEvidence){
        this.table = mp;
        this.evidence = basedEvidence;
    }

    public EvidenceTable copy(){
        return new EvidenceTable(table.clone(), evidence.copy());
    }

    public EvidenceTable copy(RoaringBitmap table){
        return new EvidenceTable(table, evidence.copy());
    }
    public void removeIds(int startId, int endId){
        this.table.remove(startId, endId + 1);
    }
    public void and(RoaringBitmap update){
        this.table.and(update);
    }

    public void remove(RoaringBitmap update){
        this.table.andNot(update);
    }

    public void updateEvidence(int columnIndex, Predicate predicate, int predicateIndex){
        this.evidence.updatePredicate(columnIndex, predicate, predicateIndex );
    }
    public Evidence getEvidence(){
        return evidence;
    }
    public long getNumber(){
        return table.getCardinality();
    }

}
