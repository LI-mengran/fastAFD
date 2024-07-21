package FastRFD.predicates;

public class Predicate {
    private double lowerBound = 0;
    private double higherBound = 0;
    private int columnIndex = 0;
    public Predicate(double lowerBound, double higherBound, int columnIndex){
        this.lowerBound = lowerBound;
        this.higherBound = higherBound;
        this.columnIndex = columnIndex;
    }


    public double getLowerBound() {
        return lowerBound;
    }

    public void setLowerBound(double lowerBound) {
        this.lowerBound = lowerBound;
    }

    public void setHigherBound(double higherBound) {
        this.higherBound = higherBound;
    }

    public double getHigherBound() {
        return higherBound;
    }

    public int getColumn(){return this.columnIndex;}
    public boolean equal(Predicate predicate){
        return predicate.getColumn() == columnIndex && predicate.getLowerBound() == lowerBound && predicate.getHigherBound() == higherBound;
    }
}
