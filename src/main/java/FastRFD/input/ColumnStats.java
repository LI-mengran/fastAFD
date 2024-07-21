package FastRFD.input;

public class ColumnStats<T> {
    public boolean isNum;
    private Class<T> type;

    public int shortestLength;
    public int longestLength;
    public double minNumber;
    public double maxNumber;
    public boolean hasShortValue; // value that is shorter or equal to edit distance
    int columnIndex;

    public ColumnStats(int columnIndex, Class<T> type) {
        this.shortestLength = Integer.MAX_VALUE;
        this.longestLength = 0;
        this.maxNumber = 0;
        this.minNumber = Integer.MAX_VALUE;
        this.hasShortValue = false;
        this.columnIndex = columnIndex;
        this.type = type;
    }

    public int getColumnIndex() {
        return columnIndex;
    }
    public int getLongestLength() {
        return longestLength;
    }
}
