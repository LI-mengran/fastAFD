package FastAFD.passjoin;

import java.util.stream.IntStream;

public class SubstringableString implements CharSequence {
    private final char[] value;
    private final byte offset;
    private final byte length;
    protected final int hash;

    public SubstringableString(char[] value, byte offset, byte length) {
        this.value = value;
        this.offset = offset;
        this.length = length;
        this.hash = getHash();
    }

    public SubstringableString(char[] value) {
        this.value = value;
        this.offset = 0;
        this.length = (byte) this.value.length;
        this.hash = getHash();
    }

    public SubstringableString(String string) {
        this.value = string.toCharArray();
        this.offset = 0;
        this.length = (byte) this.value.length;
        this.hash = getHash();
    }

    public SubstringableString(char[] value, int offset, int length) {
        this.value = value;
        this.offset = (byte) offset;
        this.length = (byte) length;
        this.hash = getHash();
    }

    public int length() {
        return length;
    }

    @Override
    public char charAt(int i) {
        return value[offset + i];
    }

    @Override
    public CharSequence subSequence(int i, int i1) {
        return this.substring(i, i1);
    }

    public int getStartPosition() {
        return offset;
    }

    public char[] getUnderlyingChars() {
        return value;
    }

    @Override
    public String toString() {
        return new String(value, offset, length);
    }

    @Override
    public IntStream chars() {
        System.out.println("chars");
        return this.toString().chars();
    }

    @Override
    public IntStream codePoints() {
        System.out.println("codePoints");
        return this.toString().codePoints();
    }

    public SubstringableString substring(int startPosition, int endPosition) {
        if (startPosition == 0 && endPosition - startPosition == length) return this;
        return new SubstringableString(this.value, offset + startPosition, endPosition - startPosition);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        } else {
            if (o instanceof SubstringableString) {
                SubstringableString other = (SubstringableString) o;
                if (this.length == other.length) {
                    for (int i = 0; i < length; i++) {
                        if (this.value[this.offset + i] != other.value[other.offset + i]) {
                            return false;
                        }
                    }
                    return true;
                }
            }
            return false;
        }
    }

    @Override
    public int hashCode() {
        return hash;
    }


    private int getHash() {
        int hash = 0;
        for (int i = 0; i < this.length; i++) {
            hash = 31 * hash + this.value[offset + i];
        }
        return hash;
    }

    protected int getCachedHash() {
        return hash;
    }

    public int elLength() {
        return length;
    }
}
