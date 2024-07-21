package FastRFD.input;

import FastRFD.helpers.ParserHelper;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;


public class Column {

    public enum Type {
        STRING, NUMERIC, DOUBLE
    }

    private final String tableName;
    private final String name;
    private HashSet<String> valueSet = new HashSet<>();
    private List<String> values = new ArrayList<>();
    private Type type = Type.NUMERIC;

    public Type getType() {
        if (name.contains("String"))
            return Type.STRING;
        if (name.contains("Double"))
            return Type.DOUBLE;
        if (name.contains("Integer"))
            return Type.NUMERIC;
        return type;
    }

    public Column(String tableName, String name) {
        this.tableName = tableName;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return tableName + "." + name;
    }

    public void addLine(String string) {
        if (type == Type.NUMERIC && !ParserHelper.isDouble(string))
            type = Type.STRING;
        valueSet.add(string);
        values.add(string);
    }

    public int size() {
        return values.size();
    }

    public Comparable<?> getValue(int line) {
        switch (type) {
            case DOUBLE:
                return Double.parseDouble(values.get(line));
            case NUMERIC:
                return Integer.parseInt(values.get(line));
            case STRING:
            default:
                return values.get(line);
        }
    }

    public Integer getInteger(int line) {
        return values.get(line).isEmpty() ? 0 : Integer.parseInt(values.get(line));
    }

    public Double getDouble(int line) {
        return values.get(line).isEmpty() ? 0 : Double.parseDouble(values.get(line));
    }

    public String getString(int line) {
        return values.get(line) == null ? "" : values.get(line);
    }

    public int getLineCount() {
        return values.size();
    }
}