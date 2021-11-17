package com.danifoldi.dataverse.dml;

public class DmlParseException extends Exception {

    public DmlParseException(int line, int column) {
        super("DML parsing failed at %d:%d".formatted(line, column));
    }

    public DmlParseException(int line, int column, String reason) {
        super("DML parsing failed at %d:%d (%s)".formatted(line, column, reason));
    }

    public DmlParseException(int line, int column, String expect, String found) {
        super("DML parsing failed at %d:%d (Expected %s, found %s)".formatted(line, column, expect, found));
    }
}
