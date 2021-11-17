package com.danifoldi.dataverse.dml;

import org.w3c.dom.CDATASection;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DmlParser {
    private final String dmlString;
    private int pointer;
    private int line;
    private int column;

    private DmlParser(String dmlString) {
        this.dmlString = dmlString + "\n";
        line = 1;
        column = 1;
        pointer = 0;
    }

    private void step() {
        if (currentChar() == '\n') {
            column = 0;
            line++;
        }

        column++;
        pointer++;
    }

    private void step(int count) {
        for (int i = 0; i < count; i++) {
            step();
        }
    }

    private void stepNextNonWhitespace() {
        while (pointer < dmlString.length() && isWhitespace(currentChar())) {
            step();
        }
    }

    private int nextNonWhitespace(int pointer) {
        while (pointer < dmlString.length() && isWhitespace(charAt(pointer))) {
            pointer++;
        }

        return pointer;
    }

    private char nextNonWhitespaceChar(int pointer) {
        while (pointer < dmlString.length() && isWhitespace(charAt(pointer))) {
            pointer++;
        }

        return charAt(pointer);
    }

    private char charAt(int pointer) {
        return dmlString.charAt(pointer);
    }

    private char currentChar() {
        return charAt(pointer);
    }

    private char nextChar() {
        return charAt(pointer + 1);
    }

    private boolean isCommentStart(char c, char n) {
        return c == '/' && (n == '/' || n == '*');
    }

    private boolean isMultilineCommentEnd(char c, char n) {
        return c == '*' && n == '/';
    }

    private boolean isWhitespace(char c) {
        return String.valueOf(c).matches("\\s");
    }

    private boolean isDigit(char c) {
        return String.valueOf(c).matches("[0-9]");
    }

    private boolean isNewline(char c) {
        return c == '\n';
    }

    private boolean isString(char c) {
        return c == '"' || c == '\'';
    }

    private boolean isKey(char c) {
        return String.valueOf(c).matches("[a-zA-Z0-9-_]");
    }


    private DmlComment parseComment() throws DmlParseException {
        StringBuilder comment = new StringBuilder();

        stepNextNonWhitespace();

        while (isCommentStart(currentChar(), nextChar())) {
            switch (nextChar()) {
                case '/' -> {
                    step();
                    step();
                    int start = pointer;
                    while (!isNewline(currentChar())) {
                        step();
                    }
                    comment.append(dmlString, start, pointer);
                }
                case '*' -> {
                    step();
                    step();
                    int start = pointer;
                    while (!isMultilineCommentEnd(currentChar(), nextChar())) {
                        step();
                    }
                    step();
                    comment.append(dmlString, start, pointer);
                }
            }
        }

        return new DmlComment(comment.toString());
    }

    private DmlBoolean parseBoolean(DmlComment comment) throws DmlParseException {
        if (dmlString.substring(pointer, pointer + 4).equalsIgnoreCase("true")) {
            step(4);
            DmlBoolean bool = new DmlBoolean(true);
            bool.comment(comment);
            return bool;
        }
        if (dmlString.substring(pointer, pointer + 5).equalsIgnoreCase("false")) {
            step(5);
            DmlBoolean bool = new DmlBoolean(false);
            bool.comment(comment);
            return bool;
        }
        throw new DmlParseException(line, column, "true or false", dmlString.substring(pointer, pointer + 4));
    }

    private DmlNumber parseNumber(DmlComment comment) throws DmlParseException {
        stepNextNonWhitespace();
        boolean negative = currentChar() == '-';
        if (negative) {
            step();
        }
        String whole;
        String fraction = "0";
        String exponent = "0";

        int start = pointer;
        while (isDigit(currentChar())) {
            step();
        }
        whole = pointer > start ? dmlString.substring(start, pointer) : "0";
        if (currentChar() == '.') {
            step();
            start = pointer;
            while (isDigit(currentChar())) {
                step();
            }
            fraction = pointer > start ? dmlString.substring(start, pointer) : "0";
        }
        if (currentChar() == 'e') {
            step();
            start = pointer;
            while (isDigit(currentChar())) {
                step();
            }
            boolean nexp = currentChar() == '-';
            if (nexp) {
                step();
            }
            exponent = (nexp ? "-" : "") + (pointer > start ? dmlString.substring(start, pointer) : "0");
        }
        BigDecimal b = new BigDecimal(whole + "." + fraction);
        if (negative) {
            b = b.negate();
        }
        b = b.scaleByPowerOfTen(Integer.parseInt(exponent));
        DmlNumber number = new DmlNumber(b);
        number.comment(comment);
        return number;
    }

    private DmlString parseString(DmlComment comment) throws DmlParseException {
        stepNextNonWhitespace();
        switch (charAt(pointer)) {
            case '\'' -> {
                int start = pointer;
                do {
                    step();
                } while (charAt(pointer) != '\'');
                step();
                DmlString string = new DmlString(dmlString.substring(start + 1, pointer - 1));
                string.comment(comment);
                return string;
            }
            case '"' -> {
                int start = pointer;
                do {
                    step();
                } while (charAt(pointer) != '"');
                step();
                DmlString string = new DmlString(dmlString.substring(start + 1, pointer - 1));
                string.comment(comment);
                return string;
            }
            default -> throw new DmlParseException(line, column, "' or \"", String.valueOf(charAt(pointer)));
        }
    }

    private DmlKey parseKey(DmlComment comment) throws DmlParseException {
        stepNextNonWhitespace();
        int start = pointer;
        while (isKey(charAt(pointer))) {
            step();
        }
        DmlKey key = new DmlKey(dmlString.substring(start, pointer));
        key.comment(comment);
        return key;
    }

    private DmlValue parseValue(DmlComment comment) throws DmlParseException {
        stepNextNonWhitespace();
        return switch (charAt(pointer)) {
            case '{' -> parseObject(comment);
            case '[' -> parseArray(comment);
            case '\'', '"' -> parseString(comment);
            case 't', 'f' -> parseBoolean(comment);
            default -> parseNumber(comment);
        };
    }

    private DmlObject parseObjectValue() throws DmlParseException {
        DmlObject object = new DmlObject(new HashMap<>());
        while (nextNonWhitespace(pointer) != '}' && pointer < dmlString.length()) {
            stepNextNonWhitespace();
            DmlComment comment = parseComment();
            DmlKey key = parseKey(comment);
            stepNextNonWhitespace();
            if (currentChar() != ':') {
                throw new DmlParseException(line, column, ":", String.valueOf(currentChar()));
            }
            step();
            DmlComment comment1 = parseComment();
            DmlValue value = parseValue(comment1);
            object.add(key, value);
            int sline = line;
            stepNextNonWhitespace();
            if (currentChar() == '}') {
                break;
            }
            if (currentChar() != ',' && sline == line) {
                throw new DmlParseException(line, column, ",", String.valueOf(currentChar()));
            }
        }
        return object;
    }

    private DmlObject parseObject(DmlComment comment) throws DmlParseException {
        step();
        if (comment == null) {
            comment = parseComment();
        }
        DmlObject object = parseObjectValue();
        object.comment(comment);

        if (charAt(nextNonWhitespace(pointer)) != '}') {
            throw new DmlParseException(line, column, "}", String.valueOf(charAt(nextNonWhitespace(pointer))));
        }
        stepNextNonWhitespace();
        step();
        return object;
    }

    private DmlArray parseArray(DmlComment comment) throws DmlParseException {
        step();
        if (comment == null) {
            comment = parseComment();
        }
        DmlArray array = new DmlArray(new ArrayList<>());
        array.comment(comment);

        while (charAt(nextNonWhitespace(pointer)) != ']') {
            stepNextNonWhitespace();
            DmlComment vcomment = parseComment();
            array.add(parseValue(vcomment));
            stepNextNonWhitespace();
            if (currentChar() == ']') {
                break;
            }
            if (currentChar() != ',') {
                throw new DmlParseException(line, column, ",", String.valueOf(currentChar()));
            }
            step();
        }

        if (charAt(nextNonWhitespace(pointer)) != ']') {
            throw new DmlParseException(line, column, "]", String.valueOf(charAt(nextNonWhitespace(pointer))));
        }
        stepNextNonWhitespace();
        step();
        return array;
    }

    private DmlValue parseDocument() throws DmlParseException {
        stepNextNonWhitespace();
        DmlValue result;

        switch (nextNonWhitespaceChar(pointer)) {
            case '{':
                result = parseObject(null);
                break;
            case '[':
                result = parseArray(null);
                break;
            default:
                if (isString(charAt(pointer)) || isKey(charAt(pointer))) {
                    result = parseObjectValue();
                } else {
                    throw new DmlParseException(line, column, "{, [, \", ' or key", String.valueOf(charAt(pointer)));
                }
        }

        if (nextNonWhitespace(pointer) < dmlString.length()) {
            throw new DmlParseException(line, column, "document end", String.valueOf(charAt(nextNonWhitespace(pointer))));
        }

        return result;
    }

    public static DmlValue parse(String dmlString) throws DmlParseException {
        return new DmlParser(dmlString).parseDocument();
    }
}



abstract class DmlValue {
}

abstract class DmlCommentableValue extends DmlValue {
    private DmlComment comment;

    public void comment(DmlComment comment) {
        this.comment = comment;
    }

    public DmlComment comment() {
        return this.comment;
    }
}

class DmlObject extends DmlCommentableValue {
    private Map<DmlKey, DmlValue> value;

    public DmlObject(Map<DmlKey, DmlValue> value) {
        this.value = value;
    }

    public Map<DmlKey, DmlValue> value() {
        return this.value;
    }

    public void value(Map<DmlKey, DmlValue> value) {
        this.value = value;
    }

    public void add(DmlKey key, DmlValue value) {
        this.value.put(key, value);
    }

    @Override
    public String toString() {
        return "DmlObject{" +
                "value=" + value +
                '}';
    }
}

class DmlArray extends DmlCommentableValue {
    private List<DmlValue> value;

    public DmlArray(List<DmlValue> value) {
        this.value = value;
    }

    public List<DmlValue> value() {
        return this.value;
    }

    public void value(List<DmlValue> value) {
        this.value = value;
    }

    public void add(DmlValue value) {
        this.value.add(value);
    }

    @Override
    public String toString() {
        return "DmlArray{" +
                "value=" + value +
                '}';
    }
}

class DmlString extends DmlCommentableValue {
    private String value;

    public DmlString(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public void value(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DmlString{" +
                "value='" + value + '\'' +
                '}';
    }
}

class DmlKey extends DmlCommentableValue {
    private String value;

    public DmlKey(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public void value(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DmlKey{" +
                "value='" + value + '\'' +
                '}';
    }
}

class DmlNumber extends DmlCommentableValue {
    private BigDecimal value;

    public DmlNumber(BigDecimal value) {
        this.value = value;
    }

    public BigDecimal value() {
        return this.value;
    }

    public void value(BigDecimal value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DmlNumber{" +
                "value=" + value +
                '}';
    }
}

class DmlBoolean extends DmlCommentableValue {
    private boolean value;

    public DmlBoolean(boolean value) {
        this.value = value;
    }

    public boolean value() {
        return this.value;
    }

    public void value(boolean value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DmlBoolean{" +
                "value=" + value +
                '}';
    }
}

class DmlComment extends DmlValue {
    private String value;

    public DmlComment(String value) {
        this.value = value;
    }

    public String value() {
        return this.value;
    }

    public void value(String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "DmlComment{" +
                "value='" + value + '\'' +
                '}';
    }
}