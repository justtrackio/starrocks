package io.justtrack.starrocks;

public class StringEchoDebug {
    public String evaluate(String value) {
        if (value == null) {
            return "<null>";
        }

        return value.length() + ":" + value;
    }
}
