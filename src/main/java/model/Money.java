package model;

import java.util.Locale;

/**
 * Money is stored everywhere as integer cents (exact arithmetic, no double rounding errors).
 * This class only formats cents for display.
 */
public final class Money {
    private Money() { }

    public static String format(long cents) {
        return String.format(Locale.US, "$%d.%02d", cents / 100, cents % 100);
    }
}
