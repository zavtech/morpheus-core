/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.zavtech.morpheus.util;

/**
 * A utility class that can be used to make various assertions to trigger exceptions early.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author Xavier Witdouck
 */
public class Asserts {

    /**
     * Checks an expression is true otherwise throws an exception
     * @param expression    the expression to check is true
     * @param message       the exception message if expression is false
     * @throws AssertException    if expression is false
     */
    public static void check(boolean expression, String message) {
        if (!expression) {
            throw new AssertException(message);
        }
    }

    /**
     * Checks an expression is true otherwise throws an exception
     * @param expression    the expression to check is true
     * @param message       the formatted exception message if expression is false
     * @param args          the argumets for the formatted string
     * @throws AssertException    if expression is false
     */
    public static void check(boolean expression, String message, Object... args) {
        if (!expression) {
            throw new AssertException(String.format(message, args));
        }
    }

    /**
     * Checks that an object reference is not null
     * @param object    the object reference to check
     * @param message   the exception message if object is null
     * @return          the same as the argument
     * @throws AssertException    if the object is null
     */
    public static <T> T notNull(T object, String message) {
        if (object == null) {
            throw new AssertException(message);
        } else {
            return object;
        }
    }

    /**
     * Checks that a string is not null and its trimmed length > 0
     * @param text  the text to check
     * @param message   the exception message if empty
     */
    public static void notEmpty(String text, String message) {
        if (text == null || text.trim().length() == 0) {
            throw new AssertException(message);
        }
    }

    public static void assertEquals(double actual, double expected, String message) {
        assertEquals(actual, expected, 0d, message);
    }


    public static void assertEquals(double actual, double expected, double delta) {
        if (Double.isInfinite(expected)) {
            if (expected != actual) {
                fail("Assertion failed", "Actual value," + actual + ", not equal to expected: " + expected);
            }
        } else if (Math.abs(expected - actual) > delta) {
            fail("Assertion failed", "Actual value," + actual + ", not equal to expected: " + expected);
        }
    }


    public static void assertEquals(double actual, double expected, double delta, String message) {
        if (Double.isInfinite(expected)) {
            if (expected != actual) {
                fail(message, "Actual value," + actual + ", not equal to expected: " + expected);
            }
        } else if (Math.abs(expected - actual) > delta) {
            fail(message, "Actual value," + actual + ", not equal to expected: " + expected);
        }
    }


    private static void fail(String message, String reason) {
        throw new AssertException(message + " - " + reason);
    }


}
