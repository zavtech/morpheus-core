/**
 * Copyright (C) 2014-2017 Xavier Witdouck
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
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


    /**
     * Asserts some condition is true and raises an AssertException if not
     * @param condition     the condition outcome
     */
    public static void assertTrue(boolean condition) {
        if (!condition) {
            fail("Boolean assertion failed", null);
        }
    }


    /**
     * Asserts some condition is true and raises an AssertException if not
     * @param condition     the condition outcome
     * @param message       the message
     */
    public static void assertTrue(boolean condition, String message) {
        if (!condition) {
            fail(message, null);
        }
    }


    /**
     * Asserts that the two values are equals
     * @param actual        the actual value
     * @param expected      the expected value
     * @param message       the error message if not equals
     */
    public static void assertEquals(int actual, int expected, String message) {
        assertEquals(actual, expected, 0d, message);
    }


    /**
     * Asserts that the two values are almost equal based on some threshold
     * @param actual        the actual value
     * @param expected      the expected value
     * @param delta         the delta threshold
     */
    public static void assertEquals(int actual, int expected, double delta) {
        assertEquals(actual, expected, delta, null);
    }


    /**
     * Asserts that the two values are almost equal based on some threshold
     * @param actual        the actual value
     * @param expected      the expected value
     * @param message       the error message if not equals
     * @param delta         the delta threshold
     */
    public static void assertEquals(int actual, int expected, double delta, String message) {
        if (Integer.compare(actual, expected) != 0) {
            if (Math.abs(expected - actual) > delta) {
                fail(message, "Actual value," + actual + ", not equal to expected: " + expected);
            }
        }
    }


    /**
     * Asserts that the two values are equals
     * @param actual        the actual value
     * @param expected      the expected value
     * @param message       the error message if not equals
     */
    public static void assertEquals(double actual, double expected, String message) {
        assertEquals(actual, expected, 0d, message);
    }


    /**
     * Asserts that the two values are almost equal based on some threshold
     * @param actual        the actual value
     * @param expected      the expected value
     * @param delta         the delta threshold
     */
    public static void assertEquals(double actual, double expected, double delta) {
        assertEquals(actual, expected, delta, null);
    }


    /**
     * Asserts that the two values are almost equal based on some threshold
     * @param actual        the actual value
     * @param expected      the expected value
     * @param message       the error message if not equals
     * @param delta         the delta threshold
     */
    public static void assertEquals(double actual, double expected, double delta, String message) {
        if (Double.compare(actual, expected) != 0) {
            if (Math.abs(expected - actual) > delta) {
                fail(message, "Actual value," + actual + ", not equal to expected: " + expected);
            }
        }
    }


    /**
     * Asserts that the two values are equals
     * @param actual        the actual value
     * @param expected      the expected value
     */
    public static void assertEquals(Object actual, Object expected) {
        assertEquals(actual, expected, null);
    }


    /**
     * Asserts that the two values are equals
     * @param actual        the actual value
     * @param expected      the expected value
     * @param message       the error message if not equals
     */
    public static void assertEquals(Object actual, Object expected, String message) {
        if (actual != expected) {
            if (actual != null && expected != null) {
                final Class<?> type1 = actual.getClass();
                final Class<?> type2 = expected.getClass();
                if (type1 != type2) {
                    fail(message, String.format("Type mismatch, %s != %s", type1, type2));
                } else if (!actual.equals(expected)) {
                    fail(message, String.format("Actual and expected value mismatch, %s != %s", type1, type2));
                }
            } else if (actual == null) {
                fail(message, String.format("Actual value is null, expected value = %s", expected));
            } else {
                fail(message, String.format("Expected value is null, actual value = %s", actual));
            }
        }
    }


    /**
     * Throws an AssertException with the message specified
     * @param message       the message string
     * @param reason        the reason string
     */
    private static void fail(String message, String reason) {
        if (message != null && reason != null) {
            throw new AssertException(message + " - " + reason);
        } else if (message != null) {
            throw new AssertException(message);
        } else {
            throw new AssertException(reason);
        }
    }

}
