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

import java.util.Comparator;
import java.util.Currency;

/**
 * A class that defines some commonly used Comparators
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class Comparators {

    @SuppressWarnings("unchecked")
    private static Comparator<Object> defaultComparator = (o1, o2) -> {
        final Comparable c1 = (Comparable) o1;
        final Comparable c2 = (Comparable) o2;
        return (c1 == null ? c2 == null ? 0 : -1 : c2 == null ? 1 : c1.compareTo(c2));
    };


    @SuppressWarnings("unchecked")
    private static Comparator<Object> smartComparator = (o1, o2) -> {
        final Comparable c1 = o1 instanceof Comparable ? (Comparable)o1 : o1 != null ? o1.toString() : null;
        final Comparable c2 = o2 instanceof Comparable ? (Comparable)o2 : o2 != null ? o2.toString() : null;
        return (c1 == null ? c2 == null ? 0 : -1 : c2 == null ? 1 : c1.compareTo(c2));
    };


    private static Comparator<Currency> currencyComparator = (c1, c2) -> {
        final String s1 = c1 != null ? c1.getCurrencyCode() : null;
        final String s2 = c2 != null ? c2.getCurrencyCode() : null;
        return (s1 == null ? s2 == null ? 0 : -1 : s2 == null ? 1 : s1.compareTo(s2));
    };


    /**
     * Returns the default comparator for the type specified
     * @param type      the data type for comparator
     * @param <T>       the type
     * @return          the comparator for natural ordering
     */
    @SuppressWarnings("unchecked")
    public static <T> Comparator<T> getDefaultComparator(Class<T> type) {
        if (Comparable.class.isAssignableFrom(type)) {
            return (Comparator<T>)defaultComparator;
        } else if (type == Currency.class) {
            return (Comparator<T>)currencyComparator;
        } else {
            return (Comparator<T>)smartComparator;
        }
    }

}
