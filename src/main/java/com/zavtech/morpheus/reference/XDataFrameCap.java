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
package com.zavtech.morpheus.reference;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameCap;

/**
 * An implementation of the DataFrameCap interface which provides a convenient API for capping values in a frame.
 *
 * @param <C>   the column key type
 * @param <R>   the row key type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameCap<R,C> implements DataFrameCap<R,C> {

    private boolean inPlace;
    private DataFrame<R,C> frame;

    /**
     * Constructor
     * @param inPlace   true if capping should be done in place, false to copy & cap
     * @param frame     the frame to operate on
     */
    XDataFrameCap(boolean inPlace, DataFrame<R, C> frame) {
        this.inPlace = inPlace;
        this.frame = frame;
    }


    @Override
    public DataFrame<R,C> ints(int lower, int upper) {
        final DataFrame<R,C> target = inPlace ? frame : frame.copy();
        return target.applyInts(v -> {
            final int value = v.getInt();
            if (value < lower) {
                return lower;
            } else if (value > upper) {
                return upper;
            } else {
                return value;
            }
        });
    }

    @Override
    public DataFrame<R,C> longs(int lower, int upper) {
        final DataFrame<R,C> target = inPlace ? frame : frame.copy();
        return target.applyLongs(v -> {
            final long value = v.getLong();
            if (value < lower) {
                return lower;
            } else if (value > upper) {
                return upper;
            } else {
                return value;
            }
        });
    }

    @Override
    public DataFrame<R,C> doubles(double lower, double upper) {
        final DataFrame<R,C> target = inPlace ? frame : frame.copy();
        return target.applyDoubles(v -> {
            final double value = v.getDouble();
            if (Double.isNaN(value)) {
                return Double.NaN;
            } else if (value < lower) {
                return lower;
            } else if (value > upper) {
                return upper;
            } else {
                return value;
            }
        });
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T extends Comparable> DataFrame<R,C> Values(T lower, T upper) {
        final DataFrame<R,C> target = inPlace ? frame : frame.copy();
        return target.applyValues(v -> {
            final Comparable value = v.getValue();
            if (value == null) {
                return null;
            } else if (value.compareTo(lower) < 0) {
                return lower;
            } else if (value.compareTo(upper) > 0) {
                return upper;
            } else {
                return value;
            }
        });
    }
}
