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

import java.util.Arrays;
import java.io.Serializable;
import java.util.Objects;

/**
 * An immutable object that maintains a fixed collection of items
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface Tuple extends Comparable<Tuple>, Serializable {

    /**
     * Returns a newly created tuple from elements of the array
     * @param items    the input array to sample from
     * @return          the newly created tuple
     */
    static Tuple of(Object... items) {
        switch (items.length) {
            case 0:     return TupleN.empty();
            case 1:     return new Tuple1(items[0]);
            case 2:     return new Tuple2(items[0], items[1]);
            case 3:     return new Tuple3(items[0], items[1], items[2]);
            case 4:     return new Tuple4(items[0], items[1], items[2], items[3]);
            case 5:     return new Tuple5(items[0], items[1], items[2], items[3], items[4]);
            case 6:     return new Tuple6(items[0], items[1], items[2], items[3], items[4], items[5]);
            case 7:     return new Tuple7(items[0], items[1], items[2], items[3], items[4], items[5], items[6]);
            default:    return new TupleN(items);
        }
    }

    /**
     * Returns an empty tuple with size == 0
     * @return  an empty tuple of size == 0
     */
    static Tuple empty() {
        return TupleN.empty();
    }

    /**
     * Returns the number of values in this tuple
     * @return  the number of values in this tuple
     */
    int size();

    /**
     * Returns the item for the index specified
     * @param index     the index of element
     * @return          the element value
     */
    <T> T item(int index);

    /**
     * Returns a filter of this tuple based on the offset and length
     * @param offset    the offset value for start
     * @param length    the number of items to include
     * @return          the Tuple selection
     */
    Tuple filter(int offset, int length);


    @Override
    @SuppressWarnings("unchecked")
    default int compareTo(Tuple other) {
        try {
            final int length = Math.max(size(), other.size());
            for (int i=0; i<length; ++i) {
                final Object v1 = i < this.size() ? item(i) : null;
                final Object v2 = i < other.size() ? other.item(i) : null;
                if (v1 != v2) {
                    if      (v1 == null) return -1;
                    else if (v2 == null) return 1;
                    else if (v1.getClass() == v2.getClass() && v1 instanceof Comparable) {
                        final int result = ((Comparable)v1).compareTo(v2);
                        if (result != 0) {
                            return result;
                        }
                    } else {
                        final String s1 = v1.toString();
                        final String s2 = v2.toString();
                        final int result = s1.compareTo(s2);
                        if (result != 0) {
                            return result;
                        }
                    }
                }
            }
            return 0;
        } catch (Exception ex) {
            throw new RuntimeException(String.format("Failed to compare two Tuple values for order: %s vs %s", this, other));
        }
    }


    /**
     * A convenuence base class for building Tuple implementations
     */
    abstract class TupleBase implements Tuple {

        private int hashCode;

        /**
         * Constructor
         * @param hashCode  the hash code for this entity
         */
        TupleBase(int hashCode) {
            this.hashCode = hashCode;
        }

        @Override
        public Tuple filter(int offset, int length) {
            if (offset < 0) {
                throw new IllegalArgumentException("The offset must be >= 0");
            } else if (length < 0) {
                throw new IllegalArgumentException("The length must be >= 0");
            } else if (length == 0) {
                return Tuple.empty();
            } else if (offset == 0 && length == size()) {
                return this;
            } else if (length > size() - offset) {
                throw new IllegalArgumentException("Offset and length exceeds bounds of Tuple: " + offset + " length: " + length);
            } else {
                final Object[] itens = new Object[length];
                for (int i=0; i<length; ++i) {
                    itens[i] = item(offset + i);
                }
                return Tuple.of(itens);
            }
        }

        @Override
        public String toString() {
            final StringBuilder text = new StringBuilder();
            text.append("(");
            for (int i=0; i<size(); ++i) {
                final Object item = item(i);
                text.append(item);
                if (i < size()-1) {
                    text.append(",");
                }
            }
            text.append(")");
            return text.toString();
        }

        @Override
        public final int hashCode() {
            return hashCode;
        }

        @Override
        public final boolean equals(Object other) {
            final int size = this.size();
            if (other instanceof Tuple && ((Tuple)other).size() == size) {
                final Tuple tuple = (Tuple)other;
                for (int i=0; i<size; ++i) {
                    final Object v1 = item(i);
                    final Object v2 = tuple.item(i);
                    if (!Objects.equals(v1, v2)) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }
    }


    /**
     * A Tuple implementation that holds 1 item
     */
    class Tuple1 extends TupleBase {

        private Object item0;

        /**
         * Constructor
         * @param item0     the single value for this Tuple
         */
        Tuple1(Object item0) {
            super(item0 != null ? item0.hashCode() : 1);
            this.item0 = item0;
        }

        @Override
        public final int size() {
            return 1;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0:     return (T) item0;
                default:    throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 1");
            }
        }
    }


    /**
     * A Tuple implementation that holds 2 items
     */
    class Tuple2 extends TupleBase {

        private Object item0;
        private Object item1;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         */
         Tuple2(Object item0, Object item1) {
             super(Objects.hash(item0, item1));
             this.item0 = item0;
             this.item1 = item1;
        }

        @Override
        public final int size() {
            return 2;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 2");
            }
        }
    }


    /**
     * A Tuple implementation that holds 3 items
     */
    class Tuple3 extends TupleBase {

        private Object item0;
        private Object item1;
        private Object item2;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         * @param item2 the third item
         */
        Tuple3(Object item0, Object item1, Object item2) {
            super(Objects.hash(item0, item1, item2));
            this.item0 = item0;
            this.item1 = item1;
            this.item2 = item2;
        }

        @Override
        public final int size() {
            return 3;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                case 2: return (T) item2;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 3");
            }
        }
    }


    /**
     * A Tuple implementation that holds 4 items
     */
    class Tuple4 extends TupleBase {

        private Object item0;
        private Object item1;
        private Object item2;
        private Object item3;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         * @param item2 the third item
         * @param item3 the fourth item
         */
        Tuple4(Object item0, Object item1, Object item2, Object item3) {
            super(Objects.hash(item0, item1, item2, item3));
            this.item0 = item0;
            this.item1 = item1;
            this.item2 = item2;
            this.item3 = item3;
        }

        @Override
        public final int size() {
            return 4;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                case 2: return (T) item2;
                case 3: return (T) item3;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 4");
            }
        }
    }


    /**
     * A Tuple implementation that holds 5 items
     */
    class Tuple5 extends TupleBase {

        private Object item0;
        private Object item1;
        private Object item2;
        private Object item3;
        private Object item4;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         * @param item2 the third item
         * @param item3 the fourth item
         * @param item4 the fifth item
         */
        Tuple5(Object item0, Object item1, Object item2, Object item3, Object item4) {
            super(Objects.hash(item0, item1, item2, item3, item4));
            this.item0 = item0;
            this.item1 = item1;
            this.item2 = item2;
            this.item3 = item3;
            this.item4 = item4;
        }

        @Override
        public final int size() {
            return 5;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                case 2: return (T) item2;
                case 3: return (T) item3;
                case 4: return (T) item4;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 4");
            }
        }
    }



    /**
     * A Tuple implementation that holds 6 items
     */
    class Tuple6 extends TupleBase {

        private Object item0;
        private Object item1;
        private Object item2;
        private Object item3;
        private Object item4;
        private Object item5;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         * @param item2 the third item
         * @param item3 the fourth item
         * @param item4 the fifth item
         * @param item5 the sizth item
         */
        Tuple6(Object item0, Object item1, Object item2, Object item3, Object item4, Object item5) {
            super(Objects.hash(item0, item1, item2, item3, item4, item5));
            this.item0 = item0;
            this.item1 = item1;
            this.item2 = item2;
            this.item3 = item3;
            this.item4 = item4;
            this.item5 = item5;
        }

        @Override
        public final int size() {
            return 6;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                case 2: return (T) item2;
                case 3: return (T) item3;
                case 4: return (T) item4;
                case 5: return (T) item5;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 6");
            }
        }
    }


    /**
     * A Tuple implementation that holds 7 items
     */
    class Tuple7 extends TupleBase {

        private Object item0;
        private Object item1;
        private Object item2;
        private Object item3;
        private Object item4;
        private Object item5;
        private Object item6;

        /**
         * Constructor
         * @param item0 the first item
         * @param item1 the second item
         * @param item2 the third item
         * @param item3 the fourth item
         * @param item4 the fifth item
         * @param item5 the sizth item
         * @param item6 the sizth item
         */
        Tuple7(Object item0, Object item1, Object item2, Object item3, Object item4, Object item5, Object item6) {
            super(Objects.hash(item0, item1, item2, item3, item4, item5, item6));
            this.item0 = item0;
            this.item1 = item1;
            this.item2 = item2;
            this.item3 = item3;
            this.item4 = item4;
            this.item5 = item5;
            this.item6 = item6;
        }

        @Override
        public final int size() {
            return 7;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            switch (index) {
                case 0: return (T) item0;
                case 1: return (T) item1;
                case 2: return (T) item2;
                case 3: return (T) item3;
                case 4: return (T) item4;
                case 5: return (T) item5;
                case 6: return (T) item6;
                default: throw new IllegalArgumentException("The tuple index is out of bounds: " + index + ", size = 6");
            }
        }
    }


    /**
     * A Tuple implementation that can hold any number of items
     */
    class TupleN extends TupleBase {

        private static final long serialVersionUID = 1L;

        private static Tuple empty = new TupleN();

        private Object[] items;

        /**
         * Constructor
         * @param items the items for this tuple
         */
        TupleN(Object... items) {
            super(Arrays.hashCode(items));
            this.items = items;
        }

        /**
         * Returns a reference to the empty Tuple
         * @return  the empty Tuple
         */
        static Tuple empty() {
            return empty;
        }

        @Override
        public final int size() {
            return items.length;
        }

        @Override
        @SuppressWarnings("unchecked")
        public final <T> T item(int index) {
            return (T)items[index];
        }
    }


}
