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
package com.zavtech.morpheus.array;

/**
 * Defines the various styles of arrays supported by the Morpheus library.
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public enum ArrayStyle {

    DENSE,
    SPARSE,
    MAPPED;

    /**
     * Returns true if this represents the DENSE style
     * @return  true if dense
     */
    public boolean isDense() {
        return this == DENSE;
    }

    /**
     * Returns true if this represents the SPARSE style
     * @return  true if sparse
     */
    public boolean isSparse() {
        return this == SPARSE;
    }

    /**
     * Returns true if this represents the MEMORY MAPPED style
     * @return  true if memory mapped
     */
    public boolean isMapped() {
        return this == MAPPED;
    }

    /**
     * Returns the supported types for this array style
     * @return      the supported array types for this style
     */
    public ArrayType[] getSupportedTypes() {
        switch (this) {
            case DENSE:     return ArrayType.values();
            case SPARSE:    return ArrayType.values();
            case MAPPED:    return new ArrayType[] {
                    ArrayType.BOOLEAN,
                    ArrayType.INTEGER,
                    ArrayType.LONG,
                    ArrayType.DOUBLE,
                    ArrayType.DATE,
                    ArrayType.STRING,
                    ArrayType.ENUM,
                    ArrayType.YEAR,
                    ArrayType.CURRENCY,
                    ArrayType.ZONE_ID,
                    ArrayType.TIME_ZONE,
                    ArrayType.INSTANT,
                    ArrayType.LOCAL_DATE,
                    ArrayType.LOCAL_TIME,
                    ArrayType.LOCAL_DATETIME,
                    ArrayType.ZONED_DATETIME
            };
            default:
                throw new IllegalArgumentException("Unsupported style: " + this);
        }
    }

}
