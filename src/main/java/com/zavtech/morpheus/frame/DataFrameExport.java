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
package com.zavtech.morpheus.frame;

import org.apache.commons.math3.linear.RealMatrix;

import com.zavtech.morpheus.jama.Matrix;

/**
 * An interface that can export a <code>DataFrame</code> as other common used representations
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameExport {

    /**
     * Returns a Jama Matrix representation of the DataFrame
     * @return      the Jama Matrix representation of the DataFrame
     */
    Matrix asMatrix();

    /**
     * Returns a Apache Commons Math <code>RealMatrix</code> representation of this <code>DataFrame</code>
     * @return      the <code>RealMatrix</code> representing this <code>DataFrame</code>
     */
    RealMatrix asApacheMatrix();



}
