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

import java.io.OutputStream;
import java.util.function.Consumer;

import com.zavtech.morpheus.frame.DataFrame;
import com.zavtech.morpheus.frame.DataFrameOutput;
import com.zavtech.morpheus.util.text.Formats;

/**
 * The reference implementation of the DataFrameIO interface
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameOutput<R,C> implements DataFrameOutput<R,C> {

    private Formats formats;
    private DataFrame<R,C> frame;

    /**
     * Constructor
     * @param frame     the frame to operate on
     * @param formats   the formats for this output
     */
    XDataFrameOutput(DataFrame<R,C> frame, Formats formats) {
        this.frame = frame;
        this.formats = formats;
    }

    @Override()
    public final void print() {
        this.print(10, System.out);
    }


    @Override()
    public final void print(int maxRows) {
        this.print(maxRows, System.out);
    }


    @Override
    public void print(int maxRows, OutputStream stream) {
        this.print(maxRows, stream, null);
    }


    @Override
    public void print(Consumer<Formats> formatting) {
        this.print(10, System.out, formatting);
    }


    @Override
    public void print(int maxRows, Consumer<Formats> formatting) {
        this.print(maxRows, System.out, formatting);
    }


    @Override
    public final void print(int maxRows, OutputStream stream, Consumer<Formats> formatting) {
        if (formatting != null) formatting.accept(formats);
        final XDataFramePrinter printer = new  XDataFramePrinter(maxRows, formats, stream);
        printer.print(frame);
    }

}
