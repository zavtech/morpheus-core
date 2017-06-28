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

import java.lang.ref.WeakReference;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import com.zavtech.morpheus.frame.DataFrameEvent;
import com.zavtech.morpheus.frame.DataFrameListener;
import com.zavtech.morpheus.frame.DataFrameEvents;

/**
 * The default implementation of the DataFrameNotify interface.
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
class XDataFrameEvents implements DataFrameEvents {

    private boolean enabled;
    private transient Set<WeakReference> listenerSet = new LinkedHashSet<>();


    @Override
    public boolean isEnabled() {
        return enabled;
    }


    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }


    @Override
    public final void addDataFrameListener(DataFrameListener listener) {
        synchronized (this) {
            this.listenerSet.add(new WeakReference<>(listener));
        }
    }


    @Override
    public final void removeDataFrameListener(DataFrameListener listener) {
        synchronized (this) {
            for (Iterator<WeakReference> references = listenerSet.iterator() ; references.hasNext(); ) {
                final WeakReference reference = references.next();
                final DataFrameListener target = (DataFrameListener)reference.get();
                if (target == null || listener == target) {
                    references.remove();
                }
            }
        }
    }


    @Override
    public final void fireDataFrameEvent(DataFrameEvent event) {
        synchronized (this) {
            if (listenerSet.size() > 0) {
                for (Iterator<WeakReference> references = listenerSet.iterator() ; references.hasNext(); ) {
                    final WeakReference reference = references.next();
                    final DataFrameListener listener = (DataFrameListener)reference.get();
                    if (listener != null) {
                        listener.onDataFrameEvent(event);
                    } else {
                        references.remove();
                    }
                }
            }
        }
    }
}
