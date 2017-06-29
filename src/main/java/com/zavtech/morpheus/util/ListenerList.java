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

import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * A listener list used to maintain a set of event listeners with support for weak references.
 *
 * @param <T>   the listener type
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 *
 * @author  Xavier Witdouck
 */
public class ListenerList<T> {

    private transient Set<WeakReference<T>> listenerSet = new LinkedHashSet<>();

    public final void addListener(T listener) {
        synchronized (this) {
            this.listenerSet.add(new WeakReference<>(listener));
        }
    }

    public final void removeListener(T listener) {
        synchronized (this) {
            for (Iterator<WeakReference<T>> references = listenerSet.iterator(); references.hasNext(); ) {
                final WeakReference<T> reference = references.next();
                final T target = reference.get();
                if (target == null || listener == target) {
                    references.remove();
                }
            }
        }
    }

    /**
     * Returns the stream of listeners for this list
     * @return  the stream of registered listeners
     */
    public final Stream<T> stream() {
        if (listenerSet.size() == 0) {
            return Stream.empty();
        } else {
            final List<T> listeners = new ArrayList<>(listenerSet.size());
            for (Iterator<WeakReference<T>> references = listenerSet.iterator() ; references.hasNext(); ) {
                final WeakReference<T> reference = references.next();
                final T listener = reference.get();
                if (listener != null) {
                    listeners.add(listener);
                } else {
                    references.remove();
                }
            }
            return listeners.stream();
        }
    }
}

