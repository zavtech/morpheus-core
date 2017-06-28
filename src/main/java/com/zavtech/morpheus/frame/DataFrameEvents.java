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

/**
 * The interface to manage event notifications for a DataFrame
 *
 * <p>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></p>
 *
 * @author  Xavier Witdouck
 */
public interface DataFrameEvents {

    /**
     * Returns true if event notifications are enabled
     * @return  true if event notifications are enabled
     */
    boolean isEnabled();

    /**
     * Sets whether event notifications are enabled/disabled
     * @param notifying true to enable event notifications
     */
    void setEnabled(boolean notifying);

    /**
     * Adds a listener to be notified of <code>DataFrame</code> events.
     * @param listener      the listener reference
     */
    void addDataFrameListener(DataFrameListener listener);

    /**
     * Removes a listener so it is no longer notified of <code>DataFrame</code> events
     * @param listener      the listener reference
     */
    void removeDataFrameListener(DataFrameListener listener);

    /**
     * Called to notify all registered listeners of a <code>DataFrame</code> event
     * @param event     the DataFrame event (cannot be null).
     */
    void fireDataFrameEvent(DataFrameEvent event);

}
