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

import java.util.ArrayList;
import java.util.List;

import com.zavtech.morpheus.frame.DataFrameEvent;
import com.zavtech.morpheus.frame.DataFrameListener;
import org.testng.annotations.BeforeMethod;

/**
 * A unit test to assess the matrix notification functionality
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class EventTests implements DataFrameListener {

    private List<DataFrameEvent> eventList = new ArrayList<DataFrameEvent>();


    /** @inheritDoc */
    public void onDataFrameEvent(DataFrameEvent event) {
        this.eventList.add(event);
    }


    @BeforeMethod()
    public void reset() {
        this.eventList.clear();
    }


}
