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

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.time.LocalDate;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonParseException;
import java.lang.reflect.Type;
import java.time.ZonedDateTime;

/**
 * A simple API abstraction for serializing and de-serializing objects to and from Json
 *
 * @author Xavier Witdouck
 *
 * <p><strong>This is open source software released under the <a href="http://www.apache.org/licenses/LICENSE-2.0">Apache 2.0 License</a></strong></p>
 */
public class Json {

    private Gson gson;
    private GsonBuilder builder;

    /**
     * Constructor
     */
    public Json() {
        this.builder = new GsonBuilder();
        this.builder.registerTypeAdapter(LocalDate.class, new LocalDateDeserializer());
        this.builder.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeDeserializer());
        this.gson = builder.create();
    }

    /**
     * Returns an object of the type specified parsed as JSON from the URL
     * @param url       the URL to parse JSON content from
     * @param type      the type to unmarshal json to
     * @param <T>       the class type
     * @return          the newly created object of type T
     */
    public <T> T parse(URL url, Class<T> type) throws IOException {
        Reader reader = null;
        try {
            reader = new BufferedReader(new InputStreamReader(url.openStream()));
            return gson.fromJson(reader, type);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
    }


    private class LocalDateDeserializer implements JsonDeserializer<LocalDate> {
        public LocalDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : LocalDate.parse(json.getAsJsonPrimitive().getAsString());
        }
    }

    private class ZonedDateTimeDeserializer implements JsonDeserializer<ZonedDateTime> {
        public ZonedDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : ZonedDateTime.parse(json.getAsJsonPrimitive().getAsString());
        }
    }

}
