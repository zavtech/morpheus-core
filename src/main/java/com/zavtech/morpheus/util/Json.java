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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.time.LocalDate;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonNull;
import com.google.gson.JsonParseException;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.gson.stream.JsonReader;

import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Currency;
import java.util.TimeZone;

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
        this.gson = builder().create();
    }


    /**
     * Returns a newly created GsonBuilder configured with some default serializers / deserializers
     * @return      the newly created GsonBuilder which can be further custimized
     */
    public static GsonBuilder builder() {
        final GsonBuilder builder = new GsonBuilder();
        builder.registerTypeAdapter(Currency.class, new CurrencySerializer());
        builder.registerTypeAdapter(ZoneId.class, new ZoneIdSerializer());
        builder.registerTypeAdapter(TimeZone.class, new TimeZoneSerializer());
        builder.registerTypeAdapter(LocalTime.class, new LocalTimeSerializer(DateTimeFormatter.ISO_LOCAL_TIME));
        builder.registerTypeAdapter(LocalDate.class, new LocalDateSerializer(DateTimeFormatter.ISO_LOCAL_DATE));
        builder.registerTypeAdapter(LocalDateTime.class, new LocalDateTimeSerializer(DateTimeFormatter.ISO_LOCAL_DATE_TIME));
        builder.registerTypeAdapter(ZonedDateTime.class, new ZonedDateTimeSerializer(DateTimeFormatter.ISO_ZONED_DATE_TIME));
        return builder;
    }

    /**
     * Returns a newly created Google GSON JsonReader
     * @param is        the input stream to create reader from
     * @return          the JsonReader
     * @throws IOException  if there is an I/O exception
     */
    public static JsonReader reader(InputStream is) throws IOException {
        return reader(is, "UTF-8");
    }

    /**
     * Returns a newly created Google GSON JsonReader
     * @param is        the input stream to create reader from
     * @param encoding  the charset encoding
     * @return          the JsonReader
     * @throws IOException  if there is an I/O exception
     */
    public static JsonReader reader(InputStream is, String encoding) throws IOException {
        if (is instanceof BufferedInputStream) {
            return new JsonReader(new InputStreamReader(is, encoding));
        } else {
            return new JsonReader(new InputStreamReader(new BufferedInputStream(is), encoding));
        }
    }

    /**
     * Returns an object of the type specified parsed as JSON from the URL
     * @param url       the URL to parse JSON content from
     * @param type      the type to unmarshal json to
     * @param <T>       the class type
     * @return          the newly created object of type T
     */
    public <T> T parse(URL url, Class<T> type) throws IOException {
        try (Reader reader = new BufferedReader(new InputStreamReader(url.openStream()))) {
            return gson.fromJson(reader, type);
        }
    }




    /**
     * A GSON Deserializer for Currency objects
     */
    private static class CurrencySerializer implements JsonDeserializer<Currency>, JsonSerializer<Currency> {

        @Override
        public Currency deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : Currency.getInstance(json.getAsJsonPrimitive().getAsString());
        }

        @Override
        public JsonElement serialize(Currency currency, Type type, JsonSerializationContext context) {
            return currency == null ? JsonNull.INSTANCE : new JsonPrimitive(currency.getCurrencyCode());
        }
    }


    /**
     * A GSON Deserializer for ZoneId objects
     */
    private static class ZoneIdSerializer implements JsonDeserializer<ZoneId>, JsonSerializer<ZoneId> {

        @Override
        public ZoneId deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : ZoneId.of(json.getAsJsonPrimitive().getAsString());
        }

        @Override
        public JsonElement serialize(ZoneId value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(value.getId());
        }
    }


    /**
     * A GSON Deserializer for TimeZone objects
     */
    private static class TimeZoneSerializer implements JsonDeserializer<TimeZone>, JsonSerializer<TimeZone> {

        @Override
        public TimeZone deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : TimeZone.getTimeZone(json.getAsJsonPrimitive().getAsString());
        }

        @Override
        public JsonElement serialize(TimeZone value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(value.getID());
        }
    }


    /**
     * A GSON Serializer for LocalTime objects
     */
    public static class LocalTimeSerializer implements JsonDeserializer<LocalTime>, JsonSerializer<LocalTime> {

        private DateTimeFormatter formatter;

        /**
         * Constructor
         * @param pattern   the format patterm
         */
        public LocalTimeSerializer(String pattern) {
            this(DateTimeFormatter.ofPattern(pattern));
        }

        /**
         * Constructor
         * @param formatter the date time formatter
         */
        public LocalTimeSerializer(DateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public JsonElement serialize(LocalTime value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(formatter.format(value));
        }

        @Override
        public LocalTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : LocalTime.parse(json.getAsJsonPrimitive().getAsString());
        }
    }


    /**
     * A GSON Serializer for LocalDate objects
     */
    public static class LocalDateSerializer implements JsonDeserializer<LocalDate>, JsonSerializer<LocalDate> {

        private DateTimeFormatter formatter;

        /**
         * Constructor
         * @param pattern   the format patterm
         */
        public LocalDateSerializer(String pattern) {
            this(DateTimeFormatter.ofPattern(pattern));
        }

        /**
         * Constructor
         * @param formatter the date time formatter
         */
        public LocalDateSerializer(DateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public JsonElement serialize(LocalDate value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(formatter.format(value));
        }

        @Override
        public LocalDate deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : LocalDate.parse(json.getAsJsonPrimitive().getAsString());
        }
    }


    /**
     * A GSON Serializer for LocalDateTime objects
     */
    public static class LocalDateTimeSerializer implements JsonDeserializer<LocalDateTime>, JsonSerializer<LocalDateTime> {

        private DateTimeFormatter formatter;

        /**
         * Constructor
         * @param pattern   the format patterm
         */
        public LocalDateTimeSerializer(String pattern) {
            this(DateTimeFormatter.ofPattern(pattern));
        }

        /**
         * Constructor
         * @param formatter the date time formatter
         */
        public LocalDateTimeSerializer(DateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public JsonElement serialize(LocalDateTime value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(formatter.format(value));
        }

        @Override
        public LocalDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : LocalDateTime.parse(json.getAsJsonPrimitive().getAsString());
        }
    }


    /**
     * A GSON Serializer for LocalDateTime objects
     */
    public static class ZonedDateTimeSerializer implements JsonDeserializer<ZonedDateTime>, JsonSerializer<ZonedDateTime> {

        private DateTimeFormatter formatter;

        /**
         * Constructor
         * @param pattern   the format patterm
         */
        public ZonedDateTimeSerializer(String pattern) {
            this(DateTimeFormatter.ofPattern(pattern));
        }

        /**
         * Constructor
         * @param formatter the date time formatter
         */
        public ZonedDateTimeSerializer(DateTimeFormatter formatter) {
            this.formatter = formatter;
        }

        @Override
        public JsonElement serialize(ZonedDateTime value, Type type, JsonSerializationContext context) {
            return value == null ? JsonNull.INSTANCE : new JsonPrimitive(formatter.format(value));
        }

        @Override
        public ZonedDateTime deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context) throws JsonParseException {
            return json.isJsonNull() ? null : ZonedDateTime.parse(json.getAsJsonPrimitive().getAsString());
        }
    }

}
