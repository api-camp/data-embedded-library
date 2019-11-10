package com.de314.data.local.api.service;

import com.de314.data.local.utils.JsonUtils;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

import java.nio.charset.Charset;
import java.util.function.Function;

@Data
public class DataAdapter<A, B> {

    private static final JsonUtils jsonUtils = new JsonUtils();

    private final Function<A, B> abFunc;
    private final Function<B, A> baFunc;

    public B ab(A value) {
        return abFunc.apply(value);
    }

    public A ba(B value) {
        return baFunc.apply(value);
    }

    public static <FromT, ToT> DataAdapter<FromT, ToT> of(
            Function<FromT, ToT> abFunc,
            Function<ToT, FromT> baFunc
    ) {
        return new DataAdapter<>(abFunc, baFunc);
    }

    public static DataAdapter<String, byte[]> stringConverter() {
        return new DataAdapter<>(
                s -> s.getBytes(Charset.defaultCharset()),
                b -> new String(b, Charset.defaultCharset())
        );
    }

    public static <T> DataAdapter<T, byte[]> pojoByteConverter(Class<T> targetClass) {
        return new DataAdapter<>(
                jsonUtils::asBytes,
                b -> jsonUtils.fromJson(b, targetClass)
        );
    }

    public static DataAdapter<JsonNode, byte[]> jsonByteAdapter() {
        return new DataAdapter<>(
                jsonUtils::asBytes,
                jsonUtils::fromJson
        );
    }

    public static <T> DataAdapter<T, String> pojoStringConverter(Class<T> targetClass) {
        return new DataAdapter<>(
                jsonUtils::asString,
                s -> jsonUtils.fromJson(s, targetClass)
        );
    }

    public static DataAdapter<JsonNode, String> jsonStringAdapter() {
        return new DataAdapter<>(
                jsonUtils::asString,
                jsonUtils::fromJson
        );
    }
}
