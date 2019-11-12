package com.de314.data.local.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Singular;

import java.util.List;

@Data
@Builder(toBuilder = true)
@NoArgsConstructor
@AllArgsConstructor
public class Article {

    public static final String NAMESPACE = Article.class.getSimpleName();

    private long id;
    private String title;
    private String body;
    @Singular
    private List<String> tags;

    @JsonIgnore
    public String getKey() {
        return getKey(id);
    }

    public static String getKey(long id) {
        return String.format("%05d", id);
    }
}
