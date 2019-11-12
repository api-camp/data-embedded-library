package com.de314.data.local.api.model;

import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class KVInfo {

    private final String kind;
    private final String namespace;
    private final long size;
    private final String prettySize;
}
