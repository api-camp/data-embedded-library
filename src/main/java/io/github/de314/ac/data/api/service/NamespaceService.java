package io.github.de314.ac.data.api.service;

import io.github.de314.ac.data.api.model.NamespaceOptions;

import java.util.List;

public interface NamespaceService {

    boolean exists(String namespace);

    List<NamespaceOptions> findAll();

    NamespaceOptions findOne(String namespace);

    void save(NamespaceOptions save);

    void delete(String namespace);
}
