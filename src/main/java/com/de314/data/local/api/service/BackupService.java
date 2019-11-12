package com.de314.data.local.api.service;

import com.de314.data.local.utils.FileUtils;
import com.de314.data.local.utils.metrics.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Optional;

@Slf4j
@RequiredArgsConstructor
public class BackupService {

//    private final DataStoreFactory storeFactory;
//    private final String doSpaceAccessKey;
//    private final String doSpaceSecret;
//
//    public Optional<Runnable> backup(String namespace) {
//        return storeFactory.getRocksStore(namespace)
//                .map(store -> () -> {
//                    log.info("Backing up: {}", namespace);
//                    Timer timer = Timer.create();
//                    List<String> urls = store.backup(FileUtils.uploadArchiveStrategy(doSpaceAccessKey, doSpaceSecret, namespace));
//                    timer.stop();
//                    log.info("Backup Complete: {} {} - {}", timer, namespace, urls);
//                });
//    }
//
//    public Optional<Runnable> rollback(String namespace) {
//        return storeFactory.getRocksStore(namespace)
//                .map(store -> () -> {
//                    log.info("Rolling back: {}", namespace);
//                    Timer timer = Timer.create();
//                    boolean reload = FileUtils.reload(doSpaceAccessKey, doSpaceSecret, namespace);
//                    timer.stop();
//                    log.info("Rollback Complete: {} {} - success={}", timer, namespace, reload);
//                });
//    }
}
