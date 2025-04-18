/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.packages.management.storage.filesystem;

import com.google.common.io.ByteStreams;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.packages.management.core.PackagesStorage;
import org.apache.pulsar.packages.management.core.PackagesStorageConfiguration;


/**
 * Packages management storage implementation with filesystem.
 */
@Slf4j
public class FileSystemPackagesStorage implements PackagesStorage {

    private static final String STORAGE_PATH = "STORAGE_PATH";
    private static final String DEFAULT_STORAGE_PATH = "packages-storage";

    private final File storagePath;

    FileSystemPackagesStorage(PackagesStorageConfiguration configuration) {
        String storagePath = configuration.getProperty(STORAGE_PATH);
        if (storagePath != null) {
            this.storagePath = new File(storagePath);
        } else {
            this.storagePath = new File(DEFAULT_STORAGE_PATH);
        }
    }

    private File getPath(String path) throws IOException {
        // Normalize the path to remove any redundant path elements
        File f = Paths.get(storagePath.toString(), path).normalize().toFile();

        // Ensure the normalized path is still within the storagePath
        if (!f.getAbsolutePath().startsWith(storagePath.getAbsolutePath())) {
            throw new IOException("Invalid path: " + path);
        }

        if (!f.getParentFile().exists()) {
            if (!f.getParentFile().mkdirs()) {
                throw new RuntimeException("Failed to create parent dirs for " + path);
            }
        }
        return f;
    }

    @Override
    public void initialize() {
        if (!storagePath.exists()) {
            if (!storagePath.mkdirs()) {
                throw new RuntimeException("Failed to create base storage directory at " + storagePath);
            }
        }

        log.info("Packages management filesystem storage initialized on {}", storagePath);
    }

    @Override
    public CompletableFuture<Void> writeAsync(String path, InputStream inputStream) {
        try {
            File f = getPath(path);

            @Cleanup
            OutputStream os = new FileOutputStream(f);

            @Cleanup
            BufferedOutputStream bos = new BufferedOutputStream(os);
            ByteStreams.copy(inputStream, bos);

            return CompletableFuture.completedFuture(null);
        } catch (IOException e) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<Void> readAsync(String path, OutputStream outputStream) {
        try {
            @Cleanup
            InputStream is = new FileInputStream(getPath(path));

            @Cleanup
            BufferedInputStream bis = new BufferedInputStream(is);
            ByteStreams.copy(bis, outputStream);

            return CompletableFuture.completedFuture(null);
        } catch (IOException e) {
            CompletableFuture<Void> f = new CompletableFuture<>();
            f.completeExceptionally(e);
            return f;
        }
    }

    @Override
    public CompletableFuture<Void> deleteAsync(String path) {
        try {
            if (getPath(path).delete()) {
                return CompletableFuture.completedFuture(null);
            } else {
                CompletableFuture<Void> f = new CompletableFuture<>();
                f.completeExceptionally(new IOException("Failed to delete file at " + path));
                return f;
            }
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<List<String>> listAsync(String path) {
        try {
            String[] files = getPath(path).list();
            if (files == null) {
                return CompletableFuture.completedFuture(Collections.emptyList());
            } else {
                return CompletableFuture.completedFuture(Arrays.asList(files));
            }
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Boolean> existAsync(String path) {
        try {
            return CompletableFuture.completedFuture(getPath(path).exists());
        } catch (IOException e) {
            return CompletableFuture.failedFuture(e);
        }
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public String dataPath() {
        return "/data";
    }
}
