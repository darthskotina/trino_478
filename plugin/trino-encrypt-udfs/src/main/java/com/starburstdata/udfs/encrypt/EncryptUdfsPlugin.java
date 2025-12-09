package com.starburstdata.udfs.encrypt;

import io.trino.spi.Plugin;

import java.util.Set;

public class EncryptUdfsPlugin implements Plugin {
    @Override
    public Set<Class<?>> getFunctions() {
        return Set.of(EncryptDecrypt.class);
    }
}
