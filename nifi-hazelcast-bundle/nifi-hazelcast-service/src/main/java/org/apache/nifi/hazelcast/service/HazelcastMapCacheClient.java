/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.hazelcast.service;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.hazelcast.map.IMap;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.distributed.cache.client.Deserializer;
import org.apache.nifi.distributed.cache.client.DistributedMapCacheClient;
import org.apache.nifi.distributed.cache.client.Serializer;
import org.apache.nifi.processor.util.StandardValidators;

// TODO: Doc
@Tags({"distributed", "cache", "map", "cluster", "hazelcast"})
@CapabilityDescription("Provides the ability to communicate with a Hazelcast Server cluster as a DistributedMapCacheServer." +
        " This can be used in order to share a Map between nodes in a NiFi cluster." +
        " Hazelcast Server cluster can provide a high available and persistent cache storage.")
public class HazelcastMapCacheClient extends AbstractControllerService implements DistributedMapCacheClient {

    private HazelcastService clientService;
    private IMap<byte[], byte[]> iMap;
    private Long ttl;

    public static final PropertyDescriptor HAZELCAST_CLUSTER_SERVICE = new PropertyDescriptor.Builder()
            .name("HAZELCAST_CLUSTER_SERVICE")
            .displayName("Hazelcast Cluster Client Service")
            .description("A Hazelcast Cluster Client Service which manages connections to a Hazelcast cluster.")
            .required(true)
            .identifiesControllerService(HazelcastService.class)
            .build();

    public static final PropertyDescriptor HAZELCAST_MAP = new PropertyDescriptor
            .Builder().name("HAZELCAST_MAP")
            .displayName("Hazelcast Map name")
            .description("Name of the IMap object to query")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();

    public static final PropertyDescriptor HAZELCAST_TIME_TO_LIVE = new PropertyDescriptor
            .Builder().name("HAZELCAST_TIME_TO_LIVE")
            .displayName("Time To Live")
            .description("Time To Live (TTL) of objects to put into the cache. Default, 0 sec, is equivalent to set unlimited TTL")
            .required(true)
            .defaultValue("0 sec")
            .addValidator(StandardValidators.TIME_PERIOD_VALIDATOR)
            .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(HAZELCAST_CLUSTER_SERVICE);
        descriptors.add(HAZELCAST_MAP);
        descriptors.add(HAZELCAST_TIME_TO_LIVE);
        return descriptors;
    }

    @OnEnabled
    public void configure(final ConfigurationContext context) {
        this.ttl = context.getProperty(HAZELCAST_TIME_TO_LIVE).asTimePeriod(TimeUnit.SECONDS);
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) {
        this.clientService = context.getProperty(HAZELCAST_CLUSTER_SERVICE).asControllerService(HazelcastService.class);
        final String iMapName = context.getProperty(HAZELCAST_MAP).evaluateAttributeExpressions().getValue();
        this.iMap = this.clientService.getIMap(iMapName);
    }

    static private <K> byte[] toDocKey(K key, Serializer<K> keySerializer) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        keySerializer.serialize(key, bos);
        return bos.toByteArray();
    }

    static private <V> byte[] toDocVal(V value, Serializer<V> valueSerializer) throws IOException {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        valueSerializer.serialize(value, bos);
        return bos.toByteArray();
    }

    @Override
    public <K> boolean containsKey(K key, Serializer<K> keySerializer) throws IOException {
        return this.iMap.containsKey(toDocKey(key, keySerializer));
    }

    @Override
    public <K, V> boolean putIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] k = toDocKey(key, keySerializer);
        final byte[] v = toDocVal(value, valueSerializer);

        if (containsKey(key, keySerializer)) {
            return false;
        }

        this.iMap.putIfAbsent(k, v, this.ttl, TimeUnit.SECONDS);
        return true;
    }

    private <V> V deserialize(byte[] val, Deserializer<V> valueDeserializer) throws IOException {
        if (val == null) {
            return null;
        }
        return valueDeserializer.deserialize(val);
    }

    @Override
    public <K, V> V get(K key, Serializer<K> keySerializer, Deserializer<V> valueDeserializer) throws IOException {
        final byte[] k = toDocKey(key, keySerializer);
        final byte[] value = this.iMap.get(k);
        return deserialize(value, valueDeserializer);
    }

    @Override
    public <K, V> void put(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer) throws IOException {
        final byte[] k = toDocKey(key, keySerializer);
        final byte[] v = toDocVal(value, valueSerializer);
        this.iMap.put(k, v, this.ttl, TimeUnit.SECONDS);
    }

    @Override
    public <K, V> V getAndPutIfAbsent(K key, V value, Serializer<K> keySerializer, Serializer<V> valueSerializer, Deserializer<V> valueDeserializer) throws IOException {
        final V existing = get(key, keySerializer, valueDeserializer);
        if (existing != null) {
            return existing;
        }

        // If there's no existing value, put this value.
        if (!putIfAbsent(key, value, keySerializer, valueSerializer)) {
            // If putting this value failed, it's possible that other client has put different doc, so return that.
            return get(key, keySerializer, valueDeserializer);
        }

        // If successfully put this value, return this.
        return value;
    }

    @Override
    public <K> boolean remove(K key, Serializer<K> keySerializer) throws IOException {

        if (!containsKey(key, keySerializer)) {
            return false;
        }

        this.iMap.delete(toDocKey(key, keySerializer));
        return true;
    }

    @Override
    public long removeByPattern(String regex) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() throws IOException {
    }
}