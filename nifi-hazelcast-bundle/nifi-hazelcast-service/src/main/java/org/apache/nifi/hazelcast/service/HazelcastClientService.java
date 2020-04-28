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

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.expression.ExpressionLanguageScope;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

@Tags({"distributed", "cache", "map", "cluster", "hazelcast"})
@CapabilityDescription("Provides a Client Service to connect a Hazelcast Server/Cluster")
public class HazelcastClientService extends AbstractControllerService implements HazelcastService {
    public static final PropertyDescriptor HAZELCAST_HOST_PORT = new PropertyDescriptor
            .Builder().name("HAZELCAST_HOST_PORT")
            .displayName("Hazelcast Hosts")
            .description("Comma-separated list of Hazelcast instance(s) (host:port)")
            .required(true)
            .defaultValue("localhost:5701")
            .addValidator(StandardValidators.HOSTNAME_PORT_LIST_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();

    private static final List<PropertyDescriptor> properties;
    private ReentrantLock lock;
    private AtomicReference<HazelcastInstance> hazelcastClient = new AtomicReference<HazelcastInstance>();
    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(HAZELCAST_HOST_PORT);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ControllerServiceInitializationContext config) throws InitializationException {
        lock = new ReentrantLock();
    }

    /**
     * @param context
     *            the configuration context
     * @throws InitializationException
     *             if unable to create a database connection
     */
    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {
        final String hazelcastAddresses = context.getProperty(HAZELCAST_HOST_PORT).getValue();
        getLogger().info("Hazelcast Client Staring... ");
        lock.lock();
        ClientConfig config = new ClientConfig();
        for(String address : hazelcastAddresses.split(","))
		{
			config.getNetworkConfig().addAddress(address);
		}
        config.setProperty("hazelcast.client.statistics.enabled","true");
        hazelcastClient.set(HazelcastClient.newHazelcastClient(config));
        lock.unlock();
    }

    @OnDisabled
    public void shutdown() {
        getLogger().info("Hazelcast Shutting down.. "+ hazelcastClient.get());
        if(hazelcastClient.get() != null) hazelcastClient.get().shutdown();
    }

    @Override
    public HazelcastInstance getInstance() {
        return hazelcastClient.get();
    }

    @Override
    public IMap<byte[], byte[]> getIMap(final String mapName) {
        return hazelcastClient.get().getMap(mapName);
    }
}
