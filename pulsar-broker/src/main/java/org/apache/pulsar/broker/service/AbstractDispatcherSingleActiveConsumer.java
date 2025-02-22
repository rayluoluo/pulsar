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
package org.apache.pulsar.broker.service;

import static com.google.common.base.Preconditions.checkArgument;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import org.apache.bookkeeper.mledger.ManagedCursor;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.broker.loadbalance.extensions.data.BrokerLookupData;
import org.apache.pulsar.broker.service.BrokerServiceException.ConsumerBusyException;
import org.apache.pulsar.broker.service.BrokerServiceException.ServerMetadataException;
import org.apache.pulsar.broker.service.persistent.DispatchRateLimiter;
import org.apache.pulsar.client.impl.Murmur3Hash32;
import org.apache.pulsar.common.api.proto.CommandSubscribe.SubType;
import org.apache.pulsar.common.util.FutureUtil;
import org.apache.pulsar.common.util.Murmur3_32Hash;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractDispatcherSingleActiveConsumer extends AbstractBaseDispatcher {

    protected final String topicName;
    private volatile Consumer activeConsumer = null;
    protected final CopyOnWriteArrayList<Consumer> consumers;
    protected StickyKeyConsumerSelector stickyKeyConsumerSelector;
    protected boolean isKeyHashRangeFiltered = false;
    protected CompletableFuture<Void> closeFuture = null;
    protected final int partitionIndex;
    protected final ManagedCursor cursor;
    // This dispatcher supports both the Exclusive and Failover subscription types
    protected final SubType subscriptionType;

    protected static final int FALSE = 0;
    protected static final int TRUE = 1;
    protected static final AtomicIntegerFieldUpdater<AbstractDispatcherSingleActiveConsumer> IS_CLOSED_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(AbstractDispatcherSingleActiveConsumer.class, "isClosed");
    private volatile int isClosed = FALSE;

    protected boolean isFirstRead = true;
    private static final int CONSUMER_CONSISTENT_HASH_REPLICAS = 100;

    public AbstractDispatcherSingleActiveConsumer(SubType subscriptionType, int partitionIndex,
                                                  String topicName, Subscription subscription,
                                                  ServiceConfiguration serviceConfig, ManagedCursor cursor) {
        super(subscription, serviceConfig);
        this.topicName = topicName;
        this.consumers = new CopyOnWriteArrayList<>();
        this.partitionIndex = partitionIndex;
        this.subscriptionType = subscriptionType;
        this.cursor = cursor;
    }

    /**
     * @apiNote this method does not need to be thread safe
     */
    protected abstract void scheduleReadOnActiveConsumer();

    /**
     * @apiNote this method does not need to be thread safe
     */
    protected abstract void cancelPendingRead();

    protected void notifyActiveConsumerChanged(Consumer activeConsumer) {
        if (null != activeConsumer && subscriptionType == SubType.Failover) {
            consumers.forEach(consumer ->
                consumer.notifyActiveConsumerChange(activeConsumer));
        }
    }

    /**
     * Pick active consumer for a topic for {@link SubType#Failover} subscription.
     * If it's a non-partitioned topic then it'll pick consumer based on order they subscribe to the topic.
     * If is's a partitioned topic, first sort consumers based on their priority level and consumer name then
     * distributed partitions evenly across consumers with highest priority level.
     *
     * @return the true consumer if the consumer is changed, otherwise false.
     * @apiNote this method is not thread safe
     */
    protected boolean pickAndScheduleActiveConsumer() {
        checkArgument(!consumers.isEmpty());
        AtomicBoolean hasPriorityConsumer = new AtomicBoolean(false);
        consumers.sort((c1, c2) -> {
            int priority = c1.getPriorityLevel() - c2.getPriorityLevel();
            if (priority != 0) {
                hasPriorityConsumer.set(true);
                return priority;
            }
            return c1.consumerName().compareTo(c2.consumerName());
        });

        int consumersSize = consumers.size();
        // find number of consumers which are having the highest priorities. so partitioned-topic assignment happens
        // evenly across highest priority consumers
        if (hasPriorityConsumer.get()) {
            int highestPriorityLevel = consumers.get(0).getPriorityLevel();
            for (int i = 0; i < consumers.size(); i++) {
                if (highestPriorityLevel != consumers.get(i).getPriorityLevel()) {
                    consumersSize = i;
                    break;
                }
            }
        }
        int index = partitionIndex >= 0 && !serviceConfig.isActiveConsumerFailoverConsistentHashing()
                ? partitionIndex % consumersSize
                : peekConsumerIndexFromHashRing(makeHashRing(consumersSize));

        Consumer selectedConsumer = consumers.get(index);

        if (selectedConsumer == activeConsumer) {
            // Active consumer did not change. Do nothing at this point
            return false;
        } else {
            // If the active consumer is changed, send notification.
            activeConsumer = selectedConsumer;
            scheduleReadOnActiveConsumer();
            return true;
        }
    }

    private int peekConsumerIndexFromHashRing(NavigableMap<Integer, Integer> hashRing) {
        int hash = Murmur3Hash32.getInstance().makeHash(topicName);
        Map.Entry<Integer, Integer> ceilingEntry = hashRing.ceilingEntry(hash);
        return ceilingEntry != null ? ceilingEntry.getValue() : hashRing.firstEntry().getValue();
    }

    private NavigableMap<Integer, Integer> makeHashRing(int consumerSize) {
        NavigableMap<Integer, Integer> hashRing = new TreeMap<>();
        for (int i = 0; i < consumerSize; i++) {
            for (int j = 0; j < CONSUMER_CONSISTENT_HASH_REPLICAS; j++) {
                String key = consumers.get(i).consumerName() + j;
                int hash = Murmur3_32Hash.getInstance().makeHash(key.getBytes());
                hashRing.put(hash, i);
            }
        }
        return Collections.unmodifiableNavigableMap(hashRing);
    }

    public synchronized CompletableFuture<Void> addConsumer(Consumer consumer) {
        if (IS_CLOSED_UPDATER.get(this) == TRUE) {
            log.warn("[{}] Dispatcher is already closed. Closing consumer {}", this.topicName, consumer);
            consumer.disconnect();
            return CompletableFuture.completedFuture(null);
        }

        if (subscriptionType == SubType.Exclusive && !consumers.isEmpty()) {
            Consumer actConsumer = getActiveConsumer();
            if (actConsumer != null) {
                return actConsumer.cnx().checkConnectionLiveness().thenCompose(actConsumerStillAlive -> {
                    if (actConsumerStillAlive.isEmpty() || actConsumerStillAlive.get()) {
                        return FutureUtil.failedFuture(new ConsumerBusyException("Exclusive consumer is already"
                                + " connected"));
                    } else {
                        return addConsumer(consumer);
                    }
                });
            } else {
                // It should never happen.

                return FutureUtil.failedFuture(new ConsumerBusyException("Active consumer is in a strange state."
                        + " Active consumer is null, but there are " + consumers.size() + " registered."));
            }
        }

        if (subscriptionType == SubType.Failover && isConsumersExceededOnSubscription()) {
            log.warn("[{}] Attempting to add consumer to subscription which reached max consumers limit",
                    this.topicName);
            return FutureUtil.failedFuture(new ConsumerBusyException("Subscription reached max consumers limit"));
        }

        if (subscriptionType == SubType.Exclusive
                && consumer.getKeySharedMeta() != null
                && consumer.getKeySharedMeta().getHashRangesList() != null
                && consumer.getKeySharedMeta().getHashRangesList().size() > 0) {
            stickyKeyConsumerSelector = new HashRangeExclusiveStickyKeyConsumerSelector();
            stickyKeyConsumerSelector.addConsumer(consumer);
            isKeyHashRangeFiltered = true;
        } else {
            isKeyHashRangeFiltered = false;
        }

        if (consumers.isEmpty()) {
            isFirstRead = true;
        }

        consumers.add(consumer);

        if (!pickAndScheduleActiveConsumer()) {
            // the active consumer is not changed
            Consumer currentActiveConsumer = getActiveConsumer();
            if (null == currentActiveConsumer) {
                if (log.isDebugEnabled()) {
                    log.debug("Current active consumer disappears while adding consumer {}", consumer);
                }
            } else {
                consumer.notifyActiveConsumerChange(currentActiveConsumer);
            }
        }

        return CompletableFuture.completedFuture(null);
    }

    public synchronized void removeConsumer(Consumer consumer) throws BrokerServiceException {
        log.info("Removing consumer {}", consumer);
        if (!consumers.remove(consumer)) {
            throw new ServerMetadataException("Consumer was not connected");
        }

        if (consumers.isEmpty()) {
            activeConsumer = null;
        }

        if (closeFuture == null && !consumers.isEmpty()) {
            pickAndScheduleActiveConsumer();
            return;
        }

        cancelPendingRead();

        if (consumers.isEmpty() && closeFuture != null && !closeFuture.isDone()) {
            // Control reaches here only when closeFuture is created
            // and no more connected consumers left.
            closeFuture.complete(null);
        }
    }

    /**
     * Handle unsubscribe command from the client API For failover subscription, if consumer is connected consumer, we
     * can unsubscribe.
     *
     * @param consumer
     *            Calling consumer object
     */
    public synchronized boolean canUnsubscribe(Consumer consumer) {
        return (consumers.size() == 1) && Objects.equals(consumer, activeConsumer);
    }

    @Override
    public CompletableFuture<Void> close(boolean disconnectConsumers,
                                         Optional<BrokerLookupData> assignedBrokerLookupData) {
        IS_CLOSED_UPDATER.set(this, TRUE);
        getRateLimiter().ifPresent(DispatchRateLimiter::close);
        return disconnectConsumers
                ? disconnectAllConsumers(false, assignedBrokerLookupData) : CompletableFuture.completedFuture(null);
    }

    public boolean isClosed() {
        return isClosed == TRUE;
    }

    /**
     * Disconnect all consumers on this dispatcher (server side close). This triggers channelInactive on the inbound
     * handler which calls dispatcher.removeConsumer(), where the closeFuture is completed.
     *
     * @param isResetCursor
     *              Specifies if the cursor has been reset.
     * @param assignedBrokerLookupData
     *              Optional target broker redirect information. Allows the consumer to quickly reconnect to a broker
     *              during bundle unloading.
     *
     * @return CompletableFuture indicating the completion of the operation.
     */
    @Override
    public synchronized CompletableFuture<Void> disconnectAllConsumers(
            boolean isResetCursor, Optional<BrokerLookupData> assignedBrokerLookupData) {
        closeFuture = new CompletableFuture<>();

        if (!consumers.isEmpty()) {
            consumers.forEach(consumer -> consumer.disconnect(isResetCursor, assignedBrokerLookupData));
            cancelPendingRead();
        } else {
            // no consumer connected, complete disconnect immediately
            closeFuture.complete(null);
        }
        return closeFuture;
    }

    public synchronized CompletableFuture<Void> disconnectActiveConsumers(boolean isResetCursor) {
        closeFuture = new CompletableFuture<>();
        if (activeConsumer != null) {
            activeConsumer.disconnect(isResetCursor);
        }
        closeFuture.complete(null);
        return closeFuture;
    }

    @Override
    public synchronized void resetCloseFuture() {
        closeFuture = null;
    }

    public void reset() {
        resetCloseFuture();
        IS_CLOSED_UPDATER.set(this, FALSE);
    }

    public SubType getType() {
        return subscriptionType;
    }

    public Consumer getActiveConsumer() {
        return activeConsumer;
    }

    @Override
    public List<Consumer> getConsumers() {
        return consumers;
    }

    public boolean isConsumerConnected() {
        return activeConsumer != null;
    }

    private static final Logger log = LoggerFactory.getLogger(AbstractDispatcherSingleActiveConsumer.class);

}
