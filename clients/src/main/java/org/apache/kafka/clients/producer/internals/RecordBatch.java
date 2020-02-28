/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.kafka.clients.producer.internals;

import java.util.ArrayList;
import java.util.List;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A batch of records that is or will be sent.
 * 
 * This class is not thread safe and external synchronization must be used when modifying it
 */
public final class RecordBatch { //按照RecordBatch进行消息的发送和确认

    private static final Logger log = LoggerFactory.getLogger(RecordBatch.class);

    public int recordCount = 0; //记录了保存的record的个数
    public int maxRecordSize = 0;//最大record的字节数
    public volatile int attempts = 0; //尝试发送当前RecordBatch的次数
    public final long createdMs;
    public long drainedMs;
    public long lastAttemptMs; //最后一次尝试发送的时间戳
    public final MemoryRecords records; //消息最终存储的地方
    public final TopicPartition topicPartition;//当前RecordBatch缓存的消息都会发送给此TopicPartition
    public final ProduceRequestResult produceFuture; //标识RecordBatch状态的Future对象
    public long lastAppendTime; //最后一次向RecordBatch追加消息的时间戳
    private final List<Thunk> thunks; //消息的回调对象队列
    private long offsetCounter = 0L; //用来记录某消息在RecordBatch中的偏移量
    private boolean retry; //是否正在重试. 如果RecordBatch的数据发送失败，就会尝试重新发送

    public RecordBatch(TopicPartition tp, MemoryRecords records, long now) {
        this.createdMs = now;
        this.lastAttemptMs = now;
        this.records = records;
        this.topicPartition = tp;
        this.produceFuture = new ProduceRequestResult();
        this.thunks = new ArrayList<Thunk>();
        this.lastAppendTime = createdMs;
        this.retry = false;
    }

    /**
     * Append the record to the current record set and return the relative offset within that record set
     * 
     * @return The RecordSend corresponding to this record or null if there isn't sufficient room.
     */ //尝试将消息添加到当前的RecordBatch缓存
    public FutureRecordMetadata tryAppend(long timestamp, byte[] key, byte[] value, Callback callback, long now) {
        if (!this.records.hasRoomFor(key, value)) { //估算剩余空间，不是准确值
            return null;
        } else {
            long checksum = this.records.append(offsetCounter++, timestamp, key, value); //向MemoryRecords中添加数据
            this.maxRecordSize = Math.max(this.maxRecordSize, Record.recordSize(key, value));
            this.lastAppendTime = now;
            FutureRecordMetadata future = new FutureRecordMetadata(this.produceFuture, this.recordCount,
                                                                   timestamp, checksum,
                                                                   key == null ? -1 : key.length,
                                                                   value == null ? -1 : value.length);
            if (callback != null)
                thunks.add(new Thunk(callback, future)); //callback参数就是对应消息的Callback对象。  将用户自定义的Callback和FutureRecordMetadata封装成Thunk
            this.recordCount++;
            return future;
        }
    }

    /**
     * Complete the request
     * 
     * @param baseOffset The base offset of the messages assigned by the server
     * @param timestamp The timestamp returned by the broker.
     * @param exception The exception that occurred (or null if the request was successful)
     */ //当RecordBatch成功收到响应、超时、关闭生产者都会调用该方法
    public void done(long baseOffset, long timestamp, RuntimeException exception) {
        log.trace("Produced messages to topic-partition {} with base offset offset {} and error: {}.",
                  topicPartition,
                  baseOffset,
                  exception);
        // execute callbacks   回调RecordBatch中所有消息的CallBack回调
        for (int i = 0; i < this.thunks.size(); i++) {
            try {
                Thunk thunk = this.thunks.get(i);
                if (exception == null) { //正常处理完成
                    // If the timestamp returned by server is NoTimestamp, that means CreateTime is used. Otherwise LogAppendTime is used.
                    RecordMetadata metadata = new RecordMetadata(this.topicPartition,  baseOffset, thunk.future.relativeOffset(),
                                                                 timestamp == Record.NO_TIMESTAMP ? thunk.future.timestamp() : timestamp,
                                                                 thunk.future.checksum(),
                                                                 thunk.future.serializedKeySize(),
                                                                 thunk.future.serializedValueSize()); //将服务端返回的消息： offset、timestamp和消息的其他信息封装成RecordMetadata
                    thunk.callback.onCompletion(metadata, null); //调用消息自定义的CallBack
                } else {
                    thunk.callback.onCompletion(null, exception); //处理过程中出现异常的情况
                }
            } catch (Exception e) {
                log.error("Error executing user-provided callback on message for topic-partition {}:", topicPartition, e);
            }
        }
        this.produceFuture.done(topicPartition, baseOffset, exception); //标识整个RecordBatch已经处理完成
    }

    /**
     * A callback and the associated FutureRecordMetadata argument to pass to it.
     */
    final private static class Thunk {
        final Callback callback;
        final FutureRecordMetadata future;

        public Thunk(Callback callback, FutureRecordMetadata future) {
            this.callback = callback;
            this.future = future;
        }
    }

    @Override
    public String toString() {
        return "RecordBatch(topicPartition=" + topicPartition + ", recordCount=" + recordCount + ")";
    }

    /**
     * A batch whose metadata is not available should be expired if one of the following is true:
     * <ol>
     *     <li> the batch is not in retry AND request timeout has elapsed after it is ready (full or linger.ms has reached).
     *     <li> the batch is in retry AND request timeout has elapsed after the backoff period ended.
     * </ol>
     */
    public boolean maybeExpire(int requestTimeoutMs, long retryBackoffMs, long now, long lingerMs, boolean isFull) {
        boolean expire = false;

        if (!this.inRetry() && isFull && requestTimeoutMs < (now - this.lastAppendTime))
            expire = true;
        else if (!this.inRetry() && requestTimeoutMs < (now - (this.createdMs + lingerMs)))
            expire = true;
        else if (this.inRetry() && requestTimeoutMs < (now - (this.lastAttemptMs + retryBackoffMs)))
            expire = true;

        if (expire) {
            this.records.close();
            this.done(-1L, Record.NO_TIMESTAMP, new TimeoutException("Batch containing " + recordCount + " record(s) expired due to timeout while requesting metadata from brokers for " + topicPartition));
        } //标识整个RecordBatch中消息过期

        return expire;
    }

    /**
     * Returns if the batch is been retried for sending to kafka
     */
    public boolean inRetry() {
        return this.retry;
    }

    /**
     * Set retry to true if the batch is being retried (for send)
     */
    public void setRetry() {
        this.retry = true;
    }
}
