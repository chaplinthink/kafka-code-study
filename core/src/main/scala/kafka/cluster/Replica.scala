/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package kafka.cluster

import kafka.log.Log
import kafka.utils.{SystemTime, Time, Logging}
import kafka.server.{LogReadResult, LogOffsetMetadata}
import kafka.common.KafkaException

import java.util.concurrent.atomic.AtomicLong

class Replica(val brokerId: Int,  //标识该副本所在的Broker的id
              val partition: Partition,
              time: Time = SystemTime,
              initialHighWatermarkValue: Long = 0L,
              val log: Option[Log] = None) extends Logging {
  //LogOffsetMetadata对象，此字段用来记录HW（HighWatermark）的值。消费者只能获取到HW之前的消息，其后的消息对消费者是不可见的
  //此字段由Leader副本负责更新维护，更新时机是消息被ISR集合中所有副本同步成功，即消息被成功提交
  // the high watermark offset value, in non-leader replicas only its message offsets are kept
  @volatile private[this] var highWatermarkMetadata: LogOffsetMetadata = new LogOffsetMetadata(initialHighWatermarkValue)
  // the log end offset value, kept in all replicas;
  // for local replica it is the log's end offset, for remote replicas its value is only updated by follower fetch
  @volatile private[this] var logEndOffsetMetadata: LogOffsetMetadata = LogOffsetMetadata.UnknownOffsetMetadata

  val topic = partition.topic
  val partitionId = partition.partitionId
  //是否是本地副本，如果是就返回true,是远程副本的话log 就为空，返回false
  def isLocal: Boolean = {
    log match {
      case Some(l) => true
      case None => false
    }
  }
  //private[this] scala 只能在对象内部访问的字段， 不加this方法可以访问该类的所有对象的私有字段，称为类私有字段
  private[this] val lastCaughtUpTimeMsUnderlying = new AtomicLong(time.milliseconds) //AtomicLong 是用来处理线程安全的

  def lastCaughtUpTimeMs = lastCaughtUpTimeMsUnderlying.get()

  def updateLogReadResult(logReadResult : LogReadResult) {
    logEndOffset = logReadResult.info.fetchOffsetMetadata //更新LEO

    /* If the request read up to the log end offset snapshot when the read was initiated,
     * set the lastCaughtUpTimeMsUnderlying to the current time.
     * This means that the replica is fully caught up.
     */
    if(logReadResult.isReadFromLogEnd) {
      lastCaughtUpTimeMsUnderlying.set(time.milliseconds)
    }
  }

  private def logEndOffset_=(newLogEndOffset: LogOffsetMetadata) {
    if (isLocal) {//本地副本不能直接更新，本地副本是由 log.get.logEndOffsetMetadata决定
      throw new KafkaException("Should not set log end offset on partition [%s,%d]'s local replica %d".format(topic, partitionId, brokerId))
    } else { //远程副本，LEO 是通过请求进行更新。
      logEndOffsetMetadata = newLogEndOffset
      trace("Setting log end offset for replica %d for partition [%s,%d] to [%s]"
        .format(brokerId, topic, partitionId, logEndOffsetMetadata))
    }
  }

  def logEndOffset =
    if (isLocal)
      log.get.logEndOffsetMetadata
    else
      logEndOffsetMetadata

  def highWatermark_=(newHighWatermark: LogOffsetMetadata) {
    if (isLocal) {
      highWatermarkMetadata = newHighWatermark
      trace("Setting high watermark for replica %d partition [%s,%d] on broker %d to [%s]"
        .format(brokerId, topic, partitionId, brokerId, newHighWatermark))
    } else {
      throw new KafkaException("Should not set high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  def highWatermark = highWatermarkMetadata

  def convertHWToLocalOffsetMetadata() = {
    if (isLocal) {
      highWatermarkMetadata = log.get.convertToOffsetMetadata(highWatermarkMetadata.messageOffset)
    } else {
      throw new KafkaException("Should not construct complete high watermark on partition [%s,%d]'s non-local replica %d".format(topic, partitionId, brokerId))
    }
  }

  override def equals(that: Any): Boolean = {
    if(!(that.isInstanceOf[Replica]))
      return false
    val other = that.asInstanceOf[Replica]
    if(topic.equals(other.topic) && brokerId == other.brokerId && partition.equals(other.partition))
      return true
    false
  }

  override def hashCode(): Int = {
    31 + topic.hashCode() + 17*brokerId + partition.hashCode()
  }


  override def toString(): String = {
    val replicaString = new StringBuilder
    replicaString.append("ReplicaId: " + brokerId)
    replicaString.append("; Topic: " + topic)
    replicaString.append("; Partition: " + partition.partitionId)
    replicaString.append("; isLocal: " + isLocal)
    if(isLocal) replicaString.append("; Highwatermark: " + highWatermark)
    replicaString.toString()
  }
}
