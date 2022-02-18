/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.utils;

import org.apache.kafka.common.record.RecordBatch;

public class MessageID
{
    public static final MessageID NONE = new MessageID(RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH ,
            RecordBatch.NO_SEQUENCE,
            RecordBatch.NO_SEQUENCE);

    public final long producerId;
    public final long producerEpoch;
    public final long sequenceBegin;
    public final long sequenceEnd;

    public MessageID(long producerId, long epoch, long sequenceBegin, long sequenceEnd)
    {
        this.producerId = producerId;
        this.producerEpoch = epoch;
        this.sequenceBegin = sequenceBegin;
        this.sequenceEnd = sequenceEnd;
    }

    public boolean isValid() {
        return RecordBatch.NO_PRODUCER_ID < producerId;
    }

    @Override
    public String toString() {
        return "(producerId=" + producerId + ", epoch=" + producerEpoch + ", sequenceBegin=" + sequenceBegin + ", sequenceEnd=" + sequenceEnd + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        MessageID that = (MessageID) o;

        if (producerId != that.producerId) return false;
        if (producerEpoch != that.producerEpoch) return false;
        if (sequenceBegin != that.sequenceBegin) return false;
        return sequenceEnd == that.sequenceEnd;
    }


    @Override
    public int hashCode() {
        int hashCode = 0;
        hashCode = 31 * hashCode + ((int) (producerEpoch >> 32) ^ (int) producerEpoch);
        hashCode = 31 * hashCode + ((int) (producerId >> 32) ^ (int) producerId);
        hashCode = 31 * hashCode + ((int) (sequenceBegin >> 32) ^ (int) sequenceBegin);
        hashCode = 31 * hashCode + ((int) (sequenceEnd >> 32) ^ (int) sequenceEnd);
        return hashCode;
    }

}
