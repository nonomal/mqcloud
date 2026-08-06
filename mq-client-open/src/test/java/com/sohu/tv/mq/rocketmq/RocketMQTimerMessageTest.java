package com.sohu.tv.mq.rocketmq;

import com.sohu.index.tv.mq.common.ConsumerCallback;
import com.sohu.index.tv.mq.common.MQMessage;
import com.sohu.index.tv.mq.common.Result;
import com.sohu.tv.mq.rocketmq.RocketMQProducerJsonTest.Video;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class RocketMQTimerMessageTest {

    String topic = "mqcloud-timer-test-topic";

    @Test
    public void testProduce() throws InterruptedException {
        String producer = "mqcloud-timer-test-topic-producer";
        RocketMQProducer rocketMQProducer = TestUtil.buildProducer(producer, topic);
        rocketMQProducer.start();

        int msgSize = 300000;
        int count = 0;
        for (int i = 1; i <= msgSize; ++i) {
            Video video = new Video(i, "sohu-tv");
            int delayMinute = Math.max((int) (8 * Math.random()), 1);
            long deliveryTimestamp = System.currentTimeMillis() + delayMinute * 60 * 1000;
            MQMessage<?> mqMessage = MQMessage.build(video).setDeliveryTimestamp(deliveryTimestamp);
            Result<SendResult> sendResult = rocketMQProducer.send(mqMessage);
            ++count;
            Thread.sleep(1);
        }
        rocketMQProducer.shutdown();
    }

    @Test
    public void testConsume() throws InterruptedException {
        String consumer = "mqcloud-timer-test-consumer";
        AtomicLong counter = new AtomicLong();
        RocketMQConsumer rocketMQConsumer = TestUtil.buildConsumer(consumer, topic);
        rocketMQConsumer.setConsumerCallback(new ConsumerCallback<String, MessageExt>() {
            public void call(String t, MessageExt k) throws Exception {
                counter.incrementAndGet();
            }
        });
        rocketMQConsumer.start();
        long start = System.currentTimeMillis();
        while (true) {
            long count = counter.get();
            System.out.println(count + ", second:" + (System.currentTimeMillis() - start) / 1000);
            Thread.sleep(1000);
        }
    }
}
