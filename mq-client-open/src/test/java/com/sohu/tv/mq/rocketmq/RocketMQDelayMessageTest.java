package com.sohu.tv.mq.rocketmq;

import com.sohu.index.tv.mq.common.ConsumerCallback;
import com.sohu.index.tv.mq.common.MQMessage;
import com.sohu.index.tv.mq.common.Result;
import com.sohu.tv.mq.rocketmq.RocketMQProducer.MessageDelayLevel;
import com.sohu.tv.mq.rocketmq.RocketMQProducerJsonTest.Video;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.MessageExt;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicLong;

public class RocketMQDelayMessageTest {

    String topic = "mqcloud-delay-test-topic";

    @Test
    public void testProduce() throws InterruptedException {
        String producer = "mqcloud-delay-test-topic-producer";
        RocketMQProducer rocketMQProducer = TestUtil.buildProducer(producer, topic);
        rocketMQProducer.start();

        MessageDelayLevel[] levels = {
                MessageDelayLevel.LEVEL_30_SECONDS,
                MessageDelayLevel.LEVEL_1_MINUTE,
                MessageDelayLevel.LEVEL_2_MINUTES,
                MessageDelayLevel.LEVEL_3_MINUTES,
                MessageDelayLevel.LEVEL_4_MINUTES,
                MessageDelayLevel.LEVEL_5_MINUTES,
                MessageDelayLevel.LEVEL_6_MINUTES,
                MessageDelayLevel.LEVEL_7_MINUTES,
        };
        int count = 0;
        for(int i = 1; i <= 300000; ++i) {
            for (MessageDelayLevel messageDelayLevel: levels) {
                Video video = new Video(messageDelayLevel.getLevel(), "msg" + messageDelayLevel);
                MQMessage<?> mqMessage = MQMessage.build(video).setDelayTimeLevel(messageDelayLevel.getLevel());
                Result<SendResult> sendResult = rocketMQProducer.send(mqMessage);
                ++count;
            }
            Thread.sleep(1);
        }
        rocketMQProducer.shutdown();
    }

    @Test
    public void testConsume() throws InterruptedException {
        String consumer = "mq-delay-test-consumer";
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
