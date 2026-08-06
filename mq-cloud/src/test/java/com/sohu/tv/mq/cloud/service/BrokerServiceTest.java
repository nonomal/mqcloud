package com.sohu.tv.mq.cloud.service;

import com.sohu.tv.mq.cloud.Application;
import com.sohu.tv.mq.cloud.bo.Broker;
import com.sohu.tv.mq.cloud.bo.Cluster;
import com.sohu.tv.mq.cloud.util.Result;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class BrokerServiceTest {

    @Autowired
    private BrokerService brokerService;

    @Autowired
    private ClusterService clusterService;

    @Test
    public void test() {
        Cluster cluster = clusterService.queryById(8).getResult();
        List<Broker> brokerList = brokerService.query(cluster.getId()).getResult();
        if (brokerList == null) {
            return;
        }
        Result<Boolean> rst = brokerService.switchToMaster(cluster, brokerList.get(0));
        Assert.assertTrue(rst.isOK());
    }

}