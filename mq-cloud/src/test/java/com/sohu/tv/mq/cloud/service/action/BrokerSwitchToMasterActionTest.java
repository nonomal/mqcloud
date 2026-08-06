package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.Application;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.dao.BrokerAutoUpdateStepDao;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = Application.class)
public class BrokerSwitchToMasterActionTest {

    @Autowired
    BrokerSwitchToMasterAction brokerSwitchToMasterAction;

    @Autowired
    BrokerAutoUpdateStepDao brokerAutoUpdateStepDao;

    @Test
    public void test() {
        BrokerAutoUpdateStep step = brokerAutoUpdateStepDao.selectById(4390);
        step.setCid(8);
        brokerSwitchToMasterAction.stepCheckStatusOK(step, null);
    }

}