package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import org.springframework.stereotype.Component;

/**
 * broker 定时器恢复消费行为
 *
 * @author yongfeigao
 * @date 2026年05月20日
 */
@Component
public class BrokerTimerRecoverDequeueAction extends BrokerAction {
    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        BrokerConfigUpdateParam brokerConfigUpdateParam = new BrokerConfigUpdateParam();
        brokerConfigUpdateParam.setCid(step.getCid());
        brokerConfigUpdateParam.setAddr(step.getBrokerAddr());
        brokerConfigUpdateParam.setConfig("timerStopDequeue=false");
        return brokerService.updateBrokerConfig(brokerConfigUpdateParam);
    }

    @Override
    protected Action getAction() {
        return Action.TIMER_RECOVER_DEQUEUE;
    }
}
