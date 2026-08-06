package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import org.springframework.stereotype.Component;

/**
 * broker停写行为
 *
 * @author yongfeigao
 * @date 2024年11月06日
 */
@Component
public class BrokerStopWriteAction extends BrokerAction {

    @Override
    public Result<?> executeStep(BrokerAutoUpdateStep step) {
        return brokerService.wipeWritePerm(step.getCid(), step.getBrokerName(), step.getBrokerAddr());
    }

    @Override
    protected Result<?> stepCheckStatusOK(BrokerAutoUpdateStep step, Result<?> executeResult) {
        return brokerService.checkBrokerStopWritable(step.getCid(), step.getBrokerAddr());
    }

    @Override
    protected Action getAction() {
        return Action.STOP_WRITE;
    }
}
