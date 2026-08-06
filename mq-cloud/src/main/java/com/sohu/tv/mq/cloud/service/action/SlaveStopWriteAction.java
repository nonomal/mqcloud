package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import org.springframework.stereotype.Component;

/**
 * broker停写行为
 *
 * @author yongfeigao
 * @date 2026年05月14日
 */
@Component
public class SlaveStopWriteAction extends BrokerAction {

    @Override
    public Result<?> executeStep(BrokerAutoUpdateStep step) {
        return brokerService.wipeWritePerm(step.getCid(), step.getBrokerName(), step.getBrokerAddr());
    }

    @Override
    protected Action getAction() {
        return Action.SLAVE_STOP_WRITE;
    }
}
