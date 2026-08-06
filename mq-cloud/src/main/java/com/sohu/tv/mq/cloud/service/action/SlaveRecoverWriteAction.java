package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import org.springframework.stereotype.Component;

/**
 * broker slave恢复写入行为
 *
 * @author yongfeigao
 * @date 2026年05月12日
 */
@Component
public class SlaveRecoverWriteAction extends BrokerAction {

    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        return brokerService.addWritePerm(step.toBroker());
    }

    @Override
    protected Action getAction() {
        return Action.SLAVE_RECOVER_WRITE;
    }
}
