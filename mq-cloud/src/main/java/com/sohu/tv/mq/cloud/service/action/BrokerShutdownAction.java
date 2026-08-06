package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import org.springframework.stereotype.Component;

/**
 * broker关闭行为
 *
 * @author yongfeigao
 * @date 2024年11月06日
 */
@Component
public class BrokerShutdownAction extends BrokerAction {

    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        boolean cleanEpochFile = step.isEnableController() && !step.isMaster();
        return mqDeployer.shutdownBroker(step.getIp(), step.getPort(), step.getBrokerBaseDir(), cleanEpochFile);
    }

    @Override
    protected Action getAction() {
        return Action.SHUTDOWN;
    }
}
