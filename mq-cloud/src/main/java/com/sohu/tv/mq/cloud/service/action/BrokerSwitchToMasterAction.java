package com.sohu.tv.mq.cloud.service.action;


import com.sohu.tv.mq.cloud.bo.Broker;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.util.Result;
import org.springframework.stereotype.Component;

/**
 * broker切换到master行为
 *
 * @author yongfeigao
 * @date 2026年05月11日
 */
@Component
public class BrokerSwitchToMasterAction extends BrokerAction {

    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        Broker broker = new Broker();
        broker.setCid(step.getCid());
        broker.setBrokerName(step.getBrokerName());
        broker.setBrokerID(step.getBrokerId());
        broker.setAddr(step.getBrokerAddr());
        return brokerService.switchToMaster(broker);
    }

    @Override
    protected Result<?> stepCheckStatusOK(BrokerAutoUpdateStep step, Result<?> executeResult) {
        Result<Broker> otherBrokerResult = brokerService.queryOtherBroker(step.getCid(), step.getBrokerName(), step.getBrokerAddr());
        if (otherBrokerResult.isNotOK()) {
            return otherBrokerResult;
        }
        return brokerService.checkBrokerConnectionSize(step.toBroker(), otherBrokerResult.getResult());
    }

    @Override
    protected Action getAction() {
        return Action.SWITCH_TO_MASTER;
    }
}
