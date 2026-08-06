package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.service.ControllerService;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * controller禁用master选举行为
 */
@Component
public class ControllerDisableElectMasterAction extends BrokerAction {

    @Autowired
    private ControllerService controllerService;

    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        BrokerConfigUpdateParam param = new BrokerConfigUpdateParam();
        param.setCid(step.getCid());
        param.setCluster(true);
        param.setConfig("electMasterMaxRetryCount=0");
        return controllerService.updateControllerConfig(param);
    }

    @Override
    protected Action getAction() {
        return Action.DISABLE_ELECT_MASTER;
    }
}
