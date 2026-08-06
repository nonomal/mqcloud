package com.sohu.tv.mq.cloud.service.action;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep;
import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;
import com.sohu.tv.mq.cloud.bo.Cluster;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.springframework.stereotype.Component;

/**
 * broker 定时器停止消费行为
 *
 * @author yongfeigao
 * @date 2026年05月20日
 */
@Component
public class BrokerTimerStopDequeueAction extends BrokerAction {
    @Override
    protected Result<?> executeStep(BrokerAutoUpdateStep step) {
        BrokerConfigUpdateParam brokerConfigUpdateParam = new BrokerConfigUpdateParam();
        brokerConfigUpdateParam.setCid(step.getCid());
        brokerConfigUpdateParam.setAddr(step.getBrokerAddr());
        brokerConfigUpdateParam.setConfig("timerStopDequeue=true");
        return brokerService.updateBrokerConfig(brokerConfigUpdateParam);
    }

    @Override
    protected Result<?> stepCheckStatusOK(BrokerAutoUpdateStep step, Result<?> executeResult) {
        Cluster cluster = clusterService.getMQClusterById(step.getCid());
        Result<KVTable> kvTableResult = brokerService.fetchBrokerRuntimeStats(step.getBrokerAddr(), cluster);
        if (kvTableResult.isNotOK()) {
            return kvTableResult;
        }
        KVTable stats = kvTableResult.getResult();
        String timerDequeueTpsStr = stats.getTable().get("timerDequeueTps");
        double timerDequeueTps = NumberUtils.toDouble(timerDequeueTpsStr, 9999);
        if (timerDequeueTps > 0) {
            return Result.getErrorResult("timerDequeueTps:" + timerDequeueTpsStr);
        }
        return Result.getOKResult();
    }

    @Override
    protected Action getAction() {
        return Action.TIMER_STOP_DEQUEUE;
    }
}
