package com.sohu.tv.mq.cloud.web.controller.admin.operate;

import com.sohu.tv.mq.cloud.bo.Broker;
import com.sohu.tv.mq.cloud.bo.BrokerControllerConfig;
import com.sohu.tv.mq.cloud.service.BrokerService;
import com.sohu.tv.mq.cloud.service.MQDeployer;
import com.sohu.tv.mq.cloud.util.MQCloudConfigHelper;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.util.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * broker自动运维控制器
 *
 * @author yongfeigao
 * @date 2025年11月07日
 */
@RestController
@RequestMapping("/admin/auto/operate/broker")
public class AutoOperateBrokerController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private MQCloudConfigHelper mqCloudConfigHelper;

    @Autowired
    private BrokerService brokerService;

    @Autowired
    private AutoOperateHelper autoOperateHelper;

    @Autowired
    private MQDeployer mqDeployer;

    /**
     * 启动
     */
    @ResponseBody
    @RequestMapping("/startup")
    public Result<?> startup(String addr, String token, HttpServletRequest request) {
        if (!autoOperateHelper.hasPermission(addr, token, request)) {
            return Result.getResult(Status.PERMISSION_DENIED_ERROR);
        }
        Result<Broker> brokerResult = brokerService.queryBroker(addr);
        if (brokerResult.isNotOK()) {
            logger.warn("addr:{} startup failed, not found", addr);
            return brokerResult;
        }
        Broker broker = brokerResult.getResult();
        // 发送通知
        autoOperateHelper.sendAlarm(broker, "启动");
        // 启动broker
        for (int i = 0; i < 3; i++) {
            Result<?> result = mqDeployer.startup(broker.getIp(), broker.getBaseDir(), broker.getPort(), true);
            logger.info("addr:{} startup result:{}", addr, result);
            if (result.isOK()) {
                brokerService.addWritePerm(broker);
                logger.info("addr:{} add write perm", addr);
                return result;
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.warn("startup sleep interrupted", e);
            }
        }
        return Result.getErrorResult("start failed");
    }

    /**
     * 关闭
     */
    @ResponseBody
    @RequestMapping("/_shutdown")
    public Result<?> shutdown(String addr, String token, HttpServletRequest request) {
        if (!autoOperateHelper.hasPermission(addr, token, request)) {
            return Result.getResult(Status.PERMISSION_DENIED_ERROR);
        }
        Result<Broker> brokerResult = brokerService.queryBroker(addr);
        if (brokerResult.isNotOK()) {
            logger.warn("addr:{} shutdown failed, not found", addr);
            return brokerResult;
        }
        Broker broker = brokerResult.getResult();
        // 发送通知
        autoOperateHelper.sendAlarm(broker, "关闭");
        if (broker.isMaster()) {
            processMasterBeforeShutdown(broker);
        } else {
            BrokerControllerConfig config = brokerService.fetchBrokerControllerConfig(broker.getCid(), broker.getAddr());
            broker.setControllerEnabled(config.isControllerEnabled());
        }
        boolean cleanEpochFile = broker.isControllerEnabled() && !broker.isMaster();
        Result<?> result = mqDeployer.shutdownBroker(broker.getIp(), broker.getPort(), broker.getBaseDir(), cleanEpochFile);
        logger.info("addr:{} shutdown result:{}", addr, result);
        return result;
    }

    /**
     * master关闭前处理
     */
    public void processMasterBeforeShutdown(Broker broker) {
        // 1.停写
        brokerService.wipeWritePerm(broker.getCid(), broker.getBrokerName(), broker.getAddr());
        logger.info("broker:{} wipe write perm", broker);
        waitWriteStop(broker);
        // 2.如果启用了controller需要切主
        BrokerControllerConfig config = brokerService.fetchBrokerControllerConfig(broker.getCid(), broker.getAddr());
        if (!config.isControllerEnabled()) {
            return;
        }
        Result<Broker> otherBrokerResult = brokerService.queryOtherBroker(broker.getCid(), broker.getBrokerName(), broker.getAddr());
        if (otherBrokerResult.isNotOK()) {
            logger.warn("query other broker failed, broker:{}", broker);
            return;
        }
        Broker otherBroker = otherBrokerResult.getResult();
        Result<?> switchToMasterResult = brokerService.switchToMaster(otherBroker);
        if (switchToMasterResult.isNotOK()) {
            logger.warn("switch to master failed, broker:{}", otherBroker);
        }
        // 3.等待连接数完成
        waitBrokerConnectionOK(otherBroker, broker);
    }

    public void waitBrokerConnectionOK(Broker newBroker, Broker preBroker) {
        long start = System.currentTimeMillis();
        while (!mqCloudConfigHelper.isAutoOperateTimeout(start)) {
            Result result = brokerService.checkBrokerConnectionSize(newBroker, preBroker);
            if (result.isOK()) {
                logger.info("addr:{} connection ok, use:{}ms", newBroker.getAddr(), System.currentTimeMillis() - start);
                return;
            }
            logger.info("addr:{} waiting connection ok, result:{}", newBroker.getAddr(), result);
            try {
                Thread.sleep(3000);
            } catch (InterruptedException e) {
                logger.warn("waitBrokerConnectionOK sleep interrupted", e);
            }
        }
        logger.warn("addr:{} wait connection ok timeout, use:{}", newBroker.getAddr(), System.currentTimeMillis() - start);
    }

    /**
     * 等待写入停止
     */
    public void waitWriteStop(Broker broker) {
        long start = System.currentTimeMillis();
        while (!mqCloudConfigHelper.isAutoOperateTimeout(start)) {
            Result<?> result = brokerService.checkBrokerStopWritable(broker.getCid(), broker.getAddr());
            if (result.isOK()) {
                logger.info("addr:{} write stopped, use:{}ms", broker.getAddr(), System.currentTimeMillis() - start);
                return;
            }
            logger.info("addr:{} waiting write stop, result:{}", broker.getAddr(), result);
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                logger.warn("waitWriteStop sleep interrupted", e);
            }
        }
        logger.warn("addr:{} wait write stop timeout, use:{}", broker.getAddr(), System.currentTimeMillis() - start);
    }
}
