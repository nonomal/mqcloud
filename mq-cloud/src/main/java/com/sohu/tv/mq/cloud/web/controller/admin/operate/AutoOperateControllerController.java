package com.sohu.tv.mq.cloud.web.controller.admin.operate;

import com.sohu.tv.mq.cloud.bo.Cluster;
import com.sohu.tv.mq.cloud.bo.Controller;
import com.sohu.tv.mq.cloud.service.ClusterService;
import com.sohu.tv.mq.cloud.service.ControllerService;
import com.sohu.tv.mq.cloud.service.MQDeployer;
import com.sohu.tv.mq.cloud.util.MQCloudConfigHelper;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.util.Status;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

/**
 * controller自动运维控制器
 *
 * @author yongfeigao
 * @date 2026年05月08日
 */
@RestController
@RequestMapping("/admin/auto/operate/controller")
public class AutoOperateControllerController {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ControllerService controllerService;

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
        Result<Controller> controllerResult = controllerService.queryByAddr(addr);
        if (controllerResult.isNotOK()) {
            logger.warn("addr:{} startup failed, not found", addr);
            return controllerResult;
        }
        Controller controller = controllerResult.getResult();
        // 发送通知
        autoOperateHelper.sendAlarm(controller, "启动");
        // 启动controller
        for (int i = 0; i < 3; i++) {
            Result<?> result = mqDeployer.startup(controller.getIp(), controller.getBaseDir(), controller.getPort(), true);
            logger.info("addr:{} startup result:{}", addr, result);
            if (result.isOK()) {
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
        Result<Controller> controllerResult = controllerService.queryByAddr(addr);
        if (controllerResult.isNotOK()) {
            logger.warn("addr:{} shutdown failed, not found", addr);
            return controllerResult;
        }
        Controller controller = controllerResult.getResult();
        Cluster cluster = clusterService.getMQClusterById(controller.getCid());
        GetMetaDataResponseHeader metaData = controllerService.getControllerMetaData(cluster, controller.getAddr()).getResult();
        // switch leader
        if (metaData != null && metaData.isLeader()) {
            Controller otherController = controllerService.getOtherController(controller.getCid(), controller.getAddr());
            if (otherController != null) {
                controllerService.switchToLeader(otherController.getAddr());
            }
        }
        // 发送通知
        autoOperateHelper.sendAlarm(controller, "关闭");
        Result<?> result = mqDeployer.shutdown(controller.getIp(), controller.getPort());
        logger.info("addr:{} shutdown result:{}", addr, result);
        return result;
    }
}
