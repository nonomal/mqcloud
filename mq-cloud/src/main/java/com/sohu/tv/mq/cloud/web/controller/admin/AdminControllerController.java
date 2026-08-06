package com.sohu.tv.mq.cloud.web.controller.admin;

import com.sohu.tv.mq.cloud.bo.CheckStatusEnum;
import com.sohu.tv.mq.cloud.bo.Cluster;
import com.sohu.tv.mq.cloud.bo.Controller;
import com.sohu.tv.mq.cloud.bo.ControllerNode;
import com.sohu.tv.mq.cloud.service.ClusterService;
import com.sohu.tv.mq.cloud.service.ControllerService;
import com.sohu.tv.mq.cloud.service.MQDeployer;
import com.sohu.tv.mq.cloud.util.MQCloudConfigHelper;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.util.Status;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import com.sohu.tv.mq.cloud.web.vo.UserInfo;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.CollectionUtils;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * controller
 *
 * @author yongfeigao
 * @date 2023年05月25日
 */
@org.springframework.stereotype.Controller
@RequestMapping("/admin/controller")
public class AdminControllerController extends AdminViewController {

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private ControllerService controllerService;

    @Autowired
    private MQCloudConfigHelper mqCloudConfigHelper;

    @Autowired
    private MQDeployer mqDeployer;

    @RequestMapping("/list")
    public String list(@RequestParam(name = "cid", required = false) Integer cid, Map<String, Object> map) {
        setView(map, "list");
        Cluster mqCluster = clusterService.getOrDefaultMQCluster(cid);
        if (mqCluster == null) {
            return view();
        }
        Result<List<Controller>> result = controllerService.query(mqCluster.getId());
        if (result.isNotEmpty()) {
            // 检查状态
            result.getResult().forEach(controller -> {
                Result<GetMetaDataResponseHeader> metaData = controllerService.getControllerMetaData(mqCluster, controller.getAddr());
                if (metaData.isOK()) {
                    controller.setCheckStatus(CheckStatusEnum.OK.getStatus());
                    GetMetaDataResponseHeader getMetaDataResponseHeader = metaData.getResult();
                    controller.setLeader(getMetaDataResponseHeader.isLeader());
                    controller.setGroup(getMetaDataResponseHeader.getGroup());
                    if (controller.isLeader()) {
                        Map<String, Object> config = controllerService.getControllerConfig(mqCluster, controller.getAddr()).getResult();
                        Object countObject = config != null ? config.get("electMasterMaxRetryCount") : null;
                        if (countObject != null && NumberUtils.toInt(String.valueOf(countObject)) <= 0) {
                            setResult(map, "brokerElectEnabled", false);
                        }
                        List<ControllerNode> controllerNodes = controllerService.getControllerNode(mqCluster, controller.getAddr());
                        setResult(map, "needRefreshConfig", needRefreshConfig(config, controllerNodes));
                    }
                } else {
                    controller.setCheckStatus(CheckStatusEnum.FAIL.getStatus());
                }
            });
            setResult(map, "jRaftInitConf", controllerService.buildJRaftInitConf(result.getResult()));
            setResult(map, "jRaftControllerRPCAddr", controllerService.buildJRaftControllerRPCAddr(result.getResult()));
        }
        setResult(map, result);
        setResult(map, "clusters", clusterService.getAllMQCluster());
        setResult(map, "selectedCluster", mqCluster);
        setResult(map, "username", mqCloudConfigHelper.getServerUser());
        return view();
    }

    private boolean needRefreshConfig(Map<String, Object> config, List<ControllerNode> controllerNodes) {
        if (CollectionUtils.isEmpty(controllerNodes)) {
            return false;
        }
        String jRaftInitConf = config.get("jRaftInitConf").toString();
        String jRaftControllerRPCAddr = config.get("jRaftControllerRPCAddr").toString();
        return isConfigChanged(jRaftInitConf, controllerNodes) || isConfigChanged(jRaftControllerRPCAddr, controllerNodes);
    }

    private boolean isConfigChanged(String conf, List<ControllerNode> controllerNodes) {
        String[] confArr = conf.split(",");
        if (confArr.length != controllerNodes.size()) {
            return true;
        }
        for (String node : confArr) {
            String[] nodeArr = node.split(":");
            boolean exist = controllerNodes.stream().anyMatch(n -> n.getIp().equals(nodeArr[0]));
            if (!exist) {
                return true;
            }
        }
        return false;
    }

    @ResponseBody
    @RequestMapping("/config")
    public Result<Map<String, Object>> config(@RequestParam(name = "cid") Integer cid, @RequestParam(name = "addr") String addr, Map<String, Object> map) {
        Cluster mqCluster = clusterService.getOrDefaultMQCluster(cid);
        if (mqCluster == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        return controllerService.getControllerConfig(mqCluster, addr);
    }

    @ResponseBody
    @RequestMapping("/node")
    public Result<List<ControllerNode>> node(@RequestParam(name = "cid") Integer cid, Map<String, Object> map) {
        Cluster mqCluster = clusterService.getOrDefaultMQCluster(cid);
        if (mqCluster == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        return Result.getResult(controllerService.getControllerNode(mqCluster));
    }

    /**
     * 关联
     *
     * @param cid
     * @param broker
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/add", method = RequestMethod.POST)
    public Result<?> add(UserInfo ui, @RequestParam(name = "addr") String addr,
                         @RequestParam(name = "cid") int cid) {
        String[] addrs = addr.split(":");
        if (addrs.length != 2) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        String ip = addrs[0];
        String portStr = addrs[1];
        int port = NumberUtils.toInt(portStr);
        if (port == 0) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Result<?> portResult = mqDeployer.getListenPortInfo(ip, port);
        if (portResult.getStatus() != Status.DB_ERROR.getKey()) {
            return Result.getResult(Status.NO_RESULT);
        }
        Result<?> result = controllerService.save(cid, addr);
        return Result.getWebResult(result);
    }

    /**
     * 下线
     *
     * @param cid
     * @param broker
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/offline", method = RequestMethod.POST)
    public Result<?> offline(UserInfo ui, @RequestParam(name = "addr") String addr,
                             @RequestParam(name = "cid") int cid) {
        logger.warn("offline:{}, user:{}", addr, ui);
        String[] addrs = addr.split(":");
        String ip = addrs[0];
        String portStr = addrs[1];
        int port = NumberUtils.toInt(portStr);
        if (port == 0) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        return mqDeployer.shutdown(ip, port);
    }

    /**
     * 删除
     *
     * @param cid
     * @param broker
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/delete", method = RequestMethod.POST)
    public Result<?> delete(UserInfo ui, @RequestParam(name = "addr") String addr,
                            @RequestParam(name = "cid") int cid) {
        logger.warn("offline:{}, user:{}", addr, ui);
        Result<?> result = controllerService.delete(cid, addr);
        return Result.getWebResult(result);
    }

    /**
     * 启动
     *
     * @return
     */
    @ResponseBody
    @RequestMapping(value = "/startup", method = RequestMethod.POST)
    public Result<?> startup(UserInfo ui, @RequestParam(name = "ip") String ip, @RequestParam(name = "listenPort") int port,
                             @RequestParam(name = "dir") String dir, @RequestParam(name = "cid") int cid) {
        logger.warn("startup, ip:{}, dir:{}, user:{}", ip, dir, ui);
        Result<?> result = mqDeployer.startup(ip, dir, port);
        if (result.isOK()) {
            controllerService.save(cid, ip + ":" + port, dir);
        }
        return result;
    }

    /**
     * 更新线上配置
     */
    @ResponseBody
    @RequestMapping(value = "/update/config")
    public Result<?> updateConfig(UserInfo ui, BrokerConfigUpdateParam configUpdateParam) {
        logger.info("user:{} modify configUpdateParam:{}", ui, configUpdateParam);
        return Result.getWebResult(controllerService.updateControllerConfig(configUpdateParam));
    }

    /**
     * 切换至leader
     */
    @ResponseBody
    @PostMapping(value = "/switchToLeader")
    public Result<?> switchToLeader(UserInfo ui, @RequestParam(name = "addr") String addr, Map<String, Object> map) {
        logger.info("user:{} switchToLeader, addr:{}", ui, addr);
        return controllerService.switchToLeader(addr);
    }

    /**
     * 获取副本信息
     */
    @RequestMapping(value = "/broker/replicaInfo")
    public String getBrokerReplicaInfo(UserInfo ui, @RequestParam(name = "addr", required = false) String addr,
                                       @RequestParam(name = "cid") int cid, Map<String, Object> map) {
        String view = adminViewModule() + "/replicaInfo";
        if (addr == null) {
            Result<List<com.sohu.tv.mq.cloud.bo.Controller>> rst = controllerService.query(cid);
            if (rst.isNotOK()) {
                setResult(map, rst);
                return view;
            }
            addr = rst.getResult().get(0).getAddr();
        }
        setResult(map, controllerService.getBrokerReplicasInfo(clusterService.getMQClusterById(cid), addr));
        setResult(map, "controllerAddr", addr);
        return view;
    }

    /**
     * 清理数据
     */
    @ResponseBody
    @RequestMapping(value = "/broker/cleanData", method = RequestMethod.POST)
    public Result<?> cleanBrokerData(UserInfo ui, @RequestParam(name = "controllerAddr") String controllerAddr,
                                               @RequestParam(name = "brokerName") String brokerName,
                                               @RequestParam(name = "brokerId") int brokerId,
                                               @RequestParam(name = "cid") int cid) {
        logger.warn("cleanControllerBrokerData, controllerAddr:{}, brokerName:{}, brokerId:{}, user:{}", controllerAddr, brokerName, brokerId, ui);
        Cluster cluster = clusterService.getMQClusterById(cid);
        return controllerService.cleanControllerBrokerData(cluster, controllerAddr, brokerName, brokerId);
    }

    /**
     * 刷新配置
     */
    @ResponseBody
    @RequestMapping(value = "/_refresh/config", method = RequestMethod.POST)
    public Result<?> refreshConfig(UserInfo ui, @RequestParam(name = "cid") int cid) {
        logger.info("refreshControllerConfig, cid:{}, user:{}", cid, ui);
        Cluster cluster = clusterService.getMQClusterById(cid);
        return controllerService.refreshConfig(cluster);
    }

    @Override
    public String viewModule() {
        return "controller";
    }
}
