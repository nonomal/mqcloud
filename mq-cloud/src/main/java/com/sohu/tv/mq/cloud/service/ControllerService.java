package com.sohu.tv.mq.cloud.service;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.google.common.collect.Lists;
import com.sohu.tv.mq.cloud.bo.*;
import com.sohu.tv.mq.cloud.dao.ControllerDao;
import com.sohu.tv.mq.cloud.mq.DefaultCallback;
import com.sohu.tv.mq.cloud.mq.MQAdminCallback;
import com.sohu.tv.mq.cloud.mq.MQAdminTemplate;
import com.sohu.tv.mq.cloud.util.JRaftClientUtil;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo;
import org.apache.rocketmq.remoting.protocol.header.controller.GetMetaDataResponseHeader;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;
import java.util.stream.Collectors;

import static com.sohu.tv.mq.cloud.util.Status.NO_RESULT;

/**
 * ControllerService
 *
 * @author yongfeigao
 * @date 2023年05月22日
 */
@Service
public class ControllerService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private ControllerDao controllerDao;

    @Autowired
    private MQAdminTemplate mqAdminTemplate;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private BrokerService brokerService;

    /**
     * 保存记录
     *
     * @return 返回Result
     */
    public Result<?> save(int cid, String addr) {
        return save(cid, addr, null);
    }

    /**
     * 保存记录
     */
    public Result<?> save(int cid, String addr, String baseDir) {
        try {
            // 1.将节点加入jraft集群
            Cluster cluster = clusterService.getMQClusterById(cid);
            String leaderAddr = getLeaderAddr(cid, addr);
            if (StringUtils.isNotBlank(leaderAddr)) {
                List<PeerId> peerIds = JRaftClientUtil.getPeers(cluster.getName(), leaderAddr);
                String[] addrArray = addr.split(":");
                String ip = addrArray[0];
                if (peerIds.stream().anyMatch(p -> p.getIp().equals(ip))) {
                    logger.warn("peer already exist, cluster:{}, addr:{}", cluster.getName(), addr);
                } else {
                    int port = Integer.parseInt(addrArray[1]) - 1;
                    Status status = JRaftClientUtil.addPeer(cluster.getName(), leaderAddr, ip, port);
                    if (!status.isOk()) {
                        logger.error("addPeer err, cluster:{}, addr:{}", cluster.getName(), addr);
                        return Result.getErrorResult(status.getErrorMsg());
                    }
                }
            }
            // 2.保存到数据库中
            return Result.getResult(controllerDao.insert(cid, addr, baseDir));
        } catch (Exception e) {
            logger.error("insert err, cid:{}, addr:{}, baseDir:{}", cid, addr, baseDir, e);
            return Result.getDBErrorResult(e);
        }
    }

    private String getLeaderAddr(int cid, String addr) {
        Controller otherController = getOtherController(cid, addr);
        if (otherController == null) {
            return null;
        }
        Cluster cluster = clusterService.getMQClusterById(cid);
        Result<GetMetaDataResponseHeader> metaResult = getControllerMetaData(cluster, otherController.getAddr());
        if (metaResult.isNotOK()) {
            return null;
        }
        return metaResult.getResult().getControllerLeaderId();
    }

    public Controller getOtherController(int cid, String addr) {
        List<Controller> controllers = query(cid).getResult();
        if (CollectionUtils.isEmpty(controllers)) {
            return null;
        }
        Optional<Controller> controllerOptional = controllers.stream()
                .filter(c -> !c.getAddr().equals(addr))
                .findFirst();
        return controllerOptional.orElse(null);
    }

    /**
     * 查询集群列表
     *
     * @return Result<List<Controller>>
     */
    public Result<List<Controller>> query(int cid) {
        try {
            return Result.getResult(controllerDao.selectByClusterId(cid));
        } catch (Exception e) {
            logger.error("query cid:{} err", cid, e);
            return Result.getDBErrorResult(e);
        }
    }

    public Result<Controller> queryByAddr(String addr) {
        try {
            return Result.getResult(controllerDao.selectByAddr(addr));
        } catch (Exception e) {
            logger.error("query addr:{} err", addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 查询全部Controller
     *
     * @return Result<List<Controller>>
     */
    public Result<List<Controller>> queryAll() {
        try {
            return Result.getResult(controllerDao.selectAll());
        } catch (Exception e) {
            logger.error("query all err", e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 删除记录
     */
    public Result<?> delete(int cid, String addr) {
        try {
            // 1.从jraft集群中删除
            Cluster cluster = clusterService.getMQClusterById(cid);
            String leaderId = getLeaderAddr(cid, addr);
            if (StringUtils.isNotBlank(leaderId)) {
                List<PeerId> peerIds = JRaftClientUtil.getPeers(cluster.getName(), leaderId);
                String ip = addr.split(":")[0];
                PeerId peerId = peerIds.stream()
                        .filter(n -> n.getIp().equals(ip))
                        .findFirst()
                        .get();
                Status status = JRaftClientUtil.removePeer(cluster.getName(), leaderId, peerId.getIp(), peerId.getPort());
                if (!status.isOk()) {
                    logger.error("removePeer err, cluster:{}, addr:{}", cluster.getName(), addr);
                    return Result.getErrorResult(status.getErrorMsg());
                }
            }
            // 2.从数据库中删除
            return Result.getResult(controllerDao.delete(cid, addr));
        } catch (Exception e) {
            logger.error("delete err, cid:{}, addr:{}", cid, addr, e);
            return Result.getDBErrorResult(e);
        }
    }


    /**
     * 更新记录
     *
     * @param cid
     * @param addr
     * @return
     */
    public Result<?> update(int cid, String addr, CheckStatusEnum checkStatusEnum) {
        try {
            return Result.getResult(controllerDao.update(cid, addr, checkStatusEnum.getStatus()));
        } catch (Exception e) {
            logger.error("update err, cid:{}, addr:{}", cid, addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 获取Controller元数据
     */
    public Result<GetMetaDataResponseHeader> getControllerMetaData(Cluster cluster, String addr) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<GetMetaDataResponseHeader>>() {
            public Result<GetMetaDataResponseHeader> callback(MQAdminExt mqAdmin) throws Exception {
                try {
                    GetMetaDataResponseHeader getMetaDataResponseHeader = mqAdmin.getControllerMetaData(addr);
                    return Result.getResult(getMetaDataResponseHeader);
                } catch (Exception e) {
                    return Result.getDBErrorResult(e).setMessage("addr:" + addr + ";Exception: " + e.getMessage());
                }
            }

            public Cluster mqCluster() {
                return cluster;
            }

            @Override
            public Result<GetMetaDataResponseHeader> exception(Exception e) throws Exception {
                return Result.getDBErrorResult(e).setMessage("Exception: " + e.getMessage());
            }
        });
    }

    /**
     * 获取Controller配置
     */
    public Result<Map<String, Object>> getControllerConfig(Cluster cluster, String addr) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<Map<String, Object>>>() {
            public Result<Map<String, Object>> callback(MQAdminExt mqAdmin) throws Exception {
                try {
                    Map<String, Properties> map = mqAdmin.getControllerConfig(Lists.newArrayList(addr));
                    Properties props = map.get(addr);
                    Map<String, Object> sortedMap = new TreeMap<>();
                    for (String key : props.stringPropertyNames()) {
                        sortedMap.put(key, props.getProperty(key));
                    }
                    return Result.getResult(sortedMap);
                } catch (Exception e) {
                    return Result.getDBErrorResult(e).setMessage("addr:" + addr + ";Exception: " + e.getMessage());
                }
            }

            public Cluster mqCluster() {
                return cluster;
            }

            @Override
            public Result<Map<String, Object>> exception(Exception e) throws Exception {
                return Result.getDBErrorResult(e).setMessage("Exception: " + e.getMessage());
            }
        });
    }

    /**
     * 更新Controller配置
     */
    public Result<?> updateControllerConfig(BrokerConfigUpdateParam configUpdateParam) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<?>>() {
            public Cluster mqCluster() {
                return clusterService.getMQClusterById(configUpdateParam.getCid());
            }

            public Result<?> callback(MQAdminExt mqAdmin) throws Exception {
                Properties properties = configUpdateParam.getConfigProperties();
                List<String> controllers = new ArrayList<>();
                if (configUpdateParam.isCluster()) {
                    Result<List<Controller>> result = query(configUpdateParam.getCid());
                    if (result.isNotOK()) {
                        return result;
                    }
                    result.getResult().stream().forEach(controller -> controllers.add(controller.getAddr()));
                } else {
                    controllers.add(configUpdateParam.getAddr());
                }
                mqAdmin.updateControllerConfig(properties, controllers);
                return Result.getOKResult();
            }

            public Result<?> exception(Exception e) {
                logger.error("updateControllerConfig:{} err", configUpdateParam, e);
                return Result.getDBErrorResult(e);
            }
        });
    }


    /**
     * 获取broker的副本信息
     */
    public Result<Map<String, ReplicasInfo>> getBrokerReplicasInfo(Cluster cluster, String controllerAddr) {
        Result<List<Broker>> brokersResult = brokerService.query(cluster.getId());
        if (brokersResult.isEmpty()) {
            return Result.getResult(NO_RESULT).setMessage("no broker");
        }
        List<String> brokers = brokersResult.getResult().stream()
                .map(Broker::getBrokerName)
                .distinct()
                .collect(Collectors.toList());
        return mqAdminTemplate.execute(new MQAdminCallback<Result<Map<String, ReplicasInfo>>>() {
            public Result<Map<String, ReplicasInfo>> callback(MQAdminExt mqAdmin) throws Exception {
                BrokerReplicasInfo replicasInfo = mqAdmin.getInSyncStateData(controllerAddr, brokers);
                Map<String, ReplicasInfo> replicasInfoMap = new TreeMap<>(replicasInfo.getReplicasInfoTable());
                return Result.getResult(replicasInfoMap);
            }
            @Override
            public Result<Map<String, ReplicasInfo>> exception(Exception e) throws Exception {
                logger.error("getBrokerReplicasInfo:{} err", controllerAddr, e);
                return Result.getDBErrorResult(e);
            }

            public Cluster mqCluster() {
                return cluster;
            }
        });
    }

    /**
     * 清理Controller上Broker数据
     */
    public Result<?> cleanControllerBrokerData(Cluster cluster, String controllerAddr, String brokerName, int brokerId) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<?>>() {
            public Result<?> callback(MQAdminExt mqAdmin) throws Exception {
                try {
                    if (brokerId == -1) {
                        mqAdmin.cleanControllerBrokerData(controllerAddr, cluster.getName(), brokerName, null, true);
                    } else {
                        mqAdmin.cleanControllerBrokerData(controllerAddr, cluster.getName(), brokerName, String.valueOf(brokerId), false);
                    }
                    return Result.getOKResult();
                } catch (Exception e) {
                    return Result.getDBErrorResult(e).setMessage("broker " + brokerName + ":" + brokerId + ";Exception: " + e.getMessage());
                }
            }

            public Cluster mqCluster() {
                return cluster;
            }

            @Override
            public Result<?> exception(Exception e) throws Exception {
                return Result.getDBErrorResult(e).setMessage("Exception: " + e.getMessage());
            }
        });
    }


    public List<ControllerNode> getControllerNode(Cluster cluster) {
        return getControllerNode(cluster, null);
    }

    /**
     * 获取Controller配置
     */
    public List<ControllerNode> getControllerNode(Cluster cluster, String controllerAddr) {
        if (controllerAddr == null) {
            Result<List<Controller>> controllersResult = query(cluster.getId());
            if (controllersResult.isEmpty()) {
                return null;
            }
            controllerAddr = controllersResult.getResult().get(0).getAddr();
        }
        GetMetaDataResponseHeader metaData = getControllerMetaData(cluster, controllerAddr).getResult();
        if (metaData == null) {
            return null;
        }
        String leaderId = metaData.getControllerLeaderId();
        List<PeerId> allPeerIds = JRaftClientUtil.getPeers(cluster.getName(), leaderId);
        List<PeerId> alivePeerIds = JRaftClientUtil.getAlivePeers(cluster.getName(), leaderId);
        List<ControllerNode> controllerNodes = new ArrayList<>();
        for (PeerId peerId : allPeerIds) {
            boolean alive = alivePeerIds.stream().anyMatch(p -> p.equals(peerId));
            ControllerNode controllerNode = new ControllerNode();
            controllerNode.setIp(peerId.getIp());
            controllerNode.setPort(peerId.getPort());
            controllerNode.setAlive(alive);
            controllerNodes.add(controllerNode);
        }
        return controllerNodes;
    }

    /**
     * controller增减后，将配置刷新到controller和broker配置中
     */
    public Result<?> refreshConfig(Cluster cluster) {
        Result<List<Controller>> controllersResult = query(cluster.getId());
        if (controllersResult.isEmpty()) {
            return Result.getResult(NO_RESULT);
        }
        List<Controller> controllers = controllersResult.getResult();

        // 1.更新controller配置
        BrokerConfigUpdateParam configUpdateParam = new BrokerConfigUpdateParam();
        configUpdateParam.setCluster(true);
        configUpdateParam.setCid(cluster.getId());
        configUpdateParam.setConfig(buildControllerConfig(controllers));
        Result<?> updateResult = updateControllerConfig(configUpdateParam);
        if (updateResult.isNotOK()) {
            logger.error("updateControllerConfig err, cluster:{}", cluster.getName());
            return updateResult;
        }

        // 2.更新broker集群配置
        String controllerAddr = controllers.stream()
                .map(Controller::getAddr)
                .collect(Collectors.joining(";"));
        BrokerConfigUpdateParam brokerConfigUpdateParam = new BrokerConfigUpdateParam();
        brokerConfigUpdateParam.setCluster(true);
        brokerConfigUpdateParam.setCid(cluster.getId());
        brokerConfigUpdateParam.setConfig("controllerAddr=" + controllerAddr);
        Result configResult = brokerService.updateBrokerConfig(brokerConfigUpdateParam);
        if (configResult.isNotOK()) {
            logger.error("updateBrokerConfig err, cluster:{}", cluster.getName());
            return configResult;
        }
        return configResult;
    }

    private String buildControllerConfig(List<Controller> controllers) {
        StringBuilder controllerConfig = new StringBuilder();
        controllerConfig.append("jRaftInitConf=");
        controllerConfig.append(buildJRaftInitConf(controllers));
        controllerConfig.append("&");
        controllerConfig.append("jRaftControllerRPCAddr=");
        controllerConfig.append(buildJRaftControllerRPCAddr(controllers));
        return controllerConfig.toString();
    }

    public String buildJRaftInitConf(List<Controller> controllers) {
        return controllers.stream()
                .map(c -> {
                    String[] addrArray = c.getAddr().split(":");
                    String ip = addrArray[0];
                    int port = Integer.parseInt(addrArray[1]) - 1;
                    return ip + ":" + port;
                })
                .collect(Collectors.joining(","));
    }

    public String buildJRaftControllerRPCAddr(List<Controller> controllers){
        return String.join(",", controllers.stream()
                .map(Controller::getAddr)
                .collect(Collectors.toList()));
    }

    /**
     * 切换addr到leader
     */
    public Result<?> switchToLeader(String addr) {
        Result<Controller> result = queryByAddr(addr);
        if (result.isNotOK()) {
            return Result.getWebResult(result);
        }
        Controller controller = result.getResult();
        Cluster cluster = clusterService.getMQClusterById(controller.getCid());
        Result<GetMetaDataResponseHeader> metaResult = getControllerMetaData(cluster, controller.getAddr());
        if (metaResult.isNotOK()) {
            return Result.getWebResult(metaResult);
        }
        logger.info("switch to leader, addr:{}", addr);
        String leaderId = metaResult.getResult().getControllerLeaderId();
        List<PeerId> peerIds = JRaftClientUtil.getAlivePeers(cluster.getName(), leaderId);
        PeerId peerId = peerIds.stream()
                .filter(n -> n.getIp().equals(controller.getIp()))
                .findFirst()
                .get();
        com.alipay.sofa.jraft.Status status = JRaftClientUtil.transferLeader(cluster.getName(), leaderId, peerId.getIp(), peerId.getPort());
        if (!status.isOk()) {
            logger.error("switchToLeader err, cluster:{}, addr:{}, status:{}", cluster.getName(), addr, status);
            return Result.getErrorResult(status.getErrorMsg());
        }
        return Result.getOKResult();
    }
}
