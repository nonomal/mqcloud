package com.sohu.tv.mq.cloud.service;

import com.sohu.tv.mq.cloud.bo.*;
import com.sohu.tv.mq.cloud.common.model.*;
import com.sohu.tv.mq.cloud.common.mq.SohuMQAdmin;
import com.sohu.tv.mq.cloud.dao.BrokerDao;
import com.sohu.tv.mq.cloud.dao.BrokerTmpDao;
import com.sohu.tv.mq.cloud.mq.DefaultCallback;
import com.sohu.tv.mq.cloud.mq.DefaultSohuMQAdmin;
import com.sohu.tv.mq.cloud.mq.MQAdminCallback;
import com.sohu.tv.mq.cloud.mq.MQAdminTemplate;
import com.sohu.tv.mq.cloud.util.DBUtil;
import com.sohu.tv.mq.cloud.util.Result;
import com.sohu.tv.mq.cloud.util.Status;
import com.sohu.tv.mq.cloud.web.controller.param.BrokerConfigUpdateParam;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.remoting.protocol.RemotingSysResponseCode;
import org.apache.rocketmq.remoting.protocol.body.BrokerReplicasInfo.ReplicasInfo;
import org.apache.rocketmq.remoting.protocol.body.BrokerStatsData;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.body.KVTable;
import org.apache.rocketmq.remoting.protocol.body.ProducerTableInfo;
import org.apache.rocketmq.remoting.protocol.header.controller.ElectMasterResponseHeader;
import org.apache.rocketmq.remoting.protocol.route.BrokerData;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.*;

/**
 * broker
 * 
 * @author yongfeigao
 * @date 2018年11月14日
 */
@Service
public class BrokerService {

    private Logger logger = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private BrokerDao brokerDao;

    @Autowired
    private MQAdminTemplate mqAdminTemplate;

    @Autowired
    private ClusterService clusterService;

    @Autowired
    private SqlSessionFactory mqSqlSessionFactory;

    @Autowired
    private BrokerTmpDao brokerTmpDao;

    @Autowired
    private ControllerService controllerService;

    /**
     * 查询集群的broker
     * 
     * @return Result<List<Broker>>
     */
    public Result<List<Broker>> query(int cid) {
        List<Broker> result = null;
        try {
            result = brokerDao.selectByClusterId(cid);
            if (result != null && result.size() == 0) {
                result = null;
            }
        } catch (Exception e) {
            logger.error("query cid:{} err", cid, e);
            return Result.getDBErrorResult(e);
        }
        return Result.getResult(result);
    }

    /**
     * 查询所有的broker
     *
     * @return Result<List<Broker>>
     */
    public Result<List<Broker>> queryAll() {
        try {
            return Result.getResult(brokerDao.selectAll());
        } catch (Exception e) {
            logger.error("query all err", e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 删除记录
     * 
     * @return 返回Result
     */
    public Result<?> delete(int cid) {
        Integer result = null;
        try {
            result = brokerDao.delete(cid);
        } catch (Exception e) {
            logger.error("delete err, cid:{}", cid, e);
            return Result.getDBErrorResult(e);
        }
        return Result.getResult(result);
    }

    /**
     * 刷新记录
     * 
     * @return 返回Result
     */
    public Result<?> refresh(int cid, List<Broker> brokerList) {
        try {
            brokerDao.delete(cid);
            for (Broker broker : brokerList) {
                brokerDao.insert(broker);
            }
        } catch (Exception e) {
            logger.error("refresh err, cid:{}", cid, e);
            return Result.getDBErrorResult(e);
        }
        return Result.getOKResult();
    }

    /**
     * 更新记录
     */
    public Result<?> update(int cid, String addr, CheckStatusEnum checkStatusEnum, int brokerId) {
        Integer result = null;
        try {
            result = brokerDao.update(cid, addr, checkStatusEnum.getStatus(), brokerId);
        } catch (Exception e) {
            logger.error("update err, cid:{}, addr:{}", cid, addr, e);
            return Result.getDBErrorResult(e);
        }
        return Result.getResult(result);
    }

    public BrokerControllerConfig fetchBrokerControllerConfig(int cid, String brokerAddr) {
        Cluster cluster = clusterService.getMQClusterById(cid);
        return fetchBrokerControllerConfig(cluster, brokerAddr);
    }

    public BrokerControllerConfig fetchBrokerControllerConfig(Cluster cluster, String brokerAddr) {
        BrokerControllerConfig brokerControllerConfig = new BrokerControllerConfig();
        Properties config = fetchBrokerConfig(cluster, brokerAddr).getResult();
        if (config == null) {
            return brokerControllerConfig;
        }
        if ("true".equals(config.getProperty("enableControllerMode"))) {
            brokerControllerConfig.setControllerEnabled(true);
            brokerControllerConfig.setBrokerId(NumberUtils.toInt(config.getProperty("brokerId"), -1));
            brokerControllerConfig.setControllerAddr(config.getProperty("controllerAddr"));
        }
        if ("4".equals(config.getProperty("brokerPermission"))) {
            brokerControllerConfig.setWritable(false);
        }
        return brokerControllerConfig;
    }

    /**
     * 抓取broker配置
     */
    public Result<Properties> fetchBrokerConfig(int cid, String brokerAddr) {
        Cluster cluster = clusterService.getMQClusterById(cid);
        return fetchBrokerConfig(cluster, brokerAddr);
    }

    /**
     * 抓取broker配置
     */
    public Result<Properties> fetchBrokerConfig(Cluster cluster, String brokerAddr) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<Properties>>() {
            public Cluster mqCluster() {
                return cluster;
            }

            public Result<Properties> callback(MQAdminExt mqAdmin) throws Exception {
                Properties properties = mqAdmin.getBrokerConfig(brokerAddr);
                return Result.getResult(properties);
            }

            public Result<Properties> exception(Exception e) {
                logger.error("cluster:{} brokerAddr:{}, getBrokerConfig err", cluster, brokerAddr, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    public boolean resetBrokerId(Cluster cluster, Broker broker) {
        BrokerControllerConfig brokerControllerConfig = fetchBrokerControllerConfig(cluster, broker.getAddr());
        if (!brokerControllerConfig.isControllerEnabled()) {
            return false;
        }
        int brokerId = brokerControllerConfig.getBrokerId();
        if (brokerId == broker.getBrokerID()) {
            return false;
        }
        logger.info("resetBrokerId broker:{} oldBrokerId:{} newBrokerId:{}", broker.getAddr(), broker.getBrokerID(), brokerId);
        broker.setBrokerID(brokerId);
        return true;
    }
    
    /**
     * 更新broker配置
     * @param cid
     * @param brokerAddr
     * @return
     */
    public Result<?> updateBrokerConfig(BrokerConfigUpdateParam brokerConfigUpdateParam) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<?>>() {
            public Cluster mqCluster() {
                return clusterService.getMQClusterById(brokerConfigUpdateParam.getCid());
            }

            public Result<?> callback(MQAdminExt mqAdmin) throws Exception {
                Properties properties = brokerConfigUpdateParam.getConfigProperties();
                if (brokerConfigUpdateParam.isCluster()) {
                    Result<List<Broker>> result = query(brokerConfigUpdateParam.getCid());
                    if (result.isNotOK()) {
                        return result;
                    }
                    List<Broker> brokers = result.getResult();
                    for (Broker broker : brokers) {
                        mqAdmin.updateBrokerConfig(broker.getAddr(), properties);
                    }
                } else {
                    mqAdmin.updateBrokerConfig(brokerConfigUpdateParam.getAddr(), properties);
                }
                return Result.getOKResult();
            }

            public Result<?> exception(Exception e) {
                logger.error("brokerConfigUpdateParam:{} updateBrokerConfig err", brokerConfigUpdateParam, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 获取发送消息限速情况
     * @param cid
     * @param brokerAddr
     * @return
     */
    public Result<BrokerRateLimitData> fetchSendMessageRateLimitInBroker(int cid, String brokerAddr) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<BrokerRateLimitData>>() {
            public Cluster mqCluster() {
                return clusterService.getMQClusterById(cid);
            }

            public Result<BrokerRateLimitData> callback(MQAdminExt mqAdmin) throws Exception {
                BrokerRateLimitData brokerRateLimitData =
                        ((SohuMQAdmin) mqAdmin).fetchSendMessageRateLimitInBroker(brokerAddr);
                if (brokerRateLimitData != null) {
                    List<TopicRateLimit> list = brokerRateLimitData.getTopicRateLimitList();
                    if (list != null) {
                        Collections.sort(list, (t1, t2) -> {
                            // 等待时长逆序
                            if (t1.getLastNeedWaitMicrosecs() > t2.getLastNeedWaitMicrosecs()) {
                                return -1;
                            }
                            if (t1.getLastNeedWaitMicrosecs() < t2.getLastNeedWaitMicrosecs()) {
                                return 1;
                            }
                            // 限流时间逆序
                            if (t1.getLastRateLimitTimestamp() > t2.getLastRateLimitTimestamp()) {
                                return -1;
                            }
                            if (t1.getLastRateLimitTimestamp() < t2.getLastRateLimitTimestamp()) {
                                return 1;
                            }
                            return 0;
                        });
                    }
                }
                return Result.getResult(brokerRateLimitData);
            }

            public Result<BrokerRateLimitData> exception(Exception e) {
                logger.warn("cid:{}, brokerAddr:{}, fetchSendMessageRateLimitInBroker err:{}", cid, brokerAddr, e.toString());
                // 判断是否支持
                if (e instanceof MQClientException && ((MQClientException) e).getResponseCode() == RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED) {
                    Result<BrokerRateLimitData> result = Result.getResult(Status.BROKER_UNSUPPORTED_ERROR);
                    return result.setException(e);
                }
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 更新限速
     * @param cid
     * @param brokerAddr -1表示更新整个集群
     * @param updateSendMsgRateLimitRequestHeader
     */
    public Result<?> updateSendMessageRateLimit(int cid, String brokerAddr, UpdateSendMsgRateLimitRequestHeader updateSendMsgRateLimitRequestHeader) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<Void>>() {
            public Cluster mqCluster() {
                return clusterService.getMQClusterById(cid);
            }

            public Result<Void> callback(MQAdminExt mqAdmin) throws Exception {
                SohuMQAdmin sohuMQAdmin = (SohuMQAdmin) mqAdmin;
                if ("-1".equals(brokerAddr)) {
                    Result<List<Broker>> result = query(cid);
                    List<Broker> brokers = result.getResult();
                    for (Broker broker : brokers) {
                        sohuMQAdmin.updateSendMessageRateLimit(broker.getAddr(), updateSendMsgRateLimitRequestHeader);
                    }
                } else {
                    sohuMQAdmin.updateSendMessageRateLimit(brokerAddr, updateSendMsgRateLimitRequestHeader);
                }
                return Result.getOKResult();
            }

            public Result<Void> exception(Exception e) {
                logger.error("cid:{}, brokerAddr:{}, param:{} updateSendMessageRateLimit err", cid, brokerAddr, updateSendMsgRateLimitRequestHeader, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 查询broker
     */
    public Result<Broker> queryBroker(int cid, String addr) {
        try {
            return Result.getResult(brokerDao.selectBroker(cid, addr));
        } catch (Exception e) {
            logger.error("queryBroker:{} err", addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 查询broker
     */
    public Result<Broker> queryBroker(String addr) {
        try {
            return Result.getResult(brokerDao.selectBrokerByAddr(addr));
        } catch (Exception e) {
            logger.error("queryBroker:{} err", addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 查询临时broker
     */
    public Result<List<Broker>> queryTmpBroker(int cid) {
        try {
            return Result.getResult(brokerTmpDao.selectByClusterId(cid));
        } catch (Exception e) {
            logger.error("queryTmpBroker:{} err", cid, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 查询临时broker
     */
    public Result<Broker> queryTmpBroker(int cid, String addr) {
        try {
            return Result.getResult(brokerTmpDao.select(cid, addr));
        } catch (Exception e) {
            logger.error("queryTmpBroker cid:{} addr:{} err", cid, addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 更新是否可写
     */
    public Result<?> updateWritable(int cid, String addr, boolean writable) {
        Integer result = null;
        try {
            if (writable) {
                result = brokerDao.updateWritable(cid, addr, 1);
            } else {
                result = brokerDao.updateWritable(cid, addr, 0);
            }
        } catch (Exception e) {
            logger.error("updateWritable err, cid:{}, addr:{}", cid, addr, e);
            return Result.getDBErrorResult(e);
        }
        return Result.getResult(result);
    }

    /**
     * 获取broker的timerWheel信息
     */
    public Result<?> getTimerWheelMetrics(int cid, String brokerAddr) {
        return mqAdminTemplate.execute(new DefaultCallback<Result<Void>>() {
            public Cluster mqCluster() {
                return clusterService.getMQClusterById(cid);
            }

            public Result<Void> callback(MQAdminExt mqAdmin) throws Exception {
                TimerMetricsSerializeWrapper timerMetricsSerializeWrapper = ((SohuMQAdmin) mqAdmin).getTimerWheelMetrics(brokerAddr);
                return Result.getResult(timerMetricsSerializeWrapper);
            }

            public Result<Void> exception(Exception e) {
                logger.error("cid:{}, brokerAddr:{} getTimerWheelMetrics err", cid, brokerAddr, e);
                if (e instanceof MQBrokerException &&
                        ((MQBrokerException) e).getResponseCode() == RemotingSysResponseCode.REQUEST_CODE_NOT_SUPPORTED) {
                    return Result.getResult(Status.BROKER_UNSUPPORTED_ERROR);
                }
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 重置topic流量
     */
    public Result<Integer> resetDayCount() {
        try {
            return Result.getResult(brokerDao.resetDayCount());
        } catch (Exception e) {
            logger.error("resetDayCount err", e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 更新topic日流量
     *
     * @param brokerTrafficList
     * @return
     */
    public Result<Integer> updateDayCount(List<BrokerTraffic> brokerTrafficList) {
        return DBUtil.batchUpdate(mqSqlSessionFactory, BrokerDao.class, dao -> {
            for (BrokerTraffic brokerTraffic : brokerTrafficList) {
                dao.updateDayCount(brokerTraffic);
            }
        });
    }

    /**
     * 保存broker到临时表
     */
    public Result<?> saveBrokerTmp(Map<String, Object> param){
        Object brokerName = param.get("brokerName");
        if (brokerName == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Object brokerIdObj = param.get("brokerId");
        if (brokerIdObj == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        int brokerId = NumberUtils.toInt(String.valueOf(brokerIdObj), -1);
        if (brokerId == -1) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Object ipObj = param.get("ip");
        if (ipObj == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Object portObj = param.get("listenPort");
        if (portObj == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Object dirObj = param.get("dir");
        if (dirObj == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        Object cidObj = param.get("rmqAddressServerSubGroup");
        if (cidObj == null) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        int cid = NumberUtils.toInt(String.valueOf(cidObj).split("-")[1]);
        if (cid == 0) {
            return Result.getResult(Status.PARAM_ERROR);
        }
        // 保存broker到临时表
        Broker broker = new Broker();
        broker.setBrokerName(brokerName.toString());
        broker.setAddr(String.valueOf(ipObj) + ":" + String.valueOf(portObj));
        broker.setBrokerID(brokerId);
        broker.setCid(cid);
        broker.setBaseDir(String.valueOf(dirObj));
        try {
            return Result.getResult(brokerTmpDao.insert(broker));
        } catch (Exception e) {
            logger.error("saveBrokerTmp:{}", param, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 删除broker到临时表
     */
    public Result<?> deleteBrokerTmp(int cid, String addr) {
        try {
            return Result.getResult(brokerTmpDao.delete(cid, addr));
        } catch (Exception e) {
            logger.error("deleteBrokerTmp:{}", addr, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 查询broker的运行时统计
     */
    public Result<KVTable> fetchBrokerRuntimeStats(String brokerAddr, Cluster mqCluster) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<KVTable>>() {
            public Result<KVTable> callback(MQAdminExt mqAdmin) throws Exception {
                return Result.getResult(mqAdmin.fetchBrokerRuntimeStats(brokerAddr));
            }

            public Cluster mqCluster() {
                return mqCluster;
            }

            public Result<KVTable> exception(Exception e) throws Exception {
                logger.error("fetchBrokerRuntimeStats:{} err", brokerAddr, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 查询producer连接
     */
    public Result<ClientConnectionInfo> fetchAllProducerConnection(String brokerAddr, Cluster mqCluster) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<ClientConnectionInfo>>() {
            public Result<ClientConnectionInfo> callback(MQAdminExt mqAdmin) throws Exception {
                DefaultSohuMQAdmin sohuMQAdmin = (DefaultSohuMQAdmin) mqAdmin;
                ProducerTableInfo producerTableInfo = sohuMQAdmin.getAllProducerInfo(brokerAddr, true);
                return Result.getResult(ClientConnectionInfo.build(producerTableInfo));
            }

            public Cluster mqCluster() {
                return mqCluster;
            }

            public Result<ClientConnectionInfo> exception(Exception e) throws Exception {
                logger.error("fetchAllProducerConnection:{} err", brokerAddr, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 查询consumer连接
     */
    public Result<ClientConnectionInfo> fetchAllConsumerConnection(String brokerAddr, Cluster mqCluster) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<ClientConnectionInfo>>() {
            public Result<ClientConnectionInfo> callback(MQAdminExt mqAdmin) throws Exception {
                DefaultSohuMQAdmin sohuMQAdmin = (DefaultSohuMQAdmin) mqAdmin;
                ConsumerTableInfo consumerTableInfo = sohuMQAdmin.getAllConsumerInfo(brokerAddr, true);
                return Result.getResult(ClientConnectionInfo.build(mqCluster.getId(), consumerTableInfo));
            }

            public Cluster mqCluster() {
                return mqCluster;
            }

            public Result<ClientConnectionInfo> exception(Exception e) throws Exception {
                logger.error("fetchAllConsumerConnection:{} err", brokerAddr, e);
                return Result.getDBErrorResult(e);
            }
        });
    }

    /**
     * 查看broker统计数据，只包括外部流量
     */
    public Result<BrokerStatsData> viewBrokerPutStats(int cid, String brokerAddr) {
        Cluster cluster = clusterService.getMQClusterById(cid);
        return viewBrokerStatsData(cluster, brokerAddr, "BROKER_PUT_NUMS_FROM_EXTERNAL", cluster.getName());
    }

    public Result<BrokerStatsData> viewBrokerStatsData(Cluster cluster, String brokerAddr, String statsName, String statsKey) {
        return viewBrokerStatsData(cluster, brokerAddr, statsName, statsKey, true);
    }

    /**
     * 查看broker统计数据
     */
    public Result<BrokerStatsData> viewBrokerStatsData(Cluster cluster, String brokerAddr, String statsName, String statsKey, boolean logWhenError) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<BrokerStatsData>>() {
            public Result<BrokerStatsData> callback(MQAdminExt mqAdmin) throws Exception {
                return Result.getResult(mqAdmin.viewBrokerStatsData(brokerAddr, statsName, statsKey));
            }

            @Override
            public Result<BrokerStatsData> exception(Exception e) throws Exception {
                if (logWhenError) {
                    logger.error("viewBrokerStatsData broker:{} statsName:{} statsKey:{} err", brokerAddr, statsName, statsKey, e);
                } else {
                    logger.debug("viewBrokerStatsData broker:{} statsName:{} statsKey:{} err", brokerAddr, statsName, statsKey, e);
                }
                return Result.getDBErrorResult(e);
            }

            public Cluster mqCluster() {
                return cluster;
            }
        });
    }

    /**
     * 擦除写权限
     */
    public Result<?> wipeWritePerm(int cid, String brokerName, String brokerAddr) {
        BrokerConfigUpdateParam brokerConfigUpdateParam = new BrokerConfigUpdateParam();
        brokerConfigUpdateParam.setCid(cid);
        brokerConfigUpdateParam.setAddr(brokerAddr);
        brokerConfigUpdateParam.setConfig("brokerPermission=4");
        Result<?> rst = updateBrokerConfig(brokerConfigUpdateParam);
        if (rst.isOK()) {
            updateWritable(cid, brokerAddr, false);
        }
        return rst;
    }

    /**
     * 添加写权限
     */
    public Result<?> addWritePerm(Broker broker) {
        Result<?> rst = null;
        if (broker.isRocketMQV5()) {
            BrokerConfigUpdateParam brokerConfigUpdateParam = new BrokerConfigUpdateParam();
            brokerConfigUpdateParam.setCid(broker.getCid());
            brokerConfigUpdateParam.setAddr(broker.getAddr());
            brokerConfigUpdateParam.setConfig("brokerPermission=6");
            rst = updateBrokerConfig(brokerConfigUpdateParam);
        } else {
            rst = mqAdminTemplate.execute(new MQAdminCallback<Result<Integer>>() {
                public Result<Integer> callback(MQAdminExt mqAdmin) throws Exception {
                    SohuMQAdmin sohuMQAdmin = (SohuMQAdmin) mqAdmin;
                    List<String> namesrvList = mqAdmin.getNameServerAddressList();
                    if (namesrvList == null) {
                        return Result.getResult(Status.NO_RESULT).setMessage("namesrvList is empty");
                    }
                    for (String namesrvAddr : namesrvList) {
                        sohuMQAdmin.unregisterBroker(namesrvAddr, mqCluster().getName(), broker.getAddr(), broker.getBrokerName(), broker.getBrokerID());
                    }
                    return Result.getOKResult();
                }

                @Override
                public Result<Integer> exception(Exception e) throws Exception {
                    logger.error("addWritePerm {} err", broker, e);
                    return Result.getDBErrorResult(e);
                }

                public Cluster mqCluster() {
                    return clusterService.getMQClusterById(broker.getCid());
                }
            });
        }
        if (rst.isOK()) {
            updateWritable(broker.getCid(), broker.getAddr(), true);
        }
        return rst;
    }

    /**
     * 获取broker的连接数
     */
    public Result<ClientConnectionSize> getClientConnectionSize(int cid, String brokerAddr) {
        return mqAdminTemplate.execute(new MQAdminCallback<Result<ClientConnectionSize>>() {
            public Result<ClientConnectionSize> callback(MQAdminExt mqAdmin) throws Exception {
                DefaultSohuMQAdmin sohuMQAdmin = (DefaultSohuMQAdmin) mqAdmin;
                return Result.getResult(sohuMQAdmin.getClientConnectionSize(brokerAddr));
            }

            @Override
            public Result<ClientConnectionSize> exception(Exception e) throws Exception {
                logger.error("getClientConnectionSize {} err", brokerAddr, e);
                return Result.getDBErrorResult(e);
            }

            public Cluster mqCluster() {
                return clusterService.getMQClusterById(cid);
            }
        });
    }

    /**
     * 从nameserver 拉取当前集群的broker地址
     */
    public Result<List<Broker>> getBrokerListFromNameServer(Cluster mqCluster) {
        Result<List<Broker>> brokerListResult = mqAdminTemplate.execute(new MQAdminCallback<Result<List<Broker>>>() {
            public Result<List<Broker>> callback(MQAdminExt mqAdmin) throws Exception {
                // 获取集群信息
                ClusterInfo clusterInfo = mqAdmin.examineBrokerClusterInfo();
                // 获得broker地址map
                Map<String, BrokerData> brokerAddrTable = clusterInfo.getBrokerAddrTable();
                List<Broker> list = new ArrayList<Broker>();
                // 遍历集群中所有的broker
                for (String brokerName : brokerAddrTable.keySet()) {
                    HashMap<Long, String> brokerAddrs = brokerAddrTable.get(brokerName).getBrokerAddrs();
                    for (Long brokerId : brokerAddrs.keySet()) {
                        Broker broker = new Broker();
                        broker.setAddr(brokerAddrs.get(brokerId));
                        broker.setBrokerID(brokerId.intValue());
                        broker.setBrokerName(brokerName);
                        list.add(broker);
                    }
                }
                return Result.getResult(list);
            }

            public Cluster mqCluster() {
                return mqCluster;
            }

            public Result<List<Broker>> exception(Exception e) throws Exception {
                logger.error("cluster:{} err", mqCluster(), e);
                return Result.getWebErrorResult(e);
            }
        });
        return brokerListResult;
    }

    /**
     * 切换主备
     */
    public Result<Boolean> switchToMaster(Broker broker) {
        Cluster cluster = clusterService.getMQClusterById(broker.getCid());
        return switchToMaster(cluster, broker);
    }

    /**
     * 切换主备
     */
    public Result<Boolean> switchToMaster(Cluster cluster, Broker broker) {
        String brokerAddr = broker.getAddr();
        Properties properties = fetchBrokerConfig(cluster, brokerAddr).getResult();
        String controllerAddr = properties.getProperty("controllerAddr").split(";")[0];
        long brokerId = NumberUtils.toLong(properties.getProperty("brokerId"));
        if (brokerId == 0) {
            // master切换为master，需要从controller获取brokerId
            Result<Map<String, ReplicasInfo>> brokerReplicasInfoMapResult = controllerService.getBrokerReplicasInfo(cluster, controllerAddr);
            if (brokerReplicasInfoMapResult.isNotOK()) {
                return (Result) brokerReplicasInfoMapResult;
            }
            ReplicasInfo replicasInfo = brokerReplicasInfoMapResult.getResult().get(broker.getBrokerName());
            if (replicasInfo == null) {
                return Result.getResult(Status.BROKER_UNSUPPORTED_ERROR).setMessage("broker " + broker.getBrokerName() + " is not in replication");
            }
            if (replicasInfo.getMasterAddress().equals(brokerAddr)) {
                brokerId = replicasInfo.getMasterBrokerId();
            }
        }
        long finalBrokerId = brokerId;
        return mqAdminTemplate.execute(new MQAdminCallback<Result<Boolean>>() {
            public Result<Boolean> callback(MQAdminExt mqAdmin) throws Exception {
                ElectMasterResponseHeader response = mqAdmin.electMaster(controllerAddr, cluster.getName(),
                        broker.getBrokerName(), finalBrokerId).getObject1();
                String newMasterAddr = response.getMasterAddress();
                boolean success = brokerAddr.equals(newMasterAddr);
                logger.info("switchToMaster broker:{} success:{}", brokerAddr, success);
                return Result.getResult(success);
            }

            @Override
            public Result<Boolean> exception(Exception e) throws Exception {
                logger.error("switchToMaster {} err", brokerAddr, e);
                return Result.getDBErrorResult(e);
            }

            public Cluster mqCluster() {
                return cluster;
            }
        });
    }

    public Result<List<Broker>> queryBrokerByName(int cid, String brokerName) {
        try {
            return Result.getResult(brokerDao.selectBrokerByName(cid, brokerName));
        } catch (Exception e) {
            logger.error("queryBroker:{} err", brokerName, e);
            return Result.getDBErrorResult(e);
        }
    }

    /**
     * 检查broker是否停写
     */
    public Result<?> checkBrokerStopWritable(int cid, String brokerAddr) {
        // 检查broker是否停写
        Result<BrokerStatsData> result = viewBrokerPutStats(cid, brokerAddr);
        if (result.isNotOK()) {
            // 如果查询失败，且异常是MQClientException，且异常信息包含"not exist"，说明broker上没有put stats数据，说明broker停写了
            if (result.getException() != null && result.getException() instanceof MQClientException) {
                String error = ((MQClientException) result.getException()).getErrorMessage();
                if (error != null && error.contains("not exist")) {
                    return Result.getOKResult().setMessage("put stats:0");
                }
            }
            return result;
        }
        long putStats = result.getResult().getStatsMinute().getSum();
        if (putStats <= 0) {
            return Result.getOKResult().setMessage("put stats:0");
        }
        return Result.getErrorResult("put stats:" + putStats);
    }


    /**
     * 查询同一个brokerName的其他broker
     */
    public Result<Broker> queryOtherBroker(int cid, String brokerName, String brokerAddr) {
        Result<List<Broker>> brokerListResult = queryBrokerByName(cid, brokerName);
        if (brokerListResult.isEmpty()) {
            return (Result) brokerListResult;
        }
        Broker broker = brokerListResult.getResult().stream()
                .filter(b -> !b.getAddr().equals(brokerAddr))
                .findFirst()
                .orElseGet(null);
        return Result.getResult(broker);
    }

    /**
     * 检查切主后的broker，连接数是否追上之前的broker
     */
    public Result<?> checkBrokerConnectionSize(Broker newBroker, Broker preBroker) {
        Result<ClientConnectionSize> newResult = getClientConnectionSize(newBroker.getCid(), newBroker.getAddr());
        if (newResult.isNotOK()) {
            return newResult;
        }
        Result<ClientConnectionSize> preResult = getClientConnectionSize(preBroker.getCid(), preBroker.getAddr());
        if (preResult.isNotOK()) {
            return preResult;
        }
        int newConsumerConnectionSize = newResult.getResult().getConsumerConnectionSize();
        int newProducerConnectionSize = newResult.getResult().getProducerConnectionSize();
        int preConsumerConnectionSize = preResult.getResult().getConsumerConnectionSize();
        int preProducerConnectionSize = preResult.getResult().getProducerConnectionSize();
        if (newConsumerConnectionSize >= preConsumerConnectionSize && newProducerConnectionSize >= preProducerConnectionSize) {
            return Result.getOKResult();
        }
        return Result.getErrorResult("conn c:" + newConsumerConnectionSize + "<" + preConsumerConnectionSize + ", p:" + newProducerConnectionSize + "<" + preProducerConnectionSize);

    }
}
