package com.sohu.tv.mq.cloud.bo;

import com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action;

import java.util.*;

import static com.sohu.tv.mq.cloud.bo.BrokerAutoUpdateStep.Action.*;

/**
 * broker自动更新步骤构建器
 *
 * @author yongfeigao
 * @date 2025年03月06日
 */
public class BrokerAutoUpdateStepBuilder {
    // 更新操作
    public static final int UPDATE_ACTION = 0;
    // 操作
    private int action;
    // 操作步骤
    private List<BrokerAutoUpdateStep> steps;

    private BrokerAutoUpdateStepBuilder(int action) {
        this.action = action;
        steps = new ArrayList<>();
    }

    /**
     * 构建broker自动更新步骤
     */
    public static List<BrokerAutoUpdateStep> build(List<Broker> brokerList, int action) {
        BrokerAutoUpdateStepBuilder builder = new BrokerAutoUpdateStepBuilder(action);
        Map<String, List<Broker>> brokerMap = builder.groupBroker(brokerList);
        for (List<Broker> brokerGroup : brokerMap.values()) {
            // 如果只有一个broker, 则可以直接更新
            if (brokerGroup.size() == 1) {
                builder.addSingleBrokerUpdateStep(brokerGroup.get(0));
            } else {
                // 如果全部是slave，则直接更新
                if (!brokerGroup.get(0).isMaster()) {
                    for (Broker broker : brokerGroup) {
                        builder.addCommonBrokerUpdateStep(broker);
                    }
                } else {
                    builder.addBrokerUpdateStepWithMaster(brokerGroup);
                }
            }
        }
        return builder.steps;
    }

    /**
     * 分组broker
     */
    private Map<String, List<Broker>> groupBroker(List<Broker> brokerList) {
        Map<String, List<Broker>> brokerMap = new TreeMap<>();
        for (Broker broker : brokerList) {
            brokerMap.computeIfAbsent(broker.getBrokerName(), k -> new ArrayList<>()).add(broker);
        }
        // 每组broker按照brokerID排序
        brokerMap.values().forEach(brokers -> brokers.sort(Comparator.comparing(Broker::getBrokerID)));
        return brokerMap;
    }

    /**
     * 添加单个broker的更新步骤
     */
    private void addSingleBrokerUpdateStep(Broker broker) {
        if (broker.isMaster()) {
            addBrokerAutoUpdateStep(broker, STOP_WRITE);
        }
        addCommonBrokerUpdateStep(broker);
        if (broker.isMaster()) {
            addBrokerAutoUpdateStep(broker, RECOVER_WRITE);
        }
    }

    /**
     * 添加通用的broker的更新步骤
     */
    private void addCommonBrokerUpdateStep(Broker broker) {
        addBrokerAutoUpdateStep(broker, SHUTDOWN);
        // action=0表示更新
        if (action == UPDATE_ACTION) {
            addBrokerAutoUpdateStep(broker, BACKUP_DATA);
            addBrokerAutoUpdateStep(broker, DOWNLOAD);
            addBrokerAutoUpdateStep(broker, UNZIP);
            addBrokerAutoUpdateStep(broker, RECOVER_DATA);
        }
        addBrokerAutoUpdateStep(broker, START);
    }

    /**
     * 添加有master的broker的更新步骤
     */
    private void addBrokerUpdateStepWithMaster(List<Broker> brokers) {
        Broker master = brokers.get(0);
        if (!master.isControllerEnabled()) {
            addCommonBrokerUpdateStepWithMaster(brokers);
        } else {
            addBrokerUpdateStepWithController(brokers);
        }
    }

    /**
     * 添加有master的broker的更新步骤
     */
    private void addCommonBrokerUpdateStepWithMaster(List<Broker> brokers) {
        Broker master = brokers.get(0);
        // master停写
        addBrokerAutoUpdateStep(master, STOP_WRITE);
        // 更新所有slave
        addSlaveBrokerUpdateStep(brokers);
        // master取消注册
        addBrokerAutoUpdateStep(master, UNREGISTER);
        // master更新
        addCommonBrokerUpdateStep(master);
        // master注册
        addBrokerAutoUpdateStep(master, REGISTER);
        // master恢复写入
        addBrokerAutoUpdateStep(master, RECOVER_WRITE);
    }

    /**
     * 添加有master的broker的更新步骤-启用了controller选主的场景
     */
    private void addBrokerUpdateStepWithController(List<Broker> brokers) {
        Broker master = brokers.get(0);
        // master停写
        addBrokerAutoUpdateStep(master, STOP_WRITE);
        // slave停写
        Broker slave = brokers.get(1);
        addBrokerAutoUpdateStep(slave, SLAVE_STOP_WRITE);
        // 更新所有slave
        addSlaveBrokerUpdateStep(brokers);
        // master停止定时消息写入
        addBrokerAutoUpdateStep(master, TIMER_STOP_DEQUEUE);
        // slave停止定时消息写入
        addBrokerAutoUpdateStep(slave, TIMER_STOP_DEQUEUE);
        // slave切为master
        addBrokerAutoUpdateStep(slave, SWITCH_TO_MASTER);
        // master更新
        addCommonBrokerUpdateStep(master);
        if (master.isDeployedOnPhysicalMachine()) {
            // 切回master
            addBrokerAutoUpdateStep(master, SWITCH_TO_MASTER);
        } else {
            // 不用切回master，slave此时是master，master此时是slave
            Broker slaveTmp = slave;
            slave = master;
            master = slaveTmp;
        }
        // master恢复定时消息写入
        addBrokerAutoUpdateStep(master, TIMER_RECOVER_DEQUEUE);
        // slave恢复定时消息写入
        addBrokerAutoUpdateStep(slave, TIMER_RECOVER_DEQUEUE);
        // master恢复写入
        addBrokerAutoUpdateStep(master, RECOVER_WRITE);
        // slave恢复写入
        addBrokerAutoUpdateStep(slave, SLAVE_RECOVER_WRITE);
    }

    /**
     * 添加有master的broker的更新步骤-禁止了controller选主的场景
     */
    private void addBrokerUpdateStepWithForbiddenController(List<Broker> brokers) {
        Broker master = brokers.get(0);
        // master停写
        addBrokerAutoUpdateStep(master, STOP_WRITE);
        // master取消注册
        addBrokerAutoUpdateStep(master, UNREGISTER);
        // 暂停Controller选主
        addBrokerAutoUpdateStep(master, DISABLE_ELECT_MASTER);
        // master更新
        addCommonBrokerUpdateStep(master);
        // 恢复Controller选主
        addBrokerAutoUpdateStep(master, ENABLE_ELECT_MASTER);
        // master注册
        addBrokerAutoUpdateStep(master, REGISTER);
        // 更新所有slave
        addSlaveBrokerUpdateStep(brokers);
        // master恢复写入
        addBrokerAutoUpdateStep(master, RECOVER_WRITE);
    }

    private void addSlaveBrokerUpdateStep(List<Broker> brokers) {
        for (int i = 1; i < brokers.size(); ++i) {
            addCommonBrokerUpdateStep(brokers.get(i));
        }
    }

    private BrokerAutoUpdateStep addBrokerAutoUpdateStep(Broker broker, Action action) {
        BrokerAutoUpdateStep step = BrokerAutoUpdateStep.build(steps.size(), broker, action);
        steps.add(step);
        return step;
    }
}
