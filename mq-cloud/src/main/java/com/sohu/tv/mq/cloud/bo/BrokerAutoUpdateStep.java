package com.sohu.tv.mq.cloud.bo;

import com.sohu.tv.mq.cloud.common.util.WebUtil;
import com.sohu.tv.mq.cloud.util.DateUtil;
import com.sohu.tv.mq.cloud.util.RocketMQVersion;
import org.apache.commons.lang3.math.NumberUtils;

import java.util.Date;

/**
 * broker自动更新步骤
 *
 * @author yongfeigao
 * @date 2024年10月31日
 */
public class BrokerAutoUpdateStep {
    // id
    private int id;
    // 父id
    private int brokerAutoUpdateId;
    // broker地址
    private String brokerAddr;
    // broker名字
    private String brokerName;
    // broker ID，0-master，1-slave
    private int brokerId;
    // broker baseDir
    private String brokerBaseDir;
    // broker版本
    private String brokerVersion;
    // 操作顺序
    private int order;
    // 0:停写,1:取消注册,2:关闭,3:备份数据,4:下载安装包,5:解压安装包,6:恢复数据,7:启动,8:注册,9:恢复写入
    private int action;
    // 状态: 1:未开始,3:进行中,5:成功,6:失败,7:手动结束
    private int status;
    // 操作信息
    private String info;
    // 开始时间
    private Date startTime;
    // 结束时间
    private Date endTime;

    private int controllerEnabled;

    // 集群id,冗余字段
    private int cid;

    // 状态检查为成功的次数
    private int statusCheckOkCount;
    // 状态检查最大次数，超过则认为失败
    private int statusCheckMaxCount;
    // 状态检查间隔，单位秒
    private int statusCheckInterval;

    public static BrokerAutoUpdateStep build(int order, Broker broker, Action action) {
        BrokerAutoUpdateStep brokerAutoUpdateStep = new BrokerAutoUpdateStep();
        brokerAutoUpdateStep.setOrder(order);
        brokerAutoUpdateStep.setBrokerAddr(broker.getAddr());
        brokerAutoUpdateStep.setAction(action.getValue());
        brokerAutoUpdateStep.setBrokerName(broker.getBrokerName());
        brokerAutoUpdateStep.setBrokerId(broker.getBrokerID());
        brokerAutoUpdateStep.setBrokerBaseDir(broker.getBaseDir());
        brokerAutoUpdateStep.setBrokerVersion(broker.getVersion());
        brokerAutoUpdateStep.setStatus(Status.INIT.getValue());
        brokerAutoUpdateStep.setControllerEnabled(broker.isControllerEnabled() ? 1 : 0);
        brokerAutoUpdateStep.setStatusCheckOkCount(action.getStatusOKCount());
        brokerAutoUpdateStep.setStatusCheckMaxCount(action.getStatusCheckMaxCount());
        brokerAutoUpdateStep.setStatusCheckInterval(action.getStatusCheckInterval());
        return brokerAutoUpdateStep;
    }

    public Broker toBroker() {
        Broker broker = new Broker();
        broker.setAddr(brokerAddr);
        broker.setBrokerName(brokerName);
        broker.setBrokerID(brokerId);
        broker.setBaseDir(brokerBaseDir);
        broker.setVersion(brokerVersion);
        broker.setCid(cid);
        return broker;
    }

    public static enum Status {
        // 任务刚创建的初始状态
        INIT(1, "未开始", "text-info"),
        // 任务正在执行，自动从READY状态转换为RUNNING状态
        RUNNING(2, "进行中", "text-yellow"),
        // 任务成功
        SUCCESS(3, "成功", "text-success"),
        // 任务失败
        FAILED(4, "失败", "text-danger"),
        FINISHED(5, "手动结束", "text-primary"),
        ;
        // 可执行状态
        public static final int[] EXECUTE_STATUS = {INIT.getValue(), RUNNING.getValue(), FAILED.getValue()};

        private int value;
        private String desc;
        private String color;

        Status(int value, String desc, String color) {
            this.value = value;
            this.desc = desc;
            this.color = color;
        }

        public int getValue() {
            return value;
        }

        public String getDesc() {
            return desc;
        }

        public String getColor() {
            return color;
        }

        public static Status valueOf(int value) {
            for (Status status : Status.values()) {
                if (status.getValue() == value) {
                    return status;
                }
            }
            return null;
        }
    }

    public static enum Action {
        STOP_WRITE(0, "停写", 2),
        UNREGISTER(1, "取消注册", 2),
        SHUTDOWN(2, "关闭"),
        BACKUP_DATA(3, "备份数据"),
        DOWNLOAD(4, "下载安装包"),
        UNZIP(5, "解压安装包"),
        RECOVER_DATA(6, "恢复数据"),
        START(7, "启动", 2),
        REGISTER(8, "注册", 2),
        RECOVER_WRITE(9, "恢复写入", 2),
        SLAVE_RECOVER_WRITE(10, "slave恢复写入"),
        DISABLE_ELECT_MASTER(11, "暂停Controller选主"),
        ENABLE_ELECT_MASTER(12, "恢复Controller选主"),
        SWITCH_TO_MASTER(13, "切为Master"),
        SLAVE_STOP_WRITE(14, "slave停写"),
        TIMER_STOP_DEQUEUE(15, "暂停定时消息"),
        TIMER_RECOVER_DEQUEUE(16, "恢复定时消息"),
        ;
        private int value;
        private String desc;

        // 状态为成功的次数
        private int statusOKCount = 1;
        // 状态检查最大次数，超过则认为失败
        private int statusCheckMaxCount = 15;
        // 状态检查间隔，单位秒
        private int statusCheckInterval = 35;

        Action(int value, String desc) {
            this.value = value;
            this.desc = desc;
        }

        Action(int value, String desc, int statusOKCount) {
            this.value = value;
            this.desc = desc;
            this.statusOKCount = statusOKCount;
        }

        public int getValue() {
            return value;
        }

        public String getDesc() {
            return desc;
        }

        public int getStatusOKCount() {
            return statusOKCount;
        }

        public int getStatusCheckMaxCount() {
            return statusCheckMaxCount;
        }

        public int getStatusCheckInterval() {
            return statusCheckInterval;
        }

        public static Action valueOf(int value) {
            for (Action action : Action.values()) {
                if (action.getValue() == value) {
                    return action;
                }
            }
            return null;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "value=" + value +
                    ", desc='" + desc + '\'' +
                    "} " + super.toString();
        }
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public int getBrokerAutoUpdateId() {
        return brokerAutoUpdateId;
    }

    public void setBrokerAutoUpdateId(int brokerAutoUpdateId) {
        this.brokerAutoUpdateId = brokerAutoUpdateId;
    }

    public String getBrokerAddr() {
        return brokerAddr;
    }

    public String getIp() {
        return brokerAddr.split(":")[0];
    }

    public int getPort() {
        return NumberUtils.toInt(brokerAddr.split(":")[1]);
    }

    public void setBrokerAddr(String brokerAddr) {
        this.brokerAddr = brokerAddr;
    }

    public String getBrokerName() {
        return brokerName;
    }

    public void setBrokerName(String brokerName) {
        this.brokerName = brokerName;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isMaster() {
        return brokerId == 0;
    }

    public int getOrder() {
        return order;
    }

    public void setOrder(int order) {
        this.order = order;
    }

    public int getAction() {
        return action;
    }

    public Action getActionEnum() {
        return Action.valueOf(action);
    }

    public String getActionDesc() {
        return Action.valueOf(action).getDesc();
    }

    public void setAction(int action) {
        this.action = action;
    }

    public int getStatus() {
        return status;
    }

    public boolean isRunning() {
        return status == Status.RUNNING.getValue();
    }

    public boolean isFailed() {
        return status == Status.FAILED.getValue();
    }

    public boolean isSuccess() {
        return status == Status.SUCCESS.getValue();
    }

    public boolean isFinished() {
        return status == Status.FINISHED.getValue();
    }

    public boolean isOver() {
        return status == Status.SUCCESS.getValue() || status == Status.FINISHED.getValue();
    }

    public Status getStatusEnum() {
        return Status.valueOf(status);
    }

    public String getStatusColor() {
        return getStatusEnum().getColor();
    }

    public String getStatusDesc() {
        return getStatusEnum().getDesc();
    }

    public void setStatus(int status) {
        this.status = status;
    }

    public String getInfo() {
        return info;
    }

    public void setInfo(String info) {
        this.info = info;
    }

    public Date getStartTime() {
        return startTime;
    }

    public int getCid() {
        return cid;
    }

    public void setCid(int cid) {
        this.cid = cid;
    }

    public String getBrokerBaseDir() {
        return brokerBaseDir;
    }

    public void setBrokerBaseDir(String brokerBaseDir) {
        this.brokerBaseDir = brokerBaseDir;
    }

    public String getBrokerVersion() {
        return brokerVersion;
    }

    public RocketMQVersion getRocketMQVersion() {
        return RocketMQVersion.getRocketMQVersion(brokerVersion);
    }

    public void setBrokerVersion(String brokerVersion) {
        this.brokerVersion = brokerVersion;
    }

    public String getStartTimeDesc() {
        if (startTime == null) {
            return null;
        }
        return DateUtil.getFormat(DateUtil.YMD_DASH_BLANK_HMS_COLON).format(startTime);
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public String getEndTimeDesc() {
        if (endTime == null) {
            return null;
        }
        return DateUtil.getFormat(DateUtil.YMD_DASH_BLANK_HMS_COLON).format(endTime);
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public String getCostTimeDesc() {
        if (startTime == null || endTime == null) {
            return null;
        }
        return WebUtil.timeFormat(endTime.getTime() - startTime.getTime());
    }

    public int getControllerEnabled() {
        return controllerEnabled;
    }

    public void setControllerEnabled(int controllerEnabled) {
        this.controllerEnabled = controllerEnabled;
    }

    public boolean isEnableController() {
        return controllerEnabled == 1;
    }

    public String toSimpleString() {
        return id + ":" + order + ":" + brokerName + ":" + brokerId + ":" + brokerAddr;
    }

    public int getStatusCheckOkCount() {
        return statusCheckOkCount;
    }

    public void setStatusCheckOkCount(int statusCheckOkCount) {
        this.statusCheckOkCount = statusCheckOkCount;
    }

    public int getStatusCheckInterval() {
        return statusCheckInterval;
    }

    public void setStatusCheckInterval(int statusCheckInterval) {
        this.statusCheckInterval = statusCheckInterval;
    }

    public int getStatusCheckMaxCount() {
        return statusCheckMaxCount;
    }

    public void setStatusCheckMaxCount(int statusCheckMaxCount) {
        this.statusCheckMaxCount = statusCheckMaxCount;
    }

    @Override
    public String toString() {
        return "BrokerAutoUpdateStep{" +
                "id=" + id +
                ", brokerAutoUpdateId=" + brokerAutoUpdateId +
                ", brokerAddr='" + brokerAddr + '\'' +
                ", brokerName='" + brokerName + '\'' +
                ", brokerId=" + brokerId +
                ", brokerBaseDir='" + brokerBaseDir + '\'' +
                ", brokerVersion='" + brokerVersion + '\'' +
                ", order=" + order +
                ", action=" + action +
                ", status=" + status +
                ", info='" + info + '\'' +
                ", startTime=" + startTime +
                ", endTime=" + endTime +
                ", controllerEnabled=" + controllerEnabled +
                ", cid=" + cid +
                ", statusCheckOKCount=" + statusCheckOkCount +
                ", statusCheckInterval=" + statusCheckInterval +
                ", statusCheckMaxCount=" + statusCheckMaxCount +
                '}';
    }
}
