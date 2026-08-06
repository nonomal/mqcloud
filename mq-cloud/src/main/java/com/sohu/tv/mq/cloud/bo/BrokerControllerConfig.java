package com.sohu.tv.mq.cloud.bo;

/**
 * broker controller配置
 */
public class BrokerControllerConfig {
    private boolean controllerEnabled;
    private int brokerId;
    private boolean writable = true;
    private String controllerAddr;

    public boolean isControllerEnabled() {
        return controllerEnabled;
    }

    public void setControllerEnabled(boolean controllerEnabled) {
        this.controllerEnabled = controllerEnabled;
    }

    public int getBrokerId() {
        return brokerId;
    }

    public void setBrokerId(int brokerId) {
        this.brokerId = brokerId;
    }

    public boolean isWritable() {
        return writable;
    }

    public void setWritable(boolean writable) {
        this.writable = writable;
    }

    public String getControllerAddr() {
        return controllerAddr;
    }

    public void setControllerAddr(String controllerAddr) {
        this.controllerAddr = controllerAddr;
    }
}
