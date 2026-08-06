package com.sohu.tv.mq.cloud.bo;

import lombok.Data;

@Data
public class ControllerNode {
    private String ip;
    private int port;
    private boolean alive;
}
