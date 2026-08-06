package com.sohu.tv.mq.cloud.util;

import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class JRaftClientUtilTest {

    String groupId = "fat-cluster";

    String prevConfig = "127.0.0.1:10001";

    @Test
    public void testGetPeers() {
        List<PeerId> peers = JRaftClientUtil.getPeers(groupId, prevConfig);
        Assert.assertNotNull(peers);
    }

    @Test
    public void testGetAlivePeers() {
        List<PeerId> peers = JRaftClientUtil.getAlivePeers(groupId, prevConfig);
        Assert.assertNotNull(peers);
    }

    @Test
    public void testRemove() {
        String ip = "127.0.0.1";
        int port = 10001;
        Status status = JRaftClientUtil.removePeer(groupId, prevConfig, ip, port);
        Assert.assertNotNull(status);
    }

    @Test
    public void testAdd() {
        String ip = "127.0.0.1";
        int port = 10001;
        Status status = JRaftClientUtil.addPeer(groupId, prevConfig, ip, port);
        Assert.assertNotNull(status);
    }

    @Test
    public void testTransferLeader() {
        String ip = "127.0.0.1";
        int port = 10001;
        Status status = JRaftClientUtil.transferLeader(groupId, prevConfig, ip, port);
        Assert.assertNotNull(status);
    }
}