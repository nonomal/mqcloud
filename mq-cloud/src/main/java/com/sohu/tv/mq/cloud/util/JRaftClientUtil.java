package com.sohu.tv.mq.cloud.util;

import com.alipay.sofa.jraft.CliService;
import com.alipay.sofa.jraft.JRaftUtils;
import com.alipay.sofa.jraft.RaftServiceFactory;
import com.alipay.sofa.jraft.Status;
import com.alipay.sofa.jraft.entity.PeerId;
import com.alipay.sofa.jraft.option.CliOptions;

import java.util.List;
import java.util.function.Function;

public class JRaftClientUtil {

    public static List<PeerId> getAlivePeers(String groupId, String prevConfig) {
        return execute(cliService -> cliService.getAlivePeers(groupId, JRaftUtils.getConfiguration(prevConfig)));
    }

    public static List<PeerId> getPeers(String groupId, String prevConfig) {
        return execute(cliService -> cliService.getPeers(groupId, JRaftUtils.getConfiguration(prevConfig)));
    }

    public static Status addPeer(String groupId, String prevConfig, String ip, int port) {
        return execute(cliService -> cliService.addPeer(groupId, JRaftUtils.getConfiguration(prevConfig), new PeerId(ip, port)));
    }

    public static Status removePeer(String groupId, String prevConfig, String ip, int port) {
        return execute(cliService -> cliService.removePeer(groupId, JRaftUtils.getConfiguration(prevConfig), new PeerId(ip, port)));
    }

    public static Status transferLeader(String groupId, String config, String ip, int port) {
        return execute(cliService -> cliService.transferLeader(groupId, JRaftUtils.getConfiguration(config), new PeerId(ip, port)));
    }

    public static <T> T execute(Function<CliService, T> func) {
        CliService cliService = null;
        try {
            cliService = RaftServiceFactory.createAndInitCliService(new CliOptions());
            return func.apply(cliService);
        } finally {
            if (cliService != null) {
                cliService.shutdown();
            }
        }
    }
}
