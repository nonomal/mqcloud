alter table `broker_auto_update_step` add column `controller_enabled` int(4) NOT NULL DEFAULT '0' COMMENT '0:没有启用controller,1:启用了controller';
alter table `broker_auto_update_step` add column `status_check_ok_count` int(11) NOT NULL DEFAULT '-1' COMMENT '状态检查为成功的次数';
alter table `broker_auto_update_step` add column `status_check_max_count` int(11) NOT NULL DEFAULT '-1' COMMENT '状态检查最大次数，超过则认为失败';
alter table `broker_auto_update_step` add column `status_check_interval` int(11) NOT NULL DEFAULT '-1' COMMENT '状态检查间隔，单位秒';