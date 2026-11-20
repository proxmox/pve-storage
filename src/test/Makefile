all: test

test: \
    test_bwlimit \
    test_disk_api \
    test_disklist \
    test_import_stream \
    test_iscsi \
    test_lun_identity \
    test_lvm_base_activation \
    test_lvm_base_volume \
    test_lvm_cleanup_name \
    test_lvm_create \
    test_lvm_hints \
    test_lvm_lock_retry \
    test_lvm_status \
    test_lvmplugin \
    test_multipath \
    test_multipath_api \
    test_ovf \
    test_plugin \
    test_remote_command \
    test_san_api \
    test_san_libraries \
    test_san_lun_scan \
    test_san_taint \
    test_sensitive_properties \
    test_volume_access \
    test_zfspoolplugin \

test_zfspoolplugin: run_test_zfspoolplugin.pl
	./run_test_zfspoolplugin.pl

test_lvmplugin: run_test_lvmplugin.pl run_test_lvm_throughput.pl
	./run_test_lvmplugin.pl
	./run_test_lvm_throughput.pl

test_lvm_create: run_test_lvm_create.pl run_test_lvm_base_conflicts.pl run_san_write_check_tests.pl
	./run_test_lvm_create.pl
	./run_test_lvm_base_conflicts.pl
	./run_san_write_check_tests.pl

test_lvm_lock_retry: run_test_lvm_lock_retry.pl
	./run_test_lvm_lock_retry.pl

test_lvm_cleanup_name: run_test_lvm_cleanup_name.pl
	./run_test_lvm_cleanup_name.pl

test_lvm_base_volume: run_test_lvm_base_volume.pl
	./run_test_lvm_base_volume.pl

test_lvm_hints: run_test_lvm_hints.pl
	./run_test_lvm_hints.pl

test_lvm_status: run_test_lvm_status.pl
	./run_test_lvm_status.pl

test_disklist: run_disk_tests.pl
	./run_disk_tests.pl

test_bwlimit: run_bwlimit_tests.pl
	./run_bwlimit_tests.pl

test_plugin: run_plugin_tests.pl
	./run_plugin_tests.pl

test_ovf: run_ovf_tests.pl
	./run_ovf_tests.pl

test_volume_access: run_volume_access_tests.pl
	./run_volume_access_tests.pl

test_remote_command: run_remote_command_tests.pl
	./run_remote_command_tests.pl

test_iscsi: run_iscsi_activation_tests.pl run_iscsi_discovery_tests.pl run_iscsi_rescan_tests.pl run_iscsi_device_list_tests.pl run_iscsi_session_tests.pl run_iscsi_volume_tests.pl
	./run_iscsi_activation_tests.pl
	./run_iscsi_discovery_tests.pl
	./run_iscsi_rescan_tests.pl
	./run_iscsi_device_list_tests.pl
	./run_iscsi_session_tests.pl
	./run_iscsi_volume_tests.pl

test_sensitive_properties: run_sensitive_properties_tests.pl
	./run_sensitive_properties_tests.pl

test_import_stream: run_import_stream_tests.pl
	./run_import_stream_tests.pl

test_multipath: run_multipath_tests.pl
	./run_multipath_tests.pl

test_san_libraries: run_san_library_tests.pl
	./run_san_library_tests.pl

test_lun_identity: run_lun_identity_tests.pl
	./run_lun_identity_tests.pl

test_lvm_base_activation: run_test_lvm_base_activation.pl
	./run_test_lvm_base_activation.pl

test_multipath_api: run_multipath_api_tests.pl
	./run_multipath_api_tests.pl

test_san_taint: run_san_taint_tests.pl
	perl -T ./run_san_taint_tests.pl

test_san_lun_scan: run_san_lun_scan_tests.pl
	./run_san_lun_scan_tests.pl

test_san_api: run_san_api_tests.pl run_san_wipe_tests.pl run_san_nvme_tests.pl
	./run_san_api_tests.pl
	./run_san_wipe_tests.pl
	./run_san_nvme_tests.pl

test_disk_api: run_disk_api_tests.pl
	./run_disk_api_tests.pl
