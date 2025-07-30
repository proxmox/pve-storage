all: test

test: test_zfspoolplugin test_lvmplugin test_disklist test_bwlimit test_plugin test_ovf test_volume_access

test_zfspoolplugin: run_test_zfspoolplugin.pl
	./run_test_zfspoolplugin.pl

test_lvmplugin: run_test_lvmplugin.pl
	./run_test_lvmplugin.pl

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
