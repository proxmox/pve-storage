all: test

test: test_zfspoolplugin test_disklist test_bwlimit

test_zfspoolplugin: run_test_zfspoolplugin.pl
	./run_test_zfspoolplugin.pl

test_disklist: run_disk_tests.pl
	./run_disk_tests.pl

test_bwlimit: run_bwlimit_tests.pl
	./run_bwlimit_tests.pl
