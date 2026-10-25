

.PHONY: install
install:
	install -D -m 0644 Storage.pm ${DESTDIR}${PERLDIR}/PVE/Storage.pm
	install -D -m 0644 Diskmanage.pm ${DESTDIR}${PERLDIR}/PVE/Diskmanage.pm
	install -D -m 0644 ISCSI/Topology.pm ${DESTDIR}${PERLDIR}/PVE/ISCSI/Topology.pm
	install -D -m 0644 Multipath.pm ${DESTDIR}${PERLDIR}/PVE/Multipath.pm
	install -D -m 0644 Multipath/Config.pm ${DESTDIR}${PERLDIR}/PVE/Multipath/Config.pm
	install -D -m 0644 Multipath/Generator.pm ${DESTDIR}${PERLDIR}/PVE/Multipath/Generator.pm
	install -D -m 0644 Multipath/Identity.pm ${DESTDIR}${PERLDIR}/PVE/Multipath/Identity.pm
	install -D -m 0644 CephConfig.pm ${DESTDIR}${PERLDIR}/PVE/CephConfig.pm
	install -D -m 0644 GuestImport.pm ${DESTDIR}${PERLDIR}/PVE/GuestImport.pm
	make -C Storage install
	make -C GuestImport install
	make -C API2 install
	make -C BackupProvider install
	make -C CLI install

.PHONY: test
test:
	$(MAKE) -C test test

clean:
