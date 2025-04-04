

.PHONY: install
install:
	install -D -m 0644 Storage.pm ${DESTDIR}${PERLDIR}/PVE/Storage.pm
	install -D -m 0644 Diskmanage.pm ${DESTDIR}${PERLDIR}/PVE/Diskmanage.pm
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
