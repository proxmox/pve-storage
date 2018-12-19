

.PHONY: install
install:
	install -D -m 0644 Storage.pm ${DESTDIR}${PERLDIR}/PVE/Storage.pm
	install -D -m 0644 Diskmanage.pm ${DESTDIR}${PERLDIR}/PVE/Diskmanage.pm
	install -D -m 0644 CephConfig.pm ${DESTDIR}${PERLDIR}/PVE/CephConfig.pm
	make -C Storage install
	make -C API2 install
	make -C CLI install
