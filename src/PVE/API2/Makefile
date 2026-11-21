

.PHONY: install
install:
	install -D -m 0644 Disks.pm ${DESTDIR}${PERLDIR}/PVE/API2/Disks.pm
	install -D -m 0644 ISCSI.pm ${DESTDIR}${PERLDIR}/PVE/API2/ISCSI.pm
	install -D -m 0644 Multipath.pm ${DESTDIR}${PERLDIR}/PVE/API2/Multipath.pm
	make -C Storage install
	make -C Disks install
	make -C Plugins install
	make -C SAN install
