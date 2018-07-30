

.PHONY: install
install:
	install -D -m 0644 Disks.pm ${DESTDIR}${PERLDIR}/PVE/API2/Disks.pm
	make -C Storage install
	make -C Disks install
