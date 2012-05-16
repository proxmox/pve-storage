

.PHONY: install
install:
	install -D -m 0644 Storage.pm ${DESTDIR}${PERLDIR}/PVE/Storage.pm
	make -C Storage install
	make -C API2 install