SOURCES=Comstar.pm Istgt.pm Iet.pm LIO.pm

.PHONY: install
install:
	for i in ${SOURCES}; do install -D -m 0644 $$i ${DESTDIR}${PERLDIR}/PVE/Storage/LunCmd/$$i; done
