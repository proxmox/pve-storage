
SOURCES= Content.pm Status.pm Config.pm PruneBackups.pm Scan.pm FileRestore.pm

.PHONY: install
install:
	for i in ${SOURCES}; do install -D -m 0644 $$i ${DESTDIR}${PERLDIR}/PVE/API2/Storage/$$i; done
