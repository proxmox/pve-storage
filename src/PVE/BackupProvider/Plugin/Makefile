SOURCES = Base.pm

.PHONY: install
install:
	for i in ${SOURCES}; do install -D -m 0644 $$i ${DESTDIR}${PERLDIR}/PVE/BackupProvider/Plugin/$$i; done
