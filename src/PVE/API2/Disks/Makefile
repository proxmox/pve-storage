
SOURCES= LVM.pm\
	 LVMThin.pm\
	 ZFS.pm\
	 Directory.pm

.PHONY: install
install:
	for i in ${SOURCES}; do install -D -m 0644 $$i ${DESTDIR}${PERLDIR}/PVE/API2/Disks/$$i; done
