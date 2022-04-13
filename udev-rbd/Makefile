PACKAGE=libpve-storage-perl

DESTDIR=
PREFIX=/usr
LIBEXECDIR=${PREFIX}/libexec
LIBDIR=${PREFIX}/lib

all:

.PHONY: install
install: 50-rbd-pve.rules ceph-rbdnamer-pve
	install -d ${DESTDIR}${LIBEXECDIR}
	install -m 0755 ceph-rbdnamer-pve ${DESTDIR}${LIBEXECDIR}
	install -d ${DESTDIR}${LIBDIR}/udev/rules.d
	install -m 0644 50-rbd-pve.rules ${DESTDIR}${LIBDIR}/udev/rules.d

.PHONY: clean
clean:

.PHONY: distclean
distclean: clean
