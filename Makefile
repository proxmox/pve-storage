RELEASE=2.1

VERSION=2.0
PACKAGE=libpve-storage-perl
PKGREL=19

DESTDIR=
PREFIX=/usr
BINDIR=${PREFIX}/bin
SBINDIR=${PREFIX}/sbin
MANDIR=${PREFIX}/share/man
DOCDIR=${PREFIX}/share/doc/${PACKAGE}
PODDIR=${DOCDIR}/pod
MAN1DIR=${MANDIR}/man1/
export PERLDIR=${PREFIX}/share/perl5

#ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
ARCH=all
DEB=${PACKAGE}_${VERSION}-${PKGREL}_${ARCH}.deb


all: ${DEB}

.PHONY: dinstall
dinstall: deb
	dpkg -i ${DEB}

%.1.gz: %.1.pod
	rm -f $@
	cat $<|pod2man -n $* -s 1 -r ${VERSION} -c "Proxmox Documentation"|gzip -c9 >$@

pvesm.1.pod: pvesm PVE/Storage.pm
	perl -I. ./pvesm printmanpod >$@

.PHONY: install
install: pvesm.1.pod pvesm.1.gz
	install -d ${DESTDIR}${SBINDIR}
	install -m 0755 pvesm ${DESTDIR}${SBINDIR}
	make -C PVE install
	install -d ${DESTDIR}/usr/share/man/man1
	install -d ${DESTDIR}${PODDIR}
	install -m 0644 pvesm.1.gz ${DESTDIR}/usr/share/man/man1/
	install -m 0644 pvesm.1.pod ${DESTDIR}/${PODDIR}

.PHONY: deb ${DEB}
deb ${DEB}:
	rm -rf debian
	mkdir debian
	make DESTDIR=${CURDIR}/debian install
	perl -I. ./pvesm verifyapi 
	install -d -m 0755 debian/DEBIAN
	sed -e s/@@VERSION@@/${VERSION}/ -e s/@@PKGRELEASE@@/${PKGREL}/ -e s/@@ARCH@@/${ARCH}/ <control.in >debian/DEBIAN/control
	install -D -m 0644 copyright debian/${DOCDIR}/copyright
	install -m 0644 changelog.Debian debian/${DOCDIR}/
	gzip -9 debian/${DOCDIR}/changelog.Debian
	dpkg-deb --build debian	
	mv debian.deb ${DEB}
	rm -rf debian
	lintian ${DEB}

.PHONY: clean
clean: 	
	rm -rf debian *.deb ${PACKAGE}-*.tar.gz dist *.1.pod *.1.gz
	find . -name '*~' -exec rm {} ';'

.PHONY: distclean
distclean: clean


.PHONY: upload
upload: ${DEB}
	umount /pve/${RELEASE}; mount /pve/${RELEASE} -o rw 
	mkdir -p /pve/${RELEASE}/extra
	rm -f /pve/${RELEASE}/extra/${PACKAGE}_*.deb
	rm -f /pve/${RELEASE}/extra/Packages*
	cp ${DEB} /pve/${RELEASE}/extra
	cd /pve/${RELEASE}/extra; dpkg-scanpackages . /dev/null > Packages; gzip -9c Packages > Packages.gz
	umount /pve/${RELEASE}; mount /pve/${RELEASE} -o ro

