RELEASE=2.0

VERSION=2.0
PACKAGE=libpve-storage-perl
PKGREL=4

DESTDIR=
PREFIX=/usr
BINDIR=${PREFIX}/bin
SBINDIR=${PREFIX}/sbin
MANDIR=${PREFIX}/share/man
DOCDIR=${PREFIX}/share/doc
MAN1DIR=${MANDIR}/man1/
export PERLDIR=${PREFIX}/share/perl5

#ARCH:=$(shell dpkg-architecture -qDEB_BUILD_ARCH)
ARCH=all
DEB=${PACKAGE}_${VERSION}-${PKGREL}_${ARCH}.deb


all: ${DEB}

.PHONY: dinstall
dinstall: deb
	dpkg -i ${DEB}

.PHONY: install
install:
	install -d ${DESTDIR}${SBINDIR}
	install -m 0755 pvesm ${DESTDIR}${SBINDIR}
	make -C PVE install
	install -d ${DESTDIR}/usr/share/man/man1
	pod2man -n pvesm -s 1 -r "proxmox 1.0" -c "Proxmox Documentation" <pvesm | gzip -9 > ${DESTDIR}/usr/share/man/man1/pvesm.1.gz

.PHONY: deb ${DEB}
deb ${DEB}:
	rm -rf debian
	mkdir debian
	make DESTDIR=${CURDIR}/debian install
	perl -I. ./pvesm verifyapi 
	install -d -m 0755 debian/DEBIAN
	sed -e s/@@VERSION@@/${VERSION}/ -e s/@@PKGRELEASE@@/${PKGREL}/ -e s/@@ARCH@@/${ARCH}/ <control.in >debian/DEBIAN/control
	install -D -m 0644 copyright debian/${DOCDIR}/${PACKAGE}/copyright
	install -m 0644 changelog.Debian debian/${DOCDIR}/${PACKAGE}/
	gzip -9 debian/${DOCDIR}/${PACKAGE}/changelog.Debian
	dpkg-deb --build debian	
	mv debian.deb ${DEB}
	rm -rf debian
	lintian ${DEB}

.PHONY: clean
clean: 	
	rm -rf debian *.deb ${PACKAGE}-*.tar.gz dist
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

