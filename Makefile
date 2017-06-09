VERSION=5.0
PACKAGE=libpve-storage-perl
PKGREL=5

DESTDIR=
PREFIX=/usr
BINDIR=${PREFIX}/bin
SBINDIR=${PREFIX}/sbin
MANDIR=${PREFIX}/share/man
DOCDIR=${PREFIX}/share/doc/${PACKAGE}
MAN1DIR=${MANDIR}/man1/
BASHCOMPLDIR=${PREFIX}/share/bash-completion/completions/

export PERLDIR=${PREFIX}/share/perl5

export SOURCE_DATE_EPOCH ?= $(shell dpkg-parsechangelog -STimestamp)

ARCH=all
GITVERSION:=$(shell cat .git/refs/heads/master)

DEB=${PACKAGE}_${VERSION}-${PKGREL}_${ARCH}.deb

# this require package pve-doc-generator
export NOVIEW=1
include /usr/share/pve-doc-generator/pve-doc-generator.mk

all:

.PHONY: dinstall
dinstall: deb
	dpkg -i ${DEB}

pvesm.bash-completion:
	perl -I. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->generate_bash_completions();" >$@.tmp
	mv $@.tmp $@

.PHONY: install
install: PVE pvesm.1 pvesm.bash-completion
	install -d ${DESTDIR}${SBINDIR}
	install -m 0755 pvesm ${DESTDIR}${SBINDIR}
	make -C PVE install
	install -d ${DESTDIR}/usr/share/man/man1
	install -m 0644 pvesm.1 ${DESTDIR}/usr/share/man/man1/
	gzip -9 -n ${DESTDIR}/usr/share/man/man1/pvesm.1
	install -m 0644 -D pvesm.bash-completion ${DESTDIR}${BASHCOMPLDIR}/pvesm

.PHONY: deb
deb: ${DEB}
${DEB}:
	rm -rf build
	rsync -a * build
	echo "git clone git://git.proxmox.com/git/pve-storage.git\\ngit checkout ${GITVERSION}" >build/debian/SOURCE
	cd build; dpkg-buildpackage -rfakeroot -b -us -uc
	lintian ${DEB}

.PHONY: test
test:
	perl -I. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->verify_api();"
	make -C test

.PHONY: clean
clean:
	make cleanup-docgen
	rm -rf build *.deb *.buildinfo *.changes
	find . -name '*~' -exec rm {} ';'

.PHONY: distclean
distclean: clean


.PHONY: upload
upload: ${DEB}
	tar cf - ${DEB} | ssh repoman@repo.proxmox.com -- upload --product pve --dist stretch

