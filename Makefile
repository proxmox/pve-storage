include /usr/share/dpkg/pkg-info.mk

PACKAGE=libpve-storage-perl

GITVERSION:=$(shell git rev-parse HEAD)

DEB=$(PACKAGE)_$(DEB_VERSION_UPSTREAM_REVISION)_all.deb

all:

.PHONY: dinstall
dinstall: deb
	dpkg -i $(DEB)

.PHONY: deb
deb: $(DEB)
$(DEB):
	rm -rf build
	cp -a src build
	cp -a debian build/
	echo "git clone git://git.proxmox.com/git/pve-storage.git\\ngit checkout $(GITVERSION)" >build/debian/SOURCE
	cd build; dpkg-buildpackage -b -us -uc
	lintian $(DEB)

.PHONY: clean distclean
distclean: clean
clean:
	rm -rf build *.deb *.buildinfo *.changes

.PHONY: upload
upload: $(DEB)
	tar cf - $(DEB) | ssh -X repoman@repo.proxmox.com -- upload --product pve --dist bullseye
