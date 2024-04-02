DESTDIR=
PREFIX=/usr

export PERLDIR=$(PREFIX)/share/perl5

all:

.PHONY: install
install: PVE bin udev-rbd
	$(MAKE) -C bin install
	$(MAKE) -C PVE install
	$(MAKE) -C udev-rbd install

.PHONY: test
test:
	perl -I. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->verify_api();"
	$(MAKE) -C test
	$(MAKE) -C PVE test

.PHONY: clean
clean:
	$(MAKE) -C bin clean
	$(MAKE) -C PVE clean
	$(MAKE) -C udev-rbd clean
