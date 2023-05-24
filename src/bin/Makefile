DESTDIR=
PREFIX=/usr
SBINDIR=$(PREFIX)/sbin
MANDIR=$(PREFIX)/share/man
MAN1DIR=$(MANDIR)/man1/
BASHCOMPLDIR=$(PREFIX)/share/bash-completion/completions/
ZSHCOMPLDIR=$(PREFIX)/share/zsh/vendor-completions/

export PERLDIR=$(PREFIX)/share/perl5

PERL_DOC_INC_DIRS=..
-include /usr/share/pve-doc-generator/pve-doc-generator.mk

all:

pvesm.bash-completion:
	perl -I.. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->generate_bash_completions();" >$@.tmp
	mv $@.tmp $@

pvesm.zsh-completion:
	perl -I.. -T -e "use PVE::CLI::pvesm; PVE::CLI::pvesm->generate_zsh_completions();" >$@.tmp
	mv $@.tmp $@

.PHONY: install
install: pvesm.1 pvesm.bash-completion pvesm.zsh-completion
	install -d $(DESTDIR)$(SBINDIR)
	install -m 0755 pvesm $(DESTDIR)$(SBINDIR)
	install -d $(DESTDIR)$(MAN1DIR)
	install -m 0644 pvesm.1 $(DESTDIR)$(MAN1DIR)
	gzip -9 -n $(DESTDIR)$(MAN1DIR)/pvesm.1
	install -m 0644 -D pvesm.bash-completion $(DESTDIR)$(BASHCOMPLDIR)/pvesm
	install -m 0644 -D pvesm.zsh-completion $(DESTDIR)$(ZSHCOMPLDIR)/_pvesm

.PHONY: clean
clean:
	rm -f *.xml.tmp *.1 *.5 *.8 *{synopsis,opts}.adoc docinfo.xml
