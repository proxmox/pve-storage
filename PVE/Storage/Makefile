SOURCES=Plugin.pm DirPlugin.pm LVMPlugin.pm NFSPlugin.pm ISCSIPlugin.pm RBDPlugin.pm SheepdogPlugin.pm ISCSIDirectPlugin.pm GlusterfsPlugin.pm ZFSPlugin.pm

.PHONY: install
install:
	for i in ${SOURCES}; do install -D -m 0644 $$i ${DESTDIR}${PERLDIR}/PVE/Storage/$$i; done
