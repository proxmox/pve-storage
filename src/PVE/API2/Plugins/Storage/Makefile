SOURCES = Plugin.pm		\


SUBDIRS =


INSTALL_PATH = ${DESTDIR}${PERLDIR}/PVE/API2/Plugins/Storage

.PHONY: install
install:
	set -e && for SOURCE in ${SOURCES}; \
		do install -D -m 0644 $$SOURCE ${INSTALL_PATH}/$$SOURCE; \
	done
	set -e && for SUBDIR in ${SUBDIRS}; \
		do make -C $$SUBDIR install; \
	done
