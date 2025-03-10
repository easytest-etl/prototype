PLUGIN=easytest-plugin
VERSION=0.1.0

all: build install
	@echo "Done"

build:
	mvn clean package -DskipTests -U

install:
	${CDAP_HOME}/bin/cdap cli delete artifact ${PLUGIN} ${VERSION}
	${CDAP_HOME}/bin/cdap cli load artifact ./target/$(PLUGIN)-$(VERSION).jar config-file ./target/$(PLUGIN)-$(VERSION).json

remove:
	${CDAP_HOME}/bin/cdap cli delete artifact ${PLUGIN} ${VERSION}

stop:
	${CDAP_HOME}/bin/cdap sandbox stop

start:
	${CDAP_HOME}/bin/cdap sandbox start --enable-debug

start2:
	${CDAP_HOME}/bin/cdap sandbox start
