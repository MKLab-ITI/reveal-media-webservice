#!/bin/sh

java    -server -Xss256K -Xms6G -Xmx6G -XX:+UseNUMA -XX:+UseConcMarkSweepGC  \
        -XX:+UseTLAB -XX:+ResizeTLAB -XX:NewRatio=4 -XX:MaxTenuringThreshold=15 -XX:+CMSParallelRemarkEnabled \
        -verbose:gc -Xloggc:gc.log -XX:+PrintGCDetails -XX:+PrintGCDateStamps -XX:+PrintTenuringDistribution -XX:+PrintGCApplicationConcurrentTime -XX:+PrintGCApplicationStoppedTime \
        -Djava.rmi.server.hostname=127.0.0.1 \
        -Djava.net.preferIPv4Stack=true \
        -Djgroups.bind_addr=127.0.0.1 \
        -Dlogback.configurationFile=bubing-logback.xml \
        -Dcom.sun.management.jmxremote.port=9995 -Dcom.sun.management.jmxremote.ssl=false -Dcom.sun.management.jmxremote.authenticate=false \
        -cp reveal-crawler.jar  gr.iti.mklab.bubing.ItiAgent \
        -h 127.0.0.1 -P reveal.properties -g eu agent -n 2>err >out