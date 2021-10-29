cd .git/objects/pack

chmod 777 *.pack
chmod 777 *.idx

ANT_HOME=/home/ormb/install/ANT/apache-ant-1.8.4
export ANT_HOME

PATH=$PATH:/home/ormb/install/ANT/apache-ant-1.8.4/bin
export PATH

JAVA_HOME=/home/ormb/install/java/jdk1.5.0_22
export JAVA_HOME


CLASSPATH=/home/ormb/ORMB-JARs/yjp-controller-api-redist.jar:/home/ormb/ORMB-JARs/xstream-1.2.1.jar:/home/ormb/ORMB-JARs/xquery-11.1.1.3.0.jar:/home/ormb/ORMB-JARs/xmlparserv2-11.1.1.3.0.jar:/home/ormb/ORMB-JARs/xalan-2.7.1.jar:/home/ormb/ORMB-JARs/wstx-asl-3.2.1.jar:/home/ormb/ORMB-JARs/staxmate-0.9.1.jar:/home/ormb/ORMB-JARs/stax-api-1.0.1.jar:/home/ormb/ORMB-JARs/stax2.jar:/home/ormb/ORMB-JARs/spl-xai-2.2.0.jar:/home/ormb/ORMB-JARs/spl-weblogicstubs-2.2.0.jar:/home/ormb/ORMB-JARs/spl-shared-2.2.0.jar:/home/ormb/ORMB-JARs/spl-servicebean-2.2.0.jar:/home/ormb/ORMB-JARs/spl-properties.jar:/home/ormb/ORMB-JARs/spl-ccb-2.2.0.jar:/home/ormb/ORMB-JARs/spl-base-2.2.0.jar:/home/ormb/ORMB-JARs/serializer-2.7.1.jar:/home/ormb/ORMB-JARs/orai18n-utility.jar:/home/ormb/ORMB-JARs/orai18n-mapping.jar:/home/ormb/ORMB-JARs/orai18n-collation.jar:/home/ormb/ORMB-JARs/orai18n.jar:/home/ormb/ORMB-JARs/ojdbc5-11.1.0.7.0.jar:/home/ormb/ORMB-JARs/mfcobol.jar:/home/ormb/ORMB-JARs/mail_api-1.4.jar:/home/ormb/ORMB-JARs/log4j-1.2.15.jar:/home/ormb/ORMB-JARs/jtds-1.2.jar:/home/ormb/ORMB-JARs/jcip-annotations.jar:/home/ormb/ORMB-JARs/jaxen-1.1.1.jar:/home/ormb/ORMB-JARs/icu4j-3.6.1.jar:/home/ormb/ORMB-JARs/hibernate-3.2.7.jar:/home/ormb/ORMB-JARs/geronimo-spec-j2ee-1.4-rc4.jar:/home/ormb/ORMB-JARs/ehcache-1.2.3.jar:/home/ormb/ORMB-JARs/dom4j-1.6.1.jar:/home/ormb/ORMB-JARs/concurrent-1.3.4.jar:/home/ormb/ORMB-JARs/commons-logging-1.0.4.jar:/home/ormb/ORMB-JARs/commons-lang-2.2.jar:/home/ormb/ORMB-JARs/commons-io-1.3.2.jar:/home/ormb/ORMB-JARs/commons-httpclient-3.0.1.jar:/home/ormb/ORMB-JARs/commons-fileupload-1.2.jar:/home/ormb/ORMB-JARs/commons-collections-2.1.1.jar:/home/ormb/ORMB-JARs/commons-codec-1.3.jar:/home/ormb/ORMB-JARs/commons-cli-1.1.jar:/home/ormb/ORMB-JARs/commons-beanutils-core-1.7.0.jar:/home/ormb/ORMB-JARs/commonj-3.6.0.jar:/home/ormb/ORMB-JARs/coherence-work-3.6.0.jar:/home/ormb/ORMB-JARs/coherence-3.6.0.jar:/home/ormb/ORMB-JARs/cglib-2.1.3.jar:/home/ormb/ORMB-JARs/c3p0-0.9.1.2.jar:/home/ormb/ORMB-JARs/asm-attrs-1.5.3.jar:/home/ormb/ORMB-JARs/asm-1.5.3.jar:/home/ormb/ORMB-JARs/antlr-2.7.6.jar:/home/ormb/ORMB-JARs/activation_api-1.1.jar

export CLASSPATH

cd /data/jenkins/workspace/ormb_dev_CI

mkdir target
mkdir target/cm
mkdir ORMB-JARs

ant