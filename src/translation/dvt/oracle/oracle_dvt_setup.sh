#!/bin/bash
#oracle_dvt_setup.sh

file_path=$1
odbc_version=$2

echo 'file_path - ' $file_path ' and version - ' $odbc_version

install_oracle="no"
if [ "${file_path}" != "" ] && [ "${odbc_version}" != "" ]; 
then 
    echo "Oracle .rpm file exists" 
    install_oracle="yes" 
else 
    echo "Oracle .rpm not exists"; 
fi

echo "Install oracle dependencies is ${install_oracle}"

if [ ${install_oracle} == "yes" ]; 
then

    # Oracle Dependencies
    # if you are using Oracle you should add .rpm files
    # under your license to a directory called oracle/
    # and then uncomment the setup below.

    echo "Install Oracle dependencies for dvt"

    ORACLE_SID=oracle
	ORACLE_ODBC_VERSION=${_oracle_odbc_version_number}
    ORACLE_HOME=/usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64
    
    apt-get -y install --fix-missing --upgrade vim alien unixodbc-dev wget libaio1 libaio-dev
    
    echo "List directory -- ls -ltr"
    ls -ltr

    alien -i *.rpm && rm *.rpm \
        && mkdir -p /usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64/lib/ \
        && echo "/usr/lib/oracle/${ORACLE_ODBC_VERSION}/client64/lib/" > /etc/ld.so.conf.d/oracle.conf \
        && ln -s /usr/include/oracle/${ORACLE_ODBC_VERSION}/client64 $ORACLE_HOME/include \
        && ldconfig -v 
fi

echo "Done..."