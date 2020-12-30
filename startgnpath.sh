#!/bin/bash

cd /opt/GnanaPath/gnappsrv
nohup python gnp_appsrv_main.py
echo "GnanaPath has started " >/var/log/all
