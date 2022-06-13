#!/bin/bash
../Executables/bin/pg_ctl -D DemoDir stop
make
make install
../Executables/bin/pg_ctl -D DemoDir start
../Executables/bin/psql -h /tmp/ -p 5450 popowskm