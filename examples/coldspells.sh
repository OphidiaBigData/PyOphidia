#!/bin/bash

set -e

mkdir -p /usr/local/ophidia/{historical,model}

echo "Start download of tasmin_day_CMCC-CESM_historical_r1i1p1_20000101-20041231.nc"
cd /usr/local/ophidia/historical
curl -k -s -O 'https://esgf-node.cmcc.it/thredds/fileServer/esg_dataroot/cmip5/output1/CMCC/CMCC-CESM/historical/day/atmos/day/r1i1p1/v20170725/tasmin/tasmin_day_CMCC-CESM_historical_r1i1p1_20000101-20041231.nc'
echo "Download of tasmin_day_CMCC-CESM_historical_r1i1p1_20000101-20041231.nc completed"

echo "Start download of tasmin_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc"
cd /usr/local/ophidia/model
curl -k -s -O 'https://esgf-node.cmcc.it/thredds/fileServer/esg_dataroot/cmip5/output1/CMCC/CMCC-CESM/rcp85/day/atmos/day/r1i1p1/v20170725/tasmin/tasmin_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc'
echo "Download of tasmin_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc completed"

exit 0

