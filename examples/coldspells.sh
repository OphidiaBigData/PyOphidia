#!/bin/bash

mkdir -p /usr/local/ophidia/{historical,model}

cd /usr/local/ophidia/historical
curl -s -O -k 'https://esgf-node.cmcc.it/thredds/fileServer/esg_dataroot/cmip5/output1/CMCC/CMCC-CESM/historical/day/atmos/day/r1i1p1/v20170725/tasmin/tasmin_day_CMCC-CESM_historical_r1i1p1_20000101-20041231.nc'

cd /usr/local/ophidia/model
curl -s -O -k 'https://esgf-node.cmcc.it/thredds/fileServer/esg_dataroot/cmip5/output1/CMCC/CMCC-CESM/rcp85/day/atmos/day/r1i1p1/v20170725/tasmin/tasmin_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc'
cdo -splityear -delete,dom=29feb tasmin_day_CMCC-CESM_rcp85_r1i1p1_20960101-21001231.nc year

exit 0

