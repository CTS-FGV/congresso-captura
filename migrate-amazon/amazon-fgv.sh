pg_dump -b -c -x --no-owner --no-acl --no-tablespaces -t $1 -h pcc-pg001.cjawoehtqnpx.us-east-1.rds.amazonaws.com -U pcc_pg001 pcc_pg001 | sed s/capture/c_camdep/g | psql -h 172.16.4.229 -U cts cts
