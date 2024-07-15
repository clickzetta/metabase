
# build
```shell
docker build -f Dockerfile.cz . -t clickzetta/metabase-clickzetta:v0.47.13
```

# run
```shell
docker run -itd --name metabase-clickzetta \
 -p 3001:3000 \
 -e 'MB_DB_TYPE=mysql' \
 -e "MB_DB_DBNAME=metabase_v047" \
 -e "MB_DB_PORT=3306" \
 -e "MB_DB_USER=root" \
 -e "MB_DB_PASS=" \
 -e "MB_DB_HOST=host.docker.internal" \
 clickzetta/metabase-clickzetta:v0.47.13
 ```