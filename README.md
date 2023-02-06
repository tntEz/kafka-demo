# kafka-demo

Simple demo. 

Before running add `kakfa` into your /etc/hosts (on windows `C:\Windows\System32\drivers\etc\hosts`):

```
<your machine IP> kafka
```

Then to run do: 

`docker-compose up -d`

For local developement it is safer to each time do 

`docker-compose down; docker-compose up -d`

Then kafka-ui is running on 

localhost:9010

The java app is self explanatory spring boot application. 

