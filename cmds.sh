./bin/coordinator --port 9060 --proxies "proxy-1=10.10.0.1:8051,proxy-2=10.10.0.2:8051,proxy-3=10.10.0.3:8051,proxy-4=10.10.0.4:8051,proxy-5=10.10.0.5:8051,proxy-6=10.10.0.6:8051,proxy-7=10.10.0.7:8051,proxy-8=10.10.0.8:8051,proxy-9=10.10.0.9:8051,proxy-10=10.10.0.10:8051" --interval 10 --loadgen 10.10.0.12:9050

./bin/proxy --id proxy-1 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.8:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-2 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.8:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-3 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.9:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-4 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.9:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-5 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-6 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-7 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 1

./bin/proxy --id proxy-8 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 100

./bin/proxy --id proxy-9 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 100

./bin/proxy --id proxy-10 --port 8050 --control-port 8051 --local-triton localhost:8001 --idle-proxies "10.10.0.10:8001" --coordinator 10.10.0.11:9060 --qthresh 100

./bin/loadgen -u 10.10.0.1:8050,10.10.0.2:8050,10.10.0.3:8050,10.10.0.4:8050,10.10.0.5:8050,10.10.0.6:8050,10.10.0.7:8050 -dist poisson -rps 100
