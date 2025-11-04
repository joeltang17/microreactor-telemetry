# Micro-Reactor Telemetry Data System

An end-to-end data system that simulates micro-reactor telemetry,
processes it in real time with Apache Flink, and stores results in
Delta Lake for analysis with Spark SQL.

# Data source
Source: UC Irvine Machine Learning Repository
https://www.kaggle.com/datasets/rinichristy/combined-cycle-power-plant-data-set-uci-data?resource=download

Features consist of hourly average ambient variables

Temperature (T) in the range 1.81°C and 37.11°C,
Ambient Pressure (AP) in the range 992.89-1033.30 milibar,
Relative Humidity (RH) in the range 25.56% to 100.16%,
Exhaust Vacuum (V) in teh range 25.36-81.56 cm Hg,
Net hourly electrical energy output (EP) 420.26-495.76 MW