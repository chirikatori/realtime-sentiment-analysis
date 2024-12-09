class Config:
    kafka_topic = "your-topic"
    # Producer
    url = "your-url"
    bootstrap_servers = "<IP Address>:9092"
    data_encoding = 'utf-8'
    querystring = {"aweme_id": "6944028931875949829",
                   "count": "10",
                   "cursor": "0"}
    headers = {
        "x-rapidapi-key": "your-api-key",
        "x-rapidapi-host": "your-host"
    }
    # Consumer
    appName = "your-appname"
    kafka_brokers = bootstrap_servers
