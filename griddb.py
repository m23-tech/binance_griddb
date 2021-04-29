# Create the GridDB Collection
import griddb_python as griddb

factory = griddb.StoreFactory.get_instance()
try:
    # Get GridStore object
    gridstore = factory.get_store(
        host='10.10.10.100',
        port=250,
        cluster_name='default',
        username='admin',
        password='admin'
    )

    # Create Collection
    containerName = "Crypto_Data_Container"
    crypto_container = griddb.ContainerInfo(containerName,
                    [["symbol", griddb.Type.STRING],
                     ["priceChange", griddb.Type.FLOAT],
                     ["priceChangePercent", griddb.Type.FLOAT],
                     ["weightedAvgPrice", griddb.Type.FLOAT],
                     ["prevClosePrice", griddb.Type.FLOAT],
                     ["lastPrice", griddb.Type.FLOAT],
                     ["openPrice", griddb.Type.FLOAT],
                     ["highPrice", griddb.Type.FLOAT],
                     ["lowPrice", griddb.Type.FLOAT],
                     ["volume", griddb.Type.FLOAT],
                     ["openTime", griddb.Type.TIMESTAMP],
                     ["closeTime", griddb.Type.TIMESTAMP]],
                    griddb.ContainerType.COLLECTION, True)

    data_container = gridstore.put_container(crypto_container)

except griddb.GSException as e:
    for i in range(e.get_error_stack_size()):
        print("[", i, "]")
        print(e.get_error_code(i))
        print(e.get_location(i))
        print(e.get_message(i))