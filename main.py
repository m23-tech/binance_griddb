from binance.client import Client
import griddb_python as griddb
import pandas as pd

def retrive_market_data(currency_list):
    try:
        # Create the Connection
        client = Client(os.environ['binance_api_key'],
                        os.environ['binance_api_secret'])
        currency_data = []
        # Iterate the Currency List
        for currency in currency_list:
            # Get Crypto Information
            tickers = client.get_ticker(symbol=str(currency))
            currency_data.append(tickers)
        # Create a DataFrame
        currency_data_df = pd.DataFrame(currency_data)
        # Return the DataFrame
        return currency_data_df
    except BaseException as error:
        print(f"Error Occuered: {error}")

def insert_data(binance_data):
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

        # Get the Collection
        containerName = "Crypto_Data_Container"
        data_container = gridstore.get_container(containerName)

        # Insert the DataFrame data
        data_container.put_rows(binance_data)

    except griddb.GSException as e:
        for i in range(e.get_error_stack_size()):
            print("[", i, "]")
            print(e.get_error_code(i))
            print(e.get_location(i))
            print(e.get_message(i))

if __name__ == "__main__":
    list = ['DOGEUSDT','BNBBTC','BTCUSDT','ETHUSDT']

    # Get Market Data
    market_data = retrive_market_data(list)

    # Drop the columns
    market_data.drop(columns=['lastQty','bidPrice','bidQty','askPrice','askQty', 'quoteVolume', 'firstId', 'lastId', 'count'],inplace=True)


    # Convert String to Float
    market_data['priceChange'] = market_data['priceChange'].astype(float)
    market_data['priceChangePercent'] = market_data['priceChangePercent'].astype(float)
    market_data['weightedAvgPrice'] = market_data['weightedAvgPrice'].astype(float)
    market_data['prevClosePrice'] = market_data['prevClosePrice'].astype(float)
    market_data['lastPrice'] = market_data['lastPrice'].astype(float)
    market_data['openPrice'] = market_data['openPrice'].astype(float)
    market_data['highPrice'] = market_data['highPrice'].astype(float)
    market_data['lowPrice'] = market_data['lowPrice'].astype(float)
    market_data['volume'] = market_data['volume'].astype(float)

    # Convert from Time since Epoch
    market_data['openTime'] = pd.to_datetime(market_data['openTime'], unit='ms')
    market_data['closeTime'] = pd.to_datetime(market_data['closeTime'], unit='ms')

    # Insert the Data
    insert_data(market_data)

