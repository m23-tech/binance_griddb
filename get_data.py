import griddb_python as griddb
import pandas as pd

def get_data(symbol):
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

		# Fetch all rows - Crypto_Data_Container
		query_string = f"select symbol, weightedAvgPrice, closeTime where symbol = '{symbol}'"
		query = data_container.query(query_string)
		rs = query.fetch(False)
		
		# Create a List form the Crypto_Data_Container
		retrieved_data= []
		while rs.has_next():
			data = rs.next()
			retrieved_data.append(data)

		return retrieved_data

	except griddb.GSException as e:
		for i in range(e.get_error_stack_size()):
			print("[", i, "]")
			print(e.get_error_code(i))
			print(e.get_location(i))
			print(e.get_message(i))

if __name__ == "__main__":
	# Get the Necessary Data
	crypto_data = get_data('ETHUSDT')
	crypto_dataframe = pd.DataFrame(crypto_data, columns=['symbol', 'weightedAvgPrice', 'closeTime'])

	# Create the Plot using Pandas
	crypto_dataframe.sort_values('closeTime', ascending=True, inplace=True)
	lines = crypto_dataframe.plot.line(x='closeTime', y="weightedAvgPrice", figsize=(20, 10))
