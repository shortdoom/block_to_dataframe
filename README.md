# block_to_dataframe
Transform Ethereum block data (JSON-RPC) into Pandas Dataframe with Web3.py and Infura node.

# Setup

1. Get your API-KEY from Infura.
2. Specify blockNumbers you want to retrive or default to ```latest```.

# Dataframe

Returned dataframe contains all of events for a given transaction within specified block.

# Modify

You can return more values in ```txDetails``` function. However, returned dataframe should be considered a valid ```transactionRecipt``` for each transaction in retrived blocks.
