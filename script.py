from web3 import Web3
import time
import pandas as pd

def buildDataFrames(txDetailData, txDetailLogs):

    txDetailData = pd.DataFrame(txDetailData)
    txDetailLogs = pd.concat([pd.DataFrame(i) for i in txDetailLogs])
    txDetailLogs['topics'] = txDetailLogs['topics'].apply(lambda element: [row.hex() for row in element])

    topic_split = txDetailLogs['topics'].apply(pd.Series)
    topic_split.columns = ['topic_1', 'topic_2', 'topic_3']
    txDetailLogs = pd.concat([txDetailLogs, topic_split], axis=1).drop('topics', axis=1)

    txDetailLogs['blockHash'] = txDetailLogs['blockHash'].apply(lambda x: x.hex())
    txDetailLogs['transactionHash'] = txDetailLogs['transactionHash'].apply(lambda x: x.hex())
    txDetailLogs['data'] = txDetailLogs['data'].apply(lambda x: int(x, 0))

    txDetailLogs.drop(columns=['blockNumber'], inplace=True)
    txDetailLogs = txDetailLogs[~txDetailLogs.duplicated(subset=['data'])]
    txDataFrame = pd.merge(txDetailData, txDetailLogs, on='transactionHash')
    txDataFrame = txDataFrame[~txDataFrame.duplicated(subset=['transactionHash', 'data'])]

    return txDataFrame

def txDetails(transactionHash):
    tx_details = w3.eth.getTransaction(transactionHash)

    arrayTxDetails = {
            "transactionHash": transactionHash,
            "blockNumber": tx_details.blockNumber,
            "from":tx_details['from'],
            "gas":tx_details.gas,
            "gasPrice":tx_details.gasPrice,
            "to":tx_details.to,
            "value":w3.fromWei(tx_details.value, 'ether'),
    }

    print('Processing txDetails on block: ', tx_details['blockNumber'])
    return arrayTxDetails

def getTxLogs(transactionHash):
    tx_receipt = w3.eth.getTransactionReceipt(transactionHash)
    tx_receipt = tx_receipt.logs

    print('Processing txDetails on block: ', transactionHash)
    return tx_receipt

def getAllLogs(dailyBlocks, latest):

    if latest == True:
        all_Logs = w3.eth.getLogs({"fromBlock": 'latest'})
    else:
        all_Logs = w3.eth.getLogs({"fromBlock": dailyBlocks[-1], "toBlock": dailyBlocks[0]})

    allLogData = []

    for block_filter in all_Logs:
        checkBlock = {
            "transactionHash": w3.toJSON(block_filter['transactionHash']),
        }
        allLogData.append(checkBlock)
    return allLogData

def checkBlock(latest):

    if latest == True:
        blockNumberList = 'latest'
    else:
        blockNumberList = [11079351, 11079350] # Example. Remember about the order of blocks.
    tokenLog_array = getAllLogs(blockNumberList, latest)

    return tokenLog_array

if __name__ == "__main__":
    print('Getting data...')
    start = time.time()
    w3 = Web3(Web3.WebsocketProvider('wss://mainnet.infura.io/ws/v3/YOUR-API-KEY'))

    blockData = checkBlock(latest=True)

    txHashes = [key['transactionHash'].strip('"\""') for key in blockData[0:10]]

    txDetailData = [txDetails(key) for key in txHashes[0:10]]
    txDetailLogs = [getTxLogs(key) for key in txHashes[0:10]]

    txDataFrame = buildDataFrames(txDetailData, txDetailLogs)

    end = time.time()
    print('It took... ', end - start, ' seconds')