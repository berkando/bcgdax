import requests

# Code that is collecting the same data as the following curl command:
# $  curl 'https://api.gdax.com/products/BTC-USD/trades?after=100' | jq

api_url = "https://api.gdax.com"
product = "BTC-USD"


def getLast100Trades(after_trade_id=None):
    after = 'after={}'.format(after_trade_id) if after_trade_id else ''

    url = '{url}//products/{product}/trades?{options}'.format(url=api_url, product=product, options=after)
    r = requests.get(url)
    return r.json()


def collect_trades(days=1):
    from dateutil.parser import parse

    all_trades = getLast100Trades()  # type: list
    latest_trade = parse(all_trades[0]['time'])

    trade_id = all_trades[-1]['trade_id']
    trade_time = parse(all_trades[-1]['time'])
    while (latest_trade - trade_time).days < days:
        print(latest_trade - trade_time)
        trades = getLast100Trades(after_trade_id=trade_id)

        trade_id = trades[-1]['trade_id']
        trade_time = parse(trades[-1]['time'])

        all_trades.extend(trades)

    return all_trades


import pandas as pd


def store_trades(filepath, data):
    with pd.HDFStore(filepath) as store:
        df = pd.DataFrame(data, columns=data[0].keys())
        df.set_index(['trade_id'])

        if '/trades' in store:
            # select indices of duplicated rows:
            idx = store.select('trades', where="index in df.index", columns=['index']).index
            # only those rows, which weren't yet saved:
            sub_df = df.query("index not in @idx")
        else:
            sub_df = df
        store.append('trades', sub_df, data_columns=True)


def read_trades(filepath):
    store = pd.HDFStore(filepath)


if __name__ == '__main__':
    trades = collect_trades(days=0)
    store_trades('mystore.h5', trades)
