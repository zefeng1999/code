from vnpy.event import EventEngine
from vnpy.trader.engine import MainEngine
from vnpy.trader.ui import MainWindow, create_qapp
from vnpy_ctp import CtpGateway
import csv, json
import os
import pandas as pd
from vnpy.trader.event import EVENT_TICK
from datetime import date

def load_json(filename: str) -> dict:
    """
    Load data from json file in temp path.
    """
    if filename:
        with open(filename, mode="r", encoding="UTF-8") as f:
            data: dict = json.load(f)
        return data
    else:
        print("@!")

# 创建 Pandas DataFrame 存储所有合约的行情数据
data_dict: dict[str,pd.DataFrame] = {}

def process_tick_data(event):

    tick = event.data
    instrument = tick.symbol

    if instrument not in data_dict:
        # 创建新的 pd.DataFrame 对象
        data_dict[instrument] = pd.DataFrame(columns=["datetime", "volume", "open_interest", "last_price","turnover", "limit_down",
        "limit_up","PreSettlementPrice","pre_close","bid_price_1", "ask_price_1", "bid_volume_1", "ask_volume_1"])
    # 将行情数据添加到 DataFrame
    row = {
        "datetime": tick.datetime,
        "volume": tick.volume,
        "open_interest": tick.open_interest,
        "last_price": tick.last_price,
        "turnover": tick.turnover,
        "limit_down": tick.limit_down,
        "limit_up": tick.limit_up,
        "PreSettlementPrice": tick.PreSettlementPrice,
        "pre_close": tick.pre_close,
        "bid_price_1": tick.bid_price_1,
        "ask_price_1": tick.ask_price_1,
        "bid_volume_1": tick.bid_volume_1,
        "ask_volume_1": tick.ask_volume_1
    }

    data_dict[instrument].loc[len(data_dict[instrument])] = row #底部插入最新一行
    folder_name = 'tick_data'
    if not os.path.exists(folder_name):
        os.makedirs(folder_name)

    # 切换到 `tick_data` 目录
    os.chdir(folder_name)
    current_date = date.today().strftime("%Y-%m-%d")
    if not os.path.exists("{}".format(current_date)):
        os.makedirs("{}".format(current_date), exist_ok=True)
    # 切换回上一级目录
    os.chdir('..')

    filename = "{}/{}.csv".format(current_date, instrument)
    filepath = os.path.join("tick_data", filename)

    # 文件不存在增加列的表头，存在就不显示header
    if not os.path.exists(filepath):
        data_dict[instrument].tail(1).to_csv(filepath,mode='a', header=True, index=False)
    else:
        data_dict[instrument].tail(1).to_csv(filepath,mode='a', header=False, index=False)


def main():
    qapp = create_qapp()

    event_engine = EventEngine()
    main_engine = MainEngine(event_engine)
    main_engine.add_gateway(CtpGateway, "CtpGateway4")

    # 连接CTP行情服务器
    filepath = 'connect_ctpgateway4.json'
    # 读取 JSON 文件
    setting: dict = load_json(filepath)
    main_engine.connect(setting,"CtpGateway4")
    # 获取CTP行情接口


    # 注册回调函数
    event_engine.register(EVENT_TICK, process_tick_data)
    # 开启事件引擎和主循环
    qapp.exec()

if __name__ == "__main__":
    main()