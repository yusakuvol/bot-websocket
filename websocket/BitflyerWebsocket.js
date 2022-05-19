"use strict";

const io = require("socket.io-client");
const axios = require("axios");
const moment = require("moment");

const { WebsocketBase } = require("./WebsocketBase");

class BitflyerWebsocket extends WebsocketBase {
  // bitFlyerのWebsocketを扱うクラスです.

  constructor(logger) {
    // コンストラクタです.
    // :param my_id: WebSocket識別用の名前. 主にログ出力用.
    // :param logger: ロガーインスタンス.
    // WebSocketのURL.
    const WS_URL = "https://io.lightstream.bitflyer.com";
    super(WS_URL, logger);

    this.channels = [
      "lightning_ticker_FX_BTC_JPY",
      "lightning_executions_FX_BTC_JPY",
    ];

    this.symbol = "FX_BTC_JPY";
    const REST_URL =
      "https://api.bitflyer.com/v1/getticker?product_code=" + this.symbol;

    // Ticker.
    this._fetchTicker(REST_URL);
    this.tickerInitialized = true;
  }

  _initializeWebsocket() {
    super._initializeWebsocket();
    this.ws.on("connect", () => {
      this._onOpen();
    });
  }

  _onOpen() {
    // WebSocketでコネクションオープン時の処理です.
    super._onOpen();
    this._subscribe();
  }

  _subscribe() {
    // WebSocketメッセージの購読を開始します.
    this.logger.info("Subscribing start.");
    for (const channel of this.channels) {
      this.ws.emit("subscribe", channel, (err) => {
        if (err) {
          this.logger.error(channel, "Subscribe Error:", err);
          return;
        }
        this.logger.info(channel, "Subscribed.");
      });
    }
    this.logger.info("Subscribing end.");
    for (const channel of this.channels) {
      this.ws.on(channel, (message) => {
        this._onMessage(channel, message);
      });
    }
  }

  _onMessage(channel, message) {
    // 受信したWebSocketメッセージを処理します.
    // :param message: WebSocketメッセージ.
    super._onMessage(message);
    try {
      if (channel == "lightning_ticker_FX_BTC_JPY") {
        this._onGetTicker(message);
      } else if (channel == "lightning_executions_FX_BTC_JPY") {
        this._onGetExecutions(message);
      }
    } catch (e) {
      this.logger.error(e);
    }
  }

  _onGetTicker(ticker) {
    // Ticker受信時の処理です.
    // :param ticker: Ticker情報.
    this.lastTickerTimestamp = moment().unix();
    this._updateTicker(ticker);
  }

  _onGetExecutions(executions) {
    // 約定履歴受信時の処理です.
    // :param executions: 約定情報.
    this.lastExecutionTimestamp = moment().unix();
    this._updateExecutionCaptures(executions);
  }

  async _fetchTicker(URL) {
    // Ticker情報を取得します.
    try {
      const ticker = await axios.get(URL);
      this._updateTicker(ticker.data);
    } catch (e) {
      this.logger.error(e);
    }
  }

  _updateTicker(ticker) {
    // 保持しているTicker情報をアップデートします.
    // :param ticker: Ticker情報.
    const tickerTimestamp = moment(ticker.timestamp).unix();
    this._saveTicker(
      tickerTimestamp,
      ticker.ltp,
      ticker.best_ask,
      ticker.best_bid,
      ticker.best_ask_size,
      ticker.best_bid_size
    );
  }

  _updateTickerByExecution(execution) {
    // 保持しているTicker情報を約定情報でアップデートします.
    // :param execution: 約定情報.
    // :return:
    const execTimestamp = moment(execution["execDate"]).unix();
    if (execTimestamp > this.lastTicker["timestamp"]) {
      const nowTimestamp = moment().unix();
      const latency = nowTimestamp * 1000 - execTimestamp * 1000;
      this.lastTicker["timestamp"] = execTimestamp;
      this.lastTicker["ltp"] = execution["price"];
      this.lastTicker["latency"] = latency;
    }
  }

  _updateExecutionCaptures(executions) {
    // 約定履歴キャプチャをアップデートします.
    // :param executions: 約定情報.
    for (const execution of executions) {
      if (
        execution["buy_child_order_acceptance_id"] in this.executionCaptures
      ) {
        this.executionCaptures[execution["buy_child_order_acceptance_id"]][
          "filled"
        ] += execution["size"];
      }
      if (
        execution["sell_child_order_acceptance_id"] in this.executionCaptures
      ) {
        this.executionCaptures[execution["sell_child_order_acceptance_id"]][
          "filled"
        ] += execution["size"];
      }
      for (const key in this.executionCaptures) {
        if (!this.executionCaptures[key]["start"]) {
          this.executionCaptures[key]["start"] = execution["exec_date"];
        }
        if (this.executionCaptures[key]["open"] == 0) {
          this.executionCaptures[key]["open"] = execution["price"];
        }
        if (this.executionCaptures[key]["high"] < execution["price"]) {
          this.executionCaptures[key]["high"] = execution["price"];
        }
        if (this.executionCaptures[key]["low"] > execution["price"]) {
          this.executionCaptures[key]["low"] = execution["price"];
        }
        this.executionCaptures[key]["close"] = execution["price"];
        this.executionCaptures[key]["end"] = execution["exec_date"];
        if (execution["side"] == "BUY") {
          this.executionCaptures[key]["buy_volume"] += execution["size"];
        } else if (execution["side"] == "SELL") {
          this.executionCaptures[key]["sell_volume"] += execution["size"];
        }
      }
    }
  }
}

module.exports = { BitflyerWebsocket };
