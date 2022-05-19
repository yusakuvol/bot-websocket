"use strict";

const { TapClient } = require("liquid-tap");
const moment = require("moment");
const { WebsocketBase } = require("./WebsocketBase");

class LiquidWebsocket extends WebsocketBase {

  constructor(logger) {
    // コンストラクタ
    // :param my_id: WebSocket識別用の名前. 主にログ出力用.
    // :param logger: ロガーインスタンス.
    // WebSocketのURL.
    const WS_URL = null;
    super(WS_URL, logger);

    this.tap = new TapClient();
    this.channels = ["product_cash_btcjpy_5", "executions_cash_btcjpy"];

    this.tickerInitialized = true;
  }

  _initializeWebsocket() {
    this._subscribe();
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
      const tap = this.tap.subscribe(channel);
      if (channel == "executions_cash_btcjpy") {
        tap.bind("created", (message) => {
          this._onMessage(message);
        });
      } else {
        tap.bind("updated", (ticker) => {
          this._onGetTicker(ticker);
        });
      }
    }
    this.logger.info("Subscribing end.");
  }

  _onMessage(message) {
    // 受信したWebSocketメッセージを処理します.
    // :param message: WebSocketメッセージ.
    super._onMessage(message);
    try {
      this._onGetExecutions(message);
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

  _updateTicker(ticker) {
    // 保持しているTicker情報をアップデートします.
    // :param ticker: Ticker情報.
    const tickerJSON = JSON.parse(ticker);
    const nowTimestamp = moment().unix();
    this._saveTicker(
      nowTimestamp,
      tickerJSON["last_traded_price"],
      tickerJSON["market_ask"],
      tickerJSON["market_bid"],
      0,
      0
    );
  }

  _updateExecutionCaptures(execution) {
    // 約定履歴キャプチャをアップデートします.
    // :param executions: 約定情報.
    const price = execution["price"];
    const quantity = execution["quantity"];
    for (const key in this.executionCaptures) {
      if (!this.executionCaptures[key]["start"]) {
        this.executionCaptures[key]["start"] = execution["created_at"];
      }
      if (this.executionCaptures[key]["open"] == 0) {
        this.executionCaptures[key]["open"] = price;
      }
      if (this.executionCaptures[key]["high"] < 0) {
        this.executionCaptures[key]["high"] = price;
      }
      if (this.executionCaptures[key]["low"] > 0) {
        this.executionCaptures[key]["low"] = price;
      }
      this.executionCaptures[key]["close"] = price;
      this.executionCaptures[key]["end"] = execution["created_at"];
      if (execution["taker_side"] == "buy") {
        this.executionCaptures[key]["buy_volume"] += quantity;
      } else if (execution["taker_side"] == "sell") {
        this.executionCaptures[key]["sell_volume"] += quantity;
      }
    }
  }
}

module.exports = { LiquidWebsocket };
