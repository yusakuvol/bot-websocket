"use strict";

const io = require("socket.io-client");
const sleep = require("../util/Sleep");
const moment = require("moment");

class WebsocketBase {
  // WebSocketの基底クラスです.
  // WebSocketとのコネクション確立、エラー時の再接続を行います.
  // また、WebSocketのメッセージを元にTicker情報を作成します.
  constructor(wsUrl, logger) {
    // コンストラクタです.
    // :param ws_url: WebSocketの接続先URL.
    // :param logger: ロガーインスタンス.

    // インスタンス変数.
    this.wsUrl = wsUrl;
    this.logger = logger;
    this.ws = null;
    this.status = null;
    // 再接続用のプロパティ.
    this.retryConnectLimit = 3;
    this.retryConnectInterval = 10 * 1000;
    this.retry = true;
    this.retryCount = 0;
    // 遅延計測用のプロパティ.
    this.lastMessageTimestamp = null;
    this.lastTickerTimestamp = null;
    this.lastExecutionTimestamp = null;
    // Tickerを保持するプロパティ.
    this.tickerInitialized = false;
    this.lastTicker = null;
    // Execution Capture. 約定履歴を元に、ある時点から現在までの高値、安値、ボリュームを保持する仕組みです.
    this.executionCaptures = {};
  }

  start() {
    // WebSocketへ接続し、処理を開始します.
    // 本メソッドをメインスレッドから別スレッドとして起動します.
    this.ws = null;
    this.retry = true;
    this.retryCount = 0;
    this.lastMessageTimestamp = null;
    this.lastTickerTimestamp = null;
    this.lastExecutionTimestamp = null;
    try {
      this._initializeWebsocket();
      this._run();
      // stopメソッドが呼ばれる、あるいは再接続リトライ回数を超過することで_runメソッドが終了します.
      this.status = "Terminated";
      this.logger.info("WebSocket has been terminated.");
    } catch (e) {
      this.logger.error(e);
      throw e;
    }
  }

  stop() {
    // WebSocketをクローズし、処理を停止します.
    // メインスレッドから本メソッドを呼ぶことでWebSocketとのコネクションが切断され、
    // 別スレッドで起動したstartメソッドが終了します.
    this.logger.info("Start closing websockets.");
    this.retry = false;
    // this.ws.close();
    this.logger.info("Websocket has been closed.");
  }

  getMyId() {
    // WebSocket識別用の名前を取得します.
    // :return: WebSocket識別用の名前.
    return this.myId;
  }

  isAvailable() {
    // 利用可能な状態かを取得します.
    // :return: 利用可能な状態か否か.
    if (this.status == "Connected" && this.tickerInitialized) {
      return true;
    } else {
      return false;
    }
  }

  getLatency() {
    // 遅延時間(ms)を取得します.
    // ここでの遅延時間とは「Tickerの配信遅延時間」と「最後のメッセージ受信から現在までの経過時間」の大きなほうです.
    // :return: 遅延時間(ms).
    return Math.max(
      this.getTicker()["latency"],
      this.getMillisecondFromLastMessage()
    );
  }

  getLastMessageTimestamp() {
    // 最後のメッセージ受信時刻のタイムスタンプを取得します.
    // :return: 最後のメッセージ受信時刻のタイムスタンプ.
    return this.lastMessageTimestamp;
  }

  getLastTickerTimestamp() {
    // 最後のTickerメッセージ受信時刻のタイムスタンプを取得します.
    // :return: 最後のTickerメッセージ受信時刻のタイムスタンプ.
    return this.lastTickerTimestamp;
  }

  getLastExecutionTimestamp() {
    // 最後のExecutionメッセージ受信時刻のタイムスタンプを取得します.
    // :return: 最後のExecutionメッセージ受信時刻のタイムスタンプ.
    return this.lastExecutionTimestamp;
  }

  getMillisecondFromLastMessage() {
    // 最後のメッセージ受信から現在までの経過時間(ms)を取得します.
    // :return: 最後のメッセージ受信から現在までの経過時間(ms).
    if (this.lastMessageTimestamp) {
      return Number(moment().unix() * 1000 - this.lastMessageTimestamp * 1000);
    } else {
      return 0;
    }
  }

  getMillisecondFromLastTicker() {
    // 最後のTickerメッセージ受信から現在までの経過時間(ms)を取得します.
    // :return: 最後のTickerメッセージ受信から現在までの経過時間(ms).
    if (this.lastTickerTimestamp) {
      return Number(moment().unix() * 1000 - this.lastTickerTimestamp * 1000);
    } else {
      return 0;
    }
  }

  getMillisecondFromLastExecution() {
    // 最後のExecutionメッセージ受信から現在までの経過時間(ms)を取得します.
    // :return: 最後のExecutionメッセージ受信から現在までの経過時間(ms).
    if (this.lastExecutionTimestamp) {
      return Number(
        moment().unix() * 1000 - this.lastExecutionTimestamp * 1000
      );
    } else {
      return 0;
    }
  }

  getTicker() {
    // Tickerを取得します.
    // :return: Ticker.
    return this.lastTicker;
  }

  createExecutionCapture(captureId) {
    // 約定履歴キャプチャを作成します.
    // 約定履歴キャプチャは、それが作られた時点からの高値、安値、ボリュームを記録するための仕組みです.
    // :param capture_id: 約定履歴キャプチャID
    const price = this.lastTicker ? this.lastTicker.ltp : null;
    this.executionCaptures[captureId] = {
      start: null,
      end: null,
      open: price,
      high: price,
      low: price,
      close: price,
      buy_volume: 0,
      sell_volume: 0,
      filled: 0,
    };
  }

  removeExecutionCapture(captureId) {
    // 約定履歴キャプチャを削除します.
    // :param capture_id: 約定履歴キャプチャID
    if (captureId in this.executionCaptures) {
      delete this.executionCaptures[captureId];
    }
  }

  getExecutionCapture(captureId) {
    // 約定履歴キャプチャを取得します.
    // :param capture_id: 約定履歴キャプチャID.
    // :return: 約定履歴キャプチャ.
    if (captureId in this.executionCaptures) {
      return this.executionCaptures[captureId];
    } else {
      return null;
    }
  }

  _initializeWebsocket() {
    // WebSocketを初期化します.
    this.status = "Initializing";
    this.logger.info("Start initializing websocket.");
    this.ws = io(this.wsUrl, {
      transports: ["websocket"], // specify explicitly
    });
    this.status = "Initialized";
    this.logger.info("Finish initializing websocket.");
  }

  async _run() {
    // WebSocketへ接続し、メッセージの処理を開始します.
    // 途中、コネクションが途切れた際、再接続リトライ数の範囲で接続しなおします.
    while (true) {
      this.logger.info("Run_forever has been terminated.");
      if (this.retry) {
        this.retryCount++;
        const sleepTime = this.retryCount * this.retryConnectInterval + 100;
        this.logger.info(
          "Retry connecting after " +
            sleepTime +
            " sec. retry_count=" +
            this.retryCount +
            ")"
        );
        await sleep.sleep(sleepTime * 1000);
      } else {
        this.logger.info("Retry is false. Not retry anymore.");
        break;
      }
    }
  }

  _authenticate() {
    // WebSocketの認証を行います.
  }

  _onOpen() {
    // WebSocketでコネクションオープン時の処理です.
    this.logger.debug("on_open start.");
    // this._authenticate();
    this.lastMessageTimestamp = null;
    this.lastTickerTimestamp = null;
    this.lastExecutionTimestamp = null;
    this.logger.debug("on_open end.");
    this.status = "Connected";
  }

  _onError(error) {
    // WebSocketでエラー発生時の処理です.
    this.logger.debug("on_error start.");
    this.logger.error("retry_count={this.retry_count}");
    this.retry = false;
    this.logger.debug("on_error end.");
  }

  _onClose() {
    // WebSocketコネクションクローズ時の処理です.
    this.logger.debug("on_close start.");
    this.logger.debug("retry={this.retry}");
    if (this.retry_count >= this.retry_connect_limit) {
      this.logger.error("Retry count exceeded. Set retry=False.");
      this.retry = false;
    }
    this.logger.debug("on_close end.");
    this.status = "Disconnected";
  }

  _onMessage(message) {
    // 受信したWebSocketメッセージを処理します.
    // :param message: WebSocketメッセージ.
    const nowTimestamp = moment().unix();
    this.lastMessageTimestamp = nowTimestamp;
  }

  _saveTicker(tickerTimestamp, ltp, askPrice, bidPrice, askSize, bidSize) {
    // Tickerを保存します.
    // :param ticker_timestamp: Tickerのタイムスタンプ.
    // :param ltp: 現在価格.
    // :param ask_price: ベストアスク価格.
    // :param bid_price: ベストビッド価格.
    // :param ask_size: ベストアスクサイズ.
    // :param bid_size: ベストビッドサイズ.
    const nowTimestamp = moment().unix();
    const latency = nowTimestamp * 1000 - tickerTimestamp * 1000;
    this.lastTicker = {
      timestamp: tickerTimestamp,
      ltp: ltp,
      ask_price: askPrice,
      bid_price: bidPrice,
      ask_size: askSize,
      bid_size: bidSize,
      latency: latency,
    };
  }

  _updateExecutionCaptures(executions) {
    // 約定履歴キャプチャの内容をアップデートします.
    // 本メソッドは約定履歴取得時に呼び出されます.
    // :param executions: 約定履歴.
  }
}

module.exports = { WebsocketBase };
