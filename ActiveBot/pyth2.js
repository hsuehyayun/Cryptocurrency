import WebSocket from "ws";

const ASSET = "SOL";
const PYTH_ID =
  "ef0d8b6fda2ceba41da15d4095d1da392a0d2f8ed0c6c7bc0f4cfac8c280b56d";

const WS_URL = "wss://hermes.pyth.network/ws";
const ws = new WebSocket(WS_URL); //creates a new websocket connection to the pyth network

const priceHistory = [];
const UPDATE_INTERVAL = 1;
const MOVING_AVERAGE_PERIOD = 10;
let lastUpdateTime = 0;
let movingAverage = 0;

//when the websocket connection is opened
ws.onopen = () => {
  console.log("Connected to Pyth Websocket");
  //when the connection is opened, subscribe to price updates for the asset
  console.log(`Subscribing to ${ASSET} price updates...`);
  ws.send(
    JSON.stringify({
      type: "subscribe",
      ids: [PYTH_ID],
    })
  );
  console.log(`Subscribed to ${ASSET} price updates`);
};

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  // console.log(data);
  if (data.type == "price_update") {
    const price_feed = data.price_feed;
    const priceObject = price_feed.price;
    const price = priceObject.price;
    const expo = priceObject.expo;
    const timestamp = priceObject.publish_time;

    const actualPrice = price * 10 ** expo;

    let message = `Price ${actualPrice} `;

    if (timestamp - lastUpdateTime >= UPDATE_INTERVAL) {
      priceHistory.push(actualPrice);

      if (priceHistory.length > MOVING_AVERAGE_PERIOD) {
        priceHistory.shift();
      }
      lastUpdateTime = timestamp;
    }

    if (priceHistory.length === MOVING_AVERAGE_PERIOD) {
      movingAverage =
        priceHistory.reduce((a, b) => a + b, 0) / MOVING_AVERAGE_PERIOD;
      message += `Moving Average: ${movingAverage}`;
      const signal = generateSignal(actualPrice, movingAverage);
      message += ` Signal: ${signal}`;
    } else {
      message += `Moving average not ready yet: ${priceHistory.length} of ${MOVING_AVERAGE_PERIOD}`;
    }

    console.log(message);
  }
};

function generateSignal(price, movingAverage) {
  if (price > movingAverage) {
    return "SELL";
  } else if (price < movingAverage) {
    return "BUY";
  } else {
    return "HOLD";
  }
}
