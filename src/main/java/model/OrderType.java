package model;

/** Which side of the market an order is on. */
public enum OrderType {
    BUY,    // a request: "I will pay at most this price"
    SELL    // an offer:  "I will accept at least this price"
}
