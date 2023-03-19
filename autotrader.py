# Copyright 2021 Optiver Asia Pacific Pty. Ltd.
#
# This file is part of Ready Trader Go.
#
#     Ready Trader Go is free software: you can redistribute it and/or
#     modify it under the terms of the GNU Affero General Public License
#     as published by the Free Software Foundation, either version 3 of
#     the License, or (at your option) any later version.
#
#     Ready Trader Go is distributed in the hope that it will be useful,
#     but WITHOUT ANY WARRANTY; without even the implied warranty of
#     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#     GNU Affero General Public License for more details.
#
#     You should have received a copy of the GNU Affero General Public
#     License along with Ready Trader Go.  If not, see
#     <https://www.gnu.org/licenses/>.
import asyncio
import itertools
import math
import csv # DELETEME

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


# LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
LIQUIDITY_MAGNITUDE = 8
LIQUIDITY_THRESHOLDS = [t * 10**LIQUIDITY_MAGNITUDE for t in (0.15, 0.4, 0.65)]
POSITION_THRESHOLDS = [-90, -50, -25, 25, 50, 90]
UNHEDGED_LIMIT = 50
HEDGED_THRESHOLD = 100
HEDGE_PERCENTAGE = .001
SPREAD = 3
HEDGED_UPPER_THRESHOLD = 60
LOT_SIZE_ARBITARY_NUMBER = 40

class Order:
    def __init__(self, id, price, lot, start):
        self.id = id
        self.price = price
        self.lot = lot
        self.start = start
    
    def copy(self):
        return Order(self.id, self.price, self.lot, self.start)

    def __repr__(self):
        return f"Order(id={self.id}, price={self.price}, lot={self.lot} start={self.start})"

    def __str__(self):
        return self.__repr__()

class AutoTrader(BaseAutoTrader):
    """Example Auto-trader.

    When it starts this auto-trader places ten-lot bid and ask orders at the
    current best-bid and best-ask prices respectively. Thereafter, if it has
    a long position (it has bought more lots than it has sold) it reduces its
    bid and ask prices. Conversely, if it has a short position (it has sold
    more lots than it has bought) then it increases its bid and ask prices.
    """

    def __init__(self, loop: asyncio.AbstractEventLoop, team_name: str, secret: str):
        """Initialise a new instance of the AutoTrader class."""
        print("Initialising AutoTrader")
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bid_base = self.bid_shifted = self.ask_base = self.ask_shifted = None
        self.new_bid_lot = self.new_bid_price = self.bid_liquidity = \
            self.new_ask_lot = self.new_ask_price = self.ask_liquidity = \
                self.etf_position = self.futures_position = self.position = \
                      self.unhedged_start = self.unhedged_interval = 0
        self.market_state = 0 # -1 = short, 0 = neutral, 1 = long
        self.etf_bids, self.etf_asks, self.futures_asks, self.futures_bids = (dict() for _ in range(4))
        self.is_hedging = False

        with open("output/inputs.csv", "w", newline='') as f: # DELETEME
            writer = csv.writer(f)
            writer.writerow(['position', 'hedged', 'avg_price', 'market_state', 'bid_liquidity', 'bid_spread', 'bid_lot', 'ask_liquidity', 'ask_spread', 'ask_lot'])
        
        with open('output/logs.txt' , 'w') as f: # DELETEME
            f.write("")


    def print_status(self): # DELETEME
        """Log the current status of the autotrader."""
        with open('output/logs.txt', 'a') as f:
            f.write(f"Asks: {self.etf_asks}, Ask base: {self.ask_base}, Ask shifted: {self.ask_shifted}\n")
            f.write(f"Bids: {self.etf_bids}, Bid base: {self.bid_base}, Bid shifted: {self.bid_shifted}\n")


    def log(self, text): # DELETEME
        """Log text to a file."""
        with open('output/logs.txt', 'a') as f:
            f.write(text + "\n")


    def define_market_state(self):
        """Defines the current state of the market.
        
        Uses the relative liquidities of the bid and ask orders to determine
        whether the market is long, short, or neutral on a scale of -2 to 2.
        """
        if self.bid_liquidity == 0 or self.ask_liquidity == 0:
            return 0

        if self.bid_liquidity > self.ask_liquidity:
            return self.bid_liquidity/self.ask_liquidity - 1
        else:
            return -(self.ask_liquidity/self.bid_liquidity) + 1

    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.
        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s",
                            client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.etf_bids or client_order_id in self.etf_asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.
        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)

        if instrument == Instrument.ETF:
            pass

        if instrument == Instrument.FUTURE:
            # Keeping track of unhedged lots
            if abs(self.position) > 10 and not self.is_hedging:
                self.unhedged_interval = self.event_loop.time() - self.unhedged_start
            else:
                self.unhedged_start = self.event_loop.time()
                self.unhedged_interval = 0

            # Hedging in emergencies
            if self.unhedged_interval > UNHEDGED_LIMIT and not self.is_hedging:
                self.emergency_hedge()
                self.unhedged_start = self.event_loop.time()
                self.unhedged_interval = 0
                self.reset_orders(self.etf_bids, 0, 0, 0)
                self.reset_orders(self.etf_asks, 0, 0, 0)
                return

            # Calculating inputs
            avg_price = (ask_prices[0] + bid_prices[0]) / 2
            self.calc_lot_sizes(
                avg_price, ask_prices, ask_volumes, bid_prices, bid_volumes)
            self.market_state = self.define_market_state()

            self.new_bid_price, bid_spread = self.calc_price(
                avg_price, bid_prices, self.bid_liquidity)
            self.new_ask_price, ask_spread = self.calc_price(
                avg_price, ask_prices, self.ask_liquidity, True)
            
        

            # Reset orders
            self.bid_base = self.reset_orders(
                self.etf_bids, Side.BUY, self.new_bid_lot, self.new_bid_price)
            self.ask_base = self.reset_orders(
                self.etf_asks, Side.SELL, self.new_ask_lot, self.new_ask_price)
            self.bid_shifted = self.ask_shifted = None

            # Log inputs
            with open("output/inputs.csv", "a", newline='') as f:  # DELETEME
                writer = csv.writer(f)
                writer.writerow([self.etf_position, self.futures_position, avg_price, self.market_state, self.bid_liquidity,
                                bid_spread, self.new_bid_lot, self.ask_liquidity, ask_spread, self.new_ask_lot])

    def reset_orders(self, order_set, side, lot, price):
        """Replace all orders in the order set with new orders.
        
        Order parameters should be calculated beforehand."""
        base = None

        for order_id in order_set:
            self.send_cancel_order(order_id)

        if lot and price and abs(self.etf_position + (lot if side == Side.BUY else -lot)) < POSITION_LIMIT:
            base = Order(next(self.order_ids), price, lot, 0)
            self.send_insert_order(
                base.id, side, base.price, base.lot, Lifespan.GOOD_FOR_DAY)
            order_set[base.id] = base

        return base

    def calc_price(self, avg_price, prices: List[int], liquidity, is_ask=False) -> None:
        """Calculates price based on liquidity and position.
        We calculate the prices of the bid and ask orders separately based
        on the liquidity of each side of the market and the position of the
        trader.
        """

        spread = SPREAD

        for threshold in LIQUIDITY_THRESHOLDS:
            if liquidity > threshold:
                spread -= 1

        adj = -3

        for threshold in POSITION_THRESHOLDS:
            if self.etf_position > threshold:
                adj += 1

        if is_ask:
            adj = -adj
        emergency_adj = 0

        if adj == -3:
            emergency_adj = 3
        elif adj == 3:
            emergency_adj = -3

        if is_ask:
            emergency_adj = -emergency_adj

        spread += adj
        spread = min(4, max(0, spread))

        return prices[spread] + emergency_adj*TICK_SIZE_IN_CENTS if prices[spread] != 0 else 0, spread

    def calc_lot_sizes(self, avg_price, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int],
                       bid_volumes: List[int]) -> None:
        """Calculates lot sizes based on liquidity and position.
        We consider the liquidity of the bid and ask prices separately based
        on the average price between the best bid and ask, the volume traded,
        and the prices traded at for bids and asks.
        """
        if ask_prices[0] != 0 and bid_prices[0] != 0:

            self.bid_liquidity = self.calc_liquidity(
                avg_price, bid_prices, bid_volumes)
            self.new_bid_lot = self.calc_lot_size(self.bid_liquidity)

            self.ask_liquidity = self.calc_liquidity(
                avg_price, ask_prices, ask_volumes)
            self.new_ask_lot = self.calc_lot_size(
                self.ask_liquidity, is_ask=True)

    def calc_liquidity(self, avg_price: int, prices: List[int], volumes: List[int]):
        """Calculates liquidity of the market for bids and asks.
        TODO: Calculate the average price based on the entire distribution of
        bids and asks rather than just the best bid and ask.
        """
        distances = [0, 0, 0, 0, 0]
        for i in range(len(prices)):
            if prices[i] != 0:
                distances[i] = 1 / \
                    abs(math.log(prices[i]) - math.log(avg_price))
            else:
                distances[i] = 0
            weights = [distances[i] * volumes[i] for i in range(len(volumes))]
            liquidity = sum(weights)
        return liquidity

    def calc_lot_size(self, liquidity: int, is_ask=False):
        """Calculates the lot size for bids and asks.
        Calculates the lot size based on the liquidity of the market and the
        position of the trader. The lot size is proportional to the
        liquidity of the market and inversely proportional to the position 
        of the trader.
        """
        max_l = 2 * 10 ** 7
        liquidity = min(liquidity, max_l)

        position = -1*self.etf_position if is_ask else self.etf_position
        p = math.sqrt(1 - (position+100)/200)
        l = math.sqrt(1 - (max_l-liquidity)/max_l)

        # if liquidity > LIQUIDITY_THRESHOLD:
        #     return 15
        # else:
        #     return 5

        return math.floor(LOT_SIZE_ARBITARY_NUMBER * p * l)

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.
        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        TODO: Shift our orders depending on our position.
        """
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

        # they are hitting our bids
        if client_order_id in self.etf_bids:
            self.etf_position += volume
            self.position += volume

            # order = Order(next(self.order_ids), MAX_ASK_NEAREST_TICK, volume, 0)
            # self.send_hedge_order(order.id, Side.BID, order.price, order.lot)
            # self.futures_bids[order.id] = order                

            # if self.etf_position > 20:
            #     self.shifted_ask = self.insert_shifted_order(
            #         self.ask_base, self.ask_shifted, self.etf_asks, Side.SELL, volume//2)

        # they are lifting our asks
        elif client_order_id in self.etf_asks:
            self.etf_position -= volume
            self.position -= volume

            # order = Order(next(self.order_ids), MIN_BID_NEAREST_TICK, volume, 0)
            # self.send_hedge_order(order.id, Side.ASK, order.price, order.lot)
            # self.futures_asks[order.id] = order

            # if self.etf_position < -20:
            #     self.shifted_bid = self.insert_shifted_order(
            #         self.bid_base, self.bid_shifted, self.etf_bids, Side.BUY, volume//2)

        if  abs(self.etf_position) > HEDGED_THRESHOLD:
            lot_size = self.etf_position - self.futures_position
            if lot_size > 0:
                order = Order(next(self.order_ids), MAX_ASK_NEAREST_TICK, lot_size, 0)
                self.send_hedge_order(order.id, Side.BID, order.price, order.lot)
                self.futures_bids[order.id] = order
            else:
                order = Order(next(self.order_ids), MIN_BID_NEAREST_TICK, -lot_size, 0)
                self.send_hedge_order(order.id, Side.ASK, order.price, order.lot)
                self.futures_asks[order.id] = order
        else:
            if self.futures_position > 0:
                order = Order(next(self.order_ids), MIN_BID_NEAREST_TICK, int(self.futures_position), 0)
                self.send_hedge_order(order.id, Side.ASK, order.price, order.lot)
                self.futures_asks[order.id] = order
            elif self.futures_position < 0:
                order = Order(next(self.order_ids), MAX_ASK_NEAREST_TICK, int(-self.futures_position), 0)
                self.send_hedge_order(order.id, Side.BID, order.price, order.lot)
                self.futures_bids[order.id] = order    

            # if self.position > 0:
            #     order = Order(next(self.order_ids), MIN_BID_NEAREST_TICK, int(self.position*HEDGE_PERCENTAGE), 0)
            #     self.send_hedge_order(order.id, Side.ASK, order.price, order.lot)
            #     self.futures_asks[order.id] = order
            # elif self.position < 0:
            #     order = Order(next(self.order_ids), MAX_ASK_NEAREST_TICK, -int(self.position*HEDGE_PERCENTAGE), 0)
            #     self.send_hedge_order(order.id, Side.BID, order.price, order.lot)
            #     self.futures_bids[order.id] = order

    def insert_shifted_order(self, base, shifted, order_set, side, volume):
        """Inserts a shifted order at a more competitive price to combat position drift.
        
        If there is already a shifted order, we insert a new shifted order
        below it and reassign shifted to the new order. If there is no shifted
        order, we insert a new shifted order below the base order."""
        if shifted:
            shifted.id = next(self.order_ids)
            shifted.price += TICK_SIZE_IN_CENTS if side == Side.BUY else - \
                (TICK_SIZE_IN_CENTS)
            shifted.lot = volume
            self.send_insert_order(
                shifted.id, side, shifted.price, shifted.lot, Lifespan.GOOD_FOR_DAY)
        else:
            price = base.price + TICK_SIZE_IN_CENTS if side == Side.BUY else base.price - \
                TICK_SIZE_IN_CENTS
            shifted = Order(next(self.order_ids), price, volume, 0)
            self.send_insert_order(
                shifted.id, side, shifted.price, shifted.lot, Lifespan.GOOD_FOR_DAY)

        order_set[shifted.id] = shifted

        return shifted

    def emergency_hedge(self):
        """Called to hedge all unhedged lots in emergencies"""
        self.log("")
        self.log("Emergency hedging...")

        self.is_hedging = True
        side = Side.ASK if self.position > 0 else Side.BID
        price = MIN_BID_NEAREST_TICK if self.position > 0 else MAX_ASK_NEAREST_TICK
        order_set = self.futures_asks if self.position > 0 else self.futures_bids

        # technically self.position - 10 should never be negative, but keep this comment
        # in case we get an argument out of bounds error when we try to hedge
        lot_size = self.position - \
            10 if self.position > 0 else abs(10 + self.position)

        order = Order(next(self.order_ids), price, lot_size, 0)
        self.send_hedge_order(order.id, side, order.price, order.lot)
        order_set[order.id] = order

        self.log(
            f"Emergency hedging {order.lot} lots at {order.price} on side {side}")
        self.log("")

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.
        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

        if client_order_id in self.futures_bids:
            self.futures_position += volume
            self.position += volume
            del self.futures_bids[client_order_id]
            # Unhedge
            if self.is_hedging:
                self.log("")
                self.log("Emergency unhedging...")
                order = Order(next(self.order_ids),
                              MIN_BID_NEAREST_TICK, volume, 0)
                self.log(
                    f"Emergency unhedging {order.lot} lots at {order.price} on side {Side.ASK}")
                self.log("")
                self.send_hedge_order(order.id, Side.ASK,
                                      order.price, order.lot)
                self.futures_asks[order.id] = order
                self.is_hedging = False

        elif client_order_id in self.futures_asks:
            self.futures_position -= volume
            self.position -= volume
            del self.futures_asks[client_order_id]
            # Unhedge
            if self.is_hedging:
                self.log("")
                self.log("Emergency unhedging...")
                order = Order(next(self.order_ids),
                              MAX_ASK_NEAREST_TICK, volume, 0)
                self.log(
                    f"Emergency unhedging {order.lot} lots at {order.price} on side {Side.BID}")
                self.log("")
                self.send_hedge_order(order.id, Side.BID,
                                      order.price, order.lot)
                self.futures_bids[order.id] = order
                self.is_hedging = False

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.
        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.
        If an order is cancelled its remaining volume will be zero.
        NB: this function only triggers when an order status changes, not when
        we change the status of an order ourselves.
        """
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)

        if remaining_volume == 0:
            if client_order_id in self.etf_bids:
                del self.etf_bids[client_order_id]
            elif client_order_id in self.etf_asks:
                del self.etf_asks[client_order_id]

    def on_trade_ticks_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                               ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically when there is trading activity on the market.
        The five best ask (i.e. sell) and bid (i.e. buy) prices at which there
        has been trading activity are reported along with the aggregated volume
        traded at each of those price levels.
        If there are less than five prices on a side, then zeros will appear at
        the end of both the prices and volumes arrays.
        """
        self.logger.info("received trade ticks for instrument %d with sequence number %d", instrument,
                         sequence_number)

