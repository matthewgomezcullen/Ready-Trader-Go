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
import csv
import math

from typing import List

from ready_trader_go import BaseAutoTrader, Instrument, Lifespan, MAXIMUM_ASK, MINIMUM_BID, Side


# LOT_SIZE = 10
POSITION_LIMIT = 100
TICK_SIZE_IN_CENTS = 100
MIN_BID_NEAREST_TICK = (MINIMUM_BID + TICK_SIZE_IN_CENTS) // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
MAX_ASK_NEAREST_TICK = MAXIMUM_ASK // TICK_SIZE_IN_CENTS * TICK_SIZE_IN_CENTS
LIQUIDITY_THRESHOLD = 1000000

class Book:
    def __init__(self, instrument: Instrument, sequence_number: int, ask_prices: List[int],
                 ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]):
        self.instrument = instrument
        # list = [new, old]
        self.bid_volumes = {'new': bid_volumes, 'old': []}
        self.bid_prices = {'new': bid_prices, 'old': []}
        self.ask_volumes = {'new': ask_volumes, 'old': []}
        self.ask_prices = {'new': ask_prices, 'old': []}
        self.sequence_number = sequence_number

    def update(self, sequence_number: int, ask_prices: List[int], ask_volumes: List[int],
               bid_prices: List[int], bid_volumes: List[int]):
        self.sequence_number = sequence_number
        self.bid_volumes['old'] = self.bid_volumes['new']
        self.bid_prices['old'] = self.bid_prices['new']
        self.ask_volumes['old'] = self.ask_volumes['new']
        self.ask_prices['old'] = self.ask_prices['new']

        self.bid_volumes['new'] = bid_volumes
        self.bid_prices['new'] = bid_prices
        self.ask_volumes['new'] = ask_volumes
        self.ask_prices['new'] = ask_prices

    def is_ready(self):
        return self.bid_prices['new'] and self.bid_prices['old'] and self.ask_prices['new'][0] != 0 \
            and self.ask_prices['old'][0] != 0 and self.bid_prices['new'][0] != 0 and self.bid_prices['old'][0] != 0
    
    def delta(self):
        old_avg_price = self.ask_prices['old'][0] * self.bid_prices['old'][0] / 2
        new_avg_price = self.ask_prices['new'][0] * self.bid_prices['new'][0] / 2
        return new_avg_price - old_avg_price

    def __repr__(self):
        return f"Book(instrument={self.instrument}, bid_volumes={self.bid_volumes}, bid_prices={self.bid_prices}, ask_volumes={self.ask_volumes}, ask_prices={self.ask_prices}, sequence_number={self.sequence_number})"
    
    def __str__(self):
        return self.__repr__()

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
        self.log("")
        self.log("Initialising autotrader...")
        self.log("")
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bid_base = self.bid_shifted = self.ask_base = self.ask_shifted = None
        self.bids = dict()
        self.asks = dict()
        self.position = 0
        self.futures_position = 0
        self.etf_books = self.futures_books = None


        with open('output/liquidity.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["bid_liquidity", "ask_liquidity", "avg_price"])

        with open('output/logs.txt' , 'w') as f:
            f.write("")
    
    def print_status(self):
        """Log the current status of the autotrader."""
        with open('output/logs.txt', 'a') as f:
            f.write(f"Asks: {self.asks}, Ask base: {self.ask_base}, Ask shifted: {self.ask_shifted}\n")
            f.write(f"Bids: {self.bids}, Bid base: {self.bid_base}, Bid shifted: {self.bid_shifted}\n")
    
    def log(self, text):
        """Log text to a file."""
        with open('output/logs.txt', 'a') as f:
            f.write(text + "\n")
        
    def on_error_message(self, client_order_id: int, error_message: bytes) -> None:
        """Called when the exchange detects an error.

        If the error pertains to a particular order, then the client_order_id
        will identify that order, otherwise the client_order_id will be zero.
        """
        self.logger.warning("error with order %d: %s", client_order_id, error_message.decode())
        if client_order_id != 0 and (client_order_id in self.bids or client_order_id in self.asks):
            self.on_order_status_message(client_order_id, 0, 0, 0)

    def on_hedge_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your hedge orders is filled.

        The price is the average price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.
        """
        self.logger.info("received hedge filled for order %d with average price %d and volume %d", client_order_id,
                         price, volume)

    def on_order_book_update_message(self, instrument: int, sequence_number: int, ask_prices: List[int],
                                     ask_volumes: List[int], bid_prices: List[int], bid_volumes: List[int]) -> None:
        """Called periodically to report the status of an order book.

        The sequence number can be used to detect missed or out-of-order
        messages. The five best available ask (i.e. sell) and bid (i.e. buy)
        prices are reported along with the volume available at each of those
        price levels.
        """

        if instrument == Instrument.ETF:
            if self.etf_books is None:
                self.etf_books = Book(instrument, sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
            elif self.etf_books.sequence_number < sequence_number:
                self.etf_books.update(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
        
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)

        if instrument == Instrument.FUTURE:
            if self.futures_books is None:
                self.futures_books = Book(instrument, sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)
            elif self.futures_books.sequence_number < sequence_number:
                self.futures_books.update(sequence_number, ask_prices, ask_volumes, bid_prices, bid_volumes)

            avg_price = (ask_prices[0] + bid_prices[0]) / 2
            new_bid_lot, new_ask_lot, bid_liquidity, ask_liquidity = self.calc_lot_sizes(avg_price, ask_prices, ask_volumes, bid_prices, bid_volumes)
            new_ask_price = self.calc_price(avg_price, ask_prices, ask_liquidity, True)
            new_bid_price = self.calc_price(avg_price, bid_prices, bid_liquidity)

            for bid_id in self.bids:
                self.log("Bids: " + str(self.bids))
                self.log("Cancelling bid order " + str(bid_id))
                self.send_cancel_order(bid_id)            
            self.bid_base = None
            self.bid_shifted = None
            
            for ask_id in self.asks:
                self.log("Asks: " + str(self.asks))
                self.log("Cancelling ask order " + str(ask_id))
                self.send_cancel_order(ask_id)
            self.bid_base = None
            self.bid_shifted = None

            
            if new_bid_lot and self.position+new_bid_lot < POSITION_LIMIT:

                if new_bid_price :
                    self.bid_base = Order(next(self.order_ids), new_bid_price, new_bid_lot, 0)
                    self.send_insert_order(self.bid_base.id, Side.BUY, new_bid_price, new_bid_lot, Lifespan.GOOD_FOR_DAY)
                    self.bids[self.bid_base.id] = self.bid_base
            

            if new_ask_lot and self.position-new_ask_lot > -POSITION_LIMIT:

                if new_ask_price:
                    self.ask_base = Order(next(self.order_ids), new_ask_price, new_ask_lot, 0)
                    self.send_insert_order(self.ask_base.id, Side.SELL, new_ask_price, new_ask_lot, Lifespan.GOOD_FOR_DAY)
                    self.asks[self.ask_base.id] = self.ask_base

    
    def send_enlargen_order(self, order, side, order_set):
        """Sends an enlargen order to the exchange 
        
        Required to amend orders to be larger than the original
        order size as amend order only allows for smaller orders"""
        self.log("Enlarging order...")
        self.send_cancel_order(order.id)
        order.id = next(self.order_ids)
        self.send_insert_order(order.id, side, order.price, order.lot, Lifespan.GOOD_FOR_DAY)
        order_set[order.id] = order
        self.log("Enlarged order")

    def insert_shifted_order(self, base, shifted, order_set, side, volume):
        """Inserts a shifted order at a more competitive price to combat position drift.
        
        If there is already a shifted order, we insert a new shifted order
        below it and reassign shifted to the new order. If there is no shifted
        order, we insert a new shifted order below the base order."""
        if shifted:
            shifted.id = next(self.order_ids)
            shifted.price += TICK_SIZE_IN_CENTS if side==Side.BUY else -(TICK_SIZE_IN_CENTS)
            shifted.lot = volume
            self.send_insert_order(shifted.id, side, shifted.price, shifted.lot, Lifespan.GOOD_FOR_DAY)
        else:
            price = base.price + TICK_SIZE_IN_CENTS if side==Side.BUY else base.price - TICK_SIZE_IN_CENTS
            shifted = Order(next(self.order_ids), price, volume, 0)
            self.send_insert_order(shifted.id, side, shifted.price, shifted.lot, Lifespan.GOOD_FOR_DAY)

        order_set[shifted.id] = shifted

        return shifted
            

    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.

        TODO: Shift our orders depending on our position.
        """
        self.log("")
        self.log(f"Order filled: {client_order_id} {price} {volume}")
        self.print_status()
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

        # they are hitting our bids
        if client_order_id in self.bids:
            self.position += volume
            
            hedge_volume = self.delta_hedge(price, volume)
            if self.futures_position - hedge_volume > -100:
                self.send_hedge_order(next(self.order_ids), Side.ASK, MIN_BID_NEAREST_TICK, hedge_volume)
                self.futures_position -= hedge_volume
            if self.position > 10:
                self.shifted_ask = self.insert_shifted_order(self.ask_base, self.ask_shifted, self.asks, Side.SELL, volume//2)
        
        # they are lifting our asks
        elif client_order_id in self.asks:
            self.position -= volume

            hedge_volume = self.delta_hedge(price, volume)
            if self.futures_position + hedge_volume < 100:
                self.send_hedge_order(next(self.order_ids), Side.BID, MAX_ASK_NEAREST_TICK, hedge_volume)
                self.futures_position += hedge_volume
            if self.position < -10:
                self.shifted_bid = self.insert_shifted_order(self.bid_base, self.bid_shifted, self.bids, Side.BUY, volume//2)
        
        self.print_status()
        self.log("")


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
        self.log("")
        self.log(f"Order status: {client_order_id} {fill_volume} {remaining_volume} {fees}")
        self.print_status()
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        
        if remaining_volume == 0:
            if client_order_id in self.bids:
                del self.bids[client_order_id]
            elif client_order_id in self.asks:
                del self.asks[client_order_id]
            else:
                raise Exception("Order not found")
            
        self.print_status()
        self.log("")
            

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


    def delta_hedge(self, price: int, volume: int) -> None:
        if self.etf_books is None or self.futures_books is None or \
            self.etf_books.is_ready() is False or self.futures_books.is_ready() is False:
            return volume // 2
        
        etf_delta = self.etf_books.delta()
        futures_delta = self.futures_books.delta()

        if etf_delta == 0 or futures_delta == 0:
            return volume // 2
        
        delta = etf_delta / futures_delta
        if delta > 0:
            return min(volume // 2, int(delta*volume))
        else:
            return 0
        


    def calc_price(self, avg_price, prices: List[int], liquidity, is_ask=False) -> None:
        """Calculates price based on liquidity and position.

        We consider the liquidity of the bid and ask prices separately based
        on the average price between the best bid and ask, the volume traded,
        and the prices traded at for bids and asks.
        """
        price_adjustment = -(self.position // 10) * TICK_SIZE_IN_CENTS
        spread = int(4 - min(2, liquidity // (0.2*10**8)))
        # print(liquidity // (0.2*10**8), spread)

        return prices[spread] + price_adjustment if prices[spread] != 0 else 0
        

    def calc_lot_sizes(self, avg_price, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int],
                                   bid_volumes: List[int]) -> None:
        """Calculates lot sizes based on liquidity and position.

        We consider the liquidity of the bid and ask prices separately based
        on the average price between the best bid and ask, the volume traded,
        and the prices traded at for bids and asks.
        """
        if ask_prices[0] != 0 and bid_prices[0] != 0:

            bid_liquidity = self.calc_liquidity(avg_price, bid_prices, bid_volumes)
            next_bid_lot = self.calc_lot_size(bid_liquidity)

            ask_liquidity = self.calc_liquidity(avg_price, ask_prices, ask_volumes)
            next_ask_lot = self.calc_lot_size(ask_liquidity, is_ask=True)

            try:
                with open('output/liquidity.csv', 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([bid_liquidity, ask_liquidity, avg_price])
            except:
                self.log(f"Error writing to csv file: {bid_liquidity} {ask_liquidity} {avg_price}")
            
            return next_bid_lot, next_ask_lot, bid_liquidity, ask_liquidity
        
        return 0, 0, 0, 0


    def calc_liquidity(self, avg_price: int, prices: List[int], volumes: List[int]):
        """Calculates liquidity of the market for bids and asks.

        TODO: Calculate the average price based on the entire distribution of
        bids and asks rather than just the best bid and ask.
        """
        distances = [0,0,0,0,0]
        for i in range(len(prices)):
            if prices[i] != 0:
                distances[i] = 1 / abs(math.log(prices[i]) - math.log(avg_price))
            else:
                distances[i] = 0
            weights = [distances[i] * volumes[i] for i in range(len(volumes))] 
            liquidity = sum(weights)
        return liquidity
        

    def calc_lot_size(self, liquidity: int, is_ask=False):
        """Calculates the lot size for bids and asks.

        TODO: 
            x Calculate the lot size based on the liquidity and position.
            x Calculate lot size linearly to position.
            x Calculate lot size linearly to liquidity.
        """
        max_l = 2 * 10 ** 7
        liquidity = min(liquidity, max_l)
        
        position = -1*self.position if is_ask else self.position
        p = math.sqrt(1 - (position+100)/200)
        l = math.sqrt(1 - (max_l-liquidity)/max_l)

        # if liquidity > LIQUIDITY_THRESHOLD:
        #     return 15
        # else:
        #     return 5

        return math.floor(30 * p * l)