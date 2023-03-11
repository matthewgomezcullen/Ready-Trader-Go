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
        print("Initialising autotrader...")
        super().__init__(loop, team_name, secret)
        self.order_ids = itertools.count(1)
        self.bid_base = self.bid_shifted = self.ask_base = self.ask_shifted = None
        self.next_bid_lot = self.next_ask_lot = 0
        self.bids = dict()
        self.asks = dict()
        self.position = 0

        print("Tracking the liquidity of the market...")
        with open('output/liquidity.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(["bid_liquidity", "ask_liquidity", "avg_price"])

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
        print("Received order book update message.")

        # print("Received order book update message.")
        if instrument == Instrument.ETF:
            print("Calculating lot sizes...")
            self.calc_lot_sizes(ask_prices, ask_volumes, bid_prices, bid_volumes)
            print(f"Lots calculated: {self.next_bid_lot}, {self.next_ask_lot}")
        
        self.logger.info("received order book for instrument %d with sequence number %d", instrument,
                         sequence_number)

        if instrument == Instrument.FUTURE:
            print("Clearing orders:", self.bids, self.asks)
            for bid in [self.bid_base, self.bid_shifted]:
                if bid:
                    self.send_cancel_order(bid.id)
                    # del self.bids[bid.id]
                    bid = None
            
            for ask in [self.ask_base, self.ask_shifted]:
                if ask:
                    self.send_cancel_order(ask.id)
                    # del self.asks[ask.id]
                    ask = None
            print("Orders cleared:", self.bids, self.asks)
            
            price_adjustment = - (self.position // 10) * TICK_SIZE_IN_CENTS
            
            if self.next_bid_lot and self.position < POSITION_LIMIT:
                print("Calculating bid price...")
                new_bid_price = bid_prices[2] + price_adjustment if bid_prices[2] != 0 else 0

                if new_bid_price :
                    self.bid_base = Order(next(self.order_ids), new_bid_price, self.next_bid_lot, 0)
                    print(f"Sending insert order for bid: {self.bid_base.id}, {Side.BUY}, {new_bid_price}, {self.next_bid_lot}, {Lifespan.GOOD_FOR_DAY}")
                    self.send_insert_order(self.bid_base.id, Side.BUY, new_bid_price, self.next_bid_lot, Lifespan.GOOD_FOR_DAY)
                    self.bids[self.bid_base.id] = self.bid_base
            
                print("Base bid order inserted.")

            if self.next_ask_lot and self.position > -POSITION_LIMIT:
                print("Calculating ask price...")
                new_ask_price = ask_prices[2] + price_adjustment if ask_prices[2] != 0 else 0

                if new_ask_price:
                    self.ask_base = Order(next(self.order_ids), new_ask_price, self.next_ask_lot, 0)
                    print(f"Sending insert order for ask: {self.ask_base.id}, {Side.SELL}, {new_ask_price}, {self.next_ask_lot}, {Lifespan.GOOD_FOR_DAY}")
                    self.send_insert_order(self.ask_base.id, Side.SELL, new_ask_price, self.next_ask_lot, Lifespan.GOOD_FOR_DAY)
                    self.asks[self.ask_base.id] = self.ask_base
                
                print("Base ask order inserted.")
    
    def send_enlargen_order(self, order, side):
        """Sends an enlargen order to the exchange 
        
        Required to amend orders to be larger than the original
        order size as amend order only allows for smaller orders"""
        self.send_cancel_order(order.id)
        order.id = next(self.order_ids)
        self.send_insert_order(order.id, side, order.price, order.lot, Lifespan.GOOD_FOR_DAY)

    def shift_order_lots(self, base, shifted, order_set, side, volume):
        """Shifts orders when partially or fully filled
        
        Volume is moved from the base order to a shifted order. If 
        the base order is fully filled, it is cancelled and removed 
        from the order set. If the shifted order is fully filled, the
        excess volume is moved to a new shifted order, and the base order
        becomes the previous shifted order."""
        volume = min(volume, base.lot)

        if volume < base.lot:
            base.lot -= volume
            self.send_amend_order(base.id, base.lot)
            if shifted:
                shifted.lot += volume
                self.send_enlargen_order(shifted, side)
            else:
                price = base.price - TICK_SIZE_IN_CENTS if side==Side.BUY else base.price + TICK_SIZE_IN_CENTS
                shifted = Order(next(self.order_ids), price, volume, 0)
                self.send_insert_order(shifted.id, side, shifted.price, shifted.lot, Lifespan.GOOD_FOR_DAY)
                order_set[shifted.id]=shifted
        elif shifted:
            self.send_cancel_order(base.id)
            # del order_set[base.id]
            shifted.lot += volume
            self.send_enlargen_order(shifted, side)
            base = shifted.copy()
            shifted = None
        else:
            self.send_cancel_order(base.id)
            base.price = base.price - TICK_SIZE_IN_CENTS if side==Side.BUY else base.price + TICK_SIZE_IN_CENTS
            base.id = next(self.order_ids)
            self.send_insert_order(base.id, side, base.price, base.lot, Lifespan.GOOD_FOR_DAY)
            


    def on_order_filled_message(self, client_order_id: int, price: int, volume: int) -> None:
        """Called when one of your orders is filled, partially or fully.

        The price is the price at which the order was (partially) filled,
        which may be better than the order's limit price. The volume is
        the number of lots filled at that price.

        TODO: Shift our orders depending on our position.
        """
        print("Order filled!")
        self.logger.info("received order filled for order %d with price %d and volume %d", client_order_id,
                         price, volume)

        # they are hitting our bids
        if client_order_id in self.bids:
            print("Bid hit, volume: ", volume)
            print(self.bids, self.bid_base.id, self.asks, self.ask_base.id)
            self.position += volume
            order = self.bids[client_order_id]
            order.lot -= volume
            print("Sending hedge order...")
            self.send_hedge_order(next(self.order_ids), Side.ASK, MIN_BID_NEAREST_TICK, volume//2)
            if order.lot:
                print("Shifting orders...")
                self.shift_order_lots(self.bid_base, self.bid_shifted, self.bids, Side.BUY, volume)
            else:
                print("No more lots in base order, cancelling...")
                self.send_cancel_order(self.bid_base.id)
                # del self.bids[self.bid_base.id]
                if self.bid_shifted:
                    self.bid_base = self.bid_shifted.copy()
                    self.bid_shifted = None
                else:
                    self.bid_base = None
            print("After hit:")
            # print(self.bids, None if not self.bid_base.id else self.bid_base.id, self.asks, None if not self.ask_base else self.ask_base.id)
            
        # they are lifting our asks
        elif client_order_id in self.asks:
            print("Ask lifted, volume: ", volume)
            print(self.bids, self.bid_base.id, self.asks, self.ask_base.id)
            self.position -= volume
            order = self.asks[client_order_id]
            order.lot -= volume
            print("Sending hedge order...")
            self.send_hedge_order(next(self.order_ids), Side.BID, MAX_ASK_NEAREST_TICK, volume//2)
            if order.lot:
                print("Shifting orders...")
                self.shift_order_lots(self.ask_base, self.ask_shifted, self.asks, Side.SELL, volume)
            else:
                print("No more lots in base order, cancelling...")
                self.send_cancel_order(self.ask_base.id)
                # del self.asks[self.ask_base.id]
                if self.ask_shifted:
                    self.ask_base = self.ask_shifted.copy()
                    self.ask_shifted = None
                else:
                    self.ask_base = None
            print("After lifted:")
            # print(self.bids, None if not self.bid_base.id else self.bid_base.id, self.asks, None if not self.ask_base else self.ask_base.id)
        
        print("Returning...")
  

    def on_order_status_message(self, client_order_id: int, fill_volume: int, remaining_volume: int,
                                fees: int) -> None:
        """Called when the status of one of your orders changes.

        The fill_volume is the number of lots already traded, remaining_volume
        is the number of lots yet to be traded and fees is the total fees for
        this order. Remember that you pay fees for being a market taker, but
        you receive fees for being a market maker, so fees can be negative.

        If an order is cancelled its remaining volume will be zero.
        """
        print("Order status message!")
        self.logger.info("received order status for order %d with fill volume %d remaining %d and fees %d",
                         client_order_id, fill_volume, remaining_volume, fees)
        
        # print out parameters to one line
        print(self.bid_base, self.ask_base)
        if remaining_volume == 0:
            if self.bid_base is not None and client_order_id == self.bid_base.id:
                self.bid_base.id = 0
            elif self.ask_base is not None and client_order_id == self.ask_base.id:
                self.ask_base.id = 0

            # It could be either a bid or an ask
            if client_order_id in self.bids:
                del self.bids[client_order_id]
            if client_order_id in self.asks:
                del self.asks[client_order_id]

            

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

    def calc_lot_sizes(self, ask_prices: List[int], ask_volumes: List[int], bid_prices: List[int],
                                   bid_volumes: List[int]) -> None:
        """Calculates lot sizes based on liquidity and position.

        We consider the liquidity of the bid and ask prices separately based
        on the average price between the best bid and ask, the volume traded,
        and the prices traded at for bids and asks.
        """
        if ask_prices[0] != 0 and bid_prices[0] != 0:
            # best_ask_volume = ask_volumes[0]
            # best_bid_volume = bid_volumes[0]

            best_ask_price = ask_prices[0]
            best_bid_price = bid_prices[0]
            # print("Calculating average price...")
            avg_price = (best_ask_price + best_bid_price) / 2

            # print("Calculating bid liquidity...")
            bid_liquidity = self.calc_liquidity(avg_price, bid_prices, bid_volumes)
            self.next_bid_lot = self.calc_lot_size(bid_liquidity)

            # print("Calculating ask liquidity...")
            ask_liquidity = self.calc_liquidity(avg_price, ask_prices, ask_volumes)
            self.next_ask_lot = self.calc_lot_size(ask_liquidity, is_ask=True)

            try:
                with open('output/liquidity.csv', 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([bid_liquidity, ask_liquidity, avg_price])
            except:
                print("Error writing to csv file:", bid_liquidity, ask_liquidity, avg_price)


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

        return math.floor(30 * p * l)