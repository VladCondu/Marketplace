"""
This module represents the Consumer.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
import time
from threading import Thread
from threading import Lock


class Consumer(Thread):
    """
    Class that represents a consumer.
    """

    def __init__(self, carts, marketplace, retry_wait_time, **kwargs):
        """
        Constructor.

        :type carts: List
        :param carts: a list of add and remove operations

        :type marketplace: Marketplace
        :param marketplace: a reference to the marketplace

        :type retry_wait_time: Time
        :param retry_wait_time: the number of seconds that a producer must wait
        until the Marketplace becomes available

        :type kwargs:
        :param kwargs: other arguments that are passed to the Thread's __init__()
        """
        Thread.__init__(self, **kwargs)
        self.carts = carts
        self.marketplace = marketplace
        self.retry_wait_time = retry_wait_time
        self.print_locked = Lock()

    def run(self):
        """
        For each cart in carts execute operations within the current cart
        """
        for task_cart in self.carts:
            """ add / remove products to / from cart"""
            current_cart = self.marketplace.new_cart()
            for task in task_cart:
                looper = task['quantity']
                while looper > 0:
                    if task['product'] in self.marketplace.market_stock:
                        self.execute_task(task['type'], current_cart, task['product'])
                        looper -= 1
                    else:
                        time.sleep(self.retry_wait_time)

            """ place order """
            order = self.marketplace.place_order(current_cart)
            with self.print_locked:
                for product in order:
                    print(self.getName(), "bought", product)

    def execute_task(self, task_type, cart_id, product):
        """
        :type task_type: String
        :param task_type: action that needs to be performed on product inside the cart

        :type cart_id: Integer
        :param cart_id: cart unique identifier

        :type product: Product
        :param product: product that the action is performed on
        """
        if task_type == 'remove':
            self.marketplace.remove_from_cart(cart_id, product)
        elif task_type == 'add':
            self.marketplace.add_to_cart(cart_id, product)
