"""
This module represents the Marketplace.

Computer Systems Architecture Course
Assignment 1
March 2021
"""
from threading import Lock


class Marketplace:
    """
    Class that represents the Marketplace. It's the central part of the implementation.
    The producers and consumers use its methods concurrently.
    """

    def __init__(self, queue_size_per_producer):
        """
        Constructor

        :type queue_size_per_producer: Int
        :param queue_size_per_producer: the maximum size of a queue associated with each producer
        """
        self.queue_size_per_producer = queue_size_per_producer  # send it to the producer too
        self.no_of_producers = -1  # first producer will have prod_id = 0
        self.no_of_carts = -1  # first cart will have the id = 0 // actually this means number of carts - 1
        self.product_creator = {}  # map product : prod_id
        self.market_stock = []  # list of all products in the market stock
        self.product_counter = []  # counter used for products made by producers
        self.cart = [[]]  # cart[i] stands for the cart of consumer i

        """DEFINED LOCKS"""
        self.register_locked = Lock()  # producer registration lock
        self.cart_locked = Lock()  # creating cart lock
        self.add_locked = Lock()  # adding to cart lock
        self.remove_locked = Lock()  # removing from cart lock
        self.publish_locked = Lock()  # publishing product lock
        self.market_locked = Lock()  # getting item from market lock

    def register_producer(self):
        """
        Returns an id for the producer that calls this.
        """
        with self.register_locked:
            self.no_of_producers += 1
            new_prod_id = self.no_of_producers

        # create counter in the product_counter list of the current producer
        self.product_counter.append(0)
        return new_prod_id

    def publish(self, producer_id, product):
        """
        Adds the product provided by the producer to the marketplace

        :type producer_id: String
        :param producer_id: producer id

        :type product: Product
        :param product: the Product that will be published in the Marketplace

        :returns True or False. If the caller receives False, it should wait and then try again.
        """
        if self.product_counter[producer_id] >= self.queue_size_per_producer:
            return False

        self.market_stock.append(product)

        with self.publish_locked:
            self.product_counter[producer_id] += 1
            self.product_creator[product] = producer_id

        return True

    def new_cart(self):
        """
        Creates a new cart for the consumer

        :returns an int representing the cart_id
        """
        with self.cart_locked:
            self.no_of_carts += 1
            new_cart_id = self.no_of_carts

        """ create new cart (list) for the current consumer"""
        self.cart.append([])
        return new_cart_id

    def add_to_cart(self, cart_id, product):
        """
        Adds a product to the given cart. The method returns

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to add to cart

        :returns True or False. If the caller receives False, it should wait and then try again
        """
        if product not in self.market_stock:
            return False
        self.cart[cart_id].append(product)
        with self.add_locked:
            self.product_counter[self.product_creator[product]] -= 1
        with self.market_locked:
            if product in self.market_stock:
                self.market_stock.remove(product)
        return True

    def remove_from_cart(self, cart_id, product):
        """
        Removes a product from cart.

        :type cart_id: Int
        :param cart_id: id cart

        :type product: Product
        :param product: the product to remove from cart
        """
        if product in self.cart[cart_id]:
            with self.cart_locked:
                self.product_counter[self.product_creator[product]] += 1
            self.cart[cart_id].remove(product)
            self.market_stock.append(product)

    def place_order(self, cart_id):
        """
        Return a list with all the products in the cart.

        :type cart_id: Int
        :param cart_id: id cart
        """
        return self.cart[cart_id]
