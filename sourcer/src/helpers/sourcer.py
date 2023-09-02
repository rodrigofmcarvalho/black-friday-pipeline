import json
import logging
import random
import time
from datetime import date, timedelta
from enum import Enum
from typing import Dict, Generator, List, Tuple, Union
import sys

from dateutil.relativedelta import TH, relativedelta
from faker import Faker

from sourcer.src.config.constants import (
    BLACK_FRIDAY_MONTH,
    BLACK_FRIDAY_START_DAY,
    BLACK_FRIDAY_WEEK,
    MAX_DELAY_EVENTS,
    MAX_NUM_ITEMS_PER_USER,
    MIN_DELAY_EVENTS,
    TOTAL_ORDERS,
    WEIGHTS_COUPON_APPLIED,
    WEIGHTS_PAYMENT_METHODS,
    WEIGHTS_SHIPPING_METHOD,
    WEIGHTS_SUBSCRIBER_USER,
)

class PaymentMethods(Enum):
    """Enum representing payment methods."""
    CREDIT_CARD = 'Credit Card'
    DEBIT_CARD = 'Debit Card'
    PAYPAL = 'PayPal'
    DIGITAL_WALLET = 'Digital Wallet'
    BLPL = 'BLPL'
    COD = 'COD'


DISCOUNT_COUPONS = {
    'FRIDAYFIVEOFF': 0.05,
    'BLACK10%': 0.1,
    'BF15DISCOUNT': 0.15,
    '20OFFFOURYOUBF': 0.2,
}

LOGGER = logging.getLogger(__name__)
LOGGER.setLevel(logging.ERROR)


class Sourcer:
    """Source random order data."""

    def __init__(
        self,
        num_users: int,
        max_num_items: int,
        products_json_file: str,
    ):
        """Initialize Sourcer.

        Parameters:
            total_orders (int): Total number of orders to generate.
            num_users (int): Number of users for data generation.
            max_num_items (int): Maximum number of items per order.
            products_json_file (str): Path to the products JSON file.
        """
        self.fake = Faker()
        self.num_users = num_users
        self.max_num_items = max_num_items
        self.products_json_file = products_json_file
        self.products_json = self._load_product_data()
        self.date = self._get_next_black_friday_date()

    def _load_product_data(self) -> Dict:
        """
        Load product data from the provided JSON file.

        Return:
            Dict: Product data.

        Raise:
            FileNotFoundError: If the JSON file does not exist.
        """
        try:
            with open(self.products_json_file, 'r') as file:
                return json.load(file)
        except FileNotFoundError:
            LOGGER.error(f'File not found: {self.products_json_file}')
            raise

    def _get_next_black_friday_date(self) -> date:
        """
        Calculate the next Black Friday date.

        Return:
            date: The next Black Friday date.

        Raise:
            Exception: If an error occurs during the calculation.
        """
        try:
            d = date.today()
            if d.month > BLACK_FRIDAY_MONTH or (
                d.month == BLACK_FRIDAY_MONTH
                and d.day > BLACK_FRIDAY_START_DAY + BLACK_FRIDAY_WEEK
            ):
                d += relativedelta(years=1)
            d = d.replace(month=BLACK_FRIDAY_MONTH, day=BLACK_FRIDAY_START_DAY)
            d += relativedelta(weekday=TH(BLACK_FRIDAY_WEEK))
            d += timedelta(days=1)
            return d
        except Exception as error:
            LOGGER.error(f'Error calculating Black Friday date: {error}')
            raise

    def _random_choice_with_weights(
        self, choices: List, weights: List
    ) -> Union[bool, str]:
        """
        Generate a random choice from a list of choices with given weights.

        Parameters:
            choices (List): A list of choices to choose from.
            weights (List): The weights associated with each choice.

        Return:
            Union[bool, str]: The randomly chosen element from the choices list.

        """

        return random.choices(choices, weights=weights, k=1)[0]

    def _apply_discount_coupon(self) -> Tuple[bool, str, float]:
        """
        Apply a discount coupon to the current order price.

        Return:
            A tuple containing:
            - is_coupon_applied (bool): True if the coupon is applied, False otherwise.
            - coupon_description (str): The description of the applied coupon.
            - coupon_value (float): The value of the applied coupon.
        """

        is_coupon_applied = self._random_choice_with_weights(
            [True, False], WEIGHTS_COUPON_APPLIED
        )
        if is_coupon_applied:
            coupon_description = random.choice(list(DISCOUNT_COUPONS.keys()))
            coupon_value = DISCOUNT_COUPONS[coupon_description]
        else:
            coupon_description = ''
            coupon_value = 0
        return is_coupon_applied, coupon_description, coupon_value

    def generate_random_product_data(
        self, order_num: int
    ) -> Dict[str, Dict[str, Union[str, float]]]:
        """
        Generate random product data based on the given order number.

        Parameters:
            order_num (int): The order number used to seed the random generator.

        Return:
            Dict[str, Dict[str, Union[str, float]]]: A dictionary containing the generated product data.
                Each key in the dictionary represents a product index, and its value is a nested dictionary
                with the following keys:
                    - 'type' (str): The type of the product.
                    - 'description' (str): The description of the product.
                    - 'price' (float): The price of the product.

        Raise:
            Exception: If there is an error generating the product data.

        """
        try:
            product_dict = {}
            random.seed(order_num)
            for product_index in range(
                1, random.randint(1, self.max_num_items) + 1
            ):
                product_type = random.choice(list(self.products_json.keys()))
                product_description = random.choice(
                    self.products_json[product_type]
                )
                product_dict[f'product_{product_index}'] = {
                    'type': product_type,
                    'description': product_description,
                    'price': round(random.uniform(1, 100), 2),
                }
            return product_dict
        except Exception as error:
            LOGGER.error(f'Error generating product data: {error}')
            raise

    def _shipping_data(
        self, subscriber_user: bool, total_order_price: float
    ) -> Tuple[str, float, str]:
        """
        Generate the shipping data for an order.

        Parameters:
            subscriber_user (bool): Indicates whether the user is a subscriber.
            total_order_price (float): The total price of the order.

        Return:
            Tuple[str, float, str]: A tuple containing the shipping method,
            shipping cost, and estimated delivery date.
        """

        shipping_method = self._random_choice_with_weights(
            ['Standard', 'Expedited', 'Next Day'], WEIGHTS_SHIPPING_METHOD
        )
        shipping_cost = (
            0
            if subscriber_user
            else round(
                random.uniform(
                    total_order_price * 0.01, total_order_price * 0.1
                ),
                2,
            )
        )
        order_date = self.date
        delivery_date = order_date + timedelta(days=random.randint(3, 30))
        estimated_delivery = delivery_date.strftime('%m/%d/%Y')
        return shipping_method, shipping_cost, estimated_delivery

    def generate_random_order_data(
        self, user_id: int, order_num: int
    ) -> Dict[str, Union[int, str, bool, Dict[str, Union[str, float, bool]]]]:
        """
        Generate random order data based on the provided user ID and order number.

        Parameters:
            user_id (int): The ID of the user.
            order_num (int): The number of the order.

        Returns:
            Dict[str, Union[int, str, bool, Dict[str, Union[str, float, bool]]]]: A dictionary containing the generated order data with the following keys:
                - 'order_id' (str): The ID of the order.
                - 'order_date' (str): The date of the order in the format 'MM/DD/YYYY'.
                - 'user_id' (int): The ID of the user.
                - 'subscriber_user' (bool): Indicates whether the user is a subscriber or not.
                - 'ordered_items' (Dict[str, Union[str, float, bool]]): A dictionary containing the generated product data for the order.
                - 'num_ordered_items' (int): The number of ordered items.
                - 'total_order_price' (float): The total price of the order.
                - 'payment_method' (str): The payment method used for the order.
                - 'discount_coupon_applied' (bool): Indicates whether a discount coupon was applied or not.
                - 'discount_coupon_description' (str): The description of the discount coupon applied.
                - 'discount_coupon_value' (float): The value of the discount coupon applied.
                - 'sales_tax_value' (float): The value of the sales tax applied.
                - 'gift_wrap' (bool): Indicates whether gift wrapping was requested or not.
                - 'shipping_method' (str): The shipping method used for the order.
                - 'shipping_cost' (float): The cost of shipping.
                - 'estimated_delivery' (str): The estimated delivery date.
                - 'platform' (str): The platform used for the order.
                - 'net_total_order_price' (float): The net total price of the order after applying discounts and taxes.
        """

        self.fake.seed_instance(user_id + order_num)
        next_black_friday_date = self.date
        order_id = f'{next_black_friday_date.year}-{next_black_friday_date.month}-{self.fake.random_number(digits=10)}'
        order_date = next_black_friday_date.strftime('%m/%d/%Y')
        subscriber_user = self._random_choice_with_weights(
            [True, False], WEIGHTS_SUBSCRIBER_USER
        )
        ordered_items = self.generate_random_product_data(order_num)
        num_ordered_items = len(ordered_items)
        total_order_price = round(
            sum(item['price'] for item in ordered_items.values()), 2
        )
        payment_method = self._random_choice_with_weights(
            list(PaymentMethods), WEIGHTS_PAYMENT_METHODS
        ).value
        (
            is_coupon_applied,
            coupon_description,
            coupon_value,
        ) = self._apply_discount_coupon()
        sales_tax = round(random.uniform(0.01, 0.1), 2)
        sales_tax_value = round(total_order_price * sales_tax, 2)
        gift_wrap = self.fake.boolean(chance_of_getting_true=20)
        (
            shipping_method,
            shipping_cost,
            estimated_delivery,
        ) = self._shipping_data(subscriber_user, total_order_price)
        platform = self.fake.user_agent()
        net_total_order_price = round(
            total_order_price - shipping_cost - coupon_value - sales_tax_value,
            2,
        )

        return {
            'order': {
                'order_id': order_id,
                'order_date': order_date,
                'user_id': user_id,
                'subscriber_user': subscriber_user,
                'ordered_items': ordered_items,
                'num_ordered_items': num_ordered_items,
                'total_order_price': total_order_price,
                'payment_method': payment_method,
                'discount_coupon_applied': is_coupon_applied,
                'discount_coupon_description': coupon_description,
                'discount_coupon_value': coupon_value,
                'sales_tax_value': sales_tax_value,
                'gift_wrap': gift_wrap,
                'shipping_method': shipping_method,
                'shipping_cost': shipping_cost,
                'estimated_delivery': estimated_delivery,
                'platform': platform,
                'net_total_order_price': net_total_order_price,
            }
        }

    def source_random_order_data(
        self,
    ) -> Generator[
        Dict[str, Union[int, str, bool, Dict[str, Union[str, float, bool]]]],
        None,
        None,
    ]:
        """
        Source random order data for each user and yields the data as JSON strings.

        Return:
            A generator that yields dictionaries representing order data. Each dictionary contains the following keys:
                - 'user_id': An integer representing the user ID.
                - 'order_num': An integer representing the order number.
                - 'data': A dictionary containing additional order data, with the following keys:
                    - 'key': A string representing the key.
                    - 'value': A float representing the value.
                    - 'flag': A boolean representing the flag.

        Raise:
            Exception: If there is an error while sourcing the order data.
        """
        try:
            try:
                all_orders = [
                    (user_id, order_num)
                    for user_id in range(1, self.num_users + 1)
                    for order_num in range(1, MAX_NUM_ITEMS_PER_USER + 1)
                ]

                random.shuffle(all_orders)

                for total_orders, (user_id, order_num) in enumerate(
                    all_orders, start=1, 
                ):
                    if total_orders <= TOTAL_ORDERS:
                        time.sleep(random.uniform(MIN_DELAY_EVENTS, MAX_DELAY_EVENTS))
                        order_data = self.generate_random_order_data(
                            user_id, order_num
                        )
                        yield json.dumps(order_data)
    
            except Exception as error:
                LOGGER.error(f'Error sourcing order data: {error}')
                raise
        except KeyboardInterrupt:
            print('\n\n\nGracefully stopping sourcing order data...\n\n\n')
            sys.exit(0)