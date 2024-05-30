from datetime import datetime, timedelta, timezone
import requests
import math


def get_next_8_UTC_date_str(plus_days=0):
    # Get the current time in UTC
    now_utc = datetime.now(timezone.utc)

    # Determine the next 8:00 UTC time
    # If the current hour is before 8:00 UTC, then the next 8:00 UTC is today
    # Otherwise, it's 8:00 UTC tomorrow
    if now_utc.hour < 8:
        next_8_utc = datetime(now_utc.year, now_utc.month, now_utc.day, 8, 0, tzinfo=timezone.utc)
        next_8_utc = next_8_utc + timedelta(days=plus_days)
    else:
        # Add one day to the current date and set the time to 8:00 UTC
        tomorrow = now_utc + timedelta(days=1 + plus_days)
        next_8_utc = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 8, 0, tzinfo=timezone.utc)

    #     # Calculate the time difference in years
    # time_difference = next_8_utc - now_utc
    # time_difference_in_years = time_difference.total_seconds() / (365.25 * 24 * 3600)

    # date_string = next_8_utc.strftime('%d%b%y').upper()
    date_string = f"{int(next_8_utc.strftime('%d'))}{next_8_utc.strftime('%b%y')}".upper()

    return date_string


def get_spot_dvol(coin='btc'):
    # Deribit API endpoint for retrieving the index value
    res = {}
    res1 = []
    l1 = [f'{coin}_usd', f'{coin}dvol_usdc', ]

    for idx in l1:
        url = f"https://www.deribit.com/api/v2/public/get_index_price?index_name={idx}"

        try:
            # Send the GET request
            response = requests.get(url)
            response.raise_for_status()  # Raises an exception for 4XX or 5XX status codes

            # Parse the JSON response
            data = response.json()
            dvol_value = data['result']['index_price']
            print(f"Current 1-day implied volatility ({idx}) for BTC on Deribit: {dvol_value}")
            res[idx] = dvol_value
            res1.append(dvol_value)
            # return dvol_value
        except requests.RequestException as e:
            print(f"Error fetching DVOL from Deribit: {e}")
            return None

    return tuple(res1)


def round_down_to_nearest_1000(value):
    return (value // 1000) * 1000


def round_up_to_nearest_1000(value):
    return math.ceil(value / 1000) * 1000


def calculate_std_dev_moves_rounded_1000(underlying_price, atm_vol, T, std_devs=2):
    """
    Calculate the up and down moves corresponding to plus and minus 2 standard deviations.

    :param underlying_price: The price of the underlying asset.
    :param atm_vol: The at-the-money implied volatility (as a decimal).
    :param days_to_expiry: Time to expiry in days.
    :return: A tuple containing the down move and up move for plus and minus 2 standard deviations.
    """
    # T = days_to_expiry / 365  # Convert days to years
    sqrt_T = math.sqrt(T)

    # For 2 standard deviations, multiply volatility by 2
    adjusted_vol = atm_vol * std_devs

    # Calculate expected range low and high for 2 standard deviations
    expected_range_low = underlying_price / math.exp(adjusted_vol * sqrt_T)
    expected_range_high = underlying_price * math.exp(adjusted_vol * sqrt_T)

    # Calculate down move and up move
    down_move = expected_range_low - underlying_price
    up_move = expected_range_high - underlying_price
    implied_range_low = underlying_price + down_move
    implied_range_high = underlying_price + up_move

    return round_down_to_nearest_1000(implied_range_low), round_up_to_nearest_1000(implied_range_high)


from datetime import datetime, timedelta, timezone


def get_next_8_UTC_date_str(plus_days=0):
    # Get the current time in UTC
    now_utc = datetime.now(timezone.utc)

    # Determine the next 8:00 UTC time
    # If the current hour is before 8:00 UTC, then the next 8:00 UTC is today
    # Otherwise, it's 8:00 UTC tomorrow
    if now_utc.hour < 8:
        next_8_utc = datetime(now_utc.year, now_utc.month, now_utc.day, 8, 0, tzinfo=timezone.utc)
        next_8_utc = next_8_utc + timedelta(days=plus_days)
    else:
        # Add one day to the current date and set the time to 8:00 UTC
        tomorrow = now_utc + timedelta(days=1 + plus_days)
        next_8_utc = datetime(tomorrow.year, tomorrow.month, tomorrow.day, 8, 0, tzinfo=timezone.utc)

    #     # Calculate the time difference in years
    # time_difference = next_8_utc - now_utc
    # time_difference_in_years = time_difference.total_seconds() / (365.25 * 24 * 3600)

    # date_string = next_8_utc.strftime('%d%b%y').upper()
    date_string = f"{int(next_8_utc.strftime('%d'))}{next_8_utc.strftime('%b%y')}".upper()

    return date_string


def get_startup_channels(coin='BTC', days=3, std_dev=2, raw_or_agg2 = 'agg2' , additional_tickers=['ticker.BTC-PERPETUAL.agg2']):
    str_array = [get_next_8_UTC_date_str(i) for i in range(days)]
    print(str_array)
    spot, dvol = get_spot_dvol(coin=coin.lower())
    k_low, k_high = calculate_std_dev_moves_rounded_1000(spot, dvol / 100, days / 365, std_dev)
    print(k_low, k_high)

    l1 = [f'ticker.{coin.upper()}-' + i + '-' + str(j) + '-' + k + f'.{raw_or_agg2}' for i in str_array for j in
          list(range(int(k_low), int(k_high + 1000), 1000)) for k in ['C', 'P']] + additional_tickers
    print(l1)
    return l1


def get_day_n_options(coin='BTC', day=3, std_dev=2, raw_or_agg2 = 'agg2' , additional_tickers=[]):
    str_array = [get_next_8_UTC_date_str(day)]
    print(str_array)
    spot, dvol = get_spot_dvol(coin=coin.lower())
    k_low, k_high = calculate_std_dev_moves_rounded_1000(spot, dvol / 100, day / 365, std_dev)
    print(k_low, k_high)

    l1 = [f'ticker.{coin.upper()}-' + i + '-' + str(j) + '-' + k + f'.{raw_or_agg2}' for i in str_array for j in list(range(int(k_low), int(k_high + 1000), 1000)) for k in ['C', 'P']] + additional_tickers
    print(l1)
    return l1

# get_startup_channels(std_dev =1 )
