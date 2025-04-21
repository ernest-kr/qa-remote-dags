from airflow.sdk.definitions.asset.decorators import asset


@asset(schedule=None, tags=["asset", "AIP-75"])
def dag1_asset():
    pass


@asset(schedule=None, tags=["asset", "AIP-75"])
def dag2_asset():
    pass


@asset.multi(outlets=[dag1_asset, dag2_asset], schedule=None, tags=["asset", "AIP-75"])
def asset_multi():
    pass


# @asset(schedule="@daily") #   Future work, attach task to the asset decorator
# def raw_quotes():
#     quotes = "Hello"
#
#     @task
#     def make_it_uppercase(quotes):
#         return quotes.upper()
#
#     upper_quote = make_it_uppercase(quotes)
#
#     return upper_quote
