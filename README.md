# Facebook Ads Insights Pull

This is a Python script that pulls insights data from Facebook Ads API and saves it to a CSV file for further analysis. It uses the Facebook Ads API to fetch data related to Facebook advertising campaigns, ad sets, and ads, and stores the data in a structured format for easy analysis.

## Features

- Connects to the Facebook Ads API to fetch insights data for Facebook advertising campaigns, ad sets, and ads.
- Fetches data such as impressions, reach, clicks, spend, and other performance metrics for specified date ranges.
- Saves the fetched data to a CSV file for further analysis.
- Supports customization of parameters such as access token, ad account ID, time range, and fields to fetch.
- Handles errors and exceptions gracefully to ensure smooth execution.

## Prerequisites

Before running the script, you need to have the following:

- Python 3.x installed on your machine.
- A Facebook Ads account with an access token that has appropriate permissions to access insights data.
- Facebook Business Manager account with an active ad account.
- Python `requests` library installed. If it's not installed, you can install it using `pip`:

```bash
pip install requests
```

# Usage
1. Clone the repository or download the main.py file from the GitHub repository at https://raw.githubusercontent.com/omimaq/facebook-ads-insights-pull/main/main.py.
2. Open the main.py file and modify the ACCESS_TOKEN, AD_ACCOUNT_ID, TIME_RANGE, and FIELDS variables according to your requirements. You can also customize other parameters as needed.
3. Save the main.py file with your changes.
4. Open a terminal and navigate to the directory where you saved the main.py file.
5. Run the script using the following command:

