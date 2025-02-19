import yfinance as yf
import pandas as pd
import datetime as dt
import matplotlib.pyplot as plt
import seaborn as sns

# Define parameters
currency = "USD"
metric = "Close"
start = dt.datetime(2018, 1, 1)
end = dt.datetime.now()

crypto = ['BTC', 'ETH', 'LTC', 'XRP', 'DASH', 'SC']
colnames = []

# Download Data
combined = pd.DataFrame()

for ticker in crypto:
    try:
        data = yf.download(f"{ticker}-{currency}", start=start, end=end)[metric]
        combined[ticker] = data  # Add column for each crypto
    except Exception as e:
        print(f"Error retrieving {ticker}: {e}")

# Plot Crypto Prices
plt.figure(figsize=(12, 6))
plt.yscale('log')  # Logarithmic scale

for ticker in crypto:
    if ticker in combined.columns:
        plt.plot(combined[ticker], label=ticker)

plt.legend(loc="upper right")
plt.xlabel("Date")
plt.ylabel(f"{metric} Price (log scale)")
plt.title("Cryptocurrency Prices Over Time")
plt.grid(True)
plt.show()

# Print Data (Optional)
print(combined)

# Compute and Plot Correlation Heatmap
correlation_matrix = combined.pct_change().corr(method='pearson')

plt.figure(figsize=(8, 6))
sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", linewidths=0.5)
plt.title("Cryptocurrency Correlation Heatmap")
plt.show()