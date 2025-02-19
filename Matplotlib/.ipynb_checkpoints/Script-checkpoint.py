import yfinance as yf
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

def get_stock_data(stock_symbol):
    """Fetch last 5 years of stock data using Yahoo Finance API"""
    stock = yf.Ticker(stock_symbol)
    df = stock.history(period="5y")  # Get 5 years of historical data
    
    if df.empty:
        print("Stock data not found!")
        return None
    
    df = df[['Close', 'Volume']]
    df.reset_index(inplace=True)
    df['Date'] = df['Date'].dt.strftime('%Y-%m')  # Convert to Year-Month format
    return df

def plot_stock_data(stock_symbol):
    """Main function to get stock data and plot it"""
    df = get_stock_data(stock_symbol)
    if df is None:
        return
    
    df = df.groupby('Date').mean().reset_index()  # Aggregate monthly data
    x = np.arange(len(df))
    
    fig, ax1 = plt.subplots(figsize=(10,5))
    
    color = 'tab:blue'
    ax1.set_xlabel("Date")
    ax1.set_ylabel("Closing Price", color=color)
    ax1.plot(x, df['Close'], marker='o', linestyle='-', color=color, label='Closing Price')
    ax1.tick_params(axis='y', labelcolor=color)
    plt.xticks(x[::6], df['Date'][::6], rotation=45)
    
    ax2 = ax1.twinx()
    color = 'tab:red'
    ax2.set_ylabel("Trading Volume", color=color)
    ax2.plot(x, df['Volume'], marker='s', linestyle='-', color=color, label='Trading Volume')
    ax2.tick_params(axis='y', labelcolor=color)
    
    plt.title(f"Stock Closing Price & Trading Volume - {stock_symbol}")
    plt.grid(True)
    plt.show()

if __name__ == "__main__":
    stock_symbol = input("Enter Stock Symbol (e.g., AAPL, TSLA, MSFT): ")
    plot_stock_data(stock_symbol)
