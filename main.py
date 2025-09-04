import streamlit as st
import pyodbc
import pandas as pd
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt
import json
import plotly.express as px
from statsmodels.tsa.arima.model import ARIMA
import numpy as np
from statsmodels.tsa.holtwinters import ExponentialSmoothing
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Dense
from tensorflow.keras.optimizers import Adam


# DB Fetch
@st.cache_data
def fetch_data(scheme_code):
    conn = pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=mutual-fund-server-storage.database.windows.net;"
        "DATABASE=MutualFundS;"
        "UID=cdac;"
        "PWD=Azure@123;"
    )
    df = pd.read_sql(f"SELECT [date] , [nav] FROM [dbo].[ActiveMutualFundsNavs]  where scheme_code = '{scheme_code}' order by date desc;", conn)
    conn.close()
    return df

def meta_fund_data(scheme_code):
    conn = pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=mutual-fund-server-storage.database.windows.net;"
        "DATABASE=MutualFundS;"
        "UID=cdac;"
        "PWD=Azure@123;"
    )
    df = pd.read_sql(f"SELECT * FROM [Prod].[KuveraPortfolioExtractsInformation] where scheme_code = '{scheme_code}';", conn)
    conn.close()
    return df

st.set_page_config(layout="wide")  # Use wide layout
st.title("Mutual Fund NAV Forecasting")

with open("scheme_codes.json", "r") as f:
    funds_data = json.load(f)

fund_dict = {fund["name"]: fund["scheme_code"] for fund in funds_data}

fund_names = sorted(fund_dict.keys())

selected_fund = st.selectbox("Search and select your Mutual Fund", fund_names)

scheme_code = fund_dict[selected_fund]
st.success(f"Selected Scheme Code: {scheme_code}")

# Fetch data when user enters scheme code
if scheme_code:
    df = fetch_data(scheme_code)
    df2 = meta_fund_data(scheme_code)
    st.success(f"Fetched {len(df)} records for Scheme Code: {scheme_code}")

df["date"] = pd.to_datetime(df["date"])
df = df.sort_values("date")

# Line chart with formatted x-axis
fig = px.line(df, x="date", y="nav", title="NAV Over Time")

# Customize x-axis ticks
fig.update_layout(
    xaxis=dict(
        tickformat="%d-%b-%Y",      # Format: 27-Jul-2025
        tickmode="linear",
        tick0=df["date"].min(),
        dtick=60 * 24 * 60 * 60 * 1000  # Show tick every 60 days (in ms)
    ),
    yaxis_title="NAV",
    xaxis_title="Date",
    template="plotly_dark"  # Optional: dark theme
)

st.plotly_chart(fig, use_container_width=True)

# First row contains all meta info
meta = df2.iloc[0]

fund_name = meta['fund_name']
fund_house = meta['fund_house']
category = meta['category']
fund_type = meta['fund_type']
plan = meta['plan']
expense_ratio = meta['expense_ratio']
start_date = meta['start_date']
manager = meta['fund_manager']
rating = meta['crisil_rating']

r1 = meta['returns_year_1']
r3 = meta['returns_year_3']
r5 = meta['returns_year_5']
rinception = meta['returns_inception']
st.subheader("Fund Information")

col1, col2, col3 = st.columns(3)

with col1:
    st.metric("Fund Name", fund_name)
    st.metric("Fund House", fund_house)
    st.metric("Category", category)

with col2:
    st.metric("Type", fund_type)
    st.metric("Plan", plan)
    st.metric("Manager", manager)

with col3:
    st.metric("Start Date", str(start_date)[:10])
    st.metric("CRISIL Rating", rating)
    st.metric("Expense Ratio", f"{expense_ratio}%")

# New row for AUM + Returns
col4, col5, col6, col7 = st.columns(4)
with col4:
    st.metric("**Return Since Inception:**", f"{rinception}%")

with col5:
    st.metric("Return (1Y)", f"{r1}%")
with col6:
    st.metric("Return (3Y)", f"{r3}%")
with col7:
    st.metric("Return (5Y)", f"{r5}%")

more_info_link = meta["detail_info"]
if isinstance(more_info_link, str) and more_info_link.startswith("http"):
    st.markdown(
    f"""
    <a href="{more_info_link}" target="_blank">
        <button style="
            background-color:#008CBA;
            border:none;
            color:white;
            padding:10px 16px;
            text-align:center;
            font-size:14px;
            border-radius:6px;
            cursor:pointer;">
            🔗 Explore More About This Fund
        </button>
    </a>
    """,
    unsafe_allow_html=True
    )


st.markdown("#### Similar Funds")

cols = st.columns(5)  # 5 buttons side-by-side

for i in range(1, 6):
    name = meta.get(f"cmp_{i}_name")
    code = meta.get(f"cmp_{i}_code")

    if pd.notna(name) and pd.notna(code):
        with cols[i - 1]:  # Place in the i-th column
            if st.button(f"{name}", key=f"cmp_btn_{i}"):
                st.session_state["selected_scheme_code"] = code
                st.rerun()

# Check for session_state for similar fund selection
if "selected_scheme_code" not in st.session_state:
    st.session_state["selected_scheme_code"] = None

scheme_code = (
    st.session_state["selected_scheme_code"]
    if st.session_state["selected_scheme_code"]
    else fund_dict[selected_fund]
)
st.markdown("## Forecast NAV")

# Slider: number of days to predict
days_to_predict = st.slider("Predict next how many days?", min_value=1, max_value=30, value=7)

# Algorithm selection dropdown
algorithm = st.selectbox(
    "Select prediction algorithm",
    [
        "Linear Regression",
        "ARIMA (Rolling Forecast ARIMA(2,1,2))",
        "Exponential Smoothing",
        "LSTM Neural Network"
    ]
)

# Predict button
if st.button("Predict"):
    # Prepare training data: last 100 days (or fewer if not available)
    history_window = 100
    df_for_model = df.copy()
    if len(df_for_model) < 2:
        st.error("Not enough data to forecast.")
    else:
        train_df = df_for_model.tail(history_window).reset_index(drop=True)
        train_nav = train_df["nav"].values
        train_dates = train_df["date"]

        last_date = train_dates.iloc[-1]
        future_dates = pd.date_range(start=last_date + pd.Timedelta(days=1), periods=days_to_predict, freq="D")

        try:
            if algorithm == "Linear Regression":
                X = np.arange(len(train_nav)).reshape(-1, 1)
                model = LinearRegression()
                model.fit(X, train_nav)
                future_X = np.arange(len(train_nav), len(train_nav) + days_to_predict).reshape(-1, 1)
                preds = model.predict(future_X)

            elif algorithm == "ARIMA (Rolling Forecast ARIMA(2,1,2))":
                history_window = 100
                history = list(train_nav[-history_window:])
                predictions = []
                step = 1
                total_steps = days_to_predict
                for i in range(total_steps):
                    model = ARIMA(history, order=(2, 1, 2))
                    model_fit = model.fit()
                    forecast = model_fit.forecast(steps=step)
                    predictions.extend(forecast)
                    history.append(forecast[0])
                preds = np.array(predictions)

            elif algorithm == "Exponential Smoothing":
                esm = ExponentialSmoothing(train_nav, trend="add", seasonal=None, initialization_method="estimated")
                esm_fit = esm.fit()
                preds = esm_fit.forecast(steps=days_to_predict)
            
            elif algorithm == "LSTM Neural Network":
                look_back = 30

                def create_dataset(dataset, look_back=30):
                    X, Y = [], []
                    for i in range(len(dataset) - look_back):
                        X.append(dataset[i:(i + look_back), 0])
                        Y.append(dataset[i + look_back, 0])
                    return np.array(X), np.array(Y)

                nav_series = train_nav.reshape(-1, 1)
                scaler = MinMaxScaler(feature_range=(0, 1))
                data_scaled = scaler.fit_transform(nav_series)

                X_train, y_train = create_dataset(data_scaled, look_back)
                X_train = X_train.reshape((X_train.shape[0], X_train.shape[1], 1))

                @st.cache_resource(show_spinner="Training LSTM model...")
                def train_lstm_model():
                    model = Sequential()
                    model.add(LSTM(50, return_sequences=True, input_shape=(look_back, 1)))
                    model.add(LSTM(50))
                    model.add(Dense(1))
                    model.compile(loss='mean_squared_error', optimizer=Adam(learning_rate=0.01))
                    model.fit(X_train, y_train, epochs=20, batch_size=16, verbose=0)
                    return model

                model = train_lstm_model()

                # Forecast next N days
                input_seq = data_scaled[-look_back:].reshape(1, look_back, 1)
                preds_scaled = []

                for _ in range(days_to_predict):
                    pred = model.predict(input_seq, verbose=0)[0][0]
                    preds_scaled.append(pred)
                    input_seq = np.append(input_seq[:, 1:, :], [[[pred]]], axis=1)

                preds = scaler.inverse_transform(np.array(preds_scaled).reshape(-1, 1)).flatten()

            # Plot
            forecast_df = pd.DataFrame({
                "date": pd.date_range(start=df["date"].iloc[-1] + pd.Timedelta(days=1),periods=days_to_predict),
                "nav": preds
            })

            plot_history = train_df.copy()
            plot_history["Type"] = "History"
            plot_forecast = forecast_df.copy()
            plot_forecast["Type"] = "Forecast"

            combined = pd.concat([plot_history, plot_forecast], ignore_index=True)

            fig_pred = px.line(
                combined,
                x="date",
                y="nav",
                color="Type",
                title=f"LSTM Forecast: last {min(len(df), 100)} days + next{days_to_predict} days",
            )

            fig_pred.update_layout(
                xaxis_title="Date",
                yaxis_title="NAV",
                template="plotly_dark"
            )

            st.plotly_chart(fig_pred, use_container_width=True)

        except Exception as e:
            st.error(f"Forecasting failed: {e}")
