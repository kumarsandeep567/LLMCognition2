import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import requests
import os

# Function to perform cost efficiency analysis
def cost_efficiency_analysis(df):
    st.subheader("Cost Efficiency Analysis")
    
    # Calculate cost per token (for both prompt and attachment)
    df['total_tokens'] = df['tokens_per_text_prompt'] + df['tokens_per_attachment']
    df['cost_per_token'] = df['total_cost'] / df['total_tokens']
    
    # Fill NaN values with 0 (in case of division by 0 errors)
    df['cost_per_token'] = df['cost_per_token'].fillna(0)

    # Display cost per token
    st.write("Cost per Token for each timestamp:")
    st.write(df[['time_stamp', 'cost_per_token']])

    # Visualize cost per token over time
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(df['time_stamp'], df['cost_per_token'], label='Cost per Token', marker='o', color='purple')
    
    # Add labels and titles
    ax.set_title('Cost per Token Over Time', fontsize=16)
    ax.set_xlabel('Time', fontsize=12)
    ax.set_ylabel('Cost per Token', fontsize=12)
    ax.legend()
    plt.xticks(rotation=45)
    
    # Display the plot in Streamlit
    st.pyplot(fig)

# Function to create the operational efficiency dashboard
def operational_efficiency_dashboard(df):
    st.subheader("Operational Efficiency Dashboard")

    # Calculate efficiency metrics
    df['total_tokens'] = df['tokens_per_text_prompt'] + df['tokens_per_attachment']
    df['tokens_per_second'] = df['total_tokens'] / df['time_consumed']

    # Calculate key metrics
    avg_completion_time = df['time_consumed'].mean()
    avg_cost_per_task = df['total_cost'].mean()
    total_tasks = len(df)
    total_cost = df['total_cost'].sum()

    # Display key metrics
    st.metric("Total Tasks", total_tasks)
    st.metric("Average Completion Time", f"{avg_completion_time:.2f} seconds")
    st.metric("Average Cost per Task", f"${avg_cost_per_task:.4f}")
    st.metric("Total Cost", f"${total_cost:.2f}")

    # Metrics and values for the bar plot
    metrics = ['Avg Completion Time (s)', 'Avg Cost per Task ($)', 'Total Tasks', 'Total Cost ($)']
    values = [avg_completion_time, avg_cost_per_task, total_tasks, total_cost]

    # Visualize the metrics in a bar plot
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.bar(metrics, values, color='skyblue')

    # Add labels and titles
    ax.set_title('Operational Efficiency Metrics', fontsize=16)
    ax.set_ylabel('Values', fontsize=12)
    plt.xticks(rotation=45)
    
    # Display the plot in Streamlit
    st.pyplot(fig)

# Call the function to display the analytics page
def display_analytics_page():
    # Set the title of the analytics dashboard
    st.title("Analytics Dashboard")
    st.subheader("User data")
    
    # Fetch analytics data from the API
    response = requests.get("http://"+ os.getenv("HOSTNAME") + ":8000/analytics")
    
    # Check if the request was successful
    if response.status_code == 200:
        response_data = response.json()
        
        # Convert the response data to a DataFrame
        df = pd.DataFrame(response_data)
        
        # Ensure necessary columns are present
        required_columns = ['time_stamp', 'tokens_per_text_prompt', 'tokens_per_attachment', 'total_cost', 'time_consumed']
        if all(col in df.columns for col in required_columns):
            # Convert time_stamp to datetime and numeric columns
            df['time_stamp'] = pd.to_datetime(df['time_stamp'])
            df['tokens_per_text_prompt'] = pd.to_numeric(df['tokens_per_text_prompt'], errors='coerce')
            df['tokens_per_attachment'] = pd.to_numeric(df['tokens_per_attachment'], errors='coerce')
            df['total_cost'] = pd.to_numeric(df['total_cost'], errors='coerce')
            df['time_consumed'] = pd.to_numeric(df['time_consumed'], errors='coerce')
            
            # Fill NaN values with 0 for tokens_per_attachment and total_cost
            df['tokens_per_attachment'] = df['tokens_per_attachment'].fillna(0)
            df['total_cost'] = df['total_cost'].fillna(0)

            # Display DataFrame in Streamlit
            st.write(df)

            # Call the cost efficiency analysis function
            cost_efficiency_analysis(df)

            # Call the operational efficiency dashboard function
            operational_efficiency_dashboard(df)

        else:
            st.error("Response data does not contain the required columns.")
    else:
        st.error("Failed to fetch data from the API. Status code: {}".format(response.status_code))

# Call the function to display the analytics page
if __name__ == "__main__":
    display_analytics_page()


 


        # # Display DataFrame in Streamlit
        # st.write(df)

        # # Plot the data for token usage and cost over time
        # fig, ax1 = plt.subplots(figsize=(10, 6))

        # # Plot tokens per text prompt and tokens per attachment
        # ax1.plot(df['time_stamp'], df['tokens_per_text_prompt'], label='Tokens per Text Prompt', color='blue', marker='o')
        # if df['tokens_per_attachment'].notna().sum() > 0:
        #     ax1.plot(df['time_stamp'], df['tokens_per_attachment'], label='Tokens per Attachment', color='green', marker='o')

        # ax1.set_xlabel('Time', fontsize=12)
        # ax1.set_ylabel('Number of Tokens', fontsize=12)
        # ax1.tick_params(axis='x', rotation=45)
        # ax1.legend(loc='upper left')

        # # Customize x-axis to show both date and time
        # ax1.xaxis.set_major_formatter(DateFormatter('%H:%M:%S'))

        # # Create a second y-axis for total cost
        # ax2 = ax1.twinx()
        # ax2.plot(df['time_stamp'], df['total_cost'], label='Total Cost', color='red', marker='x')
        # ax2.set_ylabel('Total Cost ($)', fontsize=12)
        # ax2.legend(loc='upper right')

        # # Add titles and labels
        # plt.title('Token Usage and Cost Over Time', fontsize=16)

        # # Display the plot in Streamlit
        # st.pyplot(fig)


# Call the function to display the analytics page
if __name__ == "__main__":
    st.title("Token Usage Analytics")
    display_analytics_page()