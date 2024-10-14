import os
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import time
import pandas as pd
import snowflake.connector
import streamlit as st

# Watchdog event handler to monitor folder
class FileEventHandler(FileSystemEventHandler):
    def on_created(self, event):
        # When a file is created, load it into Snowflake
        if not event.is_directory:
            file_path = event.src_path
            st.write(f"New file detected: {file_path}")
            load_file_to_snowflake(file_path)

# Function to load file data into Snowflake
def load_file_to_snowflake(file_path):
    # Assuming the file is a CSV; adjust as needed
    df = pd.read_csv(file_path)

    # Snowflake connection details
    conn = snowflake.connector.connect(
        user='YOUR_USERNAME',
        password='YOUR_PASSWORD',
        account='YOUR_ACCOUNT'
    )

    # Write to Snowflake table
    cursor = conn.cursor()
    try:
        for _, row in df.iterrows():
            # Insert each row into the Snowflake table
            cursor.execute(f"INSERT INTO your_table_name VALUES ({','.join(row.astype(str))})")
        st.write(f"Data from {file_path} loaded successfully into Snowflake.")
    except Exception as e:
        st.write(f"Error loading file to Snowflake: {e}")
    finally:
        cursor.close()
        conn.close()

# Folder monitoring function
def monitor_folder(folder_to_monitor):
    event_handler = FileEventHandler()
    observer = Observer()
    observer.schedule(event_handler, folder_to_monitor, recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        observer.stop()

    observer.join()

# Streamlit interface to select folder and start monitoring
def main():
    st.title("File Monitor and Snowflake Loader")

    folder_path = st.text_input("Enter the folder path to monitor:", "")
    if st.button("Start Monitoring"):
        if os.path.exists(folder_path):
            st.write(f"Monitoring folder: {folder_path}")
            monitor_folder(folder_path)
        else:
            st.write("Invalid folder path")

if __name__ == "__main__":
    main()
