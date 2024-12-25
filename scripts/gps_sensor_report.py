import os
import sys
import json
import pandas as pd
import requests
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMENonMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from datetime import datetime, timedelta
RECEIVER_EMAILS="eeneeamidral@gmail.com,uuganbileg@tttools.mn"

# Configuration (Replace with your actual details)
CONFIG = {
    'GPS_API_KEY': os.environ.get('GPS_API_KEY'),
    'GPS_DEVICE_ID': os.environ.get('GPS_DEVICE_ID'),
    'SENDER_EMAIL': os.environ.get('SENDER_EMAIL'),
    'SENDER_PASSWORD': os.environ.get('SENDER_PASSWORD'),
    'RECEIVER_EMAILS': RECEIVER_EMAILS.split(','),
    'REPORT_FREQUENCY_DAYS': 1
}

def parse_gps_temp_humidity(json_data):
    """
    Parse GPS tracking data to extract refrigerator temperature and humidity.
    
    :param json_data: List of GPS tracking entries
    :return: Dictionary with parsed sensor data
    """
    sensor_data = {
        'refrigerator_temp': [],
        'humidity': [],
        'plate_numbers': []
    }
    last_valid_humidity = 0
    last_valid_temp = 0
    
    for entry in json_data:
        # Extract timestamp and plate number
        timestamp = datetime.strptime(entry[0], '%Y-%m-%d %H:%M:%S') + timedelta(hours=8)
        plate_number = entry[3]  # Assuming the plate number is in the 4th column
        
        # Extract refrigerator temperature
        if 'io10800' in entry[6]:
            refrigerator_temp = float(entry[6]['io10800']) / 100  # Assuming scaling
            if refrigerator_temp == 250:
                refrigerator_temp = last_valid_temp
            else:
                last_valid_temp = refrigerator_temp
            sensor_data['refrigerator_temp'].append({
                'timestamp': timestamp,
                'value': refrigerator_temp
            })
        
        # Extract humidity
        if 'io10804' in entry[6]:
            humidity = float(entry[6]['io10804'])  # Direct value
            if humidity == 250:
                humidity = last_valid_humidity
            else:
                last_valid_humidity = humidity
            sensor_data['humidity'].append({
                'timestamp': timestamp,
                'value': humidity
            })
        
        # Collect unique plate numbers
        plate_number = "7228УКР"
        sensor_data['plate_numbers'].append(plate_number)
    
    return sensor_data

def construct_api_url(device_id, start_date, end_date, api_key):
    """
    Construct GPS tracking API URL.
    
    :param device_id: GPS device identifier
    :param start_date: Start date for data retrieval
    :param end_date: End date for data retrieval
    :param api_key: API authentication key
    :return: Constructed API URL
    """
    base_url = "https://fms2.gpsbox.mn/api/api.php"
    params = {
        "api": "user",
        "key": api_key,
        "cmd": f"OBJECT_GET_MESSAGES,{device_id},{start_date} 00:00:00,{end_date} 00:00:00,0.01"
    }
    
    return f"{base_url}?api={params['api']}&key={params['key']}&cmd={params['cmd']}"

def fetch_gps_data(api_url):
    """
    Fetch GPS tracking data from the API.
    
    :param api_url: Full API URL with parameters
    :return: JSON data of GPS tracking entries
    """
    try:
        response = requests.get(api_url)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        print(f"Error fetching GPS data: {e}")
        return None

def process_sensor_data(sensor_data, interval_minutes=5):
    """
    Process sensor data into specified minute intervals.
    
    :param sensor_data: List of sensor entries
    :param interval_minutes: Interval for grouping data
    :return: Processed data with interval statistics
    """
    # Convert to DataFrame
    df = pd.DataFrame(sensor_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    # Round timestamps to specified interval
    df['interval_timestamp'] = df['timestamp'].dt.floor(f'{interval_minutes}min')
    
    # Group by interval and calculate statistics
    grouped_data = df.groupby('interval_timestamp').agg({
        'value': ['mean', 'min', 'max', 'count']
    }).reset_index()
    
    # Flatten column names
    grouped_data.columns = ['timestamp', 'mean', 'min', 'max', 'count']
    
    # Convert to list of dictionaries
    processed_data = grouped_data.to_dict('records')
    
    return processed_data

def calculate_daily_averages(sensor_data):
    """
    Calculate daily averages for sensor data.
    
    :param sensor_data: List of processed sensor entries
    :return: Daily statistics
    """
    # Convert to DataFrame
    df = pd.DataFrame(sensor_data)
    df['date'] = df['timestamp'].dt.date
    
    # Calculate daily statistics
    daily_averages = df.groupby('date').agg({
        'mean': ['mean', 'min', 'max']
    }).reset_index()
    
    # Flatten column names
    daily_averages.columns = ['date', 'average', 'minimum', 'maximum']
    daily_averages['date'] = daily_averages['date'].astype(str)
    
    return daily_averages.to_dict('records')

def export_to_excel(refrigerator_temp_data, humidity_data, 
                     refrigerator_temp_daily_avg, humidity_daily_avg, 
                     plate_numbers, output_file='gps_sensor_analysis.xlsx'):
    """
    Export sensor analysis to Excel.
    
    :param refrigerator_temp_data: 5-minute temperature data
    :param humidity_data: 5-minute humidity data
    :param refrigerator_temp_daily_avg: Daily temperature averages
    :param humidity_daily_avg: Daily humidity averages
    :param plate_numbers: List of plate numbers
    :param output_file: Excel file path
    """
    import openpyxl
    from openpyxl.styles import Border, Side, Alignment
    
    workbook = openpyxl.Workbook()
    
    # Create sheets
    combined_5min_sheet = workbook.active
    combined_5min_sheet.title = "Combined 5-Minute Data"
    
    combined_daily_avg_sheet = workbook.create_sheet(title="Combined Daily Averages")
    
    # Prepare border and alignment
    border = Border(
        left=Side(style='thin'),
        right=Side(style='thin'),
        top=Side(style='thin'),
        bottom=Side(style='thin')
    )
    alignment = Alignment(wrap_text=True, horizontal='center', vertical='center')
    
    # Use first plate number or 'N/A'
    first_plate_number = plate_numbers[0] if plate_numbers else 'N/A'
    
    # 5-Minute Data Sheet
    combined_5min_sheet.append(['Plate Number', 'Timestamp', 'Refrigerator Temp', 'Humidity'])
    
    # Find the longest data list
    max_length = max(len(refrigerator_temp_data), len(humidity_data))

    is_first_date = True
    
    # Merge temperature and humidity 5-minute data
    for i in range(max_length):
        temp_value = round(refrigerator_temp_data[i]['mean'], 2) if i < len(refrigerator_temp_data) else None
        humidity_value = round(humidity_data[i]['mean'], 2) if i < len(humidity_data) else None
        timestamp = (refrigerator_temp_data[i]['timestamp'] if i < len(refrigerator_temp_data) 
                     else humidity_data[i]['timestamp'])
        
        row = [
            first_plate_number if is_first_date else '' , 
            timestamp,
            temp_value,
            humidity_value
        ]
        combined_5min_sheet.append(row)
        is_first_date = False
    
    # Daily Average Sheet
    combined_daily_avg_sheet.append([
        'Date', 
        'Refrigerator Temp Avg', 'Refrigerator Temp Min', 'Refrigerator Temp Max',
        'Humidity Avg', 'Humidity Min', 'Humidity Max'
    ])
    
    # Find the longest daily average list
    max_daily_length = max(len(refrigerator_temp_daily_avg), len(humidity_daily_avg))
    
    # Merge temperature and humidity daily averages
    for i in range(max_daily_length):
        # Temperature daily average data
        temp_date = refrigerator_temp_daily_avg[i]['date'] if i < len(refrigerator_temp_daily_avg) else None
        temp_avg = round(refrigerator_temp_daily_avg[i]['average'], 2) if i < len(refrigerator_temp_daily_avg) else None
        temp_min = round(refrigerator_temp_daily_avg[i]['minimum'], 2) if i < len(refrigerator_temp_daily_avg) else None
        temp_max = round(refrigerator_temp_daily_avg[i]['maximum'], 2) if i < len(refrigerator_temp_daily_avg) else None
        
        # Humidity daily average data
        humidity_avg = round(humidity_daily_avg[i]['average'], 2) if i < len(humidity_daily_avg) else None
        humidity_min = round(humidity_daily_avg[i]['minimum'], 2) if i < len(humidity_daily_avg) else None
        humidity_max = round(humidity_daily_avg[i]['maximum'], 2) if i < len(humidity_daily_avg) else None
        
        row = [
            temp_date,
            temp_avg, temp_min, temp_max,
            humidity_avg, humidity_min, humidity_max
        ]
        combined_daily_avg_sheet.append(row)
    
    # Apply borders and alignment
    for sheet in [combined_5min_sheet, combined_daily_avg_sheet]:
        for row_cells in sheet.iter_rows(min_row=1, max_row=sheet.max_row, min_col=1, max_col=sheet.max_column):
            for cell in row_cells:
                cell.border = border
                cell.alignment = alignment
        
        # Adjust column widths
        for col in range(1, sheet.max_column + 1):
            max_length = 0
            for row in sheet.iter_rows(min_col=col, max_col=col):
                for cell in row:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
            adjusted_width = (max_length + 2)
            sheet.column_dimensions[chr(64 + col)].width = adjusted_width
    
    # Save to Excel
    workbook.save(output_file)
    return output_file

def send_email_with_attachment(sender_email, sender_password, receiver_emails, 
                             subject, message, attachment_path):
    """
    Send an email with an Excel file attachment to multiple recipients.
    
    :param sender_email: Sender's email address
    :param sender_password: Sender's email password
    :param receiver_emails: List of recipient email addresses or a single email address
    :param subject: Email subject
    :param message: Email body text
    :param attachment_path: Path to the Excel file to attach
    """
    # Convert single email to list if necessary
    if isinstance(receiver_emails, str):
        receiver_emails = [receiver_emails]
    
    # Remove any empty strings and whitespace
    receiver_emails = [email.strip() for email in receiver_emails if email.strip()]
    
    if not receiver_emails:
        print("No valid receiver emails provided")
        return False
    
    try:
        # Create email message
        email_message = MIMEMultipart()
        email_message['From'] = sender_email
        email_message['To'] = ', '.join(receiver_emails)  # Join all recipients with commas
        email_message['Subject'] = subject

        # Attach message body
        email_message.attach(MIMEText(message, 'plain'))

        # Attach Excel file
        with open(attachment_path, 'rb') as file:
            part = MIMEApplication(file.read(), Name=os.path.basename(attachment_path))
            part['Content-Disposition'] = f'attachment; filename="{os.path.basename(attachment_path)}"'
            email_message.attach(part)

        # Send email
        with smtplib.SMTP("smtp.gmail.com", 587) as server:
            server.starttls()
            server.login(sender_email, sender_password)
            server.send_message(email_message)
        
        print(f"Email sent successfully to: {', '.join(receiver_emails)}")
        return True
    except Exception as e:
        print(f"Error sending email: {e}")
        return False

def main():
    """
    Main function to automate GPS sensor data report generation and email.
    """
    # Update required keys to check for RECEIVER_EMAILS instead of RECEIVER_EMAIL
    required_keys = ['GPS_API_KEY', 'GPS_DEVICE_ID', 'SENDER_EMAIL', 'SENDER_PASSWORD', 'RECEIVER_EMAILS']
    for key in required_keys:
        if not CONFIG[key]:
            print(f"Error: {key} environment variable is not set.")
            sys.exit(1)

    # Additional check for empty receiver emails list
    if not any(CONFIG['RECEIVER_EMAILS']):
        print("Error: No valid receiver email addresses provided.")
        sys.exit(1)
    
    # Determine date range with 8-hour subtraction
    end_date = datetime.now().replace(hour=16, minute=0, second=0, microsecond=0) - timedelta(days=1)
    start_date = (end_date - timedelta(days=1)).strftime('%Y-%m-%d %H:%M')
    end_date = end_date.strftime('%Y-%m-%d %H:%M')
    
    # Construct API URL
    api_url = construct_api_url(
        CONFIG['GPS_DEVICE_ID'], 
        start_date, 
        end_date, 
        CONFIG['GPS_API_KEY']
    )
    
    # Fetch GPS data
    json_data = fetch_gps_data(api_url)
    
    if not json_data:
        print("No GPS data retrieved.")
        return
    
    # Parse sensor data
    sensor_data = parse_gps_temp_humidity(json_data)
    
    # Process temperature and humidity data
    refrigerator_temp_data = process_sensor_data(sensor_data['refrigerator_temp'])
    humidity_data = process_sensor_data(sensor_data['humidity'])
    
    # Calculate daily averages
    refrigerator_temp_daily_avg = calculate_daily_averages(refrigerator_temp_data)
    humidity_daily_avg = calculate_daily_averages(humidity_data)
    
    # Generate Excel report
    report_file = export_to_excel(
        refrigerator_temp_data, 
        humidity_data, 
        refrigerator_temp_daily_avg, 
        humidity_daily_avg,
        sensor_data['plate_numbers']
    )
    
    # Send email with report to all recipients
    send_email_with_attachment(
        CONFIG['SENDER_EMAIL'], 
        CONFIG['SENDER_PASSWORD'], 
        CONFIG['RECEIVER_EMAILS'], 
        f"GPS Sensor Report - {end_date}", 
        f"Attached is the GPS sensor report from {start_date} to {end_date}.", 
        report_file
    )

if __name__ == "__main__":
    main()
