import boto3
import folium
from math import sin, cos, sqrt, atan2, radians
import datetime
import pytz   

cli = boto3.client('s3')
radius = 2


def gps_one(lat, lon):
    mapit = folium.Map(location=[lat, lon], zoom_start=15)
    folium.Marker(location=[lat, lon],
                  popup='OBD Initial Location (ID): <br> 65615765277',
                  fill_color='#43d9de',
                  tooltip="Initial location",
                  radius=radius).add_to(mapit)
    folium.Circle([lat, lon],
                  radius=radius,
                  fill=True,
                  ).add_to(mapit)
                  
    # mapit.save('OBD-Project/GeoMaps/current_0_location.html')
    # HTML('<iframe src=OBD-Project/GeoMaps/current_0_location.html width=700 height=450></ifrme>')


def gps_main(lat, lon, lat_live, lon_live):
    # approximate radius of earth in km
    R = 6373.0
    # ------------------------------------------------
    lat1 = radians(float(lat))
    lon1 = radians(float(lon))
    # ------------------------------------------------
    print("Initial Device Latitude : ", lat)
    print("Initial Device Longitude: ", lon)

    lat2 = radians(float(lat_live))
    lon2 = radians(float(lon_live))
    # ------------------------------------------------

    dlon = lon2 - lon1
    dlat = lat2 - lat1

    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    distance = R * c

    print("Result:")
    print("In KM.   : ", distance, "km")
    print("In meters: ", distance * 1000, "m")
    cli = boto3.client('s3')
    cli.put_object(
        Body=str(distance * 1000),
        Bucket='ec2-obd2-bucket',
        Key='GPS/Distance/OBD2--{}.txt'.format(str(datetime.datetime.now())))

    if distance * 1000 <= float(radius):
        print("OBD device is under given area")
    else:
        print("OBD device is NOT under given area")

    map_obd = folium.Map(location=[lat, lon], zoom_start=15)
    folium.Marker(location=[lat, lon], fill_color='#43d9de', radius=radius).add_to(map_obd)
    if distance * 1000 <= float(radius):
        folium.Marker(location=[lat_live, lon_live],
                      popup='OBD Device No: <br> 65615765277',
                      # icon=folium.Icon(icon="cloud", color='green'),
                      tooltip="OBD Device",
                      radius=radius).add_to(map_obd)
    else:
        folium.Marker(location=[lat_live, lon_live],
                      popup='OBD Device No: <br> 65615765277',
                      # icon=folium.Icon(icon="cloud", color='red'),
                      tooltip="OBD Device",
                      radius=radius).add_to(map_obd)

    folium.Circle([lat, lon],
                  radius=radius,
                  fill=True,
                  ).add_to(map_obd)
    map_obd.add_child(folium.LatLngPopup())

    # map_obd.save('OBD-Project/GeoMaps/current_OBD_current_location.html')
    # HTML('<iframe src=OBD-Project/GeoMaps/current_OBD_current_location.html width=700 height=450></ifrme>')


def convert_LOGIN_data(login_data):
    """
    Function that'll convert Raw LOGIN data to Readable JSON Object
    """
    # --------- Headers for final dictionary ---------
    HEADERS = ['Live/Memory', 'Signature', 'IMEI', 'Message Type', 'Sequence No', 'CHECKSUM']

    # Result dictionary
    result = {}

    # --------- Data Processing ---------
    for index, header in enumerate(HEADERS):
        result[header] = login_data[index]

    return result


def convert_GPS_data(gps_data):
    """
    Function that'll convert Raw GPS data to Readable JSON Object
    """

    if len(gps_data) == 23:
        # --------- Headers for final dictionary ---------
        HEADERS = ['Live/Memory', 'Signature', 'IMEI', 'Message Type', 'Sequence No', 'Time (GMT)', 'Date',
                   'valid/invalid', 'Latitude', 'Longitude', 'Speed (knots)', 'Angle of motion', 'Odometer (KM)',
                   'Internal battery Level (Volts)', 'Signal Strength', 'Mobile country code', 'Mobile network code',
                   'Cell id', 'Location area code',
                   '#Ignition(0/1), RESERVED ,Harsh Braking / Acceleration//Non(0/2/3),Main power status(0/1)',
                   'Over speeding', 'Signature', 'CHECKSUM']

        # Result dictionary
        result = {}

        # --------- Data Processing ---------
        for index, header in enumerate(HEADERS):
            result[header] = gps_data[index]

        return result

    else:
        # --------- Headers for final dictionary ---------
        HEADERS = ['Live/Memory', 'Signature', 'IMEI', 'Message Type', 'Sequence No', 'Time (GMT)', 'Date',
                   'valid/invalid', 'Latitude', 'North/South', 'Longitude', 'East/West', 'Speed (knots)',
                   'Angle of motion', 'Odometer (KM)', 'Internal battery Level (Volts)', 'Signal Strength',
                   'Mobile country code', 'Mobile network code', 'Cell id', 'Location area code',
                   '#Ignition(0/1), RESERVED ,Harsh Braking / Acceleration//Non(0/2/3),Main power status(0/1)',
                   'Over speeding', 'Signature', 'CHECKSUM']

        # Result dictionary
        result = {}

        # --------- Data Processing ---------
        for index, header in enumerate(HEADERS):
            result[header] = gps_data[index]

        return result


def convert_OBD_data(obd_data):
    """
    Function that'll convert Raw OBD data to Readable JSON Object
    """

    # --------- Headers for final dictionary ---------
    HEADERS = ['Live/Memory', 'Signature', 'IMEI', 'Message Type', 'Sequence No', 'Time (GMT)', 'Date', 'OBD Protocol']

    # Result dictionary
    result = {}

    # --------- Data processing ---------
    first_half_raw = obd_data[:8]
    second_half_raw = obd_data[8:-1]

    # --------- First Half Data Processing ---------
    for index, header in enumerate(HEADERS):
        result[header] = first_half_raw[index]

    # --------- Second Half Data Processing ---------
    for pid in second_half_raw:
        i = pid.split(':')
        if len(i) > 1:
            result[i[0]] = i[1]

    return result


def convert_raw_to_information(input_data):
    """
    Function that'll convert Raw input from OBD to formatted dictionary containing all the information
    needed for the UI
    """
    IST = pytz.timezone('Asia/Kolkata') 
    dateTimeIND = datetime.datetime.now(IST).strftime("%Y-%m-%dT%H:%M:%S.%f")
    # --------- Data decoding from byte to str ---------
    input_file = input_data.decode("UTF-8", errors='ignore')

    # --------- Data splitting based on comma ---------
    input_file = input_file.replace(';', ',')
    raw_data = input_file.split(',')

    # --------- Check for Login packet ---------
    if len(raw_data) < 8:
        login_data = convert_LOGIN_data(raw_data)
        IMEI = login_data["IMEI"]
        # S3 Log Login Data
        cli.put_object(
            Body=str(login_data),
            Bucket='ec2-obd2-bucket',
            Key='{0}/Login/Log/OBD2--{1}.txt'.format(IMEI,str(dateTimeIND)))
        
        # S3 Latest Login Data
        cli.put_object(
            Body=str(login_data),
            Bucket='ec2-obd2-bucket',
            Key='{0}/Login/Latest/login.txt'.format(IMEI))
        return login_data

    # --------- GPS Data ---------
    elif raw_data[1] == "ATL":
        gps_data = convert_GPS_data(raw_data)
        IMEI = gps_data["IMEI"]
        # S3 Log GPS Data
        cli.put_object(
            Body=str(gps_data),
            Bucket='ec2-obd2-bucket',
            Key='{0}/GPS/Log/OBD2--{1}.txt'.format(IMEI,str(dateTimeIND)))

        if raw_data[0] == "L":     
            # S3 Latest GPS 'L' Data
            cli.put_object(
                Body=str(gps_data),
                Bucket='ec2-obd2-bucket',
                Key='{0}/GPS/Latest/L.txt'.format(IMEI))
            
        elif raw_data[0] == "H":
             # S3 Latest GPS 'H' Data
            cli.put_object(
                Body=str(gps_data),
                Bucket='ec2-obd2-bucket',
                Key='{0}/GPS/Latest/H.txt'.format(IMEI))
        return gps_data

    # --------- OBD Data ---------
    elif raw_data[1] == "ATLOBD":
        obd_data = convert_OBD_data(raw_data)
        rpm = calculate_engine_RPM(obd_data)
        IMEI = obd_data["IMEI"]
        
        if rpm:
            print(f'Engine RPM = {rpm}')
            rpmdata = { 'RPM' : rpm, 'IMEI': IMEI,'timestamp' : dateTimeIND}  
            cli.put_object(
                Body=str(rpmdata),
                Bucket='ec2-obd2-bucket',
                Key='{0}/Data/{0}_rpm.txt'.format(IMEI))
        else:
            print("No RPM data received")
        
        # S3 RPM Data
        cli.put_object(
                Body=str(rpm),
                Bucket='ec2-obd2-bucket',
                Key='{0}/OBD/Latest/RPM.txt'.format(IMEI))
        # S3 Log OBD Data
        cli.put_object(
            Body=str(obd_data),
            Bucket='ec2-obd2-bucket',
            Key='{0}/OBD/Log/OBD2--{1}.txt'.format(IMEI,str(dateTimeIND)))

        if raw_data[0] == "L":     
            # S3 Latest OBD 'L' Data
            cli.put_object(
                Body=str(obd_data),
                Bucket='ec2-obd2-bucket',
                Key='{0}/OBD/Latest/L.txt'.format(IMEI))
            
        elif raw_data[0] == "H":
             # S3 Latest GPS 'H' Data
            cli.put_object(
                Body=str(obd_data),
                Bucket='ec2-obd2-bucket',
                Key='{0}/OBD/Latest/H.txt'.format(IMEI))

        return obd_data
    # -----------------------------------

def calculate_engine_RPM(obd_data:dict):
    '''
    This method takes a JSON/Dictionary as input. The PID for engine RPM is 010C. The HEX value is split in two
    namely A and B.
    
    For Example
    
    let,
    OBD_HEX_VALUE = 541B
    
    A = 54(hex) = 84(dec)
    B = 1B(hex) = 27(dec)
    
    Using Formula
    rpm = ( ( A * 256 ) + B ) / 4
    
    Result RPM
    5382.75
    
    '''
    if not obd_data['010C'] == "XXXX":
        # Getting RPM from OBD Data and splitting it into two
        rpm_A = obd_data['010C'][0:2]
        rpm_B = obd_data['010C'][2:]
        
        # Converting Hex to Integer
        converted_decimal_A = int(rpm_A, 16)
        converted_decimal_B = int(rpm_B, 16)
        
        # Formula for conversion to RPM
        RPM = ((converted_decimal_A * 256) + converted_decimal_B)/4
        print(f'Engine RPM {RPM}')
        
        return RPM
