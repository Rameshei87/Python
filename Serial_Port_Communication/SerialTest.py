import serial

ser = 0


# function to initialise serial port

def init_serial():
    COMNUM = 1
    global ser
    ser = serial.Serial()
    ser.baudrate = 9600
    # ser.port = COMNUM - 1  # comport Name start from 0

    ser.port = 'dev/ttyUSB0'  # If using Linux

    # Specify the timeouts in secs - so that SerialPort doesnt hangs

    ser.timeout = 10
    ser.open()  # Opens Serial Port

    if ser.isopen():
        print('open: ' + ser.portstr)


# Function ends here

init_serial()

temp = raw_input('Type The Alphabet A S D F G H J K : \r\n')
ser.write(temp)

while 1:
    bytes = ser.readline()  # Read from the serial Port
    print('you sent ' + bytes)
