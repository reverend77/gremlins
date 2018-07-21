import socket
import os
import re
import subprocess

s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
s.connect(('google.com', 0))

ip = s.getsockname()[0]

# Get the Local IP
end = re.search('^[\d]{1,3}.[\d]{1,3}.[\d]{1,3}.[\d]{1,3}', ip)

# Chop down the last IP Digits
create_ip = re.search('^[\d]{1,3}.[\d]{1,3}.[\d]{1,3}.', ip)

# Print IP to the user
print("Your IP Address is: " + str(end.group(0)))


# Pinging the IP
def ping(ip):
    response = subprocess.Popen("ping -c 1 " + str(ip), stdout=subprocess.PIPE, shell=True)
    response.wait()
    return response.poll() == 0


# Check IP
def check_loop_back(ip):
    if end.group(0) == '127.0.0.1':
        return True


print("Pinging IP's...")

if check_loop_back(create_ip):
    print("Either your IP is a Loop Back or it does not belong in local IP range")
else:
    for i in range(1, 244):
        ip = create_ip.group(0) + str(i)
        alive = ping(ip)
        if alive:
            print("Found online: " + ip)
        else:
            print("Found offline: " + ip)
