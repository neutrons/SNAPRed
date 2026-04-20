#
# Test if we can successfully connect to SNAP's live-data listener.
#
import socket

SNAP_ADARA_SERVER = ("bl3-daq1.sns.gov", 31415)
#1. Create the socket (AF_INET = IPv4, SOCK_STREAM = TCP)
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    # 2. Connect to the ADARA server
    s.connect(SNAP_ADARA_SERVER)
    
    print(f"SUCCESSFULLY connected to {SNAP_ADARA_SERVER}")
