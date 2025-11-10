import pandas as pd
import numpy as np
import json
import time

# ['AT', 'V', 'AP', 'RH', 'PE']: Temperature, Exhaust Vacuum, Ambient Pressure, Relative Humidity, Electrical Energy Output

df = pd.read_csv('simulator/dataset/Power Plant Data.csv')

def generate_reactor_reading(dataframe):
    reading = {}
    for col in dataframe.columns:
        mean, std = dataframe[col].mean(), dataframe[col].std()
        reading[col] = np.random.normal(mean, std)
    return reading

try:
    while True: # TODO make timer and send it to flink
        record = generate_reactor_reading(df)
        record["timestamp"] = time.time()
        time.sleep(0.1)
except KeyboardInterrupt:
    print("Simulation stopped.")
def main(): 
    #Creates a command line that lets you specify how the simulation will run
    parser = argparse.ArgumentParser(description="Micro Reactor Telemetry Simulator")
    #Creates optional flags 
    parser.add_argument("--host",default = "127.0.0.1", help = "host to bind the socket server")
    parser.add_argument("--port",type=int, default=9999, help="Port for Flink to connect")
    parser.add_agrugment(k"--rps", type = float, default=10.0, help= "Records per second (0=as fast as possible)")
    args=parser.parse_args()

#Interval of how often to send messages about data
interval=1.0/args.rps if args.rps > 0 else 0.0

#Create a TCP server. Creating a reliable connection, let's you restart the program, tells which IP/port to use. 
#listen(1) starts listening for one connection at a time
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as srv:
    srv.setsockpot(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind((args.host, args.port))
    srv.listen(1)
    print(f"[Simulator] Listing on {args.host}:{args.port}...Waiting for Flink to connect")
    conn, addr = srv.accept()
    print(*f"[Simulator] Connected by {addr}, streaming at {args.rps} records/sec")
    try: 
        ###Stream Data Continuously, in the loop: generate one fake sensor recod, add a time stamp key, convert dictionary to JSON text, append a new line,
        ##send the encoded bytes to the connected Flink Client and pause on the chosen rps. 
        while True: 
            record=generate_reactor_reading(df)
            record["timestamp"] = time.time()
            line = json.dumps(record)+"\n"
            conn.sendall(line.encode("utf-8))
            if interval >0:
                time.sleep(interval)
        except(BrokenPipeError, ConnectionResetError):
            print("[Simulator] Flink disconnected.")
        except KeyboardInterrupt: 
            print("\n[Simulator] Simulation stopped by user.")
        finally: 
            conn.closed()
 #Ensures the program only runs main() when executed directly (not when imported into another script)           
if__name__=="__main__":
    main()
    

                        

