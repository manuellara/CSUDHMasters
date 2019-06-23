
from scapy.all import *
from prettytable import PrettyTable
from collections import Counter
import numpy as np  
import matplotlib.pyplot as plt
import pandas as pd


###################################################################################################################################################

# prints ethernet frame/packet
def printFrame( suppliedPCAP ):
    pcap = suppliedPCAP

    ethernet_frame = pcap[5]                                        # complete frame
    #ip_packet = ethernet_frame.payload                              # IP frame
    #segment = ip_packet.payload                                     # segment frame
    #data = segment.payload                                          # Retrieve payload 
 
    print( ethernet_frame.summary() )                               # output summary of response
    #print( ip_packet.summary() )
    #print( segment.summary() )
    #print( data.summary() ) 

    ethernet_frame.show()                                           #shows entire ethernet frame 

# prints packet info
def packetInfo( suppliedPCAP ):
    pcap = suppliedPCAP

    for i in pcap:                                                      # for every packet in the pcap 
        print( f"Info for packet {pcap.index(i)}:" )

        if IP in i:
            print( f"     source IP :: {i[IP].src} " )
            print( f"     destination IP :: {i[IP].dst} " )
            print( f"     protocol :: {i[IP].proto} " )                 # 17 is UDP , 6 is TCP
            print( f"     source port :: {i[IP].sport} " )
            print( f"     destination port :: {i[IP].dport} \n" )

# created pandas dataframe from pcap 
def pandasFrame( suppliedPCAP ):
    pcap = suppliedPCAP

    srcIP = []                                                   # list for source IPs 
    dstIP = []                                                     # list for destination IPs
    proto = []                                                      # list for protocols
    srcPort = []                                                    # list for source port
    dstPort = []                                                    # list for destination port
    
    for i in pcap:
        if IP in i:
            srcIP.append( i[IP].src )                            # add source IP to list
            dstIP.append( i[IP].dst )                              # add destination IP to list
            proto.append( i[IP].proto )                          # add protocol
            srcPort.append( i[IP].sport )                           # add source port
            dstPort.append( i[IP].dport )                           # add destination port


    packetDetails = {                                               # create dictionary 
        'Source' : srcIP ,
        'Destination' : dstIP ,
        'Protocol' : proto ,
        'Source Port' : srcPort ,
        'Destination Port' : dstPort
    }

    df = pd.DataFrame( packetDetails )

    print( df )

    return df

# prints PRettyTable of most frequent IP
def pTable( suppliedPCAP ):
    pcap = suppliedPCAP

    df = pandasFrame( pcap )

    sourceIP = []                                                   # list for source IPs 
    for i in df.Source:
        sourceIP.append( i )                                # add every source IP to list 

    count = Counter()                                               # creates dictionary 
    for i in sourceIP:
        count[i] += 1                                               # iterates through sourceIP, adds & counts how many times each IP appears 

    table = PrettyTable( ["Source IPs" , "Count"] )                         # table header 
    for ip , cnt in count.most_common():                            # sorts count by most common and inserts a row 
        table.add_row( [ip , cnt] )

    print( table )

    return count

# will plot data using matplotlib
def plot( suppliedPCAP ):
    pcap = suppliedPCAP

    count = pTable( pcap )                                      # ditionary with key + values returned

    ips = []                                                        # create stand alone list for IP
    frequency = []                                                  # create stand alone list for count
    for key, value in count.most_common():
        ips.append( key )
        frequency.append( value )

    placeholder = np.arange( len( ips ) )                                # replaces keys with number value
    
    plt.title("Most frequent source IPs")
    plt.ylabel("Frequency")
    plt.xlabel("IPs")
    plt.xticks( placeholder , ips )                                    # replaces number with corresponding value 
    plt.xticks(rotation=35)

    plt.bar( placeholder , frequency )

    plt.show()                                                      # keeps graph window open until closed 

###################################################################################################################################################


pcap = rdpcap( "http_espn.pcap" )

#printFrame( pcap )

#packetInfo( pcap )

#pandasFrame( pcap )

#pTable( pcap )

#plot( pcap )