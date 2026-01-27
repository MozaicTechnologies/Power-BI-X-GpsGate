#!/usr/bin/env python
"""
Quick script to detect Render's outbound IP address
Run this on Render to get the IP that GpsGate sees
"""

import requests

def get_outbound_ip():
    """Get the outbound IP address that external services see"""
    
    services = [
        'https://api.ipify.org?format=json',
        'https://ipinfo.io/json',
        'https://api.my-ip.io/ip.json',
        'https://ifconfig.me/all.json'
    ]
    
    for service in services:
        try:
            response = requests.get(service, timeout=10)
            if response.ok:
                data = response.json()
                
                # Different services use different keys
                ip = (
                    data.get('ip') or 
                    data.get('IP') or 
                    data.get('ipAddress') or
                    str(data)
                )
                
                print(f"\n{'='*60}")
                print(f"Outbound IP Address Detection")
                print(f"{'='*60}")
                print(f"\nService: {service}")
                print(f"IP Address: {ip}")
                print(f"\nFull Response:")
                print(data)
                print(f"\n{'='*60}")
                
                return ip
                
        except Exception as e:
            print(f"Failed to get IP from {service}: {e}")
            continue
    
    print("\n❌ Could not detect outbound IP from any service")
    return None

if __name__ == "__main__":
    ip = get_outbound_ip()
    
    if ip:
        print(f"\n✅ Your Render server's outbound IP: {ip}")
        print(f"\nProvide this IP to GpsGate administrator for whitelisting.")
    else:
        print("\n⚠️  Could not determine IP. Check internet connectivity.")
