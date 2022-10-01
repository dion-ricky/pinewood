from datetime import datetime

import functions_framework

@functions_framework.cloud_event
def main(cloud_event):
    print(f"Executed at {datetime.now()}")