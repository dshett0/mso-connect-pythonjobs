from datetime import datetime

def greet_based_on_time():
    # Get the current time
    now = datetime.now()
    hour = now.hour
    
    if 5 <= hour < 12:
        print("Good morning!")
    elif 12 <= hour < 18:
        print("Good afternoon!")
    elif 18 <= hour < 22:
        print("Good evening!")
    else:
        print("Good night!")

# Call the function to print the greeting
greet_based_on_time()