import platform
import os

def clear_console():
    system = platform.system()
    if system == 'Windows':
        os.system('cls')
    else:
        os.system('clear')