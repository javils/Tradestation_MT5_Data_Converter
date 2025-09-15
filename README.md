# TradeStation -> MetaTrader (M1) Converter

Small Tkinter GUI to convert TradeStation .txt data files to the format expected by MetaTrader (M1).
It shifts each bar’s timestamp from close (TS) to open (MT) by subtracting 1 minute.
Date stays MM/DD/YYYY; time stays HH:MM. Output is saved next to the input with suffix _MT.

This could be use to import data into StrategyQuant (SQX) and use the Metatrader engine instead of Multicharts engine.

## Features

* Input: comma-separated TXT from TradeStation.

* Output: same layout and date/time format; filename suffixed with _MT.

* No external dependencies (Python standard library only).

## Requirements

* Windows (recommended).

* Python 3.8+.

## Run the script
`python main.py`


A window will open. Select your TradeStation TXT and click Convert.

Tip: Double-clicking main.py also works if .py files are associated with Python.

## Build a standalone .exe with PyInstaller

1) Install PyInstaller:

`pip install pyinstaller`


2) Build the GUI executable (no console window):

`pyinstaller --onefile --windowed --name TS_to_MT main.py`


3) The executable will be created at:

`dist/TS_to_MT.exe`

# Author
Javier Luque Sanabria