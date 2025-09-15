#!/usr/bin/env python3
import csv
import os
import threading
import time
import tkinter as tk
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from tkinter import filedialog, messagebox, ttk
from typing import Callable, TextIO



def process_row(row: list[str]) -> list[str]:
    month, day, year = row[0].split("/")
    hour, minute = row[1].split(":")
    date_time = datetime(int(year), int(month), int(day), int(hour), int(minute))
    date_time = date_time - timedelta(minutes=1)

    row[0] = f"{date_time.month:02d}/{date_time.day:02d}/{date_time.year:04d}"
    row[1] = f"{date_time.hour:02d}:{date_time.minute:02d}"

    return row


def autotune(
        input_file: TextIO,
        reader,
        workers: int
):
    prefetch_out = []

    initial_time = time.perf_counter()
    initial_pos = input_file.buffer.tell()

    for i in range(5000):
        row = next(reader)
        prefetch_out.append(process_row(row))

    final_pos = input_file.buffer.tell()
    total_time = time.perf_counter() - initial_time
    bytes_read = final_pos - initial_pos
    bytes_per_row = bytes_read / len(prefetch_out)
    rows_per_second = len(prefetch_out) / total_time

    chunk_rows = max(500, min(20000, int(rows_per_second * 0.10)))
    mem_limit_bytes = 64 * 1024 * 1024
    batch_by_formula = chunk_rows * max(1, workers) * 4
    batch_by_mem = max(5000, min(200000, int(mem_limit_bytes / bytes_per_row)))
    batch_rows = max(chunk_rows, min(batch_by_formula, batch_by_mem))

    return prefetch_out, chunk_rows, batch_rows


def convert_csv_minus_one_minute(
        input_path: str,
        progress_callback: Callable[[float], None]
) -> str:
    source_path = Path(input_path)
    result_path = source_path.with_name(source_path.stem + "_MT" + source_path.suffix)
    total_bytes = os.path.getsize(source_path)
    workers = os.cpu_count()

    if not source_path.exists():
        raise FileNotFoundError(f"File not exist.: {source_path}")

    with (open(source_path, "r", encoding="utf-8", newline="") as input_file,
          open(result_path, "w", encoding="utf-8", newline="") as result_file):
        reader = csv.reader(input_file)
        writer = csv.writer(result_file)

        header = next(reader)
        writer.writerow(header)

        prefetch_out, chunk_size, batch_rows = autotune(input_file, reader, workers)

        for row in prefetch_out:
            writer.writerow(row)

        last_pos = input_file.buffer.tell()

        progress_callback(last_pos / total_bytes * 100)

        process = partial(process_row)

        with ProcessPoolExecutor(max_workers=workers) as process_pool:
            batch = []

            for row in reader:
                batch.append(row)

                if len(batch) >= batch_rows:
                    for new_row in process_pool.map(process, batch, chunksize=chunk_size):
                        writer.writerow(new_row)
                    batch.clear()
                    progress_callback(input_file.buffer.tell() / total_bytes * 100)

            if batch:
                for new_row in process_pool.map(process, batch, chunksize=chunk_size):
                    writer.writerow(new_row)
                progress_callback(input_file.buffer.tell() / total_bytes * 100)

    return str(result_path)


def browse(in_file_var: tk.StringVar):
    path = filedialog.askopenfilename(title="Select file", filetypes=[("Text", "*.txt")])
    if path:
        in_file_var.set(path)


def make_conversion(
        path: str,
        root: tk.Tk,
        on_progress: Callable[[float], None],
        on_complete: Callable[[], None],
):
    convert_csv_minus_one_minute(path, on_progress)
    root.after(0, on_complete)


def build_ui():
    root = tk.Tk()
    in_file_var = tk.StringVar()
    in_date_var = tk.StringVar(value="mm/dd/yyyy")
    in_time_var = tk.StringVar(value="hh:mm")
    out_date_var = tk.StringVar(value="mm/dd/yyyy")
    out_time_var = tk.StringVar(value="hh:mm")

    root.title("Tradestation to MT data converter")

    try:
        root.call("tk", "scaling", 1.2)
    except Exception:
        pass

    main = ttk.Frame(root, padding=12)
    main.grid(row=0, column=0, sticky="nsew")

    root.columnconfigure(0, weight=1)
    root.rowconfigure(0, weight=1)
    main.columnconfigure(1, weight=1)

    ttk.Label(main, text="Tradestation data file:").grid(row=0, column=0, sticky="e", padx=(0, 8), pady=(0, 8))
    entry_file = ttk.Entry(main, textvariable=in_file_var)
    entry_file.grid(row=0, column=1, sticky="ew", pady=(0, 8))
    ttk.Button(main, text="Browse…", command=lambda: browse(in_file_var)).grid(row=0, column=2, padx=(8, 0), pady=(0, 8))

    pb = ttk.Progressbar(main, mode="determinate", maximum=100.0)
    pb.grid(row=1, column=0, columnspan=3, sticky="ew", pady=(10, 0))

    btn = ttk.Button(main, text="Convert")
    btn.grid(row=2, column=0, columnspan=3, pady=8)
    btn.focus_set()

    def on_progress(percent: int):
        root.after(0, lambda p=percent: pb.configure(value=p))

    def on_complete():
        root.after(0)
        btn.config(state="normal")
        pb.configure(value=100.0)
        messagebox.showinfo("Success!", f"Conversion finished!\n")

    def on_convert_button_clicked():
        path = in_file_var.get().strip()

        if not path:
            messagebox.showwarning("Not file selected", "Please, select a valid tradestation data file.")
            return

        btn.config(state="disabled")
        pb["value"] = 0.0
        args = (path, root, on_progress, on_complete)
        threading.Thread(target=make_conversion, daemon=True, name="convert-thread", args=args).start()

    btn.config(command=on_convert_button_clicked)
    root.bind("<Return>", lambda e: on_convert_button_clicked())

    root.update_idletasks()
    w = root.winfo_width()
    h = root.winfo_height()
    x = (root.winfo_screenwidth() - w) // 2
    y = (root.winfo_screenheight() - h) // 3
    root.geometry(f"{w}x{h}+{x}+{y}")
    root.resizable(False, False)

    root.mainloop()


def main():
    build_ui()


if __name__ == "__main__":
    import multiprocessing as mp

    mp.freeze_support()
    main()
