#!/usr/bin/env python3
import csv
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, ttk

root = tk.Tk()
in_file_var = tk.StringVar()
in_date_var = tk.StringVar(value="mm/dd/yyyy")
in_time_var = tk.StringVar(value="hh:mm")
out_date_var = tk.StringVar(value="mm/dd/yyyy")
out_time_var = tk.StringVar(value="hh:mm")


def convert_csv_minus_one_minute(
        input_path: str,
        in_date_fmt: str = "%m/%d/%Y",
        in_time_fmt: str = "%H:%M",
        out_date_fmt: str = "%m/%d/%Y",
        out_time_fmt: str = "%H:%M",
) -> str:
    source_path = Path(input_path)
    if not source_path.exists():
        raise FileNotFoundError(f"File not exist.: {source_path}")

    result_path = source_path.with_name(source_path.stem + "_MT" + source_path.suffix)

    with open(source_path, "r", encoding="utf-8", newline="") as input_file, open(result_path, "w", encoding="utf-8",
                                                                                  newline="") as result_file:
        reader = csv.reader(input_file)
        writer = csv.writer(result_file, lineterminator="\n")

        header = next(reader)
        writer.writerow(header)

        for row in reader:
            if len(row) < 2:
                continue

            date_time = datetime.strptime(f"{row[0].strip()} {row[1].strip()}", f"{in_date_fmt} {in_time_fmt}")
            date_time = date_time - timedelta(minutes=1)

            row[0] = date_time.strftime(out_date_fmt)
            row[1] = date_time.strftime(out_time_fmt)
            writer.writerow(row)

    return str(result_path)


def normalize_date_format(date_format: str) -> str:
    date_format = date_format.strip()
    date_format = date_format.replace("mm", "%m").replace("dd", "%d").replace("yyyy", "%Y")
    date_format = date_format.replace("MM", "%m").replace("DD", "%d").replace("YYYY", "%Y")
    return date_format


def normalize_time_format(date_format: str) -> str:
    date_format = date_format.strip()
    date_format = date_format.replace("hh", "%H").replace("HH", "%H").replace("MM", "%M").replace("mm", "%M")
    return date_format


def browse():
    path = filedialog.askopenfilename(title="Select file", filetypes=[("Text", "*.txt")])
    if path:
        in_file_var.set(path)


def make_conversion():
    try:
        path = in_file_var.get().strip()
        if not path:
            messagebox.showwarning("Not file selected", "Please, select a valid tradestation data file.")
            return

        in_date_format = normalize_date_format(in_date_var.get() or "mm/dd/yyyy")
        in_time_format = normalize_time_format(in_time_var.get() or "hh:mm")
        out_date_format = normalize_date_format(out_date_var.get() or "mm/dd/yyyy")
        out_time_format = normalize_time_format(out_time_var.get() or "hh:mm")

        out_path = convert_csv_minus_one_minute(path, in_date_format, in_time_format, out_date_format, out_time_format)
        messagebox.showinfo("Success!", f"Conversion finished: \n{out_path}")
    except Exception as e:
        messagebox.showerror("Error", str(e))


def build_ui():
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
    ttk.Button(main, text="Browse…", command=browse).grid(row=0, column=2, padx=(8, 0), pady=(0, 8))

    formats = ttk.Frame(main)
    formats.grid(row=1, column=0, columnspan=3, sticky="nsew")
    formats.columnconfigure(0, weight=1)
    formats.columnconfigure(1, weight=1)

    lf_in = ttk.LabelFrame(formats, text="Input formats", padding=10)
    lf_out = ttk.LabelFrame(formats, text="Output formats", padding=10)
    lf_in.grid(row=0, column=0, sticky="nsew", padx=(0, 6))
    lf_out.grid(row=0, column=1, sticky="nsew", padx=(6, 0))

    ttk.Label(lf_in, text="Date:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
    ttk.Entry(lf_in, textvariable=in_date_var, width=14).grid(row=0, column=1, sticky="w", pady=4)

    ttk.Label(lf_in, text="Time:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
    ttk.Entry(lf_in, textvariable=in_time_var, width=10).grid(row=1, column=1, sticky="w", pady=4)

    ttk.Label(lf_out, text="Date:").grid(row=0, column=0, sticky="e", padx=(0, 6), pady=4)
    ttk.Entry(lf_out, textvariable=out_date_var, width=14).grid(row=0, column=1, sticky="w", pady=4)

    ttk.Label(lf_out, text="Time:").grid(row=1, column=0, sticky="e", padx=(0, 6), pady=4)
    ttk.Entry(lf_out, textvariable=out_time_var, width=10).grid(row=1, column=1, sticky="w", pady=4)

    hint = ttk.Label(main, text="Examples: mm/dd/yyyy · dd/mm/yyyy · hh:mm", foreground="#555")
    hint.grid(row=2, column=0, columnspan=3, sticky="w", pady=(8, 0))

    btn = ttk.Button(main, text="Convert", command=make_conversion)
    btn.grid(row=3, column=0, columnspan=3, pady=8)
    btn.focus_set()

    root.bind("<Return>", lambda e: make_conversion())

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
    main()
