import os
import sys
import subprocess
import tkinter as tk
from tkinter import messagebox, scrolledtext
import pyodbc

SCRIPTS = [
    ("Justice DB Import", "01_JusticeDB_Import.py"),
    ("Operations DB Import", "02_OperationsDB_Import.py"),
    ("Financial DB Import", "03_FinancialDB_Import.py"),
    ("LOB Column Processing", "04_LOBColumns.py"),
]

class App(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("EJ Supervision Importer")
        self.resizable(False, False)
        self.conn_str = None
        self._create_connection_widgets()

    def _create_connection_widgets(self):
        fields = ["Driver", "Server", "Database", "User", "Password"]
        self.entries = {}
        for i, field in enumerate(fields):
            lbl = tk.Label(self, text=field+":")
            lbl.grid(row=i, column=0, sticky="e", padx=5, pady=2)
            ent = tk.Entry(self, width=40)
            if field.lower() == "password":
                ent.config(show="*")
            ent.grid(row=i, column=1, padx=5, pady=2)
            self.entries[field.lower()] = ent

        test_btn = tk.Button(self, text="Test Connection", command=self.test_connection)
        test_btn.grid(row=len(fields), column=0, columnspan=2, pady=10)

    def _show_script_widgets(self):
        if hasattr(self, "script_frame"):
            return

        self.script_frame = tk.Frame(self)
        start_row = len(self.entries) + 1
        self.script_frame.grid(row=start_row, column=0, columnspan=2, sticky="nsew")

        for idx, (label, path) in enumerate(sorted(SCRIPTS, key=lambda x: x[1])):
            tk.Label(self.script_frame, text=path).grid(row=idx, column=0, sticky="w", padx=5, pady=2)
            tk.Button(
                self.script_frame,
                text="Run",
                command=lambda p=path: self.run_script(p)
            ).grid(row=idx, column=1, padx=5, pady=2)

        self.output_text = scrolledtext.ScrolledText(self.script_frame, width=80, height=20)
        self.output_text.grid(row=len(SCRIPTS), column=0, columnspan=2, pady=(10, 0))

    def _build_conn_str(self):
        driver = self.entries["driver"].get() or "{ODBC Driver 17 for SQL Server}"
        server = self.entries["server"].get()
        database = self.entries["database"].get()
        user = self.entries["user"].get()
        password = self.entries["password"].get()

        parts = [f"DRIVER={driver}", f"SERVER={server}"]
        if database:
            parts.append(f"DATABASE={database}")
        if user:
            parts.append(f"UID={user}")
        if password:
            parts.append(f"PWD={password}")
        return ";".join(parts)

    def test_connection(self):
        conn_str = self._build_conn_str()
        if not conn_str:
            messagebox.showerror("Error", "Please provide connection details")
            return
        try:
            pyodbc.connect(conn_str, timeout=5)
        except Exception as exc:
            messagebox.showerror("Connection Failed", str(exc))
            return

        messagebox.showinfo("Success", "Connection successful!")
        self.conn_str = conn_str
        os.environ["MSSQL_TARGET_CONN_STR"] = conn_str
        self._show_script_widgets()

    def run_script(self, path):
        if not self.conn_str:
            messagebox.showerror("Error", "Please test the connection first")
            return
        self.output_text.insert(tk.END, f"Running {path}...\n")
        self.output_text.see(tk.END)
        try:
            result = subprocess.run([
                sys.executable,
                path
            ], capture_output=True, text=True)
            if result.stdout:
                self.output_text.insert(tk.END, result.stdout)
            if result.stderr:
                self.output_text.insert(tk.END, result.stderr)
        except Exception as exc:
            self.output_text.insert(tk.END, f"Error running {path}: {exc}\n")
        self.output_text.insert(tk.END, f"Finished {path}\n\n")
        self.output_text.see(tk.END)

if __name__ == "__main__":
    App().mainloop()
