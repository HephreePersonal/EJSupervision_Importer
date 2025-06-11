import os
import sys
import subprocess
import tkinter as tk
from tkinter import messagebox

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
        self._create_widgets()

    def _create_widgets(self):
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

        tk.Label(self, text="Select scripts to run:").grid(
            row=len(fields), column=0, columnspan=2, pady=(10, 0), sticky="w"
        )
        self.script_vars = {}
        for idx, (label, path) in enumerate(SCRIPTS):
            var = tk.BooleanVar(value=False)
            cb = tk.Checkbutton(self, text=label, variable=var)
            cb.grid(row=len(fields)+1+idx, column=0, columnspan=2, sticky="w", padx=20)
            self.script_vars[path] = var

        run_btn = tk.Button(self, text="Run", command=self.run_scripts)
        run_btn.grid(row=len(fields)+1+len(SCRIPTS), column=0, columnspan=2, pady=10)

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

    def run_scripts(self):
        conn_str = self._build_conn_str()
        if not conn_str:
            messagebox.showerror("Error", "Please provide connection details")
            return
        os.environ["MSSQL_TARGET_CONN_STR"] = conn_str
        for path, var in self.script_vars.items():
            if var.get():
                subprocess.run([sys.executable, path], check=False)
        messagebox.showinfo("Done", "Selected scripts have finished running.")

if __name__ == "__main__":
    App().mainloop()
