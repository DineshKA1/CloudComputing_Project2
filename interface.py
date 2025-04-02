import tkinter as tk
from tkinter import scrolledtext, messagebox
from preprocessing import get_query_plan
from pipesyntax import convert_to_pipe_syntax

def execute_conversion():
    #handle SQL input, retrieve QEP, convert it to pipe-syntax
    sql_query = sql_input.get("1.0", tk.END).strip()
    if not sql_query:
        messagebox.showerror("Error", "Enter a valid SQL query")
        return
    
    db_config = {
        "dbname": "tpch",
        "user": "postgres",
        "password": "password",
        "host": "localhost",
        "port": "5432"
    }
    
    qep = get_query_plan(sql_query, db_config)
    pipe_syntax = convert_to_pipe_syntax(qep)
    
    qep_output.delete("1.0", tk.END)
    qep_output.insert(tk.END, qep)
    
    pipe_output.delete("1.0", tk.END)
    pipe_output.insert(tk.END, pipe_syntax)

#GUI Setup
root = tk.Tk()
root.title("SQL to Pipe-Syntax Converter")
root.geometry("700x500")

tk.Label(root, text="Enter SQL Query:").pack()
sql_input = scrolledtext.ScrolledText(root, height=5, width=80)
sql_input.pack()

tk.Button(root, text="Convert", command=execute_conversion).pack()

tk.Label(root, text="Query Execution Plan:").pack()
qep_output = scrolledtext.ScrolledText(root, height=10, width=80)
qep_output.pack()

tk.Label(root, text="Pipe-Syntax Output:").pack()
pipe_output = scrolledtext.ScrolledText(root, height=10, width=80)
pipe_output.pack()

root.mainloop()