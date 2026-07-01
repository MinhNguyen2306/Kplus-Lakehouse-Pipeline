import os
import subprocess
import sys

files = ["20220403", "20220404"]
rows  = ["10", "10"]

for date_input, n_rows in zip(files, rows):
    print(f"\n{'='*50}")
    print(f"Processing {date_input} - {n_rows} rows")
    print(f"{'='*50}")
    
    for script in ["bronze_ingest.py", "silver_ingest.py", "gold_ingest.py", "export_data.py"]:
        print(f"\n--- Running {script} ---")
        
        if script == "bronze_ingest.py":
            # bronze cần input từ user → dùng stdin
            process = subprocess.run(
                [sys.executable, script],
                input=f"{date_input}\n{n_rows}\n",
                text=True,
                cwd=r"C:\Users\admin\Desktop\log_search"
            )
        else:
            process = subprocess.run(
                [sys.executable, script],
                cwd=r"C:\Users\admin\Desktop\log_search"
            )
        
        if process.returncode != 0:
            print(f"❌ {script} failed for {date_input}!")
            break
        
        print(f"✅ {script} done")

print("\n🎉 Pipeline finished!")