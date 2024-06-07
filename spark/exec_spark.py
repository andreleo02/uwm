import subprocess

def execute_spark_sh():
    """
    Execute the spark.sh script to run the machine learning python script.

    eseguire con python3 ./spark/exec_spark.py
    """
    try:
        subprocess.run(["chmod", "+x", "./spark/spark.sh"], check=True)
        subprocess.run(["./spark/spark.sh"], check=True)
        print("spark.sh executed successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error executing spark.sh: {e}")

if __name__ == "__main__":
    execute_spark_sh()

