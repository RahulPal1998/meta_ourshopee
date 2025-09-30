# Use a Python base image
FROM python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application code
COPY main.py . 
# Assuming your updated script is named main.py

# Command to run your script when the container starts
CMD ["python", "main.py"]
