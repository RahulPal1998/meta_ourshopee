# Use official Python slim image
FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Copy dependencies and install
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project code
COPY . .

# Set environment variable for Cloud Run
ENV PORT 8080
EXPOSE 8080

# Start the Flask server
CMD ["python", "main.py"]
