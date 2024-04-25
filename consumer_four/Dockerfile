# Use the official Python base image
FROM python:3.9

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install the dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code into the container
COPY read.py .

# Expose the port for the HTTP server
EXPOSE 8084

# Start the application
CMD ["python", "read.py"]
