# docker build -t echo_test .
#
# docker run -p 5001:5001 echo_test
#
# curl -X POST http://localhost:5001/hello-world -d "Secret Message"

# Use a slim version of Python for a smaller, faster image
FROM python:3.12-slim

# Set the directory inside the container
WORKDIR /app

# Install Flask directly (or use a requirements.txt if you have one)
RUN pip install flask

# Copy your echo_test.py into the container
COPY echo_test.py .

# Expose port 5001 (matching your app's code)
EXPOSE 5001

# Run the app
CMD ["python", "echo_test.py"]