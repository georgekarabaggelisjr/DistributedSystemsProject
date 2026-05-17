# Χρησιμοποιούμε μια ελαφριά έκδοση της Python
FROM python:3.10-slim

# Ορίζουμε τον φάκελο εργασίας μέσα στο container
WORKDIR /app

# Αντιγράφουμε το requirements.txt και εγκαθιστούμε τις εξαρτήσεις
# Το κάνουμε αυτό πριν αντιγράψουμε τον κώδικα για να χρησιμοποιήσουμε το Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Αντιγράφουμε όλο το περιεχόμενο του UI_service στο container
COPY . .

# Ανοίγουμε τη θύρα 8000
EXPOSE 8000

# Εντολή εκκίνησης (προσοχή: χωρίς --reload σε production/kubernetes)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]