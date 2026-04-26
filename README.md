# README Containts
This README has 2 main sections. 
* 1. Run the UI_service from a development point of view 
* 2. Run the UI_service as a real service in the kubernetes cluster that we have created.

**Assumptions**:

I assume that you already have the following services running, as seen below:
```bash
***@***:~$ kubectl get pods
NAME                                 READY   STATUS    RESTARTS        AGE
keycloak-0                           1/1     Running   0               2d19h
minio-99d4b5f44-lgv2m                1/1     Running   0               2d23h
postgres-dds-5dd67b4fd-rwwkw         1/1     Running   0               2d23h
postgres-keycloak-84688b88d4-2zbtc   1/1     Running   3 (2d23h ago)   25d
rabbitmq-cb974d587-c69vx             1/1     Running   0               2d22h
```

This README tells you how to run the following services all together (The Manager and the workers are not yet included):
* UI_service
* CLI
* Keycloak
* minIO
* DDS

## Section 1: UI_service for Development

### minIO
**1. Port Forwarding**

In a terminal run:
```bash
kubectl port-forward svc/minio 9000:9000
```

### Keycloak
**1. Realm and User Creation**
Στον browser πήγαινε στο http://keycloak.192.168.49.2.nip.io:8080/admin και κάνε τα ακόλουθα:
* **Δημιουργία Realm**

    1. Πήγαινε πάνω αριστερά που λέει Master και πάτα Create Realm.

    2. Δώσε το όνομα που έχεις βάλει στο .env σου (π.χ. myrealm) και πάτα Create.

* **Δημιουργία Client**

    1. Στο νέο σου Realm, πάτα Clients -> Create client.

        *Client ID*: Βάλε mapreduce-client.

    2. Πάτα Next και βεβαιώσου ότι το Standard Flow είναι ενεργοποιημένο.

    3. Στα Valid Redirect URIs βάλε * (για το development) και πάτα Save.

* **Δημιουργία Χρήστη (User)**

    1. Πάτα Users -> Create new user.

        Username: iason.

    2. Αφού τον φτιάξεις, πήγαινε στην καρτέλα Credentials -> Set password.

    3. Βάλε ένα password, __κλείσε το "Temporary"__ (να είναι Off) και πάτα Save.

* **Δημιουργία Admin Role (Προεραιτικά)**

    1. Πάτα Realm Roles -> Create role.

        Όνομα: admin.

    2. Πήγαινε πάλι στους Users, βρες τον χρήστη σου (iason), πάτα Role Mapping -> Assign role και διάλεξε το admin.


**2. Port Forwarding**

In a terminal run:
```bash
kubectl port-forward svc/keycloak 8080:8080
```

### RabbitMQ
**1. Port Forwarding**

In a terminal run:
```bash
kubectl port-forward svc/rabbitmq 5672:5672
```

### DDS
**1. Port Forwarding**

In a terminal run:
```bash
kubectl port-forward svc/postgres 5433:5432
```

### UI_Service
**1. Build the .env File**

Δημιούργησε ένα αρχείο με το όνομα **.env** μέσα στον φάκελο UI_service. Αυτό το αρχείο λειτουργεί ως το κεντρικό σημείο ρυθμίσεων (configuration hub), συνδέοντας το UI Service με όλες τις υποδομές που έχουμε κάνει port-forward.

Εδώ έχουμε ένα demo .env αρχείο:

```bash
# DB Credentials: Same as in the dds .yaml
DATABASE_URL="postgresql+asyncpg://admin:DisSami26@localhost:5433/dds_db"

# ==================== Keycloak ====================
KEYCLOAK_SERVER_URL="http://localhost:8080"
KEYCLOAK_REALM="myrealm"
# Keycloak admin credentials, as configured in the keycloak .yaml
KEYCLOAK_ADMIN_USER="admin"
KEYCLOAK_ADMIN_PASSWORD="admin"
# Keycloak Public Key: Admin Console -> Realm Settings -> Keys -> RS256 -> Public Key
KEYCLOAK_PUBLIC_KEY="MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmOrAlFzwUodXsYrYvSGV/u+gzdL+Nc3bPPYBara7rv591qLRU7H0Yt6fbWS3v0oreRxmi7T+i5NEw4CVgF18LOOFbRzFTORr6pId2TyyWoixGQLzzLp+aVU0KzFLq8hyu1g8w04xUiRij3Oqy5oiW/javKrYcFiZS5aL4wzYTDrrYEnkEYeP89m2Fwx7/c8/PSMBfjx806jPj8NI4gOo+ZLK7MBhOqk5DoQl8epFV2l2uElDqOkxI+xDez7SREsFUtqK/dEurdwXINTwzroSVqpIV/hzrCJafD67vSNrBH0ZhHweiXwnksnCzkDdlM8D33/mPApAHTnG2K5zYO/lYwIDAQAB"

# ==================== MinIO ====================
MINIO_ENDPOINT="http://localhost:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

# ==================== RabbitMQ ====================
RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
```

#### Troubleshoot: Πού θα βρεις αυτές τις τιμές αν χρειαστεί να τις αλλάξεις?
##### DDS
* **DATABASE_URL**: 

    Πρέπει να ταιριάζει ακριβώς με τα στοιχεία (credentials) που όρισες στο Kubernetes .yaml αρχείο του postgres-dds.

    Μορφή: postgresql+asyncpg://<USER>:<PASSWORD>@localhost:<PORT>/<DB_NAME>

    ***Σημείωση***: Χρησιμοποιούμε τη θύρα 5433 επειδή αυτήν ορίσαμε στην εντολή port-forwarding του DDS στο προηγούμενο βήμα.

##### Keycloak
* **KEYCLOAK_SERVER_URL**: 

    Παραμένει http://localhost:8080 χάρη στο port-forwarding.

* **KEYCLOAK_REALM**: 

    Το ακριβές όνομα του Realm που δημιούργησες νωρίτερα (π.χ., myrealm).

* **KEYCLOAK_ADMIN_USER & PASSWORD**: 

    Τα διαπιστευτήρια του master admin, όπως ορίστηκαν στο .yaml του Keycloak.

* **KEYCLOAK_PUBLIC_KEY**: 

    Αυτό είναι κρίσιμο για την ασφαλή επικύρωση των user tokens. Για να το βρεις:

    1. Πήγαινε στο Keycloak Admin Console (http://localhost:8080).

    2. Επίλεξε το Realm σου από το αναδιπλούμενο μενού πάνω αριστερά.

    3. Στο αριστερό μενού, πάτα στο Realm Settings.

    4. Πήγαινε στην καρτέλα Keys.

    5. Στην υπο-καρτέλα Active, βρες τη γραμμή όπου το Algorithm είναι RS256.

    6. Πάτα το κουμπί Public Key στα δεξιά αυτής της γραμμής.

    7. Θα εμφανιστεί ένα παράθυρο. Αντέγραψε όλο το string και κάνε το επικόλληση στο .env αρχείο σου.

##### **MinIO**
* **MINIO_ENDPOINT**: 

    Παραμένει http://localhost:9000 χάρη στο port-forwarding.

* **MINIO_ACCESS_KEY & SECRET_KEY**: 

    Τα διαπιστευτήρια που ορίστηκαν στο .yaml αρχείο του MinIO.

    (Αν δεν τα έχεις αλλάξει, η προεπιλογή συνήθως είναι minioadmin / minioadmin.)

##### RabbitMQ

* **RABBITMQ_URL**:

    Μορφή: *amqp://<USER>:<PASSWORD>@localhost:<PORT>/*

    Η προεπιλογή είναι *amqp://guest:guest@localhost:5672/*, εκτός αν άλλαξες τα στοιχεία στο kubernetes deployment.

2. Εκκίνηση του Server

Μέσα στο UI_service folder εκτέλε το ακόλουθο ώστε να ξεκινήσει ο Uvicorn server στο port 8000:
```bash
uvicorn main:app --reload --port 8000
```

Αν όλα πήγαν καλά, θα δεις:
```bash
INFO:     Will watch for changes in these directories: ['/filepath/UI_service']
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
INFO:     Started reloader process [367103] using StatReload
INFO:     Started server process [367105]
INFO:     Waiting for application startup.
INFO:     Application startup complete.
```

### CLI
**1. Login**
```bash
python main.py login username
```
Και συμπλήρωσε τον κωδικό του χρήστη. Αν όλα πάνε καλά θα δεις:
```bash
Successfully logged in as username!
```


* **Troubleshoot**:

    Αν δεις:
    ```bash
    Login failed. Check credentials.
    ```
    πάρχει πιθανότητα να φταίει μια ατέλεια του Keycloak. Κάνε τα ακόλουθα γαι να το λύσεις (Tip: Ίσως ξανασυμβεί)
    1. Επέλεξε το *Realm* που έφτιαξες νωρότερα.
    2. Πήγαινε στους *Users*.
    3. Στον χρήστη σου, πάτα *Reset password*.
    4. Ξανβάλε το password (ή νεο αν θες) και **κάνε off την επιλογή Temporary**.
    5. Πάτα *Save*.

**2. Υπόλοιπες Εντολές**

Από εκεί και περά, εφόσον έχεις κάνει όλα τα παραπάνω βήματα σωστά, θα μπορείς να τρέξεις όλες τις εντολές που έχουμε στο Design.md. 

*Tip*: Μπορείς να χρησιμοποιείς την εντολή --help για να σε βοηθήσει με τα διαθέσιμα comamnds.


## Παρατηρήσεις:
### Port Forwarding: 
Το port-forwarding απαιτείται επειδή το UI και το CLI εκτελούνται εκτός του Kubernetes cluster (είμαστε στο devolpment phase πρακικά). Αν τα συγκεκριμένα services γινόντουσαν deploy εντός του cluster (στο 2ο μέρος του README κάνουμε deploy και το UI_service, αλλά όχι το CLI), η επικοινωνία θα γινόταν απευθείας μέσω του εσωτερικού δικτύου του Kubernetes.

## Section 2: UI_service Deployment as a Service
Σε αυτήν την περίπτωση το UI_service γίνεται deploy στο kubernetes cluster ως service και το CLI τρέχει localy.

### UI_service

**1. Build the .env File**

Ισχύουν τα ίδια με πριν για το **.env**αρχείο όσο αναφορά την δομή του και τα credentials (όπως και το που μπορείς να τα βρεις). Το μόνο που αλλάζει είναι το ότι βγάζουμε το localhost. 

Εδώ έχουμε ένα demo .env αρχείο:

```bash
# DB Credentials: Same as in the dds .yaml
DATABASE_URL="postgresql+asyncpg://admin:DisSami26@postgres:5432/dds_db"

# ==================== Keycloak ====================
KEYCLOAK_SERVER_URL="http://keycloak:8080"
KEYCLOAK_REALM="myrealm"
# Keycloak admin credentials, as configured in the keycloak .yaml
KEYCLOAK_ADMIN_USER="admin"
KEYCLOAK_ADMIN_PASSWORD="admin"
# Keycloak Public Key: Admin Console -> Realm Settings -> Keys -> RS256 -> Public Key
KEYCLOAK_PUBLIC_KEY="MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAmOrAlFzwUodXsYrYvSGV/u+gzdL+Nc3bPPYBara7rv591qLRU7H0Yt6fbWS3v0oreRxmi7T+i5NEw4CVgF18LOOFbRzFTORr6pId2TyyWoixGQLzzLp+aVU0KzFLq8hyu1g8w04xUiRij3Oqy5oiW/javKrYcFiZS5aL4wzYTDrrYEnkEYeP89m2Fwx7/c8/PSMBfjx806jPj8NI4gOo+ZLK7MBhOqk5DoQl8epFV2l2uElDqOkxI+xDez7SREsFUtqK/dEurdwXINTwzroSVqpIV/hzrCJafD67vSNrBH0ZhHweiXwnksnCzkDdlM8D33/mPApAHTnG2K5zYO/lYwIDAQAB"

# ==================== MinIO ====================
MINIO_ENDPOINT="http://minio:9000"
MINIO_ACCESS_KEY="minioadmin"
MINIO_SECRET_KEY="minioadmin"

# ==================== RabbitMQ ====================
RABBITMQ_URL="amqp://guest:guest@rabbitmq:5672/"
```

**2. Deployment Guide:**

Εδώ περιγράφουμε τη διαδικασία μεταφοράς της υπηρεσίας UI_service από το τοπικό περιβάλλον ανάπτυξης (Development) σε ένα τοπικό Kubernetes cluster (Minikube).

* **1. Προετοιμασία της Εφαρμογής (Containerization)**:

    Αρχικά η εφαρμογή πρέπει να "πακεταριστεί" σε ένα Docker Image.
    
    * **Δημιουργία requirements.txt**:

    Μέσα στον φάκελο UI_service φτιάχνουμε το *requirements.txt*, και εξάγουμε εκεί τις απαραίτητες βιβλιοθήκες από το virtual environment, με την εντολή:
    ```bash
    pip freeze > requirements.txt
    ```
    * **Ρύθμιση .dockerignore**:

    Μέσα στον φάκελο UI_service φτιάχνουμε το *.dockerignore*,και εξαιρούμε το τοπικό .venv και άλλα περιττά αρχεία για τη μείωση του  image size, βάζοντας μέσα του τα ακόλουθα:
    ```bash
    .venv
    __pycache__
    .git
    .env
    ```

    * **Docker Build**:

    Κατασκευή του image της εφαρμογής.
    ```bash
    docker build --network host -t ui-service:latest .
    ```

* **2. Μεταφορά στο Kubernetes (Minikube)**:

Το Kubernetes δεν έχει άμεση πρόσβαση στο τοπικό Docker registry, οπότε πρέπει να "ανεβάσουμε" την εικόνα στο εσωτερικό του registry.
```bash
minikube image load ui-service:latest
```

* 3. **ui-service.yaml**:
Μπορείς να το βρεις στον φάκελο με τα .yaml αρχεί στο github.

### CLI

#### Port Forwarding

Επειδή το cluster τρέχει σε απομονωμένο δίκτυο, χρησιμοποιούμε μια "γέφυρα" για να επικοινωνήσει το τοπικό CLI με το service μέσα στο Kubernetes. Εκτέλεσε την εντολή σε ένα terminal και άστο να τρέχει:
```bash
kubectl port-forward svc/ui-service 8000:80
```


