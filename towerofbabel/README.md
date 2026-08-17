## Load and Run Environment

Pakai image Docker lokal yang disertakan di `images/babel-go-images.tar`. Load image service terlebih dahulu:

```bash
docker load -i images/babel-go-images.tar
```

```bash
docker compose up -d
```

Gateway Anda harus berjalan di:

```text
http://localhost:8080
```

## Client Python dan smoke test

Siapkan dependency public client dan smoke test:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Jalankan smoke test environment:

```bash
python -m pytest tests/smoke
```
