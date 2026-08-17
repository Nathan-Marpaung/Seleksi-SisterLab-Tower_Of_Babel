# Integrasi Kandidat

Implementasikan Babel Gateway. Jangan mengubah service, test, atau control-plane milik Grader Hiura.

Gateway Anda harus mengekspos:

- `POST /execute`
- `GET /services`
- `GET /status`

URL gateway default: `http://localhost:8080`.

Kontrak request dan response lengkap ada di `protocols/gateway-api.md`.

Jika gateway berjalan langsung di host, hubungkan ke:

- Service A: `http://localhost:8101`
- Service B: `localhost:8201`
- Service C: `localhost:8301`

Jika gateway berjalan di dalam `babel-network`, hubungkan ke:

- Service A: `http://service-a:8101`
- Service B: `service-b:8201`
- Service C: `service-c:8301`

Smoke test:

```bash
python -m pytest tests/smoke
```

Smoke test hanya memeriksa environment publik dan contoh protokol. Smoke test tidak menilai kebenaran gateway. Penilaian gateway dilakukan oleh Grader Hiura.

Jika Anda mengimplementasikan objektif bonus, jelaskan dan demonstrasikan perilakunya di laporan. Anda bebas memilih nama konfigurasi, feature toggle, modul, adapter, atau algoritma internal. Grader Hiura menilai perilaku yang terlihat dari luar berdasarkan spesifikasi.
