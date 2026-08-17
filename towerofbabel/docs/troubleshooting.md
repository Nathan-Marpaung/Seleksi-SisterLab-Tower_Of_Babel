# Pemecahan Masalah

Jika test gateway langsung gagal, pastikan `BABEL_GATEWAY_URL` menunjuk ke gateway kandidat.

Jika test backend ter-skip, jalankan service Babel dengan:

```bash
docker compose up -d
```

Jika fault masih aktif padahal tidak diharapkan, jalankan:

```bash
curl -X POST http://localhost:8090/reset \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Jika networking Docker berbeda di Linux, jalankan gateway container di `babel-network` atau atur alamat host secara eksplisit di konfigurasi gateway.
