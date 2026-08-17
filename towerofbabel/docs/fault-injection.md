# Panduan Fault Injection

URL control-plane: `http://localhost:8090`.

Token control-plane lokal: `babel-local-dev`, kecuali diganti lewat `BABEL_CONTROL_TOKEN`.

Endpoint:

- `GET /state`
- `POST /reset`
- `POST /seed`
- `POST /scenario/{name}`
- `POST /services/{service_id}/faults`
- `DELETE /services/{service_id}/faults`
- `GET /ledger`
- `DELETE /ledger`

Fault mendukung `probability`, `delay_ms`, `remaining_occurrences`, `after_requests`, `request_id`, dan `operation`.

## Setup

Control-plane disediakan untuk debugging dan validasi lokal. Grader Hiura dapat memakai jadwal fault yang berbeda selama tetap mengikuti kontrak protokol publik.

Service ID yang valid:

```text
service-a
service-b
service-c
```

## Reset

Menghapus fault aktif, counter, dan entry ledger:

```bash
curl -X POST http://localhost:8090/reset \
  -H "X-Babel-Control-Token: babel-local-dev"
```

## Seed

Mengatur seed deterministik untuk fault probabilistik:

```bash
curl -X POST http://localhost:8090/seed \
  -H "X-Babel-Control-Token: babel-local-dev" \
  -H "Content-Type: application/json" \
  -d '{"seed": 42}'
```

Jika tidak diisi atau diatur ke `0`, environment memakai seed default.

## Skenario

Mengaktifkan skenario publik bernama:

```bash
curl -X POST http://localhost:8090/scenario/service-c-lossy \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Lihat `docs/scenario-reference.md` untuk nama skenario publik.

## Fault injection langsung

Mengatur fault untuk satu backend service:

```bash
curl -X POST http://localhost:8090/services/service-c/faults \
  -H "X-Babel-Control-Token: babel-local-dev" \
  -H "Content-Type: application/json" \
  -d '{
    "faults": [
      {
        "mode": "packet_loss",
        "probability": 0.25,
        "remaining_occurrences": 4
      }
    ]
  }'
```

Menghapus fault untuk satu backend service:

```bash
curl -X DELETE http://localhost:8090/services/service-c/faults \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Field objek fault:

| Field | Tipe | Default | Makna |
| --- | --- | --- | --- |
| `mode` | string | wajib | Perilaku fault yang diinjeksi. |
| `probability` | number | `1` | Probabilitas penerapan dari `0` sampai `1`. |
| `delay_ms` | integer | `0` | Durasi delay untuk fault berbasis delay. |
| `remaining_occurrences` | integer | unlimited | Jumlah penerapan berhasil sebelum fault dihapus. |
| `after_requests` | integer | `0` | Fault tidak aktif sebelum jumlah request service ini terlewati. |
| `request_id` | string | any | Berlaku hanya untuk request ID ini. |
| `operation` | string | any | Berlaku hanya untuk operasi logis ini. |

Mode fault yang didukung:

| Service | Mode |
| --- | --- |
| `service-a` | `delayed_response`, `http_429`, `http_500`, `http_503`, `invalid_json`, `missing_required_field`, `incorrect_request_id`, `unsupported_version`, `connection_termination` |
| `service-b` | `delayed_response`, `invalid_magic`, `invalid_protocol_version`, `excessive_frame_length`, `truncated_frame`, `connection_reset`, `incorrect_request_id`, `unsolicited_response`, `duplicate_response`, `fragmented` |
| `service-c` | `packet_loss`, `packet_delay`, `corrupt_checksum`, `duplicate_datagram`, `reordered` |

## Ledger

Melihat aktivitas backend terbaru:

```bash
curl http://localhost:8090/ledger \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Melihat entry untuk satu request ID:

```bash
curl http://localhost:8090/ledger/client-001 \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Menghapus entry ledger:

```bash
curl -X DELETE http://localhost:8090/ledger \
  -H "X-Babel-Control-Token: babel-local-dev"
```
