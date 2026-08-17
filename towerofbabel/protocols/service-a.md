# Kontrak HTTP Service A

Service A berjalan di `http://localhost:8101`.

## Endpoint

| Method | Path | Fungsi |
| --- | --- | --- |
| `GET` | `/v1/health` | Health check. |
| `GET` | `/v1/capabilities` | Metadata service dan operasi yang didukung. |
| `GET` | `/v1/metadata` | Alias untuk capabilities. |
| `POST` | `/v1/execute` | Menjalankan satu operasi. |

## Permintaan execute

Header wajib:

| Header | Nilai |
| --- | --- |
| `Content-Type` | `application/json` |
| `X-Protocol-Version` | `1` |
| `X-Request-ID` | Identifier permintaan opaque dari pemanggil. |

Body:

```json
{
  "operation_name": "uppercase",
  "operation_arguments": {
    "value": "babel"
  }
}
```

## Tanggapan sukses

Status HTTP: `200`.

```json
{
  "request_id": "smoke-a",
  "service_name": "service-a",
  "operation_name": "uppercase",
  "operation_result": {
    "value": "BABEL"
  }
}
```

## Tanggapan error

```json
{
  "request_id": "smoke-a",
  "error_code": "UNSUPPORTED_PROTOCOL_VERSION",
  "error_message": "Only version 1 is supported.",
  "retryable": false
}
```

Kode status umum:

| Status | Makna |
| --- | --- |
| `400` | Permintaan tidak valid, versi tidak didukung, JSON malformed, atau operasi tidak didukung. |
| `415` | Content type bukan `application/json`. |
| `429` | Fault rate limit yang retryable. |
| `500` | Fault internal yang retryable. |
| `503` | Fault unavailable yang retryable. |

## Operasi

| Operasi | Argumen | Result |
| --- | --- | --- |
| `echo` | `{ "value": any }` | `{ "value": any }` |
| `uppercase` | `{ "value": string }` | `{ "value": string }` |
| `metadata` | `{}` | Metadata service. |
