# Kontrak API Babel Gateway

Gateway kandidat wajib mengekspos API HTTP/JSON di `http://localhost:8080` secara default.

Public client dan Grader Hiura hanya berbicara ke API ini. Isi internal gateway, algoritma routing, struktur adapter, kebijakan retry, dan pilihan penyimpanan adalah tanggung jawab kandidat.

## `POST /execute`

Menjalankan satu operasi logis melalui salah satu backend service.

Permintaan:

```json
{
  "request_id": "client-001",
  "operation": "sum",
  "arguments": {
    "values": [1, 2, 3]
  },
  "options": {
    "preferred_service": "service-c",
    "timeout_ms": 2000
  }
}
```

Field wajib:

| Field | Tipe | Makna |
| --- | --- | --- |
| `request_id` | string | ID korelasi opaque dari pemanggil. |
| `operation` | string | Nama operasi logis. |
| `arguments` | object | Argumen operasi. |
| `options` | object | Hint eksekusi opsional. Gunakan `{}` jika tidak diperlukan. |

Opsi yang dikenali:

| Field | Tipe | Makna |
| --- | --- | --- |
| `preferred_service` | string | Preferensi backend opsional: `service-a`, `service-b`, atau `service-c`. |
| `timeout_ms` | number | Batas waktu dari sisi pemanggil dalam milidetik. |

Gateway boleh mengabaikan opsi yang tidak didukung, tetapi tidak boleh gagal hanya karena ada opsi yang tidak dikenal.

`timeout_ms` adalah batas atas untuk seluruh operasi gateway dari sudut pandang pemanggil. Gateway boleh memilih deadline per-backend yang lebih pendek, retry, atau fallback secara internal, tetapi seharusnya tidak terus mengerjakan request setelah budget timeout pemanggil habis.

Tanggapan sukses:

```json
{
  "request_id": "client-001",
  "status": "success",
  "service_id": "service-c",
  "operation": "sum",
  "result": {
    "value": 6
  },
  "error": null
}
```

Tanggapan error:

```json
{
  "request_id": "client-001",
  "status": "error",
  "service_id": "service-c",
  "operation": "sum",
  "result": null,
  "error": {
    "code": "BACKEND_TIMEOUT",
    "message": "Backend did not respond within timeout.",
    "retryable": true
  }
}
```

Field response wajib:

| Field | Tipe | Makna |
| --- | --- | --- |
| `request_id` | string | Harus sama dengan request masuk, kecuali gateway menolak input malformed sebelum menerimanya. |
| `status` | string | `success` atau `error`. Nilai lain tidak valid. |
| `service_id` | string atau null | Backend yang menghasilkan result, atau null jika tidak ada backend yang aman dipilih. |
| `operation` | string | Nama operasi logis. |
| `result` | object atau null | Hasil operasi yang sudah dinormalisasi saat sukses. |
| `error` | object atau null | Objek error saat gagal. |

Invariant response:

| Status | Invariant wajib |
| --- | --- |
| `success` | `result` tidak null dan `error` null. |
| `error` | `result` null dan `error` tidak null. |

`service_id` sebaiknya mengidentifikasi backend yang menghasilkan result atau error jika diketahui. Nilai ini boleh null hanya ketika gateway tidak dapat memilih atau menyelesaikan route backend dengan aman.

## Operasi logis

Gateway menerima operasi logis dan menerjemahkannya ke protokol spesifik backend.

| Operasi logis | Service A | Service B | Service C | Result ternormalisasi |
| --- | --- | --- | --- | --- |
| `echo` | `echo` | `ECHO` | opcode `1` | `{ "value": any }` |
| `uppercase` | `uppercase` | `UPPERCASE` | tidak didukung | `{ "value": string }` |
| `sum` | tidak didukung | `SUM` dengan `numberList` | opcode `2` | `{ "value": number }` |
| `reverse` | tidak didukung | `REVERSE` | tidak didukung | `{ "value": string }` |
| `metadata` | `metadata` | `METADATA` | opcode `3` | Objek metadata service. |

Jika `preferred_service` menunjuk backend yang tidak mendukung operasi yang diminta, gateway boleh mengembalikan error atau memilih backend aman lain, selama perilakunya dijelaskan di laporan kandidat dan response tetap mengikuti kontrak API ini.

Input numerik untuk `sum` adalah JSON number finite dalam rentang signed 32-bit. `NaN`, infinity, angka berbentuk string, dan nilai di luar rentang tersebut tidak valid. Result boleh berupa integer atau floating-point JSON number, tergantung input.

## Normalisasi response

Tanggapan gateway wajib memakai skema Gateway API, bukan bentuk tanggapan spesifik backend.

Aturan:

| Bentuk backend | Result gateway |
| --- | --- |
| Service A `operation_result` | `result` |
| Service B `resultData.value` | `result.value` |
| Service B `resultData.numericResult` | `result.value` |
| Service C `result` | `result` |

Field spesifik backend seperti `operation_result`, `resultData`, `numericResult`, `errorData`, dan `serviceId` tidak boleh bocor ke envelope response top-level gateway.

Untuk error, normalisasikan error backend menjadi:

```json
{
  "code": "ERROR_CODE",
  "message": "Human-readable message.",
  "retryable": false
}
```

`retryable: true` berarti retry mungkin berhasil. Ini tidak berarti gateway wajib retry selamanya, dan tidak mengalahkan budget `timeout_ms` pemanggil. Gateway boleh berhenti retry ketika budget timeout, kebijakan keamanan, atau batas retry tercapai.

## Korelasi dan duplikasi

Gateway wajib mengorelasikan response backend dengan request logis yang benar.

- Korelasi Service A memakai `X-Request-ID` dan `request_id` pada tanggapan.
- Korelasi Service B memakai request ID di frame.
- Korelasi Service C memakai request ID dan sequence number di packet.

Duplicate response adalah response kedua untuk request backend logis yang sama. Untuk Service C, datagram duplikat dengan request ID dan sequence number yang sama harus diperlakukan sebagai response logis yang sama. Gateway tidak boleh menghasilkan sukses client-visible ganda untuk `request_id` yang sama.

`request_id` adalah identifier korelasi, bukan otomatis idempotency key. Dua request client yang sudah diterima tidak otomatis dideduplicate hanya karena memakai `request_id` yang sama, kecuali kandidat mendokumentasikan perilaku itu secara eksplisit. Retransmission untuk attempt backend yang sama tetap harus dikorelasikan dan dideduplicate secara internal.

Kode status HTTP:

| Status | Makna |
| --- | --- |
| `200` | Gateway menerima request dan mengembalikan envelope sukses atau error domain-level. |
| `400` | Body request malformed atau melanggar kontrak Gateway API. |
| `408` | Timeout level gateway sebelum result backend valid tersedia. |
| `429` | Rate limit level gateway, jika diimplementasikan. |
| `500` | Kesalahan internal gateway. |
| `503` | Gateway unavailable atau tidak ada route backend yang aman. |

Grader Hiura terutama menilai envelope JSON dan perilaku yang terlihat dari luar. Jangan bergantung pada quirk service yang tidak terdokumentasi, timing fault yang persis, atau jadwal skenario publik tertentu.

## `GET /services`

Mengembalikan backend service yang diketahui gateway beserta status penggunaannya dari sudut pandang gateway.

Contoh response:

```json
{
  "services": [
    {
      "service_id": "service-a",
      "protocol": "http-json",
      "status": "available",
      "capabilities": ["echo", "metadata", "uppercase"]
    },
    {
      "service_id": "service-b",
      "protocol": "tcp-frame-json",
      "status": "available",
      "capabilities": ["echo", "metadata", "reverse", "sum", "uppercase"]
    },
    {
      "service_id": "service-c",
      "protocol": "udp-crc-json",
      "status": "available",
      "capabilities": ["echo", "metadata", "sum"]
    }
  ]
}
```

## `GET /status`

Mengembalikan health gateway dan ringkasan state runtime.

Contoh response:

```json
{
  "status": "ok",
  "gateway_id": "candidate-gateway",
  "uptime_ms": 12345,
  "backends": {
    "service-a": "available",
    "service-b": "available",
    "service-c": "available"
  }
}
```

Field internal boleh berbeda, tetapi response harus berupa JSON valid dan menyertakan `status` di level paling atas.
