# Kontrak TCP Service B

Service B berjalan di `localhost:8201` melalui TCP.

Service B memakai header big-endian tetap berukuran 16 byte, lalu diikuti payload JSON. Ukuran header selalu tepat 16 byte.

## Layout frame

| Offset | Ukuran | Field | Nilai |
| ---: | ---: | --- | --- |
| `0` | `2` | magic | `0xBA 0xBE` |
| `2` | `1` | version | `1` |
| `3` | `1` | flags | Reserved. Kirim `0`. |
| `4` | `4` | payload length | Panjang byte unsigned big-endian. Maksimum `65536`. |
| `8` | `8` | request ID | ID korelasi unsigned big-endian. |
| `16` | variable | payload | JSON UTF-8. |

Frame response mengulang request ID, kecuali ada fault yang menginjeksi kondisi korelasi invalid. Gateway wajib memvalidasi magic, version, length, dan korelasi request ID.

## Payload request

```json
{
  "opCode": "SUM",
  "args": {
    "numberList": [1, 2, 3]
  }
}
```

`opCode` bersifat case-insensitive. Nilai yang dikenal: `ECHO`, `UPPERCASE`, `SUM`, `REVERSE`, `METADATA`.

## Payload response sukses

```json
{
  "requestId": 42,
  "serviceId": "service-b",
  "resultData": {
    "numericResult": 6
  },
  "errorData": null
}
```

Result operasi berbasis string memakai `resultData.value`. Result numerik `value` dapat dikembalikan sebagai `resultData.numericResult`.

## Payload response error

```json
{
  "requestId": 42,
  "serviceId": "service-b",
  "resultData": null,
  "errorData": {
    "code": "OPERATION_NOT_SUPPORTED",
    "message": "service-b does not support lookup",
    "retryable": false
  }
}
```

## Operasi

| Operasi | Argumen | Result |
| --- | --- | --- |
| `ECHO` | `{ "value": any }` | `{ "value": any }` |
| `UPPERCASE` | `{ "value": string }` | `{ "value": string }` |
| `SUM` | `{ "numberList": number[] }` | `{ "numericResult": number }` |
| `REVERSE` | `{ "value": string }` | `{ "value": string }` |
| `METADATA` | `{}` | Metadata service. |
