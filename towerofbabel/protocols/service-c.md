# Kontrak UDP Service C

Service C berjalan di `localhost:8301` melalui UDP.

Service C memakai header big-endian tetap berukuran 20 byte, payload JSON, dan checksum CRC32 di akhir packet.

## Layout packet

| Offset | Ukuran | Field | Nilai |
| ---: | ---: | --- | --- |
| `0` | `2` | magic | `0xC0 0xDE` |
| `2` | `1` | version | `1` |
| `3` | `1` | message type | `1` request, `2` response, `3` error, `4` ack |
| `4` | `4` | sequence number | Sequence unsigned big-endian. |
| `8` | `8` | request ID | ID korelasi unsigned big-endian. |
| `16` | `1` | operation code | `1` echo, `2` sum, `3` metadata |
| `17` | `1` | flags | Reserved. Kirim `0`. |
| `18` | `2` | payload length | Panjang byte unsigned big-endian. Maksimum `4096`. |
| `20` | variable | payload | JSON UTF-8. |
| last `4` | `4` | checksum | IEEE CRC32 standar atas semua byte packet sebelumnya, kompatibel dengan `zlib.crc32`. |

Gateway wajib memvalidasi checksum, length, version, dan korelasi request ID. Pengiriman UDP dapat terlambat, terduplikasi, berubah urutan, atau hilang; gateway yang robust sebaiknya memakai timeout dan retry sesuai desainnya.

## Payload request

Untuk `sum`:

```json
{
  "values": [7, 8]
}
```

Untuk `echo`:

```json
{
  "value": "babel"
}
```

Untuk `metadata`:

```json
{}
```

## Payload response sukses

```json
{
  "serviceId": "service-c",
  "result": {
    "value": 15
  },
  "error": null
}
```

## Payload response error

```json
{
  "serviceId": "service-c",
  "result": null,
  "error": {
    "code": "INVALID_PACKET",
    "message": "Packet failed validation.",
    "retryable": false
  }
}
```

## Operasi

| Operation code | Operasi | Argumen | Result |
| ---: | --- | --- | --- |
| `1` | `echo` | `{ "value": any }` | `{ "value": any }` |
| `2` | `sum` | `{ "values": number[] }` | `{ "value": number }` |
| `3` | `metadata` | `{}` | Metadata service. |
