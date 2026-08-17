# Babel Gateway

Sebuah protocol gateway yang menaruh satu API di depan tiga backend yang tidak
sepakat pada apa pun di bawah lapisan aplikasi: HTTP dengan JSON, protokol frame
biner di atas TCP, dan protokol biner ber-CRC32 di atas UDP.

```bash
docker compose up -d
curl -s http://localhost:8080/status | jq .
```

```bash
curl -s -X POST http://localhost:8080/execute \
  -H 'Content-Type: application/json' \
  -d '{"request_id":"r1","operation":"uppercase","arguments":{"value":"babel"},"options":{}}'
```

```json
{"request_id":"r1","status":"success","service_id":"service-a","operation":"uppercase","result":{"value":"BABEL"},"error":null}
```

> **Sebelum submit:** ganti `"nim_anda"` di `submission.json` dengan NIM Anda.
> Sisanya di file itu sudah terisi.

## Isi repositori

| Path | Isinya |
|---|---|
| `docker-compose.yml` | Stack yang disubmit: tiga backend, control plane fault injection, dan service `gateway` di `localhost:8080` |
| `src/` | Source gateway. Go 1.25, standard library saja |
| `demo/` | Skrip demonstrasi, penjelasannya ada di `demo/README.md` |
| `laporan.md` dan `laporan.pdf` | Laporan teknis |
| `submission.json` | Manifest submission |
| `AI_USAGE.md` | Deklarasi penggunaan AI |
| `towerofbabel/` | Kit yang disediakan panitia, tidak diubah sama sekali |

## Cara menjalankan

```bash
docker compose up -d     # gateway siap di http://localhost:8080
./demo/run-all.sh        # demonstrasi lengkap, sekitar satu menit
docker compose logs -f gateway
```

Di PowerShell, `./demo/run-all.sh` tidak akan berjalan karena PowerShell tidak
mengeksekusi file `.sh`, ia hanya membukanya. Pakai wrapper-nya:

```powershell
docker compose up -d
.\demo\run-all.ps1        # atau .\demo\run-all.ps1 --core
```

Wrapper itu mencari Git Bash lalu mendelegasikan ke skrip aslinya. Alternatifnya
buka Git Bash langsung, atau panggil bash-nya secara eksplisit dari PowerShell:

```powershell
& "C:\Program Files\Git\bin\bash.exe" demo/run-all.sh
```

Image gateway dibangun dari `src/`, dan proses build-nya menjalankan `go vet`
beserta seluruh test suite. Jadi kalau build-nya berhasil, itu berarti lapisan
translasi protokolnya sudah terbukti benar terhadap byte yang direkam dari
backend sungguhan, bukan sekadar terkompilasi.

Test juga bisa dijalankan langsung:

```bash
cd src && go test -race ./...
```

## API publik

| Endpoint | Kegunaan |
|---|---|
| `POST /execute` | Menjalankan sebuah operasi logis |
| `GET /services` | Daftar backend terdaftar: protokol, kesehatan, capability, versi protokol |
| `GET /status` | Keadaan gateway, kesehatan per backend, adapter, breaker, counter transport |
| `GET /metrics` | Counter saja |
| `GET /healthz` | Liveness proses gateway itu sendiri |

Operasi yang dikenal: `echo`, `uppercase`, `sum`, `reverse`, `metadata`.

Options yang dikenal: `preferred_service` dan `timeout_ms`. Option lain di luar
itu diabaikan, bukan ditolak, sesuai kontrak.

### API operator

Dipakai oleh demonstrasi bonus. Secara default terbuka di dalam jaringan
compose, bisa dikunci dengan `X-Babel-Admin-Token` kalau `BABEL_ADMIN_TOKEN`
diisi, dan bisa dihilangkan sama sekali dengan `BABEL_ADMIN_ENABLED=false`.

| Endpoint | Kegunaan |
|---|---|
| `GET /admin/registry` | Membaca dokumen registry yang dipersistensi |
| `POST /admin/registry/service` | Memasang atau mengganti definisi sebuah service |
| `POST /admin/registry/enabled` | Memasukkan atau mengeluarkan backend dari rotasi |
| `POST /admin/registry/weights` | Menggeser trafik antar versi protokol |
| `POST /admin/registry/replay-safe` | Mengubah gerbang fallback untuk satu operasi |
| `GET /admin/adapters` | Daftar adapter yang termuat, lengkap dengan generasi dan jumlah request in-flight |
| `POST /admin/adapters` | Memuat atau mengganti adapter saat runtime |
| `POST /admin/adapters/remove` | Melepas adapter yang sudah tidak diikat variant mana pun |
| `POST /admin/migrate` | Memuat versi baru, mendaftarkannya di bobot nol, lalu menggeser trafik |
| `POST /admin/breakers/reset` | Membersihkan state circuit setelah backend diperbaiki |

## Bagaimana ini disusun

```
POST /execute
    |
    +- validasi envelope ---------- pelanggaran kontrak caller dijawab 400
    +- validasi argumen ----------- jawabannya sama apa pun backend-nya
    +- admission control ---------- batas konkurensi global dan per backend
    |
    +- ROUTER
    |     lookup capability di registry persisten
    |     urutan kandidat deterministik: priority, lalu service_id
    |     deadline tiap percobaan diiris dari budget caller
    |     circuit breaker per (service, versi protokol)
    |     gerbang fallback: hanya kalau percobaan gagal terbukti tidak bekerja
    |
    +- ADAPTER  satu per service dan versi, deklaratif, bisa diganti runtime
    |     request  : operasi logis dan argumen  ->  dialek backend
    |     response : divalidasi dulu, baru dinormalisasi ke envelope
    |
    +- TRANSPORT
          http-json        HTTP dengan connection pool
          tcp-frame-json   frame multiplexed, korelasi lewat request id
          udp-crc-json     ARQ dengan adaptive RTO, dedupe, reassembly, window
```

Ada empat keputusan desain yang menanggung sebagian besar beban. Laporan
menjelaskan masing-masing secara lengkap, dan berikut ringkasannya.

**Adapter adalah dokumen, bukan kode.** Sebuah `Spec` mendeskripsikan satu versi
protokol: endpoint, parameter wire, pemetaan field per operasi, dan golden test
vector yang direkam dari backend sungguhan. Tiga adapter bawaan gateway ini juga
berupa spec, dan dimuat lewat jalur yang persis sama dengan apa pun yang dikirim
ke `/admin/adapters`. Karena hanya ada satu jalur, jalur pemuatan runtime itu
tidak bisa diam-diam membusuk.

**Satu service memegang beberapa variant protokol berbobot sekaligus.** Rolling
upgrade adalah `{v1: 70, v2: 30}`, cutover adalah `{v1: 0, v2: 100}`, dan
rollback adalah kebalikannya. Migrasi runtime dan kompatibilitas multi-versi
jadi satu mekanisme yang sama, diekspresikan sebagai data, dan dipersistensi
begitu diterapkan.

**Fallback butuh bukti, bukan optimisme.** Koneksi yang ditolak, HTTP 503, atau
penolakan oleh breaker berarti backend terbukti tidak mengerjakan apa pun, jadi
backend lain boleh mengerjakannya. Timeout berbeda, karena backend kemungkinan
masih menjalankan operasinya. Untuk kasus itu gateway melaporkan timeout, bukan
menduplikasi pekerjaan. Ledger eksekusi milik environment referensi
mengonfirmasi kedua sisi pembedaan ini.

**Correlation id dialokasikan gateway dan tidak pernah dipakai ulang**, termasuk
melintasi restart. Alokatornya disemai di atas high water mark yang
dipersistensi dan dibatasi bawah oleh jam dinding. Nilai `request_id` dari klien
hanya dipakai untuk logging, karena klien boleh saja mengulanginya.

## Konfigurasi

Konfigurasi struktural, yaitu backend apa yang ada, apa saja yang bisa mereka
lakukan, dan adapter mana yang diikat tiap versi, tersimpan di registry
persisten pada `/data/registry.json`, bukan di environment variable. Environment
variable hanya mengatur kebijakan.

| Variabel | Default | Efeknya |
|---|---|---|
| `BABEL_LISTEN_ADDR` | `:8080` | Alamat listen |
| `BABEL_GATEWAY_STATE_DIR` | `/data` | Lokasi registry dan spec adapter |
| `BABEL_SERVICE_A_URL` | `http://service-a:8101` | Hanya dipakai menyemai registry saat boot pertama |
| `BABEL_SERVICE_B_ADDR` | `service-b:8201` | Idem |
| `BABEL_SERVICE_C_ADDR` | `service-c:8301` | Idem |
| `BABEL_DEFAULT_TIMEOUT_MS` | `2000` | Budget waktu kalau caller tidak menentukan |
| `BABEL_RESPONSE_MARGIN_MS` | `120` | Cadangan supaya jawaban mendahului deadline caller |
| `BABEL_MAX_ATTEMPTS` | `4` | Jumlah percobaan backend per request klien |
| `BABEL_SAME_SERVICE_ATTEMPTS` | `2` | Percobaan pada satu backend sebelum pindah |
| `BABEL_MAX_INFLIGHT` | `512` | Batas admission global |
| `BABEL_PER_BACKEND_LIMIT` | `128` | Batas konkurensi per backend |
| `BABEL_BREAKER_THRESHOLD` | `5` | Kegagalan beruntun yang membuka circuit |
| `BABEL_BREAKER_COOLDOWN_MS` | `3000` | Lama circuit tetap terbuka |
| `BABEL_HEALTH_INTERVAL_MS` | `2000` | Interval probe liveness |
| `BABEL_INTRUSIVE_PROBE_INTERVAL_MS` | `30000` | Irama lambat untuk backend yang probe-nya memakan satu operasi nyata |
| `BABEL_UDP_MAX_RETRIES` | `6` | Anggaran retransmisi datagram |
| `BABEL_UDP_RTO_MS` | `120` | RTO awal sebelum estimator punya sampel |
| `BABEL_TCP_POOL_SIZE` | `4` | Jumlah koneksi multiplexed per backend frame |
| `BABEL_TCP_FRAME_BODY_TIMEOUT_MS` | `1500` | Batas tunggu payload frame setelah header-nya tiba |
| `BABEL_PREFERRED_INCAPABLE` | `fallback` | `strict` akan menolak `preferred_service` yang tidak mampu |
| `BABEL_FALLBACK_ON_CORRUPT` | `true` | `false` membuat respons rusak bersifat terminal |
| `BABEL_STRICT_HTTP_STATUS` | `false` | `true` mengeluarkan 408, 503, 429 alih-alih 200 untuk error domain |
| `BABEL_ADMIN_ENABLED` | `true` | `false` menghapus seluruh route `/admin/*` |
| `BABEL_ADMIN_TOKEN` | kosong | Kalau diisi, `/admin/*` mewajibkan header `X-Babel-Admin-Token` |
| `BABEL_LOG_LEVEL` | `info` | `debug` menambahkan event per datagram dan per frame |

### Tentang status HTTP

Kontrak Gateway API mencantumkan status untuk kondisi level gateway, misalnya
408 saat timeout dan 503 saat tidak ada rute. Tetapi kontrak yang sama juga
menyatakan bahwa envelope-lah yang dinilai. Masalahnya, klien referensi
memanggil `raise_for_status()`, sehingga balasan non-2xx apa pun akan
menggagalkan klien sebelum envelope sempat dibaca. Menjawab 408 untuk sebuah
timeout berarti mengganti envelope error yang presisi dengan sebuah exception di
sisi klien.

Gateway ini menyelesaikannya dengan menjawab **200 disertai envelope error yang
lengkap** untuk setiap hasil level domain, dan menyisakan non-2xx untuk
pelanggaran kontrak oleh caller (400) serta cacat gateway sendiri (500). Kalau
pemetaan literalnya yang diinginkan, set `BABEL_STRICT_HTTP_STATUS=true`.
Envelope-nya identik byte per byte di kedua mode.

## Keterbatasan yang diketahui

**Transport family baru tetap butuh kode Go.** Spec mengomposisi tiga family
yang sudah ada. Protokol seperti gRPC memerlukan implementasi family baru, bukan
sekadar dokumen baru.

**Fragmentasi UDP sisi kirim dimatikan** untuk backend referensi, yang memang
akan menolak request terfragmentasi. Jalur terimanya melakukan reassembly dan
sudah diuji, dan payload berlebih ditolak sebelum dikirim supaya router bisa
memilih backend yang berorientasi stream.

**Retransmisi datagram bersifat at-least-once.** Kalau yang hilang adalah
responsnya, backend sudah mengeksekusi dan akan mengeksekusi lagi. Ini melekat
pada ARQ, dan itulah sebabnya router tidak pernah menambahkan fallback lintas
backend di atas timeout UDP.

**`/status` tidak diautentikasi** dan memaparkan endpoint beserta counter. Cukup
untuk environment ini, tetapi di produksi perlu dibatasi.

**API admin terbuka secara default** di dalam jaringan compose. Mengisi
`BABEL_ADMIN_TOKEN` akan menutupnya.
