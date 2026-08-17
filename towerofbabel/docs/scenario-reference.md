# Referensi Skenario Test

Environment ini menyediakan skenario representatif untuk kelas fault berikut:

- delay transport
- packet/frame loss
- duplikasi
- reordering
- korupsi
- response malformed
- version mismatch
- partial outage
- backend unavailable
- keamanan fallback

Setiap skenario deterministik untuk seed aktif. Reset dengan:

```bash
curl -X POST http://localhost:8090/reset \
  -H "X-Babel-Control-Token: babel-local-dev"
```

Identifier skenario publik:

```text
normal
service-a-slow
service-a-down
service-b-out-of-order
service-b-duplicate
service-b-fragmented
service-b-invalid-frame
service-c-lossy
service-c-reordered
service-c-corrupt-checksum
service-c-duplicate
unsupported-version
malformed-responses
partial-outage
fallback-safe
fallback-unsafe
hostile
```

Skenario ini adalah alat bantu pengembangan, bukan rencana grading resmi. Skenario sengaja dibuat cukup luas agar kandidat dapat melatih timeout handling, validasi, fallback, dan edge case transport sambil tetap merancang internal gateway sendiri.

Grader Hiura dapat mengombinasikan fault, mengubah timing, mereset seed, dan memakai jadwal berbeda selama tetap mengikuti kontrak protokol publik.
