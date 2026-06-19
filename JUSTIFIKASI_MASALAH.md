# Justifikasi Masalah & Kebutuhan Big Data
### Sistem Prediksi Keterlambatan Penerbangan Komersial

> Dokumen ini melengkapi `README.md` utama. Isinya: bukti kuantitatif bahwa masalah keterlambatan penerbangan nyata dan belum terselesaikan, kerangka 5V yang menjustifikasi pendekatan Big Data, dan analisis gap terhadap solusi yang sudah ada di pasaran.

---

## 1. Masalah, Dibuktikan dengan Data

| Indikator | Nilai | Sumber | Periode |
|---|---|---|---|
| OTP domestik kumulatif | **78,7%** (artinya ±21% penerbangan delay) | Kemenhub, RDP dengan Komisi V DPR RI | Jan–Apr 2025 |
| OTP domestik kumulatif tahun sebelumnya | 79,73% | Kemenhub | Jan–Apr 2024 |
| OTP domestik periode mudik Lebaran | 83% | Kemenhub (Ditjen Perhubungan Udara) | 21 Mar–11 Apr 2025 |
| OTP internasional (pembanding) | 91,88% | Kemenhub | periode sama |
| OTP Badan Usaha Angkutan Udara Berjadwal Dalam Negeri | 73,99% (26,01% delay) | Statistik Angkutan Udara 2023 | 2023 |
| Tren | **Menurun** dibanding tahun sebelumnya | Kemenhub | 2024→2025 |

**Penyebab dominan menurut Kemenhub:** faktor cuaca (paling dominan), disusul faktor teknis operasional dan manajemen maskapai. Pengamat penerbangan turut menyoroti minimnya armada cadangan, sehingga satu keterlambatan **merembet ke rute-rute berikutnya** ("keterlambatan berjamaah") — ini justru menjadi justifikasi langsung untuk fitur **Ripple Effect Score (RES)** yang sudah dibangun di sistem ini.

**Kesimpulan justifikasi masalah:** OTP domestik secara konsisten berada di bawah 85% dan tren-nya menurun tahun ke tahun. Ini bukan masalah musiman, melainkan persoalan struktural yang berulang setiap tahun — sehingga sistem prediksi proaktif (bukan sekadar pelaporan delay setelah terjadi) punya urgensi nyata.

---

## 2. Kerangka 5V — Mengapa Perlu Big Data

| Dimensi | Penjelasan pada konteks proyek ini |
|---|---|
| **Volume** | Indonesia memproses puluhan ribu pergerakan penerbangan domestik per hari di lebih dari 56 bandara yang dipantau Kemenhub, dengan jutaan penumpang per bulan (39,4 juta penumpang domestik Jan–Agu 2025 menurut BPS). Setiap pesawat aktif memancarkan posisi setiap beberapa detik via ADS-B/FR24 — data posisi pesawat saja sudah berskala ribuan record per menit secara nasional. |
| **Velocity** | Data posisi pesawat dan cuaca berubah real-time; sistem ini melakukan polling tiap 30 detik dari FlightRadar24 dan Open-Meteo, lalu diproses Spark Structured Streaming dengan micro-batch 10 detik. Delay tidak bisa diprediksi secara berguna dari data batch harian — perlu kecepatan streaming. |
| **Variety** | Sistem menggabungkan data heterogen: posisi/telemetri pesawat (lat/lon, altitude, speed, heading), cuaca (presipitasi, angin, visibilitas, kode cuaca WMO), metadata pesawat & maskapai (tipe, registrasi, kapasitas kursi), dan data temporal (jam, hari). Ini bukan satu sumber tabular tunggal. |
| **Veracity** | Data lapangan tidak selalu bersih: field bisa null, pesawat di darat ikut terbaca, nilai altitude/speed kadang di luar rentang masuk akal. Sistem menerapkan data cleaning eksplisit (filter, `fillna`, validasi rentang) sebelum data dipakai model — lihat `Spark/spark_processor.py` fungsi `parse_and_clean`. |
| **Value** | Output bukan sekadar "berapa menit delay", tapi insight yang bisa ditindaklanjuti: estimasi penumpang terdampak, estimasi biaya kompensasi (model EU261), Flight Delay Index, Ripple Effect Score, dan ranking performa maskapai — nilai yang relevan untuk operator bandara, maskapai, maupun penumpang.

---

## 3. Analisis Gap — Posisi terhadap Solusi yang Ada

| Aspek | FlightAware / FlightStats | Aplikasi tracking umum (mis. Flightradar24 publik) | Sistem ini |
|---|---|---|---|
| Prediksi delay | Ada (berbayar, fokus pasar AS/Eropa) | Tidak ada — hanya tracking posisi | Ada (RandomForestRegressor, real-time) |
| Cakupan rute Indonesia | Terbatas, tidak fokus domestik Indonesia | Cakupan posisi global, tanpa analisis delay | Fokus eksplisit pada konteks domestik (kode ICAO maskapai Indonesia: GIA, LNI, BTK, dll.) |
| Estimasi dampak penumpang & biaya | Tidak tersedia ke publik | Tidak ada | Ada — `affected_passengers`, `estimated_compensation_eur` |
| Skor dampak jaringan (ripple effect) | Tidak dipublikasikan | Tidak ada | Ada — Ripple Effect Score berbasis rotasi pesawat |
| Ranking transparansi performa maskapai | Tidak ada fitur publik setara | Tidak ada | Ada — Airline Performance Index |
| Model harga | Berbayar/API enterprise | Gratis tapi terbatas fitur tracking saja | Open, dibangun untuk kebutuhan riset/akademik |

**Gap yang coba ditutup:** layanan prediksi delay yang sudah ada umumnya hanya melaporkan estimasi delay itu sendiri, tanpa mengukur **dampak lanjutannya** terhadap jaringan penerbangan, penumpang, dan biaya operasional maskapai secara transparan dan real-time. Diferensiasi sistem ini ada di lapisan metrik turunan (FDI, RES, estimasi biaya, ranking maskapai), bukan semata akurasi prediksi delay.

⚠️ **Catatan jujur:** tabel di atas disusun dari pengetahuan umum tentang positioning produk-produk tersebut, bukan dari uji langsung berdampingan (head-to-head benchmark). Untuk laporan akhir yang lebih kuat, idealnya dilakukan pengecekan langsung ke situs/dokumentasi resmi tiap kompetitor.

---

## 4. Data yang Masih Dibutuhkan & Cara Mendapatkannya

Bagian ini transparan soal apa yang **belum** terintegrasi, supaya tidak diklaim sebagai sudah selesai saat sesi tanya-jawab.

| Data yang dibutuhkan | Kenapa penting | Cara mendapatkan |
|---|---|---|
| **Data OTP historis resmi per maskapai/rute** | Untuk melatih model dengan ground truth nyata (saat ini training pakai data sintetis) | Kemenhub via `sisfoangud.dephub.go.id`, atau API berbayar seperti OAG / Cirium yang menyediakan data OTP historis terstruktur |
| **NOTAM (Notice to Airmen) resmi** | Indikator gangguan operasional bandara/rute yang memengaruhi delay | AIM Indonesia (AirNav Indonesia) atau FAA NOTAM API untuk referensi internasional |
| **Kondisi runway real-time** (kepadatan, closure, maintenance) | Faktor langsung penyebab delay yang belum masuk model | AirNav Indonesia / data ATC, biasanya perlu kerja sama institusional |
| **Jadwal rotasi pesawat resmi (bukan hasil tracking)** | Saat ini RES dihitung dari hasil pengamatan posisi pesawat (Redis sorted set), bukan jadwal resmi maskapai | API maskapai (jika tersedia), atau OAG Schedules — umumnya berbayar |
| **Data harga tiket & load factor aktual** | Saat ini load factor memakai asumsi tetap 0,85, belum dari data riil | Data maskapai langsung atau API agregator tiket (umumnya tidak terbuka gratis) |
| **Validasi independen hasil prediksi vs delay aktual** | Untuk mengukur akurasi model di dunia nyata, bukan hanya RMSE/MAE pada data sintetis | Bandingkan output sistem dengan data realisasi dari Kemenhub/laporan maskapai pasca-penerbangan |

**Status sumber data saat ini yang sudah berjalan (gratis, real-time, tanpa API key berbayar):**
- FlightRadar24 feed publik (`data-cloud.flightradar24.com`) — posisi pesawat real-time
- Open-Meteo — data cuaca gratis tanpa API key

---
