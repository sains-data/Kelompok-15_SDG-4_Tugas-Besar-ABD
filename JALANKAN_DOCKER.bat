@echo off
echo ==============================================
echo MENJALANKAN DOCKER UNTUK TUBES ABD
echo ==============================================
echo Menghentikan container lama (jika ada)...
docker compose down

echo.
echo Membangun ulang dan menjalankan container...
docker compose up --build -d

echo.
echo Selesai! Docker sedang berjalan di background.
echo Silakan tunggu beberapa menit sampai semua sistem siap.
echo Airflow bisa diakses di http://localhost:8081
echo Superset bisa diakses di http://localhost:8088
echo ==============================================
pause
