# Image Airflow resmi (Apache), bukan Astro Runtime -- lihat catatan di README
# soal kenapa dipilih docker-compose resmi Airflow, bukan `astro dev start`,
# untuk mendapatkan CeleryExecutor + Redis di lingkungan lokal/Codespaces.
FROM apache/airflow:3.0.2-python3.11

COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt
