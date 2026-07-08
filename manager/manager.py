import csv
import os
import random
import time
from typing import Dict, List, Optional, Tuple

import docker
import mysql.connector

# ============================================================
# KONFIGURASI DATABASE
# ============================================================

MYSQL_USER = "root"
MYSQL_PASSWORD = "root"
MYSQL_DB = "uts_basisdataterdistribusi"
MASTER_HOST = "mysql-master"

TOTAL_SHARDS = 5

CSV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "mahasiswa.csv")

SHARDS: Dict[int, Dict[str, str]] = {
    1: {"host": "mysql-shard-1", "container": "utsbtd-mysql-shard-1"},
    2: {"host": "mysql-shard-2", "container": "utsbtd-mysql-shard-2"},
    3: {"host": "mysql-shard-3", "container": "utsbtd-mysql-shard-3"},
    4: {"host": "mysql-shard-4", "container": "utsbtd-mysql-shard-4"},
    5: {"host": "mysql-shard-5", "container": "utsbtd-mysql-shard-5"},
}

# Dipakai HANYA untuk membuat data tambahan saat demo insert (CSV asli statis).
# nim tambahan sengaja berprefix "90" supaya tidak pernah bentrok dengan nim CSV (berprefix 23xxx).
PRODI_LIST = [
    "Sistem Informasi",
    "Rekayasa Perangkat Lunak",
    "Pendidikan Teknik Informatika",
    "Ilmu Komputer",
]

ALAMAT_LIST = [
    "Badung", "Bangli", "Buleleng", "Denpasar", "Gianyar",
    "Jembrana", "Karangasem", "Klungkung", "Tabanan",
]

NAMA_DEPAN = [
    "Adi", "Ayu", "Bagus", "Citra", "Dewi", "Eka", "Gita", "Hendra", "Indah", "Jaya",
    "Kadek", "Komang", "Luh", "Made", "Nanda", "Oka", "Putu", "Ratna", "Sari", "Tia",
    "Wayan", "Yoga", "Dimas", "Raka", "Nita", "Agus", "Dika", "Lina", "Mira", "Rizky",
]

NAMA_BELAKANG = [
    "Pratama", "Prastika", "Saputra", "Dewi", "Wijaya", "Lestari", "Utami", "Permana", "Mahendra", "Sanjaya",
    "Wibawa", "Kusuma", "Nugraha", "Artini", "Suardana", "Wulandari", "Pradnyani", "Yasa", "Putri", "Wardana",
]

random.seed(42)

# Tipe row mahasiswa dari master:
# (id, nim, nama, prodi, semester, ipk, alamat, created_at)
StudentRow = Tuple[int, str, str, str, int, float, str, object]


# ============================================================
# BASIC HELPER
# ============================================================

def load_students_from_csv(path: str = CSV_PATH) -> List[dict]:
    """
    Membaca dataset mahasiswa dari CSV.
    Kolom wajib: nim, nama, prodi, semester, ipk, alamat, shard_id
    """
    students = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            students.append({
                "nim": row["nim"].strip(),
                "nama": row["nama"].strip(),
                "prodi": row["prodi"].strip(),
                "semester": int(row["semester"]),
                "ipk": float(row["ipk"]),
                "alamat": row["alamat"].strip(),
                "shard_id": int(row["shard_id"]),
            })
    return students


def generate_additional_student(number: int):
    """
    Dipakai untuk demo insert_data_event() (data tambahan, bukan dari CSV).
    nim berprefix '90' supaya tidak bentrok dengan nim asli CSV (prefix 23xxx).
    """
    nama = f"{random.choice(NAMA_DEPAN)} {random.choice(NAMA_BELAKANG)}"
    nim = f"90{number:06d}"
    prodi = random.choice(PRODI_LIST)
    semester = random.choice([2, 4, 6, 8])
    ipk = round(random.uniform(2.5, 4.0), 2)
    alamat = random.choice(ALAMAT_LIST)
    return nim, nama, prodi, semester, ipk, alamat


def connect_mysql(host: str, database: str = MYSQL_DB, retry: int = 40):
    for _ in range(retry):
        try:
            return mysql.connector.connect(
                host=host,
                user=MYSQL_USER,
                password=MYSQL_PASSWORD,
                database=database,
                autocommit=True,
                connection_timeout=3,
            )
        except Exception:
            time.sleep(2)

    raise Exception(f"Gagal konek ke MySQL host: {host}")


def execute(conn, query: str, params=None):
    cur = conn.cursor()
    cur.execute(query, params or ())
    cur.close()


def fetchone(conn, query: str, params=None):
    cur = conn.cursor()
    cur.execute(query, params or ())
    result = cur.fetchone()
    cur.close()
    return result


def fetchall(conn, query: str, params=None):
    cur = conn.cursor()
    cur.execute(query, params or ())
    result = cur.fetchall()
    cur.close()
    return result


def log_event(event_type: str, detail: str):
    conn = connect_mysql(MASTER_HOST)
    cur = conn.cursor()
    cur.execute(
        "INSERT INTO events(event_type, detail) VALUES (%s, %s)",
        (event_type, detail[:255]),
    )
    cur.close()
    conn.close()
    print(f"[EVENT] {event_type} -> {detail}")


# ============================================================
# SETUP SCHEMA
# ============================================================

def setup_master_schema():
    conn = connect_mysql(MASTER_HOST)

    execute(conn, "DROP TABLE IF EXISTS pending_sync")
    execute(conn, "DROP TABLE IF EXISTS shard_map")
    execute(conn, "DROP TABLE IF EXISTS shard_status")
    execute(conn, "DROP TABLE IF EXISTS events")
    execute(conn, "DROP TABLE IF EXISTS master_data")

    execute(conn, """
        CREATE TABLE master_data (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nim VARCHAR(20) UNIQUE,
            nama VARCHAR(100),
            prodi VARCHAR(100),
            semester INT,
            ipk DECIMAL(3,2),
            alamat VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    execute(conn, """
        CREATE TABLE shard_status (
            shard_id INT PRIMARY KEY,
            container_name VARCHAR(100),
            is_active TINYINT DEFAULT 1,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        )
    """)

    execute(conn, """
        CREATE TABLE shard_map (
            data_id INT PRIMARY KEY,
            shard_id INT NOT NULL,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        )
    """)

    execute(conn, """
        CREATE TABLE pending_sync (
            id INT AUTO_INCREMENT PRIMARY KEY,
            round_no INT,
            operation_type VARCHAR(20) DEFAULT 'INSERT',
            data_id INT,
            target_shard_id INT,
            current_shard_id INT NULL,
            detail VARCHAR(255),
            synced TINYINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            synced_at TIMESTAMP NULL
        )
    """)

    execute(conn, """
        CREATE TABLE events (
            id INT AUTO_INCREMENT PRIMARY KEY,
            event_type VARCHAR(50),
            detail VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    for shard_id, info in SHARDS.items():
        execute(
            conn,
            """
            INSERT INTO shard_status(shard_id, container_name, is_active)
            VALUES (%s, %s, 1)
            """,
            (shard_id, info["container"]),
        )

    conn.close()


def setup_shard_schema(shard_id: int):
    conn = connect_mysql(SHARDS[shard_id]["host"])
    execute(conn, "DROP TABLE IF EXISTS shard_data")
    execute(conn, """
        CREATE TABLE shard_data (
            id INT PRIMARY KEY,
            nim VARCHAR(20),
            nama VARCHAR(100),
            prodi VARCHAR(100),
            semester INT,
            ipk DECIMAL(3,2),
            alamat VARCHAR(100),
            master_created_at TIMESTAMP NULL,
            synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                ON UPDATE CURRENT_TIMESTAMP
        )
    """)
    conn.close()


# ============================================================
# STATUS SHARD DAN EVENT CHECKING
# ============================================================

def is_shard_mysql_alive(shard_id: int) -> bool:
    try:
        conn = mysql.connector.connect(
            host=SHARDS[shard_id]["host"],
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB,
            autocommit=True,
            connection_timeout=2,
        )
        conn.close()
        return True
    except Exception:
        return False


def update_single_shard_status(shard_id: int, is_active: bool):
    conn = connect_mysql(MASTER_HOST)
    execute(
        conn,
        "UPDATE shard_status SET is_active = %s WHERE shard_id = %s",
        (1 if is_active else 0, shard_id),
    )
    conn.close()


def update_shard_status(shard_ids: List[int], is_active: bool):
    for shard_id in shard_ids:
        update_single_shard_status(shard_id, is_active)


def refresh_shard_status_from_real_condition():
    for shard_id in SHARDS:
        real_active = is_shard_mysql_alive(shard_id)

        conn = connect_mysql(MASTER_HOST)
        row = fetchone(
            conn,
            "SELECT is_active FROM shard_status WHERE shard_id = %s",
            (shard_id,),
        )
        conn.close()

        db_active = bool(row[0]) if row else False

        if real_active != db_active:
            update_single_shard_status(shard_id, real_active)

            if real_active:
                log_event(
                    "SHARD_UP_AUTO",
                    f"Shard {shard_id} terdeteksi hidup kembali oleh manager",
                )
                handle_shard_up_event([shard_id])
            else:
                log_event(
                    "SHARD_DOWN_AUTO",
                    f"Shard {shard_id} terdeteksi mati oleh manager",
                )


def get_active_shards() -> List[int]:
    refresh_shard_status_from_real_condition()

    conn = connect_mysql(MASTER_HOST)
    rows = fetchall(
        conn,
        "SELECT shard_id FROM shard_status WHERE is_active = 1 ORDER BY shard_id",
    )
    conn.close()
    return [row[0] for row in rows]


def get_inactive_shards() -> List[int]:
    refresh_shard_status_from_real_condition()

    conn = connect_mysql(MASTER_HOST)
    rows = fetchall(
        conn,
        "SELECT shard_id FROM shard_status WHERE is_active = 0 ORDER BY shard_id",
    )
    conn.close()
    return [row[0] for row in rows]


# ============================================================
# OPERASI DATA
# ============================================================

def get_master_rows() -> List[StudentRow]:
    conn = connect_mysql(MASTER_HOST)
    rows = fetchall(
        conn,
        """
        SELECT id, nim, nama, prodi, semester, ipk, alamat, created_at
        FROM master_data
        ORDER BY id
        """,
    )
    conn.close()
    return rows


def get_master_row_by_id(data_id: int) -> Optional[StudentRow]:
    conn = connect_mysql(MASTER_HOST)
    row = fetchone(
        conn,
        """
        SELECT id, nim, nama, prodi, semester, ipk, alamat, created_at
        FROM master_data
        WHERE id = %s
        """,
        (data_id,),
    )
    conn.close()
    return row


def get_shard_row_by_id(shard_id: int, data_id: int):
    conn = connect_mysql(SHARDS[shard_id]["host"])
    row = fetchone(
        conn,
        """
        SELECT id, nim, nama, prodi, semester, ipk, alamat, master_created_at
        FROM shard_data
        WHERE id = %s
        """,
        (data_id,),
    )
    conn.close()
    return row


def insert_row_to_shard_cursor(cursor, row):
    data_id, nim, nama, prodi, semester, ipk, alamat, created_at = row
    cursor.execute(
        """
        INSERT INTO shard_data(
            id, nim, nama, prodi, semester, ipk, alamat, master_created_at
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            nim = VALUES(nim),
            nama = VALUES(nama),
            prodi = VALUES(prodi),
            semester = VALUES(semester),
            ipk = VALUES(ipk),
            alamat = VALUES(alamat),
            master_created_at = VALUES(master_created_at),
            synced_at = CURRENT_TIMESTAMP
        """,
        (data_id, nim, nama, prodi, semester, ipk, alamat, created_at),
    )


def upsert_row_to_shard(shard_id: int, row):
    conn = connect_mysql(SHARDS[shard_id]["host"])
    cur = conn.cursor()
    insert_row_to_shard_cursor(cur, row)
    cur.close()
    conn.close()


def delete_row_from_shard(shard_id: int, data_id: int):
    conn = connect_mysql(SHARDS[shard_id]["host"])
    execute(conn, "DELETE FROM shard_data WHERE id = %s", (data_id,))
    conn.close()


def update_shard_map_location(data_id: int, shard_id: int):
    conn = connect_mysql(MASTER_HOST)
    execute(
        conn,
        """
        INSERT INTO shard_map(data_id, shard_id)
        VALUES (%s, %s)
        ON DUPLICATE KEY UPDATE shard_id = VALUES(shard_id)
        """,
        (data_id, shard_id),
    )
    conn.close()


def get_data_current_shard(data_id: int) -> Optional[int]:
    conn = connect_mysql(MASTER_HOST)
    row = fetchone(conn, "SELECT shard_id FROM shard_map WHERE data_id = %s", (data_id,))
    conn.close()
    return row[0] if row else None


def count_master() -> int:
    conn = connect_mysql(MASTER_HOST)
    total = fetchone(conn, "SELECT COUNT(*) FROM master_data")[0]
    conn.close()
    return total


def count_shard(shard_id: int) -> int:
    conn = connect_mysql(SHARDS[shard_id]["host"])
    total = fetchone(conn, "SELECT COUNT(*) FROM shard_data")[0]
    conn.close()
    return total


def count_pending() -> int:
    conn = connect_mysql(MASTER_HOST)
    total = fetchone(conn, "SELECT COUNT(*) FROM pending_sync WHERE synced = 0")[0]
    conn.close()
    return total


def truncate_all_shards():
    for shard_id in SHARDS:
        conn = connect_mysql(SHARDS[shard_id]["host"])
        execute(conn, "TRUNCATE TABLE shard_data")
        conn.close()


# ============================================================
# SETUP DATA AWAL DARI CSV (menggantikan generator random)
# ============================================================

def setup_initial_data_from_csv(csv_path: str = CSV_PATH):
    """
    Membaca data mahasiswa dari CSV (termasuk shard_id yang sudah ditentukan
    di file CSV), memasukkannya ke master_data, lalu langsung mendistribusikan
    tiap baris ke shard sesuai kolom shard_id di CSV.
    """
    students = load_students_from_csv(csv_path)
    print(f"Membaca {len(students)} baris data dari {csv_path}")

    master_conn = connect_mysql(MASTER_HOST)
    master_cursor = master_conn.cursor()

    shard_conns = {}
    shard_cursors = {}
    for shard_id in SHARDS:
        shard_conns[shard_id] = connect_mysql(SHARDS[shard_id]["host"])
        shard_cursors[shard_id] = shard_conns[shard_id].cursor()

    for s in students:
        master_cursor.execute(
            """
            INSERT INTO master_data(nim, nama, prodi, semester, ipk, alamat)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (s["nim"], s["nama"], s["prodi"], s["semester"], s["ipk"], s["alamat"]),
        )
        data_id = master_cursor.lastrowid

        row = fetchone(
            master_conn,
            """
            SELECT id, nim, nama, prodi, semester, ipk, alamat, created_at
            FROM master_data
            WHERE id = %s
            """,
            (data_id,),
        )

        target_shard = s["shard_id"]
        insert_row_to_shard_cursor(shard_cursors[target_shard], row)

        master_cursor.execute(
            """
            INSERT INTO shard_map(data_id, shard_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE shard_id = VALUES(shard_id)
            """,
            (data_id, target_shard),
        )

    master_cursor.close()
    master_conn.close()

    for shard_id in SHARDS:
        shard_cursors[shard_id].close()
        shard_conns[shard_id].close()

    log_event(
        "INITIAL_LOAD_FROM_CSV",
        f"{len(students)} data mahasiswa dimuat dari CSV dan didistribusikan sesuai shard_id di CSV",
    )


# ============================================================
# INSERT EVENT-DRIVEN (data tambahan, bukan dari CSV)
# ============================================================

def insert_data_event(total: int = 50, round_no: int = 0, forced_target_shard: Optional[int] = None):
    active_shards = get_active_shards()
    inactive_shards = get_inactive_shards()

    print("\n" + "=" * 70)
    print("EVENT INSERT DATA KE MASTER")
    print("=" * 70)
    print(f"Jumlah data masuk: {total}")
    print(f"Shard aktif: {active_shards}")
    print(f"Shard mati : {inactive_shards}")

    master_conn = connect_mysql(MASTER_HOST)
    master_cursor = master_conn.cursor()

    # basis nomor urut nim tambahan, supaya tidak pernah bentrok antar-run
    existing_extra = fetchone(
        master_conn,
        "SELECT COUNT(*) FROM master_data WHERE nim LIKE '90%%'",
    )[0]
    start_number = existing_extra + 1

    shard_loads = {}
    shard_conns = {}
    shard_cursors = {}

    for shard_id in active_shards:
        shard_loads[shard_id] = count_shard(shard_id)
        shard_conns[shard_id] = connect_mysql(SHARDS[shard_id]["host"])
        shard_cursors[shard_id] = shard_conns[shard_id].cursor()

    for i in range(total):
        nim, nama, prodi, semester, ipk, alamat = generate_additional_student(start_number + i)

        master_cursor.execute(
            """
            INSERT INTO master_data(nim, nama, prodi, semester, ipk, alamat)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (nim, nama, prodi, semester, ipk, alamat),
        )

        data_id = master_cursor.lastrowid

        row = fetchone(
            master_conn,
            """
            SELECT id, nim, nama, prodi, semester, ipk, alamat, created_at
            FROM master_data
            WHERE id = %s
            """,
            (data_id,),
        )

        if forced_target_shard is not None:
            ideal_shard = forced_target_shard
        else:
            ideal_shard = ((data_id - 1) % TOTAL_SHARDS) + 1

        if not active_shards:
            master_cursor.execute(
                """
                INSERT INTO pending_sync(
                    round_no, operation_type, data_id,
                    target_shard_id, current_shard_id, detail
                )
                VALUES (%s, 'INSERT', %s, %s, NULL, %s)
                """,
                (
                    round_no,
                    data_id,
                    ideal_shard,
                    f"Data {data_id} sudah masuk master, tetapi semua shard mati. Target asli Shard {ideal_shard}",
                ),
            )
            print(f"Data {data_id} masuk master, pending karena semua shard mati")
            continue

        if ideal_shard in active_shards:
            selected_shard = ideal_shard
        else:
            selected_shard = min(shard_loads, key=shard_loads.get)

        insert_row_to_shard_cursor(shard_cursors[selected_shard], row)
        shard_loads[selected_shard] += 1

        master_cursor.execute(
            """
            INSERT INTO shard_map(data_id, shard_id)
            VALUES (%s, %s)
            ON DUPLICATE KEY UPDATE shard_id = VALUES(shard_id)
            """,
            (data_id, selected_shard),
        )

        if selected_shard != ideal_shard:
            master_cursor.execute(
                """
                INSERT INTO pending_sync(
                    round_no, operation_type, data_id,
                    target_shard_id, current_shard_id, detail
                )
                VALUES (%s, 'INSERT', %s, %s, %s, %s)
                """,
                (
                    round_no,
                    data_id,
                    ideal_shard,
                    selected_shard,
                    f"Data {data_id} sementara masuk Shard {selected_shard}, target asli Shard {ideal_shard}",
                ),
            )

    for shard_id in active_shards:
        shard_cursors[shard_id].close()
        shard_conns[shard_id].close()

    master_cursor.close()
    master_conn.close()

    log_event(
        "INSERT_DATA_EVENT",
        f"{total} data masuk ke master. Shard aktif saat insert: {active_shards}",
    )

    show_counts("SETELAH INSERT EVENT")


# ============================================================
# DELETE BULK UNTUK DEMO GRAFANA
# ============================================================

def delete_bulk_data(total: int = 50):
    print("\n" + "=" * 70)
    print(f"DELETE BULK {total} DATA TERAKHIR")
    print("=" * 70)

    active_shards = get_active_shards()
    master_conn = connect_mysql(MASTER_HOST)

    rows = fetchall(
        master_conn,
        "SELECT id FROM master_data ORDER BY id DESC LIMIT %s",
        (total,),
    )

    if not rows:
        print("Tidak ada data yang bisa dihapus.")
        master_conn.close()
        return

    deleted_count = 0

    for (data_id,) in rows:
        current_shard_id = get_data_current_shard(data_id)

        if current_shard_id is not None and current_shard_id in active_shards:
            delete_row_from_shard(current_shard_id, data_id)
        elif current_shard_id is not None:
            print(
                f"Data {data_id} ada di Shard {current_shard_id}, tetapi shard sedang mati. "
                "Data master tetap dihapus, shard akan perlu sinkronisasi manual bila diperlukan."
            )

        execute(master_conn, "DELETE FROM pending_sync WHERE data_id = %s", (data_id,))
        execute(master_conn, "DELETE FROM shard_map WHERE data_id = %s", (data_id,))
        execute(master_conn, "DELETE FROM master_data WHERE id = %s", (data_id,))

        print(f"Data ID {data_id} berhasil dihapus")
        deleted_count += 1

    master_conn.close()

    log_event("DELETE_BULK_DATA", f"{deleted_count} data terakhir dihapus dari master dan shard")
    show_counts("SETELAH DELETE BULK")


# ============================================================
# AUTO SYNC PENDING SAAT SHARD HIDUP KEMBALI
# ============================================================

def sync_pending_for_shard(target_shard_id: int):
    print("\n" + "=" * 70)
    print(f"AUTO SYNC PENDING UNTUK SHARD {target_shard_id}")
    print("=" * 70)

    master_conn = connect_mysql(MASTER_HOST)

    pending_rows = fetchall(
        master_conn,
        """
        SELECT id, data_id, target_shard_id, current_shard_id
        FROM pending_sync
        WHERE target_shard_id = %s AND synced = 0 AND operation_type = 'INSERT'
        ORDER BY data_id
        """,
        (target_shard_id,),
    )

    if not pending_rows:
        print(f"Tidak ada pending sync untuk Shard {target_shard_id}.")
        master_conn.close()
        return

    print(f"Jumlah pending sync: {len(pending_rows)}")
    master_cursor = master_conn.cursor()

    for pending_id, data_id, target_id, current_shard_id in pending_rows:
        row = None

        if current_shard_id is not None and current_shard_id != target_shard_id:
            try:
                row = get_shard_row_by_id(current_shard_id, data_id)
            except Exception:
                row = None

        if row is None:
            row = get_master_row_by_id(data_id)

        if row is None:
            print(f"Data {data_id} tidak ditemukan, dilewati.")
            continue

        upsert_row_to_shard(target_shard_id, row)

        if current_shard_id is not None and current_shard_id != target_shard_id:
            try:
                delete_row_from_shard(current_shard_id, data_id)
            except Exception:
                pass

        update_shard_map_location(data_id, target_shard_id)

        master_cursor.execute(
            "UPDATE pending_sync SET synced = 1, synced_at = CURRENT_TIMESTAMP WHERE id = %s",
            (pending_id,),
        )

        print(f"Data {data_id} dipindahkan ke Shard {target_shard_id}")

    master_cursor.close()
    master_conn.close()

    log_event(
        "AUTO_SYNC_PENDING",
        f"Pending sync untuk Shard {target_shard_id} berhasil diproses otomatis",
    )


# ============================================================
# INCREMENTAL LOAD BALANCING TANPA TRUNCATE
# ============================================================

def incremental_rebalance_without_truncate():
    print("\n" + "=" * 70)
    print("AUTO LOAD BALANCING TANPA MENGOSONGKAN SHARD")
    print("=" * 70)

    active_shards = get_active_shards()

    if not active_shards:
        print("Tidak ada shard aktif untuk balancing.")
        return

    shard_counts = {shard_id: count_shard(shard_id) for shard_id in active_shards}

    print("Jumlah data sebelum balancing:")
    for shard_id, total in shard_counts.items():
        print(f"Shard {shard_id}: {total} data")

    total_data = sum(shard_counts.values())
    jumlah_shard = len(active_shards)
    target_per_shard = total_data // jumlah_shard
    sisa = total_data % jumlah_shard

    targets = {shard_id: target_per_shard for shard_id in active_shards}
    for index in range(sisa):
        targets[active_shards[index]] += 1

    print("\nTarget jumlah data:")
    for shard_id, target in targets.items():
        print(f"Shard {shard_id}: {target} data")

    while True:
        donor = None
        receiver = None

        for shard_id in active_shards:
            if shard_counts[shard_id] > targets[shard_id]:
                donor = shard_id
                break

        for shard_id in active_shards:
            if shard_counts[shard_id] < targets[shard_id]:
                receiver = shard_id
                break

        if donor is None or receiver is None:
            break

        excess = shard_counts[donor] - targets[donor]
        deficit = targets[receiver] - shard_counts[receiver]
        move_count = min(excess, deficit)

        print(f"Memindahkan {move_count} data dari Shard {donor} ke Shard {receiver}")

        donor_conn = connect_mysql(SHARDS[donor]["host"])
        donor_cursor = donor_conn.cursor()
        donor_cursor.execute(
            """
            SELECT id, nim, nama, prodi, semester, ipk, alamat, master_created_at
            FROM shard_data
            ORDER BY id DESC
            LIMIT %s
            """,
            (move_count,),
        )
        rows = donor_cursor.fetchall()

        for row in rows:
            data_id = row[0]
            upsert_row_to_shard(receiver, row)
            delete_row_from_shard(donor, data_id)
            update_shard_map_location(data_id, receiver)
            shard_counts[donor] -= 1
            shard_counts[receiver] += 1

        donor_cursor.close()
        donor_conn.close()

    log_event(
        "AUTO_INCREMENTAL_REBALANCE",
        "Data antar-shard diseimbangkan otomatis tanpa truncate",
    )

    print("\nJumlah data setelah balancing:")
    for shard_id in active_shards:
        print(f"Shard {shard_id}: {count_shard(shard_id)} data")


def distribute_pending_to_active_shards():
    print("\n" + "=" * 70)
    print("DISTRIBUSI PENDING KE SHARD AKTIF SEMENTARA")
    print("=" * 70)

    active_shards = get_active_shards()

    if not active_shards:
        print("Tidak ada shard aktif. Pending belum bisa dibagikan.")
        return

    master_conn = connect_mysql(MASTER_HOST)

    pending_rows = fetchall(
        master_conn,
        """
        SELECT id, data_id, target_shard_id
        FROM pending_sync
        WHERE synced = 0 AND current_shard_id IS NULL AND operation_type = 'INSERT'
        ORDER BY data_id
        """,
    )

    if not pending_rows:
        print("Tidak ada pending dengan current_shard_id NULL.")
        master_conn.close()
        return

    print(f"Jumlah pending yang akan dibagikan sementara: {len(pending_rows)}")
    print(f"Shard aktif: {active_shards}")

    shard_loads = {}
    shard_conns = {}
    shard_cursors = {}

    for shard_id in active_shards:
        shard_loads[shard_id] = count_shard(shard_id)
        shard_conns[shard_id] = connect_mysql(SHARDS[shard_id]["host"])
        shard_cursors[shard_id] = shard_conns[shard_id].cursor()

    master_cursor = master_conn.cursor()

    for pending_id, data_id, target_shard_id in pending_rows:
        row = get_master_row_by_id(data_id)

        if row is None:
            print(f"Data {data_id} tidak ditemukan di master, dilewati.")
            continue

        selected_shard = min(shard_loads, key=shard_loads.get)
        insert_row_to_shard_cursor(shard_cursors[selected_shard], row)
        shard_loads[selected_shard] += 1
        update_shard_map_location(data_id, selected_shard)

        master_cursor.execute(
            """
            UPDATE pending_sync
            SET current_shard_id = %s, detail = %s
            WHERE id = %s
            """,
            (
                selected_shard,
                f"Data {data_id} sementara dibagikan ke Shard {selected_shard}, target asli Shard {target_shard_id}",
                pending_id,
            ),
        )

        print(f"Data {data_id} sementara masuk Shard {selected_shard}, target asli Shard {target_shard_id}")

    for shard_id in active_shards:
        shard_cursors[shard_id].close()
        shard_conns[shard_id].close()

    master_cursor.close()
    master_conn.close()

    log_event(
        "DISTRIBUTE_PENDING_TEMP",
        "Pending yang belum masuk shard dibagikan sementara ke shard aktif",
    )


# ============================================================
# EVENT HANDLER
# ============================================================

def handle_shard_up_event(shard_ids: List[int]):
    print("\n" + "=" * 70)
    print(f"EVENT HANDLER: SHARD UP {shard_ids}")
    print("=" * 70)

    for shard_id in shard_ids:
        sync_pending_for_shard(shard_id)

    distribute_pending_to_active_shards()
    incremental_rebalance_without_truncate()


# ============================================================
# SHARD CONTROL
# ============================================================

def stop_shards(shard_ids, round_no=0):
    client = docker.from_env()
    print(f"\nMematikan shard: {shard_ids}")

    for shard_id in shard_ids:
        container = client.containers.get(SHARDS[shard_id]["container"])
        container.stop(timeout=10)

    update_shard_status(shard_ids, False)
    log_event("SHARD_DOWN", f"Shard {shard_ids} dimatikan")

    for shard_id in shard_ids:
        redistribute_dead_shard_data(shard_id)

    show_counts(f"SETELAH SHARD {shard_ids} MATI DAN DATA DIREDISTRIBUSIKAN")


def start_shards(shard_ids: List[int], round_no: int = 0):
    client = docker.from_env()
    print(f"\nMenyalakan shard: {shard_ids}")

    for shard_id in shard_ids:
        container = client.containers.get(SHARDS[shard_id]["container"])
        container.start()

    for shard_id in shard_ids:
        while not is_shard_mysql_alive(shard_id):
            print(f"Menunggu Shard {shard_id} siap...")
            time.sleep(2)

    update_shard_status(shard_ids, True)
    log_event("SHARD_UP", f"Shard {shard_ids} hidup kembali")

    handle_shard_up_event(shard_ids)
    show_counts(f"SETELAH SHARD {shard_ids} HIDUP DAN AUTO SYNC")


# ============================================================
# OUTPUT / VERIFIKASI
# ============================================================

def show_counts(title: str = "STATUS DATA"):
    print("\n" + "=" * 70)
    print(title)
    print("=" * 70)

    print(f"Master       : {count_master()} data")

    conn = connect_mysql(MASTER_HOST)
    statuses = fetchall(
        conn,
        "SELECT shard_id, is_active FROM shard_status ORDER BY shard_id",
    )
    conn.close()

    for shard_id, is_active in statuses:
        if is_active == 0:
            print(f"Shard {shard_id}     : OFFLINE / ERROR")
        else:
            print(f"Shard {shard_id}     : {count_shard(shard_id)} data")

    print(f"Pending sync : {count_pending()} data")


def show_events(limit: Optional[int] = None):
    conn = connect_mysql(MASTER_HOST)

    if limit is None:
        rows = fetchall(conn, "SELECT id, event_type, detail, created_at FROM events ORDER BY id")
    else:
        rows = fetchall(
            conn,
            "SELECT id, event_type, detail, created_at FROM events ORDER BY id DESC LIMIT %s",
            (limit,),
        )

    conn.close()

    print("\n" + "=" * 70)
    print("LOG EVENT SYSTEM MANAGEMENT")
    print("=" * 70)

    for row in rows:
        print(f"{row[0]}. [{row[1]}] {row[2]} - {row[3]}")


def show_sample_data(limit: int = 10):
    print("\n" + "=" * 70)
    print(f"CONTOH {limit} DATA MASTER")
    print("=" * 70)

    conn = connect_mysql(MASTER_HOST)
    rows = fetchall(
        conn,
        """
        SELECT id, nim, nama, prodi, semester, ipk, alamat
        FROM master_data
        ORDER BY id
        LIMIT %s
        """,
        (limit,),
    )
    conn.close()

    for row in rows:
        print(row)


def final_verification():
    print("\n" + "=" * 70)
    print("VERIFIKASI AKHIR")
    print("=" * 70)

    master_total = count_master()
    print(f"Total data master: {master_total}")

    total_shard_data = 0
    for shard_id in SHARDS:
        try:
            shard_total = count_shard(shard_id)
            total_shard_data += shard_total
            print(f"Total data Shard {shard_id}: {shard_total}")
        except Exception:
            print(f"Total data Shard {shard_id}: OFFLINE")

    print(f"Total data semua shard aktif: {total_shard_data}")
    print(f"Pending sync belum selesai: {count_pending()}")


# ============================================================
# REDISTRIBUSI SAAT SHARD MATI
# ============================================================

def redistribute_dead_shard_data(dead_shard_id):
    print("\n" + "=" * 70)
    print(f"REDISTRIBUSI DATA SHARD {dead_shard_id} YANG MATI")
    print("=" * 70)

    active_shards = []

    for shard_id in SHARDS:
        if shard_id != dead_shard_id and is_shard_mysql_alive(shard_id):
            active_shards.append(shard_id)
            update_single_shard_status(shard_id, True)

    update_single_shard_status(dead_shard_id, False)

    if not active_shards:
        print("Tidak ada shard aktif untuk menerima redistribusi data.")
        return

    master_conn = connect_mysql(MASTER_HOST)

    rows = fetchall(
        master_conn,
        "SELECT data_id FROM shard_map WHERE shard_id = %s ORDER BY data_id",
        (dead_shard_id,),
    )

    if not rows:
        print(f"Tidak ada data yang terdaftar pada Shard {dead_shard_id}.")
        master_conn.close()
        return

    print(f"Jumlah data yang akan disebar ulang: {len(rows)}")
    print(f"Shard aktif penerima: {active_shards}")

    shard_loads = {}
    shard_conns = {}
    shard_cursors = {}

    for shard_id in active_shards:
        shard_loads[shard_id] = count_shard(shard_id)
        shard_conns[shard_id] = connect_mysql(SHARDS[shard_id]["host"])
        shard_cursors[shard_id] = shard_conns[shard_id].cursor()

    master_cursor = master_conn.cursor()
    moved_count = 0

    for (data_id,) in rows:
        row = get_master_row_by_id(data_id)

        if row is None:
            print(f"Data {data_id} tidak ditemukan di master, dilewati.")
            continue

        selected_shard = min(shard_loads, key=shard_loads.get)

        insert_row_to_shard_cursor(shard_cursors[selected_shard], row)
        shard_loads[selected_shard] += 1

        master_cursor.execute(
            "UPDATE shard_map SET shard_id = %s WHERE data_id = %s",
            (selected_shard, data_id),
        )

        master_cursor.execute(
            """
            INSERT INTO pending_sync(round_no, data_id, target_shard_id, current_shard_id, detail)
            VALUES (%s, %s, %s, %s, %s)
            """,
            (
                0,
                data_id,
                dead_shard_id,
                selected_shard,
                f"Data {data_id} dari Shard {dead_shard_id} disebar sementara ke Shard {selected_shard} karena shard asal mati",
            ),
        )

        print(f"Data {data_id} dari Shard {dead_shard_id} disebar sementara ke Shard {selected_shard}")
        moved_count += 1

    for shard_id in active_shards:
        shard_cursors[shard_id].close()
        shard_conns[shard_id].close()

    master_cursor.close()
    master_conn.close()

    log_event(
        "REDISTRIBUTE_DEAD_SHARD",
        f"{moved_count} data dari Shard {dead_shard_id} disebar otomatis ke shard aktif"
    )

    show_counts(f"SETELAH REDISTRIBUSI DATA SHARD {dead_shard_id}")


# ============================================================
# DEMO UTAMA
# ============================================================

def setup_initial_data():
    print("\nMenunggu semua database siap...")
    connect_mysql(MASTER_HOST).close()
    for shard_id in SHARDS:
        connect_mysql(SHARDS[shard_id]["host"]).close()

    print("Semua database sudah siap.")

    print("\nSetup schema master dan shard...")
    setup_master_schema()
    for shard_id in SHARDS:
        setup_shard_schema(shard_id)

    print(f"\nMemuat data mahasiswa dari CSV ({CSV_PATH})...")
    setup_initial_data_from_csv()

    show_counts("KONDISI AWAL SETELAH LOAD CSV")
    show_sample_data(10)
    final_verification()


def main():
    setup_initial_data()


if __name__ == "__main__":
    main()