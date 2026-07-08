import time
import manager

CHECK_INTERVAL = 5


def watch_shards():
    print("Watcher aktif. Mengecek shard setiap 5 detik...", flush=True)

    last_status = {}

    for shard_id in manager.SHARDS:
        is_alive = manager.is_shard_mysql_alive(shard_id)
        last_status[shard_id] = is_alive
        manager.update_single_shard_status(shard_id, is_alive)

        print(
            f"Status awal Shard {shard_id}: {'HIDUP' if is_alive else 'MATI'}",
            flush=True
        )

    while True:
        for shard_id in manager.SHARDS:
            try:
                current_status = manager.is_shard_mysql_alive(shard_id)
                previous_status = last_status.get(shard_id)

                if previous_status is True and current_status is False:
                    print(
                        f"DETEKSI: Shard {shard_id} MATI dari Docker Desktop.",
                        flush=True
                    )
                    manager.update_single_shard_status(shard_id, False)
                    manager.log_event(
                        "SHARD_DOWN_AUTO_WATCHER",
                        f"Shard {shard_id} mati dan terdeteksi otomatis oleh watcher",
                    )
                    manager.redistribute_dead_shard_data(shard_id)
                    last_status[shard_id] = False

                elif previous_status is False and current_status is True:
                    print(
                        f"DETEKSI: Shard {shard_id} HIDUP kembali dari Docker Desktop.",
                        flush=True
                    )
                    manager.update_single_shard_status(shard_id, True)
                    manager.log_event(
                        "SHARD_UP_AUTO_WATCHER",
                        f"Shard {shard_id} hidup kembali dan terdeteksi otomatis oleh watcher",
                    )
                    manager.handle_shard_up_event([shard_id])
                    last_status[shard_id] = True

                else:
                    last_status[shard_id] = current_status

            except Exception as e:
                print(f"Watcher error pada Shard {shard_id}: {e}", flush=True)
                
        manager.sync_new_master_rows()
        manager.sync_deleted_master_rows()        

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    watch_shards()