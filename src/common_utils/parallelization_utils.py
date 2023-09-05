def make_run_batches(payload_list, batch_size, id_prefix):
    current_id = 0
    for batch_index in range(0, len(payload_list), batch_size):
        run_batch = payload_list[batch_index : batch_index + batch_size]
        run_id = f"{id_prefix}-{current_id}"
        current_id += 1
        yield run_id, run_batch
