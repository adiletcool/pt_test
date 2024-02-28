import json
import multiprocessing as mp
import pathlib
import time

from jsf import JSF
from tqdm import tqdm

path = pathlib.Path(__file__).parent.resolve()
OUTPUT_FILENAME = f"{path}/sample.txt"
JSF_FILENAME = f"{path}/sample_model.json"
N_PROCESSES = mp.cpu_count()


def process_generated_data(tqdm_pos: int, queue: mp.Queue, n_lines: int):
    desc = f"Prorcess {tqdm_pos}"
    generator = JSF.from_json(JSF_FILENAME)

    for _ in tqdm(range(n_lines), desc=desc, position=tqdm_pos + 1, leave=False):
        generated_data = generator.generate()
        queue.put(generated_data)


def write_sample(queue: mp.Queue):
    with open(OUTPUT_FILENAME, "w") as f:
        while True:
            generated_data = queue.get()
            if generated_data == "END":
                break
            f.write(json.dumps(generated_data) + "\n")
            f.flush()


def generate_data(n_lines=10_000):
    manager = mp.Manager()
    queue = manager.Queue()
    now = time.time()
    listen_process = mp.Process(target=write_sample, args=(queue,))
    listen_process.start()

    print(f"Pool size: {N_PROCESSES}")
    jobs = []
    n_lines_per_process = n_lines // N_PROCESSES

    tqdm.set_lock(mp.RLock())

    with mp.Pool(
        processes=N_PROCESSES,
        initializer=tqdm.set_lock,
        initargs=(tqdm.get_lock(),),
    ) as pool:
        for i in range(N_PROCESSES):
            job = pool.apply_async(process_generated_data, args=(i, queue, n_lines_per_process))
            jobs.append(job)

        for job in tqdm(jobs, position=0, desc="Processes completed", leave=True):
            job.get()

    queue.put("END")
    listen_process.join()

    tqdm.write(f"Total time: {time.time() - now:.2f} seconds. Generated {n_lines:,} lines.")


if __name__ == "__main__":
    generate_data()
