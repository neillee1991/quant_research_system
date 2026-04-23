import sys
from pathlib import Path
backend_path = Path(__file__).parent.parent / "backend"
sys.path.insert(0, str(backend_path))
import multiprocessing
from engine.script.sandbox import execute_sandbox, SandboxConfig
import time

print(f"Python version: {sys.version}")
print(f"Multiprocessing start method: {multiprocessing.get_start_method()}")

# 测试超时
config = SandboxConfig(timeout_seconds=1, max_memory_mb=256)
code = "import time; time.sleep(2)"
print("Testing timeout...")
start = time.time()
result = execute_sandbox(code, config=config)
print(f"Execution time: {time.time() - start:.2f} seconds")
print(f"Success: {result.success}")
print(f"Error: {repr(result.error)}")
