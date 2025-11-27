import numpy as np
import os

file_path = "sliding_5m_5d_20211029.npz"

if not os.path.exists(file_path):
    # Try searching recursively if not in root
    for root, dirs, files in os.walk("."):
        if file_path in files:
            file_path = os.path.join(root, file_path)
            break

print(f"Inspecting: {file_path}")

import gzip
import pickle
import io

try:
    print("Attempting to read as standard .npz...")
    data = np.load(file_path, allow_pickle=True)
    print(f"Keys: {data.files}")
except Exception as e:
    print(f"Standard load failed: {e}")
    
    print("\nAttempting to read as GZIP...")
    try:
        with gzip.open(file_path, 'rb') as f:
            content = f.read()
            print(f"Decompressed size: {len(content)} bytes")
            
            # Try to load from bytes as .npy
            try:
                print("Attempting to load as .npy from bytes...")
                arr = np.load(io.BytesIO(content), allow_pickle=True)
                print(f"Success! Loaded array.")
                print(f"Shape: {arr.shape}")
                print(f"Dtype: {arr.dtype}")
                print(f"Sample: {arr[:2]}")
            except Exception as e_npy:
                print(f"Not a .npy file: {e_npy}")
                
                # Try pickle
                try:
                    print("Attempting to unpickle...")
                    obj = pickle.loads(content)
                    print(f"Success! Unpickled object type: {type(obj)}")
                    if isinstance(obj, dict):
                        print(f"Keys: {obj.keys()}")
                except Exception as e_pkl:
                    print(f"Not a pickle: {e_pkl}")

    except Exception as e_gz:
        print(f"Gzip read failed: {e_gz}")
