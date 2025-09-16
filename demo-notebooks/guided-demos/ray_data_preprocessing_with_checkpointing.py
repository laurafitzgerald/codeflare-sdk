#!/usr/bin/env python3
"""
Ray Data preprocessing script with checkpointing and suspension handling.
This script demonstrates distributed data preprocessing with fault tolerance.
"""

import os
import time
import json
import ray
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, Any, List

# Initialize Ray
ray.init()

@ray.remote
class PreprocessingCheckpoint:
    """Ray actor for managing preprocessing checkpoints"""
    
    def __init__(self, checkpoint_dir: str):
        self.checkpoint_dir = Path(checkpoint_dir)
        self.checkpoint_dir.mkdir(parents=True, exist_ok=True)
        self.processed_files = set()
        self.current_batch = 0
        
    def save_checkpoint(self, batch_id: int, processed_files: List[str], stats: Dict[str, Any]):
        """Save preprocessing checkpoint"""
        checkpoint = {
            'batch_id': batch_id,
            'processed_files': list(processed_files),
            'stats': stats,
            'timestamp': time.time()
        }
        
        checkpoint_path = self.checkpoint_dir / f'preprocessing_checkpoint_{batch_id}.json'
        with open(checkpoint_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        
        # Save latest checkpoint
        latest_path = self.checkpoint_dir / 'latest_preprocessing_checkpoint.json'
        with open(latest_path, 'w') as f:
            json.dump(checkpoint, f, indent=2)
        
        print(f"Preprocessing checkpoint saved: {checkpoint_path}")
        return checkpoint_path
    
    def load_checkpoint(self):
        """Load latest preprocessing checkpoint"""
        latest_path = self.checkpoint_dir / 'latest_preprocessing_checkpoint.json'
        
        if not latest_path.exists():
            print("No preprocessing checkpoint found")
            return None, set(), {}
        
        with open(latest_path, 'r') as f:
            checkpoint = json.load(f)
        
        batch_id = checkpoint['batch_id']
        processed_files = set(checkpoint['processed_files'])
        stats = checkpoint['stats']
        
        print(f"Preprocessing checkpoint loaded: batch {batch_id}, {len(processed_files)} files processed")
        return batch_id, processed_files, stats
    
    def get_processed_files(self):
        """Get set of already processed files"""
        return self.processed_files
    
    def update_processed_files(self, files: List[str]):
        """Update the set of processed files"""
        self.processed_files.update(files)

def create_synthetic_dataset(num_files: int = 100, rows_per_file: int = 10000) -> List[str]:
    """Create synthetic dataset files for preprocessing"""
    data_dir = Path('/tmp/ray_data_input')
    data_dir.mkdir(parents=True, exist_ok=True)
    
    file_paths = []
    
    for i in range(num_files):
        # Create synthetic data
        data = {
            'id': range(i * rows_per_file, (i + 1) * rows_per_file),
            'feature_1': np.random.randn(rows_per_file),
            'feature_2': np.random.randn(rows_per_file),
            'feature_3': np.random.randn(rows_per_file),
            'category': np.random.choice(['A', 'B', 'C', 'D'], rows_per_file),
            'target': np.random.randint(0, 2, rows_per_file)
        }
        
        df = pd.DataFrame(data)
        file_path = data_dir / f'data_batch_{i:03d}.parquet'
        df.to_parquet(file_path, index=False)
        file_paths.append(str(file_path))
    
    print(f"Created {num_files} synthetic data files in {data_dir}")
    return file_paths

def preprocess_batch(file_paths: List[str], batch_id: int) -> Dict[str, Any]:
    """Preprocess a batch of files"""
    print(f"Processing batch {batch_id} with {len(file_paths)} files")
    
    # Read and preprocess data using Ray Data
    ds = ray.data.read_parquet(file_paths)
    
    # Apply preprocessing transformations
    def normalize_features(batch):
        """Normalize numerical features"""
        batch['feature_1'] = (batch['feature_1'] - batch['feature_1'].mean()) / batch['feature_1'].std()
        batch['feature_2'] = (batch['feature_2'] - batch['feature_2'].mean()) / batch['feature_2'].std()
        batch['feature_3'] = (batch['feature_3'] - batch['feature_3'].mean()) / batch['feature_3'].std()
        return batch
    
    def encode_categories(batch):
        """Encode categorical features"""
        category_map = {'A': 0, 'B': 1, 'C': 2, 'D': 3}
        batch['category_encoded'] = batch['category'].map(category_map)
        return batch
    
    def add_derived_features(batch):
        """Add derived features"""
        batch['feature_sum'] = batch['feature_1'] + batch['feature_2'] + batch['feature_3']
        batch['feature_product'] = batch['feature_1'] * batch['feature_2'] * batch['feature_3']
        return batch
    
    # Apply transformations
    processed_ds = (ds
                   .map_batches(normalize_features, batch_format="pandas")
                   .map_batches(encode_categories, batch_format="pandas")
                   .map_batches(add_derived_features, batch_format="pandas"))
    
    # Save processed data
    output_dir = Path('/tmp/ray_data_output')
    output_dir.mkdir(parents=True, exist_ok=True)
    
    output_path = output_dir / f'processed_batch_{batch_id:03d}.parquet'
    processed_ds.write_parquet(str(output_path))
    
    # Get statistics
    stats = {
        'batch_id': batch_id,
        'input_files': len(file_paths),
        'total_rows': processed_ds.count(),
        'output_path': str(output_path),
        'processing_time': time.time()
    }
    
    print(f"Batch {batch_id} processed: {stats['total_rows']} rows")
    return stats

def main_preprocessing():
    """Main preprocessing function with checkpointing"""
    print("Starting Ray Data preprocessing with checkpointing...")
    
    # Configuration
    config = {
        'num_files': 100,
        'rows_per_file': 10000,
        'batch_size': 10,  # Process 10 files at a time
        'checkpoint_dir': '/tmp/ray_preprocessing_checkpoints',
        'output_dir': '/tmp/ray_data_output'
    }
    
    print(f"Configuration: {config}")
    
    # Create checkpoint manager
    checkpoint_manager = PreprocessingCheckpoint.remote(config['checkpoint_dir'])
    
    # Load existing checkpoint
    batch_id, processed_files, stats = ray.get(checkpoint_manager.load_checkpoint.remote())
    start_batch = batch_id + 1 if batch_id is not None else 0
    
    print(f"Starting from batch {start_batch}")
    
    # Create synthetic dataset if not exists
    input_dir = Path('/tmp/ray_data_input')
    if not input_dir.exists() or len(list(input_dir.glob('*.parquet'))) == 0:
        print("Creating synthetic dataset...")
        all_files = create_synthetic_dataset(config['num_files'], config['rows_per_file'])
    else:
        all_files = [str(f) for f in input_dir.glob('*.parquet')]
        print(f"Using existing dataset with {len(all_files)} files")
    
    # Filter out already processed files
    remaining_files = [f for f in all_files if f not in processed_files]
    print(f"Remaining files to process: {len(remaining_files)}")
    
    if not remaining_files:
        print("All files already processed!")
        return "COMPLETED"
    
    # Process files in batches
    total_batches = (len(remaining_files) + config['batch_size'] - 1) // config['batch_size']
    
    for i in range(start_batch, total_batches):
        batch_start = i * config['batch_size']
        batch_end = min(batch_start + config['batch_size'], len(remaining_files))
        batch_files = remaining_files[batch_start:batch_end]
        
        print(f"\nProcessing batch {i}/{total_batches-1}")
        print(f"Files: {batch_files[0]} to {batch_files[-1]}")
        
        try:
            # Process batch
            batch_stats = preprocess_batch(batch_files, i)
            
            # Update checkpoint
            ray.get(checkpoint_manager.update_processed_files.remote(batch_files))
            ray.get(checkpoint_manager.save_checkpoint.remote(i, batch_files, batch_stats))
            
            print(f"Batch {i} completed successfully")
            
        except Exception as e:
            print(f"Error processing batch {i}: {e}")
            # Save checkpoint even on error
            ray.get(checkpoint_manager.save_checkpoint.remote(i, batch_files, {'error': str(e)}))
            return "FAILED"
    
    print("\n✅ All preprocessing completed successfully!")
    return "COMPLETED"

if __name__ == "__main__":
    result = main_preprocessing()
    print(f"Preprocessing result: {result}")
