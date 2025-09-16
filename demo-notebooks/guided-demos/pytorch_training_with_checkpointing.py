#!/usr/bin/env python3
"""
PyTorch training script with automatic checkpointing.
This script demonstrates proper checkpointing to enable resume after RayJob suspension.
"""

import os
import time
import json
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader, TensorDataset
import numpy as np
from pathlib import Path

class SimpleCNN(nn.Module):
    """Simple CNN for demonstration purposes"""
    def __init__(self, num_classes=10):
        super(SimpleCNN, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout2d(0.25)
        self.dropout2 = nn.Dropout2d(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, num_classes)

    def forward(self, x):
        x = self.conv1(x)
        x = torch.relu(x)
        x = self.conv2(x)
        x = torch.relu(x)
        x = torch.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = torch.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        return x

def create_synthetic_data(num_samples=1000, num_classes=10):
    """Create synthetic data for demonstration"""
    # Generate random data
    X = torch.randn(num_samples, 1, 28, 28)
    y = torch.randint(0, num_classes, (num_samples,))
    
    # Split into train/test
    train_size = int(0.8 * num_samples)
    X_train, X_test = X[:train_size], X[train_size:]
    y_train, y_test = y[:train_size], y[train_size:]
    
    return (X_train, y_train), (X_test, y_test)

def save_checkpoint(model, optimizer, epoch, loss, checkpoint_dir):
    """Save training checkpoint"""
    checkpoint_dir = Path(checkpoint_dir)
    checkpoint_dir.mkdir(parents=True, exist_ok=True)
    
    checkpoint = {
        'epoch': epoch,
        'model_state_dict': model.state_dict(),
        'optimizer_state_dict': optimizer.state_dict(),
        'loss': loss,
        'timestamp': time.time()
    }
    
    checkpoint_path = checkpoint_dir / f'checkpoint_epoch_{epoch}.pth'
    torch.save(checkpoint, checkpoint_path)
    
    # Also save latest checkpoint
    latest_path = checkpoint_dir / 'latest_checkpoint.pth'
    torch.save(checkpoint, latest_path)
    
    print(f"Checkpoint saved: {checkpoint_path}")
    return checkpoint_path

def load_checkpoint(checkpoint_path, model, optimizer=None):
    """Load training checkpoint"""
    if not os.path.exists(checkpoint_path):
        print(f"No checkpoint found at {checkpoint_path}")
        return 0, float('inf')
    
    checkpoint = torch.load(checkpoint_path, map_location='cpu')
    model.load_state_dict(checkpoint['model_state_dict'])
    
    if optimizer:
        optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
    
    epoch = checkpoint['epoch']
    loss = checkpoint['loss']
    
    print(f"Checkpoint loaded: epoch {epoch}, loss {loss:.4f}")
    return epoch, loss


def train_model():
    """Main training function with checkpointing"""
    print("Starting PyTorch training with checkpointing...")
    
    # Configuration
    config = {
        'num_epochs': 50,
        'batch_size': 32,
        'learning_rate': 0.001,
        'checkpoint_dir': '/tmp/pytorch_checkpoints',
        'num_classes': 10,
        'device': 'cuda' if torch.cuda.is_available() else 'cpu'
    }
    
    print(f"Using device: {config['device']}")
    
    # Create model
    model = SimpleCNN(config['num_classes'])
    model = model.to(config['device'])
    
    # Create optimizer
    optimizer = optim.Adam(model.parameters(), lr=config['learning_rate'])
    criterion = nn.CrossEntropyLoss()
    
    # Create data
    (X_train, y_train), (X_test, y_test) = create_synthetic_data()
    
    train_dataset = TensorDataset(X_train, y_train)
    train_loader = DataLoader(train_dataset, batch_size=config['batch_size'], shuffle=True)
    
    test_dataset = TensorDataset(X_test, y_test)
    test_loader = DataLoader(test_dataset, batch_size=config['batch_size'], shuffle=False)
    
    # Try to load existing checkpoint
    latest_checkpoint = os.path.join(config['checkpoint_dir'], 'latest_checkpoint.pth')
    start_epoch, best_loss = load_checkpoint(latest_checkpoint, model, optimizer)
    
    print(f"Starting training from epoch {start_epoch + 1}")
    
    # Training loop
    for epoch in range(start_epoch, config['num_epochs']):
        print(f"\nEpoch {epoch + 1}/{config['num_epochs']}")
        
        # Training phase
        model.train()
        total_loss = 0
        
        for batch_idx, (data, target) in enumerate(train_loader):
            data, target = data.to(config['device']), target.to(config['device'])
            
            optimizer.zero_grad()
            output = model(data)
            loss = criterion(output, target)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            
            if batch_idx % 50 == 0:
                print(f"Batch {batch_idx}/{len(train_loader)}, Loss: {loss.item():.4f}")
        
        avg_loss = total_loss / len(train_loader)
        
        # Validation phase
        model.eval()
        val_loss = 0
        correct = 0
        
        with torch.no_grad():
            for data, target in test_loader:
                data, target = data.to(config['device']), target.to(config['device'])
                output = model(data)
                val_loss += criterion(output, target).item()
                pred = output.argmax(dim=1, keepdim=True)
                correct += pred.eq(target.view_as(pred)).sum().item()
        
        val_loss /= len(test_loader)
        accuracy = 100. * correct / len(test_loader.dataset)
        
        print(f"Train Loss: {avg_loss:.4f}, Val Loss: {val_loss:.4f}, Accuracy: {accuracy:.2f}%")
        
        # Save checkpoint every epoch
        save_checkpoint(model, optimizer, epoch + 1, val_loss, config['checkpoint_dir'])
        
        # Update best loss
        if val_loss < best_loss:
            best_loss = val_loss
            print(f"New best validation loss: {best_loss:.4f}")
    
    print("\n✅ Training completed successfully!")
    return "COMPLETED"

if __name__ == "__main__":
    result = train_model()
    print(f"Training result: {result}")
