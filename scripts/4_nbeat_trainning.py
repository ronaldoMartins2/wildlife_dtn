import torch 
import torch.nn as nn

from n_beat import  NBeats

# how to
# python3 4_nbeat_trainning.py

# Assuming you have a model class (e.g., your NBeats model)
#model = NBeats(input_dim=12, output_dim=6, hidden_dim=64, num_blocks=3)
#model = NBeats(input_dim=32, output_dim=6, hidden_dim=6, num_blocks=3)

model = NBeats(input_dim=32, output_dim=6, hidden_dim=6, num_blocks=3)



# Example training loop
criterion = nn.MSELoss()  # Mean Squared Error Loss
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

output_dim = 6


#x = torch.rand(32, 12)  # Random input for example
#x = torch.rand(32, 6)  # Random input for example

x = torch.rand(32, 32)  # to fix

# Dummy target values (actual future values)
y_true = torch.rand(32, output_dim)  # Batch of 32, future 6 time steps

# Training loop (simple example)
for epoch in range(100):  # 100 epochs
    model.train()  # Set the model to training mode
    optimizer.zero_grad()  # Zero the gradients

    # Forward pass
    forecast = model( x )

    # Compute loss
    loss = criterion(forecast, y_true)

    # Backward pass and optimization
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item()}")