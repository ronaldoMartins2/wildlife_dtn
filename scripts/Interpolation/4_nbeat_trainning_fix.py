import torch
import torch.nn as nn

# how to
# python3 4_nbeat_trainning_fix.py

# Define the NBeatsBlock with a residual connection fix
class NBeatsBlock(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim):
        super(NBeatsBlock, self).__init__()
        self.fc1 = nn.Linear(input_dim, hidden_dim)
        self.fc2 = nn.Linear(hidden_dim, hidden_dim)
        self.fc3 = nn.Linear(hidden_dim, output_dim)
        self.fc_res = nn.Linear(output_dim, input_dim)  # Linear layer to match input and output dimensions

    def forward(self, x):
        x_residual = x  # Save the residual input for later addition

        x = torch.relu(self.fc1(x))  # Shape: [batch_size, hidden_dim]
        x = torch.relu(self.fc2(x))  # Shape: [batch_size, hidden_dim]
        forecast = self.fc3(x)  # Shape: [batch_size, output_dim]

        forecast = self.fc_res(forecast)  # Match forecast shape to input shape [batch_size, input_dim]

        return forecast


# Define the NBeats model
class NBeats(nn.Module):
    def __init__(self, input_dim, output_dim, hidden_dim, num_blocks):
        super(NBeats, self).__init__()
        self.blocks = nn.ModuleList([NBeatsBlock(input_dim, output_dim, hidden_dim) for _ in range(num_blocks)])

    def forward(self, x):
        forecasts = []
        for block in self.blocks:
            forecast = block(x)  # Pass the input through each block
            forecasts.append(forecast)
            x = x + forecast  # Residual connection (now they have the same shape)
        return sum(forecasts) / len(forecasts)  # Average the forecasts

# Hyperparameters
#input_dim = 32  # Input size (number of past time steps)
input_dim = 6  # Input size (number of past time steps)

#output_dim = 6  # Output size (number of future time steps)
output_dim = 6  # Output size (number of future time steps)

hidden_dim = 6  # Hidden layer size
num_blocks = 3  # Number of N-BEATS blocks

# Create the model
model = NBeats(input_dim, output_dim, hidden_dim, num_blocks)

# Example training loop
criterion = nn.MSELoss()  # Mean Squared Error Loss
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

output_dim = 6

# Example input (batch size, input_dim)
x = torch.rand(32, input_dim)  # Random input for example with shape [32, 32]

# Dummy target values (actual future values)
y_true = torch.rand(32, output_dim)  # Batch of 32, future 6 time steps

# Training loop (simple example)
for epoch in range(100):  # 100 epochs
    model.train()  # Set the model to training mode
    optimizer.zero_grad()  # Zero the gradients

    # Forward pass
    forecast = model(x)

    # Compute loss
    loss = criterion(forecast, y_true)

    # Backward pass and optimization
    loss.backward()
    optimizer.step()

    if epoch % 10 == 0:
        print(f"Epoch {epoch}, Loss: {loss.item()}")
