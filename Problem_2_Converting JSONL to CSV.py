import json
import pandas as pd
from pandas import json_normalize

# File paths
input_path = 'Beauty_and_Personal_Care.jsonl'  # Input JSONL file 
output_path = 'flattened_reviews.csv'          # Output CSV file

# Step 1: Read JSON from file
with open(input_path, 'r', encoding='utf-8') as file:
    data = [json.loads(line) for line in file]  # Parse each JSON line

# Step 2: Clean/flatten specific fields (for e.g., remove 'images' field)
for review in data:
    if 'images' in review:
        review['image_count'] = len(review['images'])  # Add count instead
        del review['images']

# Step 3: Flatten nested structures
df = json_normalize(data)

# Step 4: Save the result to a CSV file
df.to_csv(output_path, index=False)
print(f"Flattened data saved to {output_path}")