```python
from sklearn.preprocessing import LabelEncoder

# Create the label encoder and fit_transform the team column
label_encoder = LabelEncoder()
df['team_encoded'] = label_encoder.fit_transform(df['team'])

# ... train your machine learning model ...

# Get the encoded label for 'DEN' (assuming 'DEN' is in the training data)
encoded_label = label_encoder.transform(['DEN'])[0]

# ... predict using the encoded label ...

# Get the original team value corresponding to the encoded label
original_team = label_encoder.inverse_transform([encoded_label])[0]
print(original_team)  # Output: 'DEN'


```