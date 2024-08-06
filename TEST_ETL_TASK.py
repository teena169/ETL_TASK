import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from ETL_TASK import extract_team_stats, fetch_page

# Sample HTML content for testing
sample_html = """
<html>
<head><title>Test Page</title></head>
<body>
<table class="table">
<thead>
<tr>
<th>Team Name</th>
<th>Year</th>
<th>Wins</th>
<th>Losses</th>
<th>OT Losses</th>
<th>Win %</th>
<th>Goals For (GF)</th>
<th>Goals Against (GA)</th>
<th>+ / -</th>
</tr>
</thead>
<tbody>
<tr>
<td>Team A</td>
<td>1990</td>
<td>10</td>
<td>5</td>
<td>1</td>
<td>0.667</td>
<td>50</td>
<td>40</td>
<td>+10</td>
</tr>
<tr>
<td>Team B</td>
<td>1990</td>
<td>8</td>
<td>7</td>
<td>1</td>
<td>0.533</td>
<td>45</td>
<td>42</td>
<td>+3</td>
</tr>
<tr>
<td>Team A</td>
<td>1991</td>
<td>12</td>
<td>4</td>
<td>0</td>
<td>0.75</td>
<td>60</td>
<td>35</td>
<td>+25</td>
</tr>
<tr>
<td>Team B</td>
<td>1991</td>
<td>9</td>
<td>6</td>
<td>1</td>
<td>0.6</td>
<td>55</td>
<td>50</td>
<td>+5</td>
</tr>
</tbody>
</table>
</body>
</html>
"""

# Mock response for fetch_page function
mock_response = MagicMock()
mock_response.content = sample_html

@patch('ETL_TASK.requests.get')
def test_extract_team_stats(mock_get):
    mock_get.return_value = mock_response
    _, soup = fetch_page("http://fakeurl.com")
    df = extract_team_stats(soup)
    assert df.shape == (4, 9)
    assert list(df.columns) == ['Team Name', 'Year', 'Wins', 'Losses', 'OT Losses', 'Win %', 'Goals For (GF)', 'Goals Against (GA)', '+ / -']
    assert df.iloc[0]['Team Name'] == 'Team A'
    assert df.iloc[1]['Wins'] == 8

def test_summary_calculation():
    data = {
        'Team Name': ['Team A', 'Team B', 'Team A', 'Team B'],
        'Year': [1990, 1990, 1991, 1991],
        'Wins': [10, 8, 12, 9]
    }
    df = pd.DataFrame(data)
    df['Year'] = df['Year'].astype(int)
    summary = df.groupby('Year').agg(
        Winner=pd.NamedAgg(column='Team Name', aggfunc=lambda x: x[df.loc[x.index, 'Wins'].idxmax()]),
        Winner_Num_of_Wins=pd.NamedAgg(column='Wins', aggfunc='max'),
        Loser=pd.NamedAgg(column='Team Name', aggfunc=lambda x: x[df.loc[x.index, 'Wins'].idxmin()]),
        Loser_Num_of_Wins=pd.NamedAgg(column='Wins', aggfunc='min')
    ).reset_index()

    assert summary.shape == (2, 5)
    assert summary.loc[summary['Year'] == 1990, 'Winner'].values[0] == 'Team A'
    assert summary.loc[summary['Year'] == 1990, 'Loser'].values[0] == 'Team B'
    assert summary.loc[summary['Year'] == 1991, 'Winner_Num_of_Wins'].values[0] == 12
    assert summary.loc[summary['Year'] == 1991, 'Loser_Num_of_Wins'].values[0] == 9



