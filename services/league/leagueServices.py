import requests
from dotenv import load_dotenv
import os
import io
import boto3
import json
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from PIL import Image
from io import BytesIO


load_dotenv()

RIOT_API_KEY = os.environ.get("RIOT_API_KEY")
s3 = boto3.client('s3')

def uploadToS3Match(jsonData, bucket, objectName):
    try:
        json_string = json.dumps(jsonData)
        file_like_object = io.StringIO(json_string)        
        response = s3.put_object(Body=file_like_object.getvalue(), Bucket=bucket, Key=objectName)        
        print(response)
    except Exception as e:
        print(e)


def insertDataMatch(jsonData, matchId, puuid):
    try:
        uploadToS3Match(jsonData, "riftrewind", f"match/{puuid}/{matchId}.json")
        print("completed")
    except Exception as e:
        print(e)

def retrieveAccountData(riotId: str, tag: str):
    try:
        url = f"https://americas.api.riotgames.com/riot/account/v1/accounts/by-riot-id/{riotId}/{tag}?api_key={RIOT_API_KEY}"
        response = requests.get(url)
        data = response.json()
        return data['puuid']
    except Exception as e:
        print(e)

def retrieveRankedData(PUUID: str):
    try:
        response = requests.get(f"https://na1.api.riotgames.com/lol/league/v4/entries/by-puuid/{PUUID}?api_key={RIOT_API_KEY}")
        data = response.json()
        uploadToS3Match(data, "riftrewind", f"player/{PUUID}/{PUUID}.json")    
        return data
    except Exception as e:
        print(e)

def retrieveEntriesData(PUUID: str):
    try:
        response = requests.get(f"https://na1.api.riotgames.com/lol/league/v4/entries/by-puuid/{PUUID}?api_key={RIOT_API_KEY}")
        data = response.json()
        return data
    except Exception as e:
        print(e)

def retrieveMatchIds(PUUID: str):
    try:
        response = requests.get(f"https://americas.api.riotgames.com/lol/match/v5/matches/by-puuid/{PUUID}/ids?api_key={RIOT_API_KEY}")
        data = response.json()
        return data
    except Exception as e:
        print(e)

def retrieveMatchData(matchId: str, puuid):
    try:
        response = requests.get(f"https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}?api_key={RIOT_API_KEY}")
        data = response.json()
        uploadToS3Match(data["info"], "riftrewind", f"match/{puuid}/{matchId}.json")
    except Exception as e:
        print(e)

def retrieveMatchDataFramesTimeline(matchId: str, puuid: str):
    try:
        url = f"https://americas.api.riotgames.com/lol/match/v5/matches/{matchId}/timeline?api_key={RIOT_API_KEY}"        
        response = requests.get(url)
        response.raise_for_status() 
        data = response.json() 
        uploadToS3Match(data["info"], "riftrewind", f"timestamp/{puuid}/{matchId}.json")    

    except Exception as e:
        print(f"An unexpected error occurred: {e}")

#Function inserts all necessary Data
def uploadAllDataToS3(riotId: str, tag: str):
    puuid = retrieveAccountData(riotId, tag)
    matchIdData = retrieveMatchIds(puuid)
    try:
        retrieveRankedData(puuid)
        for i in range(len(matchIdData)):
            retrieveMatchDataFramesTimeline(matchIdData[i], puuid)
            retrieveMatchData(matchIdData[i], puuid)
        print("Successfully Inserted Data")
    except Exception as e:
        raise Exception(e)

uploadAllDataToS3("jerrrrbear", "NA1")







#DATA VISUALISATIONS
















#Visualisations
def visualize_cs_over_time(timeline_data):
    """
    Creates a line chart of minions and monsters killed (CS) over time.
    """
    print("Generating CS over time line chart...")
    
    records = []
    for frame in timeline_data['frames']:
        minutes = frame['timestamp'] / 60000
        for participant_id, frame_data in frame['participantFrames'].items():
            cs = frame_data.get('minionsKilled', 0) + frame_data.get('jungleMinionsKilled', 0)
            records.append({
                'minutes': minutes,
                'participantId': int(participant_id),
                'cs': cs
            })

    df = pd.DataFrame(records)
    
    # --- Plotting ---
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.figure(figsize=(12, 7))
    
    sns.lineplot(
        data=df,
        x='minutes',
        y='cs',
        hue='participantId',
        palette='tab10',
        linewidth=2.5
    )
    
    plt.title('Creep Score (CS) Over Time', fontsize=16)
    plt.xlabel("Game Time (Minutes)", fontsize=12)
    plt.ylabel("Total CS", fontsize=12)
    plt.legend(title='Player ID')
    plt.tight_layout()
    plt.show()



def visualize_match_stat(timeline_data, stat_to_plot='totalGold'):
    """
    Processes match data and creates a plot for a specific stat over time.

    Args:
        timeline_data (dict): The raw dictionary containing the match timeline.
        stat_to_plot (str): The key of the stat to visualize (e.g., 'totalGold', 'xp', 'level').
    """
    print(f"Processing match data to visualize '{stat_to_plot}'...")

    # --- Step 1: Extract Data ---
    # Create an empty list to hold the structured data for each player at each frame.
    records = []
    # Loop through each frame in the timeline.
    for frame in timeline_data['frames']:
        # Each frame has a single timestamp for all participants in it.
        # We convert it from milliseconds to minutes for a more readable chart.
        minutes = frame['events'][0]['timestamp'] / 60000
        
        # Loop through each participant's data within the frame.
        for participant_id, frame_data in frame['participantFrames'].items():
            records.append({
                'minutes': minutes,
                'participantId': int(participant_id),
                stat_to_plot: frame_data.get(stat_to_plot, 0)
            })

    # --- Step 2: Create a DataFrame ---
    # Convert the list of records into a pandas DataFrame, which is great for handling tabular data.
    df = pd.DataFrame(records)

    # --- Step 3: Plot the Data ---
    # Set a visually appealing style for the plot.
    plt.style.use('seaborn-v0_8-darkgrid')
    # Set the size of the output figure.
    plt.figure(figsize=(12, 7))

    # Use the seaborn library to create a line plot.
    sns.lineplot(
        data=df,
        x='minutes',
        y=stat_to_plot,
        hue='participantId',  # This creates a separate colored line for each player.
        palette='tab10',      # A color scheme that looks good with up to 10 lines.
        linewidth=2.5
    )

    # --- Step 4: Customize and Show the Plot ---
    # Add a title and labels to make the chart easy to understand.
    plt.title(f"{stat_to_plot.replace('T', ' T').title()} Over Time", fontsize=16)
    plt.xlabel("Game Time (Minutes)", fontsize=12)
    plt.ylabel(f"{stat_to_plot.replace('T', ' T').title()}", fontsize=12)
    plt.legend(title='Player ID')
    plt.tight_layout()  # Ensures everything fits without overlapping.

    # Finally, display the plot on the screen.
    plt.show()

def generate_kill_heatmap(timeline_data):
    """
    Generates a heatmap of champion kill locations on the Summoner's Rift map.
    """
    print("Generating kill location heatmap...")
    
    kill_positions = []
    for frame in timeline_data['frames']:
        for event in frame['events']:
            if event.get('type') == 'CHAMPION_KILL':
                pos = event.get('position')
                if pos:
                    kill_positions.append({'x': pos['x'], 'y': pos['y']})

    if not kill_positions:
        print("No champion kill events found in the data.")
        return

    df_kills = pd.DataFrame(kill_positions)

    # --- Fetch Map Image and Plot ---
    try:
        url = 'https://preview.redd.it/fgrxon2d9km71.png?width=750&format=png&auto=webp&s=6e0157b2ec829080cb4f137c1e9aa99a3f0f30cd'
        response = requests.get(url)
        map_img = Image.open(BytesIO(response.content))
    except Exception as e:
        print(f"Could not download map image. Heatmap will have a blank background. Error: {e}")
        map_img = None

    fig, ax = plt.subplots(figsize=(10, 10))
    ax.set_xticks([])
    ax.set_yticks([])
    
    # Set map coordinates for the image background
    map_extent = [0, 14820, 0, 14881]
    if map_img:
        ax.imshow(map_img, extent=map_extent)
        
    # Create the 2D density plot (heatmap)
    sns.kdeplot(
        data=df_kills,
        x='x',
        y='y',
        fill=True,      # Fill the area under the density curve
        cmap='rocket',  # A "hot" color map
        alpha=0.6,      # Make it semi-transparent to see the map
        ax=ax
    )
    
    ax.set_xlim(map_extent[0], map_extent[1])
    ax.set_ylim(map_extent[2], map_extent[3])
    ax.set_title('Heatmap of Champion Kill Locations', fontsize=16)
    plt.tight_layout()
    plt.show()

#generate_kill_heatmap(retrieveMatchDataFramesTimeline("NA1_5368916340"))
#visualize_match_stat(retrieveMatchDataFramesTimeline("NA1_5368916340"))
#visualize_cs_over_time(retrieveMatchDataFramesTimeline("NA1_5368916340"))

def visualize_champion_stat_over_time(timeline_data, stat_to_plot='healthMax'):
    """
    Creates a line chart for a specific champion stat over time for all players.
    
    Args:
        timeline_data (dict): The raw dictionary containing the match timeline.
        stat_to_plot (str): The key of the stat inside 'championStats' to visualize.
                           Examples: 'healthMax', 'attackDamage', 'abilityPower', 'armor'.
    """
    print(f"Generating chart for champion stat: '{stat_to_plot}'...")
    
    # --- Step 1: Extract Data ---
    records = []
    for frame in timeline_data['frames']:
        minutes = frame['timestamp'] / 60000
        for participant_id, frame_data in frame['participantFrames'].items():
            # Navigate into the nested championStats dictionary
            champion_stats = frame_data.get('championStats', {})
            # Get the specific stat, defaulting to 0 if not found
            stat_value = champion_stats.get(stat_to_plot, 0)
            
            records.append({
                'minutes': minutes,
                'participantId': int(participant_id),
                'statValue': stat_value
            })
            
    # --- Step 2: Create a DataFrame ---
    df = pd.DataFrame(records)
    
    # --- Step 3: Plot the Data ---
    plt.style.use('seaborn-v0_8-darkgrid')
    plt.figure(figsize=(12, 8))
    
    sns.lineplot(
        data=df,
        x='minutes',
        y='statValue',
        hue='participantId',
        palette='tab10',
        linewidth=2.5
    )
    
    # --- Step 4: Customize and Show the Plot ---
    # Create a clean title from the stat key (e.g., 'healthMax' -> 'Health Max')
    clean_title = ''.join([' ' + char if char.isupper() else char for char in stat_to_plot]).strip().title()
    
    plt.title(f"{clean_title} Over Time", fontsize=16)
    plt.xlabel("Game Time (Minutes)", fontsize=12)
    plt.ylabel(clean_title, fontsize=12)
    plt.legend(title='Player ID', bbox_to_anchor=(1.05, 1), loc='upper left')
    plt.tight_layout()
    plt.show()

#visualize_champion_stat_over_time(retrieveMatchDataFramesTimeline("NA1_5368916340"), stat_to_plot='healthMax')

# Example 2: Graph the attack damage of all champions over time
#visualize_champion_stat_over_time(retrieveMatchDataFramesTimeline("NA1_5368916340"), stat_to_plot='attackDamage')

# Example 3: Graph the ability power of all champions over time
#visualize_champion_stat_over_time(retrieveMatchDataFramesTimeline("NA1_5368916340"), stat_to_plot='abilityPower')

# Example 4: Graph the movement speed of all champions over time
#visualize_champion_stat_over_time(retrieveMatchDataFramesTimeline("NA1_5368916340"), stat_to_plot='movementSpeed')
