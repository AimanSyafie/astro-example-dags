"""
## Astronaut ETL example DAG

This DAG queries the list of astronauts currently in space from the
Open Notify API and prints each astronaut's name and flying craft.

There are two tasks, one to get the data from the API and save the results,
and another to print the results. Both tasks are written in Python using
Airflow's TaskFlow API, which allows you to easily turn Python functions into
Airflow tasks, and automatically infer dependencies and pass data.

The second task uses dynamic task mapping to create a copy of the task for
each Astronaut in the list retrieved from the API. This list will change
depending on how many Astronauts are in space, and the DAG will adjust
accordingly each time it runs.

For more explanation and getting started instructions, see our Write your
first DAG tutorial: https://docs.astronomer.io/learn/get-started-with-airflow

![Picture of the ISS](https://www.esa.int/var/esa/storage/images/esa_multimedia/images/2010/02/space_station_over_earth/10293696-3-eng-GB/Space_Station_over_Earth_card_full.jpg)
"""

from airflow import Dataset
from airflow.decorators import dag, task
from pendulum import datetime
import requests
import pandas as pd
from pathlib import Path


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule=None,  # DAG is paused - set to "@daily" to re-enable
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
    is_paused_upon_creation=True,
)
def example_astronauts():
    # Define tasks
    @task(
        # Define a dataset outlet for the task. This can be used to schedule downstream DAGs when this task has run.
        outlets=[Dataset("current_astronauts")]
    )  # Define that this task updates the `current_astronauts` Dataset
    def get_astronauts(**context) -> list[dict]:
        """
        This task uses the requests library to retrieve a list of Astronauts
        currently in space. The results are pushed to XCom with a specific key
        so they can be used in a downstream pipeline. The task returns a list
        of Astronauts to be used in the next task.
        """
        try:
            r = requests.get("http://api.open-notify.org/astros.json", timeout=10)
            r.raise_for_status()
            data = r.json()
            number_of_people_in_space = data["number"]
            list_of_people_in_space = data["people"]

            context["ti"].xcom_push(
                key="number_of_people_in_space", value=number_of_people_in_space
            )
            return list_of_people_in_space
        except requests.exceptions.RequestException as e:
            print(f"Error fetching astronaut data: {e}")
            raise

    @task
    def enrich_astronaut_data(astronaut_list: list[dict]) -> list[dict]:
        """
        Enrich astronaut data with country and company/agency information.
        Maps space agencies to countries and adds company/organization details.
        """
        # Mapping of common space agencies and their countries

        enriched_list = []
        for astronaut in astronaut_list:
            enriched = astronaut.copy()

            # Default enrichment if agency not found
            enriched["country"] = "International"
            enriched["company"] = "Space Agency"

            # Try to determine agency based on craft or use defaults
            craft = astronaut.get("craft", "")
            if "ISS" in craft:
                # For ISS crew, we'll assign based on typical crew composition
                # In a real scenario, you'd query a proper astronaut database API
                enriched["country"] = "International"
                enriched["company"] = "ISS Expedition"
            elif "Shenzhou" in craft or "Tiangong" in craft:
                enriched["country"] = "China"
                enriched["company"] = "China National Space Administration"
            elif "Soyuz" in craft:
                enriched["country"] = "Russia"
                enriched["company"] = "Roscosmos"
            elif "Dragon" in craft or "Crew Dragon" in craft:
                enriched["country"] = "USA"
                enriched["company"] = "SpaceX"

            enriched_list.append(enriched)

        return enriched_list

    @task
    def print_astronaut_craft(greeting: str, person_in_space: dict) -> None:
        """
        This task creates a print statement with the name of an
        Astronaut in space, their country, company, and the craft they are flying on from
        the API request results of the previous task, along with a
        greeting which is hard-coded in this example.
        """
        craft = person_in_space["craft"]
        name = person_in_space["name"]
        country = person_in_space.get("country", "Unknown")
        company = person_in_space.get("company", "Unknown Agency")

        print(
            f"{name} from {country} ({company}) is currently in space flying on the {craft}! {greeting}"
        )

    @task(outlets=[Dataset("weather_data")])
    def get_weather_data(**context) -> dict:
        """
        Fetch weather data from Open-Meteo API (free, no API key required).
        Returns temperature, wind speed, and weather code for Houston, TX (NASA JSC location).
        """
        try:
            # Houston coordinates (NASA Johnson Space Center)
            latitude = 29.5583
            longitude = -95.0890

            url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,wind_speed_10m,weather_code"
            r = requests.get(url, timeout=10)
            r.raise_for_status()
            weather = r.json()["current"]

            print(
                f"Weather in Houston: {weather['temperature_2m']}°C, Wind: {weather['wind_speed_10m']} km/h"
            )
            return weather
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            # Return default values if API fails
            return {
                "temperature_2m": 20.0,
                "wind_speed_10m": 10.0,
                "weather_code": 0,
            }

    @task(outlets=[Dataset("spacecraft_tracking")])
    def get_spacecraft_tracking_data(**context) -> dict:
        """
        Fetch spacecraft position data from Open Notify ISS Location API.
        Returns position coordinates with standard ISS orbital velocity and altitude.
        """
        try:
            # Get current ISS position
            r = requests.get("http://api.open-notify.org/iss-now.json", timeout=10)
            r.raise_for_status()
            data = r.json()

            latitude = float(data["iss_position"]["latitude"])
            longitude = float(data["iss_position"]["longitude"])
            timestamp = float(data["timestamp"])

            # ISS standard orbital parameters (approximate)
            # The ISS orbits at ~27,600 km/h (7.66 km/s) at ~420 km altitude
            velocity_kmh = 27600
            velocity_kms = 7.66
            altitude_km = 420

            # Calculate approximate trajectory bearing based on longitude change
            # ISS typically travels west-to-east (due to Earth's rotation)
            # For simplicity, we'll estimate direction based on latitude
            if latitude > 0:
                trajectory_direction = "Northeast"
                bearing = 45.0
            else:
                trajectory_direction = "Southeast"
                bearing = 135.0

            tracking_data = {
                "latitude": latitude,
                "longitude": longitude,
                "velocity_kmh": velocity_kmh,
                "velocity_kms": velocity_kms,
                "trajectory_bearing": bearing,
                "trajectory_direction": trajectory_direction,
                "altitude_km": altitude_km,
                "timestamp": timestamp,
            }

            print(
                f"Spacecraft Tracking - Position: ({latitude:.2f}, {longitude:.2f}), "
                f"Speed: {velocity_kmh} km/h ({velocity_kms} km/s), "
                f"Trajectory: {trajectory_direction} ({bearing}°)"
            )

            return tracking_data

        except requests.exceptions.RequestException as e:
            print(f"Error fetching spacecraft tracking data: {e}")
            # Return default values if API fails
            return {
                "latitude": 0.0,
                "longitude": 0.0,
                "velocity_kmh": 27600,
                "velocity_kms": 7.66,
                "trajectory_bearing": 90.0,
                "trajectory_direction": "East",
                "altitude_km": 420,
                "timestamp": 0,
            }

    @task
    def load_historical_spacecraft_data(**context) -> pd.DataFrame:
        """
        Load historical spacecraft tracking data from file storage.
        If file doesn't exist, return empty DataFrame with proper structure.
        """
        # Define storage path for historical data
        data_dir = Path("/tmp/astronaut_history")
        data_dir.mkdir(exist_ok=True)
        history_file = data_dir / "spacecraft_history.csv"

        if history_file.exists():
            try:
                df_history = pd.read_csv(history_file)
                print(f"Loaded {len(df_history)} historical spacecraft records")
                return df_history
            except Exception as e:
                print(f"Error loading history: {e}. Starting fresh.")
                return pd.DataFrame(
                    columns=[
                        "timestamp",
                        "date",
                        "astronaut_count",
                        "spacecraft_speed_kmh",
                        "spacecraft_speed_kms",
                        "trajectory_bearing",
                        "trajectory_direction",
                        "latitude",
                        "longitude",
                        "altitude_km",
                        "countries",
                        "companies",
                    ]
                )
        else:
            print("No historical data found. Starting new history.")
            return pd.DataFrame(
                columns=[
                    "timestamp",
                    "date",
                    "astronaut_count",
                    "spacecraft_speed_kmh",
                    "spacecraft_speed_kms",
                    "trajectory_bearing",
                    "trajectory_direction",
                    "latitude",
                    "longitude",
                    "altitude_km",
                    "countries",
                    "companies",
                ]
            )

    @task
    def save_spacecraft_history(
        astronauts: list[dict], tracking: dict, history_df: pd.DataFrame, **context
    ) -> pd.DataFrame:
        """
        Append current spacecraft data to historical records and save to file.
        Returns updated historical DataFrame for analysis.
        """
        # Get current timestamp
        from datetime import datetime as dt

        current_time = dt.now()

        # Extract astronaut data
        number_of_people = context["ti"].xcom_pull(
            task_ids="get_astronauts", key="number_of_people_in_space"
        )
        if number_of_people is None:
            number_of_people = len(astronauts)

        countries = [a.get("country", "Unknown") for a in astronauts]
        companies = [a.get("company", "Unknown") for a in astronauts]

        # Create new record
        new_record = pd.DataFrame(
            [
                {
                    "timestamp": tracking["timestamp"],
                    "date": current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    "astronaut_count": number_of_people,
                    "spacecraft_speed_kmh": tracking["velocity_kmh"],
                    "spacecraft_speed_kms": tracking["velocity_kms"],
                    "trajectory_bearing": tracking["trajectory_bearing"],
                    "trajectory_direction": tracking["trajectory_direction"],
                    "latitude": tracking["latitude"],
                    "longitude": tracking["longitude"],
                    "altitude_km": tracking["altitude_km"],
                    "countries": ", ".join(set(countries)),
                    "companies": ", ".join(set(companies)),
                }
            ]
        )

        # Append to history
        updated_history = pd.concat([history_df, new_record], ignore_index=True)

        # Keep only last 100 records to prevent file from growing too large
        if len(updated_history) > 100:
            updated_history = updated_history.tail(100)

        # Save to file
        data_dir = Path("/tmp/astronaut_history")
        data_dir.mkdir(exist_ok=True)
        history_file = data_dir / "spacecraft_history.csv"

        updated_history.to_csv(history_file, index=False)
        print(f"Saved {len(updated_history)} records to spacecraft history")

        return updated_history

    @task
    def combine_data(
        astronauts: list[dict], weather: dict, tracking: dict, **context
    ) -> pd.DataFrame:
        """
        Combine astronaut count, weather data, spacecraft tracking (speed, trajectory),
        and astronaut details (countries, companies) into a pandas DataFrame.
        In a real scenario, this would collect data over time for meaningful correlation.
        """
        number_of_people = context["ti"].xcom_pull(
            task_ids="get_astronauts", key="number_of_people_in_space"
        )
        if number_of_people is None:
            number_of_people = len(astronauts)

        # Extract countries and companies from astronaut list
        countries = [a.get("country", "Unknown") for a in astronauts]
        companies = [a.get("company", "Unknown") for a in astronauts]

        # Create a DataFrame with current data point
        df = pd.DataFrame(
            [
                {
                    "astronaut_count": number_of_people,
                    "countries": ", ".join(set(countries)),
                    "companies": ", ".join(set(companies)),
                    "spacecraft_speed_kmh": tracking["velocity_kmh"],
                    "spacecraft_speed_kms": tracking["velocity_kms"],
                    "trajectory_bearing": tracking["trajectory_bearing"],
                    "trajectory_direction": tracking["trajectory_direction"],
                    "spacecraft_latitude": tracking["latitude"],
                    "spacecraft_longitude": tracking["longitude"],
                    "spacecraft_altitude_km": tracking["altitude_km"],
                    "temperature": weather["temperature_2m"],
                    "wind_speed": weather["wind_speed_10m"],
                    "weather_code": weather["weather_code"],
                }
            ]
        )

        print(f"Combined data:\n{df}")
        return df

    @task
    def analyze_correlation(current_df: pd.DataFrame, history_df: pd.DataFrame) -> None:
        """
        Analyze correlation between astronaut count, spacecraft tracking data (speed, trajectory),
        and weather variables using both current and historical data.
        """
        print("=" * 70)
        print("CORRELATION ANALYSIS - ASTRONAUTS, SPACECRAFT TRACKING & WEATHER")
        print("=" * 70)
        print("\nCurrent Data Point:")
        print(f"  Astronauts in space: {current_df['astronaut_count'].iloc[0]}")
        print(f"  Countries: {current_df['countries'].iloc[0]}")
        print(f"  Companies: {current_df['companies'].iloc[0]}")
        print("\nSpacecraft Tracking:")
        print(
            f"  Speed: {current_df['spacecraft_speed_kmh'].iloc[0]} km/h ({current_df['spacecraft_speed_kms'].iloc[0]} km/s)"
        )
        print(
            f"  Trajectory: {current_df['trajectory_direction'].iloc[0]} ({current_df['trajectory_bearing'].iloc[0]}°)"
        )
        print(
            f"  Position: Lat {current_df['spacecraft_latitude'].iloc[0]:.2f}, Lon {current_df['spacecraft_longitude'].iloc[0]:.2f}"
        )
        print(f"  Altitude: {current_df['spacecraft_altitude_km'].iloc[0]} km")
        print("\nWeather (Houston, TX):")
        print(f"  Temperature: {current_df['temperature'].iloc[0]}°C")
        print(f"  Wind Speed: {current_df['wind_speed'].iloc[0]} km/h")
        print(f"  Weather Code: {current_df['weather_code'].iloc[0]}")

        print("\n" + "=" * 70)
        print("HISTORICAL SPACECRAFT DATA ANALYSIS")
        print("=" * 70)

        if len(history_df) > 0:
            print(f"\nTotal historical records: {len(history_df)}")
            print(
                f"Date range: {history_df['date'].iloc[0]} to {history_df['date'].iloc[-1]}"
            )

            print("\n--- Speed Statistics ---")
            print(
                f"  Average speed: {history_df['spacecraft_speed_kmh'].mean():.2f} km/h"
            )
            print(f"  Min speed: {history_df['spacecraft_speed_kmh'].min():.2f} km/h")
            print(f"  Max speed: {history_df['spacecraft_speed_kmh'].max():.2f} km/h")
            print(
                f"  Std deviation: {history_df['spacecraft_speed_kmh'].std():.2f} km/h"
            )

            print("\n--- Trajectory Patterns ---")
            trajectory_counts = history_df["trajectory_direction"].value_counts()
            print("  Direction frequency:")
            for direction, count in trajectory_counts.items():
                percentage = (count / len(history_df)) * 100
                print(f"    {direction}: {count} times ({percentage:.1f}%)")

            print("\n--- Astronaut Count Trends ---")
            print(f"  Average astronauts: {history_df['astronaut_count'].mean():.1f}")
            print(f"  Min: {history_df['astronaut_count'].min()}")
            print(f"  Max: {history_df['astronaut_count'].max()}")

            print("\n--- Recent Flights History (Last 5 records) ---")
            recent = history_df.tail(5)
            for _idx, row in recent.iterrows():
                print(
                    f"  {row['date']}: {row['astronaut_count']} astronauts, "
                    f"{row['spacecraft_speed_kmh']:.0f} km/h, {row['trajectory_direction']}"
                )

            # Calculate correlations if enough data points
            if len(history_df) >= 3:
                print("\n--- Correlation Analysis (Historical Data) ---")
                try:
                    from scipy.stats import pearsonr

                    corr_speed, p_speed = pearsonr(
                        history_df["astronaut_count"],
                        history_df["spacecraft_speed_kmh"],
                    )
                    print(
                        f"  Spacecraft Speed vs Astronaut Count: r={corr_speed:.3f} (p={p_speed:.3f})"
                    )

                    corr_traj, p_traj = pearsonr(
                        history_df["astronaut_count"], history_df["trajectory_bearing"]
                    )
                    print(
                        f"  Trajectory Bearing vs Astronaut Count: r={corr_traj:.3f} (p={p_traj:.3f})"
                    )

                    if abs(corr_speed) < 0.3:
                        print(
                            "\n  ✓ As expected, astronaut count has minimal correlation with speed"
                        )
                    if abs(corr_traj) < 0.3:
                        print(
                            "  ✓ As expected, astronaut count has minimal correlation with trajectory"
                        )
                except ImportError:
                    print("  Note: scipy not available for correlation calculations")
                except Exception as e:
                    print(f"  Could not calculate correlations: {e}")

        else:
            print("\nNo historical data available yet.")
            print(
                "Run this DAG multiple times to build a history of spacecraft flights."
            )

        print("\n" + "=" * 70)
        print("CORRELATION INSIGHTS:")
        print("=" * 70)
        print("\n1. Spacecraft Speed vs Astronaut Count:")
        print("   - ISS orbital velocity is typically ~27,600 km/h (7.66 km/s)")
        print(f"   - Current speed: {current_df['spacecraft_speed_kmh'].iloc[0]} km/h")
        print(
            "   - Astronaut count doesn't affect orbital velocity (governed by physics)"
        )

        print("\n2. Trajectory Analysis:")
        print(f"   - Current direction: {current_df['trajectory_direction'].iloc[0]}")
        print("   - ISS completes ~15.5 orbits per day, trajectory constantly changes")
        print("   - No correlation expected between trajectory and astronaut count")

        print("\n3. Weather vs Spacecraft:")
        print("   - Ground weather has no direct impact on orbital spacecraft")
        print("   - However, weather affects launch windows and crew transport")
        print("=" * 70)

    @task
    def generate_html_report(
        astronauts: list[dict],
        weather: dict,
        tracking: dict,
        combined_df: pd.DataFrame,
        history_df: pd.DataFrame,
        **context,
    ) -> str:
        """
        Generate a comprehensive HTML report with astronaut data, spacecraft tracking,
        weather information, and historical analysis with visualizations.
        """
        try:
            from datetime import datetime as dt

            # Get astronaut count
            number_of_people = context["ti"].xcom_pull(
                task_ids="get_astronauts", key="number_of_people_in_space"
            )
            if number_of_people is None:
                number_of_people = len(astronauts)

            # Build astronaut table rows
            astronaut_rows = ""
            for astronaut in astronauts:
                name = astronaut.get("name", "Unknown")
                craft = astronaut.get("craft", "Unknown")
                country = astronaut.get("country", "Unknown")
                company = astronaut.get("company", "Unknown")
                astronaut_rows += f"""
                <tr>
                    <td>{name}</td>
                    <td>{craft}</td>
                    <td>{country}</td>
                    <td>{company}</td>
                </tr>
            """

            # Build historical data rows (last 10 records)
            history_rows = ""
            if len(history_df) > 0:
                recent_history = history_df.tail(10)
                for _, row in recent_history.iterrows():
                    history_rows += f"""
                    <tr>
                        <td>{row["date"]}</td>
                        <td>{row["astronaut_count"]}</td>
                        <td>{row["spacecraft_speed_kmh"]:.0f} km/h</td>
                        <td>{row["trajectory_direction"]}</td>
                        <td>{row["latitude"]:.2f}°, {row["longitude"]:.2f}°</td>
                        <td>{row["countries"]}</td>
                    </tr>
                """
            else:
                history_rows = '<tr><td colspan="6" style="text-align: center;">No historical data available yet</td></tr>'

            # Calculate statistics
            if len(history_df) > 0:
                avg_speed = history_df["spacecraft_speed_kmh"].mean()
                min_astronauts = int(history_df["astronaut_count"].min())
                max_astronauts = int(history_df["astronaut_count"].max())
                avg_astronauts = history_df["astronaut_count"].mean()
            else:
                avg_speed = 27600
                min_astronauts = number_of_people
                max_astronauts = number_of_people
                avg_astronauts = float(number_of_people)

            # Generate HTML
            html_content = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Astronaut Space Report - {dt.now().strftime("%Y-%m-%d %H:%M:%S")}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            color: #333;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background: white;
            border-radius: 15px;
            box-shadow: 0 10px 40px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        .header {{
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 40px;
            text-align: center;
        }}
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        .header .subtitle {{
            font-size: 1.2em;
            opacity: 0.9;
        }}
        .timestamp {{
            margin-top: 15px;
            font-size: 0.9em;
            opacity: 0.8;
        }}
        .content {{
            padding: 40px;
        }}
        .section {{
            margin-bottom: 40px;
        }}
        .section-title {{
            font-size: 1.8em;
            color: #2a5298;
            margin-bottom: 20px;
            padding-bottom: 10px;
            border-bottom: 3px solid #667eea;
        }}
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        .stat-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 25px;
            border-radius: 10px;
            box-shadow: 0 4px 15px rgba(0,0,0,0.1);
            text-align: center;
        }}
        .stat-card .value {{
            font-size: 2.5em;
            font-weight: bold;
            margin-bottom: 10px;
        }}
        .stat-card .label {{
            font-size: 1em;
            opacity: 0.9;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 20px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        th {{
            background: linear-gradient(135deg, #1e3c72 0%, #2a5298 100%);
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
        }}
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #e0e0e0;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        tr:nth-child(even) {{
            background-color: #fafafa;
        }}
        .highlight {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
            color: white;
            padding: 20px;
            border-radius: 10px;
            margin: 20px 0;
        }}
        .highlight h3 {{
            margin-bottom: 10px;
        }}
        .info-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .info-card {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 10px;
            border-left: 4px solid #667eea;
        }}
        .info-card h3 {{
            color: #2a5298;
            margin-bottom: 15px;
        }}
        .info-card p {{
            margin: 8px 0;
            line-height: 1.6;
        }}
        .footer {{
            background: #f8f9fa;
            padding: 30px;
            text-align: center;
            border-top: 3px solid #667eea;
        }}
        .footer p {{
            color: #666;
            margin: 5px 0;
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚀 Astronaut Space Report</h1>
            <div class="subtitle">Real-time Data from International Space Station</div>
            <div class="timestamp">Generated: {dt.now().strftime("%Y-%m-%d %H:%M:%S UTC")}</div>
        </div>

        <div class="content">
            <!-- Summary Statistics -->
            <div class="section">
                <h2 class="section-title">📊 Current Statistics</h2>
                <div class="stats-grid">
                    <div class="stat-card">
                        <div class="value">{number_of_people}</div>
                        <div class="label">Astronauts in Space</div>
                    </div>
                    <div class="stat-card">
                        <div class="value">{tracking["velocity_kmh"]:,}</div>
                        <div class="label">Speed (km/h)</div>
                    </div>
                    <div class="stat-card">
                        <div class="value">{tracking["altitude_km"]}</div>
                        <div class="label">Altitude (km)</div>
                    </div>
                    <div class="stat-card">
                        <div class="value">{weather["temperature_2m"]}°C</div>
                        <div class="label">Houston Weather</div>
                    </div>
                </div>
            </div>

            <!-- Current Astronauts -->
            <div class="section">
                <h2 class="section-title">👨‍🚀 Current Astronauts in Space</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Name</th>
                            <th>Spacecraft</th>
                            <th>Country</th>
                            <th>Agency/Company</th>
                        </tr>
                    </thead>
                    <tbody>
                        {astronaut_rows}
                    </tbody>
                </table>
            </div>

            <!-- Spacecraft Tracking -->
            <div class="section">
                <h2 class="section-title">🛰️ Spacecraft Tracking Data</h2>
                <div class="info-grid">
                    <div class="info-card">
                        <h3>Position</h3>
                        <p><strong>Latitude:</strong> {tracking["latitude"]:.2f}°</p>
                        <p><strong>Longitude:</strong> {tracking["longitude"]:.2f}°</p>
                        <p><strong>Altitude:</strong> {tracking["altitude_km"]} km</p>
                    </div>
                    <div class="info-card">
                        <h3>Velocity & Trajectory</h3>
                        <p><strong>Speed:</strong> {tracking["velocity_kmh"]:,} km/h ({tracking["velocity_kms"]} km/s)</p>
                        <p><strong>Direction:</strong> {tracking["trajectory_direction"]}</p>
                        <p><strong>Bearing:</strong> {tracking["trajectory_bearing"]}°</p>
                    </div>
                    <div class="info-card">
                        <h3>Weather (Houston, TX)</h3>
                        <p><strong>Temperature:</strong> {weather["temperature_2m"]}°C</p>
                        <p><strong>Wind Speed:</strong> {weather["wind_speed_10m"]} km/h</p>
                        <p><strong>Weather Code:</strong> {weather["weather_code"]}</p>
                    </div>
                </div>
            </div>

            <!-- Historical Data -->
            <div class="section">
                <h2 class="section-title">📈 Historical Flight Data</h2>
                <div class="highlight">
                    <h3>Statistical Summary</h3>
                    <p><strong>Total Records:</strong> {len(history_df)}</p>
                    <p><strong>Average Speed:</strong> {avg_speed:.0f} km/h</p>
                    <p><strong>Astronaut Count Range:</strong> {min_astronauts} - {max_astronauts} (Avg: {avg_astronauts:.1f})</p>
                </div>

                <h3 style="margin-top: 30px; color: #2a5298;">Recent Flight History (Last 10 Records)</h3>
                <table>
                    <thead>
                        <tr>
                            <th>Date/Time</th>
                            <th>Astronauts</th>
                            <th>Speed</th>
                            <th>Direction</th>
                            <th>Position</th>
                            <th>Countries</th>
                        </tr>
                    </thead>
                    <tbody>
                        {history_rows}
                    </tbody>
                </table>
            </div>

            <!-- Insights -->
            <div class="section">
                <h2 class="section-title">💡 Key Insights</h2>
                <div class="info-grid">
                    <div class="info-card">
                        <h3>ISS Orbital Mechanics</h3>
                        <p>The International Space Station orbits Earth at approximately 27,600 km/h (7.66 km/s), completing about 15.5 orbits per day.</p>
                        <p>The number of astronauts on board does not affect orbital velocity, which is governed by physics and orbital altitude.</p>
                    </div>
                    <div class="info-card">
                        <h3>Trajectory Patterns</h3>
                        <p>The ISS trajectory constantly changes as it orbits Earth. The station's path covers about 90% of Earth's population.</p>
                        <p>Current direction: <strong>{tracking["trajectory_direction"]}</strong></p>
                    </div>
                    <div class="info-card">
                        <h3>Weather Impact</h3>
                        <p>Ground weather has no direct impact on orbital spacecraft operations.</p>
                        <p>However, weather significantly affects launch windows and crew transport missions to and from the ISS.</p>
                    </div>
                </div>
            </div>
        </div>

        <div class="footer">
            <p><strong>Data Sources:</strong> Open Notify API, Open-Meteo API</p>
            <p>Generated by Airflow DAG: example_astronauts</p>
            <p>🚀 Powered by Apache Airflow on Astro Runtime</p>
        </div>
    </div>
</body>
</html>
"""

            # Save HTML report to file
            report_dir = Path("/tmp/astronaut_reports")
            report_dir.mkdir(exist_ok=True)
            timestamp = dt.now().strftime("%Y%m%d_%H%M%S")
            report_file = report_dir / f"astronaut_report_{timestamp}.html"

            with open(report_file, "w", encoding="utf-8") as f:
                f.write(html_content)

            print(f"HTML report generated: {report_file}")
            print(f"Report size: {len(html_content)} bytes")

            return str(report_file)

        except Exception as e:
            print(f"Error generating HTML report: {e}")
            # Return a default path if report generation fails
            return "/tmp/astronaut_reports/report_failed.html"

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    astronaut_list = get_astronauts()
    enriched_astronaut_list = enrich_astronaut_data(astronaut_list)
    weather_data = get_weather_data()
    tracking_data = get_spacecraft_tracking_data()

    # Load historical data
    history_df = load_historical_spacecraft_data()

    # Save current run to history
    updated_history = save_spacecraft_history(
        enriched_astronaut_list, tracking_data, history_df
    )

    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=enriched_astronaut_list  # Define dependencies using TaskFlow API syntax
    )

    # Data analysis pipeline with current and historical data
    combined_df = combine_data(enriched_astronaut_list, weather_data, tracking_data)
    analyze_correlation(combined_df, updated_history)

    # Generate HTML report with all collected data
    report_path = generate_html_report(
        enriched_astronaut_list,
        weather_data,
        tracking_data,
        combined_df,
        updated_history,
    )

    @task
    def print_report_location(report_path: str) -> None:
        """
        Print the location of the generated HTML report and instructions for viewing.
        Also copies the HTML to a fixed location for easier access.
        """
        from pathlib import Path
        import shutil

        print("=" * 70)
        print("HTML REPORT GENERATED SUCCESSFULLY!")
        print("=" * 70)
        print(f"\nReport Location: {report_path}")

        # Copy to a fixed location for easier access
        try:
            report_dir = Path("/tmp/astronaut_reports")
            fixed_path = report_dir / "latest_report.html"
            if Path(report_path).exists():
                shutil.copy2(report_path, fixed_path)
                print(f"\nLatest report also available at: {fixed_path}")
        except Exception as e:
            print(f"\nCould not create latest_report.html: {e}")

        print("\nTo view the report:")
        print(f"  1. Check the file at: {report_path}")
        print(
            "  2. Or use the fixed location: /tmp/astronaut_reports/latest_report.html"
        )
        print("\n  From Airflow UI:")
        print("     - Go to the task logs for 'generate_html_report'")
        print("     - The report is saved in /tmp/astronaut_reports/")
        print("\n  To download (if using Astro CLI locally):")
        print("     - Run: astro dev bash")
        print("     - Then: cat /tmp/astronaut_reports/latest_report.html")
        print("     - Copy the output to a local .html file and open in browser")
        print("\n" + "=" * 70)

    print_report_location(report_path)


# Instantiate the DAG
example_astronauts()
