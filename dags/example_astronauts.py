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
import math
import time


# Define the basic parameters of the DAG, like schedule and start_date
@dag(
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "Astro", "retries": 3},
    tags=["example"],
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
        r = requests.get("http://api.open-notify.org/astros.json")
        number_of_people_in_space = r.json()["number"]
        list_of_people_in_space = r.json()["people"]

        context["ti"].xcom_push(
            key="number_of_people_in_space", value=number_of_people_in_space
        )
        return list_of_people_in_space

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
        # Houston coordinates (NASA Johnson Space Center)
        latitude = 29.5583
        longitude = -95.0890

        url = f"https://api.open-meteo.com/v1/forecast?latitude={latitude}&longitude={longitude}&current=temperature_2m,wind_speed_10m,weather_code"
        r = requests.get(url)
        weather = r.json()["current"]

        print(
            f"Weather in Houston: {weather['temperature_2m']}°C, Wind: {weather['wind_speed_10m']} km/h"
        )
        return weather

    @task(outlets=[Dataset("spacecraft_tracking")])
    def get_spacecraft_tracking_data(**context) -> dict:
        """
        Fetch spacecraft position and velocity data from Open Notify ISS Location API.
        Calculates velocity by taking two position readings 5 seconds apart.
        Returns position coordinates, speed, and trajectory information.
        """
        # First position reading
        r1 = requests.get("http://api.open-notify.org/iss-now.json")
        data1 = r1.json()
        lat1 = float(data1["iss_position"]["latitude"])
        lon1 = float(data1["iss_position"]["longitude"])
        time1 = float(data1["timestamp"])

        # Wait 5 seconds for second reading to calculate velocity
        time.sleep(5)

        # Second position reading
        r2 = requests.get("http://api.open-notify.org/iss-now.json")
        data2 = r2.json()
        lat2 = float(data2["iss_position"]["latitude"])
        lon2 = float(data2["iss_position"]["longitude"])
        time2 = float(data2["timestamp"])

        # Calculate distance traveled using Haversine formula
        R = 6371  # Earth's radius in kilometers
        # Add orbital altitude of ISS (~420 km average)
        orbital_radius = R + 420

        dlat = math.radians(lat2 - lat1)
        dlon = math.radians(lon2 - lon1)
        a = (
            math.sin(dlat / 2) ** 2
            + math.cos(math.radians(lat1))
            * math.cos(math.radians(lat2))
            * math.sin(dlon / 2) ** 2
        )
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        distance_km = orbital_radius * c

        # Calculate velocity in km/s and km/h
        time_diff = time2 - time1
        if time_diff > 0:
            velocity_kmh = (distance_km / time_diff) * 3600
            velocity_kms = distance_km / time_diff
        else:
            velocity_kmh = 0
            velocity_kms = 0

        # Calculate trajectory bearing (direction of travel)
        dlon_rad = math.radians(lon2 - lon1)
        y = math.sin(dlon_rad) * math.cos(math.radians(lat2))
        x = math.cos(math.radians(lat1)) * math.sin(math.radians(lat2)) - math.sin(
            math.radians(lat1)
        ) * math.cos(math.radians(lat2)) * math.cos(dlon_rad)
        bearing = math.degrees(math.atan2(y, x))
        bearing = (bearing + 360) % 360  # Normalize to 0-360

        # Determine trajectory direction (simplified cardinal directions)
        if bearing >= 337.5 or bearing < 22.5:
            trajectory_direction = "North"
        elif 22.5 <= bearing < 67.5:
            trajectory_direction = "Northeast"
        elif 67.5 <= bearing < 112.5:
            trajectory_direction = "East"
        elif 112.5 <= bearing < 157.5:
            trajectory_direction = "Southeast"
        elif 157.5 <= bearing < 202.5:
            trajectory_direction = "South"
        elif 202.5 <= bearing < 247.5:
            trajectory_direction = "Southwest"
        elif 247.5 <= bearing < 292.5:
            trajectory_direction = "West"
        else:
            trajectory_direction = "Northwest"

        tracking_data = {
            "latitude": lat2,
            "longitude": lon2,
            "velocity_kmh": round(velocity_kmh, 2),
            "velocity_kms": round(velocity_kms, 2),
            "trajectory_bearing": round(bearing, 2),
            "trajectory_direction": trajectory_direction,
            "altitude_km": 420,  # Average ISS altitude
            "timestamp": time2,
        }

        print(
            f"Spacecraft Tracking - Position: ({lat2:.2f}, {lon2:.2f}), "
            f"Speed: {velocity_kmh:.2f} km/h ({velocity_kms:.2f} km/s), "
            f"Trajectory: {trajectory_direction} ({bearing:.2f}°)"
        )

        return tracking_data

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
    def analyze_correlation(df: pd.DataFrame) -> None:
        """
        Analyze correlation between astronaut count, spacecraft tracking data (speed, trajectory),
        and weather variables. Note: This is a single data point example. In production,
        you'd collect historical data over time to perform meaningful correlation analysis.
        """
        print("=" * 70)
        print("CORRELATION ANALYSIS - ASTRONAUTS, SPACECRAFT TRACKING & WEATHER")
        print("=" * 70)
        print("\nCurrent Data Point:")
        print(f"  Astronauts in space: {df['astronaut_count'].iloc[0]}")
        print(f"  Countries: {df['countries'].iloc[0]}")
        print(f"  Companies: {df['companies'].iloc[0]}")
        print("\nSpacecraft Tracking:")
        print(
            f"  Speed: {df['spacecraft_speed_kmh'].iloc[0]} km/h ({df['spacecraft_speed_kms'].iloc[0]} km/s)"
        )
        print(
            f"  Trajectory: {df['trajectory_direction'].iloc[0]} ({df['trajectory_bearing'].iloc[0]}°)"
        )
        print(
            f"  Position: Lat {df['spacecraft_latitude'].iloc[0]:.2f}, Lon {df['spacecraft_longitude'].iloc[0]:.2f}"
        )
        print(f"  Altitude: {df['spacecraft_altitude_km'].iloc[0]} km")
        print("\nWeather (Houston, TX):")
        print(f"  Temperature: {df['temperature'].iloc[0]}°C")
        print(f"  Wind Speed: {df['wind_speed'].iloc[0]} km/h")
        print(f"  Weather Code: {df['weather_code'].iloc[0]}")

        print("\n" + "=" * 70)
        print("CORRELATION INSIGHTS:")
        print("=" * 70)
        print("\n1. Spacecraft Speed vs Astronaut Count:")
        print("   - ISS orbital velocity is typically ~27,600 km/h (7.66 km/s)")
        print(f"   - Current speed: {df['spacecraft_speed_kmh'].iloc[0]} km/h")
        print(
            "   - Astronaut count doesn't affect orbital velocity (governed by physics)"
        )

        print("\n2. Trajectory Analysis:")
        print(f"   - Current direction: {df['trajectory_direction'].iloc[0]}")
        print("   - ISS completes ~15.5 orbits per day, trajectory constantly changes")
        print("   - No correlation expected between trajectory and astronaut count")

        print("\n3. Weather vs Spacecraft:")
        print("   - Ground weather has no direct impact on orbital spacecraft")
        print("   - However, weather affects launch windows and crew transport")

        print("\n" + "=" * 70)
        print("NOTE: Correlation analysis requires multiple data points.")
        print("This DAG would need to run multiple times and store results")
        print("to build a historical dataset for meaningful correlation.")
        print("=" * 70)

        # If you have historical data, uncomment to calculate correlation:
        # from scipy.stats import pearsonr
        # if len(df) > 1:
        #     corr_speed, p_speed = pearsonr(df["astronaut_count"], df["spacecraft_speed_kmh"])
        #     corr_temp, p_temp = pearsonr(df["astronaut_count"], df["temperature"])
        #     corr_wind, p_wind = pearsonr(df["astronaut_count"], df["wind_speed"])
        #     print(f"\nCorrelation with Spacecraft Speed: {corr_speed:.3f} (p-value: {p_speed:.3f})")
        #     print(f"\nCorrelation with Temperature: {corr_temp:.3f} (p-value: {p_temp:.3f})")
        #     print(f"Correlation with Wind Speed: {corr_wind:.3f} (p-value: {p_wind:.3f})")

    # Use dynamic task mapping to run the print_astronaut_craft task for each
    # Astronaut in space
    astronaut_list = get_astronauts()
    enriched_astronaut_list = enrich_astronaut_data(astronaut_list)
    weather_data = get_weather_data()
    tracking_data = get_spacecraft_tracking_data()

    print_astronaut_craft.partial(greeting="Hello! :)").expand(
        person_in_space=enriched_astronaut_list  # Define dependencies using TaskFlow API syntax
    )

    # Data analysis pipeline
    combined_df = combine_data(enriched_astronaut_list, weather_data, tracking_data)
    analyze_correlation(combined_df)


# Instantiate the DAG
example_astronauts()
