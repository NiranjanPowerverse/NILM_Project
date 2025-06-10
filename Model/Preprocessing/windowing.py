import pandas as pd
from pathlib import Path
import os
import pandas as pd
import numpy as np

import pandas as pd
import numpy as np

def trim_inactive_periods(input_foldername, column_name, output_filename, output_folder, pre_padding=250, post_padding=250, timestamp_col="Unix"):
    """
    Keeps active segments with context before and after appliance usage.
    
    Args:
        input_foldername (str): Path to CSV file.
        column_name (str): Appliance power column name.
        pre_padding (int): Seconds before activity to keep.
        post_padding (int): Seconds after activity to keep.
        timestamp_col (str): Name of the UNIX timestamp column.
    
    Returns:
        pd.DataFrame: Trimmed DataFrame with contextual activity windows.
    """
    print("Trimming inactive periods...")
    df = pd.read_csv(input_foldername)

    # Calculate sampling interval
    unix_diff = df[timestamp_col].diff().median()
    if pd.isna(unix_diff) or unix_diff == 0:
        raise ValueError("Timestamps are not spaced properly.")
    
    # Convert seconds to sample count
    padding_before = int(pre_padding / unix_diff)
    padding_after = int(post_padding / unix_diff)

    power_series = df[column_name].values
    keep_mask = np.zeros_like(power_series, dtype=bool)

    segment_list = []
    inside_segment = False
    start_idx = None

    for idx, value in enumerate(power_series):
        if value > 0 and not inside_segment:
            inside_segment = True
            start_idx = idx

        elif value == 0 and inside_segment:
            inside_segment = False
            end_idx = idx
            start = max(0, start_idx - padding_before)
            end = min(len(power_series), end_idx + padding_after)
            segment_list.append([start, end])
            keep_mask[start:end] = True

    # Handle last segment if it ends at the end of the file
    if inside_segment and start_idx is not None:
        start = max(0, start_idx - padding_before)
        end = len(power_series)
        segment_list.append([start, end])
        keep_mask[start:end] = True

    number_of_segments = min(200, len(segment_list))

    # short_folder = os.path.join(output_folder, "short_segments")
    # long_folder = os.path.join(output_folder, "long_segments")
    # os.makedirs(short_folder, exist_ok=True)
    # os.makedirs(long_folder, exist_ok=True)

    for idx in range(number_of_segments):
        start, end = segment_list[idx]
        trimmed_df = df.iloc[start:end].reset_index(drop=True)
        trimmed_df["Unix"] = df["Unix"] - df["Unix"].iloc[0]


        # segment_folder = short_folder if (end - start) <= 150 else long_folder # seprate short and long segments
        output_path = os.path.join(output_folder, f"{output_filename}_{idx}.csv")
        
        try:
            trimmed_df.to_csv(output_path, index=False)
            print(f"Processed and saved: {input_foldername} → {output_path}")
        except Exception as e:
            print(f"❌ Error processing {input_foldername}: {e}")
    


                

def preprocess_folder(input_foldername,output_filename, output_folder, column_name):
    """
    Processes all CSV files in the input folder, trimming inactive periods
    and saving the results to the output folder.
    Args:
        input_folder (str): Path to the folder containing input CSV files.
        output_folder (str): Path to the folder where processed files will be saved.
        column_name (str): The appliance power column name (e.g., 'fridge_power').
        pre_padding (int): Seconds to keep before activity.
        post_padding (int): Seconds to keep after activity.
    """
    print("preprocessing folder")

    # if not os.path.exists(output_folder):
    #     os.makedirs(output_folder)

    # for filename in os.listdir(input_folder):
    print(f"Processing file: {input_foldername}")
    # if filename.endswith(".csv"):
        # file_path = os.path.join(input_folder, filename)
        # df = pd.read_csv(file_path)

    try:
        df_trimmed = trim_inactive_periods(
            file_name=input_foldername,
            column_name=column_name
        )
        df_trimmed["Unix"] = df_trimmed["Unix"] - df_trimmed["Unix"].iloc[0]
        print("output folder", output_folder)
        output_path = os.path.join(output_folder, output_filename)
        df_trimmed.to_csv(output_path, index=False)
        print(f"Processed and saved: {input_foldername} → {output_path}")
    except Exception as e:
        print(f"❌ Error processing {input_foldername}: {e}")

def main():

    appliance = ['Fridge','Freezer','Washing Machine','Washer Dryer','Tumble Dryer','Dishwasher','Microwave','Toaster','Kettle',
                'Computer','Television','Electric Heater','Hi-Fi','Router','Dehumidifier','Bread-maker',
                'Games Console','Network Site','Food Mixer','Overhead Fan','Vivarium','Pond Pump']
    for appliance in appliance:   
        base_dir = Path(__file__).resolve().parent.parent.parent
        reffit_path = os.path.join(base_dir, 'Reffit')
        folder_path = Path(f'{reffit_path}/{appliance}')
        try:
            # folder_path = pd.read_csv(f'{reffit_path}/{appliance}')
            output_path = os.path.join(reffit_path, 'Processed',appliance)
            os.makedirs(output_path, exist_ok=True)
            print("output path    ",output_path)
            for item in folder_path.iterdir():
                if item.is_file():
                    print("File:", item)

                    trim_inactive_periods(
                        input_foldername=item,
                        output_filename=item.name,
                        output_folder=output_path,
                        column_name=f'{appliance}'
                    )

                elif item.is_dir():
                    print("Directory:", item.name)
        except Exception as e:
            print(f"Error loading data: {e}")
if __name__ == "__main__":
    main()