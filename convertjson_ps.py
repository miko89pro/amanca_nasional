import pandas as pd
import csv
import json

def merge_location_data(weather_file, location_file, output_file):
    # Read the weather data
    weather_data = pd.read_csv(weather_file, delimiter=';', header=None)

    # Read the location data
    location_data = pd.read_csv(location_file, delimiter=';', header=None)

    # Merge the data based on the common ID
    merged_data = pd.merge(weather_data, location_data, on=0)

    # Save the merged data to a new CSV file
    merged_data.to_csv(output_file, sep=';', index=False, header=False)

def csv_to_json(csv_file_path, json_file_path):
    data = {}
    
    with open(csv_file_path, 'r') as csv_file:
        csv_reader = csv.reader(csv_file, delimiter=';')
        next(csv_reader)  # Skip header row
        
        for row in csv_reader:
            location_id = row[0]
            
            if location_id not in data:
                data[location_id] = {
                    "id": location_id,
                    "kecamatan": row[11].strip(),
                    "kabkot": row[12].strip(),
                    "lintang": float(row[14]),
                    "bujur": float(row[15]),
                    "weather": []
                }
            
            weather = {
                "tanggal": row[1],
                "cuaca": int(float(row[8])),
                "suhu": row[7].strip(),
                "rh": row[6].strip(),
                "arah": row[9].strip(),
                "kec": row[10].strip()
            }
            
            data[location_id]["weather"].append(weather)
            print(row[0])
    print("ok")

    json_data = list(data.values())
    
    with open(json_file_path, 'w') as json_file:
        json.dump(json_data, json_file, indent=4)

# Example usage
def main():
    weather_files = [
        r'/sites/amandemen/data/presentweather-banten.csv',
        r'/sites/amandemen/data/presentweather-jakarta.csv',
        r'/sites/amandemen/data/presentweather-jawabarat.csv',
        r'/sites/amandemen/data/presentweather-sumsel.csv',
        r'/sites/amandemen/data/presentweather-sumut.csv',
        r'/sites/amandemen/data/presentweather-sulsel.csv',
        r'/sites/amandemen/data/presentweather-sultenggara.csv',
        r'/sites/amandemen/data/presentweather-sulut.csv',
        r'/sites/amandemen/data/presentweather-sumbar.csv',
        r'/sites/amandemen/data/presentweather-papuabarat.csv',
        r'/sites/amandemen/data/presentweather-riau.csv',
        r'/sites/amandemen/data/presentweather-sulbar.csv',
        r'/sites/amandemen/data/presentweather-maluku.csv',
        r'/sites/amandemen/data/presentweather-malut.csv',
        r'/sites/amandemen/data/presentweather-ntb.csv',
        r'/sites/amandemen/data/presentweather-ntt.csv',
        r'/sites/amandemen/data/presentweather-papua.csv',
        r'/sites/amandemen/data/presentweather-kaltim.csv',
        r'/sites/amandemen/data/presentweather-kepriau.csv',
        r'/sites/amandemen/data/presentweather-lampung.csv',
        r'/sites/amandemen/data/presentweather-jawatengah.csv',
        r'/sites/amandemen/data/presentweather-jawatimur.csv',
        r'/sites/amandemen/data/presentweather-kalbar.csv',
        r'/sites/amandemen/data/presentweather-kalsel.csv',
        r'/sites/amandemen/data/presentweather-kalteng.csv',
        r'/sites/amandemen/data/presentweather-babel.csv',
        r'/sites/amandemen/data/presentweather-bali.csv',
        r'/sites/amandemen/data/presentweather-bengkulu.csv',
        r'/sites/amandemen/data/presentweather-gorontalo.csv',
        r'/sites/amandemen/data/presentweather-jambi.csv',
        r'/sites/amandemen/data/presentweather-sulteng.csv',
        r'/sites/amandemen/data/presentweather-aceh.csv',
        r'/sites/amandemen/data/presentweather-kaluta.csv',
        r'/sites/amandemen/data/presentweather-bali.csv',
        r'/sites/amandemen/data/presentweather-jogyakarta.csv'
    ]
    location_file = r'/sites/amandemen/data/kecamatan_geofeatures.csv'
    output_files = [
        r'/sites/amandemen/data/merged_data_banten_ps.csv',
        r'/sites/amandemen/data/merged_data_jakarta_ps.csv',
        r'/sites/amandemen/data/merged_data_jawabarat_ps.csv',
        r'/sites/amandemen/data/merged_data_sumsel_ps.csv',
        r'/sites/amandemen/data/merged_data_sumut_ps.csv',
        r'/sites/amandemen/data/merged_data_sulsel_ps.csv',
        r'/sites/amandemen/data/merged_data_sultenggara_ps.csv',
        r'/sites/amandemen/data/merged_data_sulut_ps.csv',
        r'/sites/amandemen/data/merged_data_sumbar_ps.csv',
        r'/sites/amandemen/data/merged_data_papuabarat_ps.csv',
        r'/sites/amandemen/data/merged_data_riau_ps.csv',
        r'/sites/amandemen/data/merged_data_sulbar_ps.csv',
        r'/sites/amandemen/data/merged_data_maluku_ps.csv',
        r'/sites/amandemen/data/merged_data_malut_ps.csv',
        r'/sites/amandemen/data/merged_data_ntb_ps.csv',
        r'/sites/amandemen/data/merged_data_ntt_ps.csv',
        r'/sites/amandemen/data/merged_data_papua_ps.csv',
        r'/sites/amandemen/data/merged_data_kaltim_ps.csv',
        r'/sites/amandemen/data/merged_data_kepriau_ps.csv',
        r'/sites/amandemen/data/merged_data_lampung_ps.csv',
        r'/sites/amandemen/data/merged_data_jawatengah_ps.csv',
        r'/sites/amandemen/data/merged_data_jawatimur_ps.csv',
        r'/sites/amandemen/data/merged_data_kalbar_ps.csv',
        r'/sites/amandemen/data/merged_data_kalsel_ps.csv',
        r'/sites/amandemen/data/merged_data_kalteng_ps.csv',
        r'/sites/amandemen/data/merged_data_babel_ps.csv',
        r'/sites/amandemen/data/merged_data_bali_ps.csv',
        r'/sites/amandemen/data/merged_data_bengkulu_ps.csv',
        r'/sites/amandemen/data/merged_data_gorontalo_ps.csv',
        r'/sites/amandemen/data/merged_data_jambi_ps.csv',
        r'/sites/amandemen/data/merged_data_sulteng_ps.csv',
        r'/sites/amandemen/data/merged_data_aceh_ps.csv',
        r'/sites/amandemen/data/merged_data_kaluta_ps.csv',
        r'/sites/amandemen/data/merged_data_bali_ps.csv',
        r'/sites/amandemen/data/merged_data_jogyakarta_ps.csv'
        
    ]
    json_files = [
        r'/sites/amandemen/data/outputbanten_ps.json',
        r'/sites/amandemen/data/outputjakarta_ps.json',
        r'/sites/amandemen/data/outputjawabarat_ps.json',
        r'/sites/amandemen/data/outputsumsel_ps.json',
        r'/sites/amandemen/data/outputsumut_ps.json',
        r'/sites/amandemen/data/outputsulsel_ps.json',
        r'/sites/amandemen/data/outputsultenggara_ps.json',
        r'/sites/amandemen/data/outputsulut_ps.json',
        r'/sites/amandemen/data/outputsumbar_ps.json',
        r'/sites/amandemen/data/outputpapuabarat_ps.json',
        r'/sites/amandemen/data/outputriau_ps.json',
        r'/sites/amandemen/data/outputsulbar_ps.json',
        r'/sites/amandemen/data/outputmaluku_ps.json',
        r'/sites/amandemen/data/outputmalut_ps.json',
        r'/sites/amandemen/data/outputntb_ps.json',
        r'/sites/amandemen/data/outputntt_ps.json',
        r'/sites/amandemen/data/outputpapua_ps.json',
        r'/sites/amandemen/data/outputkaltim_ps.json',
        r'/sites/amandemen/data/outputkepriau_ps.json',
        r'/sites/amandemen/data/outputlampung_ps.json',
        r'/sites/amandemen/data/outputjawatengah_ps.json',
        r'/sites/amandemen/data/outputjawatimur_ps.json',
        r'/sites/amandemen/data/outputkalbar_ps.json',
        r'/sites/amandemen/data/outputkalsel_ps.json',
        r'/sites/amandemen/data/outputkalteng_ps.json',
        r'/sites/amandemen/data/outputbabel_ps.json',
        r'/sites/amandemen/data/outputbali_ps.json',
        r'/sites/amandemen/data/outputbengkulu_ps.json',
        r'/sites/amandemen/data/outputgorontalo_ps.json',
        r'/sites/amandemen/data/outputjambi_ps.json',
        r'/sites/amandemen/data/outputsulteng_ps.json',
        r'/sites/amandemen/data/outputaceh_ps.json',
        r'/sites/amandemen/data/outputkaluta_ps.json',
        r'/sites/amandemen/data/outputbali_ps.json',
         r'/sites/amandemen/data/outputjogyakarta_ps.json'
        
    ]

    for i in range(len(weather_files)):
        merge_location_data(weather_files[i], location_file, output_files[i])
        csv_to_json(output_files[i], json_files[i])

    print("Alhamdulillah Sudah Jadi File JSON PRESENT WEATHER")

if __name__ == "__main__":
    main()

