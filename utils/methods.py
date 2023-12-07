import polars as pl

from utils.redis_pool import get_redis

# Returns a list of tuples. Each tuple contains a key word from the list of words provided, and the mean of the columns that contained that key word in the CSV specified.
async def get_averages_for_columns_including_word(list_of_words, list_of_relevant_columns, csv_name):
    ipc = await get_redis().get(f'caches:dataframes:{csv_name}:original')
    data = pl.read_ipc_stream(ipc, columns=list_of_relevant_columns)

    sorted_by_key_words = []
    columns = data.columns

    for column in columns:
        column_split_by_space = column.split(' ')
        # For each word in the list of words we're looking for, we check if it's in the column name.
        for search_word in list_of_words:
            for column_word in column_split_by_space:
                # If the word is in the column name, we fetch the data for that column and append it to an array of tuples.
                if column_word == search_word:
                    # Fetch data and append to array of tuples
                    column_data = data.lazy().select(pl.col(column)).collect()

                    column_data.columns = [search_word]

                    # Check if is already present in the list of tuples.
                    if any(search_word == item[0] for item in sorted_by_key_words):
                        for index, item in enumerate(sorted_by_key_words):
                            # If the word exists in the list of tuples already. Update the existing tuple, by concatenating the new dataframe to the existing.
                            if search_word == item[0]:
                                sorted_by_key_words[index] = (search_word, pl.concat([column_data, item[1]]))
                                break
                    else:
                        # If the word doesn't exist in the list of tuples, append a new tuple for the word. With the dataframe of the column data that contained that word.
                        sorted_by_key_words.append((search_word, column_data))
                    break  # Break the inner loop once a match is found

    final_averages = []

    # Will iterate over tuple in sorted_by_key_words. Tuple[0] is the key word.
    # Tuple[1] is the DataFrame containing the data of the columns that contained that key word.
    for tuple in sorted_by_key_words:
        key_words = tuple[1].columns
        count = 0
        sum = 0
        for key_word in key_words:
            for number in tuple[1][key_word]:
                sum = sum + number
                count = count + 1

        # We divide the numbers we've tallied up so far by the amount
        # of data points there were relating to a particular key word.
        final_mean = sum / count
        final_averages.append([tuple[0], final_mean]) # The search/key word e.g., oil boiler. And the mean.

    return final_averages

# Returns a list of tuples. Each tuple contains a key word from the list of words provided, and the mean of the columns that contained that key word in the CSV specified.
# This differs from the other method in that it uses regex to find the columns that contain the key words.
# Though this method is more efficient, it's less accurate.
# As it will return ALL columns that contain the key word, even if it's apart of a larger word.
# E.g., If you are searching for 'detached' it will return columns that contain 'detached' and 'semi-detached'.
async def get_averages_for_columns_including_word_regex(list_of_words, list_of_relevant_columns, csv_name):
    ipc = await get_redis().get(f'caches:dataframes:{csv_name}:original')
    data = pl.read_ipc_stream(ipc, columns=list_of_relevant_columns)

    # We iterate over the list of key words, and select all columns that contain this word.
    # For each, returning a tuple containing the key word and the DataFrame with the columns/data
    # that was under the column name containing that key word.
    sorted_by_key_words = [
        (
            key_word,
            data.lazy()
            .select(pl.col(f"^.*({key_word}).*$"))
            .collect()
        )
        for key_word in list_of_words
    ]

    final_averages = []

    # Will iterate over tuple in sorted_by_key_words. Tuple[0] is the key word.
    # Tuple[1] is the DataFrame containing the data/rows of the columns that contained that key word.
    for tuple in sorted_by_key_words:
        column_names = tuple[1].columns
        count = 0
        sum = 0
        for column_name in column_names:
            for number in tuple[1][column_name]:
                sum = sum + number
                count = count + 1

        # We divide the numbers we've tallied up so far by the amount
        # of data points there were relating to a particular key word.
        final_mean = sum / count
        final_averages.append([tuple[0], final_mean]) # The search/key word e.g., oil boiler. And the mean.

    return final_averages

# Returns a list of tuples. Each tuple contains a key word from the list of words provided, and the sum of the columns that contained that key word in the CSV specified.
async def get_sum_for_columns_including_word(list_of_words, list_of_relevant_columns, csv_name):
    ipc = await get_redis().get(f'caches:dataframes:{csv_name}:original')
    data = pl.read_ipc_stream(ipc, columns=list_of_relevant_columns)

    sorted_by_key_words = []
    columns = data.columns

    for column in columns:
        column_split_by_space = column.split(' ')
        # For each word in the list of words we're looking for, we check if it's in the column name.
        for search_word in list_of_words:
            for column_word in column_split_by_space:
                # If the word is in the column name, we fetch the data for that column and append it to an array of tuples.
                if column_word == search_word:
                    # Fetch data and append to array of tuples
                    column_data = data.lazy().select(pl.col(column)).collect()

                    column_data.columns = [search_word]

                    # Check if is already present in the list of tuples.
                    if any(search_word == item[0] for item in sorted_by_key_words):
                        for index, item in enumerate(sorted_by_key_words):
                            # If the word exists in the list of tuples already. Update the existing tuple, by concatenating the new dataframe to the existing.
                            if search_word == item[0]:
                                sorted_by_key_words[index] = (search_word, pl.concat([column_data, item[1]]))
                                break
                    else:
                        # If the word doesn't exist in the list of tuples, append a new tuple for the word. With the dataframe of the column data that contained that word.
                        sorted_by_key_words.append((search_word, column_data))
                    break  # Break the inner loop once a match is found

    final_sums = []
    # Will iterate over tuple in sorted_by_key_words. Tuple[0] is the key word.
    # Tuple[1] is the DataFrame containing the data of the columns that contained that key word.
    for tuple in sorted_by_key_words:
        key_words = tuple[1].columns
        sum = 0
        for key_word in key_words:
            # We sum all the values in the column that contained the key word.
            sum_of_values = tuple[1][key_word].sum()
            sum += sum_of_values

        # We push the sum of all the values associated with the particular key word.
        final_sums.append([tuple[0], sum]) # The search/key word e.g., oil boiler. And the sum.

    return final_sums

# Returns a list of tuples. Each tuple contains a key word from the list of words provided, and the sum of the columns that contained that key word in the CSV specified.
# This differs from the other method in that it uses regex to find the columns that contain the key words.
# Though this method is more efficient, it's less accurate. As it will return ALL columns that contain the key word, even if it's apart of a larger word.
# E.g., If you are searching for 'detached' it will return columns that contain 'detached' and 'semi-detached'.
async def get_sum_for_columns_including_word_regex(list_of_words, list_of_relevant_columns, csv_name):
    ipc = await get_redis().get(f'caches:dataframes:{csv_name}:original')
    data = pl.read_ipc_stream(ipc, columns=list_of_relevant_columns)

    # We iterate over the list of key words, and select all columns that contain this word.
    # For each, returning a tuple containing the key word and the DataFrame with the columns/data
    # that was under the column name containing that key word.
    sorted_by_key_words = [
        (
            key_word,
            data.lazy()
            .select(pl.col(f"^.*({key_word}).*$"))
            .collect()
        )
        for key_word in list_of_words
    ]

    final_sums = []

    # Will iterate over tuple in sorted_by_key_words. Tuple[0] is the key word.
    # Tuple[1] is the DataFrame containing the data/rows of the columns that contained that key word.
    for tuple in sorted_by_key_words:
        key_words = tuple[1].columns
        sum = 0
        for key_word in key_words:
            # We sum all the values in the column that contained the key word.
            sum_of_values = tuple[1][key_word].sum()
            sum += sum_of_values

        # We push the sum of all the values associated with the particular key word.  
        final_sums.append([tuple[0], sum]) # The search/key word e.g., oil boiler. And the sum.

    return final_sums

# Logic for getting values needed to visualise head demand before and after energy
# efficiency improvements annually in the form of a bar-chart.
async def get_thermal_characteristics_csv_data():
    # Get the IPC stream of bytes from Redis using the relevant key.
    afterEE_Data = await get_redis().get("caches:dataframes:Thermal_characteristics_afterEE:original")
    # Loads the IPC stream of bytes into a DataFrame. Only loads the columns "Average annual heat demand kWh" and "Heating systems" into memory.
    afterEE_Data = pl.read_ipc_stream(afterEE_Data, columns=["Average annual heat demand kWh", "Heating systems"])

    # Essentially here I am setting up a series of operations to be performed on the DataFrame.
    # We select the column Heating systems, and filter out any rows that have values that another
    # row has for that column. We then collect the values.

    # TODO. It would make much more sense to read both tables into memory before this query.
    # then perform a join and get the unique values from the joined tables combined. Since
    # right now we are just creating the list of heating types from one of the CSVs.
    # IMPORTANT: I should only join the tables temporarily. We don't want to use this dataframe in any other parts of this code.
    unique_heating_types = ( 
        afterEE_Data.lazy()
            .select(["Heating systems"])
            .unique(maintain_order=True)
            .collect()["Heating systems"] 
    )

    # We create a list of tuples. Each tuple contains a unique heating type and a mean average.
    # The mean is calculated with the Average annual heat demand kWh columns values for all rows
    # that have the same heating type. Generating a overall mean for each unique heating type.
    afterEE_Heating_Type_Means = [
        (
            heating_type,
            afterEE_Data.lazy()
                    .filter(pl.col("Heating systems") == heating_type)
                    .select([pl.col("Average annual heat demand kWh")])
                    .mean()
                    .collect()['Average annual heat demand kWh']
        )
        for heating_type in unique_heating_types
    ]

    # Get the max value out of all the averages in the list of tuples. This will be used for the y-axis scale.
    # The _ is a throwaway variable. We don't need the heating type here.
    afterEE_Data_Max = max([result.item() for _, result in afterEE_Heating_Type_Means])

    # We create a list of tuples. We iterate through each unique heating type and mean value
    # within the afterEE_Heating_Type_Means list of tuples.
    afterEE_Data = [(heating_type, result.item())
               for heating_type, result in afterEE_Heating_Type_Means]

    # Get the IPC stream of bytes from Redis using the relevant key.
    beforeEE_Data = await get_redis().get("caches:dataframes:Thermal_characteristics_beforeEE:original")
    # Loads the IPC stream of bytes from Redis into a DataFrame. Only loads the columns "Average annual heat demand kWh" and "Heating systems" into memory.
    beforeEE_Data = pl.read_ipc_stream(beforeEE_Data, columns=["Average annual heat demand kWh", "Heating systems"])

    # We create a list of tuples. Each tuple contains a unique heating type and a mean average.
    # The mean is calculated with the Average annual heat demand kWh columns values for all rows
    # that have the same heating type. Generating a overall mean for each unique heating type.
    beforeEE_Heating_Type_Means = [
        (
            heating_type,
            beforeEE_Data.lazy()
                    .filter(pl.col("Heating systems") == heating_type)
                    .select([pl.col("Average annual heat demand kWh")])
                    .mean()
                    .collect()['Average annual heat demand kWh']
        )
        for heating_type in unique_heating_types
    ]

    # Get the max value out of all the averages in the list of tuples. This will be used for the y-axis scale.
    # The _ is a throwaway variable. We don't need the heating type here.
    beforeEE_Data_Max = max([result.item() for _, result in beforeEE_Heating_Type_Means])

    # We create a list of tuples. We iterate through each unique heating type and mean value
    # within the beforeEE_Heating_Type_Means list of tuples.
    beforeEE_Data = [(heating_type, result.item())
               for heating_type, result in beforeEE_Heating_Type_Means]
    
    max_value = afterEE_Data_Max if afterEE_Data_Max > beforeEE_Data_Max else beforeEE_Data_Max

    bar_chart_json = {
        "heating_types": unique_heating_types.to_list(),
        "datasets": [ beforeEE_Data, afterEE_Data ],
        "max": max_value
    }

    return bar_chart_json