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
# Though this method is more efficient, it's less accurate. As it will return ALL columns that contain the key word, even if it's apart of a larger word.
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