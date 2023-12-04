from fastapi import APIRouter
from pydantic import BaseModel

from utils.methods import get_averages_for_columns_including_word, get_averages_for_columns_including_word_regex, get_sum_for_columns_including_word, get_sum_for_columns_including_word_regex

# Create a class for the options object.
class Option_Object(BaseModel):
    filter: str | None = None

router = APIRouter()

csv_column_names = [
    "Average energy efficiency improvements costs of detached gas boiler (GBP)",
    "Average energy efficiency improvements costs of detached oil boiler (GBP)",
    "Average energy efficiency improvements costs of detached resistance heating (GBP)",
    "Average energy efficiency improvements costs of detached biomass boiler (GBP)",
    "Average energy efficiency improvements costs of flat gas boiler (GBP)",
    "Average energy efficiency improvements costs of flat oil boiler (GBP)",
    "Average energy efficiency improvements costs of flat resistance heating (GBP)",
    "Average energy efficiency improvements costs of flat biomass boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached gas boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached oil boiler (GBP)",
    "Average energy efficiency improvements costs of semi-detached resistance heating (GBP)",
    "Average energy efficiency improvements costs of semi-detached biomass boiler (GBP)",
    "Average energy efficiency improvements costs of terraced gas boiler (GBP)",
    "Average energy efficiency improvements costs of terraced oil boiler (GBP)",
    "Average energy efficiency improvements costs of terraced resistance heating (GBP)",
    "Average energy efficiency improvements costs of terraced biomass boiler (GBP)"
]

def create_data_object():
    data = {}
    data['average'] = {}
    data['sum'] = {}
    return data

@router.post("/api/Energy_efficiency_improvements_costs_LA")
async def get_requested_data_for_charts(options_object: list[Option_Object]):
    heating_type_columns = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]
    dwelling_type_columns = ["detached", "flat", "semi-detached", "terraced"]

    datasets = {}
    # Loop through the options object. Create a new object for each filter containing the average and sum.
    for object in options_object:
        if object.filter == "heating_type":
            key_words = heating_type_columns
            use_regex = True
        elif object.filter == "dwelling_type":
            key_words = dwelling_type_columns
            use_regex = False
        
        # Create a unique key based on the filter.
        key = f'{object.filter}'
        # Create a object containing the keys average and sum.
        data = create_data_object()

                # Create a object containing the keys average and sum.
        data = create_data_object()
        data['average'] = await (
            (get_averages_for_columns_including_word_regex if use_regex else get_averages_for_columns_including_word) \
                                    (key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
                                    )
        data['sum'] = await (
            (get_sum_for_columns_including_word_regex if use_regex else get_sum_for_columns_including_word) \
                                    (key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
                                    )
        # Add the key alongside it's data to the overall datasets object.
        datasets[key] = data

    # Return the datasets array.
    return datasets