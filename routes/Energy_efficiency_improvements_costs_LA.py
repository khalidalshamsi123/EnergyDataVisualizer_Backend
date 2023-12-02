from fastapi import APIRouter

from utils.methods import get_averages_for_columns_including_word, get_averages_for_columns_including_word_regex, get_sum_for_columns_including_word, get_sum_for_columns_including_word_regex

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

@router.get("/api/Energy_efficiency_improvements_costs_LA/dwelling_type")
async def get_average_EE_improvement_costs_per_dwelling():
    key_words = ["detached", "semi-detached", "flat", "terraced"]
    average_ee_improvement_costs_per_dwelling_type = await get_averages_for_columns_including_word(key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
    sum_ee_improvement_costs_per_dwelling_type = await get_sum_for_columns_including_word(key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
    return { "average": average_ee_improvement_costs_per_dwelling_type, "sum": sum_ee_improvement_costs_per_dwelling_type }

@router.get('/api/Energy_efficiency_improvements_costs_LA/heating_type')
async def get_average_EE_improvement_costs_per_heating_type():
    key_words = ["gas boiler", "oil boiler", "resistance heating", "biomass boiler"]
    average_ee_improvement_costs_per_heating_type = await get_averages_for_columns_including_word_regex(key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
    sum_ee_improvement_costs_per_heating_type = await get_sum_for_columns_including_word_regex(key_words, csv_column_names, 'Energy_efficiency_improvements_costs_LA')
    return { "average": average_ee_improvement_costs_per_heating_type, "sum": sum_ee_improvement_costs_per_heating_type }