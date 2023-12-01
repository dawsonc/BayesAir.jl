using DataFrames
using CSV
using Plots
using Dates

function load_all_data()
    """Get all available WN flight data"""
    # Get the directory of the current file -- this should be BayesAir.jl-main
    script_directory = dirname(@__FILE__)
    root = dirname(script_directory)

    # Construct the relative path to the CSV file
    nominal_file_path = joinpath(root, "data", "wn_dec01_dec20.csv")
    disrupted_file_path = joinpath(root, "data", "wn_dec21_dec30.csv")

    # Read the CSV files into a DataFrame
    nominal_df = CSV.File(nominal_file_path) |> DataFrame
    disrupted_df = CSV.File(disrupted_file_path) |> DataFrame

    # Concatenate the two DataFrames
    df = vcat(nominal_df, disrupted_df)

    return df
end

function split_nominal_disrupted_data(df)
    """Split dataset into nominal data and disrupted data.

    The disruption occurred between 2022-12-21 and 2022-12-30

    Args:
        df: the dataframe of flight data

    Returns:
        A dataframe filtered to include only flights outside the disrupted period
        A dataframe filtered to include flights within the disrupted period
    """
    # Filter rows based on the date condition
    disrupted_start = Date(2022, 12, 21)
    disrupted_end = Date(2022, 12, 30)

    # Filter rows based on the date condition
    dfmt = dateformat"mm/dd/yyyy"

    nominal_data = filter(row -> row.date < disrupted_start || row.date > disrupted_end, df)
    disrupted_data = filter(row -> row.date >= disrupted_start && row.date <= disrupted_end, df)

    return nominal_data, disrupted_data
end

function split_by_date(df)
    """Split a DataFrame of flights into a list of DataFrames, one for each date.

    Args:
        df: the dataframe of flight data with a "Date" column

    Returns:
        A list of DataFrames, each containing data for a specific date
    """
    # Group the DataFrame by the "Date" column
    grouped_df = groupby(df, :date)

    # Create a list of DataFrames, one for each date
    date_dataframes = [group for group in grouped_df]

    # Sort each by scheduled departure time
    for date_df in date_dataframes
        sort!(date_df, :scheduled_departure_time)
    end

    return date_dataframes
end

"""
Convert time in 24-hour format to float hours since midnight.

Args:
    time_series (Vector{String}): A vector of time strings in 24-hour format (HH:MM).

Returns:
    Vector{Union{Float64, Nothing}}: A vector of float hours since midnight, or Nothing for canceled flights.
"""
function convert_to_float_hours_optimized(time_series::Vector)
    # Replace "--:--" with "23:59" (delay canceled flights to end of day)
    replace!(time_series, "--:--" => "23:59")

    # Replace "24:00" with "23:59" (midnight)
    replace!(time_series, "24:00" => "23:59")

    # Convert time strings to DateTime objects
    if typeof(time_series[1]) != Dates.Time
        time_series = DateTime.(time_series, dateformat"HH:MM")
    end

    # Extract hour and minute components
    hours_since_midnight = hour.(time_series) + minute.(time_series) / 60.0

    return hours_since_midnight
end

"""
Remap columns in the DataFrame to the expected names.

Args:
    df (DataFrame): The original dataframe.

Returns:
    DataFrame: A new dataframe with remapped columns.
"""
function remap_columns(df::DataFrame)
    # Define the mapping
    column_mapping = Dict(
        "Flight Number" => "flight_number",
        "Date" => "date",
        "Origin Airport Code" => "origin",
        "Dest Airport Code" => "destination",
        "Scheduled Departure Time" => "scheduled_departure_time",
        "Scheduled Arrival Time" => "scheduled_arrival_time",
        "Actual Departure Time" => "actual_departure_time",
        "Actual Arrival Time" => "actual_arrival_time",
    )

    # Filter the original DataFrame based on the desired columns
    remapped_df = df[:, collect(keys(column_mapping))]

    # Rename the columns based on the mapping
    rename!(remapped_df, column_mapping)

    # Convert flight numbers to strings
    remapped_df.flight_number .= string.(remapped_df.flight_number)

    # Convert all times to hours since midnight
    remapped_df.scheduled_departure_time .= convert_to_float_hours_optimized(remapped_df.scheduled_departure_time)
    remapped_df.scheduled_arrival_time .= convert_to_float_hours_optimized(remapped_df.scheduled_arrival_time)
    remapped_df.actual_departure_time .= convert_to_float_hours_optimized(remapped_df.actual_departure_time)
    remapped_df.actual_arrival_time .= convert_to_float_hours_optimized(remapped_df.actual_arrival_time)

    # If any flight is en-route at midnight, it's duration will be negative unless we add 24 hours
    # to the actual and scheduled arrival times
    scheduled_duration = remapped_df.scheduled_arrival_time - remapped_df.scheduled_departure_time
    actual_duration = remapped_df.actual_arrival_time - remapped_df.actual_departure_time
    remapped_df.scheduled_arrival_time .= remapped_df.scheduled_arrival_time .+ (scheduled_duration .< 0) .* 24
    remapped_df.actual_arrival_time .= remapped_df.actual_arrival_time .+ (actual_duration .< 0) .* 24

    # Convert date to DateTime type
    remapped_df.date .= Dates.DateTime.(remapped_df.date, dateformat"mm/dd/yyyy")

    return remapped_df
end

"""
Get the top N airports by arrivals and filter the dataframe to include only flights between those airports.

Args:
    df (DataFrame): The original dataframe.
    number_of_airports (Int): The number of airports to include.
"""
function top_N_df(df::DataFrame, number_of_airports::Int)
    # Get the top-N airports by arrivals
    top_N_airports = combine(groupby(df, :destination), :destination => length => :nrow)
    sort!(top_N_airports, :nrow, rev=true)
    top_N_airports = top_N_airports[1:number_of_airports, :]

    # Filter the original DataFrame based on the desired airports
    filtered_df = filter(row -> row.destination in top_N_airports.destination && row.origin in top_N_airports.destination, df)

    return filtered_df
end
