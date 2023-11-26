using DataFrames
using CSV
using Plots
using Dates
using FreqTables

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
    println(disrupted_start)
    println(disrupted_end)

    # Filter rows based on the date condition
    dfmt = dateformat"mm/dd/yyyy"

    nominal_data = filter(:Date => row -> Date(row, dfmt) < disrupted_start || Date(row, dfmt) > disrupted_end, df)
    disrupted_data = filter(:Date => row -> Date(row,dfmt) >= disrupted_start && Date(row, dfmt) <= disrupted_end, df)

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
    grouped_df = groupby(df, :Date)

    # Create a list of DataFrames, one for each date
    date_dataframes = [group for group in grouped_df]

    return date_dataframes
end

function convert_to_float_hours_optimized(time_series)
    """Convert time in 24-hour format to float hours since midnight.

    Args:
        time_series: a Series representing time in 24-hour format (HH:MM:SS)

    Returns:
        Float hours since midnight, or None for canceled flights
    """
    #Choosing datetime format
    dfmt = dateformat"HH:MM:SS"

    # Replace "--:--" with "23:59" (delay cancelled flights to end of day)
    time_series .= replace.(time_series, "--:--" => "23:59:00")

    # Replace "24:00" with "23:59" (midnight)
    time_series .= replace.(time_series, "24:00:00" => "23:59:00")

    # Convert time strings to datetime objects
    time_objects = Date(time_series, dfmt)
    # time_objects = Dates.datetime.(time_series, Dates.DateFormat("HH:MM"))

    # Extract hour and minute components
    hours_since_midnight = Dates.hour(time_objects) + Dates.minute(time_objects) / 60.0

    return hours_since_midnight
end

function remap_columns(df)

    # Define the mapping
    column_mapping = Dict(
        "Flight Number" => "flight_number",
        "Date" => "date",
        "Origin Airport Code" => "origin_airport",
        "Dest Airport Code" => "destination_airport",
        "Scheduled Departure Time" => "scheduled_departure_time",
        "Scheduled Arrival Time" => "scheduled_arrival_time",
        "Actual Departure Time" => "actual_departure_time",
        "Actual Arrival Time" => "actual_arrival_time",
    )

    # Filter the original DataFrame based on the desired columns
    column_keys =  collect(keys(column_mapping))
    remapped_df = df[:, column_keys]

    # Rename the columns based on the mapping
    rename!(remapped_df, column_mapping)

    # TODO: Type Mismatch in convert_to_float_hours_optimized function
    # Convert all times to hours since midnight
    # remapped_df.scheduled_departure_time .= convert_to_float_hours_optimized(
    #     remapped_df.scheduled_departure_time
    # )
    # remapped_df.scheduled_arrival_time .= convert_to_float_hours_optimized(
    #     remapped_df.scheduled_arrival_time
    # )
    # remapped_df.actual_departure_time .= convert_to_float_hours_optimized(
    #     remapped_df.actual_departure_time
    # )
    # remapped_df.actual_arrival_time .= convert_to_float_hours_optimized(
    #     remapped_df.actual_arrival_time
    # )

    # Convert date to DateTime type
    # remapped_df.date .= Dates.datetime.(remapped_df.date)

    return remapped_df
end

function top_N_df(df, number_of_airports::Int)
    """
    Get the top N airports by arrivals and filter the dataframe to include only
    flights between those airports.

    Args:
        df: the original dataframe
        number_of_airports: the number of airports to include
    """
    # Get the top-N airports by arrivals
    # TODO: Extract the airports in top_N_airports. top_N_airports only returns an nx1 vector of the freqtable counts but not the airport associate with the count.
    top_N_airports = sort!(freqtable(df.destination_airport), rev=true)[1:number_of_airports]

    # Filter the original DataFrame based on the desired airports
    filtered_df = filter(row -> in(row.origin_airport, top_N_airports) && in(row.destination_airport, top_N_airports), df)

    return filtered_df
end

function parse_flight(row::DataFrameRow)
    """
    parse_flight(row::DataFrameRow)

    Parse a row of the flight schedule data into a Flight object.

    # Arguments
    - `row::DataFrameRow`: A row of the flight schedule data, containing:
        - `:flight_number`: The flight number.
        - `:origin`: The origin airport code.
        - `:destination`: The destination airport code.
        - `:scheduled_departure_time`: The scheduled departure time.
        - `:scheduled_arrival_time`: The scheduled arrival time.
        - `:actual_departure_time`: The actual departure time.
        - `:actual_arrival_time`: The actual arrival time.

    # Returns
    - `flight::Flight`: The parsed flight.
    """
    flight = Flight(
        row[:flight_number],
        row[:origin],
        row[:destination],
        row[:scheduled_departure_time],
        row[:scheduled_arrival_time],
        row[:actual_departure_time],
        row[:actual_arrival_time],
    )
    return flight
end

function parse_schedule(schedule::DataFrame)
    """
    parse_schedule(schedule::DataFrame)

    Parse the flight schedule data into a vector of Flight objects.

    # Arguments
    - `schedule::DataFrame`: The flight schedule data.

    # Returns
    - `flights::Vector{Flight}`: The parsed flights.
    - `airports::Vector{Airport}`: The parsed airports.
    """
    flights = []
    for row in eachrow(schedule)
        push!(flights, parse_flight(row))
    end

    airports = []
    for airport_code in unique(vcat(schedule.origin, schedule.destination))
        push!(airports, Airport(airport_code))
    end

    return flights, airports
end
