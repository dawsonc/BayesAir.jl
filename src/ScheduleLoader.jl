using DataFrames

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
function parse_flight(row::DataFrameRow)
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

"""
    parse_schedule(schedule::DataFrame)

Parse the flight schedule data into a vector of Flight objects.

# Arguments
- `schedule::DataFrame`: The flight schedule data.

# Returns
- `flights::Vector{Flight}`: The parsed flights.
- `airports::Vector{Airport}`: The parsed airports.
"""
function parse_schedule(schedule::DataFrame)
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
