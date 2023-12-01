"""Define a data type representing a flight"""

# Time = Float64
Time = Real
AirportCode = AbstractString

"""
    Flight

A flight between two airports.

# Attributes
- `flight_number::String`: The flight number.
- `origin::AirportCode`: The origin airport code.
- `destination::AirportCode`: The destination airport code.
- `scheduled_departure_time::Time`: The scheduled departure time.
- `scheduled_arrival_time::Time`: The scheduled arrival time.
- `simulated_departure_time::Union{Time, Nothing}`: The simulated departure time (optional).
- `simulated_arrival_time::Union{Time, Nothing}`: The simulated arrival time (optional).
- `actual_departure_time::Union{Time, Nothing}`: The actual departure time (optional).
- `actual_arrival_time::Union{Time, Nothing}`: The actual arrival time (optional).
"""
mutable struct Flight
    flight_number::AbstractString
    origin::AirportCode
    destination::AirportCode
    scheduled_departure_time::Time
    scheduled_arrival_time::Time
    simulated_departure_time::Union{Time,Nothing}
    simulated_arrival_time::Union{Time,Nothing}
    actual_departure_time::Union{Time,Nothing}
    actual_arrival_time::Union{Time,Nothing}
end

function Flight(
    flight_number::AbstractString,
    origin::AirportCode,
    destination::AirportCode,
    scheduled_departure_time::Time,
    scheduled_arrival_time::Time,
    actual_departure_time::Union{Time,Nothing},
    actual_arrival_time::Union{Time,Nothing},
)
    return Flight(
        flight_number,
        origin,
        destination,
        scheduled_departure_time,
        scheduled_arrival_time,
        nothing,
        nothing,
        actual_departure_time,
        actual_arrival_time
    )
end

function flight_code(flight::Flight)
    return flight.flight_number * " " * flight.origin * "->" * flight.destination
end