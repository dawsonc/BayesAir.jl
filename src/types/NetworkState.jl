using Gen

"""
    NetworkState

Represents the state of the network at a given time.

# Attributes
- `airports::Dict{<:AirportCode, Airport}`: A dictionary mapping airport codes to
    `Airport` objects.
- `pending_flights::Vector{Flight}`: A list of flights that have not yet departed.
- `in_transit_flights::Vector{Tuple{Flight, Time}}`: A list of flights that
    have departed but not yet arrived, along with the time at which they will arrive.
- `completed_flights::Vector{Flight}`: A list of flights that have arrived.
"""
mutable struct NetworkState
    airports::Dict{<:AirportCode,Airport}
    pending_flights::Vector{Flight}
    in_transit_flights::Vector{Tuple{Flight,Time}}
    completed_flights::Vector{Flight}
end

function NetworkState(airports::Dict{<:AirportCode,Airport}, pending_flights::Vector{Flight})
    return NetworkState(
        airports,
        pending_flights,
        Vector{Tuple{Flight,Time}}(),
        Vector{Flight}()
    )
end

function sort_pending_flights!(state::NetworkState)
    sort!(state.pending_flights, by=f -> f.scheduled_departure_time)
end

"""
    pop_ready_to_depart_flights!(state::NetworkState, time::Time)

Remove all flights from `state.pending_flights` that are ready to depart at
`time`, and return them in a vector.

# Arguments
- `state::NetworkState`: The network state.
- `time::Time`: The current time.

# Return value
- `Vector{Tuple{Flight, Time}}`: The flights that are ready to depart, along with the
    times at which they were ready to depart.
"""
function pop_ready_to_depart_flights!(state::NetworkState, time::Time)
    ready_to_depart = Vector{Tuple{Flight,Time}}()
    new_pending_flights = Vector{Flight}()

    # Remove flights that are ready to depart
    for flight in state.pending_flights
        # A flight is ready to depart if its schedule departure time is less than
        # the current time and its source airport has an available aircraft and crew.
        if flight.scheduled_departure_time <= time &&
           num_available_aircraft(state.airports[flight.origin]) > 0 &&
           num_available_crew(state.airports[flight.origin]) > 0

            # Remove the aircraft and crew from the airport
            aircraft_turnaround_t = pop!(state.airports[flight.origin].available_aircraft)
            crew_turnaround_t = pop!(state.airports[flight.origin].available_crew)

            ready_t = max(aircraft_turnaround_t, crew_turnaround_t)
            ready_t = max(ready_t, flight.scheduled_departure_time)

            push!(ready_to_depart, (flight, ready_t))
        else
            # If the flight isn't ready to depart, keep it in the list of pending flights
            push!(new_pending_flights, flight)
        end
    end

    # Update the list of pending flights
    state.pending_flights = new_pending_flights

    return ready_to_depart
end

"""
    add_in_transit_flights!(
        state::NetworkState,
        departing_flights::Vector{Flight},
        travel_times::Dict{Tuple{AirportCode, AirportCode}, Time}
        travel_time_variation: Time
    )

Add a Vector of flights to the in-transit flights list, sampling random travel times
for each flight.

This is a generative function because it makes random choices about travel times

# Arguments
- `state::NetworkState`: The network state.
- `departing_flights::Vector{Flight}`: The flights that are departing.
- `travel_times::Dict{Tuple{AirportCode, AirportCode}, Time}`: A dictionary mapping
    pairs of airport codes to nominal travel times.
- `travel_time_variation::Time`: The fractional variation in travel times
"""
@gen function add_in_transit_flights!(
    state::NetworkState,
    departing_flights::Vector{Flight},
    travel_times::Dict{Tuple{AirportCode,AirportCode},Time},
    travel_time_variation::Time
)
    # For each departing flight, sample a travel time and then add it to the
    # in-transit flights list
    for flight in departing_flights
        nominal_travel_time = travel_times[(flight.origin, flight.destination)]
        nominal_travel_time = max(nominal_travel_time, 0.0)
        # travel_time = {(flight_code(flight), :travel_time)} ~ normal(
        #     nominal_travel_time,
        #     nominal_travel_time * travel_time_variation
        # )
        travel_time = nominal_travel_time  # TODO is deterministic model enough?

        # Add the flight to the in-transit flights list
        push!(state.in_transit_flights,
            (flight, flight.simulated_departure_time + travel_time))

        @debug "Flight $(flight_code(flight)) assigned travel time $(travel_time)" *
               " (nominal travel time: $(nominal_travel_time))," *
               " will arrive at $(flight.simulated_departure_time + travel_time)"
    end
end


"""
    add_completed_flights!(state::NetworkState, landing_flights::Vector{Flight})

Add a Vector of flights to the completed flights list.

# Arguments
- `state::NetworkState`: The network state.
- `landing_flights::Vector{Flight}`: The flights that are landing.
"""
function add_completed_flights!(state::NetworkState, landing_flights::Vector{Flight})
    for flight in landing_flights
        push!(state.completed_flights, flight)
    end
end

"""
    update_in_transit_flights!(state::NetworkState, time::Time)

Update the in-transit flights list by removing flights that have arrived,
moving arriving flights to the runway service queue at their destination
airports.

# Arguments
- `state::NetworkState`: The network state.
- `time::Time`: The current time.
"""
function update_in_transit_flights!(state::NetworkState, time::Time)
    new_in_transit_flights = Vector{Tuple{Flight,Time}}()

    for (flight, arrival_t) in state.in_transit_flights
        # If the flight is ready to arrive, add it to the runway queue at its destination
        if arrival_t <= time
            push!(state.airports[flight.destination].runway_queue,
                QueueEntry(flight, arrival_t))
        else
            # Otherwise, keep it in the in-transit flights list
            push!(new_in_transit_flights, (flight, arrival_t))
        end
    end

    state.in_transit_flights = new_in_transit_flights
end
