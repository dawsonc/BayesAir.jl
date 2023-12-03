module BayesAir

using Gen

include("types/Flight.jl")
include("types/Airport.jl")
include("types/NetworkState.jl")
include("ScheduleParser.jl")
include("DataLoader.jl")

"""
    simulate_day(
        state::NetworkState,
        end_time::Time,
        dt::Time,
        travel_times::Dict{Tuple{AirportCode,AirportCode},Time},
        travel_time_variation::Time,
        measurement_variation::Time,
    )

Simulate the network state from `state` until `end_time`, in increments of `dt`.

# Arguments
- `state::NetworkState`: The initial network state.
- `end_time::Time`: The time at which to stop the simulation.
- `dt::Time`: The time increment for the simulation.
- `travel_times::Dict{Tuple{AirportCode,AirportCode},Time}`: A dictionary mapping
    pairs of airport codes to travel times between those airports.
- `travel_time_variation::Time`: The fractional variation of travel time.
- `measurement_variation::Time`: The fractional variation of measurement noise.
"""
@gen function simulate_day(
    state::NetworkState,
    end_time::Time,
    dt::Time,
    travel_times::Dict{Tuple{AirportCode,AirportCode},Time},
    travel_time_variation::Time,
    measurement_variation::Time,
)

    # Simulate the network state in increments of dt
    for t in 0:dt:end_time
        # All parked aircraft that are ready to turnaround do so
        for (_, airport) in state.airports
            update_available_aircraft!(airport, t)
        end

        # All aircraft that are ready to depart move to the runway of the
        # origin airport
        ready_to_depart = pop_ready_to_depart_flights!(state, t)
        for (flight, ready_t) in ready_to_depart
            queue_entry = QueueEntry(flight, ready_t)
            @debug "Flight $(flight_code(flight)) ready to depart at $(ready_t)" *
                   " (was scheduled for $(flight.scheduled_departure_time))"
            push!(state.airports[flight.origin].runway_queue, queue_entry)
        end

        # All flights that are in transit get moved to the runway queue at their
        # destination airport
        update_in_transit_flights!(state, t)

        # All aircraft in the runway queues get serviced
        for (_, airport) in state.airports
            departing_flights, landing_flights = {*} ~ update_runway_queue!(airport, t)
            {*} ~ add_in_transit_flights!(state, departing_flights, travel_times, travel_time_variation)
            add_completed_flights!(state, landing_flights)
        end
    end

    # Link simulated and actual arrival/departure times for all flights
    # by sampling with an observation of the actual time
    for flight in state.completed_flights
        measured_departure_time = {(flight_code(flight), :actual_departure_time)} ~ normal(
            flight.simulated_departure_time,
            measurement_variation
        )
        measured_arrival_time = {(flight_code(flight), :actual_arrival_time)} ~ normal(
            flight.simulated_arrival_time,
            measurement_variation
        )

        @debug "Flight $(flight_code(flight)) measured departure time $(measured_departure_time)" *
               " (actual departure time: $(flight.actual_departure_time))"
        @debug "Flight $(flight_code(flight)) measured arrival time $(measured_arrival_time)" *
               " (actual arrival time: $(flight.actual_arrival_time))"
    end

    # For any flights that are not yet completed, sample their actual arrival
    # and departure times around the maximum simulation time
    for (flight, _) in state.in_transit_flights
        measured_departure_time = {(flight_code(flight), :actual_departure_time)} ~ normal(
            end_time,
            measurement_variation
        )
        measured_arrival_time = {(flight_code(flight), :actual_arrival_time)} ~ normal(
            end_time,
            measurement_variation
        )

        @debug "Flight $(flight_code(flight)) measured departure time $(measured_departure_time)" *
               " (actual departure time: $(flight.actual_departure_time))"
        @debug "Flight $(flight_code(flight)) measured arrival time $(measured_arrival_time)" *
               " (actual arrival time: $(flight.actual_arrival_time))"
    end

    for airport in values(state.airports)
        for queue_entry in airport.runway_queue
            flight = queue_entry.flight
            measured_departure_time = {(flight_code(flight), :actual_departure_time)} ~ normal(
                end_time,
                measurement_variation
            )
            measured_arrival_time = {(flight_code(flight), :actual_arrival_time)} ~ normal(
                end_time,
                measurement_variation
            )

            @debug "Flight $(flight_code(flight)) measured departure time $(measured_departure_time)" *
                   " (actual departure time: $(flight.actual_departure_time))"
            @debug "Flight $(flight_code(flight)) measured arrival time $(measured_arrival_time)" *
                   " (actual arrival time: $(flight.actual_arrival_time))"
        end
    end
end

"""
    simulate(
        states::Vector{NetworkState},
        end_time::Time,
        dt::Time,
        travel_time_variation::Float64 = 0.05,
        turnaround_time_variation::Float64 = 0.05,
        measurement_variation::Time = 0.1,
    )

Simulate the network states in `states` until `end_time`, in increments of `dt`.

# Arguments
- `states::Vector{NetworkState}`: The initial network states.
- `end_time::Time`: The time at which to stop the simulation.
- `dt::Time`: The time increment for the simulation.
- `travel_time_variation::Float64`: The fractional variation of travel time.
- `turnaround_time_variation::Float64`: The fractional variation of turnaround time.
- `measurement_variation::Time`: The standard deviation of measurement noise.
"""
@gen function simulate(
    states::Vector{NetworkState},
    end_time::Time,
    dt::Time,
)
    # Make a deep copy of the states to avoid modifying the original
    states = deepcopy(states)

    # Sample system-level latent variables
    measurement_variation = {:measurement_variation} ~ exponential(1 / 0.2)
    travel_time_variation = {:travel_time_variation} ~ exponential(1 / 0.2)
    turnaround_time_variation = {:turnaround_time_variation} ~ exponential(1 / 0.2)

    # Sample latent variables for the airports
    # (assume that all states have the same airports)
    airport_codes = keys(states[1].airports)
    airport_mean_turnaround_times = Dict{AirportCode,Time}()
    airport_mean_service_times = Dict{AirportCode,Time}()
    travel_times = Dict{Tuple{AirportCode,AirportCode},Time}()
    for code in airport_codes
        # Sample turnaround and service times for each airport
        airport_mean_turnaround_times[code] = {(code, :turnaround_time)} ~ exponential(1 / 1.0)
        airport_mean_service_times[code] = {(code, :service_time)} ~ exponential(1 / 0.2)

        # Sample travel times between airports
        for origin in airport_codes
            if origin != code
                travel_times[(origin, code)] = {(origin, code, :travel_time)} ~ uniform(0.0, 10.0)
            end
        end
    end

    # Simulate each network state
    for (i, state) in enumerate(states)
        # Assign latent variables to the airports
        for airport in values(state.airports)
            airport.mean_turnaround_time = airport_mean_turnaround_times[airport.code]
            airport.turnaround_time_std_dev = turnaround_time_variation *
                                              airport.mean_turnaround_time
            airport.mean_service_time = airport_mean_service_times[airport.code]
        end

        {(:day, i)} ~ simulate_day(state, end_time, dt, travel_times, travel_time_variation, measurement_variation)
    end

    return states
end

end # module BayesAir
