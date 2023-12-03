# Use Gen for probabilistic programming
using Gen

"""
    QueueEntry

An entry in a runway queue.

# Attributes
- `flight::Flight`: The flight associated with this queue entry.
- `queue_start_time::Time`: The time at which the flight entered the queue.
- `total_wait_time::Time`: The total duration the flight has spent in the queue.
- `assigned_service_time::Union{Time, Nothing}`: The time at which the flight will be serviced (optional).
"""
mutable struct QueueEntry
    flight::Flight
    queue_start_time::Time
    total_wait_time::Time
    assigned_service_time::Union{Time,Nothing}
end

function QueueEntry(flight::Flight, queue_start_time::Time)
    return QueueEntry(flight, queue_start_time, 0.0, nothing)
end

"""
    Airport

Represents a single airport in the network.

# Attributes
- `code::AirportCode`: The airport code.
- `mean_service_time::Time`: The mean service time for departing and arriving aircraft.
- `mean_turnaround_time::Time`: The nominal turnaround time for aircraft landing at
    this airport.
- `turnaround_time_std_dev::Time`: The standard deviation of the turnaround time for
    aircraft landing at this airport.
- `runway_queue::Vector{QueueEntry}`: The queue of aircraft waiting to take off or land.
- `turnaround_queue::Vector{Time}`: The queue of aircraft waiting to be refueled etc....
    Each entry in this queue represents a time at which the aircraft will be
    ready for its next departure.
- `available_aircraft::Vector{Time}`: A list of times at which aircraft became available
    (after turnaround).
- `available_crew::Vector{Time}`: A list of times at which crew become available (after
    turnaround). Treats crew for 1 aircraft as a single unit.
"""
mutable struct Airport
    code::AirportCode
    mean_service_time::Time
    mean_turnaround_time::Time
    turnaround_time_std_dev::Time
    runway_queue::Vector{QueueEntry}
    turnaround_queue::Vector{Time}
    available_aircraft::Vector{Time}
    available_crew::Vector{Time}
end

"""
    Airport(code::AirportCode)

Create an airport with the given code.

# Arguments
- `code::AirportCode`: The airport code.

# Returns
- `a::Airport`: The airport.
"""
function Airport(code::AirportCode)
    return Airport(
        code,
        30.0,
        60.0,
        10.0,
        [],
        [],
        [],
        [],
    )
end

function num_available_aircraft(a::Airport)
    return length(a.available_aircraft)
end

function num_available_crew(a::Airport)
    return length(a.available_crew)
end

"""
    update_available_aircraft!(a::Airport, t::Time)
    
Update the list of available aircraft at the airport `a` at time `t` by
checking the turnaround queue.

# Arguments
- `a::Airport`: The airport.
- `t::Time`: The current time.

# Return value
- `nothing`
"""
function update_available_aircraft!(a::Airport, t::Time)
    new_turnaround_queue = Vector{Time}()
    for turnaround_time in a.turnaround_queue
        if turnaround_time <= t
            # The aircraft/crew is ready to depart, so make it available
            push!(a.available_aircraft, turnaround_time)
            push!(a.available_crew, turnaround_time)
        else
            # The aircraft/crew is not ready to depart, so keep it in the queue
            push!(new_turnaround_queue, turnaround_time)
        end
    end

    # Update the turnaround queue
    a.turnaround_queue = new_turnaround_queue
end

"""
    update_runway_queue!(a::Airport, t::Time)

Update the runway queue at the airport `a` at time `t` by removing flights that
have been served.

This is a generative function, since it makes random choices about how long
each flight will take to service.

# Arguments
- `a::Airport`: The airport.
- `t::Time`: The current time.

# Return value
- A Vector of flights that have been taken off.
- A vector of flights that have landed.
"""
@gen function update_runway_queue!(a::Airport, t::Time)
    departing_flights = Vector{Flight}()
    landing_flights = Vector{Flight}()

    # While the flight at the front of the queue is ready to be served, service it
    while length(a.runway_queue) > 0 &&
        (isnothing(a.runway_queue[1].assigned_service_time)
         ||
         a.runway_queue[1].assigned_service_time <= t)

        # If no service time has been assigned, assign one now
        if isnothing(a.runway_queue[1].assigned_service_time)
            # Sample the service time
            {*} ~ assign_service_time!(a, a.runway_queue[1], t)
        end

        # If the service time has elapsed, the flight takes off or lands
        if a.runway_queue[1].assigned_service_time <= t
            # Remove the flight from the queue
            queue_entry = popfirst!(a.runway_queue)

            # Assign a departure or arrival time
            if queue_entry.flight.origin == a.code
                assign_departure_time!(a, queue_entry)
                push!(departing_flights, queue_entry.flight)
            else
                assign_arrival_time!(a, queue_entry)
                {*} ~ assign_turnaround_time(a, queue_entry.flight)
                push!(landing_flights, queue_entry.flight)
            end
        end
    end

    return departing_flights, landing_flights
end

"""
    assign_service_time!(a::Airport, queue_entry::QueueEntry, t::Time)

Assign a service time to a flight in the runway queue at the airport `a` at time `t`.

This is a generative function, since it makes random choices about how long
the flight will take to service.

# Arguments
- `a::Airport`: The airport.
- `queue_entry::QueueEntry`: The queue entry to assign a service time to.
- `t::Time`: The current time.
"""
@gen function assign_service_time!(a::Airport, queue_entry::QueueEntry, t::Time)
    # Determine whether the flight is arriving or departing
    departing = queue_entry.flight.origin == a.code

    # Sample a service time
    key = departing ? :departure_service_time : :arrival_service_time
    # service_time = {(flight_code(queue_entry.flight), key)} ~ exponential(1 / a.mean_service_time)
    service_time = a.mean_service_time  # TODO is deterministic model enough?

    # Update the queue waiting times for all other aircraft
    for i in 1:length(a.runway_queue)
        a.runway_queue[i].total_wait_time += service_time
    end

    # Update the time at which this aircraft leaves the queue
    queue_entry.assigned_service_time = queue_entry.queue_start_time +
                                        queue_entry.total_wait_time

    @debug "Flight $(flight_code(queue_entry.flight)) entered $(departing ? :departure : :arrival) queue" *
           " at $(queue_entry.queue_start_time)," *
           " assigned service time $(queue_entry.assigned_service_time)"
end


"""
    assign_departure_time!(a::Airport, queue_entry::QueueEntry)

Assign a departure time to a flight in the runway queue at the airport `a` at time `t`.

# Arguments
- `a::Airport`: The airport.
- `queue_entry::QueueEntry`: The queue entry to assign a departure time to.
"""
function assign_departure_time!(a::Airport, queue_entry::QueueEntry)
    queue_entry.flight.simulated_departure_time = queue_entry.assigned_service_time
    @debug "Flight $(flight_code(queue_entry.flight)) departing at $(queue_entry.flight.simulated_departure_time)" *
           " (scheduled departure time: $(queue_entry.flight.scheduled_departure_time))"
end

"""
    assign_arrival_time!(a::Airport, queue_entry::QueueEntry)

Assign a arrival time to a flight in the runway queue at the airport `a` at time `t`.

# Arguments
- `a::Airport`: The airport.
- `queue_entry::QueueEntry`: The queue entry to assign a arrival time to.
"""
function assign_arrival_time!(a::Airport, queue_entry::QueueEntry)
    queue_entry.flight.simulated_arrival_time = queue_entry.assigned_service_time
    @debug "Flight $(flight_code(queue_entry.flight)) arriving at $(queue_entry.flight.simulated_arrival_time)" *
           " (scheduled arrival time: $(queue_entry.flight.scheduled_arrival_time))"
end

"""
    assign_turnaround_time(a::Airport, f::Flight)

Assign a turnaround time for an arrived aircraft.

This is a generative function, since it makes random choices about how long
the flight will take to service.

# Arguments
- `a::Airport`: The airport.
- `f::Flight`: The flight.
"""
@gen function assign_turnaround_time(a::Airport, flight::Flight)
    # Sample a turnaround time
    # turnaround_time = {(flight_code(flight), :turnaround_time)} ~ normal(a.mean_turnaround_time, a.turnaround_time_std_dev)
    turnaround_time = a.mean_turnaround_time  # TODO is deterministic model enough?

    # Add the turnaround time to the turnaround queue
    push!(a.turnaround_queue, flight.simulated_arrival_time + turnaround_time)
end