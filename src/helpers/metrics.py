import haversine
import numpy as np

from helpers.utility import project_traectory


def SED_trips(original_trips, compressed_trips, precision):
    def assess_single_trajectory(original, compressed, delta):
        cum_distance, nmbr_instants = 0, 0
        compressed_start = min([x.timestamp() for x in compressed.instants()])
        compressed_end = max([x.timestamp() for x in compressed.instants()])
        time = compressed_start + delta

        while time < compressed_end:
            point = original.value_at_timestamp(time)
            point_compressed = compressed.value_at_timestamp(time)
            distance = (
                haversine.haversine(
                    (point.y, point.x), (point_compressed.y, point_compressed.x)
                )
                * 1000
            )

            cum_distance += distance
            nmbr_instants += 1
            time += delta

        return cum_distance, nmbr_instants

    # print("o-c:", len(original_trips), "-", len(compressed_trips), end=": ")

    cum_dist, nmbr_instants = 0, 0

    # for id, row in original_trips.iterrows():
    for id in original_trips.index:
        if id not in compressed_trips.index:
            # print(id)
            continue

        cum_dist_trip, nmbr_instants_trip = assess_single_trajectory(
            original=original_trips.loc[id].trajectory,
            compressed=compressed_trips.loc[id].trajectory,
            delta=precision,
        )
        cum_dist += cum_dist_trip
        nmbr_instants += nmbr_instants_trip

    return cum_dist / nmbr_instants


def length_loss_rate(original_trips, compressed_trips):
    original_length = original_trips["trajectory"].apply(lambda x: x.length()).sum()
    compressed_length = compressed_trips["trajectory"].apply(lambda x: x.length()).sum()
    return (original_length- compressed_length) / original_length

def synchronized_speed_difference(original_trips, compressed_trips, precision, projection):
    """Result in m/s"""
    def assess_speed_single_trajectory(original, compressed, delta, proj):
        original = project_traectory(original, proj)
        compressed = project_traectory(compressed, proj)
        cum_speed_error, nmbr_instants = 0, 0
        compressed_start = min([x.timestamp() for x in compressed.instants()])
        compressed_end = max([x.timestamp() for x in compressed.instants()])
        original_end = max([x.timestamp() for x in original.instants()])
        i_max = (compressed_end - compressed_start)//delta
        timestamps = [compressed_start + i*delta for i in range(1, i_max)]
        original_length = original.cumulative_length()
        compressed_length = compressed.cumulative_length()



        for i in range(len(timestamps) - 1):
            # print(delta)
            # print(timestamps[i+1], timestamps[i], original_end)
            # print("original_lenght i+1", original_length.value_at_timestamp(timestamps[i+1]) )
            # print("original_lenght i", original_length.value_at_timestamp(timestamps[i]) )
            length_o = original_length.value_at_timestamp(timestamps[i+1]) - original_length.value_at_timestamp(timestamps[i])
            length_c = compressed_length.value_at_timestamp(timestamps[i+1]) - compressed_length.value_at_timestamp(timestamps[i])

            cum_speed_error += np.abs(length_o - length_c) / delta.total_seconds()
            nmbr_instants += 1

        return cum_speed_error, nmbr_instants

    cum_speed_error, nmbr_instants = 0, 0

    # for id, row in original_trips.iterrows():
    for id in original_trips.index:
        if id not in compressed_trips.index:
            continue

        cum_speed_error_trip, nmbr_instants_trip = assess_speed_single_trajectory(
            original=original_trips.loc[id].trajectory,
            compressed=compressed_trips.loc[id].trajectory,
            delta=precision,
            proj=projection
        )
        cum_speed_error += cum_speed_error_trip
        nmbr_instants += nmbr_instants_trip

    return cum_speed_error / nmbr_instants

