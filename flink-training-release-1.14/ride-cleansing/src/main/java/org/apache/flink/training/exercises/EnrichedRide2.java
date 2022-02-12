package org.apache.flink.training.exercises;

import org.apache.flink.training.exercises.common.datatypes.TaxiRide;
import org.apache.flink.training.exercises.common.utils.GeoUtils;

public class EnrichedRide2 extends TaxiRide {

    public int startCell;
    public int endCell;

    // todo
    public long startTime;
    public long endTime;

    public EnrichedRide2() {}

    public EnrichedRide2(TaxiRide ride) {
        this.rideId = ride.rideId;
        this.isStart = ride.isStart;
        this.eventTime = ride.eventTime;
        this.startLon = ride.startLon;
        this.startLat = ride.startLat;
        this.endLon = ride.endLon;
        this.endLat = ride.endLat;
        this.passengerCnt = ride.passengerCnt;
        this.taxiId = ride.taxiId;
        this.driverId = ride.driverId;

        // 转换
        this.startCell = GeoUtils.mapToGridCell(ride.startLon, ride.startLat);
        this.endCell = GeoUtils.mapToGridCell(ride.endLon, ride.endLat);
    }

    @Override
    public String toString() {
        return super.toString() + "," +
                this.startCell + "," +
                this.endCell;
    }
}
