package com.local.flink.trainning.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.local.flink.trainning.datatypes.TaxiRide;
import com.local.flink.trainning.sources.TaxiRideSource;
import com.local.flink.trainning.utils.GeoUtils;

public class RideCleansingExercise {
	public static void main(String[] args) throws Exception {
		
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		int maxEventDelaySecs = 1;
		int servingSpeedFactor = 60;
		String dataFilePath = "E:\\work_log\\2018-08\\nycTaxiRides.gz";
		
		DataStream<TaxiRide> stream = environment.addSource(new TaxiRideSource(
							dataFilePath, 
							maxEventDelaySecs, 
							servingSpeedFactor))
						.filter(new NYCFilterFunction());
		stream.print();
		
		environment.execute("filter ride in NYC");
	}
	
	public static final class NYCFilterFunction implements FilterFunction<TaxiRide> {

		@Override
		public boolean filter(TaxiRide ride) throws Exception {
			return GeoUtils.isInNYC(ride.startLon, ride.startLat)
					&& GeoUtils.isInNYC(ride.endLon, ride.endLat);
		}
	}
}
