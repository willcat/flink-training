package com.local.flink.trainning.streaming;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import com.local.flink.trainning.datatypes.TaxiFare;
import com.local.flink.trainning.datatypes.TaxiRide;
import com.local.flink.trainning.sources.CheckpointedTaxiFareSource;
import com.local.flink.trainning.sources.CheckpointedTaxiRideSource;
public class ExpiringStateExercise {
	
	static final OutputTag<TaxiRide> unmatchedRides = new OutputTag<TaxiRide>("unmatchedRides") {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;};
	static final OutputTag<TaxiFare> unmatchedFares = new OutputTag<TaxiFare>("unmatchedFares") {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;};
	
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String rideDataFilePath = params.get("rides", "E:\\work_log\\2018-08\\nycTaxiRides.gz");
		///tmp/flink_exercise/nycTaxiFares.gz
		final String fareDataFilePath = params.get("fares", "E:\\work_log\\2018-08\\nycTaxiFares.gz");
		
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
	
		final int servingSpeedFactor = 600;
		
		
		//removing end event and simulation missing data
		DataStream<TaxiRide> rides = environment.addSource(new CheckpointedTaxiRideSource(
				rideDataFilePath, 
				servingSpeedFactor))
				.filter((TaxiRide ride) -> (ride.isStart && (ride.rideId % 1000 != 0)))
				.keyBy((TaxiRide ride) -> ride.rideId);
		
		DataStream<TaxiFare> fares = environment.addSource(new CheckpointedTaxiFareSource(
				fareDataFilePath,
				servingSpeedFactor))
				.keyBy((TaxiFare fare) -> fare.rideId);
		
		//must change to SingleOutputStreamOperator(DataStream's sub) not DataStream
		SingleOutputStreamOperator<Tuple2<TaxiRide, TaxiFare>>  processed = rides.connect(fares)
											.process(new EnrichProcessFunction());
		
		processed.getSideOutput(unmatchedFares);
		
		environment.execute("Expiring State");
	}
	
	public static class EnrichProcessFunction extends CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;
		private  ValueState<TaxiRide> rideState;
		private  ValueState<TaxiFare> fareState;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void processElement1(TaxiRide ride,
				CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>>.Context context,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			 TaxiFare fare = fareState.value();
			 if (fare != null) {
				 fareState.clear();
				 out.collect(new Tuple2<TaxiRide, TaxiFare>(ride, fare));
			 } else {
				 rideState.update(ride);
				 //when the watermark arrives, the event is expired, so trigger the onTimer
				 context.timerService().registerEventTimeTimer(ride.getEventTime());
			 }
		}

		@Override
		public void processElement2(TaxiFare fare,
				CoProcessFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>>.Context context,
				Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide ride = rideState.value();
			if (ride != null) {
				rideState.clear();
				out.collect(new Tuple2<TaxiRide, TaxiFare>(ride, fare));
			} else {
				fareState.update(fare);
				context.timerService().registerEventTimeTimer(fare.getEventTime());
			}
			
		}
		
		@Override
		public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			if (rideState.value() != null) {
				ctx.output(unmatchedRides, rideState.value());
				rideState.clear();
			}
			if (fareState.value() != null) {
				ctx.output(unmatchedFares, fareState.value());
				fareState.clear();
			}
		}
	}

}
