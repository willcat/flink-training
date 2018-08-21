package com.local.flink.trainning.streaming;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.util.Collector;

import com.local.flink.trainning.datatypes.TaxiFare;
import com.local.flink.trainning.datatypes.TaxiRide;
import com.local.flink.trainning.sources.TaxiFareSource;
import com.local.flink.trainning.sources.TaxiRideSource;

public class RidesAndFaresExercise {

	public static void main(String[] args) throws Exception {
		
		ParameterTool params = ParameterTool.fromArgs(args);
		final String rideDataFilePath = params.get("rides", "E:\\work_log\\2018-08\\nycTaxiRides.gz");
		final String fairDataFilePath = params.get("fares", "E:\\work_log\\2018-08\\nycTaxiFares.gz");
		
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		environment.setParallelism(4);
		
		final int maxEventDelaySecs = 60;
		final int servingSpeedFactor = 1800;
		
		DataStream<TaxiRide> rides = environment.addSource(new TaxiRideSource(
										rideDataFilePath,
										maxEventDelaySecs, 
										servingSpeedFactor))
									.filter(new FilterFunction<TaxiRide>() {
										
										@Override
										public boolean filter(TaxiRide taxiRide) throws Exception {
											return taxiRide.isStart;
										}
									})
									.keyBy("rideId");
		
		DataStream<TaxiFare> fares = environment.addSource(new TaxiFareSource(
												fairDataFilePath, 
												maxEventDelaySecs, 
												servingSpeedFactor))
									 .keyBy("rideId");
		
		DataStream<Tuple2<TaxiRide, TaxiFare>> enrichedRides = rides
									.connect(fares)
									.flatMap(new RidesAndFaresMap());
		
		
		enrichedRides.print();
		environment.execute("running");
	}
	
	public static final class RidesAndFaresMap extends RichCoFlatMapFunction<TaxiRide, TaxiFare, Tuple2<TaxiRide, TaxiFare>> {
		
		// keyed, managed state
		transient private ValueState<TaxiRide> rideState;
		transient private ValueState<TaxiFare> fareState;
		
		@Override
		public void open(Configuration parameters) throws Exception {
			rideState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved ride", TaxiRide.class));
			fareState = getRuntimeContext().getState(new ValueStateDescriptor<>("saved fare", TaxiFare.class));
		}

		@Override
		public void flatMap1(TaxiRide taxiRide, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiFare taxiFare = fareState.value();
			
			if (taxiFare != null) {
				fareState.clear();
				out.collect(new Tuple2<>());
			} else {
				rideState.update(taxiRide);
			}
		}

		@Override
		public void flatMap2(TaxiFare taxiFare, Collector<Tuple2<TaxiRide, TaxiFare>> out) throws Exception {
			TaxiRide taxiRide = rideState.value();
			
			if (taxiRide != null) {
				rideState.clear();
				out.collect(new Tuple2<>(taxiRide, taxiFare));
			} else {
				fareState.update(taxiFare);
			}
		}
	}

}
