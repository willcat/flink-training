package com.local.flink.trainning.streaming;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import com.local.flink.trainning.datatypes.TaxiFare;
import com.local.flink.trainning.sources.TaxiFareSource;

/**
 * Java reference implementation for the "Hourly Tips" exercise of the Flink training
 * 
 * The task of the exercise is to first calculate the total tips collected by each driver, hour by hour, and
 * then from that stream, find the highest tip total in each hour.
 * 
 * 
 * @see <a href="https://ci.apache.org/projects/flink/flink-docs-release-1.6/dev/stream/operators/windows.html#processwindowfunction-with-incremental-aggregation">ProcessWindowFunction with Incremental Aggregation</a>
 * @author Administrator
 *
 */
public class HourlyTipsExercise {
	public static void main(String[] args) throws Exception {
		ParameterTool params = ParameterTool.fromArgs(args);
		final String fareDataFilePath = params.get("fares", "E:\\work_log\\2018-08\\nycTaxiFares.gz");
		final String saveFile = params.get("save","/tmp/flink_exercise/hourlytips");
		
		StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
		environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		
		final int maxEventDelaySecs = 60;
		final int servingSpeedFactor = 600;
		
		DataStream<TaxiFare> fares = environment.addSource(new TaxiFareSource(fareDataFilePath, maxEventDelaySecs, servingSpeedFactor));
		
		DataStream<Tuple3<Long, Long, Float>> hourlyTips = fares
					                //use index or string will lead to error,
					                //.keyBy("tip")
					                .keyBy((TaxiFare taxiFare) -> taxiFare.driverId)
					                .timeWindow(Time.minutes(1))
					                .aggregate(new AddTips(), new WrapWithWindowInfo());

		//cascade windows,must have a duration that is multiple of the first window size
		DataStream<Tuple3<Long, Long, Float>> hourlyMax = hourlyTips
						            .timeWindowAll(Time.minutes(1))
						            .maxBy(2);
		
		hourlyMax.writeAsText(saveFile);
		
		environment.execute("Hourly tips");
	}
	
	public static class AddTips implements AggregateFunction<TaxiFare, Float, Float> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public Float add(TaxiFare taxiFare, Float acc) {
			return taxiFare.tip + acc;
		}

		@Override
		public Float createAccumulator() {
			return 0F;
		}

		@Override
		public Float getResult(Float acc) {
			return acc;
		}

		@Override
		public Float merge(Float acc1, Float acc2) {
			return acc1 + acc2;
		}
		
	}
	
	public static class WrapWithWindowInfo extends ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow> {

		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		@Override
		public void process(Long arg0,
				ProcessWindowFunction<Float, Tuple3<Long, Long, Float>, Long, TimeWindow>.Context arg1,
				Iterable<Float> arg2, Collector<Tuple3<Long, Long, Float>> arg3) throws Exception {
			// TODO Auto-generated method stub
			
		}
		
	}
}
