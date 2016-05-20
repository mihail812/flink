package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class WatermarkSync {
	private static WatermarkSync INSTANCE;
	Logger LOG = LoggerFactory.getLogger(WatermarkSync.class.getName());

	synchronized public static WatermarkSync getInstance() {
		if (INSTANCE==null) {
			INSTANCE=new WatermarkSync();
		}
		return INSTANCE;
	}
	private int counter =0;
	private ArrayList<Watermark> watermarks = new ArrayList<Watermark>();

	synchronized public int getId() {
		Integer result =counter++;
		watermarks.add(new Watermark(0));
		return result;
	}

	private Watermark getMinWm() {
		Watermark maxWm = Watermark.MAX_WATERMARK;
		for (Watermark wm: watermarks){
			if (maxWm.getTimestamp() > wm.getTimestamp()){
				maxWm = wm;
			}
		}
		return maxWm;
	}

	synchronized public long emitWatermark(int id, Watermark wm) {
		if (wm!=null) {
			watermarks.set(id, wm);
		}
		Watermark min = getMinWm();
		Watermark our = watermarks.get(id);
		long res =Math.min((our.getTimestamp() - min.getTimestamp()) / 100, 1000);
		if (res >0) {
			LOG.info(new StringBuilder("Suspending ")
				.append(id)
				.append(":")
				.append(res)
				.append(" at ")
				.append(our.getTimestamp())
				.append(" min ")
				.append(min.getTimestamp())
				.toString() );
		}
		return res;
	}
}
