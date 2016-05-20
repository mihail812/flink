package org.apache.flink.streaming.connectors.kafka.internals;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.ArrayList;

public class WatermarkSync {
	public static final WatermarkSync INSTANCE = new WatermarkSync();
	private int counter =0;
	private ArrayList<Watermark> watermarks;

	synchronized public int getId() {
		Integer result =counter++;
		watermarks.add(new Watermark(0));
		return result;
	}

	private Watermark getMinWm() {
		Watermark maxWm = new Watermark(0);
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
		return (our.getTimestamp() - min.getTimestamp()) / 100;
	}
}
