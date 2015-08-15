/**
 Copyright 2015 Miyuru Dayarathna

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.linear.storm.accident;

import java.util.LinkedList;
import java.util.Map;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;

import org.linear.storm.events.AccidentEvent;
import org.linear.storm.events.PositionReportEvent;
import org.linear.storm.util.Constants;
import org.linear.storm.accident.Car;

public class AccidentDetectionBolt  extends BaseRichBolt {
    OutputCollector _collector;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//      _collector.ack(tuple);
    	   	
//        PositionReportEvent posEvt = new PositionReportEvent();
//        posEvt.time =  tuple.getLong(0);
//        posEvt.vid = tuple.getInteger(1);
//        posEvt.speed = tuple.getByte(2);
//        posEvt.xWay = tuple.getByte(3);
//        posEvt.mile = tuple.getByte(4);
//        posEvt.offset = tuple.getShort(5);
//        posEvt.lane = tuple.getByte(6);
//        posEvt.dir = tuple.getByte(7);
        
		Car c = new Car(tuple.getLong(0),
						tuple.getInteger(1),
						tuple.getByte(2),
						tuple.getByte(3),
						tuple.getByte(6),
						tuple.getByte(4),
						tuple.getByte(5));

		detect(c);
		_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	//We declare the streams that we are emitting from Accident Detect bolt.
    	//nov_tuples
    	declarer.declareStream("accident_event", new Fields("vid1",
    														"vid2",
    		  											  	"xway",
    		  											  	"mile",
    		  											  	"dir"));
    }
    
    
    public void detect(Car c) {	
		if (c.speed > 0) {
			remove_from_smashed_cars(c);
			remove_from_stopped_cars(c);
		} else if (c.speed == 0) {
			if (is_smashed_car(c) == false) {
				if (is_stopped_car(c) == true) {
					renew_stopped_car(c);
				} else {
					stopped_cars.add(c);
				}
				
				int flag = 0;
				for (int i = 0; i < stopped_cars.size() -1; i++) {
					Car t_car = (Car)stopped_cars.get(i);
					if ((t_car.carid != c.carid)&&(!t_car.notified)&&((c.time - t_car.time) <= 120) && (t_car.posReportID >= 3) &&
							((c.xway0 == t_car.xway0 && c.mile0 == t_car.mile0 && c.lane0 == t_car.lane0 && c.offset0 == t_car.offset0 && c.dir0 == t_car.dir0) &&
							(c.xway1 == t_car.xway1 && c.mile1 == t_car.mile1 && c.lane1 == t_car.lane1 && c.offset1 == t_car.offset1 && c.dir1 == t_car.dir1) &&
							(c.xway2 == t_car.xway2 && c.mile2 == t_car.mile2 && c.lane2 == t_car.lane2 && c.offset2 == t_car.offset2 && c.dir2 == t_car.dir2) &&
							(c.xway3 == t_car.xway3 && c.mile3 == t_car.mile3 && c.lane3 == t_car.lane3 && c.offset3 == t_car.offset3 && c.dir3 == t_car.dir3))) {
						
						if (flag == 0) {
							AccidentEvent a_event = new AccidentEvent();
							a_event.vid1 = c.carid;
							a_event.vid2 = t_car.carid;
							a_event.xway = c.xway0;
							a_event.mile = c.mile0;
							a_event.dir = c.dir0;
							a_event.time = t_car.time;

							_collector.emit("accident_event", new Values(Constants.ACCIDENT_EVENT_TYPE, a_event.vid1, a_event.vid2, a_event.xway, a_event.mile, a_event.dir));
							
							t_car.notified = true;
							c.notified = true;
							flag = 1;
						}
						//The cars c and t_car have smashed with each other
						add_smashed_cars(c);
						add_smashed_cars(t_car);
						
						break;
					}
				}
			}
		}
	}
    
	public static LinkedList smashed_cars = new LinkedList();
	public static LinkedList stopped_cars = new LinkedList();
	public static LinkedList accidents = new LinkedList();
	
	public boolean is_smashed_car(Car car) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);

			if (((Car)smashed_cars.get(i)).carid == car.carid){
				return true;
			}
		}
		return false;
	}
	
	public void add_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove(i);
			}
		}
		smashed_cars.add(c);
	}
	
	public boolean is_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				return true;
			}
		}
		return false;
	}
	
	public void remove_from_smashed_cars(Car c) {
		for (int i = 0; i < smashed_cars.size(); i++) {
			Car t_car = (Car)smashed_cars.get(i);
			if (c.carid == t_car.carid) {
				smashed_cars.remove();
			}
		}
	}
	
	public void remove_from_stopped_cars(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				stopped_cars.remove();
			}
		}
	}
	
	public void renew_stopped_car(Car c) {
		for (int i = 0; i < stopped_cars.size(); i++) {
			Car t_car = (Car)stopped_cars.get(i);
			if (c.carid == t_car.carid) {
				c.xway3 = t_car.xway2;
				c.xway2 = t_car.xway1;
				c.xway1 = t_car.xway0;
				c.mile3 = t_car.mile2;
				c.mile2 = t_car.mile1;
				c.mile1 = t_car.mile0;				
				c.lane3 = t_car.lane2;
				c.lane2 = t_car.lane1;
				c.lane1 = t_car.lane0;
				c.offset3 = t_car.offset2;
				c.offset2 = t_car.offset1;
				c.offset1 = t_car.offset0;				
				c.dir3 = t_car.dir2;
				c.dir2 = t_car.dir1;
				c.dir1 = t_car.dir0;
				c.notified = t_car.notified;
				c.posReportID = (byte)(t_car.posReportID + 1);
				
				stopped_cars.remove(i);
				stopped_cars.add(c);
				
				//Since we already found the car from the list we break at here
				break;
			}
		}
	}    
}
