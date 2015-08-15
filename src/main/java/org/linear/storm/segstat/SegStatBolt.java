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

package org.linear.storm.segstat;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import org.linear.storm.util.Constants;
import org.linear.storm.events.PositionReportEvent;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;

public class SegStatBolt  extends BaseRichBolt {
    OutputCollector _collector;
	private long currentSecond;
	private LinkedList<PositionReportEvent> posEvtList;
	private ArrayList<PositionReportEvent> evtListNOV;
	private ArrayList<PositionReportEvent> evtListLAV;
	private byte minuteCounter;
	private int lavWindow = 5; //This is LAV window in minutes

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      
      currentSecond = -1;
      posEvtList = new LinkedList<PositionReportEvent>();
      evtListNOV = new ArrayList<PositionReportEvent>();
      evtListLAV = new ArrayList<PositionReportEvent>();
    }

    @Override
    public void execute(Tuple tuple) {
    //_collector.emit("position_report", new Values(time, vid, spd, xway, lane, dir, mile, ofst));
      PositionReportEvent posEvt = new PositionReportEvent();
      posEvt.time =  tuple.getLong(0);
      posEvt.vid = tuple.getInteger(1);
      posEvt.speed = tuple.getByte(2);
      posEvt.xWay = tuple.getByte(3);
      posEvt.lane = tuple.getByte(4);
      posEvt.dir = tuple.getByte(5);
      posEvt.mile = tuple.getByte(6);
      posEvt.offset = tuple.getShort(7);
    
      process(posEvt);
      _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {   	
    	//NOV Events
    	declarer.declareStream("nov_event", new Fields(	"type",
    													"minute",	//((int)Math.floor(currentSecond/60))
    													"segment",	//mile
    													"nov"));	//numVehicles
    	//LAV Events
    	declarer.declareStream("lav_event", new Fields(	"type",
    													"segment",	//mile
														"lav",		//lav
														"dir"));	//i
    }
    
    public void process(PositionReportEvent evt){       	
		if(currentSecond == -1){
			currentSecond = evt.time;
		}else{
			if((evt.time - currentSecond) > 60){
				calculateNOV();
				
				evtListNOV.clear();
				
				currentSecond = evt.time;
				minuteCounter++;
				
				if(minuteCounter >= lavWindow){
					calculateLAV(currentSecond);
						
					//LAV list cannot be cleared because it need to keep the data for 5 minutes prior to any time t
					//evtListLAV.clear();
					
					minuteCounter = 0;
				}
			}
		}
		evtListNOV.add(evt);
		evtListLAV.add(evt);
    }

	private void calculateLAV(long currentTime) {
		float result = -1;
		float avgVelocity = -1;
		
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  

		//First identify the number of segments
		Iterator<PositionReportEvent> itr = evtListLAV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		ArrayList<PositionReportEvent> tmpEvtListLAV = new ArrayList<PositionReportEvent>(); 
		float lav = -1;
		
		for(byte i =0; i < 2; i++){ //We need to do this calculation for both directions (west = 0; East = 1)
			Iterator<Byte> segItr = segList.iterator();
			int vid = -1;
			byte mile = -1;
			ArrayList<Integer> tempList = null;
			PositionReportEvent evt = null;
			long totalSegmentVelocity = 0;
			long totalSegmentVehicles = 0;
			
			//We calculate LAV per segment
			while(segItr.hasNext()){
				mile = segItr.next();
				itr = evtListLAV.iterator();
				
				while(itr.hasNext()){
					evt = itr.next();
					
					if((Math.abs((evt.time - currentTime)) < 300)){
						if((evt.mile == mile) && (i == evt.dir)){ //Need only last 5 minutes data only
							vid = evt.vid;
							totalSegmentVelocity += evt.speed;
							totalSegmentVehicles++;
						}
						
						if(i == 1){//We need to add the events to the temp list only once. Because we iterate twice through the list
							tmpEvtListLAV.add(evt);//Because of the filtering in the previous if statement we do not accumulate events that are older than 5 minutes
						}
					}
				}
				
				lav = ((float)totalSegmentVelocity/totalSegmentVehicles);
				if(!Float.isNaN(lav)){									
					_collector.emit("lav_event", new Values(Constants.LAV_EVENT_TYPE, mile, lav, i));

					totalSegmentVelocity = 0;
					totalSegmentVehicles = 0;
				}
			}
		}
			
		//We assign the updated list here. We have discarded the events that are more than 5 minutes duration
		evtListLAV = tmpEvtListLAV;	
	}
	
	private void calculateNOV() {
		ArrayList<Byte> segList = new ArrayList<Byte>();
		Hashtable<Byte, ArrayList<Integer> > htResult = new Hashtable<Byte, ArrayList<Integer> >();  
		
		//Get the list of segments first
		Iterator<PositionReportEvent> itr = evtListNOV.iterator();
		byte curID = -1;
		while(itr.hasNext()){
			curID = itr.next().mile;
			
			if(!segList.contains(curID)){
				segList.add(curID);
			}
		}
		
		Iterator<Byte> segItr = segList.iterator();
		int vid = -1;
		byte mile = -1;
		ArrayList<Integer> tempList = null;
		PositionReportEvent evt = null;

		//For each segment		
		while(segItr.hasNext()){
			mile = segItr.next();
			itr = evtListNOV.iterator();
			while(itr.hasNext()){
				evt = itr.next();
				
				if(evt.mile == mile){
					vid = evt.vid;
					
					if(!htResult.containsKey(mile)){
						tempList = new ArrayList<Integer>();
						tempList.add(vid);
						htResult.put(mile, tempList);
					}else{
						tempList = htResult.get(mile);
						tempList.add(vid);
						
						htResult.put(mile, tempList);
					}
				}
			}
		}
		
		Set<Byte> keys = htResult.keySet();
		
		Iterator<Byte> itrKeys = keys.iterator();
		int numVehicles = -1;
		mile = -1;
		
		while(itrKeys.hasNext()){
			mile = itrKeys.next();
			numVehicles = htResult.get(mile).size();
			
			_collector.emit("nov_event", new Values(Constants.NOV_EVENT_TYPE, ((int)Math.floor(currentSecond/60)), mile, numVehicles));
		}
	}
}
