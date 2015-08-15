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

package org.linear.storm.toll;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import org.linear.storm.events.AccidentEvent;
import org.linear.storm.events.LAVEvent;
import org.linear.storm.events.NOVEvent;
import org.linear.storm.events.PositionReportEvent;
import org.linear.storm.events.TollCalculationEvent;
import org.linear.storm.toll.AccNovLavTuple;
import org.linear.storm.toll.Car;
import org.linear.storm.util.Constants;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;

/**
 * The toll calculation bolt
 * @author miyuru
 *
 */

public class TollBolt  extends BaseRichBolt {
    OutputCollector _collector;
    private int count;
    
	LinkedList cars_list;
	HashMap<Integer, Car> carMap; 
	HashMap<Byte, AccNovLavTuple> segments;
	byte NUM_SEG_DOWNSTREAM = 5; //Number of segments downstream to check whether an accident has happened or not
	int BASE_TOLL = 2; //This is a predefined constant (mentioned in Richard's thesis)
	private LinkedList<PositionReportEvent> posEvtList;

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      carMap =  new HashMap<Integer, Car>();
      cars_list = new LinkedList();
      segments = new HashMap<Byte, AccNovLavTuple>();
      posEvtList = new LinkedList<PositionReportEvent>();
    }

    @Override
    public void execute(Tuple tuple) {
//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//      _collector.ack(tuple);
    	
    	//For the moment it seems we can get an Integer as the flag. But later it will be more efficient to
    	//change it to Byte. All the current flag values are set to minus values so that we can distinguish those
    	//from PositionReport events.
    	long typeField = tuple.getLong(0);
    	
    	//We cannot switch on the long values. Therefore we go for if()else() construct.
    	/*
    	switch(typeField){
	    	case Constants.NOV_EVENT_TYPE:
	    		//_collector.emit("nov_event", new Values(Constants.NOV_EVENT_TYPE, ((int)Math.floor(currentSecond/60)), mile, numVehicles));
	    	   try{
	    		   NOVEvent obj2 = new NOVEvent(tuple.getInteger(1), tuple.getByte(2), tuple.getInteger(3));
	    		   novEventOccurred(obj2);
	    	   }catch(NumberFormatException e){
	    		   System.out.println("Not Number Format Exception for tuple : " + tuple);
	    	   }	    		
	    		break;
	    	case Constants.LAV_EVENT_TYPE:
	    		//_collector.emit("lav_event", new Values(Constants.LAV_EVENT_TYPE, mile, lav, i));
		    	LAVEvent obj = new LAVEvent(tuple.getByte(1), tuple.getFloat(2), tuple.getByte(3));
		    	lavEventOcurred(obj);	    		
	    		break;
	    	case Constants.ACCIDENT_EVENT_TYPE:
	    		//_collector.emit("accident_event", new Values(a_event.vid1, a_event.vid2, a_event.xway, a_event.mile, a_event.dir));
	    		AccidentEvent accidentEvent = new AccidentEvent();
	    		accidentEvent.vid1 = tuple.getInteger(1);
	    		accidentEvent.vid2 = tuple.getInteger(2);
	    		accidentEvent.xway = tuple.getByte(3);
	    		accidentEvent.mile = tuple.getByte(4);
	    		accidentEvent.dir = tuple.getByte(5);
	    		
	    		accidentEventOccurred(accidentEvent);
	    		
	    		break;
	    	default: //Here the default will be just a PositionReport that is sent from the InputEventInjector
	    		PositionReportEvent posEvt = new PositionReportEvent();
	    		//new Values(time, vid, spd, xway, lane, dir, mile)
	    		posEvt.time = tuple.getLong(0);
	    		posEvt.vid = tuple.getInteger(1);
	    		posEvt.speed = tuple.getByte(2);
	    		posEvt.xWay = tuple.getByte(3);
	    		posEvt.lane = tuple.getByte(4);
	    		posEvt.dir = tuple.getByte(5);
	    		posEvt.mile = tuple.getByte(6);
	    		posEvt.offset = tuple.getByte(7);
	    		
	    		process(posEvt);
	    		break;
    	}
    	*/
    	
    	if(Constants.NOV_EVENT_TYPE == typeField){
    		//_collector.emit("nov_event", new Values(Constants.NOV_EVENT_TYPE, ((int)Math.floor(currentSecond/60)), mile, numVehicles));
	    	   try{
	    		   NOVEvent obj2 = new NOVEvent(tuple.getInteger(1), tuple.getByte(2), tuple.getInteger(3));
	    		   novEventOccurred(obj2);
	    	   }catch(NumberFormatException e){
	    		   System.out.println("Not Number Format Exception for tuple : " + tuple);
	    	   }	    		
    	}else if(Constants.LAV_EVENT_TYPE == typeField){
    		//_collector.emit("lav_event", new Values(Constants.LAV_EVENT_TYPE, mile, lav, i));
	    	LAVEvent obj = new LAVEvent(tuple.getByte(1), tuple.getFloat(2), tuple.getByte(3));
	    	lavEventOcurred(obj);	    		
    	}else if(Constants.ACCIDENT_EVENT_TYPE == typeField){
    		//_collector.emit("accident_event", new Values(a_event.vid1, a_event.vid2, a_event.xway, a_event.mile, a_event.dir));
    		AccidentEvent accidentEvent = new AccidentEvent();
    		accidentEvent.vid1 = tuple.getInteger(1);
    		accidentEvent.vid2 = tuple.getInteger(2);
    		accidentEvent.xway = tuple.getByte(3);
    		accidentEvent.mile = tuple.getByte(4);
    		accidentEvent.dir = tuple.getByte(5);
    		
    		accidentEventOccurred(accidentEvent);    		
    	}else{
    		PositionReportEvent posEvt = new PositionReportEvent();
    		//new Values(time, vid, spd, xway, lane, dir, mile)
    		posEvt.time = tuple.getLong(0);
    		posEvt.vid = tuple.getInteger(1);
    		posEvt.speed = tuple.getByte(2);
    		posEvt.xWay = tuple.getByte(3);
    		posEvt.lane = tuple.getByte(4);
    		posEvt.dir = tuple.getByte(5);
    		posEvt.mile = tuple.getByte(6);
    		posEvt.offset = tuple.getShort(7);
    		
    		process(posEvt);
    	}
    	
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	//_collector.emit("toll_event", new Values(evt.vid, evt.mile, 0));
    	declarer.declareStream("toll_event", new Fields("vid",
    													"mile",
    													"toll"));
    }
    
	public void accidentEventOccurred(AccidentEvent accEvent) {
		System.out.println("Accident Occurred :" + accEvent.toString());
		boolean flg = false;
		
		synchronized(this){
			flg = segments.containsKey(accEvent.mile);
		}
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.isAcc = true;
			synchronized(this){
				segments.put(accEvent.mile, obj);
			}
		}else{
			synchronized(this){
				AccNovLavTuple obj = segments.get(accEvent.mile);
				obj.isAcc = true;
				segments.put(accEvent.mile, obj);
			}
		}
	}
    
    public void novEventOccurred(NOVEvent novEvent){
		boolean flg = false;

		flg = segments.containsKey(novEvent.segment);
	
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.nov = novEvent.nov;
			segments.put(novEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(novEvent.segment);
			obj.nov = novEvent.nov;

			segments.put(novEvent.segment, obj);
		}    	
    }
    
    public void lavEventOcurred(LAVEvent lavEvent){
		boolean flg = false;
		
		flg = segments.containsKey(lavEvent.segment); 
		
		if(!flg){
			AccNovLavTuple obj = new AccNovLavTuple();
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}else{
			AccNovLavTuple obj = segments.get(lavEvent.segment);
			obj.lav = lavEvent.lav;
			segments.put(lavEvent.segment, obj);
		}
    }

	public void process(PositionReportEvent evt){
		int len = 0;				
		Iterator<Car> itr = cars_list.iterator();
	
		if(!carMap.containsKey(evt.vid)){
			Car c = new Car();
			c.carid = evt.vid;
			c.mile = evt.mile;
			carMap.put(evt.vid, c);
		}else{
			Car c = carMap.get(evt.vid);

				if(c.mile != evt.mile){ //Car is entering a new mile/new segment
					c.mile = evt.mile;
					carMap.put(evt.vid, c);

					if((evt.lane != 0)&&(evt.lane != 7)){ //This is to make sure that the car is not on an exit ramp
						AccNovLavTuple obj = null;
						
						obj = segments.get(evt.mile);

						if(obj != null){									
							if(isInAccidentZone(evt)){
								System.out.println("Its In AccidentZone");
							}
							
							if(((obj.nov < 50)||(obj.lav > 40))||isInAccidentZone(evt)){
								//TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we set the toll to 0
								//tollEvt.vid = evt.vid;
								//tollEvt.segment = evt.mile;
																										
								//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
								
//								try{
//								    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//								    bytesMessage.writeBytes(msg.getBytes());
//								    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//								    producer_output.send(bytesMessage);
//								}catch(JMSException e){
//									e.printStackTrace();
//								}
//								
//								try{
//							          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
//							          bytesMessage.writeBytes(msg.getBytes());
//							          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//							          producer_account_balance.send(bytesMessage);
//									} catch (JMSException e) {
//										e.printStackTrace();
//									}
								
								_collector.emit("toll_event", new Values(evt.vid, evt.mile, 0));
								
							}else{
								//TollCalculationEvent tollEvt = new TollCalculationEvent(); //In this case we need to calculate a toll
								//tollEvt.vid = evt.vid;
								//tollEvt.segment = evt.mile;
								
								if(segments.containsKey(evt.mile)){
									AccNovLavTuple tuple = null;
									
									synchronized(this){
										tuple = segments.get(evt.mile);
									}
																				
									//tollEvt.toll = BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50);
																		
									//String msg = (Constants.TOLL_EVENT_TYPE + " " + tollEvt.toCompressedString());
									
//									try{
//									    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//
//									    bytesMessage.writeBytes(msg.getBytes());
//									    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//									    producer_output.send(bytesMessage);
//									}catch(JMSException e){
//										e.printStackTrace();
//									}
//									
//									try{
//								          bytesMessage = jmsCtx_account_balance.getSession().createBytesMessage();
//								          bytesMessage.writeBytes(msg.getBytes());
//								          bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//								          producer_account_balance.send(bytesMessage);
//										} catch (JMSException e) {
//											e.printStackTrace();
//										}
									
									_collector.emit("toll_event", new Values(evt.vid, evt.mile, BASE_TOLL*(tuple.nov - 50)*(tuple.nov - 50)));
								}
							}						
						}
					}
				}
		}
	}
    
	private boolean isInAccidentZone(PositionReportEvent evt) {
		byte mile = evt.mile;
		byte checkMile = (byte) (mile + NUM_SEG_DOWNSTREAM);
		
		while(mile < checkMile){
			if(segments.containsKey(mile)){
				AccNovLavTuple obj = segments.get(mile);
				
				if(Math.abs((evt.time - obj.time)) > 20){
					obj.isAcc = false;
					mile++;
					continue; //May be we remove the notification for a particular mile down the xway. But another mile still might have accident. Therefore, we cannot break here.
				}
				
				if(obj.isAcc){
					return true;
				}
			}
			mile++;
		}
		
		return false;
	}
}
