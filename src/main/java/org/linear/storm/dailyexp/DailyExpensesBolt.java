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

package org.linear.storm.dailyexp;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

//import javax.jms.BytesMessage;
//import javax.jms.DeliveryMode;
//import javax.jms.JMSException;




import org.linear.storm.input.HistoryLoadingNotifier;
import org.linear.storm.events.ExpenditureEvent;
import org.linear.storm.events.HistoryEvent;
import org.linear.storm.util.Constants;
import org.linear.storm.util.Utilities;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;

public class DailyExpensesBolt  extends BaseRichBolt {
    OutputCollector _collector;
    private LinkedList<ExpenditureEvent> expEvtList;
    private LinkedList<HistoryEvent> historyEvtList;
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      expEvtList = new LinkedList<ExpenditureEvent>();
      historyEvtList = new LinkedList<HistoryEvent>();

      String historyFile = "/home/miyuru/projects/pamstream/data/xaaaa";//This is also for the moment. Have to implement a concrete solution later. 
	  HistoryLoadingNotifier notifierObj = new HistoryLoadingNotifier(false); 
	  notifierObj.start();//The notification server starts at this point
	  loadHistoricalInfo(historyFile);
		
	  notifierObj.setStatus(true);//At this moment we notify all the listeners that we have done loading the history data
    }

    @Override
    public void execute(Tuple tuple) {
//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//      _collector.ack(tuple);
    	
//    	public long time; //A timestamp measured in seconds since the start of the simulation
//    	public int vid; //vehicle identifier
//    	public int qid; //Query ID
//    	public byte xWay; //Express way number 0 .. 9
//    	public int day; //The day for which the daily expenditure value is needed
    	    
    	//_collector.emit("daily_exp", new Values(time, vid, xway, qid, day));
    	
    	ExpenditureEvent expEvt = new ExpenditureEvent(); 
    	expEvt.time = tuple.getLong(0);
    	expEvt.vid = tuple.getInteger(1);
    	expEvt.xWay = tuple.getByte(2);
    	expEvt.qid = tuple.getInteger(3);
    	expEvt.day = tuple.getInteger(4);
    	
    	process(expEvt);
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream("dailyexp_events", new Fields(	"vid",
    															"sum"));
    }
    
    public void process(ExpenditureEvent evt){    	
		int len = 0;
		Statement stmt;
		
		Iterator<HistoryEvent> itr = historyEvtList.iterator();
		int sum = 0;
		while(itr.hasNext()){
			HistoryEvent histEvt = (HistoryEvent)itr.next();
			
			if((histEvt.carid == evt.vid) && (histEvt.x == evt.xWay) && (histEvt.d == evt.day)){
				sum += histEvt.daily_exp;
			}					
		}
		
//		try{
//		    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//
//		    bytesMessage.writeBytes((Constants.DAILY_EXP_EVENT_TYPE + " " + evt.vid + " " + sum).getBytes());
//		    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//		    producer_output.send(bytesMessage);
//		}catch(JMSException e){
//			e.printStackTrace();
//		}
		
		_collector.emit("dailyexp_events", new Values(evt.vid, sum));
		
    }
    
	public void loadHistoricalInfo(String inputFileHistory) {
			BufferedReader in;
			try {
				in = new BufferedReader(new FileReader(inputFileHistory));
			
				String line;
				int counter = 0;
				int batchCounter = 0;
				int BATCH_LEN = 10000;//A batch size of 1000 to 10000 is usually OK		
				Statement stmt;
				StringBuilder builder = new StringBuilder();
						
				//log.info(Utilities.getTimeStamp() + " : Loading history data");
				while((line = in.readLine()) != null){	
					
//					try{
//						Thread.sleep(1000);//just wait one second and check again
//					}catch(InterruptedException e){
//						//Just ignore
//					}
					
					//#(1 8 0 55)
					/*
					0 - Car ID
					1 - day
					2 - x - Expressway number
					3 - daily expenditure
					*/
					
					String[] fields = line.split(" ");
					fields[0] = fields[0].substring(2);
					fields[3] = fields[3].substring(0, fields[3].length() - 1);
					
					historyEvtList.add(new HistoryEvent(Integer.parseInt(fields[0]), Integer.parseInt(fields[1]), Integer.parseInt(fields[2]), Integer.parseInt(fields[3])));
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (NumberFormatException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			}
			
			//log.info(Utilities.getTimeStamp() + " : Done Loading history data");
			//Just notfy this to the input event injector so that it can start the data emission process
			try {
				PrintWriter writer = new PrintWriter("done.txt", "UTF-8");
				writer.println("\n");
				writer.flush();
				writer.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
			
	}
}
