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

package org.linear.storm.accbalance;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import org.linear.storm.events.AccountBalanceEvent;
import org.linear.storm.util.Constants;

import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;
import backtype.storm.task.OutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.task.TopologyContext;

public class AccBalanceBolt  extends BaseRichBolt {
    OutputCollector _collector;
    private int count;
    private LinkedList<AccountBalanceEvent> accEvtList;
    private HashMap<Integer, Integer> tollList;
    
    
    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
      accEvtList = new LinkedList<AccountBalanceEvent>();
      tollList = new HashMap<Integer, Integer>();
    }

    @Override
    public void execute(Tuple tuple) {
//      _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
//      _collector.ack(tuple);
    	
    	
    	_collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream("accbalance_event", new Fields(	"vid",
    															"toll"));
    }
    
    public void process(AccountBalanceEvent evt){
		//int len = 0;
		//Statement stmt;
		//BytesMessage bytesMessage = null;
			    		
		if(evt != null){
//				try{
//				    bytesMessage = jmsCtx_output.getSession().createBytesMessage();
//				    bytesMessage.writeBytes((Constants.ACC_BAL_EVENT_TYPE + " " + evt.vid + " " + tollList.get(evt.vid)).getBytes());
//				    bytesMessage.setJMSDeliveryMode(DeliveryMode.NON_PERSISTENT);
//				    producer_output.send(bytesMessage);
//				}catch(JMSException e){
//					e.printStackTrace();
//				}
			_collector.emit("accbalance_event", new Values(evt.vid, tollList.get(evt.vid)));
		}
    }    
    
}