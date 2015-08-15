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

package org.linear.storm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.BoltDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import org.linear.storm.accbalance.AccBalanceBolt;
import org.linear.storm.accident.AccidentDetectionBolt;
import org.linear.storm.dailyexp.DailyExpensesBolt;
import org.linear.storm.input.InputEventInjectorSpout;
import org.linear.storm.output.OutputBolt;
import org.linear.storm.segstat.SegStatBolt;
import org.linear.storm.toll.TollBolt;

public class LinearRoadTopology {

  public static void main(String[] args) throws Exception {
    TopologyBuilder builder = new TopologyBuilder();

    builder.setSpout("inputEventInjector", new InputEventInjectorSpout(), 1);//For the moment we keep just one input injector spout
    builder.setBolt("segstatBolt", new SegStatBolt(), 1).shuffleGrouping("inputEventInjector", "position_report");
    builder.setBolt("accidentBolt", new AccidentDetectionBolt(), 1).shuffleGrouping("inputEventInjector", "position_report");

    BoltDeclarer bd1 = builder.setBolt("tollBolt", new TollBolt(), 1).shuffleGrouping("inputEventInjector", "position_report").shuffleGrouping("segstatBolt","nov_event").shuffleGrouping("segstatBolt","lav_event").shuffleGrouping("accidentBolt", "accident_event");
    builder.setBolt("accbalanceBolt", new AccBalanceBolt(), 1).shuffleGrouping("inputEventInjector", "accbal_report").shuffleGrouping("tollBolt", "toll_event");
    builder.setBolt("dailyExpBolt", new DailyExpensesBolt(), 1).shuffleGrouping("inputEventInjector", "daily_exp");
    builder.setBolt("outputBolt", new OutputBolt(), 1).shuffleGrouping("tollBolt", "toll_event").shuffleGrouping("accbalanceBolt", "accbalance_event").shuffleGrouping("dailyExpBolt", "dailyexp_events");

    Config conf = new Config();
    conf.setDebug(false);

    if (args != null && args.length > 0) {
      conf.setNumWorkers(3);

      StormSubmitter.submitTopology(args[0], conf, builder.createTopology());
    }
    else {

      LocalCluster cluster = new LocalCluster();
      cluster.submitTopology("LinearRoad", conf, builder.createTopology());
      Utils.sleep(3600000); //Sleep for 1 hour. This method's argument is in miliseconds
      cluster.killTopology("LinearRoad");
      cluster.shutdown();
    }
  }
}
