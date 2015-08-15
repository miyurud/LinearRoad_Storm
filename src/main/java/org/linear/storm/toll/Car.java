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

public class Car {
		long time;
		int carid;
		byte speed, mile, xway, lane, dir, offset;
		boolean notified;
		
		public Car() {
			this.time = -1;
			this.carid = 0;
			this.speed = 0;
			this.xway = -1; 
			this.lane = -1;
			this.dir = -1;
		}
		
		public Car(long time, int carid, byte speed, byte xway0, byte lane0, byte dir0, byte mile) {
			this.time = time; this.carid = carid; this.speed = speed;
			this.xway = xway0; this.lane = lane0; this.dir = dir0;
			this.mile = mile;
		}
}
