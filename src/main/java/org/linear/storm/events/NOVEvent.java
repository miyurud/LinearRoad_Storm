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

package org.linear.storm.events;

/**
 * @author miyuru
 *
 */
public class NOVEvent {
	public int minute; // Current Minute
	public byte segment; //A segment is in the range 0..99; It corresponds to a mile in the high way system
	public int nov; //Number of vehicles in this particular Segment
	
	public NOVEvent(int current_minute, byte mile, int numVehicles) {
		this.minute = current_minute;
		this.segment = mile;
		this.nov = numVehicles;
	}
}
