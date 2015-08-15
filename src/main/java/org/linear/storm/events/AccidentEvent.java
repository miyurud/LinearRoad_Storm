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

public class AccidentEvent {
	@Override
	public String toString() {
		return "AccidentEvent [vid1 = " + vid1 + ", vid2 = " + vid2 + ", xway=" + xway + ", mile="
				+ mile + ", dir=" + dir + " time=" + time + "]";
	}
	
	public String toCompressedString(){
		return "" + vid1 + " " + vid2 + " " + xway + " " + mile + " " + dir + " " + time;
	}

	public int vid1;//ID of the first vehicle
	public int vid2;
	public byte xway;
	public byte mile;
	public byte dir;
	public long time;

	public AccidentEvent(int vid1, int vid2, byte xway, byte mile, byte dir, long t) {
		this.vid1 = vid1;
		this.vid2 = vid2;
		this.xway = xway;
		this.mile = mile;
		this.dir = dir;
		this.time = t;
	}
	
	public AccidentEvent(){
		
	}
}
