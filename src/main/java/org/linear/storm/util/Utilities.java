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

package org.linear.storm.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * This is a utility class
 * @author miyuru
 *
 */
public class Utilities {
	
	/**
	 *  This method is for obtaining the time stamp of the system. The time stamp is of the following format
	 *  <yy/MM/dd HH:mm:ss>-<time stamp in milisecond>
	 * @return
	 */
	public static String getTimeStamp(){	
		DateFormat dateFormat = new SimpleDateFormat("yy/MM/dd HH:mm:ss");
		Calendar cal = Calendar.getInstance();
		
		return dateFormat.format(cal.getTime())+"-"+cal.getTimeInMillis();
	}
}
