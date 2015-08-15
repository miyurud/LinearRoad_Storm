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


public class Constants
{
    public static final String CONFIG_FILENAME = "linearroad.properties";
    
    public static final String LINEAR_HISTORY = "linear-history-file";
    public static final String LINEAR_CAR_DATA_POINTS = "linear-cardatapoints-file";
    
    public static final long POS_EVENT_TYPE = 0;
    public static final long ACC_BAL_EVENT_TYPE = 2;
    public static final long DAILY_EXP_EVENT_TYPE = 3;
    public static final long TRAVELTIME_EVENT_TYPE = 4;
    public static final long NOV_EVENT_TYPE = -5;
    public static final long LAV_EVENT_TYPE = -6;
    public static final long TOLL_EVENT_TYPE = 7;
    public static final long ACCIDENT_EVENT_TYPE = -8;
    
    public static final String LINEAR_DB_HOST = "linear-db-host";
    public static final String LINEAR_DB_PORT = "linear-db-port";
    
    public static final int HISTORY_LOADING_NOTIFIER_PORT = 2233;
    
    public static final String CLEAN_START = "clean-start";

	public static final String HISTORY_COMPONENT_HOST = "localhost"; //This is strictly a temporary value. Must find a way to 
																	 //get the correct location of the history loading component.
}
