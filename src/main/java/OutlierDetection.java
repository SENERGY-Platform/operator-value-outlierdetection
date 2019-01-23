/*
 * Copyright 2018 InfAI (CC SES)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.infai.seits.sepl.operators.Helper;
import org.infai.seits.sepl.operators.Message;
import org.infai.seits.sepl.operators.OperatorInterface;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class OutlierDetection implements OperatorInterface {


    HashMap<String, OutlierDeviceWrapper> map;
    private int sigma;

    public OutlierDetection() {
        sigma = Helper.getEnv("sigma", 3);
        map = new HashMap<>();
    }

    @Override
    public void run(Message message) {
        double newValue, oldValue, diff, avg, stddev, sum;
        long oldTime, newTime, timeDiff;
        List<Double> diffs;
        OutlierDeviceWrapper odw;
        newValue = message.getInput("value").getValue();
        newTime = DateParser.parseDateMills(message.getInput("timestamp").getString());
        String deviceID = message.getInput("deviceID").getString();
        if (!map.containsKey(deviceID)) {
            //Setup first item
            diffs = new ArrayList<>();
            odw = new OutlierDeviceWrapper();
            odw.setDiffs(diffs);
            odw.setTime(newTime);
            odw.setValue(newValue);
            map.put(deviceID, odw);
            return;
        }else{
            odw = map.get(deviceID);
            oldTime = odw.getTime();
            diffs = odw.getDiffs();
            oldValue = odw.getValue();
        }
        if ((timeDiff = newTime - oldTime) <= 0) {
            return; //Out of order or same timestamp. Skip this value and won't take it into consideration.
        }
        diff = (newValue - oldValue) / ((double) timeDiff);
        diffs.add(diff);

        //Compute average
        sum = 0.0;
        for (Double d : diffs) {
            sum += d;
        }
        avg = sum / diffs.size();

        //Compute stddev
        sum = 0.0;
        for (Double d : diffs) {
            sum += (d - avg) * (d - avg);
        }
        sum /= diffs.size();
        stddev = Math.sqrt(sum);

        //Check for outlier
        double sigmaCurrent = (diff - avg) / stddev;
        if (sigmaCurrent > sigma || sigmaCurrent < (sigma * -1)) {
            message.output("sigma", sigmaCurrent);
        }

        odw.setDiffs(diffs);
        odw.setTime(newTime);
        odw.setValue(newValue);
        map.put(deviceID, odw);
    }

    @Override
    public void config(Message message) {
        message.addInput("deviceID");
        message.addInput("timestamp");
        message.addInput("value");
    }
}
