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

import org.infai.seits.sepl.operators.Config;
import org.infai.seits.sepl.operators.Helper;
import org.infai.seits.sepl.operators.Message;
import org.infai.seits.sepl.operators.OperatorInterface;

import java.util.HashMap;

public class OutlierDetection implements OperatorInterface {


    HashMap<String, OutlierDeviceWrapper> map;
    private int sigma;
    private boolean debug;

    public OutlierDetection() {
        try{
            sigma = Integer.parseInt(new Config().getConfigValue("SIGMA", "3"));
            debug = Boolean.parseBoolean(Helper.getEnv("DEBUG", "false"));
            if(debug)
                System.out.println("Set SIGMA to "+sigma);
        }catch(NumberFormatException nfe){
            System.err.println("Environment variable SIGMA is expected to be an integer, but is none");
            System.err.println(nfe.getMessage());
            sigma = 3;
        }
        map = new HashMap<>();
    }

    @Override
    public void run(Message message) {
        double newValue, oldValue, diff;
        long oldTime, newTime, timeDiff;
        OutlierDeviceWrapper odw;
        newValue = message.getInput("value").getValue();
        newTime = DateParser.parseDateMills(message.getInput("timestamp").getString());
        String deviceID = message.getInput("device").getString();
        Welford welford;
        if (!map.containsKey(deviceID)) {
            //Setup first item
            odw = new OutlierDeviceWrapper();
            odw.setWelford(new Welford());
            odw.setTime(newTime);
            odw.setValue(newValue);
            map.put(deviceID, odw);
            return;
        }else{
            odw = map.get(deviceID);
            oldTime = odw.getTime();
            oldValue = odw.getValue();
            welford = odw.getWelford();
        }
        if ((timeDiff = newTime - oldTime) <= 0) {
            return; //Out of order or same timestamp. Skip this value and won't take it into consideration.
        }
        diff = (newValue - oldValue) / ((double) timeDiff);
        welford.update(diff);

        //Check for outlier
        double sigmaCurrent = (diff - welford.mean()) / welford.std();
        if (sigmaCurrent > sigma || sigmaCurrent < (sigma * -1)) {
            message.output("sigma", sigmaCurrent);
        }

        odw.setTime(newTime);
        odw.setValue(newValue);
        map.put(deviceID, odw);
    }

    @Override
    public void config(Message message) {
        message.addInput("device");
        message.addInput("timestamp");
        message.addInput("value");
    }
}
