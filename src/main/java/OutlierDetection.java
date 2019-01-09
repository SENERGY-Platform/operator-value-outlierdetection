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

import org.infai.seits.sepl.operators.Message;
import org.infai.seits.sepl.operators.OperatorInterface;

import java.util.ArrayList;
import java.util.List;

public class OutlierDetection implements OperatorInterface {

    private double oldValue, newValue, diff, avg, stddev, sum;
    private boolean firstRun = true;
    private List<Double> diffs;
    private long oldTime, newTime, timeDiff;
    private int sigma;

    public OutlierDetection(int sigma) {
        super();
        oldTime = 0;
        diffs = new ArrayList<>();
        this.sigma = sigma;
    }

    @Override
    public void run(Message message) {
        newValue = message.getInput("value").getValue();
        newTime = DateParser.parseDateMills(message.getInput("timestamp").getString());
        if (firstRun) {
            oldTime = newTime;
            firstRun = false;
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
    }

    @Override
    public void config(Message message) {
        message.addInput("timestammp");
        message.addInput("value");
    }
}
