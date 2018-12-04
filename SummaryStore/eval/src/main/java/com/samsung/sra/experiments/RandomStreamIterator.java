/*
* Copyright 2016 Samsung Research America. All rights reserved.
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
package com.samsung.sra.experiments;

import java.util.SplittableRandom;

public class RandomStreamIterator {
    private final Distribution<Long> interarrivals, values;
    private SplittableRandom random;
    private final long R;

    public RandomStreamIterator(Distribution<Long> interarrivals, Distribution<Long> values, long R) {
        this.interarrivals = interarrivals;
        this.values = values;
        this.R = R;
        reset();
    }

    private long T0, T1;

    public void setTimeRange(long T0, long T1) {
        this.T0 = T0;
        this.T1 = T1;
        reset();
    }

    public long currT, currV;

    public boolean hasNext() {
        return currT <= T1;
    }

    public void next() {
        currT += interarrivals.next(random);
        currV = values.next(random);
    }

    public void reset() {
        this.random = new SplittableRandom(R);
        this.currT = T0;
        this.currV = values.next(random);
    }
}
