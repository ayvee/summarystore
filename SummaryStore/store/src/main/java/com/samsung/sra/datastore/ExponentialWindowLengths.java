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
package com.samsung.sra.datastore;

/**
 * 1, b, b^2, ..., b^k, ...
 */
public class ExponentialWindowLengths implements WindowLengthsSequence {
    private double next = 1;
    private final double base;

    public ExponentialWindowLengths(double base) {
        this.base = base;
    }

    @Override
    public long nextWindowLength() {
        double prev = next;
        next *= base;
        return (long)Math.ceil(prev);
    }
}
