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
package com.samsung.sra.optimization;

/**
 * Uniform random distribution over all time intervals
 */
public class UniformTMeasure extends TMeasure {
    public UniformTMeasure(int N) {
        super(N);
    }

    @Override
    public double M_l_r(int l, int r) {
        return 1;
    }
}
