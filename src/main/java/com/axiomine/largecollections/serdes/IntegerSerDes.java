/*
 * Copyright 2014 Axiomine
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.axiomine.largecollections.serdes;

import com.google.common.primitives.Ints;

public class IntegerSerDes {
    public static class SerFunction implements TurboSerializer<Integer> {
        private static final long serialVersionUID = 6L;

        public byte[] apply(Integer arg) {
            if (arg == null) {
                return null;
            } else {
                return Ints.toByteArray(arg);
            }
        }
    }
    
    public static class DeSerFunction implements TurboDeSerializer<Integer> {
        private static final long serialVersionUID = 6L;
        public Integer apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Ints.fromByteArray(arg);    
            }
            
        }
    }
    
}
