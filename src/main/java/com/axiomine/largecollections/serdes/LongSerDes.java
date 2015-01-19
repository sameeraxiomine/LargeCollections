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

import com.google.common.base.Function;
import com.google.common.primitives.Longs;

public class LongSerDes {
    public static class SerFunction implements TurboSerializer<Long>{
        private static final long serialVersionUID = 9L;

        public byte[] apply(Long arg) {
            if(arg==null){
                return null;
            }
            else{
                return Longs.toByteArray(arg);    
            }
            
        }    
    }
    
    public static class DeSerFunction implements TurboDeSerializer<Long>{
        private static final long serialVersionUID = 9L;
        public Long apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return Longs.fromByteArray(arg);    
            }
            
        }    
    }


}
