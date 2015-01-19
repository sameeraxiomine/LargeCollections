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

import java.io.Externalizable;

import com.axiomine.largecollections.utilities.SerDeUtils;
import com.google.common.base.Function;

public class ExternalizableSerDes {
    public static class SerFunction implements TurboSerializer<Externalizable> {
        private static final long serialVersionUID = 4L;

        public byte[] apply(Externalizable arg) {
            if(arg==null){
                return null;
            }
            else{
                byte[] ba = null;
                ba = SerDeUtils.serializeExternalizable(arg);
                return ba;
            }
        }
    }

    public static class DeSerFunction implements TurboDeSerializer<Externalizable>{        
        private static final long serialVersionUID = 4L;

        public Externalizable apply(byte[] arg) {
            if(arg==null){
                return null;
            }
            else{
                return SerDeUtils.deSerializeExternalizable(arg);    
            }
            
        }    
    }
}
