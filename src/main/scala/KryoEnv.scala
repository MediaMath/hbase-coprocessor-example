/**
 * Copyright (C) 2014 MediaMath <http://www.mediamath.com>
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
 *
 * @author daikeshi
 * @author themodernlife
 */

import com.twitter.chill.algebird.AlgebirdRegistrar
import com.twitter.chill.{KryoPool, ScalaKryoInstantiator}

object KryoEnv {
  val KryoPoolSize = 10

  val kryo = {
    val inst = () ⇒ {
      val newK = (new ScalaKryoInstantiator).newKryo()
      newK.setReferences(false)
      (new AlgebirdRegistrar).apply(newK)
      newK
    }
    KryoPool.withByteArrayOutputStream(KryoPoolSize, inst)
  }
}

