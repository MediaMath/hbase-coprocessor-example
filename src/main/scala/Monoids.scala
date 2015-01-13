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

import com.twitter.algebird.{HLL, HyperLogLogMonoid, Monoid}

object Monoids {
  val longMonoid = Monoid.longMonoid
  val hllMonoid = new HyperLogLogMonoid(12)
  val doubleMonoid = Monoid.doubleMonoid

  def plus(l: Any, r: Any) = {
    (l, r) match {
        case (x: Long, y: Long) ⇒ longMonoid.plus(x, y)
        case (x: Double, y: Double) ⇒ doubleMonoid.plus(x, y)
        case (x: HLL, y: HLL) ⇒ hllMonoid.plus(x, y)
    }
  }
}
