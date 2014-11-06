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
