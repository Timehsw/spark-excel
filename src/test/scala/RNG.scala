trait RNG {

  def nextInt: (Int, RNG)

}

object RNG {

  case class SimpleRNG(seed: Long) extends RNG {

    override def nextInt: (Int, RNG) = {
      val newSeed = (seed * 0x5DEECE66DL + 0xBL) & 0xFFFFFFFFFFFFL
      val nextRGN = SimpleRNG(newSeed)
      val n = (newSeed >>> 16).toInt
      (n, nextRGN)
    }

  }

  type Rand[+A] = RNG => (A, RNG)

  val int: Rand[Int] = rng => {
    println(rng)
    rng.nextInt
  }

  def map[A, B](s: Rand[A])(f: A => B): Rand[B] = rng => {
    println(rng)
    val (a, rng2) = s(rng)
    (f(a), rng2)
  }

}