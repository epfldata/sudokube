package frontend.generators

import frontend.Sampling
import util.{BigBinary, Profiler}

class MigrosGenerator {
  //val key = Profiler("EncodeKey"){
  //  //sch.encode_tuple(r)
  //  //calculating from MSB to LSB
  //  //TODO: Fix HACK!!!
  //
  //
  //  // 22 bits product  10 bits location 32 bits time
  //  val prod = BigInt(Sampling.f2(1 << 15)) << 42
  //  assert(prod >= 0)
  //  val loc = BigInt((scala.util.Random.nextInt(1 << 10))) << 32
  //  assert(loc >= 0)
  //  assert(n < (1 << 30))
  //  val time = (BigInt(i) << 30)/n + scala.util.Random.nextInt(((1<<30)/n).toInt)
  //  assert(time >= 0)
  //  val total = time + loc + prod
  //  assert(total >= 0)
  //  assert(total < (BigInt(1) << 64))
  //  val k = BigBinary(time + loc + prod)
  //  //BigBinary((0 until sch.n_bits).foldLeft(BigInt(0)){ case (acc, cur) =>
  //  //  (acc << 1) + sampling_f(2)
  //  //  })
  //  if(n < 100) {
  //    println(s"Prod = $prod  loc = $loc time = $time ")
  //    println(s"key = $k")
  //  }
  //  k
  //}
}
