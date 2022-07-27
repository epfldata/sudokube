package core.materialization

import combinatorics.{Big, FD_Model}

class MaterializationStrategyInfo(m: MaterializationStrategy) {
  def wc_estimate(s: Int) = {
    m.projections.map(x => {
      val dense_size = Big.pow2(x.length)
      val sparse_full_db_size = Big.pow2(s)

      if (dense_size < sparse_full_db_size) dense_size
      else sparse_full_db_size
    }).sum
  }

  def fd_model_est(s: Int): BigInt = {
    m.projections.map(x => FD_Model.avg_u(m.n_bits, s, x.length)).sum
  }

  def wc_ratio(s: Int) = BigDecimal(wc_estimate(s)) / BigDecimal(Big.pow2(s))

  def fd_ratio(s: Int) = {
    BigDecimal(fd_model_est(s)) / BigDecimal(Big.pow2(s))
  }

  def apply() {
    println("Upper bounds on sparse ratios for various # data items:"
      + " Kilo->" + wc_ratio(10)
      + " Mega->" + wc_ratio(20)
      + " Giga->" + wc_ratio(30)
      + " Tera->" + wc_ratio(40)
      + " Peta->" + wc_ratio(50)
      + " Exa->" + wc_ratio(60))

    if (m.n_bits >= 60)
      println("In the FD model, the estimated storage overhead is:"
        + " Kilo->" + fd_model_est(10)
        + " Mega->" + fd_model_est(20)
        + " Giga->" + fd_model_est(30)
        + " Tera->" + fd_model_est(40)
        + " Peta->" + fd_model_est(50)
        + " Exa->" + fd_model_est(60))
  }
}
