package frontend

sealed class ResultForm
case object MATRIX extends ResultForm
case object ARRAY extends ResultForm
case object TUPLES_BIT extends ResultForm
case object TUPLES_PREFIX extends ResultForm

