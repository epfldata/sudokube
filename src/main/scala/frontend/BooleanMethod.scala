package frontend

sealed class BooleanMethod
case object EXIST extends BooleanMethod
case object FORALL extends BooleanMethod