package com.thoughtworks.sparkFlatter


import scala.annotation.StaticAnnotation
import scala.language.experimental.macros
import scala.reflect.macros.Context

/**
  * @author 杨博 (Yang Bo) &lt;pop.atry@gmail.com&gt;
  */
object generateUdt {
  def macroTransform(c: Context)(annottees: c.Expr[Any]*): c.Expr[Any] = {
    import c.universe._
    val Seq(classDefExpr, moduleDefExpr) = annottees

    val patchedModuleDef = {
      val moduleDef = moduleDefExpr.tree
      val ModuleDef(mods, name, impl@Template(parents, self, body)) = moduleDef

      val enumTypeTree = Ident(name.toTypeName)

      val dynamicClassDef =
        q"""
      final class DynamicUserDefinedType extends _root_.org.apache.spark.sql.types.UserDefinedType[$enumTypeTree] {
        override def sqlType = _root_.org.apache.spark.sql.types.StringType
        override def serialize(obj: Any) = _root_.org.apache.spark.unsafe.types.UTF8String.fromString(obj.toString)
        override def userClass = classOf[$enumTypeTree]
        override def deserialize(datum: Any): $enumTypeTree = {
          datum match {
            case ..${
          for {
            ModuleDef(_, enumValueName, _) <- body
          } yield {
            cq"${enumValueName.toString} => ${Ident(enumValueName)}"
          }
        }
          }
        }
      }
    """

      treeCopy.ModuleDef(moduleDef, mods, name, treeCopy.Template(impl, parents, self, body :+ q"_root_.org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)" :+ dynamicClassDef))
    }

    val patchedClassDef = {
      val classDef = classDefExpr.tree
      val ClassDef(mods, name, tparams, impl) = classDef
      treeCopy.ClassDef(classDef, Modifiers(mods.flags, mods.privateWithin, q"new _root_.org.apache.spark.sql.types.SQLUserDefinedType(udt = classOf[${name.toTermName}.DynamicUserDefinedType])" :: mods.annotations), name, tparams, impl)
    }


    c.Expr(q"""
       $patchedClassDef
       $patchedModuleDef
    """)
  }
}

class generateUdt extends StaticAnnotation {
  def macroTransform(annottees: Any*): Any = macro generateUdt.macroTransform
}