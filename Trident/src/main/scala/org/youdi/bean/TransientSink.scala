package org.youdi.bean

import java.lang.annotation.{ElementType, Retention, Target}
import java.lang.annotation.RetentionPolicy.RUNTIME

@Target(ElementType.FIELD)
@Retention(RUNTIME)
class TransientSink extends annotation.Annotation {}
