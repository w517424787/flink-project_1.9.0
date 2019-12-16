package com.sdf.flink.util

import org.slf4j.LoggerFactory

trait Logger extends Serializable {
  lazy val log = LoggerFactory.getLogger(this.getClass)
}
