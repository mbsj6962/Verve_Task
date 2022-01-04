package com.verve.task1_2

import scala.collection.mutable.ArrayBuffer

class Reader {
  def readImpression(address: os.Path): ArrayBuffer[ujson.Obj] = {
    val impressionJson = os.read(address)
    ujson.read(impressionJson).value.asInstanceOf[ArrayBuffer[ujson.Obj]]
  }

  def readClick(address: os.Path): ArrayBuffer[ujson.Obj] = {
    val clickJson = os.read(address)
    ujson.read(clickJson).value.asInstanceOf[ArrayBuffer[ujson.Obj]]
  }}
