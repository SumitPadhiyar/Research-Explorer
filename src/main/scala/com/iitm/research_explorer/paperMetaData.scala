package com.iitm.research_explorer

case class Author(ids:Array[String], name: String)

case class PaperMetaData(id:String, authors: Array[Author])
