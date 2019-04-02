package com.iitm.research_explorer

object EdgeType extends Enumeration {
  type EdgeType = Value
  val PaperToAuthor, PaperToVenue, AuthorToVenue, AuthorToAuthor, PaperToPaper = Value
}
