package Lab11.Transform

import Lab11.Case.Names
import Lab11.Case.Actors

object Trans {

  def hasAssignedJob(actors_row: Actors): Boolean = {
    if(actors_row.category==null) return false;
    return true;
  }

  def hasKids(names_row: Names): Boolean = {
    if(names_row.children==null) return false;
    return Integer.valueOf(names_row.children)>0;
  }

}