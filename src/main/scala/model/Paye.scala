package model

case class Paye (
             payeref : String,
             mar_jobs: Int,
             june_jobs: Int,
             sept_jobs: Int,
             dec_jobs: Int
           )