# deutschHaus
## Background information: 
This is a small "toy" scala based spark job.  It is used for the purposes of testing what spark calls result in objects landing on the JVM heap vs. which ones allow Tungsten to manage the objects.  It started out as a simple comparison of RDD vs. Dataframe/Dataset api approach.  We found that there was little difference between the two (from a run time and performance metrics perspective). 

## To run locally: 
1.  Take the files found in src/main/resources and put them in a local directory:  one of the arguments you will pass in, -d, will point to this directory.
2.  Not all of the files are included.  One file, for apartment rental data, was too large. This file can be downloaded from https://www.kaggle.com/corrieaar/apartment-rental-offers-in-germany and transformed into 
a parquet file by reading it in jupyter or zeppelin and writing to parquet.  
2.  `sbt` to invoke the sbt shell.
3.  `run` from sbt shell with appropriate parameters:
    <ul>
    <li>-a, optional:  if you use `run -a rdd` this will switch to the RDD version.</li>
    <li>-l, optional:  if you set -l false, then it will run in non-local mode (for use with emr clusters).  This defaults to -l true; running in local mode.</li>
    <li>-d, required:  set your data directory - either a local directory when running locally or an s3 bucket when running on emr.</li>
    </ul>  
    
    For example, to run locally, you could use: 
    `run -a rdd -l true -d /Users/username/data/`

    
    
    

