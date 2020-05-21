# deutschHaus
## Background information: 
This is a small "toy" scala based spark job.  It is used for the purposes of testing what spark calls result in objects landing on the JVM heap vs. which ones allow Tungsten to manage the objects.  It started out as a simple comparison of RDD vs. Dataframe/Dataset api approach.  We found that there was little difference between the two (from a run time and performance metrics perspective). 

## To run locally: 
1.  Take the files found in src/main/resources and put them in a local directory:  `/tmp/wohnungData` is what is specified in the code.
2.  `sbt` to invoke the sbt shell.
3.  run - this will default to the dataframe version.  
    a.  If you try `run -a rdd` this will switch to the RDD version.
    
    

