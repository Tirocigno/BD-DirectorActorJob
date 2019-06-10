# Big-Data-18-19-Director-Actor-Job

Elaborato di progetto per l'esame Big Data, anno 2018-2019

## Team members

 * Federico Naldini: [federico.naldini3@studio.unibo.it](mailto:federico.naldini3@studio.unibo.it)
 
  ## Guida all'utilizzo
Il progetto è importabile e compilabile mediante Gradle, in particolare sono stati disposti due tasks per 
generare due fatjar direttamente eseguibili su piattaforma Hadoop:
  
  *mapReduceJar*
  * Genera un fatjar impostando come classe principale it.unibo.bd1819.Main
  * Esegue le computazioni necessarie alla risoluzione del job utilizzando il paradigma Map-Reduce messo a disposizione da Hadoop.
  * Memorizza i risultati all'indirizzo hdfs::/usesr/fnaldini/mapreduce/output
  * Non è necessario specificare alcun path di ingresso per i dati, la loro posizione è già codificata all'interno del codice
  * Comando per l'esecuzione: hadoop jar director-actor-job-x.x.x-mr.jar  
  
   *sparkJar*
    * Genera un fatjar impostando come classe principale it.unibo.bd1819.ScalaMain
    * Esegue le computazioni necessarie alla risoluzione del job utilizzando i costrutti messi a disposizione da SparkSQL.
    * Memorizza i risultati su Hive nella tabella: fnaldini_director_actors_db.actor_director_table.
    * Non è necessario specificare alcun path di ingresso per i dati, la loro posizione è già codificata all'interno del codice
    * si può specificare il numero di \texttt{executors} con l'argomento --executors= e il numero di tasks per ogni executor con il flag --taskForExceutor=
    * Comando per l'esecuzione: spark2-submit director-actor-job-x.x.x-spark.jar

 
