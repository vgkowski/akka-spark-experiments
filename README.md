# akka-spark-experiments
Simple experiment in combining Akka HTTP, actors and Spark to run concurrent subsecond and dynamic Spark jobs

Example of an application offerig a REST API to request on demand analytics on call data records:
* topk location: calculate the topK location of an MSISDN for a time period, a usage (SMS or VOICE) and a way (IN or OUT)
* usage KPIs: calculate common telco KPIs like the number of distinct peer, average call duration... for the same parameters
