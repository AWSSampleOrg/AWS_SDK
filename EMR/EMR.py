# -*- encoding: utf-8 -*-
import boto3

#EMR
emr = boto3.client("emr")

#####################################
#   show cluster list
#####################################
cluster_list = emr.list_clusters(
    ClusterStates = ["STARTING","BOOTSTRAPPING","RUNNING","WAITING"]
)


#####################################
#   Set up Cluster
#####################################
params = {
    #https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.run_job_flow
}
emr.run_job_flow(**params)


#####################################
#   describe cluster
#####################################
emr.describe_cluster(
    ClusterId = "The identifier of the cluster to describe."
)


#####################################
#   Add steps
#####################################
emr.add_job_flow_steps(
    JobFlowId = "A string that uniquely identifies the job flow. This identifier is returned by RunJobFlow and can also be obtained from ListClusters .",
    Steps = [{
    	#https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/emr.html#EMR.Client.add_job_flow_steps
	}]
)
