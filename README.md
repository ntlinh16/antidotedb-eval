# Benchmarking the AntidoteDB cluster using FMKe on Grid5000 system
This project aims to evaluate the performance of an [AntidoteDB](https://www.antidotedb.eu/) cluster which is deployed on Grid5000 system by using the [FMKe benchmark](https://github.com/ntlinh16/FMKe). The systems deployment and the experiments running are handled by using an experiment management tool, [cloudal](https://github.com/ntlinh16/cloudal/).

If you do not install and configure all dependencies to use cloudal, please follow the [instruction](https://github.com/ntlinh16/cloudal#installation)

## Introduction

The flow of this experiment follows [an experiment workflow with cloudal](https://github.com/ntlinh16/cloudal/blob/master/docs/technical_detail.md#an-experiment-workflow-with-cloudal).

The `create_combs_queue()` function creates a list of combinations from the given parameters in the _exp_setting_antidotedb_fmke_g5k_ file which are (1) the number of repetitions of each experiment; (2) the configuration of FMKe populator such as the number of  concurrent populator processes or the database type; (3) the number of concurrent clients connects to the database; (4) the topology (number of antidotedb nodes, number of FMKe_app, number of FMKe_client)

The `setup_env()` function (1) makes a reservation for the required infrastructure; and then (2) deploys a Kubernetes cluster to managed all AntidoteDB and FMKe services which are deployed by using Docker containers.

After we have a ready system due to the `setup_env()` function, the `run_exp_workflow()` function gets one combination (which contains all necessary parameters for one scenario) from the combination queue and performs the 6 following steps to get the result of one specific experiment. With each successful run, a new directory will be created to store the results locally. Whenever a run fails, the combination will be put back to the combination queue for running again later. 

<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/antidotedb-eval/master/images/exp_fmke_antidotedb_workflow.png" width="600"/>
    <br>
<p>
                

## How to run the experiment

### 1. Prepare config files:
There are two types of configuration files to perform this experiment.

#### Setup environment config file
This system configuration file provides three following information:

* Infrastructure requirements: includes the number of clusters, name of cluster and the number of nodes for each cluster you want to provision on Grid5k system; which OS you want to deploy on reserved nodes; when and how long you want to provision nodes; etc.

* Parameters: is a list of experiment parameters that represent different aspects of the system that you want to examine. Each parameter contains a list of possible values of that aspect. For example, I want to examine the effect of the number of concurrent clients that connect to an AntidoteDB database, so I define a parameter such as `concurrent_clients: [16, 32]`; and each experiment will be repeated 10 times (`iteration: [1..10]`) for a statistically significant results.

* Experiment environment information: the path to experiment configuration files; the read/write ratio of the FMKe workload; the topology of an AntidoteDB cluster.

You need to clarify all these information in `exp_setting_antidotedb_fmke_g5k.yaml` file

#### Experiment config files 

In this experiment, I am using Kubernetes deployment files to deploy and manage AntidoteDB cluster, Antidote monitoring services and FMKe benchmark. You need to provide these deployment files. I already provided the template files which work well with this experiment in folder [exp_config_files](https://github.com/ntlinh16/antidotedb-eval/tree/master/exp_config_files). If you do not require any special configurations, you do not have to modify these files.

### 2. Run the experiment

If you are running this experiment on your local machine, remember to run the VPN to [connect to Grid5000 system from outside](https://github.com/ntlinh16/cloudal/blob/master/docs/g5k_k8s_setting.md).

Then, run the following command:

```
cd antidotedb-eval/
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k &> results/test.log
```

You can watch the log by:

```
tail -f antidotedb-eval/results/test.log
```
Depending on how many clusters you are requiring, it might take 35 minutes to 1 hour to fully set up the environment before starting the _run_exp_workflow_ function to execute the combinations.

Arguments:

* `-k`: after finishing all the runs of the experiment, all provisioned nodes on Gris5000 will be kept alive so that you can connect to them, or if the experiment is interrupted in the middle, you can use these provisioned nodes to continue the experiments. This mechanism saves time since you don't have to reserve and deploy nodes again. If you do not use `-k`, when the script is finished or interrupted, all your reserved nodes will be deleted.

### 3. Re-run the experiment
If the script is interrupted by unexpected reasons. You can re-run the experiment and it will continue with the list of combinations left in the queue. You have to provide the same result directory of the previous one. There are two possible cases:

1. If your reserved hosts on Grid5k are dead, you just run the same above command:
```
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k &>> results/test.log
```

2. If your reserved hosts on Grid5k are still alive, you can give the OAR_JOB_IDs to the script:
```
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k -j < site1:oar_job_id1,site2:oar_job_id2,...> --no-deploy-os --kube-master <the host name of the kubernetes master> &> results/test2.log
```
For example:
```
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k --no-deploy-os -j grenoble:2036582,rennes:1817521 --kube-master dahu-9.grenoble.grid5000.fr &>> results/test.log
```

3. If your script is interrupted after the step `Deploying Kubernetes cluster`, the reason maybe you forget to turn on VPN to connect to Grid5000 from your local machine or just a network problem. You can check it and re-run:
```
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k -j < site1:oar_job_id1,site2:oar_job_id2,...> --no-deploy-os --kube-master --setup-k8s-env &>> results/test.log
```
### 4. Run the experiment with the monitoring system:
If you want to use [Grafana](https://grafana.com/) and [Prometheus](https://prometheus.io/) as an AntidoteDB monitoring system during the experiment running, use can use option `--monitoring`:

```
python antidotedb_fmke_g5k.py --system_config_file exp_setting_antidotedb_fmke_g5k.yaml -k --monitoring &>> results/test.log
```

When you use this option, please make sure that you provide the corresponding Kubernetes deployment files (the [monitoring yaml files](https://github.com/ntlinh16/antidotedb-eval/tree/main/exp_config_files/monitoring_yaml)). You can connect to the url provided in the log file (`results/test.log`) to access the monitoring UI (i.e., `http://<kube_master_ip>:3000`). The default account credential is `admin/admin`. When login successfully, you can search for `Antidote` to access the pre-defined AntidoteDB dashboard.
<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/antidotedb-eval/master/images/grafana_monitoring.png" 
    width="650"/>
    <br>
<p>

### 5. Some Experiments Results 

#### 5.1. Writing performance when increasing the number of Antidotedb nodes in 1 DC

The system setting are:
- n_DC = 1
- n_antidotedc = 1, 3, 5, 7, 9
- n_FMKe_app = n_antidotedc (if we have 5 antidotedc nodes, we deploy 5 FMKe app nodes)
- FMKe populator: number of processes = 100 (as default), dataset: small (1900 entities)
- scenario: we measure the ops/s when populating the small dataset to Antidote cluster

<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/antidotedb-eval/master/images/1DC_population_pos_1-9nodes_parasilo_p-100.png"
    width="500"/>
    <br>
<p>

To plot the results of the experiment that use `fmke_pop` to populate the database to the system, you can use the Python notebook file in [plot_chart](https://github.com/ntlinh16/antidotedb-eval/tree/main/plot_chart) folder.
#### 5.2. Performance of multiple Antidote DCs

<p align="center">
    <br>
    <img src="https://raw.githubusercontent.com/ntlinh16/antidotedb-eval/master/images/2DCS_summary.png"
    width="550"/>
    <br>
<p>

To plot the above summary Figure (e.g, the factors such as throughput or latencies of the elapsed time) of the experiment that use `fmke_client` to stress the system, you should follow the [fmke_plot](https://github.com/ntlinh16/fmke_plot.git) instruction.
## Docker images used in these experiments

I use Docker images to pre-build the environment for FMKe services. All images are on Docker repository.

To deploy AntidoteDB cluster:

* **antidotedb/antidote:latest**
* **peterzel/antidote-connect**

To deploy FMKe benchmark:

* **ntlinh/fmke**
* **ntlinh/fmke_pop**
* **ntlinh/fmke_client**