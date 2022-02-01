import os
import shutil
import traceback
import re
import requests

from time import sleep


from cloudal.utils import get_logger, execute_cmd, parse_config_file, getput_file, ExecuteCommandException
from cloudal.action import performing_actions
from cloudal.provisioner import ovh_provisioner
from cloudal.configurator import kubernetes_configurator, k8s_resources_configurator
from cloudal.experimenter import create_paramsweeper, define_parameters, get_results

from execo_engine import slugify
from kubernetes import config
import yaml

logger = get_logger()


class CancelCombException(Exception):
    pass


class FMKe_antidotedb_ovh(performing_actions):
    def __init__(self):
        super(FMKe_antidotedb_ovh, self).__init__()
        self.args_parser.add_argument("--node_ids_file", dest="node_ids_file",
                                      help="the path to the file contents list of node IDs",
                                      default=None,
                                      type=str)
        self.args_parser.add_argument("--kube_master", dest="kube_master",
                                      help="name of kube master node",
                                      default=None,
                                      type=str)
        self.args_parser.add_argument("--setup-k8s-env", dest="setup_k8s_env",
                                      help="create namespace, setup label and volume for kube_workers for the experiment environment",
                                      action="store_true")
        self.args_parser.add_argument("--monitoring", dest="monitoring",
                                      help="deploy Grafana and Prometheus for monitoring",
                                      action="store_true")
        self.args_parser.add_argument("--attach_volume", dest="attach_volume",
                                      help="attach an external volume to every data node",
                                      action="store_true")

    def save_results(self, comb, pop_time, pop_error):
        logger.info("----------------------------------")
        logger.info("6. Starting dowloading the results")

        configurator = k8s_resources_configurator()
        results_nodes_info = configurator.get_k8s_resources(resource='node',
                                                            label_selectors='service_ovh=fmke')

        results_nodes = [r.metadata.annotations['flannel.alpha.coreos.com/public-ip']
                         for r in results_nodes_info.items]

        comb_dir = get_results(comb=comb,
                               hosts=results_nodes,
                               remote_result_files=['/tmp/results/*'],
                               local_result_dir=self.configs['exp_env']['results_dir'])

        with open(os.path.join(comb_dir, 'pop_time.txt'), 'w') as f:
            f.write(pop_time)
        with open(os.path.join(comb_dir, 'pop_error.txt'), 'w') as f:
            f.write(str(pop_error))

        logger.info("Finish dowloading the results")

    def deploy_fmke_client(self, kube_namespace, comb):
        logger.info('-----------------------------------------------------------------')
        logger.info('5. Starting deploying FMKe client')
        fmke_client_k8s_dir = self.configs['exp_env']['fmke_yaml_path']

        logger.debug('Delete old k8s yaml files if exists')
        for filename in os.listdir(fmke_client_k8s_dir):
            if filename.startswith('create_fmke_client_') or filename.startswith('fmke_client_'):
                if '.template' not in filename:
                    try:
                        os.remove(os.path.join(fmke_client_k8s_dir, filename))
                    except OSError:
                        logger.debug("Error while deleting file")

        test_duration = self.configs['exp_env']['test_duration']

        logger.debug('Create the new workload ratio')
        workload = ",\n".join(["  {%s, %s}" % (key, val)
                               for key, val in self.configs['exp_env']['operations'].items()])
        operations = "{operations,[\n%s\n]}." % workload

        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        fmke_list = configurator.get_k8s_resources(resource='pod',
                                                   label_selectors='app=fmke',
                                                   kube_namespace=kube_namespace)

        fmke_client_files = list()
        config_file_path = os.path.join(fmke_client_k8s_dir, 'fmke_client.config.template')
        create_file_path = os.path.join(fmke_client_k8s_dir, 'create_fmke_client.yaml.template')
        for fmke in fmke_list.items:
            node = fmke.spec.node_name
            # Modify fmke_client config files with new values
            logger.debug('Create fmke_client config files to stress database for each Antidote DC')
            with open(config_file_path) as f:
                doc = f.read()
                doc = doc.replace('127.0.0.1', '%s' % fmke.status.pod_ip)
                doc = doc.replace("{concurrent, 16}.", "{concurrent, %s}." %
                                  comb['concurrent_clients'])
                doc = doc.replace("{duration, 3}.", "{duration, %s}." % test_duration)
                doc = doc.replace("'", '"')
                doc = re.sub(r"{operations.*", operations, doc, flags=re.S)
            file_path = os.path.join(fmke_client_k8s_dir, 'fmke_client_%s.config' % node)
            with open(file_path, 'w') as f:
                f.write(doc)
            logger.debug('Create fmke_client folder on each fmke_client node')
            cmd = 'mkdir -p /tmp/fmke_client'
            execute_cmd(cmd, fmke.status.host_ip)
            logger.debug(
                'Upload fmke_client config files to kube_master to be used by kubectl to run fmke_client pods')
            getput_file(hosts=fmke.status.host_ip, file_paths=[
                        file_path], dest_location='/tmp/fmke_client/', action='put')

            logger.debug('Create create_fmke_client.yaml files to deploy one FMKe client')
            with open(create_file_path) as f:
                doc = yaml.safe_load(f)
            doc['metadata']['name'] = 'fmke-client-%s' % node
            doc['spec']['template']['spec']['containers'][0]['lifecycle']['postStart']['exec']['command'] = [
                "cp", "/cluster_node/fmke_client_%s.config" % node, "/fmke_client/fmke_client.config"]
            doc['spec']['template']['spec']['nodeSelector'] = {
                'service_ovh': 'fmke', 'kubernetes.io/hostname': '%s' % node}
            file_path = os.path.join(fmke_client_k8s_dir, 'create_fmke_client_%s.yaml' % node)
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            fmke_client_files.append(file_path)

        logger.info("Starting FMKe client instances on each Antidote DC")
        configurator.deploy_k8s_resources(files=fmke_client_files, namespace=kube_namespace)
        sleep(20)
        logger.info("Checking if deploying enough the number of running FMKe_client or not")
        fmke_client_list = configurator.get_k8s_resources_name(resource='pod',
                                                               label_selectors='app=fmke-client',
                                                               kube_namespace=kube_namespace)
        if len(fmke_client_list) != comb['n_fmke_client_per_dc'] * len(self.configs['exp_env']['clusters']):
            logger.info("n_fmke_client = %s, n_deployed_fmke_client = %s" %
                        (comb['n_fmke_client_per_dc']*len(self.configs['exp_env']['clusters']), len(fmke_client_list)))
            raise CancelCombException("Cannot deploy enough FMKe_client")

        logger.info("Stressing database in %s minutes ....." % test_duration)
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors="app=fmke-client",
                                                    timeout=(test_duration + 5)*60,
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            logger.error("Cannot wait until all FMKe client instances running completely")
            raise CancelCombException(
                "Cannot wait until all FMKe client instances running completely")

        logger.info("Finish stressing Antidote database")

    def deploy_fmke_app(self, kube_namespace, comb):
        logger.info('------------------------------------')
        logger.info('3. Starting deploying FMKe application')
        fmke_k8s_dir = self.configs['exp_env']['fmke_yaml_path']

        logger.debug('Delete old deployment files')
        for filename in os.listdir(fmke_k8s_dir):
            if '.template' not in filename:
                try:
                    os.remove(os.path.join(fmke_k8s_dir, filename))
                except OSError:
                    logger.debug("Error while deleting file")

        logger.debug('Create headless service file')
        file1 = os.path.join(fmke_k8s_dir, 'headlessService.yaml.template')
        file2 = os.path.join(fmke_k8s_dir, 'headlessService.yaml')
        shutil.copyfile(file1, file2)

        logger.debug('Create FMKe statefulSet files for each DC')
        file_path = os.path.join(fmke_k8s_dir, 'statefulSet_fmke.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)

        for i in range(1, 11):
            if 2 ** i > comb['concurrent_clients']:
                connection_pool_size = 2 ** i
                break
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        service_list = configurator.get_k8s_resources(resource='service',
                                                      label_selectors='app=antidote,type=exposer-service',
                                                      kube_namespace=kube_namespace)
        for cluster in self.configs['exp_env']['clusters']:
            # Get IP of antidote DC exposer service for each cluster
            for service in service_list.items:
                if cluster.lower() in service.metadata.name:
                    ip = service.spec.cluster_ip
            doc['spec']['replicas'] = comb['n_fmke_client_per_dc']
            doc['metadata']['name'] = 'fmke-%s' % cluster.lower()
            doc['spec']['template']['spec']['containers'][0]['env'] = [
                {'name': 'DATABASE_ADDRESSES', 'value': ip},
                {'name': 'CONNECTION_POOL_SIZE', 'value': '%s' % connection_pool_size}]
            doc['spec']['template']['spec']['nodeSelector'] = {
                'service_ovh': 'fmke', 'cluster_ovh': '%s' % cluster}
            file_path = os.path.join(fmke_k8s_dir, 'statefulSet_fmke_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)

        logger.info("Starting FMKe instances on each Antidote DC")
        configurator.deploy_k8s_resources(path=fmke_k8s_dir, namespace=kube_namespace)

        logger.info('Waiting until all fmke app instances are up')
        deploy_ok = configurator.wait_k8s_resources(resource='pod',
                                                    label_selectors="app=fmke",
                                                    timeout=600,
                                                    kube_namespace=kube_namespace)

        if not deploy_ok:
            raise CancelCombException("Cannot wait until all fmke app instances are up")
        logger.info("Checking if FMKe_app deployed correctly")
        fmke_app_list = configurator.get_k8s_resources_name(resource='pod',
                                                            label_selectors='app=fmke',
                                                            kube_namespace=kube_namespace)
        if len(fmke_app_list) != comb['n_fmke_client_per_dc'] * len(self.configs['exp_env']['clusters']):
            logger.info("n_fmke_app = %s, n_deployed_fmke_app = %s" %
                        (comb['n_fmke_client_per_dc']*len(self.configs['exp_env']['clusters']), len(fmke_app_list)))
            raise CancelCombException("Cannot deploy enough FMKe_app")

        logger.info('Finish deploying FMKe benchmark')

    def deploy_fmke_pop(self, kube_namespace, comb):
        logger.info('---------------------------')
        logger.info('4. Starting deploying FMKe populator')
        fmke_k8s_dir = self.configs['exp_env']['fmke_yaml_path']

        logger.debug('Modify the populate_data template file')
        configurator = k8s_resources_configurator()
        fmke_list = configurator.get_k8s_resources(resource='pod',
                                                   label_selectors='app=fmke',
                                                   kube_namespace=kube_namespace)
        fmke_IPs = list()
        for cluster in self.configs['exp_env']['clusters']:
            for fmke in fmke_list.items:
                if cluster.lower() in fmke.metadata.name:
                    fmke_IPs.append('fmke@%s' % fmke.status.pod_ip)
        with open(os.path.join(fmke_k8s_dir, 'populate_data.yaml.template')) as f:
            doc = yaml.safe_load(f)
        doc['metadata']['name'] = 'populate-data-without-prescriptions'
        doc['spec']['template']['spec']['containers'][0]['args'] = ['-f -d %s --noprescriptions -p %s' %
                                                                    (comb['dataset'], comb['n_fmke_pop_process'])] + fmke_IPs
        with open(os.path.join(fmke_k8s_dir, 'populate_data.yaml'), 'w') as f:
            yaml.safe_dump(doc, f)

        logger.info("Populating the FMKe benchmark data without prescriptions")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        configurator.deploy_k8s_resources(files=[os.path.join(fmke_k8s_dir, 'populate_data.yaml')],
                                          namespace=kube_namespace)

        logger.info('Waiting for populating data without prescriptions')
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors="app=fmke_pop",
                                                    timeout=3000,
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            raise CancelCombException("Cannot wait until finishing populating data")

        logger.info('Checking if the populating process finished successfully or not')
        fmke_pop_pods = configurator.get_k8s_resources_name(resource='pod',
                                                            label_selectors='job-name=populate-data-without-prescriptions',
                                                            kube_namespace=kube_namespace)
        logger.debug('FMKe pod name: %s' % fmke_pop_pods[0])
        pop_result = dict()
        if len(fmke_pop_pods) > 0:
            log = configurator.get_k8s_pod_log(
                pod_name=fmke_pop_pods[0], kube_namespace=kube_namespace)
            last_line = log.strip().split('\n')[-1]
            logger.info('Last line of log: %s' % last_line)
            if 'Populated' in last_line and 'entities in' in last_line and 'avg' in last_line:
                result = log.strip().split('\n')[-1].split(' ')
                if len(result) == 8:
                    pop_result = result[4] + "\n" + result[6]
                if len(result) == 9:
                    pop_result = result[4] + "\n" + result[7]
                t = 5
                logger.info(
                    'Waiting %s minutes for the replication and key distribution mechanisms between DCs' % t)
                sleep(t*60)
            else:
                raise CancelCombException("Populating process ERROR")
            logger.debug("FMKe populator result: \n%s" % pop_result)

        logger.debug('Modify the populate_data file to populate prescriptions')
        with open(os.path.join(fmke_k8s_dir, 'populate_data.yaml.template')) as f:
            doc = yaml.safe_load(f)
        doc['metadata']['name'] = 'populate-data-with-onlyprescriptions'
        doc['spec']['template']['spec']['containers'][0]['args'] = [
            '-f --onlyprescriptions -p 1'] + fmke_IPs
        with open(os.path.join(fmke_k8s_dir, 'populate_data.yaml'), 'w') as f:
            yaml.safe_dump(doc, f)

        logger.info("Populating the FMKe benchmark data with prescriptions")
        configurator.deploy_k8s_resources(files=[os.path.join(fmke_k8s_dir, 'populate_data.yaml')],
                                          namespace=kube_namespace)

        logger.info('Waiting for populating data with prescriptions')
        configurator.wait_k8s_resources(resource='job',
                                        label_selectors="app=fmke_pop",
                                        timeout=3000,
                                        kube_namespace=kube_namespace)
        logger.info('Checking if the populating process finished successfully or not')
        fmke_pop_pods = configurator.get_k8s_resources_name(resource='pod',
                                                            label_selectors='job-name=populate-data-with-onlyprescriptions',
                                                            kube_namespace=kube_namespace)
        logger.info('FMKe pod: %s' % fmke_pop_pods[0])
        if len(fmke_pop_pods) > 0:
            log = configurator.get_k8s_pod_log(
                pod_name=fmke_pop_pods[0], kube_namespace=kube_namespace)
            last_line = log.strip().split('\n')[-1]
            logger.info('Last line of log: %s' % last_line)
            if 'Populated' not in last_line:
                raise CancelCombException("Populating process ERROR")
            t = 5
            logger.info(
                'Waiting %s minutes for the replication and key distribution mechanisms between DCs' % t)
            sleep(t*60)
        logger.info('Finish populating data')

        return pop_result

    def _calculate_ring_size(self, n_nodes):
        # calculate the ring size base on the number of nodes in a DC
        # this setting follows the recomandation of Riak KV here:
        # https://docs.riak.com/riak/kv/latest/setup/planning/cluster-capacity/index.html#ring-size-number-of-partitions
        if n_nodes < 7:
            return 64
        elif n_nodes < 10:
            return 128
        elif n_nodes < 14:
            return 256
        elif n_nodes < 20:
            return 512
        elif n_nodes < 40:
            return 1024
        return 2048

    def deploy_antidote(self, kube_namespace, comb):
        logger.info('--------------------------------------')
        logger.info('2. Starting deploying Antidote cluster')
        antidote_k8s_dir = self.configs['exp_env']['antidote_yaml_path']

        logger.debug('Delete old createDC, connectDCs_antidote and exposer-service files if exists')
        for filename in os.listdir(antidote_k8s_dir):
            if filename.startswith('createDC_') or filename.startswith('statefulSet_') or filename.startswith('exposer-service_') or filename.startswith('connectDCs_antidote'):
                if '.template' not in filename:
                    try:
                        os.remove(os.path.join(antidote_k8s_dir, filename))
                    except OSError:
                        logger.debug("Error while deleting file")

        statefulSet_files = [os.path.join(antidote_k8s_dir, 'headlessService.yaml')]
        logger.debug('Modify the statefulSet file')

        file_path = os.path.join(antidote_k8s_dir, 'statefulSet.yaml.template')

        ring_size = self._calculate_ring_size(comb['n_antidotedb_per_dc'])
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster in self.configs['exp_env']['clusters']:
            doc['spec']['replicas'] = comb["n_antidotedb_per_dc"]
            doc['metadata']['name'] = 'antidote-%s' % cluster.lower()
            doc['spec']['template']['spec']['nodeSelector'] = {
                'service_ovh': 'antidote', 'cluster_ovh': '%s' % cluster}
            envs = doc['spec']['template']['spec']['containers'][0]['env']
            for env in envs:
                if env.get('name') == "RING_SIZE":
                    env['value'] = str(ring_size)
                    break
            file_path = os.path.join(antidote_k8s_dir, 'statefulSet_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            statefulSet_files.append(file_path)

        logger.info("Starting AntidoteDB instances")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        configurator.deploy_k8s_resources(files=statefulSet_files, namespace=kube_namespace)

        logger.info('Waiting until all Antidote instances are up')
        deploy_ok = configurator.wait_k8s_resources(resource='pod',
                                                    label_selectors="app=antidote",
                                                    timeout=600,
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            raise CancelCombException("Cannot deploy enough Antidotedb instances")

        logger.debug('Creating createDc.yaml file for each Antidote DC')
        dcs = dict()
        for cluster in self.configs['exp_env']['clusters']:
            dcs[cluster.lower()] = list()
        antidote_list = configurator.get_k8s_resources_name(resource='pod',
                                                            label_selectors='app=antidote',
                                                            kube_namespace=kube_namespace)
        logger.info("Checking if AntidoteDB are deployed correctly")
        if len(antidote_list) != comb['n_antidotedb_per_dc']*len(self.configs['exp_env']['clusters']):
            logger.info("n_antidotedb = %s, n_deployed_fmke_app = %s" %
                        (comb['n_antidotedb_per_dc']*len(self.configs['exp_env']['clusters']), len(antidote_list)))
            raise CancelCombException("Cannot deploy enough Antidotedb instances")

        for antidote in antidote_list:
            cluster = antidote.split('-')[1].strip()
            dcs[cluster].append(antidote)

        file_path = os.path.join(antidote_k8s_dir, 'createDC.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)

        antidote_masters = list()
        createdc_files = list()
        for cluster, pods in dcs.items():
            doc['spec']['template']['spec']['containers'][0]['args'] = ['--createDc',
                                                                        '%s.antidote:8087' % pods[0]] + ['antidote@%s.antidote' % pod for pod in pods]
            doc['metadata']['name'] = 'createdc-%s' % cluster.lower()
            antidote_masters.append('%s.antidote:8087' % pods[0])
            file_path = os.path.join(antidote_k8s_dir, 'createDC_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            createdc_files.append(file_path)

        logger.debug('Creating exposer-service.yaml files')
        file_path = os.path.join(antidote_k8s_dir, 'exposer-service.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        for cluster, pods in dcs.items():
            doc['spec']['selector']['statefulset.kubernetes.io/pod-name'] = pods[0]
            doc['metadata']['name'] = 'antidote-exposer-%s' % cluster.lower()
            file_path = os.path.join(antidote_k8s_dir, 'exposer-service_%s.yaml' % cluster.lower())
            with open(file_path, 'w') as f:
                yaml.safe_dump(doc, f)
            createdc_files.append(file_path)

        logger.info("Creating Antidote DCs and exposing services")
        configurator.deploy_k8s_resources(files=createdc_files, namespace=kube_namespace)

        logger.info('Waiting until all antidote DCs are created')
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors='app=antidote',
                                                    kube_namespace=kube_namespace)

        if not deploy_ok:
            raise CancelCombException("Cannot connect Antidotedb instances to create DC")

        logger.debug('Creating connectDCs_antidote.yaml to connect all Antidote DCs')
        file_path = os.path.join(antidote_k8s_dir, 'connectDCs.yaml.template')
        with open(file_path) as f:
            doc = yaml.safe_load(f)
        doc['spec']['template']['spec']['containers'][0]['args'] = [
            '--connectDcs'] + antidote_masters
        file_path = os.path.join(antidote_k8s_dir, 'connectDCs_antidote.yaml')
        with open(file_path, 'w') as f:
            yaml.safe_dump(doc, f)

        logger.info("Connecting all Antidote DCs into a cluster")
        configurator.deploy_k8s_resources(files=[file_path], namespace=kube_namespace)

        logger.info('Waiting until connecting all Antidote DCs')
        deploy_ok = configurator.wait_k8s_resources(resource='job',
                                                    label_selectors='app=antidote',
                                                    kube_namespace=kube_namespace)
        if not deploy_ok:
            raise CancelCombException("Cannot connect all Antidotedb DCs")

        logger.info('Finish deploying the Antidote cluster')

    def deploy_antidote_monitoring(self, kube_master, kube_namespace):
        logger.info('--------------------------------------')
        logger.info("Deploying monitoring system")
        monitoring_k8s_dir = self.configs['exp_env']['monitoring_yaml_path']

        logger.info("Deleting old deployment")
        cmd = "rm -rf /root/antidote_stats"
        execute_cmd(cmd, kube_master)

        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()

        cmd = "git clone https://github.com/AntidoteDB/antidote_stats.git"
        execute_cmd(cmd, kube_master)
        logger.info("Setting to allow pods created on kube_master")
        cmd = "kubectl taint nodes --all node-role.kubernetes.io/master-"
        execute_cmd(cmd, kube_master, is_continue=True)

        pods = configurator.get_k8s_resources_name(resource='pod',
                                                   label_selectors='app=antidote',
                                                   kube_namespace=kube_namespace)
        antidote_info = ["%s.antidote:3001" % pod for pod in pods]

        logger.debug('Modify the prometheus.yml file with antidote instances info')
        file_path = os.path.join(monitoring_k8s_dir, 'prometheus.yml.template')
        with open(file_path) as f:
            doc = f.read().replace('antidotedc_info', '%s' % antidote_info)
        prometheus_configmap_file = os.path.join(monitoring_k8s_dir, 'prometheus.yml')
        with open(prometheus_configmap_file, 'w') as f:
            f.write(doc)
        configurator.create_configmap(file=prometheus_configmap_file,
                                      namespace=kube_namespace,
                                      configmap_name='prometheus-configmap')
        logger.debug('Modify the deploy_prometheus.yaml file with kube_master info')
        kube_master_info = configurator.get_k8s_resources(resource='node',
                                                          label_selectors='kubernetes.io/hostname=%s' % kube_master)
        for item in kube_master_info.items[0].status.addresses:
            if item.type == 'InternalIP':
                kube_master_ip = item.address
        file_path = os.path.join(monitoring_k8s_dir, 'deploy_prometheus.yaml.template')
        with open(file_path) as f:
            doc = f.read().replace('kube_master_ip', '%s' % kube_master_ip)
            doc = doc.replace("kube_master_hostname", '%s' % kube_master)
        prometheus_deploy_file = os.path.join(monitoring_k8s_dir, 'deploy_prometheus.yaml')
        with open(prometheus_deploy_file, 'w') as f:
            f.write(doc)

        logger.info("Starting Prometheus service")
        configurator.deploy_k8s_resources(files=[prometheus_deploy_file], namespace=kube_namespace)
        logger.info('Waiting until Prometheus instance is up')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app=prometheus",
                                        kube_namespace=kube_namespace)

        logger.debug('Modify the deploy_grafana.yaml file with kube_master info')
        file_path = os.path.join(monitoring_k8s_dir, 'deploy_grafana.yaml.template')
        with open(file_path) as f:
            doc = f.read().replace('kube_master_ip', '%s' % kube_master_ip)
            doc = doc.replace("kube_master_hostname", '%s' % kube_master)
        grafana_deploy_file = os.path.join(monitoring_k8s_dir, 'deploy_grafana.yaml')
        with open(grafana_deploy_file, 'w') as f:
            f.write(doc)

        file = '/root/antidote_stats/monitoring/grafana-config/provisioning/datasources/all.yml'
        cmd = """ sed -i "s/localhost/%s/" %s """ % (kube_master_ip, file)
        execute_cmd(cmd, kube_master)

        logger.info("Starting Grafana service")
        configurator.deploy_k8s_resources(files=[grafana_deploy_file], namespace=kube_namespace)
        logger.info('Waiting until Grafana instance is up')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app=grafana",
                                        kube_namespace=kube_namespace)

        logger.info("Finish deploying monitoring system\n")
        prometheus_url = "http://%s:9090" % kube_master_ip
        grafana_url = "http://%s:3000" % kube_master_ip
        logger.info("Connect to Grafana at: %s" % grafana_url)
        logger.info("Connect to Prometheus at: %s" % prometheus_url)

        return prometheus_url, grafana_url

    def get_prometheus_metric(self, metric_name, prometheus_url):
        logger.info('---------------------------')
        logger.info('Retrieving Prometheus data')
        query = "%s/api/v1/query?query=%s" % (prometheus_url, metric_name)
        logger.debug("query = %s" % query)
        r = requests.get(query)
        normalize_result = dict()
        logger.debug("status_code = %s" % r.status_code)
        if r.status_code == 200:
            result = r.json()
            for each in result['data']['result']:
                key = each['metric']['instance']
                normalize_result[key] = int(each['value'][1])
        return normalize_result

    def clean_k8s_resources(self, kube_namespace, n_fmke_client_per_dc):
        logger.info('1. Deleting all k8s resource from the previous run in namespace "%s"' %
                    kube_namespace)
        logger.info(
            'Delete namespace "%s" to delete all the resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

        if n_fmke_client_per_dc > 0:
            logger.debug('Delete all files in /tmp/results folder on fmke_client nodes')
            results_nodes_info = configurator.get_k8s_resources(resource='node',
                                                                label_selectors='service_ovh=fmke',
                                                                kube_namespace=kube_namespace)
            results_nodes = [r.metadata.annotations['flannel.alpha.coreos.com/public-ip']
                             for r in results_nodes_info.items]
            cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
            execute_cmd(cmd, results_nodes)

    def run_exp_workflow(self, kube_namespace, comb, kube_master, sweeper):
        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_k8s_resources(kube_namespace, comb['n_fmke_client_per_dc'])
            self.deploy_antidote(kube_namespace, comb)
            if self.args.monitoring:
                prometheus_url, _ = self.deploy_antidote_monitoring(kube_master, kube_namespace)
            self.deploy_fmke_app(kube_namespace, comb)
            pop_result = self.deploy_fmke_pop(kube_namespace, comb)
            metric_result = None
            if self.args.monitoring:
                metric_result = self.get_prometheus_metric("antidote_error_count", prometheus_url)
                metric_result = sum(metric_result.values())
                logger.info("Total ops errors: %s" % metric_result)
            if comb['n_fmke_client_per_dc'] > 0:
                self.deploy_fmke_client(kube_namespace, comb)
                self.save_results(comb, pop_result, metric_result)
            else:
                self.save_results_poptime(comb, pop_result, metric_result)
            comb_ok = True
        except (ExecuteCommandException, CancelCombException) as e:
            logger.error('Combination exception: %s' % e)
            comb_ok = False
        finally:
            if comb_ok:
                sweeper.done(comb)
                logger.info('Finish combination: %s' % slugify(comb))
            else:
                sweeper.cancel(comb)
                logger.warning(slugify(comb) + ' is canceled')
            logger.info('%s combinations remaining\n' % len(sweeper.get_remaining()))
        return sweeper

    def _setup_ovh_kube_volumes(self, kube_workers, n_pv=3):
        logger.info("Setting volumes on %s kubernetes workers" % len(kube_workers))
        # cmd = '''umount /dev/sdb;
        #          mount -t ext4 /dev/sdb /tmp;
        #          chmod 777 /tmp'''
        # execute_cmd(cmd, kube_workers)
        cmd = '''rm -rf /rmp/pv'''
        execute_cmd(cmd, kube_workers)
        logger.debug('Create n_pv partitions on the physical disk to make a PV can be shared')
        cmd = '''for i in $(seq 1 %s); do
                     mkdir -p /tmp/pv/vol${i}
                     mkdir -p /mnt/disks/vol${i}
                     mount --bind /tmp/pv/vol${i} /mnt/disks/vol${i}
                 done''' % n_pv
        execute_cmd(cmd, kube_workers)

        logger.info("Creating local persistance volumes on Kubernetes cluster")
        logger.debug("Init configurator: k8s_resources_configurator")
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs['exp_env']['antidote_yaml_path']
        deploy_files = [os.path.join(antidote_k8s_dir, 'local_persistentvolume.yaml'),
                        os.path.join(antidote_k8s_dir, 'storageClass.yaml')]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info('Waiting for setting local persistance volumes')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors="app.kubernetes.io/instance=local-volume-provisioner")

    def _set_kube_workers_label(self, kube_master):
        configurator = k8s_resources_configurator()
        for node in self.nodes:
            if node['ipAddresses'][0]['ip'] == kube_master:
                pass
            else:
                configurator.set_labels_node(nodename=node['name'],
                                             labels='cluster_ovh=%s' % node['region'])
                if node['id'] in self.data_node_ids:
                    configurator.set_labels_node(nodename=node['name'],
                                                 labels='service_ovh=antidote')
                else:
                    configurator.set_labels_node(nodename=node['name'],
                                                 labels='service_ovh=fmke')

    def _get_credential(self, kube_master):
        home = os.path.expanduser('~')
        kube_dir = os.path.join(home, '.kube')
        if not os.path.exists(kube_dir):
            os.mkdir(kube_dir)
        getput_file(hosts=[kube_master],
                    file_paths=['~/.kube/config'],
                    dest_location=kube_dir,
                    action='get')
        kube_config_file = os.path.join(kube_dir, 'config')
        config.load_kube_config(config_file=kube_config_file)
        logger.info('Kubernetes config file is stored at: %s' % kube_config_file)

    def deploy_k8s(self, kube_master):
        logger.debug("Init configurator: kubernetes_configurator")
        configurator = kubernetes_configurator(hosts=self.hosts, kube_master=kube_master)
        _, kube_workers = configurator.deploy_kubernetes_cluster()

        return kube_workers

    def setup_k8s_env(self, kube_master, kube_namespace, kube_workers):
        self._get_credential(kube_master)

        logger.info('Create k8s namespace "%s" for this experiment' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.create_namespace(namespace=kube_namespace)

        logger.info('Set labels for all kubernetes workers')
        self._set_kube_workers_label(kube_master)

        self._setup_ovh_kube_volumes(kube_workers, n_pv=3)

        logger.info("Finish deploying the Kubernetes cluster")

    def config_host(self, kube_master, kube_namespace):
        logger.info("Starting configuring nodes")
        kube_workers = [host for host in self.hosts if host != kube_master]
        if self.args.kube_master is None:
            kube_workers = self.deploy_k8s(kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        elif self.args.setup_k8s_env:
            logger.info('Kubernetes master: %s' % kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        else:
            self._get_credential(kube_master)
            if self.args.attach_volume:
                self._setup_ovh_kube_volumes(kube_workers, n_pv=3)

        logger.info("Finish configuring nodes")
        return kube_master

    def setup_env(self, kube_master_site, kube_namespace):
        logger.info("STARTING SETTING THE EXPERIMENT ENVIRONMENT")
        logger.info("Starting provisioning nodes on OVHCloud")

        provisioner = ovh_provisioner(configs=self.configs, node_ids_file=self.args.node_ids_file)
        provisioner.provisioning()

        self.nodes = provisioner.nodes
        self.hosts = provisioner.hosts
        node_ids_file = provisioner.node_ids_file

        kube_master = self.args.kube_master
        if kube_master is None:
            for node in self.nodes:
                if node['region'] == kube_master_site:
                    kube_master = node['ipAddresses'][0]['ip']
                    kube_master_id = node['id']
                    break
        else:
            for node in self.nodes:
                if node['ipAddresses'][0]['ip'] == kube_master:
                    kube_master_id = node['id']
                    break
        logger.info('Kubernetes master: %s' % kube_master)

        data_nodes = list()
        clusters = dict()
        for node in self.nodes:
            if node['id'] == kube_master_id:
                continue
            cluster = node['region']
            clusters[cluster] = [node] + clusters.get(cluster, list())
        for region, nodes in clusters.items():
            data_nodes += nodes[0: max(self.normalized_parameters['n_antidotedb_per_dc'])]
        data_hosts = [node['ipAddresses'][0]['ip'] for node in data_nodes]
        self.data_node_ids = [node['id'] for node in data_nodes]

        if self.args.attach_volume:
            logger.info('Attaching external volumes to %s nodes' % len(data_nodes))
            provisioner.attach_volume(nodes=data_nodes)

            logger.info('Formatting the new external volumes')
            cmd = '''disk=$(ls -lt /dev/ | grep '^b' | head -n 1 | awk {'print $NF'})
                   mkfs.ext4 -F /dev/$disk;
                   mount -t ext4 /dev/$disk /tmp;
                   chmod 777 /tmp'''
            execute_cmd(cmd, data_hosts)

        self.config_host(kube_master, kube_namespace)

        logger.info("FINISH SETTING THE EXPERIMENT ENVIRONMENT\n")
        return kube_master, node_ids_file

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = (max(
            self.normalized_parameters['n_fmke_client_per_dc']) + max(self.normalized_parameters['n_antidotedb_per_dc']))

        # set dataset and n_fmke_pop_process to default in case not provided
        if 'dataset' not in self.normalized_parameters:
            self.normalized_parameters['dataset'] = 'standard'

        if 'n_fmke_pop_process' not in self.normalized_parameters:
            self.normalized_parameters['n_fmke_pop_process'] = 100

        # create standard cluster information to make reservation on OVHCloud, this info using by OVH provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            if cluster == kube_master_site:
                clusters.append({'region': cluster,
                                'n_nodes': n_nodes_per_cluster + 1,
                                 'instance_type': self.configs['instance_type'],
                                 'flexible_instance': self.configs['flexible_instance'],
                                 'image': self.configs['image']})
            else:
                clusters.append({'region': cluster,
                                'n_nodes': n_nodes_per_cluster,
                                 'instance_type': self.configs['instance_type'],
                                 'flexible_instance': self.configs['flexible_instance'],
                                 'image': self.configs['image']})
        self.configs['clusters'] = clusters

        # copy all YAML template folders to a new one for this experiment run to avoid conflicting
        results_dir_name = (self.configs["exp_env"]["results_dir"]).split('/')[-1]
        results_dir_path = os.path.dirname(self.configs["exp_env"]["results_dir"])

        yaml_dir_path = os.path.dirname(self.configs["exp_env"]["antidote_yaml_path"])
        yaml_dir_name = yaml_dir_path.split('/')[-1]

        new_yaml_dir_name = yaml_dir_name + "_" + results_dir_name
        new_path = results_dir_path + "/" + new_yaml_dir_name
        if os.path.exists(new_path):
            shutil.rmtree(new_path)
        shutil.copytree(yaml_dir_path, new_path)

        self.configs["exp_env"]["antidote_yaml_path"] = new_path + "/antidotedb_yaml"
        self.configs["exp_env"]["monitoring_yaml_path"] = new_path + "/monitoring_yaml"
        self.configs["exp_env"]["fmke_yaml_path"] = new_path + "/fmke_yaml"

        return kube_master_site

    def run(self):
        logger.debug('Parse and convert configs for OVH provisioner')
        self.configs = parse_config_file(self.args.config_file_path)
        # Add the number of Antidote DC as a parameter
        self.configs['parameters']['n_dc'] = len(self.configs['exp_env']['clusters'])

        logger.debug('Normalize the parameter space')
        self.normalized_parameters = define_parameters(self.configs['parameters'])

        logger.debug('Normalize the given configs')
        kube_master_site = self.create_configs()

        logger.info('''Your largest topology:
                        Antidote DCs: %s
                        n_antidotedb_per_DC: %s
                        n_fmke_client_per_DC: %s ''' % (
            len(self.configs['exp_env']['clusters']),
            max(self.normalized_parameters['n_antidotedb_per_dc']),
            max(self.normalized_parameters['n_fmke_client_per_dc'])
        )
        )

        logger.info('Creating the combination list')
        sweeper = create_paramsweeper(result_dir=self.configs['exp_env']['results_dir'],
                                      parameters=self.normalized_parameters)

        kube_namespace = 'fmke-exp'
        node_ids_file = None
        while len(sweeper.get_remaining()) > 0:
            if node_ids_file is None:
                kube_master, node_ids_file = self.setup_env(kube_master_site, kube_namespace)
            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(kube_namespace=kube_namespace,
                                            kube_master=kube_master,
                                            comb=comb,
                                            sweeper=sweeper)
            # if not is_nodes_alive(node_ids_file):
            #     node_ids_file = None
        logger.info('Finish the experiment!!!')


if __name__ == "__main__":
    logger.info("Init engine in %s" % __file__)
    engine = FMKe_antidotedb_ovh()

    try:
        logger.info("Start engine in %s" % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')