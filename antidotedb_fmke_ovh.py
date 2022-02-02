import os
import shutil
import traceback
import requests

from cloudal.utils import get_logger, execute_cmd, parse_config_file, getput_file, ExecuteCommandException
from cloudal.action import performing_actions
from cloudal.provisioner import ovh_provisioner
from cloudal.configurator import (kubernetes_configurator, 
                                  k8s_resources_configurator, 
                                  antidotedb_configurator, 
                                  fmke_configurator, 
                                  CancelException)
from cloudal.experimenter import create_paramsweeper, define_parameters, get_results

from execo_engine import slugify
from kubernetes import config

logger = get_logger()


class FMKe_antidotedb_ovh(performing_actions):
    def __init__(self):
        super(FMKe_antidotedb_ovh, self).__init__()
        self.args_parser.add_argument('--node_ids_file', dest='node_ids_file',
                                      help='the path to the file contents list of node IDs',
                                      default=None,
                                      type=str)
        self.args_parser.add_argument('--kube_master', dest='kube_master',
                                      help='name of kube master node',
                                      default=None,
                                      type=str)
        self.args_parser.add_argument('--setup_kube_env', dest='setup_kube_env',
                                      help='create namespace, setup label and volume for kube_workers for the experiment environment',
                                      action='store_true')
        self.args_parser.add_argument('--monitoring', dest='monitoring',
                                      help='deploy Grafana and Prometheus for monitoring',
                                      action='store_true')
        self.args_parser.add_argument('--attach_volume', dest='attach_volume',
                                      help='attach an external volume to every data node',
                                      action='store_true')

    def save_results(self, comb, pop_time, pop_errors, nodes=list()):
        logger.info('----------------------------------')
        logger.info('6. Starting dowloading the results')
        if nodes:
            comb_dir = get_results(comb=comb,
                                   hosts=nodes,
                                   remote_result_files=['/tmp/results/*'],
                                   local_result_dir=self.configs['exp_env']['results_dir'])

        with open(os.path.join(comb_dir, 'pop_time.txt'), 'w') as f:
            f.write(pop_time)
        if pop_errors:
            with open(os.path.join(comb_dir, 'pop_error.txt'), 'w') as f:
                f.write(str(pop_errors))

        logger.info('Finish dowloading the results')

    def deploy_fmke_client(self, kube_namespace, comb):
        logger.info('-----------------------------------------------------------------')
        logger.info('5. Starting deploying FMKe client')
        configurator = fmke_configurator()
        configurator.deploy_fmke_client(test_duration=self.configs['exp_env']['test_duration'],
                                        fmke_yaml_path=self.configs['exp_env']['fmke_yaml_path'], 
                                        workload=self.configs['exp_env']['operations'],
                                        concurrent_clients=comb['concurrent_clients'],
                                        n_total_fmke_clients=comb['n_fmke_client_per_dc'] * len(self.configs['exp_env']['clusters']),
                                        kube_namespace=kube_namespace)

    def deploy_fmke_pop(self, kube_namespace, comb):
        logger.info('---------------------------')
        logger.info('4. Starting deploying FMKe populator')
        configurator = fmke_configurator()
        pop_result = configurator.deploy_fmke_pop(fmke_yaml_path=self.configs['exp_env']['fmke_yaml_path'], 
                                                  clusters=self.configs['exp_env']['clusters'],
                                                  dataset=comb['dataset'],
                                                  n_fmke_pop_process=comb['n_fmke_pop_process'],
                                                  stabilizing_time=15,
                                                  timeout=1800,
                                                  kube_namespace=kube_namespace)
        return pop_result

    def deploy_fmke_app(self, kube_namespace, comb):
        logger.info('------------------------------------')
        logger.info('3. Starting deploying FMKe application')
        configurator = fmke_configurator()
        configurator.deploy_fmke_app(fmke_yaml_path=self.configs['exp_env']['fmke_yaml_path'],
                                     clusters=self.configs['exp_env']['clusters'],
                                     n_fmke_app_per_dc=comb['n_fmke_client_per_dc'],
                                     concurrent_clients=comb['concurrent_clients'],
                                     kube_namespace=kube_namespace)

    def get_prometheus_metric(self, metric_name, prometheus_url):
        logger.info('---------------------------')
        logger.info('Retrieving Prometheus data')
        query = '%s/api/v1/query?query=%s' % (prometheus_url, metric_name)
        logger.debug('query = %s' % query)
        r = requests.get(query)
        normalize_result = dict()
        logger.debug('status_code = %s' % r.status_code)
        if r.status_code == 200:
            result = r.json()
            for each in result['data']['result']:
                key = each['metric']['instance']
                normalize_result[key] = int(each['value'][1])
        return normalize_result

    def deploy_monitoring(self, kube_master, kube_namespace):
        logger.info('--------------------------------------')
        logger.info('Deploying monitoring system')
        configurator = antidotedb_configurator()
        prometheus_url, grafana_url = configurator.deploy_monitoring(node=kube_master,
                                                                     monitoring_yaml_path=self.configs['exp_env']['monitoring_yaml_path'], 
                                                                     kube_namespace=kube_namespace)
        return prometheus_url, grafana_url

    def deploy_antidote(self, kube_namespace, comb):
        logger.info('--------------------------------------')
        logger.info('2. Starting deploying Antidote cluster')
        configurator = antidotedb_configurator()
        configurator.deploy_antidotedb(n_nodes=comb['n_nodes_per_dc'], 
                                       antidotedb_yaml_path=self.configs['exp_env']['antidotedb_yaml_path'], 
                                       clusters=self.configs['exp_env']['clusters'], 
                                       kube_namespace=kube_namespace)

    def clean_exp_env(self, kube_namespace, n_fmke_client_per_dc):
        logger.info('1. Cleaning the experiment environment')
        logger.info('Deleting all k8s running resources from the previous run in namespace "%s"' % kube_namespace)
        logger.debug('Delete namespace "%s" to delete all the running resources, then create it again' % kube_namespace)
        configurator = k8s_resources_configurator()
        configurator.delete_namespace(kube_namespace)
        configurator.create_namespace(kube_namespace)

        if n_fmke_client_per_dc > 0:
            logger.info('Delete all files in /tmp/results folder on fmke_client nodes')
            fmke_nodes_info = configurator.get_k8s_resources(resource='node',
                                                             label_selectors='service=fmke',
                                                             kube_namespace=kube_namespace)
            fmke_nodes = [r.metadata.annotations['flannel.alpha.coreos.com/public-ip']
                             for r in fmke_nodes_info.items]
            cmd = 'rm -rf /tmp/results && mkdir -p /tmp/results'
            execute_cmd(cmd, fmke_nodes)

    def run_exp_workflow(self, kube_namespace, comb, kube_master, sweeper):
        comb_ok = False
        try:
            logger.info('=======================================')
            logger.info('Performing combination: ' + slugify(comb))

            self.clean_exp_env(kube_namespace, comb['n_fmke_client_per_dc'])
            self.deploy_antidote(kube_namespace, comb)
            if self.args.monitoring:
                prometheus_url, _ = self.deploy_monitoring(kube_master, kube_namespace)
            self.deploy_fmke_app(kube_namespace, comb)
            pop_result = self.deploy_fmke_pop(kube_namespace, comb)
            pop_errors = None
            if self.args.monitoring:
                pop_errors = self.get_prometheus_metric('antidote_error_count', prometheus_url)
                pop_errors = sum(pop_errors.values())
                logger.info('Total ops errors of population: %s' % pop_errors)
            if comb['n_fmke_client_per_dc'] > 0:
                self.deploy_fmke_client(kube_namespace, comb)
                configurator = k8s_resources_configurator()
                fmke_nodes_info = configurator.get_k8s_resources(resource='node',
                                                                 label_selectors='service=fmke')
                fmke_nodes = [r.metadata.annotations['flannel.alpha.coreos.com/public-ip'] for r in fmke_nodes_info.items]
                self.save_results(comb, pop_result, pop_errors, fmke_nodes)
            else:
                self.save_results(comb, pop_result, pop_errors)
            comb_ok = True
        except (ExecuteCommandException, CancelException) as e:
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
        logger.info('Setting volumes on %s kubernetes workers' % len(kube_workers))
        cmd = '''rm -rf /rmp/pv'''
        execute_cmd(cmd, kube_workers)
        logger.debug('Create n_pv partitions on the physical disk to make a PV can be shared')
        cmd = '''for i in $(seq 1 %s); do
                     mkdir -p /tmp/pv/vol${i}
                     mkdir -p /mnt/disks/vol${i}
                     mount --bind /tmp/pv/vol${i} /mnt/disks/vol${i}
                 done''' % n_pv
        execute_cmd(cmd, kube_workers)

        logger.info('Creating local persistance volumes on Kubernetes cluster')
        logger.debug('Init configurator: k8s_resources_configurator')
        configurator = k8s_resources_configurator()
        antidote_k8s_dir = self.configs['exp_env']['antidotedb_yaml_path']
        deploy_files = [os.path.join(antidote_k8s_dir, 'local_persistentvolume.yaml'),
                        os.path.join(antidote_k8s_dir, 'storageClass.yaml')]
        configurator.deploy_k8s_resources(files=deploy_files)

        logger.info('Waiting for setting local persistance volumes')
        configurator.wait_k8s_resources(resource='pod',
                                        label_selectors='app.kubernetes.io/instance=local-volume-provisioner')

    def _set_kube_workers_label(self, kube_master):
        configurator = k8s_resources_configurator()
        for node in self.nodes:
            if node['ipAddresses'][0]['ip'] == kube_master:
                pass
            else:
                configurator.set_labels_node(nodename=node['name'],
                                             labels='cluster=%s' % node['region'].lower())
                if node['id'] in self.data_node_ids:
                    configurator.set_labels_node(nodename=node['name'],
                                                 labels='service=antidote')
                else:
                    configurator.set_labels_node(nodename=node['name'],
                                                 labels='service=fmke')

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
        logger.debug('Init configurator: kubernetes_configurator')
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

        logger.info('Finish deploying the Kubernetes cluster')

    def config_host(self, kube_master, kube_namespace):
        logger.info('Starting configuring nodes')
        kube_workers = [host for host in self.hosts if host != kube_master]
        if self.args.kube_master is None:
            kube_workers = self.deploy_k8s(kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        elif self.args.setup_kube_env:
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        else:
            self._get_credential(kube_master)
            if self.args.attach_volume:
                self._setup_ovh_kube_volumes(kube_workers, n_pv=3)
        logger.info('Finish configuring nodes')

    def setup_env(self, kube_master_site, kube_namespace):
        logger.info('STARTING SETTING THE EXPERIMENT ENVIRONMENT')
        logger.info('Starting provisioning nodes on OVHCloud')

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

        data_nodes = list()
        clusters = dict()
        for node in self.nodes:
            if node['id'] == kube_master_id:
                continue
            cluster = node['region']
            clusters[cluster] = [node] + clusters.get(cluster, list())
        for region, nodes in clusters.items():
            data_nodes += nodes[0: max(self.normalized_parameters['n_nodes_per_dc'])]
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
        logger.info('Kubernetes master: %s' % kube_master)

        logger.info('FINISH SETTING THE EXPERIMENT ENVIRONMENT\n')
        return kube_master, node_ids_file

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = (max(
            self.normalized_parameters['n_fmke_client_per_dc']) + max(self.normalized_parameters['n_nodes_per_dc']))

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
        results_dir_name = (self.configs['exp_env']['results_dir']).split('/')[-1]
        results_dir_path = os.path.dirname(self.configs['exp_env']['results_dir'])

        yaml_dir_path = os.path.dirname(self.configs['exp_env']['antidotedb_yaml_path'])
        yaml_dir_name = yaml_dir_path.split('/')[-1]

        new_yaml_dir_name = yaml_dir_name + '_' + results_dir_name
        new_path = results_dir_path + '/' + new_yaml_dir_name
        if os.path.exists(new_path):
            shutil.rmtree(new_path)
        shutil.copytree(yaml_dir_path, new_path)

        self.configs['exp_env']['antidotedb_yaml_path'] = new_path + '/antidotedb_yaml'
        self.configs['exp_env']['monitoring_yaml_path'] = new_path + '/monitoring_yaml'
        self.configs['exp_env']['fmke_yaml_path'] = new_path + '/fmke_yaml'

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
            max(self.normalized_parameters['n_nodes_per_dc']),
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


if __name__ == '__main__':
    logger.info('Init engine in %s' % __file__)
    engine = FMKe_antidotedb_ovh()

    try:
        logger.info('Start engine in %s' % __file__)
        engine.start()
    except Exception as e:
        logger.error(
            'Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')
