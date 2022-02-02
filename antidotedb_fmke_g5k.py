import os
import shutil
import traceback
import requests

from cloudal.utils import get_logger, execute_cmd, parse_config_file, getput_file, ExecuteCommandException
from cloudal.action import performing_actions_g5k
from cloudal.provisioner import g5k_provisioner
from cloudal.configurator import (kubernetes_configurator, 
                                  k8s_resources_configurator, 
                                  antidotedb_configurator, 
                                  fmke_configurator, 
                                  CancelException)
from cloudal.experimenter import create_paramsweeper, define_parameters, is_job_alive, get_results

from execo_g5k import oardel
from execo_engine import slugify
from kubernetes import config

logger = get_logger()


class FMKe_antidotedb_g5k(performing_actions_g5k):
    def __init__(self, **kwargs):
        super(FMKe_antidotedb_g5k, self).__init__()
        self.args_parser.add_argument('--kube_master', dest='kube_master',
                                      help='name of kube master node',
                                      default=None,
                                      type=str)
        self.args_parser.add_argument('--setup_kube_env', dest='setup_kube_env',
                                      help='create namespace, setup label and volume for kube_workers for the experiment environment',
                                      action='store_true')
        self.args_parser.add_argument('--monitoring', dest='monitoring',
                                      help='deploy Grafana and Prometheus for AntidoteDB monitoring',
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
                                                  stabilizing_time=10,
                                                  timeout=600,
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
                                                                     monitoring_yaml_path = self.configs['exp_env']['monitoring_yaml_path'], 
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
            logger.info('Delete old result files in /tmp/results on FMKe client nodes')
            fmke_nodes = configurator.get_k8s_resources_name(resource='node',
                                                             label_selectors='service=fmke',
                                                             kube_namespace=kube_namespace)
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
                fmke_nodes = configurator.get_k8s_resources_name(resource='node',
                                                                 label_selectors='service=fmke')
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

    def _set_kube_workers_label(self, kube_workers):
        configurator = k8s_resources_configurator()
        clusters = dict()
        for host in kube_workers:
            cluster = host.split('-')[0]
            clusters[cluster] = [host] + clusters.get(cluster, list())
            configurator.set_labels_node(nodename=host,
                                         labels='cluster=%s' % cluster)

        n_fmke_per_dc = max(self.normalized_parameters['n_fmke_client_per_dc'])
        n_antidotedb_per_dc = max(self.normalized_parameters['n_nodes_per_dc'])

        for cluster, list_of_hosts in clusters.items():
            for n, service_name in [(n_antidotedb_per_dc, 'antidote'), (n_fmke_per_dc, 'fmke')]:
                for host in list_of_hosts[0: n]:
                    configurator.set_labels_node(nodename=host,
                                                 labels='service=%s' % service_name)
                list_of_hosts = list_of_hosts[n:]

    def _setup_g5k_kube_volumes(self, kube_workers, n_pv=3):
        logger.info('Setting volumes on %s kubernetes workers' % len(kube_workers))
        cmd = '''umount /dev/sda5;
                 mount -t ext4 /dev/sda5 /tmp'''
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

    def _get_credential(self, kube_master):
        home = os.path.expanduser('~')
        kube_dir = os.path.join(home, '.kube')
        if not os.path.exists(kube_dir):
            os.mkdir(kube_dir)
        getput_file(hosts=[kube_master], file_paths=['~/.kube/config'],
                    dest_location=kube_dir, action='get')
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

        self._setup_g5k_kube_volumes(kube_workers, n_pv=3)

        logger.info('Set labels for all kubernetes workers')
        self._set_kube_workers_label(kube_workers)

        logger.info('Finish deploying the Kubernetes cluster')

    def config_host(self, kube_master_site, kube_namespace):
        logger.info('Starting configuring nodes')
        kube_master = self.args.kube_master
        if kube_master is None:
            for host in self.hosts:
                if host.startswith(kube_master_site):
                    kube_master = host
                    break

        if self.args.kube_master is None:
            kube_workers = self.deploy_k8s(kube_master)
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        elif self.args.setup_kube_env:
            logger.info('Kubernetes master: %s' % kube_master)
            kube_workers = [host for host in self.hosts if host != kube_master]
            self.setup_k8s_env(kube_master, kube_namespace, kube_workers)
        else:
            logger.info('Kubernetes master: %s' % kube_master)
            self._get_credential(kube_master)

        logger.info('Finish configuring nodes')
        return kube_master

    def setup_env(self, kube_master_site, kube_namespace):
        logger.info('STARTING SETTING THE EXPERIMENT ENVIRONMENT')
        logger.info('Starting provisioning nodes on G5K')
        logger.info('Init provisioner: g5k_provisioner')
        provisioner = g5k_provisioner(configs=self.configs,
                                      keep_alive=self.args.keep_alive,
                                      out_of_chart=self.args.out_of_chart,
                                      oar_job_ids=self.args.oar_job_ids,
                                      no_deploy_os=self.args.no_deploy_os,
                                      is_reservation=self.args.is_reservation,
                                      job_name='cloudal_k8s_fmke')

        provisioner.provisioning()
        self.hosts = provisioner.hosts
        oar_job_ids = provisioner.oar_result
        self.oar_result = provisioner.oar_result

        kube_master = self.config_host(kube_master_site, kube_namespace)

        self.args.oar_job_ids = None
        logger.info('FINISH SETTING THE EXPERIMENT ENVIRONMENT\n')
        return kube_master, oar_job_ids

    def create_configs(self):
        logger.debug('Get the k8s master node')
        kube_master_site = self.configs['exp_env']['kube_master_site']
        if kube_master_site is None or kube_master_site not in self.configs['exp_env']['clusters']:
            kube_master_site = self.configs['exp_env']['clusters'][0]

        n_nodes_per_cluster = (max(self.normalized_parameters['n_fmke_client_per_dc']) + max(self.normalized_parameters['n_nodes_per_dc']))

        # set dataset and n_fmke_pop_process to default in case not provided
        if 'dataset' not in self.normalized_parameters:
            self.normalized_parameters['dataset'] = 'standard'

        if 'n_fmke_pop_process' not in self.normalized_parameters:
            self.normalized_parameters['n_fmke_pop_process'] = 100

        # create standard cluster information to make reservation on Grid'5000, this info using by G5k provisioner
        clusters = list()
        for cluster in self.configs['exp_env']['clusters']:
            if cluster == kube_master_site:
                clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster + 1})
            else:
                clusters.append({'cluster': cluster, 'n_nodes': n_nodes_per_cluster})
        self.configs['clusters'] = clusters

        # copy all YAML template folders to a new one for running this experiment to avoid conflicting
        # while use the same YAML template folders to run different experiments simultaneously
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
        logger.debug('Parse and convert configs for G5K provisioner')
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
        oar_job_ids = None
        while len(sweeper.get_remaining()) > 0:
            if oar_job_ids is None:
                kube_master, oar_job_ids = self.setup_env(kube_master_site, kube_namespace)

            comb = sweeper.get_next()
            sweeper = self.run_exp_workflow(kube_namespace=kube_namespace,
                                            kube_master=kube_master,
                                            comb=comb,
                                            sweeper=sweeper)

            if not is_job_alive(oar_job_ids):
                oardel(oar_job_ids)
                oar_job_ids = None
        logger.info('Finish the experiment!!!')


if __name__ == '__main__':
    logger.info('Init engine in %s' % __file__)
    engine = FMKe_antidotedb_g5k()

    try:
        logger.info('Start engine in %s' % __file__)
        engine.start()
    except Exception as e:
        logger.error('Program is terminated by the following exception: %s' % e, exc_info=True)
        traceback.print_exc()
    except KeyboardInterrupt:
        logger.info('Program is terminated by keyboard interrupt.')

    if not engine.args.keep_alive:
        logger.info('Deleting reservation')
        oardel(engine.oar_result)
        logger.info('Reservation deleted')
    else:
        logger.info('Reserved nodes are kept alive for inspection purpose.')
