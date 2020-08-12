import logging
import pytest
import threading

from tests import helpers
from ocs_ci.ocs.ocp import OCP
from ocs_ci.ocs import constants, scale_lib
from ocs_ci.framework import config
from ocs_ci.ocs.cluster import CephCluster
from ocs_ci.ocs.scale_lib import FioPodScale
from ocs_ci.ocs import machine as machine_utils
from ocs_ci.ocs.resources import storage_cluster
from ocs_ci.utility.utils import ceph_health_check
from ocs_ci.framework.testlib import scale, E2ETest, ignore_leftovers
from ocs_ci.framework.pytest_customization.marks import (
    skipif_aws_i3, skipif_bm, skipif_external_mode
)

logger = logging.getLogger(__name__)


def add_ocs_node(node_count):
    if config.ENV_DATA['platform'].lower() in constants.CLOUD_PLATFORMS:
        dt = config.ENV_DATA['deployment_type']
        if dt == 'ipi':

            # Get the initial nodes list
            initial_nodes = helpers.get_worker_nodes()

            ms_name = machine_utils.get_machineset_objs()
            if len(ms_name) == 3:
                add_replica_by = int(node_count / 3)
            else:
                add_replica_by = node_count

            replica = machine_utils.get_ready_replica_count(ms_name[0].name)

            # Increase the replica count and wait for node add to complete.
            for ms in ms_name:
                machine_utils.add_node(
                    machine_set=ms.name, count=(replica + add_replica_by)
                )
            threads = list()
            for ms in ms_name:
                process = threading.Thread(
                    target=machine_utils.wait_for_new_node_to_be_ready, kwargs={
                        'machine_set': ms.name
                    }
                )
                process.start()
                threads.append(process)
            for process in threads:
                process.join()

            # Get the node name of new spun node
            nodes_after_new_spun_node = helpers.get_worker_nodes()
            new_spun_node = list(
                set(nodes_after_new_spun_node) - set(initial_nodes)
            )
            logging.info(f"New spun node is {new_spun_node}")

            # Label it
            node_obj = OCP(kind='node')
            for node in new_spun_node:
                node_obj.add_label(
                    resource_name=node,
                    label=constants.OPERATOR_NODE_LABEL
                )
                logging.info(
                    f"Successfully labeled {new_spun_node} with OCS storage label"
                )

    elif config.ENV_DATA['platform'].lower() == constants.VSPHERE_PLATFORM:
        pytest.skip("Skipping add node in Vmware platform due to "
                    "https://bugzilla.redhat.com/show_bug.cgi?id=1844521"
                    )


def add_capacity_test(count=1):
    osd_size = storage_cluster.get_osd_size()
    result = storage_cluster.add_capacity(osd_size * count)
    pod = OCP(
        kind=constants.POD, namespace=config.ENV_DATA['cluster_namespace']
    )
    pod.wait_for_resource(
        timeout=300,
        condition=constants.STATUS_RUNNING,
        selector='app=rook-ceph-osd',
        resource_count=result * 3
    )

    ceph_health_check(
        namespace=config.ENV_DATA['cluster_namespace'], tries=80
    )
    # ceph_cluster_obj = CephCluster()
    # assert ceph_cluster_obj.wait_for_rebalance(timeout=3600), (
    #     "Data re-balance failed to complete"
    # )


@ignore_leftovers
@scale
@skipif_aws_i3
@skipif_bm
@skipif_external_mode
class TestAddNode(E2ETest):
    """
    Automates adding worker nodes to the cluster while IOs
    """
    def test_scale_node_capacity(self):
        """
        Test for adding worker nodes to the cluster while IOs
        """
        # Scale OCS worker node
        add_capacity_test(count=2)

        while True:
            # Scale 3 OCS worker and add full capacity
            add_ocs_node(3)
            add_capacity_test(count=3)

            if helpers.get_worker_nodes() == 30:
                break

        # iteration_count = 0
        # while True:
        #     # Scale PVC+POD to reach 15000 required 120 app worker nodes.
        #     # Scale FIO pods in the cluster
        #     # Single iteration will create 1500 pods.
        #     iteration_count += 1
        #     nginx_pod = FioPodScale(
        #         kind=constants.POD, pod_dict_path=constants.NGINX_POD_YAML,
        #         node_selector=constants.SCALE_NODE_SELECTOR
        #     )
        #     fedora_pod = FioPodScale(
        #         kind=constants.DEPLOYMENTCONFIG, pod_dict_path=constants.FEDORA_DC_YAML,
        #         node_selector=constants.SCALE_NODE_SELECTOR
        #     )
        #
        #     nginx_pod.create_scale_pods(
        #         scale_count=400, pods_per_iter=10, io_runtime=36000,
        #         start_io=True
        #     )
        #
        #     nginx_pod.create_scale_pods(
        #         scale_count=400, pods_per_iter=5, start_io=False
        #     )
        #
        #     fedora_pod.create_scale_pods(
        #         scale_count=350, pods_per_iter=5, io_runtime=36000,
        #         start_io=True
        #     )
        #
        #     fedora_pod.create_scale_pods(
        #         scale_count=350, pods_per_iter=5, start_io=False
        #     )
        #
        #     if iteration_count >= 10:
        #         break
