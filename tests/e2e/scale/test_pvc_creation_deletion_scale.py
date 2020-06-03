"""
Test to measure pvc scale creation & deletion time. Total pvc count would be 1500
"""
import logging
import random
import pytest
import threading

from tests import helpers
from ocs_ci.framework.testlib import scale, E2ETest, polarion_id
from ocs_ci.ocs import constants
from ocs_ci.utility.utils import ocsci_log_path
from pip._vendor.html5lib._ihatexml import charStringToList

log = logging.getLogger(__name__)


@scale
class TestPVCCreationDeletionScale(E2ETest):
    """
    Base class for PVC scale creation and deletion
    """
    def code_inside_pvc_test(self, msg):
        # Code that will be run inside the various pvc creation and deletion
        # tests goes here
        log.info(msg)

    @pytest.fixture()
    def pvc_scale_teardown(self, request):
        """
        Teardown test_multiple_pvc_creation_deletion_scale
        """
        def pvc_scale_deletion():
            log.info("tearing down pvc creation scale test")
            # Delete PVC
            helpers.delete_objs(self.pvc_objs)

            # Get PVC deletion time
            pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
                interface=self.interface, pv_name_list=self.pv_nm_list
            )

            helpers.write_csv_data(
                pvc_deletion_time,
                f"{self.log_path}-deletion-time.csv",
                "Delete"
            )
        request.addfinalizer(pvc_scale_deletion)
        return pvc_scale_deletion

    @pytest.fixture()
    def pvc_combinations_type_teardown(self, request):
        """
        Teardown test_all_combinations_type_pvc_creation_deletion_scale
        """
        def pvc_combinations_type_deletion():
            log.info("tearing down four combination scale test")
            # Delete PVC
            pvc_objs = self.fs_pvc_obj + self.rbd_pvc_obj
            for dobj in pvc_objs:
                helpers.wait_for_resource_state(
                    dobj,
                    constants.STATUS_BOUND,
                    60
                )
            helpers.delete_objs(pvc_objs)

            # Get PVC deletion time
            fs_pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
                interface=constants.CEPHFS_INTERFACE, pv_name_list=self.fs_pv_name
            )
            rbd_pvc_deletion_time = helpers.measure_pv_deletion_time_bulk(
                interface=constants.CEPHBLOCKPOOL, pv_name_list=self.rbd_pv_name
            )
            fs_pvc_deletion_time.update(rbd_pvc_deletion_time)

            helpers.write_csv_data(
                fs_pvc_deletion_time,
                f"{self.log_path}-deletion-time.csv",
                "Delete"
            )
        request.addfinalizer(pvc_combinations_type_deletion)
        return pvc_combinations_type_deletion

    @pytest.fixture()
    def namespace(self, project_factory):
        """
        Create a new project
        """
        proj_obj = project_factory()
        self.namespace = proj_obj.namespace

    @pytest.mark.parametrize(
        argnames=["access_mode", "interface"],
        argvalues=[
            pytest.param(
                *[constants.ACCESS_MODE_RWO, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-1225")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHBLOCKPOOL],
                marks=pytest.mark.polarion_id("OCS-2010")
            ),
            pytest.param(
                *[constants.ACCESS_MODE_RWX, constants.CEPHFS_INTERFACE],
                marks=pytest.mark.polarion_id("OCS-2008")
            ),
        ]
    )
    @pytest.mark.usefixtures(namespace.__name__)
    def test_multiple_pvc_creation_deletion_scale(self, namespace,
                                                  pvc_scale_teardown,
                                                  access_mode,
                                                  interface):
        self.multiple_pvc_creation_scale(access_mode, interface)
        self.code_inside_pvc_test("running multiple_pvc creation and deletion")

    def multiple_pvc_creation_scale(self, access_mode, interface):
        """
        Measuring PVC creation time while scaling PVC
        Measure PVC deletion time after creation test
        """
        self.interface = interface
        number_of_pvc = 1500
        log.info(f"Start creating {access_mode}-{interface} {number_of_pvc} PVC")

        if interface == constants.CEPHBLOCKPOOL:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_RBD
        elif interface == constants.CEPHFS_INTERFACE:
            self.sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        self.log_path = f"{ocsci_log_path()}/{self.sc_obj}-{access_mode}"

        # Create PVC
        self.pvc_objs = helpers.create_multiple_pvcs(
            sc_name=self.sc_obj,
            namespace=self.namespace,
            number_of_pvc=number_of_pvc,
            size=f"{random.randrange(5, 105, 5)}Gi",
            access_mode=access_mode
        )

        # Check for PVC status using threads
        threads = list()
        for obj in self.pvc_objs:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND, )
            )
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        # Get pvc_name, require pvc_name to fetch creation time data from log:
        threads = list()
        for pvc_obj in self.pvc_objs:
            process = threading.Thread(target=pvc_obj.reload)
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        self.pvc_nm_list, self.pv_nm_list = ([] for i in range(2))
        threads = list()
        for pvc_obj in self.pvc_objs:
            process1 = threading.Thread(target=self.pvc_nm_list.append(pvc_obj.name))
            process2 = threading.Thread(target=self.pv_nm_list.append(pvc_obj.backed_pv))
            process1.start()
            process2.start()
            threads.append(process1)
            threads.append(process2)
        for process in threads:
            process.join()

        # Get PVC creation time
        pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=interface, pvc_name_list=self.pvc_nm_list
        )
        helpers.write_csv_data(
            pvc_create_time,
            f"{self.log_path}-creation-time.csv",
            "Create"
        )

    @polarion_id('OCS-1885')
    @pytest.mark.usefixtures(namespace.__name__)
    def test_all_combinations_type_pvc_creation_deletion_scale(
        self,
        namespace,
        pvc_combinations_type_teardown
    ):
        self.all_combinations_type_pvc_creation_scale()
        self.code_inside_pvc_test("running all_combinations_type")

    def all_combinations_type_pvc_creation_scale(self):
        """
        Measuring PVC creation time while scaling PVC of all 4 types, Total 1500 PVCs
        will be created, i.e. 375 each pvc type
        Measure PVC deletion time in scale env
        """
        number_of_pvc = 375
        log.info(f"Start creating {number_of_pvc} PVC of all 4 types")
        self.log_path = f"{ocsci_log_path()}/pvc-of-all-4-types"

        cephfs_sc_obj = constants.DEFAULT_STORAGECLASS_CEPHFS
        rbd_sc_obj = constants.DEFAULT_STORAGECLASS_RBD

        # Create all 4 types of PVC
        self.fs_pvc_obj, self.rbd_pvc_obj = ([] for i in range(2))
        for mode in [constants.ACCESS_MODE_RWO, constants.ACCESS_MODE_RWX]:
            self.fs_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=cephfs_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )
            self.rbd_pvc_obj.extend(helpers.create_multiple_pvcs(
                sc_name=rbd_sc_obj, namespace=self.namespace, number_of_pvc=number_of_pvc,
                size=f"{random.randrange(5, 105, 5)}Gi", access_mode=mode)
            )

        # Check for PVC status using threads
        threads = list()
        for obj in self.fs_pvc_obj:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND, )
            )
            process.start()
            threads.append(process)
        for obj in self.rbd_pvc_obj:
            process = threading.Thread(
                target=helpers.wait_for_resource_state,
                args=(obj, constants.STATUS_BOUND,)
            )
            process.start()
            threads.append(process)
        for process in threads:
            process.join()

        # Get pvc_name, require pvc_name to fetch creation time data from log
        threads = list()
        for fs_obj, rbd_obj in zip(self.fs_pvc_obj, self.rbd_pvc_obj):
            process1 = threading.Thread(target=fs_obj.reload)
            process2 = threading.Thread(target=rbd_obj.reload)
            process1.start()
            process2.start()
            threads.append(process1)
            threads.append(process2)
        for process in threads:
            process.join()

        fs_pvc_name, rbd_pvc_name = ([] for i in range(2))
        #import pdb; pdb.set_trace()
        self.fs_pv_name, self.rbd_pv_name = ([] for i in range(2))
        threads = list()
        for fs_obj, rbd_obj in zip(self.fs_pvc_obj, self.rbd_pvc_obj):
            process1 = threading.Thread(target=fs_pvc_name.append(fs_obj.name))
            process2 = threading.Thread(target=rbd_pvc_name.append(rbd_obj.name))
            process3 = threading.Thread(target=self.fs_pv_name.append(fs_obj.backed_pv))
            process4 = threading.Thread(target=self.rbd_pv_name.append(rbd_obj.backed_pv))
            process1.start()
            process2.start()
            process3.start()
            process4.start()
            threads.append(process1)
            threads.append(process2)
            threads.append(process3)
            threads.append(process4)
        for process in threads:
            process.join()

        # Get PVC creation time
        fs_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHFS_INTERFACE, pvc_name_list=fs_pvc_name
        )
        rbd_pvc_create_time = helpers.measure_pvc_creation_time_bulk(
            interface=constants.CEPHBLOCKPOOL, pvc_name_list=rbd_pvc_name
        )
        fs_pvc_create_time.update(rbd_pvc_create_time)

        helpers.write_csv_data(
            fs_pvc_create_time,
            f"{self.log_path}-creation-time.csv",
            "Create"
        )
