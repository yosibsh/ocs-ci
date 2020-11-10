import logging
import pytest

from ocs_ci.framework.testlib import E2ETest, workloads
from ocs_ci.ocs.couchbase import CouchBase
from ocs_ci.ocs import constants

log = logging.getLogger(__name__)


@pytest.fixture(scope='function')
def couchbase(request):

    couchbase = CouchBase()

    def teardown():
        couchbase.teardown()
    request.addfinalizer(teardown)
    return couchbase


@workloads
@pytest.mark.polarion_id("OCS-785")
class TestCouchBaseWorkload(E2ETest):
    """
    Deploy an CouchBase workload using operator
    """
    @pytest.mark.parametrize(
        argnames=["new_sc"],
        argvalues=[
            pytest.param(
                *[True], marks=pytest.mark.polarion_id("OCS-776")
            ),
            pytest.param(
                *[False], marks=pytest.mark.polarion_id("OCS-776")
            ),
        ]
    )
    def test_cb_workload_simple(self, couchbase, storageclass_factory, new_sc=False):
        """
        Testing basic couchbase workload
        """

        couchbase.setup_cb()
        if new_sc:
            sc_obj = storageclass_factory(interface=constants.RBD_INTERFACE,
                                          new_rbd_pool=True, replica=2, compression='aggressive'
                                          )
            couchbase.create_couchbase_worker(replicas=3, new_sc=sc_obj.name)
        else:
            couchbase.create_couchbase_worker(replicas=3)

        couchbase.run_workload(replicas=3)
        couchbase.export_pfoutput_to_googlesheet(sheet_name='E2E Workloads',
                                                 sheet_index=2
                                                 )
