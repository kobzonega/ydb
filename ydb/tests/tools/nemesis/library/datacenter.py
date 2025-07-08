# -*- coding: utf-8 -*-

from ydb.tests.library.nemesis.nemesis_datacenter import (
    DataCenterStopNodesNemesis,
    DataCenterRouteUnreachableNemesis,
    DataCenterIptablesBlockPortsNemesis
)
from ydb.tests.library.nemesis.nemesis_bridge_pile import BridgePileStopNodesNemesis


def check_bridge_pile_name(cluster):
    for node in cluster.nodes.values():
        if node.bridge_pile_name is None:
            return False
    return True


def datacenter_nemesis_list(cluster):
    if check_bridge_pile_name(cluster):
        return [
            BridgePileStopNodesNemesis(cluster),
            # BridgePileRouteUnreachableNemesis(cluster, bridge_client),
            # BridgePileIptablesBlockPortsNemesis(cluster, bridge_client)
        ]
    return [
        DataCenterStopNodesNemesis(cluster),
        DataCenterRouteUnreachableNemesis(cluster),
        DataCenterIptablesBlockPortsNemesis(cluster)
    ]
