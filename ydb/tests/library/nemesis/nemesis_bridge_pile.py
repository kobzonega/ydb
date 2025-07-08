# -*- coding: utf-8 -*-
import collections
import time
from ydb.tests.library.clients.kikimr_bridge_client import bridge_client_factory
from ydb.tests.tools.nemesis.library.base import AbstractMonitoredNemesis

from ydb.tests.library.nemesis.nemesis_core import Nemesis, Schedule


class AbstractBridgePileNemesis(Nemesis, AbstractMonitoredNemesis):
    """
    Bridge-aware nemesis that performs master datacenter switching scenarios.
    This implements the specific scenario requested for switching off master.
    """

    def __init__(self, cluster, schedule=(300, 900), duration=60):
        super(AbstractBridgePileNemesis, self).__init__(schedule)
        AbstractMonitoredNemesis.__init__(self, 'datacenter')

        self._cluster = cluster
        self._duration = duration

        self._current_pile_id = None
        self._current_nodes = None
        self._start_time = None

        self._interval_schedule = Schedule.from_tuple_or_int(duration)

    @property
    def check_duration(self):
        if self._start_time is None:
            return False

        elapsed_time = time.time() - self._start_time
        if elapsed_time >= self._duration:
            self.logger.info("Duration (%d seconds) elapsed. Restoring services in pile '%s'",
                             self._duration, self._current_pile_id)
            return True
        return False

    def prepare_state(self):
        self.logger.info("Preparing BridgePile clients...")
        try:
            self._bridge_pile_to_nodes, self._bridge_piles = self._validate_bridge_piles()
            if self._bridge_piles is None:
                raise ValueError("Failed to validate bridge piles - need at least 2 bridge piles")
            self._bridge_clients = self._create_bridge_clients()
            self.logger.info("BridgePile clients prepared")
        except Exception as e:
            self.logger.error("Failed to prepare BridgePile clients: %s", e)
            raise

    def next_schedule(self):
        if self._current_pile_id is not None:
            return next(self._interval_schedule)
        return super(AbstractBridgePileNemesis, self).next_schedule()

    def inject_fault(self):
        """Inject fault and perform recovery with bridge state management."""
        for pile_id in self._bridge_piles:
            if pile_id != self._current_pile_id:
                result = self._bridge_clients[pile_id].switch(self._current_pile_id)
                if not result:
                    self.logger.error("Failed to switch bridge to pile %d", pile_id)
                    return

        self.on_success_inject_fault()

    def extract_fault(self):
        """Extract fault and perform recovery with bridge state management."""
        for pile_id in self._bridge_piles:
            if pile_id != self._current_pile_id:
                result = self._bridge_clients[pile_id].send_not_synchronized(
                    self._current_pile_id, specific_pile_ids=[pile_id]
                )
                if not result:
                    self.logger.error("Failed to send NOT_SYNCHRONIZED bridge to pile %d "
                                      "with specific pile ids [%s]", pile_id, pile_id)
                    return

                result = self._bridge_clients[pile_id].send_not_synchronized(
                    self._current_pile_id, specific_pile_ids=[self._current_pile_id]
                )
                if not result:
                    self.logger.error("Failed to send NOT_SYNCHRONIZED bridge to pile %d "
                                      "with specific pile ids [%s]", pile_id, self._current_pile_id)
                    return

        self.on_success_extract_fault()

    def _create_bridge_clients(self):
        bridge_clients = {}
        for pile in self._bridge_piles:
            node = self._bridge_pile_to_nodes[pile][0]
            bridge_clients[pile] = bridge_client_factory(
                node.host, node.port, cluster=self._cluster, retry_count=3
            )
            bridge_clients[pile].set_auth_token('root@builtin')
        return bridge_clients

    def _validate_bridge_piles(self, piles=2):
        bridge_pile_to_nodes = collections.defaultdict(list)
        for node in self._cluster.nodes.values():
            if node.bridge_pile_id is not None:
                bridge_pile_to_nodes[node.bridge_pile_id].append(node)

        for slot in self._cluster.slots.values():
            if slot.bridge_pile_id is not None:
                bridge_pile_to_nodes[slot.bridge_pile_id].append(slot)

        bridge_piles = list(bridge_pile_to_nodes.keys())
        if len(bridge_piles) != piles:
            return None, None
        return bridge_pile_to_nodes, bridge_piles


class BridgePileStopNodesNemesis(AbstractBridgePileNemesis):
    def __init__(self, cluster, schedule=(60, 180), duration=30):
        super(BridgePileStopNodesNemesis, self).__init__(
            cluster, schedule=schedule, duration=duration)

    def inject_fault(self):
        """Execute the master switch-off scenario."""
        # Check if duration has elapsed and extract if needed
        if self.check_duration:
            self.extract_fault()
            return

        self._current_pile_id = 0
        self._current_nodes = self._bridge_pile_to_nodes.get(self._current_pile_id, [])
        self._start_time = time.time()

        try:
            for node in self._current_nodes:
                self.logger.info("Stopping node %d on host %s", node.node_id, node.host)
                node.stop()
        except Exception as e:
            self.logger.error("Failed to stop node %d on host %s: %s", node.node_id, node.host, str(e))
            return

        super(BridgePileStopNodesNemesis, self).inject_fault()

    def extract_fault(self):
        """Extract fault and perform recovery with bridge state management."""
        if self._current_nodes is None or self._current_pile_id is None or self._start_time is None:
            self.logger.info("BridgePileNemesis is not in progress")
            return

        try:
            for node in self._current_nodes:
                self.logger.info("Starting node %d on host %s", node.node_id, node.host)
                node.start()
        except Exception as e:
            self.logger.error("Failed to start node %d on host %s: %s", node.node_id, node.host, str(e))
            return

        super(BridgePileStopNodesNemesis, self).extract_fault()

        self._current_pile_id = None
        self._current_nodes = None
        self._start_time = None

# class BridgeAwareFailoverNemesis(BridgeAwareMasterSwitchNemesis):
#     """
#     Extended version that supports different failover scenarios with bridge integration.
#     """
    
#     def __init__(self, cluster, bridge_client, bridge_pile_name=None, 
#                  schedule=(300, 900), duration=120, 
#                  scenario="switch_off_master", nemesis_class=DataCenterStopNodesNemesis):
#         super(BridgeAwareFailoverNemesis, self).__init__(
#             cluster, bridge_client, bridge_pile_name, schedule, duration, nemesis_class)
        
#         self._scenario = scenario
        
#     def run_planned_failover_scenario(self):
#         """Run a planned failover scenario without nemesis disruption."""
#         self.logger.info("Starting planned failover scenario")
        
#         # Get cluster state
#         cluster_state = self._get_cluster_state()
#         if cluster_state is None:
#             return False
            
#         primary_pile, sync_pile = self._find_primary_and_synchronized_piles(cluster_state)
#         if primary_pile is None or sync_pile is None:
#             return False
            
#         # Planned switch: promote synchronized to primary
#         from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
        
#         updates = [bridge.PileStateUpdate(pile_id=sync_pile, state=bridge.PROMOTE)]
        
#         if self._update_cluster_state(updates):
#             self.logger.info("Planned failover initiated: pile %d promoted", sync_pile)
#             return True
#         else:
#             self.logger.error("Planned failover failed")
#             return False
            
#     def run_disaster_recovery_scenario(self):
#         """Run a disaster recovery scenario with full datacenter loss."""
#         self.logger.info("Starting disaster recovery scenario")
        
#         # This would implement a more severe scenario where the primary DC is completely lost
#         # and we need to recover from backup/replica
        
#         # Step 1: Simulate complete primary DC loss
#         if self._active_nemesis is None:
#             # Use more aggressive nemesis (multiple types)
#             nemesis_types = [
#                 DataCenterStopNodesNemesis,
#                 DataCenterRouteUnreachableNemesis,
#                 DataCenterIptablesBlockPortsNemesis
#             ]
            
#             # Apply all nemesis types to primary DC
#             for nemesis_class in nemesis_types:
#                 nemesis = nemesis_class(self._cluster, duration=self._duration)
#                 nemesis.prepare_state()
#                 nemesis._current_dc = self._target_dc_for_nemesis
#                 nemesis.inject_fault()
                
#         # Step 2: Force promotion of secondary with emergency procedures
#         from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge
        
#         updates = [
#             bridge.PileStateUpdate(pile_id=self._primary_pile_id, state=bridge.DISCONNECTED),
#             bridge.PileStateUpdate(pile_id=self._synchronized_pile_id, state=bridge.PRIMARY),
#         ]
        
#         if self._update_cluster_state(updates):
#             self.logger.info("Disaster recovery switch completed")
#             return True
#         else:
#             self.logger.error("Disaster recovery switch failed")
#             return False
