#!/usr/bin/env python
# -*- coding: utf-8 -*-
import time
import logging

import grpc

from ydb.public.api.grpc.draft import ydb_bridge_v1_pb2_grpc as grpc_server
from ydb.public.api.protos.draft import ydb_bridge_pb2 as bridge_api
from ydb.public.api.protos.ydb_status_codes_pb2 import StatusIds

logger = logging.getLogger()


def bridge_client_factory(server, port, cluster=None, retry_count=1):
    return BridgeClient(
        server, port, cluster=cluster,
        retry_count=retry_count
    )


class BridgeClient(object):
    def __init__(self, server, port, cluster=None, retry_count=1):
        self.server = server
        self.port = port
        self._cluster = cluster
        self.__retry_count = retry_count
        self.__retry_sleep_seconds = 10
        self._options = [
            ('grpc.max_receive_message_length', 64 * 10 ** 6),
            ('grpc.max_send_message_length', 64 * 10 ** 6)
        ]
        self._channel = grpc.insecure_channel("%s:%s" % (self.server, self.port), options=self._options)
        self._stub = grpc_server.BridgeServiceStub(self._channel)
        self._auth_token = None

    def set_auth_token(self, token):
        self._auth_token = token

    def _get_invoke_callee(self, method):
        return getattr(self._stub, method)

    def invoke(self, request, method):
        retry = self.__retry_count
        while True:
            try:
                callee = self._get_invoke_callee(method)
                metadata = []
                if self._auth_token:
                    metadata.append(('x-ydb-auth-ticket', self._auth_token))
                return callee(request, metadata=metadata)
            except (RuntimeError, grpc.RpcError):
                retry -= 1

                if not retry:
                    raise

                time.sleep(self.__retry_sleep_seconds)

    def get_cluster_state(self):
        request = bridge_api.GetClusterStateRequest()
        return self.invoke(request, 'GetClusterState')

    def get_cluster_state_result(self):
        response = self.get_cluster_state()
        if response.operation.status != StatusIds.SUCCESS:
            logger.error("Failed to get cluster state: %s", response.operation.status)
            return None
        result = bridge_api.GetClusterStateResult()
        response.operation.result.Unpack(result)
        logger.debug("Get cluster state result: %s", result)
        return result

    def update_cluster_state(self, updates, specific_pile_ids=None):
        """
        Update cluster state with optional specific pile IDs.

        Args:
            updates: List of PileStateUpdate objects
            specific_pile_ids: Optional list of pile IDs to target for quorum

        Returns:
            Response from the bridge service
        """
        request = bridge_api.UpdateClusterStateRequest()
        request.updates.extend(updates)

        # Add specific pile IDs if provided
        if specific_pile_ids is not None:
            request.specific_pile_ids.extend(specific_pile_ids)
            logger.debug("Updating cluster state with specific pile IDs: %s", specific_pile_ids)

        return self.invoke(request, 'UpdateClusterState')

    def update_cluster_state_result(self, updates, expected_status=StatusIds.SUCCESS):
        response = self.update_cluster_state(updates)
        logger.debug("Update cluster state response: %s", response)
        if response.operation.status != expected_status:
            logger.error("Failed to update cluster state: %s", response.operation.status)
            return None

        result = bridge_api.UpdateClusterStateResult()
        response.operation.result.Unpack(result)
        return result

    @staticmethod
    def get_primary_pile(cluster_state=None):
        """
        Find primary pile ID from cluster state.

        Returns:
            Pile ID or None if not found
        """

        for s in cluster_state.per_pile_state:
            if s.state == bridge_api.PileState.PRIMARY:
                return s.pile_id

        return None

    @staticmethod
    def get_synchronized_pile(cluster_state=None):
        """
        Find synchronized pile ID from cluster state.

        Returns:
            Pile ID or None if not found
        """

        for s in cluster_state.per_pile_state:
            if s.state == bridge_api.PileState.SYNCHRONIZED:
                return s.pile_id

        return None

    def switch(self, pile_id):
        """
        Switch pile if it is primary to disconnected and synchronized to primary.
        Switch pile if it is synchronized to disconnected and primary to primary.

        Args:
            pile_id: Pile ID which was disabled

        Returns:
            True if successful, False otherwise
        """
        cluster_state = self.get_cluster_state_result()
        if cluster_state is None:
            logger.error("Failed to get cluster state")
            return False

        primary_pile_id = self.get_primary_pile(cluster_state)
        if primary_pile_id is None:
            logger.error("Primary pile not found")
            return False

        synchronized_pile_id = self.get_synchronized_pile(cluster_state)
        if synchronized_pile_id is None:
            logger.error("Synchronized pile not found")
            return False

        if pile_id == primary_pile_id:
            updates = [
                bridge_api.PileStateUpdate(pile_id=primary_pile_id, state=bridge_api.PileState.DISCONNECTED),
                bridge_api.PileStateUpdate(pile_id=synchronized_pile_id, state=bridge_api.PileState.PRIMARY),
            ]
        else:
            updates = [
                bridge_api.PileStateUpdate(pile_id=synchronized_pile_id, state=bridge_api.PileState.DISCONNECTED),
                bridge_api.PileStateUpdate(pile_id=primary_pile_id, state=bridge_api.PileState.PRIMARY),
            ]

        result = self.update_cluster_state_result(updates)
        if result is not None:
            if pile_id == primary_pile_id:
                logger.info("Switched: pile %d DISCONNECTED, pile %d PRIMARY", primary_pile_id, synchronized_pile_id)
            else:
                logger.info("Switched: pile %d DISCONNECTED, pile %d PRIMARY", synchronized_pile_id, primary_pile_id)
            return True
        else:
            logger.error("Failed to switch bridge state")
            return False

    def send_not_synchronized(self, pile_id, specific_pile_ids=None):
        """
        Switch pile to unsynchronized from each distconf quorum pile.

        Args:
            pile_id: Pile ID which was disabled

        Returns:
            True if successful, False otherwise
        """

        cluster_state = self.get_cluster_state_result()
        if cluster_state is None:
            logger.error("Failed to get cluster state")
            return False

        primary_pile_id = self.get_primary_pile(cluster_state)

        # Create a single update to set the pile to NOT_SYNCHRONIZED
        updates = [
            bridge_api.PileStateUpdate(
                pile_id=pile_id,
                state=bridge_api.PileState.NOT_SYNCHRONIZED
            )
        ]

        # Pass specific pile IDs to target for quorum
        specific_pile_ids = [primary_pile_id, pile_id]

        # Use update_cluster_state directly to pass specific_pile_ids
        response = self.update_cluster_state(updates, specific_pile_ids)
        logger.debug("Update cluster state response: %s", response)

        if response.operation.status == StatusIds.SUCCESS:
            logger.info("Successfully sent NOT_SYNCHRONIZED to pile %d", pile_id)
            return True
        else:
            logger.error("Failed to send NOT_SYNCHRONIZED to pile %d: %s", pile_id, response.operation.status)
            return False

    def close(self):
        self._channel.close()

    def __del__(self):
        self.close()
