"""Conservative Bully optimization: highest-first direct election handoff.

Based on PseudoToPython. All existing crash/recovery, timeout monitoring,
generation checks, and coordinator phases are preserved. A responding candidate
still verifies higher IDs itself before announcing leadership.
"""

import time
import threading
from enum import Enum, auto

ELECTION_TIMEOUT = 0.3  # seconds
MONITOR_INTERVAL = 0.5

_print_lock = threading.Lock()
 
def safe_print(msg):
    # Multiple threads print concurrently once messages are sent in parallel;
    # a lock keeps lines from interleaving mid-sentence.
    with _print_lock:
        print(msg)

class State(Enum):
    DOWN = auto()
    ELECTION = auto()
    REORGANIZATION = auto()
    NORMAL = auto()


class System:
    def __init__(self):
        self.nodes = []
        self._messages = 0
        self._election_messages = 0
        self._monitoring_messages = 0
        self._message_lock = threading.Lock()
        
    @property
    def message_count(self):
        with self._message_lock:
            return self._messages

    @property
    def election_message_count(self):
        with self._message_lock:
            return self._election_messages

    @property
    def monitoring_message_count(self):
        with self._message_lock:
            return self._monitoring_messages

    def addNode(self, node):
        if self.getNode(node.id) is not None:
            raise ValueError("Duplicate node ID")
        self.nodes.append(node)

    def getNodes(self):
        return self.nodes

    def getNode(self, node_id):
        # Look up a node by ID
        for node in self.nodes:
            if node.id == node_id:
                return node
        return None

    def getHigherNodes(self, node_id):
        return [node for node in self.nodes if node.id > node_id]

    def getLowerNodes(self, node_id):
        return [node for node in self.nodes if node.id < node_id]

    def send(self, to_id, method, *args, monitoring=False):
        with self._message_lock:
            self._messages += 1
            if monitoring:
                self._monitoring_messages += 1
            else:
                self._election_messages += 1

        target = self.getNode(to_id)

        if target is None or target.state == State.DOWN:
            time.sleep(ELECTION_TIMEOUT)
            raise TimeoutError(f"Node {to_id} did not respond")

        return getattr(target, method)(*args)

    def tick(self):
        # Et simuleret timer-tick: alle aktive noder kontrollerer systemet.
        for node in self.nodes:
            node.check()
            node.timeout()

    def run_for(self, seconds):
        # Timeren skal koeres for at opdage fejl uden manuelle valgkald.
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            self.tick()
            remaining = deadline - time.monotonic()
            if remaining > 0:
                time.sleep(min(MONITOR_INTERVAL, remaining))


    def time_until_normal(self, max_seconds=30):
        start = time.perf_counter()

        def election_complete():
            active = [
                node for node in self.nodes
                if node.state != State.DOWN
            ]
            if not active:
                return False
            expected = max(node.id for node in active)
            return all(
                node.state == State.NORMAL
                and node.leader_id == expected
                for node in active
            )
        while time.perf_counter() - start < max_seconds:
            if election_complete():
                return time.perf_counter() - start
            for node in self.nodes:
                node.check()
                if election_complete():
                    return time.perf_counter() - start
                node.timeout()
                if election_complete():
                    return time.perf_counter() - start
        raise TimeoutError("Election did not finish")


class Node:
    def __init__(self, id, system):
        self.id = id
        self.state = State.ELECTION  # Active, waiting for an initial election.
        self.leader_id = None
        self._election_running = False  # Local procedure execution guard.
        self._generation = 0  # Aendres, naar et igangvaerende valg skal afbrydes.
        self.last_halt_id = None
        self.system = system  # Reference to the system
        self.lock = threading.RLock()

    def are_you_there(self):
        with self.lock:
            if self.state == State.DOWN:
                raise TimeoutError(f"Node {self.id} is down")
            return True

    def are_you_normal(self):
        with self.lock:
            if self.state == State.DOWN:
                raise TimeoutError(f"Node {self.id} is down")
            return self.state == State.NORMAL

    def halt(self, i):
        with self.lock:
            if self.state == State.DOWN:
                raise TimeoutError(f"Node {self.id} is down")
            self._generation += 1
            self.last_halt_id = i
            self.state = State.ELECTION

    def new_coordinator(self, i):
        with self.lock:
            if self.state == State.ELECTION and self.last_halt_id == i:
                self.leader_id = i
                self.state = State.REORGANIZATION

    def ready(self, i):
        with self.lock:
            if self.state == State.REORGANIZATION and self.leader_id == i:
                self.state = State.NORMAL

    def crash(self):
        with self.lock:
            self._generation += 1
            self.state = State.DOWN

    def election_is_current(self, generation):
        with self.lock:
            return self.state != State.DOWN and self._generation == generation

    def is_alive(self, target_id, monitoring=False):
        # Returns true if the node with target id is alive
        try:
            return self.system.send(target_id, "are_you_there", monitoring=monitoring)
        except TimeoutError:
            return False

    def start_election(self):
        with self.lock:
            if self.state == State.DOWN or self._election_running:
                return
            self._election_running = True

        safe_print(f"Node {self.id}: Starting Election")
        try:
            while self.state != State.DOWN:
                with self.lock:
                    generation = self._generation
                # Optimization: find the highest responding node first.
                # Delegate to that candidate immediately. The candidate runs
                # start_election() itself and independently checks higher IDs.
                highest = None
                for node in sorted(self.system.getHigherNodes(self.id),
                                   key=lambda node: node.id, reverse=True):
                    if not self.election_is_current(generation):
                        return
                    if self.is_alive(node.id):
                        highest = node.id
                        break

                if not self.election_is_current(generation):
                    return

                if highest is not None:
                    try:
                        # The receiver checks its own higher nodes before
                        # becoming coordinator. No verification is skipped.
                        self.system.send(highest, "start_election")
                    except TimeoutError:
                        # Candidate may have crashed after answering.
                        # Retry discovery; the normal generation guard applies.
                        continue
                    return

                # No higher node responded: enter the original HALT,
                # NEW_COORDINATOR and READY coordinator sequence.
                with self.lock:
                    if not self.election_is_current(generation):
                        return
                    self.halt(self.id)
                    generation = self._generation
                # Et halt fra en anden kandidat eller et crash aendrer generationen.
                # Den gamle valgprocedure maa derefter ikke udpege sig selv.
                up = []
                for n in sorted(self.system.getLowerNodes(self.id),
                                key=lambda n: n.id, reverse=True):
                    if not self.election_is_current(generation):
                        return
                    try:
                        self.system.send(n.id, "halt", self.id)
                        up.append(n.id)
                    except TimeoutError:
                        continue

                with self.lock:
                    if not self.election_is_current(generation):
                        return
                    self.leader_id = self.id
                    self.state = State.REORGANIZATION

                try:
                    for j in up:
                        if not self.election_is_current(generation):
                            return
                        self.system.send(j, "new_coordinator", self.id)
                    # Reorganization is empty in this simulation.
                    for j in up:
                        if not self.election_is_current(generation):
                            return
                        self.system.send(j, "ready", self.id)
                except TimeoutError:
                    safe_print(f"Node {self.id}: Participant failed; retrying election")
                    continue

                with self.lock:
                    if not self.election_is_current(generation):
                        return
                    self.ready(self.id)
                if self.state == State.NORMAL:
                    safe_print(f"Node {self.id} is coordinator")
                return
        finally:
            with self.lock:
                self._election_running = False

    def check(self):
        with self.lock:
            if self.state != State.NORMAL or self.leader_id != self.id:
                return
        for node in self.system.getNodes():
            if node.id == self.id:
                continue
            try:
                normal = self.system.send(node.id, "are_you_normal", monitoring=True)
            except TimeoutError:
                continue
            if not normal:
                self.start_election()
                return

    def timeout(self):
        # Følgere skal også overvåge lederen; en crashet leder kan ikke selv.
        with self.lock:
            if self.state == State.DOWN or self._election_running:
                return
            state, leader_id = self.state, self.leader_id
        if state in (State.NORMAL, State.REORGANIZATION):
            if leader_id is not None and self.is_alive(leader_id, monitoring=True):
                return
        self.start_election()
    
    def recover(self):
        with self.lock:
            if self.state != State.DOWN:
                return
            self.state = State.ELECTION
            self._generation += 1
            self.last_halt_id = None
        self.start_election()

if __name__ == "__main__":
    system = System()
    N = 5
    crashed_nodes = [5]
    for node_id in range(1, N + 1):
        system.addNode(Node(node_id, system))
    for node_id in crashed_nodes:
        system.getNode(node_id).crash()
    elapsed = system.time_until_normal()
    print(f"Nodes: {N}, crashed: {crashed_nodes}")
    print(f"Election time: {elapsed:.4f} s")
    print(f"Election RPC attempts: {system.election_message_count}")
    print(f"Monitoring RPC attempts: {system.monitoring_message_count}")
    print(f"Total RPC attempts: {system.message_count}")
    for node in system.getNodes():
        print(f"Node {node.id}: {node.state.name}, leader={node.leader_id}")
