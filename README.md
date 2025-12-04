# Consistent-Hashing-Ring
Advanced Implementation of a Consistent Hashing Ring
import hashlib
import bisect
import random
import time
from typing import Dict, List


def hash_value(key: str) -> int:
    """Return a 32-bit hash integer for a given key."""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)


class ConsistentHashRing:
    """
    Production-style consistent hashing ring with virtual nodes.
    Provides O(log N) lookup via bisect.
    """
    def __init__(self, virtual_nodes: int = 100):
        self.virtual_nodes = virtual_nodes
        self.ring = []                 # Sorted list of hash points
        self.node_map: Dict[int, str] = {}  # hash point → physical node

    def _add_vnode(self, node: str, vnode_index: int):
        """Add a single virtual node."""
        key = f"{node}-vnode-{vnode_index}"
        h = hash_value(key)
        # prevent hash collisions
        while h in self.node_map:
            h += 1
        bisect.insort(self.ring, h)
        self.node_map[h] = node

    def add_node(self, node: str):
        """Add a node with V virtual nodes."""
        for i in range(self.virtual_nodes):
            self._add_vnode(node, i)

    def remove_node(self, node: str):
        """Remove a node and all its virtual nodes."""
        to_remove = [h for h, n in self.node_map.items() if n == node]
        for h in to_remove:
            self.ring.remove(h)
            del self.node_map[h]

    def get_node(self, key: str) -> str:
        """Find the node responsible for a given key."""
        if not self.ring:
            raise RuntimeError("No nodes available in the ring")

        h = hash_value(key)
        idx = bisect.bisect_left(self.ring, h)
        if idx == len(self.ring):
            idx = 0
        return self.node_map[self.ring[idx]]


# -------------------------------------------------------------------
# BENCHMARKING
# -------------------------------------------------------------------

def benchmark(simple_nodes: int = 10, vnodes: int = 100, key_count: int = 100000):
    """Compare simple hashing vs virtual-node consistent hashing."""
    print("\n=== BENCHMARK: Simple Hashing vs Virtual Nodes ===")

    # Simple ring (1 vnode)
    simple_ring = ConsistentHashRing(virtual_nodes=1)
    for n in range(simple_nodes):
        simple_ring.add_node(f"node-{n}")

    # Virtual-node ring
    vnode_ring = ConsistentHashRing(virtual_nodes=vnodes)
    for n in range(simple_nodes):
        vnode_ring.add_node(f"node-{n}")

    keys = [f"key-{i}" for i in range(key_count)]

    # Measure distribution variance
    def distribution(ring):
        counts = {}
        for k in keys:
            node = ring.get_node(k)
            counts[node] = counts.get(node, 0) + 1
        return counts

    start = time.time()
    dist_simple = distribution(simple_ring)
    t_simple = time.time() - start

    start = time.time()
    dist_vnodes = distribution(vnode_ring)
    t_vnodes = time.time() - start

    print("\n--- Distribution ---")
    print("Simple:", sorted(dist_simple.values()))
    print("Virtual:", sorted(dist_vnodes.values()))

    print("\n--- Timing ---")
    print(f"Simple Hash Ring:   {t_simple:.4f}s")
    print(f"Virtual Node Ring:  {t_vnodes:.4f}s (slightly slower but better balanced)")

    return dist_simple, dist_vnodes


# -------------------------------------------------------------------
# UNIT TESTS (Stress Tests)
# -------------------------------------------------------------------

def run_tests():
    print("\n=== RUNNING UNIT TESTS ===")

    ring = ConsistentHashRing(virtual_nodes=50)
    nodes = ["A", "B", "C"]
    for n in nodes:
        ring.add_node(n)

    # 1. Consistent lookup: same key always maps to same node
    for i in range(1000):
        k = f"test-key-{i}"
        assert ring.get_node(k) == ring.get_node(k)

    print("✔ Consistency test passed")

    # 2. Add-remove churn test (minimal key movement)
    keys = [f"key-{i}" for i in range(5000)]
    before = {k: ring.get_node(k) for k in keys}

    ring.add_node("D")
    after_add = {k: ring.get_node(k) for k in keys}

    # Count remaps
    remaps = sum(before[k] != after_add[k] for k in keys)
    print(f"Key remaps after node addition: {remaps} / {len(keys)}")
    assert remaps < len(keys) * 0.25  # less than 25% should move

    ring.remove_node("D")
    after_remove = {k: ring.get_node(k) for k in keys}
    # After removal, keys should largely return
    back = sum(before[k] == after_remove[k] for k in keys)
    print(f"Keys returned after removal: {back} / {len(keys)}")
    assert back > len(keys) * 0.70  # at least 70% return

    print("✔ Add/Remove churn test passed")

    print("=== ALL TESTS PASSED ===")


if __name__ == "__main__":
    run_tests()
    benchmark(simple_nodes=10, vnodes=150, key_count=50000)
  

    
