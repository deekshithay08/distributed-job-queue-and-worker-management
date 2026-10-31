"""
PERSON 3 - path computation (pure Python, no SDN library needed, so it can be unit-tested).

The controller keeps a graph of switches and the links that are currently UP.  For every
destination host we run Dijkstra from the host's switch.  That gives, for EVERY switch, the
output port that leads toward that host along the cheapest path (a "shortest-path tree").
Installing "to reach host H, send out port P" on every switch gives loop-free forwarding.
"""
import heapq


def build_adjacency(links, link_is_up):
    """links: [(a, pa, b, pb, cost, name)].  link_is_up(name) -> bool.
    Returns adj[dpid] = [(neighbour, port_on_dpid, port_on_neighbour, cost, link_name), ...]"""
    adj = {}
    for a, pa, b, pb, cost, name in links:
        adj.setdefault(a, [])
        adj.setdefault(b, [])
        if link_is_up(name):
            adj[a].append((b, pa, pb, cost, name))
            adj[b].append((a, pb, pa, cost, name))
    return adj


def shortest_tree(adj, root):
    """Dijkstra from `root`.  Returns {dpid: (port_toward_root, parent_dpid, distance)}.
    root itself maps to (None, None, 0)."""
    best = {root: (None, None, 0)}
    heap = [(0, root)]
    done = set()
    while heap:
        dist, node = heapq.heappop(heap)
        if node in done:
            continue
        done.add(node)
        for nbr, port_on_node, port_on_nbr, cost, _name in sorted(adj.get(node, [])):
            nd = dist + cost
            if nbr not in best or nd < best[nbr][2]:
                best[nbr] = (port_on_nbr, node, nd)       # nbr reaches root by sending out port_on_nbr
                heapq.heappush(heap, (nd, nbr))
    return best


def compute_routes(links, hosts, link_is_up, switch_is_up, host_port_is_up):
    """Returns routes[host_name][dpid] = out_port   (only for switches that can reach the host).

    hosts: {name: {"dpid":.., "port":..}}"""
    live_links = [l for l in links if switch_is_up(l[0]) and switch_is_up(l[2])]
    adj = build_adjacency(live_links, link_is_up)
    routes = {}
    for name, h in hosts.items():
        dpid = h["dpid"]
        if not switch_is_up(dpid) or not host_port_is_up(name):
            routes[name] = {}
            continue
        tree = shortest_tree(adj, dpid)
        r = {}
        for sw, (port_toward_root, _parent, _dist) in tree.items():
            if not switch_is_up(sw):
                continue
            r[sw] = h["port"] if sw == dpid else port_toward_root
        routes[name] = r
    return routes, adj


def path_between(adj, src_dpid, dst_dpid):
    """List of dpids from src to dst along the cheapest path, or None if unreachable."""
    tree = shortest_tree(adj, dst_dpid)
    if src_dpid not in tree:
        return None
    path, node = [src_dpid], src_dpid
    while node != dst_dpid:
        node = tree[node][1]
        path.append(node)
    return path
