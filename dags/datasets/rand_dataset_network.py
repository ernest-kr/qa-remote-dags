import json
from dataclasses import dataclass
from itertools import product
from math import ceil, floor
from random import choice, randint, seed
from textwrap import dedent, indent

import click
import networkx as nx
from dataclasses_json import dataclass_json
from networkx import DiGraph, connected_components, erdos_renyi_graph, nx_pydot


# generate names like A1 A2 A3...
def get_names(n, prefix="A"):
    return [f"{prefix}{i}" for i in range(1, n + 1)]


def rand_graph(node_ct, edge_probability, min_islands, rand_seed):
    """
    Randomly generate a graph.

    Later we'll generate a dag for each node and a dataset for each
    edge
    """

    if rand_seed:
        seed(rand_seed)
    else:
        rand_seed = randint(0, 999)
        print("random seed:", rand_seed)
        seed(rand_seed)

    out_graph = None

    remaining_node_ct = node_ct  # terminate when == 0
    island_ct = 0  # use to decide how many nodes to generate
    prefix_offset = 0  # use to give island nodes different names

    while remaining_node_ct > 0:
        print(remaining_node_ct)

        # how many nodes to generate?
        if island_ct < min_islands - 1:
            # if this isn't the last island
            # generate between 20% and 80% of the remaining nodes
            node_range = (
                max(floor(0.4 * remaining_node_ct), 1),
                max(ceil(0.8 * remaining_node_ct), 1),
            )
            print(node_range)
            this_pass_node_ct = randint(node_range[0], node_range[1])
        else:
            # if it is, generate all remaining nodes
            this_pass_node_ct = remaining_node_ct

        if this_pass_node_ct <= 2:
            # don't create 1-node graphs
            edge_probability = 1
            this_pass_node_ct = 2

        edge_ct = 0
        while edge_ct == 0:
            print(f"generating {this_pass_node_ct} nodes with edge_probability {edge_probability} and seed {rand_seed}")
            g = erdos_renyi_graph(this_pass_node_ct, edge_probability, rand_seed)
            edge_ct = len(g.edges())
            if not edge_ct:
                edge_probability += 0.1
                rand_seed += 1
                print(indent("no edges, try again", prefix="    "))
        islands = [g.subgraph(c).copy() for c in connected_components(g)]

        # process each island separately
        isl_ct = len(islands)
        if prefix_offset + isl_ct > 26:
            raise Exception(f"got {len(islands)} islands, can't use letters to index them")

        # start all names for this island with the same letter
        indices = "abcdefghijklmnopqrstuvwxyz"[prefix_offset : prefix_offset + isl_ct]

        for prefix, subgraph in zip(indices, islands):
            prefix_offset += 1
            island_ct += 1
            print(indent("Island " + str(subgraph), prefix="    "))

            dg = DiGraph()
            node_names = list(get_names(node_ct, prefix=prefix))
            for one_node, other_node in subgraph.edges():
                # no self-connectedness
                if one_node == other_node:
                    pass

                # randomly resolve bidirectional edge-pairs to just one direction
                if subgraph.has_edge(one_node, other_node):
                    if choice(["to", "fro"]) == "to":
                        dg.add_edge(node_names[one_node], node_names[other_node])
                    else:
                        dg.add_edge(node_names[other_node], node_names[one_node])

            nodes_added = len(dg.nodes())
            remaining_node_ct -= nodes_added

            # merge islands into one
            if not out_graph:
                out_graph = dg
            else:
                out_graph = nx.compose(out_graph, dg)

    return out_graph


@dataclass_json
@dataclass
class Task:
    task_id: str
    outlet_dataset: str


@dataclass_json
@dataclass
class Dag:
    dag_id: str
    starter: bool
    inlet_datasets: str
    tasks: list[Task]


@dataclass_json
@dataclass
class DatasetNetwork:
    datasets: set[str]
    dag_families: dict[str, list[Dag]]
    longest_path: list[str]


def get_dags(g: DiGraph):
    """
    Given a graph where the nodes are datasets, make a graph where
    the nodes are dags and the edges are datasets.
    """
    sources = set()
    targets = set()
    for node in g.nodes():
        if not g.in_edges(node):
            sources.add(node)
        if not g.out_edges(node):
            targets.add(node)
    print("targets", targets)

    # walk all cycles, mark them as such
    for cycle in nx.simple_cycles(g):
        for node in cycle:
            g.nodes[node]["type"] = "cycle"
            g.nodes[node]["start"] = False

        # paths can originate from and terminate at a cycle
        sources.add(cycle[0])
        targets.add(cycle[0])
        print("    cycle:", cycle)

    # walk all paths which don't start where they end
    longest_path = []
    for source, target in product(sources, targets):
        for path in nx.all_simple_paths(g, source, target):
            if len(path) > len(longest_path):
                longest_path = path
            for node in path:
                try:
                    if g.nodes[node]["type"] == "cycle":
                        g.nodes[node]["type"] = "both"
                except KeyError:
                    g.nodes[node]["type"] = "path"
                g.nodes[node]["start"] = False

        for path in nx.all_simple_paths(g, source, target):
            print("    path:", source, target, path)
            path[0]
            g.nodes[path[0]]["start"] = True

    # if cycles wouldn't be triggered by a path, add an arbitrary start point
    # try to add as few as possible
    needs_starters = []

    # walk the cycles again.
    for i, cycle in enumerate(nx.simple_cycles(g)):
        print(cycle)

        # which need starters?
        types = set()
        for node in cycle:
            types.add(g.nodes[node]["type"])
        if "both" in types:
            continue
        else:
            needs_starters.append(set(cycle))

    # most common nodes should be starters
    # add them until no more cycles need starters
    while needs_starters:
        freq_by_nodes = {}
        for cycle in needs_starters:
            for node in cycle:
                freq_by_nodes.setdefault(node, 0)
                freq_by_nodes[node] += 1
        nodes_by_freq = {v: k for k, v in freq_by_nodes.items()}

        starter = nodes_by_freq[max(nodes_by_freq)]

        g.nodes[starter]["start"] = True
        del freq_by_nodes[starter]
        needs_starters = [x for x in needs_starters if starter not in x]

    # tell the user what we found
    for node in g.nodes():
        print(f"    {node:20}", g.nodes[node])

    # make sure we have classified each node
    for node in g.nodes():
        if "type" not in g.nodes[node]:
            # my gut says this should never happen
            # what kind of graph has an element that is
            # neither part of a tree nor part of a cycle?
            raise ValueError(f"untyped node: {node}")
    print("Longest Path:", longest_path)

    # understand the diGraph as a bunch of dags
    datasets = set()
    dag_families = {}

    def add_dag(node):
        print(f"adding dag for node: {node}")
        dag_id = node
        family = node[0].upper()
        starter = False

        # names are different if it's a starter dag
        node_data = g.nodes[node]
        if node_data["start"]:
            dag_id = f"start_{node}"
            starter = True

        # inbound stuff (might have no inlets)
        dag = Dag(
            dag_id=dag_id,
            starter=starter,
            inlet_datasets=[f"{x[0]}_{x[1]}" for x in g.in_edges(node)],
            tasks=[],
        )
        for inlet in dag.inlet_datasets:
            datasets.add(inlet)

        # outbound stuff
        for this_dag, next_dag in g.out_edges(node):
            dataset_str = f"{this_dag}_{next_dag}"
            task = Task(task_id=dataset_str, outlet_dataset=dataset_str)
            dag.tasks.append(task)
            datasets.add(dataset_str)

        dag_families.setdefault(family, []).append(dag)

    for node in g.nodes():
        add_dag(node)

    return DatasetNetwork(datasets=datasets, dag_families=dag_families, longest_path=longest_path)


def gen_dags(dataset_network: DatasetNetwork):
    header = dedent(
        """
        from airflow import Dataset, DAG
        from airflow.providers.standard.operators.python import PythonOperator
        from datetime import datetime, timedelta
        from math import ceil
        import pause

        def wait_until_twenty_sec():
            now = datetime.now()
            minutes = 0
            if now.second < 15:
                seconds = 20
            elif now.second < 35:
                seconds = 40
            elif now.second < 55:
                seconds = 0
                minutes = +1
            else:
                seconds = 20
                minutes += 1
            go_time = now + timedelta(minutes=minutes)
            go_time = datetime(
                year=go_time.year,
                month=go_time.month,
                day=go_time.day,
                hour=go_time.hour,
                minute=go_time.minute,
                second=seconds,
            )
            print(f"waiting until {go_time}")
            pause.until(go_time)

        """
    ).strip()

    def dag_def(dag: Dag, file_datasets):
        # todo: maybe jinja templates would be better than fstrings here?

        # things we'll need to define the dag
        ####

        # dag kwargs
        kwargs = "start_date=datetime(1970, 1, 1),"

        if dag.inlet_datasets:
            schedule = ", ".join(dag.inlet_datasets)
            kwargs += f"schedule=[{schedule}],"
        else:
            kwargs += "schedule=None,"

        # used to declare the dataset objects
        for dataset in dag.inlet_datasets:
            file_datasets.add(dataset)

        # things we'll need to define the tasks
        ####

        # task defs and declarations
        task_defs = [
            f"PythonOperator(task_id='{task.task_id}', python_callable=wait_until_twenty_sec, outlets=[{task.outlet_dataset}])"
            for task in dag.tasks
        ]
        if not task_defs:
            task_defs = ["PythonOperator(task_id='no_outlets', python_callable=wait_until_twenty_sec)"]

        # used to declare the datasets
        for dataset in [task.outlet_dataset for task in dag.tasks]:
            file_datasets.add(dataset)

        task_defstr = "\n" + "\n".join(task_defs) + "\n"
        return "\n".join(
            [
                f'with DAG(dag_id="{dag.dag_id}",',
                indent(kwargs, prefix="         "),
                f") as {dag.dag_id}:",
                indent(task_defstr, prefix="    "),
            ]
        )

    # generate a dag file for each family of dags
    wrote = []
    for family, dags in dataset_network.dag_families.items():
        file_datasets = set()
        dag_defs = [dag_def(dag, file_datasets) for dag in dags]
        dataset_defstr = "\n".join([f'{d} = Dataset("{d}")' for d in file_datasets])
        dag_defstr = "\n".join(dag_defs)

        content = "\n\n".join([header, dataset_defstr, dag_defstr])

        fname = f"dataset_network_{family}.py"
        wrote.append(fname)
        with open(fname, "w") as f:
            f.write(content)
        print(f"wrote {fname}")

    return wrote


def write_png(g, filename):
    p = nx_pydot.to_pydot(g)
    print(f"writing {filename}")
    p.write_png(filename)


def main(nodes, edge_probability, min_islands, rand_seed):
    g = rand_graph(nodes, edge_probability, min_islands, rand_seed)
    # these pngs get so big that they're not worth rendering
    if len(g.nodes()) < 100:
        write_png(g, "rand_digraph.png")
    dags = get_dags(g)
    files = gen_dags(dags)
    report = dags.to_dict()
    report["files"] = files
    with open("rand_digraph.json", "w") as f:
        f.write(json.dumps(report))


@click.command()
@click.option("--node-count", type=int, default=15)
@click.option("--edge-prob", type=float, default=0.25)
@click.option("--min-islands", type=int, default=2)
@click.option("--rand-seed", type=int, default=0)
def cli(node_count, edge_prob, min_islands, rand_seed):
    main(node_count, edge_prob, min_islands, rand_seed)


if __name__ == "__main__":
    cli()
