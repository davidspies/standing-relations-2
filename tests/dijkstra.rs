use std::{fmt::Debug, hash::Hash};

use standing_relations_2::CreationContext;

fn dijkstra<Node: Debug + Eq + Hash + Clone>(
    start: Node,
    end: Node,
    edge_weights: impl IntoIterator<Item = (Node, Node, usize)>,
) -> Option<usize> {
    let mut context = CreationContext::new();

    let (mut start_input, start_rel) = context.input::<Node>();
    let (mut end_input, end_rel) = context.input::<Node>();
    let (mut edges_input, edges_rel) = context.input::<(Node, Node, usize)>();

    let (path_input, path_len) = context.input::<(Node, usize)>();
    let path_len = path_len.concat(start_rel.map(|n| (n, 0)));
    let min_path = path_len.mins().dynamic().save();

    let path_to_end = min_path.get().semijoin(end_rel).snds().dynamic().save();
    let end_path_output = context.output(path_to_end.get());
    context.interrupt(0, path_to_end.get());

    let next_path = min_path
        .get()
        .join(edges_rel.map(|(from, to, dist)| (from, (to, dist))))
        .map(|(_, prev_dist, (to, edge_dist))| (prev_dist + edge_dist, to));

    let (path_distance_input, path_distances) = context.input::<usize>();

    let larger_next_paths = next_path.antijoin(path_distances).dynamic().save();

    let next_path_distance = larger_next_paths.get().fsts().global_min().dynamic().save();

    let actual_next_paths = larger_next_paths
        .get()
        .semijoin(next_path_distance.get())
        .swaps();

    context.feedback(actual_next_paths, path_input);

    context.feedback(next_path_distance.get(), path_distance_input);

    let mut context = context.begin();

    start_input.send(start).unwrap();
    for edge in edge_weights {
        edges_input.send(edge).unwrap();
    }
    end_input.send(end).unwrap();

    match context.commit() {
        Ok(()) => None,
        Err(0) => Some(*end_path_output.get().iter().next().unwrap().0),
        Err(_) => unreachable!(),
    }
}

#[test]
fn test_dijkstra() {
    let dist = dijkstra(
        'A',
        'F',
        vec![
            ('A', 'B', 1),
            ('A', 'C', 2),
            ('A', 'F', 7),
            ('B', 'D', 2),
            ('C', 'E', 3),
            ('D', 'A', 1),
            ('D', 'E', 1),
            ('E', 'A', 1),
            ('E', 'F', 1),
        ],
    );

    assert_eq!(dist, Some(5));
}
