use std::{fmt::Debug, hash::Hash};

use standing_relations_2::CreationContext;

fn dijkstra<Node: Debug + Eq + Hash + Clone>(
    start: Node,
    end: Node,
    edge_weights: impl IntoIterator<Item = (Node, Node, usize)>,
) -> Option<usize> {
    let mut context = CreationContext::new();

    let (mut start_input, start_rel) = context.input::<Node>();
    let start_rel = start_rel.named("start");
    let (mut end_input, end_rel) = context.input::<Node>();
    let end_rel = end_rel.named("end");
    let (mut edges_input, edges_rel) = context.input::<(Node, Node, usize)>();
    let edges_rel = edges_rel.named("edges");

    let (distances_input, distances) = context.input::<(Node, usize)>();
    let distances = distances
        .concat(start_rel.map(|n| (n, 0)))
        .named("distances")
        .collect();

    let distance_to_end = distances
        .get()
        .semijoin(end_rel)
        .snds()
        .named("distance_to_end")
        .collect();
    let end_distance_output = context.output(distance_to_end.get());
    context.interrupt(0, distance_to_end.get());

    let next_distances = distances
        .get()
        .join(edges_rel.map(|(from, to, dist)| (from, (to, dist))))
        .map(|(_, prev_dist, (to, edge_dist))| (to, prev_dist + edge_dist))
        .antijoin(distances.get().fsts())
        .named("next_distances")
        .collect();

    let selection_distance = next_distances
        .get()
        .snds()
        .global_min()
        .named("selection_distance");

    let selected_next_distances = next_distances
        .get()
        .swaps()
        .semijoin(selection_distance)
        .swaps()
        .named("selected_next_distances");

    context.feedback(selected_next_distances, distances_input);

    let mut context = context.begin();

    start_input.send(start).unwrap();
    end_input.send(end).unwrap();
    for edge in edge_weights {
        edges_input.send(edge).unwrap();
    }

    match context.commit() {
        Ok(()) => None,
        Err(0) => Some(*end_distance_output.get().get_singleton().unwrap().0),
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
