use std::collections::HashMap;
use std::collections::{HashSet, VecDeque};
use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader};
// Data structure to represent a transaction node
struct TransactionNode {
    left: usize,
    right: usize,
    timestamp: usize,
}

fn parse_database(filename: &str) -> Vec<TransactionNode> {
    let file = File::open(filename).expect("Failed to open the file");
    let reader = BufReader::new(file);
    let mut nodes = Vec::new();

    for (line_number, line) in reader.lines().enumerate() {
        if line_number == 0 {
            continue; // Skip the first line (number of nodes)
        }

        let line = line.expect("Failed to read line from file");
        let parts: Vec<usize> = line
            .split_whitespace()
            .map(|s| s.parse().expect("Failed to parse number"))
            .collect();

        if parts.len() != 3 {
            panic!("Invalid input format in line {}", line_number + 1);
        }

        let left = parts[0];
        let right = parts[1];
        let timestamp = parts[2];

        let node = TransactionNode {
            left,
            right,
            timestamp,
        };

        nodes.push(node);
    }

    nodes
}

fn build_graph(nodes: &[TransactionNode]) -> HashMap<usize, Vec<usize>> {
    let mut graph = HashMap::new();

    for (node_index, node) in nodes.iter().enumerate() {
        let left_parent = node.left;
        let right_parent = node.right;
        let current_node = node_index + 1; // Node IDs are 1-based

        if left_parent > 0 {
            graph
                .entry(left_parent)
                .or_insert(Vec::new())
                .push(current_node);
        }

        if right_parent > 0 {
            graph
                .entry(right_parent)
                .or_insert(Vec::new())
                .push(current_node);
        }
    }

    graph
}

fn calculate_average_depth(graph: &HashMap<usize, Vec<usize>>) -> f64 {
    let mut total_depth = 0;
    let mut num_nodes = 0;

    // Perform BFS from the root node (Node ID 1)
    let root_node = 1;
    let mut visited = HashSet::new();
    let mut queue = VecDeque::new();
    queue.push_back((root_node, 0)); // (node, depth)

    while let Some((node, depth)) = queue.pop_front() {
        if !visited.contains(&node) {
            total_depth += depth;
            num_nodes += 1;
            visited.insert(node);

            if let Some(neighbors) = graph.get(&node) {
                for &neighbor in neighbors.iter() {
                    if !visited.contains(&neighbor) {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }
    }

    if num_nodes > 0 {
        total_depth as f64 / num_nodes as f64
    } else {
        0.0
    }
}

#[allow(unused_variables)]
fn calculate_average_txs_per_depth(
    nodes: &[TransactionNode],
    graph: &HashMap<usize, Vec<usize>>,
) -> f64 {
    let mut depth_txs: HashMap<usize, usize> = HashMap::new();
    let mut total_txs = 0;

    let root_node = 1;
    let mut visited = vec![false; nodes.len() + 1]; // +1 to account for 1-based node IDs
    let mut queue = VecDeque::new();
    queue.push_back((root_node, 0)); // (node, depth)

    while let Some((node, depth)) = queue.pop_front() {
        if !visited[node] {
            visited[node] = true;

            let tx_count = count_transactions_at_depth(nodes, depth);
            *depth_txs.entry(depth).or_insert(0) += tx_count;
            total_txs += tx_count;

            if let Some(neighbors) = graph.get(&node) {
                for &neighbor in neighbors.iter() {
                    if !visited[neighbor] {
                        queue.push_back((neighbor, depth + 1));
                    }
                }
            }
        }
    }

    // Calculate the average transactions per non-zero depth
    let num_non_zero_depths = depth_txs.len() - 1; // Excluding depth 0
    if num_non_zero_depths > 0 {
        let total_txs_non_zero_depths: usize = depth_txs.values().skip(1).sum();
        total_txs_non_zero_depths as f64 / num_non_zero_depths as f64
    } else {
        0.0
    }
}

fn count_transactions_at_depth(nodes: &[TransactionNode], depth: usize) -> usize {
    nodes.iter().filter(|&node| node.timestamp == depth).count()
}

#[allow(unused_variables)]
fn calculate_average_in_references(
    nodes: &[TransactionNode],
    graph: &HashMap<usize, Vec<usize>>,
) -> f64 {
    let mut in_reference_counts = vec![0; nodes.len()];

    for (node_index, node) in nodes.iter().enumerate() {
        if let Some(referenced_by) = graph.get(&(node_index + 1)) {
            for &referencing_node in referenced_by.iter() {
                in_reference_counts[referencing_node - 1] += 1;
            }
        }
    }

    let total_in_references: usize = in_reference_counts.iter().sum();
    total_in_references as f64 / nodes.len() as f64
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() != 2 {
        eprintln!("Usage: ./target/debug/my-project <database.txt>");
        std::process::exit(1);
    }

    let filename = &args[1];
    let nodes = parse_database(filename);
    for (index, node) in nodes.iter().enumerate() {
        println!(
            "Node {}: Left: {}, Right: {}, Timestamp: {}",
            index + 1,
            node.left,
            node.right,
            node.timestamp
        );
    }

    let graph = build_graph(&nodes);

    let average_depth = calculate_average_depth(&graph);
    println!("\n> AVG DAG DEPTH: {:.2}", average_depth);
    let average_txs_per_depth = calculate_average_txs_per_depth(&nodes, &graph);
    println!(
        "> AVG TXS PER DEPTH (excluding depth 0): {:.2}",
        average_txs_per_depth
    );
    let average_in_references = calculate_average_in_references(&nodes, &graph);
    println!("> AVG REF: {:.3}", average_in_references);
}
