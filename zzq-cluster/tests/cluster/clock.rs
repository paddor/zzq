//! Clock samples between nodes: what each node makes of its own clock.

use super::{CLEAN, started};
use crate::sim::{Sim, WALL};

const HOUR: u64 = 3_600_000;

fn majority(sim: &Sim, i: usize) -> Option<i64> {
    sim.nodes[i].as_ref().unwrap().clock().majority
}

#[test]
fn nodes_with_right_clocks_agree_and_need_no_correction() {
    let mut sim = Sim::new(1, CLEAN);
    started(&mut sim);
    sim.run(100);
    for i in 0..3 {
        let clock = sim.nodes[i].as_ref().unwrap().clock();
        assert_eq!(clock.majority, Some(0), "node {i}");
        for (peer, offset) in clock.offsets {
            let offset = offset.unwrap_or_else(|| panic!("node {i} has no sample of {peer:?}"));
            assert!(offset.abs() < 10, "node {i}: {offset} ms");
        }
    }
}

#[test]
fn a_node_with_a_wrong_clock_takes_the_others_time() {
    let mut sim = Sim::with_clocks(2, CLEAN, [WALL, WALL, WALL + HOUR]);
    started(&mut sim);
    sim.run(100);
    let hour = i64::try_from(HOUR).unwrap();
    let off = majority(&sim, 2).expect("a majority");
    assert!((off + hour).abs() < 10, "{off}");
    for i in 0..2 {
        assert_eq!(majority(&sim, i), Some(0), "node {i} keeps its own clock");
    }
}

#[test]
fn two_nodes_whose_clocks_disagree_have_no_majority_until_the_third_responds() {
    let mut sim = Sim::with_clocks(3, CLEAN, [WALL, WALL + 5000, WALL]);
    sim.isolate(2, true);
    started(&mut sim);
    sim.run(100);
    assert_eq!((majority(&sim, 0), majority(&sim, 1)), (None, None));
    sim.isolate(2, false);
    sim.run(100);
    assert_eq!(majority(&sim, 0), Some(0));
    assert!(majority(&sim, 1).is_some_and(|off| (off + 5000).abs() < 10));
}

#[test]
fn a_restarted_node_samples_its_peers_at_once() {
    let mut sim = Sim::new(4, CLEAN);
    started(&mut sim);
    sim.run(100);
    sim.crash(2);
    sim.run(6000);
    assert!(majority(&sim, 0).is_some(), "node 1 still agrees with it");
    sim.start(2);
    assert!(
        sim.run_until(50, |s| s.nodes[2]
            .as_ref()
            .unwrap()
            .clock()
            .majority
            .is_some()),
        "a reading within a few round trips"
    );
}
