//! Link cuts between nodes: which node leads after each, and that every
//! acked op is kept once and in order.

use super::{body, propose_for, report, seeds, settle, started};
use crate::sim::{Faults, Sim, timing};

/// A little loss and reordering, so each scenario takes more than one path.
const LIGHT: Faults = Faults { drop: 2, jitter: 3 };

const fn view(sim: &Sim, i: usize) -> u64 {
    sim.nodes[i].as_ref().unwrap().view()
}

/// A leader with 20 committed ops on every node.
fn ready(seed: u64) -> (Sim, usize) {
    let mut sim = Sim::new(seed, LIGHT);
    started(&mut sim);
    assert_eq!(propose_for(&mut sim, 20, 2000), 20);
    assert!(
        settle(&mut sim, 2000),
        "seed {seed}: settled\n{}",
        report(&sim)
    );
    let leader = sim.leader().unwrap();
    (sim, leader)
}

/// Joins every link again and waits until all three hold every op.
fn heal(sim: &mut Sim, seed: u64) {
    for i in 0..3 {
        sim.isolate(i, false);
    }
    assert!(
        settle(sim, 10_000),
        "seed {seed}: settled after the heal\n{}",
        report(sim)
    );
    sim.check();
}

/// A cut that leaves the leader one follower both ways: only one node can
/// suspect it, so it keeps its view through several rounds of ops.
fn leader_keeps(cut: impl Fn(&mut Sim, usize)) {
    for seed in 0..seeds(10) {
        let (mut sim, leader) = ready(7000 + seed);
        let before = view(&sim, leader);
        cut(&mut sim, leader);
        for _ in 0..3 {
            assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
            sim.run(1000);
        }
        assert_eq!(sim.leader(), Some(leader), "seed {seed}\n{}", report(&sim));
        assert_eq!(view(&sim, leader), before, "seed {seed}: the view moved");
        heal(&mut sim, seed);
    }
}

#[test]
fn a_leader_cut_from_one_follower_keeps_leading() {
    leader_keeps(|sim, l| sim.cut_pair(l, (l + 1) % 3, true));
}

#[test]
fn followers_cut_from_each_other_leave_the_leader_alone() {
    leader_keeps(|sim, l| sim.cut_pair((l + 1) % 3, (l + 2) % 3, true));
}

#[test]
fn a_follower_deaf_to_the_leader_leaves_it_alone() {
    leader_keeps(|sim, l| sim.cut(l, (l + 1) % 3, true));
}

/// The leader loses one direction to both followers: it hears no ack when
/// `deaf`, or no follower hears it otherwise. It can neither commit nor
/// hold its lease, so the view moves while it runs, whether it took ops it
/// cannot commit (`pending`) or none.
fn one_sided_leader(deaf: bool, pending: bool) {
    for seed in 0..seeds(10) {
        let (mut sim, leader) = ready(7100 + seed);
        for f in (0..3).filter(|&f| f != leader) {
            if deaf {
                sim.cut(f, leader, true);
            } else {
                sim.cut(leader, f, true);
            }
        }
        if pending {
            for n in 0..5 {
                sim.propose(leader, body(9000 + n));
            }
        }
        assert!(
            sim.run_until(5000, |s| s.leader().is_some_and(|l| l != leader)),
            "seed {seed}: a one-sided leader still leads\n{}",
            report(&sim)
        );
        assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
        heal(&mut sim, seed);
    }
}

#[test]
fn a_leader_that_hears_no_follower_is_replaced() {
    one_sided_leader(true, true);
}

#[test]
fn an_idle_leader_that_hears_no_follower_is_replaced() {
    one_sided_leader(true, false);
}

#[test]
fn a_leader_no_follower_hears_is_replaced() {
    one_sided_leader(false, true);
}

#[test]
fn an_idle_leader_no_follower_hears_is_replaced() {
    one_sided_leader(false, false);
}

#[test]
fn a_full_split_commits_nothing_until_it_heals() {
    for seed in 0..seeds(10) {
        let (mut sim, leader) = ready(7200 + seed);
        for i in 0..3 {
            sim.isolate(i, true);
        }
        let acked = sim.acked.len();
        for n in 0..5 {
            sim.propose(leader, body(9000 + n));
        }
        sim.run(5000);
        assert_eq!(
            sim.acked.len(),
            acked,
            "seed {seed}: acked without a quorum"
        );
        assert_eq!(sim.leader(), None, "seed {seed}: a lone node leads");
        heal(&mut sim, seed);
        assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
        assert!(settle(&mut sim, 5000), "seed {seed}");
        sim.check();
    }
}

#[test]
fn cutting_off_each_node_in_turn_keeps_every_acked_op() {
    for seed in 0..seeds(10) {
        let (mut sim, _) = ready(7300 + seed);
        for round in 0..6 {
            let target = round % 3;
            sim.isolate(target, true);
            assert_eq!(
                propose_for(&mut sim, 20, 5000),
                20,
                "seed {seed} round {round}: the other two go on\n{}",
                report(&sim)
            );
            heal(&mut sim, seed);
        }
    }
}

#[test]
fn a_node_cut_off_from_the_start_joins_once_healed() {
    for seed in 0..seeds(10) {
        let mut sim = Sim::cut_off_from_start(7400 + seed, LIGHT, 2);
        started(&mut sim);
        assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
        heal(&mut sim, seed);
        for target in [0, 1] {
            sim.isolate(target, true);
            assert_eq!(
                propose_for(&mut sim, 20, 5000),
                20,
                "seed {seed}: node {target} cut off\n{}",
                report(&sim)
            );
            heal(&mut sim, seed);
        }
    }
}

#[test]
fn a_node_cut_off_from_the_start_forms_a_view_with_the_follower_it_hears() {
    for seed in 0..seeds(10) {
        let mut sim = Sim::cut_off_from_start(7500 + seed, LIGHT, 2);
        started(&mut sim);
        let leader = sim.leader().unwrap();
        let follower = 3 - leader - 2;
        assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
        sim.cut_pair(2, follower, false);
        sim.run(50);
        sim.isolate(leader, true);
        assert_eq!(
            propose_for(&mut sim, 20, 5000),
            20,
            "seed {seed}: the two go on\n{}",
            report(&sim)
        );
        heal(&mut sim, seed);
    }
}

/// Node `lag` misses three views the other two form, then hears only the
/// follower of the last one when its leader is cut off. The follower's
/// response brings it into that view at once, so the two elect within about
/// one election, not one per missed view.
#[test]
fn a_node_that_missed_views_joins_them_through_a_follower() {
    for seed in 0..seeds(10) {
        let (mut sim, first) = ready(7600 + seed);
        let lag = (first + 2) % 3;
        sim.isolate(lag, true);
        for _ in 0..3 {
            let leader = sim.leader().unwrap();
            let other = 3 - leader - lag;
            let before = view(&sim, leader);
            // Leader and follower lose each other, both suspect, then heal.
            sim.cut_pair(leader, other, true);
            sim.run(400);
            sim.cut_pair(leader, other, false);
            assert!(
                sim.run_until(30_000, |s| s.leader().is_some_and(|l| l != lag)
                    && view(s, leader) > before
                    && view(s, other) > before),
                "seed {seed}: the two moved on\n{}",
                report(&sim)
            );
        }
        let leader = sim.leader().unwrap();
        let follower = 3 - leader - lag;
        assert!(view(&sim, lag) + 3 <= view(&sim, leader), "seed {seed}");
        sim.cut_pair(lag, follower, false);
        sim.run(50);
        sim.isolate(leader, true);
        let start = sim.now;
        assert!(
            sim.run_until(5000, |s| s.leader().is_some_and(|l| l != leader)),
            "seed {seed}: the two never formed a view\n{}",
            report(&sim)
        );
        let took = sim.now.saturating_sub(start);
        assert!(
            took < timing().max_election_timeout,
            "seed {seed}: took {took:?}"
        );
        assert_eq!(propose_for(&mut sim, 20, 2000), 20, "seed {seed}");
        heal(&mut sim, seed);
    }
}
