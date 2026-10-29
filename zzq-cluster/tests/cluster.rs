//! Three nodes replicating through a simulated network.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    reason = "test code"
)]

#[path = "cluster/sim.rs"]
mod sim;

use bytes::Bytes;
use sim::{Faults, Sim, timing};
use zzq_cluster::OpNumber;
use zzq_cluster::node::{LogLimits, Node, Role};

const CLEAN: Faults = Faults { drop: 0, jitter: 0 };

fn body(n: u64) -> Bytes {
    Bytes::from(format!("op {n}"))
}

#[test]
fn the_first_node_leads_a_new_cluster_and_the_others_follow() {
    let mut sim = Sim::new(1, CLEAN);
    sim.run(5);
    assert_eq!(sim.leader(), Some(0));
    assert_eq!(sim.apps[1].role, Some(Role::Follower));
    assert_eq!(sim.apps[2].role, Some(Role::Follower));
}

#[test]
fn proposed_ops_commit_and_every_follower_applies_them_in_order() {
    let mut sim = Sim::new(2, CLEAN);
    sim.run(5);
    for n in 1..=200 {
        assert!(sim.propose(0, body(n)));
        if n % 10 == 0 {
            sim.step();
        }
    }
    assert!(sim.run_until(1000, |s| s.apps[1].ops.len() == 200
        && s.apps[2].ops.len() == 200));
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(200)));
    assert_eq!(sim.acked.len(), 200);
    sim.check();
    assert_eq!(sim.apps[1].ops, sim.apps[0].ops);
}

#[test]
fn a_full_pipeline_pushes_back_until_followers_ack() {
    let mut sim = Sim::new(3, CLEAN);
    sim.run(5);
    let mut taken = 0;
    while sim.propose(0, body(taken)) {
        taken += 1;
        assert!(taken <= 64, "the pipeline holds 64 ops");
    }
    assert_eq!(taken, 64);
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(64)));
    assert!(sim.propose(0, body(64)));
}

#[test]
fn one_follower_down_does_not_stop_commits() {
    let mut sim = Sim::new(4, CLEAN);
    sim.run(5);
    sim.isolate(2, true);
    for n in 1..=50 {
        assert!(sim.propose(0, body(n)));
    }
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(50)));
    assert_eq!(sim.apps[2].ops.len(), 0);
    sim.isolate(2, false);
    assert!(sim.run_until(500, |s| s.apps[2].ops.len() == 50));
    sim.check();
}

#[test]
fn lost_and_late_prepares_are_resent_until_followers_hold_every_op() {
    for seed in 0..20 {
        let mut sim = Sim::new(
            100 + seed,
            Faults {
                drop: 10,
                jitter: 5,
            },
        );
        sim.run(5);
        let mut n = 0;
        while n < 300 {
            if sim.propose(0, body(n)) {
                n += 1;
            } else {
                sim.step();
            }
        }
        let done = sim.run_until(5000, |s| {
            s.apps[0].committed == OpNumber(300)
                && s.apps[1].ops.len() == 300
                && s.apps[2].ops.len() == 300
        });
        assert!(done, "seed {seed}: replication stalled");
        assert_eq!(sim.leader(), Some(0), "seed {seed}: no election needed");
        sim.check();
    }
}

/// Proposes on whichever node leads until `n` ops were taken or `ticks`
/// passed.
fn propose_for(sim: &mut Sim, n: u64, ticks: u64) -> u64 {
    let mut taken = 0;
    for _ in 0..ticks {
        if taken == n {
            break;
        }
        if let Some(l) = sim.leader()
            && sim.propose(l, body(1000 + sim.acked.len() as u64 * 7 + taken))
        {
            taken += 1;
            continue;
        }
        sim.step();
    }
    taken
}

#[test]
fn a_new_leader_takes_over_when_the_leader_is_cut_off() {
    let mut sim = Sim::new(5, CLEAN);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 50, 100), 50);
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(50)));
    sim.isolate(0, true);
    assert!(
        sim.run_until(2000, |s| s.leader().is_some_and(|l| l != 0)),
        "the other two elect a leader"
    );
    let leader = sim.leader().unwrap();
    assert_eq!(sim.apps[leader].ops.len(), 50);
    assert_eq!(propose_for(&mut sim, 50, 1000), 50);
    assert!(sim.run_until(1000, |s| s.apps[leader].committed == OpNumber(100)));
    sim.isolate(0, false);
    assert!(
        sim.run_until(3000, |s| s.apps[0].ops.len() == 100
            && s.apps[0].role == Some(Role::Follower)),
        "the old leader follows and catches up"
    );
    sim.check();
    assert_eq!(sim.resets[0], 0, "all it held was committed");
}

#[test]
fn a_cut_off_leader_drops_what_never_committed() {
    let mut sim = Sim::new(6, CLEAN);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 10, 100), 10);
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(10)));
    sim.isolate(0, true);
    for n in 0..5 {
        assert!(sim.propose(0, body(500 + n)), "it still leads, alone");
    }
    assert!(sim.run_until(2000, |s| s.leader().is_some_and(|l| l != 0)));
    assert_eq!(propose_for(&mut sim, 20, 1000), 20);
    let leader = sim.leader().unwrap();
    assert!(sim.run_until(1000, |s| s.apps[leader].committed == OpNumber(30)));
    sim.isolate(0, false);
    assert!(sim.run_until(3000, |s| s.apps[0].ops.len() == 30
        && s.apps[0].role == Some(Role::Follower)));
    assert_eq!(sim.resets[0], 1, "its 5 lone ops are gone");
    assert_eq!(sim.apps[0].ops, sim.apps[leader].ops);
    sim.check();
}

#[test]
fn seeded_splits_drops_and_delays_never_lose_an_acked_op() {
    for seed in 0..seeds(100) {
        eprintln!("seed {seed}");
        let mut sim = Sim::new(1000 + seed, Faults { drop: 5, jitter: 8 });
        sim.run(5);
        for round in 0..20 {
            let cut = sim.rng.below(5) as usize;
            for i in 0..3 {
                sim.isolate(i, false);
            }
            if cut < 3 {
                sim.isolate(cut, true);
            } else if cut == 3 {
                // One way only.
                let a = sim.rng.below(3) as usize;
                let b = (a + 1 + sim.rng.below(2) as usize) % 3;
                sim.links[a][b] = false;
                sim.timeline.push(format!("{:?} cut {a} to {b}", sim.now));
            }
            propose_for(&mut sim, 20, 300);
            let _ = round;
        }
        for i in 0..3 {
            sim.isolate(i, false);
        }
        sim.faults = CLEAN;
        let total = |s: &Sim| s.leader().map(|l| s.apps[l].ops.len());
        let done = sim.run_until(10_000, |s| {
            total(s).is_some_and(|n| {
                (0..3).all(|i| {
                    s.apps[i].ops.len() == n
                        && s.nodes[i].as_ref().unwrap().committed().op.0 as usize == n
                })
            })
        });
        assert!(done, "seed {seed}: the cluster did not settle");
        sim.check();
        assert!(!sim.acked.is_empty(), "seed {seed}: something committed");
    }
}

#[test]
fn a_cut_off_leader_whose_log_moved_on_rebuilds_from_an_image() {
    let small = LogLimits {
        max_operations: 32,
        max_body_bytes: 1 << 20,
    };
    let mut sim = Sim::with_log(7, CLEAN, small);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 40, 200), 40);
    assert!(sim.run_until(200, |s| s.apps[0].committed == OpNumber(40)));
    sim.isolate(0, true);
    for n in 0..5 {
        assert!(sim.propose(0, body(700 + n)));
    }
    assert!(sim.run_until(2000, |s| s.leader().is_some_and(|l| l != 0)));
    assert_eq!(propose_for(&mut sim, 100, 3000), 100);
    let leader = sim.leader().unwrap();
    assert!(sim.run_until(1000, |s| s.apps[leader].committed == OpNumber(140)));
    sim.isolate(0, false);
    assert!(
        sim.run_until(5000, |s| s.apps[0].ops.len() == 140
            && s.apps[0].role == Some(Role::Follower)),
        "the old leader rebuilds and catches up"
    );
    assert_eq!((sim.resets[0], sim.images[0]), (1, 1));
    assert_eq!(sim.apps[0].ops, sim.apps[leader].ops);
    assert_eq!(propose_for(&mut sim, 10, 1000), 10);
    assert!(sim.run_until(1000, |s| s.apps[0].ops.len() == 150));
    sim.check();
}

/// Every node holds state close to the leader: losing one more keeps
/// two that can serve recovery.
#[test]
fn a_rejoining_node_gets_no_commit_per_round_trip_while_it_catches_up() {
    let mut sim = Sim::new(5, CLEAN);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 10, 100), 10);
    assert!(sim.run_until(100, |s| s.apps[0].committed == OpNumber(10)));
    sim.isolate(0, true);
    assert!(sim.run_until(2000, |s| s.leader().is_some_and(|l| l != 0)));
    let leader = sim.leader().unwrap();
    assert_eq!(propose_for(&mut sim, 10, 1000), 10);
    assert!(sim.run_until(1000, |s| s.apps[leader].committed == OpNumber(20)));
    sim.isolate(0, false);
    sim.trace = true;
    let start = sim.now;
    assert!(
        sim.run_until(1000, |s| s.apps[0].ops.len() == 20),
        "it catches up"
    );
    let commits = sim
        .traced
        .iter()
        .filter(|&&(from, to, op)| from == leader && to == 0 && op == 0x34)
        .count();
    // A heartbeat, and at most one commit of the tail it installed.
    let heartbeats =
        (sim.now.saturating_sub(start).as_millis() / timing().heartbeat.as_millis()) as usize + 1;
    assert!(
        commits <= 2 * heartbeats,
        "{commits} commits in {heartbeats} heartbeats"
    );
}

#[test]
fn leadership_moves_around_the_ring_as_each_leader_is_cut_off_in_turn() {
    let mut sim = Sim::new(5, CLEAN);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 4, 100), 4);
    for next in [1, 2, 0] {
        let l = sim.leader().unwrap();
        sim.isolate(l, true);
        let led = sim.run_until(5000, |s| s.leader() == Some(next));
        let st: Vec<_> = sim
            .nodes
            .iter()
            .map(|n| n.as_ref().map(Node::status))
            .collect();
        assert!(
            led,
            "node {next} leads at {:?}: {st:?}\n{}",
            sim.now,
            report(&sim)
        );
        sim.isolate(l, false);
    }
}

fn caught_up(sim: &Sim) -> bool {
    let Some(l) = sim.leader() else {
        return false;
    };
    let lead = sim.nodes[l].as_ref().map_or(0, |n| n.committed().op.0);
    // A node whose application rebuilds holds no state to recover from.
    sim.nodes.iter().all(|n| {
        n.as_ref().is_some_and(|n| {
            let s = n.status();
            !s.recovering && s.committed.0 + 16 >= lead && s.applied.0 + 16 >= lead
        })
    })
}

const fn small_log() -> LogLimits {
    LogLimits {
        max_operations: 32,
        max_body_bytes: 1 << 20,
    }
}

/// Runs until every node follows or leads with all `n` ops applied.
fn settle(sim: &mut Sim, ticks: u64) -> bool {
    sim.run_until(ticks, |s| {
        s.leader().is_some_and(|l| {
            let n = s.apps[l].ops.len();
            (0..3).all(|i| {
                s.nodes[i]
                    .as_ref()
                    .is_some_and(|node| !node.recovering() && node.committed().op.0 as usize == n)
                    && s.apps[i].ops.len() == n
            })
        })
    })
}

#[test]
fn a_crashed_follower_recovers_from_the_others() {
    for log in [LogLimits::default(), small_log()] {
        let mut sim = Sim::with_log(8, CLEAN, log);
        sim.run(5);
        assert_eq!(propose_for(&mut sim, 100, 500), 100);
        assert!(sim.run_until(200, |s| s.apps[0].committed == OpNumber(100)));
        sim.crash(2);
        assert_eq!(propose_for(&mut sim, 50, 500), 50, "two nodes commit");
        sim.restart(2);
        assert!(settle(&mut sim, 5000), "the restarted node catches up");
        assert_eq!(
            sim.leader(),
            Some(0),
            "a follower's restart moves no leader"
        );
        assert_eq!(sim.apps[2].ops, sim.apps[0].ops);
        assert_eq!(propose_for(&mut sim, 10, 500), 10);
        assert!(settle(&mut sim, 1000));
        sim.check();
    }
}

#[test]
fn a_crashed_leader_is_replaced_and_recovers_as_a_follower() {
    let mut sim = Sim::with_log(9, CLEAN, small_log());
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 100, 500), 100);
    assert!(sim.run_until(200, |s| s.apps[0].committed == OpNumber(100)));
    sim.crash(0);
    assert!(sim.run_until(2000, |s| s.leader().is_some()));
    assert_eq!(propose_for(&mut sim, 50, 1000), 50);
    sim.restart(0);
    assert!(settle(&mut sim, 5000));
    assert_ne!(sim.leader(), Some(0));
    sim.check();
}

#[test]
fn seeded_crashes_and_splits_never_lose_an_acked_op() {
    for seed in 0..seeds(40) {
        eprintln!("seed {seed}");
        let mut sim = Sim::with_log(5000 + seed, Faults { drop: 3, jitter: 5 }, small_log());
        sim.run(5);
        let mut down: Option<usize> = None;
        for _ in 0..12 {
            for i in 0..3 {
                sim.isolate(i, false);
            }
            match sim.rng.below(6) {
                // A second loss while one node recovers needs a forced
                // start: crash only while the other two hold state.
                0 | 1 if down.is_none() && caught_up(&sim) => {
                    let i = sim.rng.below(3) as usize;
                    sim.crash(i);
                    down = Some(i);
                }
                2 => {
                    if let Some(i) = down.take() {
                        sim.restart(i);
                    }
                }
                3 => {
                    let i = sim.rng.below(3) as usize;
                    if down != Some(i) {
                        sim.isolate(i, true);
                    }
                }
                _ => {}
            }
            propose_for(&mut sim, 30, 400);
        }
        for i in 0..3 {
            sim.isolate(i, false);
        }
        if let Some(i) = down {
            sim.restart(i);
        }
        sim.faults = CLEAN;
        assert!(
            settle(&mut sim, 20_000),
            "seed {seed}: the cluster did not settle\n{}",
            report(&sim)
        );
        sim.check();
    }
}

/// Node states and the last role changes, for a failure message.
fn report(sim: &Sim) -> String {
    let st: Vec<_> = sim
        .nodes
        .iter()
        .map(|n| n.as_ref().map(zzq_cluster::node::Node::status))
        .collect();
    let recent = &sim.timeline[sim.timeline.len().saturating_sub(16)..];
    format!("{st:?}\n{}", recent.join("\n"))
}

/// Seeds per chaos test: `n`, or `ZZQ_SIM_SEEDS` for a longer sweep.
fn seeds(n: u64) -> u64 {
    std::env::var("ZZQ_SIM_SEEDS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(n)
}

#[test]
fn a_full_stop_in_any_order_keeps_every_acked_op() {
    for order in [[0, 1, 2], [2, 1, 0], [1, 0, 2]] {
        let mut sim = Sim::with_log(10, CLEAN, small_log());
        sim.run(5);
        assert_eq!(propose_for(&mut sim, 100, 1000), 100);
        assert!(settle(&mut sim, 1000));
        let acked = sim.acked.clone();
        for i in order {
            assert!(sim.stop(i), "node {i} dumps");
            sim.run(300);
        }
        for i in [order[2], order[0], order[1]] {
            sim.start(i);
        }
        assert!(
            settle(&mut sim, 5000),
            "{order:?}: the cluster comes back\n{}",
            report(&sim)
        );
        let l = sim.leader().unwrap();
        for (op, body) in &acked {
            assert_eq!(
                &sim.apps[l].ops[op.0 as usize - 1],
                body,
                "acked op {} kept",
                op.0
            );
        }
        assert_eq!(propose_for(&mut sim, 10, 1000), 10);
        assert!(settle(&mut sim, 1000));
        sim.check();
    }
}

#[test]
fn any_two_dumps_bring_the_cluster_back() {
    let mut sim = Sim::with_log(11, CLEAN, small_log());
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 60, 1000), 60);
    assert!(settle(&mut sim, 1000));
    for i in 0..3 {
        assert!(sim.stop(i));
    }
    sim.start(1);
    sim.start(2);
    assert!(
        sim.run_until(5000, |s| s
            .leader()
            .is_some_and(|l| s.apps[l].ops.len() == 60)),
        "two of three restore\n{}",
        report(&sim)
    );
    sim.start(0);
    assert!(settle(&mut sim, 5000));
    sim.check();
}

#[test]
fn a_rolling_restart_moves_no_acked_op() {
    let mut sim = Sim::with_log(12, CLEAN, small_log());
    sim.run(5);
    for i in [2, 1, 0] {
        assert_eq!(propose_for(&mut sim, 40, 2000), 40);
        assert!(settle(&mut sim, 2000));
        assert!(sim.stop(i));
        assert_eq!(propose_for(&mut sim, 40, 2000), 40, "two nodes go on");
        sim.start(i);
        assert!(
            settle(&mut sim, 5000),
            "node {i} catches up\n{}",
            report(&sim)
        );
    }
    sim.check();
}

#[test]
fn a_restored_node_that_crashes_recovers_instead_of_reusing_its_dump() {
    let mut sim = Sim::with_log(13, CLEAN, small_log());
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 50, 1000), 50);
    assert!(settle(&mut sim, 1000));
    assert!(sim.stop(2));
    sim.start(2);
    assert!(settle(&mut sim, 3000));
    sim.crash(2);
    assert_eq!(propose_for(&mut sim, 50, 2000), 50);
    sim.start(2);
    assert!(
        sim.nodes[2].as_ref().unwrap().recovering(),
        "no dump left: it recovers"
    );
    assert!(settle(&mut sim, 5000));
    sim.check();
}

#[test]
fn seeded_stops_crashes_and_splits_end_in_a_full_restart() {
    for seed in 0..seeds(40) {
        eprintln!("seed {seed}");
        let mut sim = Sim::with_log(9000 + seed, Faults { drop: 3, jitter: 5 }, small_log());
        sim.run(5);
        let mut stopped: Vec<usize> = Vec::new();
        let mut crashed: Option<usize> = None;
        for _ in 0..12 {
            for i in 0..3 {
                sim.isolate(i, false);
            }
            let i = sim.rng.below(3) as usize;
            let up = sim.nodes[i].is_some();
            match sim.rng.below(8) {
                0 if up && crashed.is_none() && stopped.is_empty() && caught_up(&sim) => {
                    sim.crash(i);
                    crashed = Some(i);
                }
                // A node that cannot dump yet, as while it installs a
                // view, would restart without state: only as a crash.
                1 | 2
                    if up
                        && crashed.is_none()
                        && sim.nodes[i].as_ref().is_some_and(|n| n.export().is_some()) =>
                {
                    assert!(sim.stop(i));
                    stopped.push(i);
                }
                3 | 4 => {
                    if let Some(j) = stopped.pop() {
                        sim.start(j);
                    } else if let Some(j) = crashed.take() {
                        sim.restart(j);
                    }
                }
                5 if up => sim.isolate(i, true),
                _ => {}
            }
            propose_for(&mut sim, 30, 400);
        }
        for i in 0..3 {
            sim.isolate(i, false);
        }
        for j in stopped.drain(..) {
            sim.start(j);
        }
        if let Some(j) = crashed.take() {
            sim.restart(j);
        }
        sim.faults = CLEAN;
        if !settle(&mut sim, 20_000) {
            sim.trace = true;
            sim.run(100);
            let mut seen = std::collections::BTreeMap::new();
            for (f, t, op) in &sim.traced {
                *seen.entry((f, t, op)).or_insert(0) += 1;
            }
            let errors: Vec<_> = sim.errors.iter().rev().take(6).collect();
            panic!(
                "seed {seed}: no settle before the stop\n{}\n{seen:?}\n{errors:?}",
                report(&sim)
            );
        }
        sim.check();
        let acked = sim.acked.clone();
        for i in [2, 0, 1] {
            assert!(sim.stop(i), "seed {seed}: node {i} dumps");
        }
        for i in 0..3 {
            sim.start(i);
        }
        assert!(
            settle(&mut sim, 20_000),
            "seed {seed}: no settle after the restart\n{}",
            report(&sim)
        );
        let l = sim.leader().unwrap();
        for (op, body) in &acked {
            assert_eq!(
                &sim.apps[l].ops[op.0 as usize - 1],
                body,
                "seed {seed}: acked op {} kept",
                op.0
            );
        }
        sim.check();
    }
}

#[test]
fn an_application_that_dropped_its_state_gets_the_committed_ops_again() {
    let mut sim = Sim::new(14, CLEAN);
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 30, 500), 30);
    assert!(settle(&mut sim, 1000));
    sim.apps[1].ops.clear();
    sim.node(1).reset_application();
    assert!(settle(&mut sim, 1000), "it replays from the first op");
    assert_eq!(sim.apps[1].ops, sim.apps[0].ops);
}

#[test]
fn an_application_that_dropped_its_state_past_the_log_gets_an_image() {
    let mut sim = Sim::with_log(15, CLEAN, small_log());
    sim.run(5);
    assert_eq!(propose_for(&mut sim, 100, 1000), 100);
    assert!(settle(&mut sim, 1000));
    sim.apps[1].ops.clear();
    sim.node(1).reset_application();
    assert!(settle(&mut sim, 3000), "an image, then the ops after it");
    assert_eq!(sim.images[1], 1);
    assert_eq!(sim.apps[1].ops, sim.apps[0].ops);
    assert_eq!(propose_for(&mut sim, 10, 500), 10);
    assert!(settle(&mut sim, 1000));
    sim.check();
}
