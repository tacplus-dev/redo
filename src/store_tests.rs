use std::fs::OpenOptions;
use std::io::Write;

use redo_model::types::sys::redolog::{redo_key, RedoKey, RedoReplayEntry};

use super::*;
use crate::test_support::{del, put};

fn key(tid: &str) -> LogKey {
    LogKey::new("td.test", tid).unwrap()
}

fn replay(s: &Store, k: &LogKey) -> Vec<RedoReplayEntry> {
    s.replay(k).unwrap().collect::<Result<Vec<_>, _>>().unwrap()
}

#[test]
fn keys_are_well_formed_tids_and_trust_domains() {
    assert!(LogKey::new("td.test", "a1234567").is_ok());
    assert!(LogKey::new("td.test", "00000001").is_ok());
    for tid in ["", "a123456", "a12345678", "A1234567", "../../..", "a123456/"] {
        assert!(LogKey::new("td.test", tid).is_err(), "{tid:?}");
    }
    for td in ["", ".", "..", "a/b", "TD.test", "td test"] {
        assert!(LogKey::new(td, "a1234567").is_err(), "{td:?}");
    }
}

#[test]
fn appends_in_seq_order_only() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    assert_eq!(s.last_seq(&k).unwrap(), 0);
    assert!(matches!(s.append(&k, 2, vec![put("t.R", "x", b"1")]), Err(StoreError::OutOfOrder { last: 0 })));
    s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
    assert!(matches!(s.append(&k, 1, vec![put("t.R", "x", b"1")]), Err(StoreError::OutOfOrder { last: 1 })));
    assert!(matches!(s.append(&k, 3, vec![put("t.R", "x", b"1")]), Err(StoreError::OutOfOrder { last: 1 })));
    s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
    assert_eq!(s.last_seq(&k).unwrap(), 2);
}

#[test]
fn replays_exactly_what_was_appended_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    let ops = vec![
        vec![put("t.R", "x", b"1")],
        vec![put("t.R", "y", b"2"), put("t.S", "x", b"3")],
        vec![del("t.R", "x")],
    ];
    {
        let s = Store::open(dir.path()).unwrap();
        for (i, c) in ops.iter().enumerate() {
            s.append(&k, i as u64 + 1, c.clone()).unwrap();
        }
    }
    let s = Store::open(dir.path()).unwrap();
    let got = replay(&s, &k);
    assert_eq!(got.len(), 3);
    for (i, e) in got.iter().enumerate() {
        assert_eq!(e.seq, i as u64 + 1);
        assert!(!e.from_snapshot);
        assert_eq!(e.changes, ops[i]);
    }
    assert_eq!(s.last_seq(&k).unwrap(), 3);
    s.append(&k, 4, vec![put("t.R", "z", b"4")]).unwrap();
}

#[test]
fn compaction_folds_the_tail_into_one_put_per_live_entry() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    s.append(&k, 1, vec![put("t.R", "x", b"1"), put("t.R", "y", b"1")]).unwrap();
    s.append(&k, 2, vec![put("t.R", "x", b"2")]).unwrap();
    s.append(&k, 3, vec![del("t.R", "y")]).unwrap();
    s.append(&k, 4, vec![put("t.S", "y", b"4")]).unwrap();
    assert!(!s.compact(&k, 3, 4).unwrap(), "fewer than min_fold to fold");
    assert_eq!(s.tail_len(&k).unwrap(), 4);
    assert!(s.compact(&k, 3, 3).unwrap());
    assert_eq!(s.tail_len(&k).unwrap(), 1);

    let got = replay(&s, &k);
    assert_eq!(got.len(), 2, "{got:?}");
    assert_eq!(got[0], RedoReplayEntry { seq: 3, changes: vec![put("t.R", "x", b"2")], from_snapshot: true });
    assert_eq!(got[1], RedoReplayEntry { seq: 4, changes: vec![put("t.S", "y", b"4")], from_snapshot: false });

    // A second compaction folds into the first snapshot, and survives a
    // restart. It never folds the newest operation.
    s.append(&k, 5, vec![del("t.R", "x")]).unwrap();
    s.append(&k, 6, vec![put("t.R", "z", b"6")]).unwrap();
    assert!(s.compact(&k, 6, 1).unwrap());
    drop(s);
    let s = Store::open(dir.path()).unwrap();
    assert_eq!(s.tail_len(&k).unwrap(), 1);
    assert_eq!(s.last_seq(&k).unwrap(), 6);
    let got = replay(&s, &k);
    assert_eq!(
        got,
        vec![
            RedoReplayEntry { seq: 5, changes: vec![put("t.S", "y", b"4")], from_snapshot: true },
            RedoReplayEntry { seq: 6, changes: vec![put("t.R", "z", b"6")], from_snapshot: false },
        ]
    );
    s.append(&k, 7, vec![put("t.R", "x", b"7")]).unwrap();
    assert_eq!(replay(&s, &k).len(), 3);
}

#[test]
fn keys_of_different_cases_are_different_entries() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    let mut int_one = put("t.R", "", b"int");
    int_one.key = Some(RedoKey { key: Some(redo_key::Key::IntKey(1)) });
    let mut uint_one = put("t.R", "", b"uint");
    uint_one.key = Some(RedoKey { key: Some(redo_key::Key::UintKey(1)) });
    s.append(&k, 1, vec![put("t.R", "1", b"string"), int_one, uint_one]).unwrap();
    s.append(&k, 2, vec![put("t.S", "x", b"2")]).unwrap();
    assert!(s.compact(&k, 1, 1).unwrap());
    assert_eq!(replay(&s, &k).iter().filter(|e| e.from_snapshot).count(), 3);
}

#[test]
fn a_replay_is_not_split_by_a_later_append_or_compaction() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
    s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
    let r = s.replay(&k).unwrap();
    s.append(&k, 3, vec![put("t.R", "z", b"3")]).unwrap();
    assert!(s.compact(&k, 3, 1).unwrap());
    let got: Vec<_> = r.collect::<Result<_, _>>().unwrap();
    assert_eq!(got.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2]);
}

#[test]
fn reads_the_tail_after_a_seq() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    for seq in 1..=5 {
        s.append(&k, seq, vec![put("t.R", "x", &[seq as u8])]).unwrap();
    }
    let got = s.read_tail(&k, 2, 2).unwrap();
    assert_eq!(got.iter().map(|o| o.seq).collect::<Vec<_>>(), vec![3, 4]);
    assert!(s.read_tail(&k, 5, 10).unwrap().is_empty());
    assert!(s.compact(&k, 3, 1).unwrap());
    // Folded operations are gone from the tail: the first one read is 4.
    assert_eq!(s.read_tail(&k, 0, 10).unwrap().iter().map(|o| o.seq).collect::<Vec<_>>(), vec![4, 5]);
}

#[test]
fn a_torn_last_frame_is_cut_off_on_open() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
        s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
    }
    let tail = dir.path().join("td.test/a1234567/tail");
    let len = std::fs::metadata(&tail).unwrap().len();
    // An append that crashed halfway through its frame.
    OpenOptions::new().write(true).open(&tail).unwrap().set_len(len - 3).unwrap();
    let s = Store::open(dir.path()).unwrap();
    assert_eq!(s.last_seq(&k).unwrap(), 1);
    s.append(&k, 2, vec![put("t.R", "z", b"3")]).unwrap();
    let got = replay(&s, &k);
    assert_eq!(got[1].changes, vec![put("t.R", "z", b"3")]);
}

#[test]
fn a_bad_frame_before_the_end_is_corruption() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
        s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
    }
    let tail = dir.path().join("td.test/a1234567/tail");
    let mut bytes = std::fs::read(&tail).unwrap();
    bytes[10] ^= 0xff;
    std::fs::write(&tail, bytes).unwrap();
    let s = Store::open(dir.path()).unwrap();
    assert!(matches!(s.last_seq(&k), Err(StoreError::Corrupt(_))));
}

#[test]
fn garbage_after_the_last_frame_is_cut_off() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
    }
    let tail = dir.path().join("td.test/a1234567/tail");
    OpenOptions::new().append(true).open(&tail).unwrap().write_all(&[0xff; 5]).unwrap();
    let s = Store::open(dir.path()).unwrap();
    assert_eq!(s.last_seq(&k).unwrap(), 1);
}

#[test]
fn tenants_are_isolated_by_trust_domain_and_tid() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let a = LogKey::new("td.one", "00000001").unwrap();
    let b = LogKey::new("td.two", "00000001").unwrap();
    let c = LogKey::new("td.one", "a1234567").unwrap();
    s.append(&a, 1, vec![put("t.R", "x", b"a")]).unwrap();
    s.append(&b, 1, vec![put("t.R", "x", b"b")]).unwrap();
    assert_eq!(replay(&s, &a)[0].changes, vec![put("t.R", "x", b"a")]);
    assert_eq!(replay(&s, &b)[0].changes, vec![put("t.R", "x", b"b")]);
    assert!(replay(&s, &c).is_empty());
    assert_eq!(s.tenants("td.one").unwrap(), vec![a]);
}

#[test]
fn deletion_removes_the_log_and_refuses_the_tenant_for_good() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
        s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
        assert!(s.compact(&k, 1, 1).unwrap());
        s.set_shipped(&k, 2).unwrap();
        assert!(s.delete(&k, true).unwrap());
        assert!(!s.delete(&k, true).unwrap(), "deleting twice is a no-op");
        assert!(matches!(s.append(&k, 3, vec![put("t.R", "x", b"1")]), Err(StoreError::Deleted)));
        assert!(matches!(s.replay(&k), Err(StoreError::Deleted)));
    }
    let tdir = dir.path().join("td.test/a1234567");
    for f in ["tail", "snapshot", "shipped"] {
        assert!(!tdir.join(f).exists(), "{f} survived the deletion");
    }
    let s = Store::open(dir.path()).unwrap();
    assert!(s.is_deleted(&k).unwrap());
    assert!(matches!(s.append(&k, 1, vec![put("t.R", "x", b"1")]), Err(StoreError::Deleted)));
    assert!(s.deletion_unshipped(&k).unwrap());
    s.set_deletion_shipped(&k).unwrap();
    assert!(!s.deletion_unshipped(&k).unwrap());
    // A tenant never seen is deleted all the same.
    let never = key("b1234567");
    assert!(s.delete(&never, false).unwrap());
    assert!(!s.deletion_unshipped(&never).unwrap());
}

#[test]
fn shipped_survives_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
        s.set_shipped(&k, 1).unwrap();
    }
    assert_eq!(Store::open(dir.path()).unwrap().shipped(&k).unwrap(), 1);
}

#[test]
fn a_compaction_after_one_cut_short_by_a_crash_keeps_the_log_exact() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    let tdir = dir.path().join("td.test/a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
        s.append(&k, 2, vec![del("t.R", "x")]).unwrap();
        s.append(&k, 3, vec![put("t.R", "y", b"3")]).unwrap();
        s.append(&k, 4, vec![put("t.R", "z", b"4")]).unwrap();
    }
    // A crash between the snapshot's rename and the tail's: the new snapshot
    // (folding 1..2), the old tail.
    let old_tail = std::fs::read(tdir.join("tail")).unwrap();
    assert!(Store::open(dir.path()).unwrap().compact(&k, 2, 1).unwrap());
    std::fs::write(tdir.join("tail"), &old_tail).unwrap();

    let s = Store::open(dir.path()).unwrap();
    assert!(s.compact(&k, 3, 1).unwrap());
    let state = |s: &Store| {
        let got = replay(s, &k);
        (got.last().unwrap().seq, got.iter().flat_map(|e| e.changes.clone()).collect::<Vec<_>>())
    };
    let want = (4, vec![put("t.R", "y", b"3"), put("t.R", "z", b"4")]);
    assert_eq!(state(&s), want);
    drop(s);
    let s = Store::open(dir.path()).unwrap();
    assert_eq!(s.last_seq(&k).unwrap(), 4);
    assert_eq!(state(&s), want);
}

#[test]
fn compaction_keeps_the_last_operation_so_a_replay_ends_at_the_last_seq() {
    let dir = tempfile::tempdir().unwrap();
    let s = Store::open(dir.path()).unwrap();
    let k = key("a1234567");
    s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
    s.append(&k, 2, vec![del("t.R", "x")]).unwrap();
    s.compact(&k, u64::MAX, 1).unwrap();
    // Nothing is live, yet the store learns where to append next.
    assert_eq!(replay(&s, &k).last().map(|e| e.seq), Some(2));
    assert_eq!(s.tail_len(&k).unwrap(), 1);
}

#[test]
fn a_rotted_length_before_the_end_is_corruption_not_a_torn_frame() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        for seq in 1..=5 {
            s.append(&k, seq, vec![put("t.R", "x", b"1")]).unwrap();
        }
    }
    let tail = dir.path().join("td.test/a1234567/tail");
    let mut bytes = std::fs::read(&tail).unwrap();
    bytes[3] = 0x7f; // the first frame's length, high byte
    std::fs::write(&tail, &bytes).unwrap();
    let s = Store::open(dir.path()).unwrap();
    assert!(matches!(s.last_seq(&k), Err(StoreError::Corrupt(_))));
    assert_eq!(std::fs::read(&tail).unwrap(), bytes, "nothing acked is cut");
}

#[test]
fn a_zero_filled_end_of_the_tail_is_torn() {
    let dir = tempfile::tempdir().unwrap();
    let k = key("a1234567");
    {
        let s = Store::open(dir.path()).unwrap();
        s.append(&k, 1, vec![put("t.R", "x", b"1")]).unwrap();
    }
    let tail = dir.path().join("td.test/a1234567/tail");
    OpenOptions::new().append(true).open(&tail).unwrap().write_all(&[0; 40]).unwrap();
    let s = Store::open(dir.path()).unwrap();
    assert_eq!(s.last_seq(&k).unwrap(), 1);
    s.append(&k, 2, vec![put("t.R", "y", b"2")]).unwrap();
    assert_eq!(replay(&s, &k).len(), 2);
}
