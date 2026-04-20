# frozen_string_literal: true

require "test_helper"

describe ZZQ::Routing::TopicTrie do
  # A dummy session-placeholder — TopicTrie treats it as opaque.
  Session = Struct.new(:name)


  def sub(trie, filter, qos: 0, session: nil)
    session ||= Session.new("s-#{filter}")
    entry = { filter: filter, qos: qos }
    trie.add(filter, session, entry)
    [session, entry]
  end


  it "matches exact literal filters" do
    trie = ZZQ::Routing::TopicTrie.new
    s1, e1 = sub(trie, "a/b/c")
    sub(trie, "a/b/d")
    hits = trie.match("a/b/c")
    assert_equal 1, hits.size
    assert_equal s1, hits[0].session
    assert_equal e1, hits[0].entry
  end


  it "matches + single-level wildcard" do
    trie = ZZQ::Routing::TopicTrie.new
    sub(trie, "a/+/c")
    sub(trie, "+/+/+")
    hits = trie.match("a/b/c")
    assert_equal 2, hits.size
  end


  it "matches trailing # multi-level wildcard" do
    trie = ZZQ::Routing::TopicTrie.new
    sub(trie, "a/#")
    sub(trie, "#")
    assert_equal 2, trie.match("a/b/c/d").size
    assert_equal 2, trie.match("a").size
  end


  it "excludes $SYS topics from root + and #" do
    trie = ZZQ::Routing::TopicTrie.new
    sub(trie, "#")
    sub(trie, "+/broker")
    sub(trie, "$SYS/#")
    hits = trie.match("$SYS/broker")
    assert_equal 1, hits.size
    assert_equal "$SYS/#", hits[0].entry[:filter]
  end


  it "yields one entry per session across overlapping filters" do
    # The broker collapses to one-per-session on top — the trie itself
    # returns every matching entry. Verify both entries come back.
    trie = ZZQ::Routing::TopicTrie.new
    s = Session.new("shared")
    trie.add("a/+", s, { filter: "a/+", qos: 1 })
    trie.add("a/#", s, { filter: "a/#", qos: 2 })
    hits = trie.match("a/b")
    assert_equal 2, hits.size
    assert_equal s, hits[0].session
    assert_equal s, hits[1].session
  end


  it "collapses $share subscriptions to one entry per group+filter" do
    trie = ZZQ::Routing::TopicTrie.new
    sub(trie, "$share/g/a/+")
    sub(trie, "$share/g/a/+")
    sub(trie, "$share/g/a/+")
    hits = trie.match("a/b")
    assert_equal 1, hits.size, "one member of the group should be chosen"
    assert_equal "g", hits[0].share_group
  end


  it "round-robins across $share group members" do
    trie = ZZQ::Routing::TopicTrie.new
    s1, _ = sub(trie, "$share/g/x")
    s2, _ = sub(trie, "$share/g/x")
    picks = 4.times.map { trie.match("x").first.session }
    assert_equal [s1, s2, s1, s2].sort_by(&:name), picks.sort_by(&:name)
  end


  it "remove_session drops all entries for a session" do
    trie = ZZQ::Routing::TopicTrie.new
    s = Session.new("goner")
    trie.add("a", s, { filter: "a", qos: 0 })
    trie.add("b/c", s, { filter: "b/c", qos: 0 })
    assert_equal 1, trie.match("a").size
    trie.remove_session(s)
    assert_empty trie.match("a")
    assert_empty trie.match("b/c")
    assert trie.empty?
  end


  it "remove(filter, session) removes only that entry" do
    trie = ZZQ::Routing::TopicTrie.new
    s1 = Session.new("s1")
    s2 = Session.new("s2")
    trie.add("a/+", s1, { filter: "a/+", qos: 0 })
    trie.add("a/+", s2, { filter: "a/+", qos: 0 })
    assert trie.remove("a/+", s1)
    hits = trie.match("a/b")
    assert_equal 1, hits.size
    assert_equal s2, hits[0].session
  end
end


describe ZZQ::Routing::RetainedStore do
  it "stores and retrieves by filter" do
    s = ZZQ::Routing::RetainedStore.new
    s.store(ZZQ::Message.new(topic: "a/b", payload: "p1", retain: true))
    s.store(ZZQ::Message.new(topic: "a/c", payload: "p2", retain: true))
    s.store(ZZQ::Message.new(topic: "other", payload: "p3", retain: true))
    hits = s.each_matching("a/+").to_a
    assert_equal 2, hits.size
    assert_equal %w[a/b a/c].sort, hits.map(&:topic).sort
  end


  it "empty-payload + retain deletes the retained message" do
    s = ZZQ::Routing::RetainedStore.new
    s.store(ZZQ::Message.new(topic: "a", payload: "hi", retain: true))
    assert_equal 1, s.size
    s.store(ZZQ::Message.new(topic: "a", payload: "", retain: true))
    assert_equal 0, s.size
  end
end
