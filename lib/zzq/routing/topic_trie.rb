# frozen_string_literal: true

require_relative "topic_match"

module ZZQ
  module Routing
    # Per-level topic-filter trie. Each node holds literal children,
    # one +wildcard ("+") child, and one hash-wildcard ("#") child.
    # Subscriptions are stored at the terminal node of their filter.
    #
    # Matches are collected by walking a topic down the trie, traversing
    # literal + '+' branches in parallel and collecting '#' subtree
    # entries at every step. Honours the $SYS-vs-bare-wildcard rule
    # (MQTT-4.7.2-1): filters starting with `+` or `#` at the root do
    # not match topics starting with `$SYS`.
    #
    # Shared-subscription filters ("$share/<group>/<filter>") are
    # stored with their group stripped off but tagged with the group
    # name on each SubscriberEntry; fan-out chooses exactly one entry
    # per group (MVP: round-robin).
    #
    # Not thread-safe — a broker-owned trie is mutated from the broker
    # fibers only.
    class TopicTrie
      SubscriberEntry = Struct.new(:session, :entry, :share_group, keyword_init: true) do
        # Forward-compat shorthand for fan-out.
        def filter = entry[:filter]
      end


      class Node
        attr_accessor :plus_child, :hash_child
        attr_reader :children, :entries


        def initialize
          @children   = {}
          @plus_child = nil
          @hash_child = nil
          @entries    = []
        end


        def empty?
          @children.empty? && @plus_child.nil? && @hash_child.nil? && @entries.empty?
        end
      end


      def initialize
        @root = Node.new
        # Round-robin cursor for shared groups: group_key => index.
        @share_cursor = Hash.new(0)
      end


      # Insert a subscription. +filter+ may be a plain MQTT filter
      # ("a/+/c", "#", "$SYS/b/+") or a shared-subscription filter
      # ("$share/group/a/+/c").
      def add(filter, session, entry)
        real_filter, group = strip_share(filter)
        node, _segs = walk_or_create(real_filter)
        node.entries << SubscriberEntry.new(session: session, entry: entry, share_group: group)
      end


      # Remove the entry for (session, filter). Returns true if found.
      def remove(filter, session)
        real_filter, _group = strip_share(filter)
        segs = real_filter.split("/", -1)
        path = [@root]
        segs.each do |seg|
          node = path.last
          nxt =
            case seg
            when "+" then node.plus_child
            when "#" then node.hash_child
            else node.children[seg]
            end
          return false unless nxt
          path << nxt
        end
        leaf = path.last
        before = leaf.entries.size
        leaf.entries.reject! { |e| e.session.equal?(session) && e.entry[:filter] == filter }
        return false if leaf.entries.size == before
        prune(path, segs)
        true
      end


      # Drop every entry belonging to +session+, pruning empty nodes.
      def remove_session(session)
        walk_prune(@root, []) { |e| e.session.equal?(session) }
      end


      # Return the matching subscriber entries for +topic+. One entry
      # per (session, filter); shared-group collapse is performed here
      # so fan-out sees exactly one delivery per $share group.
      def match(topic)
        segs = topic.split("/", -1)
        collected = []
        traverse(@root, segs, 0, topic.start_with?("$"), collected)
        collapse_shared(collected)
      end


      def empty? = @root.empty?


      private


      SHARE_PREFIX = "$share/"


      def strip_share(filter)
        return [filter, nil] unless filter.start_with?(SHARE_PREFIX)
        rest = filter[SHARE_PREFIX.length..]
        slash = rest.index("/") or raise ArgumentError, "malformed $share filter: #{filter}"
        group = rest[0, slash]
        real  = rest[(slash + 1)..]
        raise ArgumentError, "$share filter has empty group: #{filter}" if group.empty?
        raise ArgumentError, "$share filter has empty inner filter: #{filter}" if real.empty?
        [real, group]
      end


      def walk_or_create(filter)
        segs = filter.split("/", -1)
        node = @root
        segs.each do |seg|
          node =
            case seg
            when "+"
              node.plus_child ||= Node.new
            when "#"
              node.hash_child ||= Node.new
            else
              node.children[seg] ||= Node.new
            end
        end
        [node, segs]
      end


      def prune(path, segs)
        # path[i] is the node reached after segs[0..i-1]; path[0] = root.
        (segs.length - 1).downto(0) do |i|
          parent = path[i]
          child  = path[i + 1]
          next unless child.empty?
          seg = segs[i]
          case seg
          when "+" then parent.plus_child = nil
          when "#" then parent.hash_child = nil
          else parent.children.delete(seg)
          end
        end
      end


      def walk_prune(node, path_segs, &block)
        node.entries.reject!(&block)
        node.children.each do |seg, child|
          walk_prune(child, path_segs + [seg], &block)
          node.children.delete(seg) if child.empty?
        end
        if node.plus_child
          walk_prune(node.plus_child, path_segs + ["+"], &block)
          node.plus_child = nil if node.plus_child.empty?
        end
        if node.hash_child
          walk_prune(node.hash_child, path_segs + ["#"], &block)
          node.hash_child = nil if node.hash_child.empty?
        end
      end


      # Walk all trie branches compatible with segs[i..]. Collect
      # entries at every node that marks a terminal filter. Rules:
      #   * +#+ at a node matches the rest of the topic (including zero
      #     more segments).
      #   * MQTT-4.7.2-1: filters starting with +/# at the root don't
      #     match topics starting with $.
      def traverse(node, segs, i, sys_topic, out)
        at_root = i.zero?

        if node.hash_child && !(at_root && sys_topic)
          out.concat(node.hash_child.entries)
        end

        if i == segs.length
          out.concat(node.entries)
          return
        end

        seg = segs[i]

        if (lit = node.children[seg])
          traverse(lit, segs, i + 1, sys_topic, out)
        end

        if (plus = node.plus_child) && !(at_root && sys_topic)
          traverse(plus, segs, i + 1, sys_topic, out)
        end
      end


      # For shared subscriptions: group matches by (group, filter),
      # pick one member round-robin; unshared entries pass through.
      def collapse_shared(entries)
        return entries if entries.all? { |e| e.share_group.nil? }
        out = []
        grouped = Hash.new { |h, k| h[k] = [] }
        entries.each do |e|
          if e.share_group
            grouped[[e.share_group, e.entry[:filter]]] << e
          else
            out << e
          end
        end
        grouped.each do |key, members|
          idx = (@share_cursor[key] += 1) % members.size
          out << members[idx]
        end
        out
      end
    end
  end
end
