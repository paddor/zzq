#!/usr/bin/env escript
%% emqtt client throughput against zzq_fakebroker.
%%
%% Usage: emqtt_bench.escript <libdir> <pub|sub> <port> <qos> <size> <n> [procs]
%%
%% pub: `procs` processes share one emqtt client and call emqtt:publish until
%% n messages are sent. The fake broker measures. sub: the owner process counts
%% {publish, _} messages and measures first to n-th.

main([LibDir, Mode, Port, QoS, Size, N | Rest]) ->
    [code:add_patha(P) || P <- filelib:wildcard(filename:join(LibDir, "*/ebin"))],
    {ok, _} = application:ensure_all_started(emqtt),
    %% The client exits when the fake broker closes the socket.
    process_flag(trap_exit, true),
    Procs = case Rest of [P] -> list_to_integer(P); [] -> 1 end,
    Q = list_to_integer(QoS),
    Total = list_to_integer(N),
    {ok, C} = emqtt:start_link([{host, "127.0.0.1"}, {port, list_to_integer(Port)},
                                {proto_ver, v5}, {clientid, <<"emqtt">>},
                                {max_inflight, 100}, {retry_interval, 0}]),
    {ok, _} = emqtt:connect(C),
    case Mode of
        "pub" -> pub(C, Q, binary:copy(<<"x">>, list_to_integer(Size)), Total, Procs);
        "sub" -> sub(C, Q, list_to_integer(Size), Total)
    end.

pub(C, Q, Payload, Total, Procs) ->
    Left = counters:new(1, []),
    counters:put(Left, 1, Total),
    Self = self(),
    Pids = [spawn_link(fun() -> pub_loop(C, Q, Payload, Left), Self ! {done, self()} end)
            || _ <- lists:seq(1, Procs)],
    [receive {done, P} -> ok end || P <- Pids],
    timer:sleep(500).

pub_loop(C, Q, Payload, Left) ->
    case counters:get(Left, 1) > 0 of
        true ->
            counters:sub(Left, 1, 1),
            case emqtt:publish(C, <<"bench/t">>, Payload, Q) of
                ok -> ok;
                {ok, _} -> ok
            end,
            pub_loop(C, Q, Payload, Left);
        false ->
            ok
    end.

sub(C, Q, Size, Total) ->
    {ok, _, _} = emqtt:subscribe(C, <<"bench/t">>, Q),
    receive {publish, _} -> ok end,
    T0 = erlang:monotonic_time(),
    recv(Total - 1),
    Us = erlang:convert_time_unit(erlang:monotonic_time() - T0, native, microsecond),
    Rate = (Total - 1) / (Us / 1.0e6),
    io:format("emqtt-sub qos=~w size=~5w n=~8w  ~10.1f msg/s  ~8.1f MB/s~n",
              [Q, Size, Total, Rate, Rate * Size / 1.0e6]).

recv(0) -> ok;
recv(K) -> receive {publish, _} -> recv(K - 1) end.
