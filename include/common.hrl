-define(ELSE, true).

-define(RANGE, 134217728). % 2^27
-define(IN_RANGE(K,S,E), (
   (S < E andalso S =< K andalso K < E) orelse
   (E < S andalso S =< K andalso K < ?RANGE) orelse
   (E < S andalso 0 =< K andalso K < E))).

-record(conf, {id, version = 0, protocol, pids = []}).
