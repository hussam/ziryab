-module(ziryab_client).

% Client API
-export([
      connect/1,
      ping/2,
      put/3,
      get/2,
      delete/2,
      tx_init/1,
      tx_status/2,
      tx_abort/2,
      tx_commit/2,
      tx_put/4,
      tx_get/3
   ]).

% Utility functions
-export([
      route/2
   ]).

-include("ziryab.hrl").

-define(TxBuf(TxId), {ziryab_tx_buffer, TxId}).

% Connect a new client to the given node of the system
connect(Node) ->
   libdist_client:connect(Node, ?TIMEOUT).

% Ping the segment associated with this key
ping(ClientId, Key) ->
   libdist_client:call(ClientId, {Key, ping}).

% Associate a new value with this key
put(ClientId, Key, Value) ->
   libdist_client:call(ClientId, {Key, {put, Value}}).

% Get the value associated with this key
get(ClientId, Key) ->
   libdist_client:call(ClientId, {Key, get}).

delete(ClientId, Key) ->
   libdist_client:call(ClientId, {Key, delete}).

tx_init(ClientId) ->
   TxId = now(),
   put(?TxBuf(TxId), []),
   case libdist_client:call(ClientId, {tx, TxId, {init, self()}}) of
      {ok, {tx_ok, TxId}} -> {ok, TxId};
      Other -> Other
   end.

tx_abort(ClientId, TxId) ->
   erase(?TxBuf(TxId)),
   case libdist_client:call(ClientId, {tx, TxId, abort}) of
      {ok, {tx_ok, {aborted, TxId}}} ->
         {ok, aborted};
      {ok, {tx_err, {unabortable_status, TxId, Status}}} ->
         {ok, {unabbortable_status, Status}};
      Other -> Other
   end.

tx_commit(ClientId, TxId) ->
   TxBuffs = get(?TxBuf(TxId)),
   Ret = libdist_client:call(ClientId, {tx, TxId, {commit, self()}}),
   [libdist_client:call(ClientId, {tx, TxId, {commit_buffered, B}}) || B <- TxBuffs],
   Ret.

tx_get(ClientId, TxId, Key) ->
   libdist_client:call(ClientId, {tx, TxId, {Key, get, abort_incoming}}).

tx_put(ClientId, TxId, Key, Value) ->
   {ok, {tx_ok, BufferId}} = libdist_client:call(ClientId,
      {tx, TxId, {Key, {put, Value}, abort_incoming}}),
   NewTxBuffers = ordsets:add_element(BufferId, get(?TxBuf(TxId))),
   put(?TxBuf(TxId), NewTxBuffers),
   tx_ok.


tx_status(ClientId, TxId) ->
   case libdist_client:call(ClientId, {tx, TxId, status}) of
      {ok, {tx_ok, Status, TxId}} -> {ok, Status};
      Other -> Other
   end.


%%%%%%%%%%%%%
% Utilities %
%%%%%%%%%%%%%

route(Op, Partitions) ->
   case Op of
      {Key, ping}          -> do_route(Key, Partitions);
      {Key, get}           -> do_route(Key, Partitions);
      {Key, delete}        -> do_route(Key, Partitions);
      {Key, {put, _}}      -> do_route(Key, Partitions);
      {tx, _, {Key, _, _}} -> do_route(Key, Partitions);
      {tx, TxId, _}        -> do_route(TxId, Partitions)
   end.

do_route(Key, []) ->
   error("could not route key to proper partition", Key);
do_route(Key, [P = {{Begin, End}, _} | _]) when Key >= Begin, Key < End ->
   P;
do_route(Key, [_ | Tail]) ->
   do_route(Key, Tail).


%%%%%%%%%%%%%%%%%%%%%
% Private Functions %
%%%%%%%%%%%%%%%%%%%%%


