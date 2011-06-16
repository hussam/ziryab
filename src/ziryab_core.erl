-module(ziryab_core).
-behaviour(gen_fsm).

% public interface
-export([
      new/2,
      do/2,
      fork/1,
      add_sync_srcs/3,
      is_mutating/1
   ]).

% gen_fsm callbacks
-export([
      init/1,
      active/3,
      wedged/3,
      handle_sync_event/4,
      handle_event/3,
      handle_info/3,
      code_change/4,
      terminate/3
   ]).


-include("ziryab.hrl").
-include("params.hrl").

-record(syncs, {range, cores}).

-record(state, {range,
                store,
                succ,
                sync_state,
                misc
             }).


% start a new key/value store core on this node
new(Range, SyncState) when is_list(SyncState) ->
   {ok, Pid} = gen_fsm:start(ziryab_core, [new, Range, SyncState], []),
   Pid.

% create a forked copy of this kv core on this node
fork(Core) ->
   {ok, Pid} = gen_fsm:sync_send_all_state_event(Core, fork),
   Pid.

% perform a command on the current kv core
do(Core, Command) ->
   gen_fsm:sync_send_event(Core, Command).

% add sync sources
add_sync_srcs(Core, Range, SyncCores) ->
   gen_fsm:sync_send_all_state_event(Core, {add_sync_srcs, Range, SyncCores}).

% return true if executing this "external" command will change the lcoal state
is_mutating(Command) ->
   case Command of
      {do_command, {_, {put, _}}}  -> true;
      {do_command, {sequencer, _}} -> true;
      _ -> false
   end.


%%%%%%%%%%%%%%%%%%%%
% gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%%

% Create a new core process
init([new, Range, SyncState]) when is_list(SyncState) ->
   {A,B,C} = now(),
   Fname = "ztable"++integer_to_list(A)++integer_to_list(B)++integer_to_list(C),
   {ok, Store} = dets:open_file(list_to_atom(Fname), [{file, Fname},
                                                      {min_no_slots, 8192},
                                                      {max_no_slots, 16777216}
                                                      ,{ram_file, true}
                                                   ]),

   StateData = #state{ range = Range, store = Store, sync_state = SyncState },
   TimeToSync = if
      length(SyncState) == 0 -> infinity;
      ?ELSE -> ?SYNC_INTERVAL
   end,

   {ok, active, StateData, TimeToSync};

% Create a new core process initialized with the forked state data
init([forked, InitState, StateData = #state{store = StoreName}]) ->
   TimeToSync = if
      length(StateData#state.sync_state) == 0 -> infinity;
      ?ELSE -> ?SYNC_INTERVAL
   end,

   {ok, Store} = dets:open_file(StoreName, {file, StoreName}),

   {ok, InitState, StateData#state{store = Store}, TimeToSync}.


% State: ACTIVE
% When a core is in an active state it can add commands to its history and
% respond to client requests. A core stays in active state until it is wedged
active(Event, _Client, StateData = #state{
      range = Range, store = Store, sync_state = SyncState} ) ->

   NumSyncSrcs = length(SyncState),
   TimeToSync = if NumSyncSrcs == 0 -> infinity; ?ELSE -> ?SYNC_INTERVAL end,

   case Event of
      % perform a client command
      {do_command, Cmd} ->
         {NewStateData, Reply} = doCmd(Cmd, StateData),
         {reply, Reply, active, NewStateData, TimeToSync};

      % a proto-node requested the value associated with a particular key
      {proto_request, Key} ->
         {reply, dets:lookup(Store, Key), active, StateData, TimeToSync};

      % respond to a periodic sync request from a proto-node
      {sync_request, KnownKeys} ->
         Remaining = sets:subtract(sets:from_list(allkeys(Store)), KnownKeys),
         case sets:size(Remaining) of
            0 ->
               {reply, {done_syncing, Range}, active, StateData, TimeToSync};
            _ ->
               Keys = lists:sublist(sets:to_list(Remaining), ?SYNC_RATE),
               KVs = lists:flatten( [ dets:lookup(Store, K) || K <- Keys ] ),
               {reply, {sync_reply, KVs}, active, StateData, TimeToSync}
         end;

      % received key/value pairs for syncing
      {sync_reply, KeysValues = [{Key, _} | _]} ->
         % get the sync state related to the obtained keys
         OldTuple = {SyncSrcs, SyncedKeys} = srcsAndKeys(Key, SyncState),
         % XXX: what to do when we receive sync_reply from non-source?

         % only add pairs for which we do not have the most up-to-date value yet
         NewPairs = lists:filter(
            fun({K,_}) -> not sets:is_element(K, SyncedKeys) end,
            KeysValues),
         dets:insert(Store, NewPairs),

         % update the list of up-to-date key/value pairs
         NewKeys = sets:from_list([K || {K,_} <- NewPairs]),
         NewSyncState = [ {SyncSrcs, sets:union(SyncedKeys, NewKeys)} |
                             lists:delete(OldTuple, SyncState) ],
         {next_state, active, StateData#state{sync_state = NewSyncState}, TimeToSync};

      % nothing more to sync
      {done_syncing, _SrcRange = {Start, _}} ->
         NewSyncState = lists:delete(srcsAndKeys(Start, SyncState), SyncState),
         {next_state, active, StateData#state{sync_state = NewSyncState}, TimeToSync};

      % time to do a sync
      timeout ->
         {Srcs, SyncedKeys} = lists:nth(random:uniform(NumSyncSrcs), SyncState),
         sync(Srcs, {sync_request, SyncedKeys}),
         {next_state, active, StateData, TimeToSync}
   end.


% a wedged core can not add anything to its history until it is unwedged
wedged(activate, _, StateData) ->
   {reply, ok, active, StateData};
wedged(_, _, StateData) ->
   {reply, wedged, wedged, StateData}.


% take core into immutable state regardless of current state
handle_sync_event(wedge, _, _, StateData) ->
   {reply, wedged, wedged, StateData};

% return state data
handle_sync_event(get_state, _, State, StateData) ->
   {reply, {State, StateData}, State, StateData};

% add sync sources
handle_sync_event({add_sync_srcs, Range, Cores}, _From,
                  State, StateData = #state{sync_state = SyncState} ) ->
   % TODO: remove existing overlapping ranges from sync sources
   NewSyncState = [ #syncs{range = Range, cores = Cores} | SyncState ],
   { reply, ok, State, StateData#state{sync_state = NewSyncState} };


% create a forked copy of this kv core on this node
handle_sync_event(fork, _, State, StateData = #state{store = Store}) ->
   % copy the DETS file
   Fname = dets:info(Store, filename),
   ok = dets:close(Store),  % to avoid 'incorrectly closed' errors
   {A,B,C} = now(),
   CFname = "ztable"++integer_to_list(A)++integer_to_list(B)++integer_to_list(C),
   file:copy(Fname, CFname),

   % open data store for the cloned core
   ClonedData = StateData#state{ store = CFname },

   dets:open_file(Store, {file, Store}),    % restore original store
   % start a new forked core
   {reply, gen_fsm:start(ziryab_core, [forked, State, ClonedData], []),
      State, StateData}.



terminate(Reason, _State, _StateData = #state{store = Store}) ->
   dets:close(Store),
   Reason.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Unimplemented gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_event(_Event, _State, _StateData) ->
   exit("call to ziryab_core:handle_event unimplemented.").

handle_info(_Info, _Stat, StateData) ->
   {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
   {ok, StateName, State}.



%%%%%%%%%
% private
%%%%%%%%%


% doCmd: perform a command's operation and returns the new state and a reply

% Handle sequencing (successor configuration) commands
doCmd({sequencer, Command}, StateData = #state{succ = Succ}) ->
   case Command of
      % set successor
      {set_succ, NewSucc} ->
         { StateData#state{succ = NewSucc}, ok };

      % delete (and return) successor
      del_succ ->
         { StateData#state{succ = undefined}, Succ }
   end;

% Handle put/get commands
doCmd( {Key, Command},
       StateData = #state {
          range = {Start, End}, store = Store, sync_state = SyncState
       } ) when ?IN_RANGE(Key, Start, End) ->

   case srcsAndKeys(Key, SyncState) of
      none ->  % this is not a proto-node for this particular key-range
         case Command of
            % put a key/value pair
            {put, Value} ->
               {StateData, dets:insert(Store, {Key, Value})};

            % get the value associated with this key
            get ->
               {StateData, dets:lookup(Store, Key)}
         end;

      {SyncSrcs, SyncedKeys} = OldTuple ->   % this is a proto-node
         case Command of
            % put a key/value pair
            {put, Value} ->
               NewStateData = StateData#state{
                  sync_state = [ {SyncSrcs, sets:add_element(Key, SyncedKeys)} |
                     lists:delete(OldTuple, SyncState) ] },
               {NewStateData, dets:insert(Store, {Key, Value})};

            % get the value associated with this key
            get ->
               { Value , NewSyncState } =
               case sets:is_element(Key, SyncedKeys) of
                  true ->
                     { dets:lookup(Store, Key) , SyncState };
                  false ->
                     KV = sync(SyncSrcs, {proto_request, Key}),
                     dets:insert(Store, KV),
                     { KV, [ {SyncSrcs, sets:add_element(Key, SyncedKeys)} |
                              lists:delete(OldTuple, SyncState) ] }
               end,
               {StateData#state{sync_state = NewSyncState}, Value}
         end
   end;

% Remove all pairs where the key is not in the range [Start, End)
doCmd( rm_out_of_range,
      StateData = #state { range = {Start, End}, store = Store } ) ->
   % two options:
   % --- if Start < End then delete everything >= End *OR* < Start
   % --- if Start > End then delete everything >= End *AND* < Start
   AndOr = if Start < End -> 'or'; ?ELSE -> 'and' end,
   dets:select_delete(Store,
      [{{'$1','_'},[],[{AndOr,{'>=','$1',End},{'<','$1',Start}}]}]),

   {StateData, ok}.

%%%%%%%%%
% UTILS %
%%%%%%%%%

allkeys(Store) ->
   dets:select(Store, [{{'$1','_'},[],['$1']}]).

srcsAndKeys(Key, [{SyncSrcs = #syncs{range={Start, End}}, SyncedKeys} | Tail]) ->
   if
      ?IN_RANGE(Key, Start, End) ->
         {SyncSrcs, SyncedKeys};
      ?ELSE ->
         srcsAndKeys(Key, Tail)
   end;
srcsAndKeys(_, []) ->
   none.


sync(Srcs = #syncs{cores = Cores}, Request) ->
   Parent = self(),

   lists:foreach(
      fun(Core) ->
            spawn(fun() ->
                     Parent ! gen_fsm:sync_send_event(Core, Request)
               end)
      end,
      Cores),

   receive
      Response -> Response
   after
      ?TO -> sync(Srcs, Request)
   end.

