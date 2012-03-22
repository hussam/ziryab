-module(core_fsm).
-behaviour(gen_fsm).

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


%%%%%%%%%%%%%%%%%%%%
% gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%%

% Create a new core process
init([new, Range={Start, End}, SyncState]) when is_list(SyncState) ->
   Dataroot = ziryab_config:get(bitcask_dir),
   RangeStr = integer_to_list(Start) ++ "-" ++ integer_to_list(End),
   BitcaskOpts = ziryab_config:get(bitcask_opts),

   case bitcask:open( filename:join(Dataroot, RangeStr) , BitcaskOpts ) of
      {error, _} = Err ->
         {stop, {bitcask_err, Err}};

      Store ->
         StateData = #state{ range = Range, store = Store, sync_state = SyncState },
         TimeToSync = if
            length(SyncState) == 0 -> infinity;
            ?ELSE -> ziryab_config:get(sync_interval)
         end,

         {ok, active, StateData, TimeToSync}
   end;

% Create a new core process initialized with the forked state data
init([forked, InitState, StateData = #state{store = StoreName}]) ->
   TimeToSync = if
      length(StateData#state.sync_state) == 0 -> infinity;
      ?ELSE -> ziryab_config:get(sync_interval)
   end,

   case bitcask:open(StoreName, ziryab_config:get(bitcask_opts)) of
      {error, _} = Err ->
         {stop, {bitcask_err, Err}};

      Store ->
         {ok, InitState, StateData#state{store = Store}, TimeToSync}
   end.


% State: ACTIVE
% When a core is in an active state it can add commands to its history and
% respond to client requests. A core stays in active state until it is wedged
active(Event, _Client, StateData = #state{
      range = Range, store = Store, sync_state = SyncState} ) ->

   NumSyncSrcs = length(SyncState),
   TimeToSync = if NumSyncSrcs == 0 -> infinity; ?ELSE -> ziryab_config:get(sync_interval) end,

   case Event of
      % perform a client command
      {do_command, Cmd} ->
         {Reply, NewStateData} = core_cmd:do(Cmd, StateData),
         {reply, Reply, active, NewStateData, TimeToSync};

      % a proto-node requested the value associated with a particular key
      {proto_request, Key} ->
         {reply, bitcask:get(Store, Key), active, StateData, TimeToSync};

      % respond to a periodic sync request from a proto-node
      {sync_request, KnownKeys} ->
         AllKeys = sets:from_list(bitcask:list_keys(Store)),
         Remaining = sets:subtract(AllKeys, KnownKeys),
         case sets:size(Remaining) of
            0 ->
               {reply, {done_syncing, Range}, active, StateData, TimeToSync};
            _ ->
               Keys = lists:sublist(sets:to_list(Remaining), ziryab_config:get(sync_rate)),
               KVs = [ {K, bitcask:get(Store, K)} || K <- Keys ],
               {reply, {sync_reply, KVs}, active, StateData, TimeToSync}
         end;

      % received key/value pairs for syncing
      {sync_reply, KeysValues = [{Key, _} | _]} ->
         % get the sync state related to the obtained keys
         OldTuple = {SyncSrcs, SyncedKeys} = core_utils:srcsAndKeys(Key, SyncState),
         % XXX: what to do when we receive sync_reply from non-source?

         % only add pairs for which we do not have the most up-to-date value yet
         NewKeys = sets:from_list( lists:map(
            fun({K, V}) ->
                  bitcask:put(Store, K, V),
                  K
            end,
            lists:filter(
               fun({K, _}) -> not sets:is_element(K, SyncedKeys) end,
               KeysValues
            )
         ) ),

         % update the list of up-to-date key/value pairs
         NewSyncState = [ {SyncSrcs, sets:union(SyncedKeys, NewKeys)} | lists:delete(OldTuple, SyncState) ],
         {next_state, active, StateData#state{sync_state = NewSyncState}, TimeToSync};

      % nothing more to sync
      {done_syncing, _SrcRange = {Start, _}} ->
         NewSyncState = lists:delete(core_utils:srcsAndKeys(Start, SyncState), SyncState),
         {next_state, active, StateData#state{sync_state = NewSyncState}, TimeToSync};

      % time to do a sync
      timeout ->
         {Srcs, SyncedKeys} = lists:nth(random:uniform(NumSyncSrcs), SyncState),
         core_utils:sync(Srcs, {sync_request, SyncedKeys}),
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
handle_sync_event({fork, [ForkedRange]}, _, State, StateData = #state{range=Range, store=Store}) ->
   Dataroot = ziryab_config:get(bitcask_dir),

   % close the current bitcask store to clear locks
   ok = bitcask:close(Store),

   % current and forked data directory names
   {Start, End} = Range,
   RangeStr = integer_to_list(Start) ++ "-" ++ integer_to_list(End),
   Dirname = filename:join(Dataroot, RangeStr),

   {ForkedStart, ForkedEnd} = ForkedRange,
   ForkedRangeStr = integer_to_list(ForkedStart) ++ "-" ++ integer_to_list(ForkedEnd),
   ForkedDirName = filename:join(Dataroot, ForkedRangeStr),

   % copy the contents of the old bitcask dir to the new one
   os:cmd("cp -r " ++ Dirname ++ " " ++ ForkedDirName),

   % update the range for the forked's state data
   ClonedData = StateData#state{ range = ForkedRange },

   % reopen the local store
   ReopenedStore = bitcask:open(Dirname, ziryab_config:get(bitcask_opts)),

   % start a forked core and reply
   ForkedCore = gen_fsm:start(core_fsm, [forked, State, ClonedData], []),
   {reply, ForkedCore, State, StateData#state{store = ReopenedStore}}.


terminate(Reason, _State, _StateData = #state{store = Store}) ->
   bitcask:close(Store),
   Reason.


%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% Unimplemented gen_fsm callbacks
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

handle_event(_Event, _State, _StateData) ->
   exit("call to core_fsm:handle_event unimplemented.").

handle_info(_Info, _Stat, StateData) ->
   {stop, badmsg, StateData}.

code_change(_OldVsn, StateName, State, _Extra) ->
   {ok, StateName, State}.



