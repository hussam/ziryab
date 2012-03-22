-module(core_cmd).
-export([
      do/2
   ]).

-include("ziryab.hrl").

-define(KNF, not_found).


% do: perform a command's operation and returns the new state and a reply

do(ping, StateData) ->
   {pong, StateData};

% Handle sequencing (successor configuration) commands
do({sequencer, Command}, StateData = #state{succ = Succ}) ->
   case Command of
      % set successor
      {set_succ, NewSucc} ->
         { ok, StateData#state{succ = NewSucc} };

      % delete (and return) successor
      del_succ ->
         { Succ, StateData#state{succ = undefined} }
   end;

% Handle put/get commands
do( {Key, Command}, StateData = #state { store = Store, sync_state = SyncState } ) ->
   fprof:trace(start, "data/my_fprof.trace"),
   {Result, NewSyncState} = doRW( {value, Key}, Command, Store, SyncState ),
   fprof:trace(stop),
   fprof:profile(file, "data/my_fprof.trace"),
   fprof:analyse([{dest, "my_fprof.analysis"}, {cols, 120}]),
   { Result, StateData#state{sync_state = NewSyncState} };


% Handle transactional commands
do( {tx, TxId, {init, InitiatorPid}}, StateData = #state{ store = Store, sync_state = SyncState } ) ->
   {Result, SyncState2} = doRW( {tx_obj, TxId}, {put, {undecided, InitiatorPid}}, Store, SyncState ),
   { {tx_ok, Result}, StateData#state{sync_state = SyncState2} };


do( {tx, TxId, {commit, InitiatorPid}}, StateData = #state{ store = Store, sync_state = SyncState } ) ->
   {TxObj, SyncState2} = doRW( {tx_obj, TxId}, get, Store, SyncState ),
   case TxObj of
      {undecided, InitiatorPid} ->
         {_, SyncState3} = doRW( {tx_obj, TxId}, {put, {committed, InitiatorPid}}, Store, SyncState2 ),
         {_, SyncState4} = commit_local(TxId, Store, SyncState3),
         { {tx_ok, committed}, StateData#state{sync_state = SyncState4} };

      {Status, InitiatorPid} ->
         { {tx_err, {uncommittable_status, Status}}, StateData#state{sync_state = SyncState2} };

      _ ->
         { {tx_err, tx_obj_not_found}, StateData#state{sync_state = SyncState2} }
   end;


do( {tx, TxId, abort}, StateData = #state{ store = Store, sync_state = SyncState } ) ->
   {TxObj, SyncState2} = doRW( {tx_obj, TxId}, get, Store, SyncState ),
   case TxObj of
      {undecided, InitiatorPid} ->
         {_, SyncState3} = doRW( {tx_obj, TxId}, {put, {aborted, InitiatorPid}}, Store, SyncState2 ),
         {_, SyncState4} = doRW( {tx_buffer, TxId}, delete, Store, SyncState3 ),
         { {tx_ok, aborted}, StateData#state{sync_state = SyncState4} };

      {Status, _} ->
         { {tx_err, {unabortable_status, Status}}, StateData#state{sync_state = SyncState2} };

      _ ->
         { {tx_err, tx_obj_not_found}, StateData#state{sync_state = SyncState2} }
   end;


% Future TODO: separate store to disk store for committed tx operations and
% memory store for buffered operations and bookkeeping. Possible performance gain
do( {tx, TxId, {Key, Command, AltAction}} = Command,
         StateData = #state{ store = Store, sync_state = SyncState } ) ->

   {Timestamp, SyncState2} = doRW( {timestamp, Key}, get, Store, SyncState ),

   if
      Timestamp /= ?KNF andalso TxId < Timestamp ->   % Key has a newer timestamp than incoming TX operation
         { {tx_err, {abort_for, Timestamp}}, StateData#state{sync_state = SyncState2} };

      ?ELSE ->    % Timestamp == ?KNF orelse TxId >= Timestamp
         % Supporting interactive transactions requires checking the state of previous
         % transactions before each PUT/GET operation. Otherwise, atomicity will be sacrificed.
         NextAction = if
            TxId > Timestamp ->
               case ziryab:tx_status(Timestamp) of
                  aborted ->
                     {proceed, drop_buffered};
                  committed ->
                     {proceed, commit_buffered};
                  undecided ->
                     case AltAction of
                        abort_incoming -> {abort, {conflicting_tx, Timestamp}};
                        abort_current  -> {proceed, abort_current};
                        % TODO: the following should not abort
                        queue_incoming -> {abort, {unsupported_op, queue_incoming}}
                     end
               end;

            ?ELSE ->
               {proceed, no_side_action}
         end,

         case NextAction of
            {proceed, SideAction} ->
               SyncState3 = case SideAction of
                  drop_buffered ->
                     {_, SyncState3_} = doRW( {tx_buffer, Timestamp}, delete, Store, SyncState2 ),
                     SyncState3_;
                  commit_buffered ->
                     {_, SyncState3_} = commit_local(Timestamp, Store, SyncState2),
                     SyncState3_;
                  abort_current ->
                     {_, SyncState3_} = abort(Timestamp, Store, SyncState2),
                     SyncState3_;
                  no_side_action ->
                     SyncState2
               end,

               % Update the key's timestamp
               {_, SyncState4} = doRW( {timestamp, Key}, {put, TxId}, Store, SyncState3 ),

               % Perform the command
               case Command of
                  get ->
                     {Value, SyncState5} = doRW( {value, Key}, get, Store, SyncState4 ),
                     { {tx_ok, Value}, StateData#state{sync_state = SyncState5} };

                  {put, Value} ->
                     {Result, SyncState5} = doRW( {tx_buffer, TxId}, get, Store, SyncState4 ),
                     TxBuffer = case Result of
                        ?KNF -> [];
                        TxBuffer_ -> TxBuffer_
                     end,
                     {Result, SyncState6} = doRW( {tx_buffer, TxId}, {put, [{Key, Value} | TxBuffer]}, Store, SyncState5 ),
                     { {tx_ok, Result}, StateData#state{sync_state = SyncState6} }
               end;


            {abort, Reason} ->
               {_, SyncState3} = abort(TxId, Store, SyncState2),
               { {tx_err, Reason}, StateData#state{sync_state = SyncState3} }
         end
   end;


% Remove all pairs where the key is not in the range [Start, End)
do( {rm_out_of_range, Start, End}, StateData = #state{ store = Store } ) ->
   Keys = bitcask:list_keys(Store),
   [ bitcask:delete(Store, Key) || Key <- Keys, Key < Start orelse Key >= End ],
   {StateData, ok}.

%%%%%%%%%
% private
%%%%%%%%%

abort( TxId, Store, SyncState ) ->
   % set the transaction object's status to ABORTED
   ziryab:tx_abort(TxId),
   % perform local clean-up
   doRW( {tx_buffer, TxId}, delete, Store, SyncState ).

commit_local( TxId, Store, SyncState ) ->
   {BufferedWrites, SyncState2} = doRW( {tx_buffer, TxId}, get, Store, SyncState ),
   lists:foldl(
      fun({Key, Value}, {Results, SyncStateIn}) ->
            {Result, SyncStateOut} = doRW( {value, Key}, {put, Value}, Store, SyncStateIn ),
            {[Result | Results], SyncStateOut}
      end,
      SyncState2,
      BufferedWrites
   ).

% Perform reads/writes to some store and potentially sync and/or update sync-state
doRW( K, Command, Store, SyncState ) ->
   Key = term_to_binary(K),   % TODO: fix this!
   case core_utils:srcsAndKeys(Key, SyncState) of
      none ->  % this is not a proto-node
         case Command of
            get ->
               {bitcask:get(Store, Key), SyncState};

            {put, Value} ->
               {bitcask:put(Store, Key, Value), SyncState};

            delete ->
               {bitcask:delete(Store, Key), SyncState}
         end;

      {SyncSrcs, SyncedKeys} = OldTuple ->   % this is a proto-node
         % compute the result
         Result = case Command of
            {put, Value} ->
               bitcask:put(Store, Key, Value);

            delete ->
               bitcask:delete(Store, Key);

            get ->
               case sets:is_element(Key, SyncedKeys) of
                  true ->
                     bitcask:get(Store, Key);
                  false ->
                     case core_utils:sync(SyncSrcs, {proto_request, Key}) of
                        {ok, Value} ->
                           bitcask:put(Store, Key, Value),
                           {ok, Value};
                        ?KNF ->
                           % delete any old mappings if present
                           bitcask:delete(Store, Key),
                           ?KNF
                     end
               end
         end,

         % compute the new synchronization state
         NewSyncState = [ {SyncSrcs, sets:add_element(Key, SyncedKeys)} | lists:delete(OldTuple, SyncState) ],

         % return
         {Result, NewSyncState}
   end.
