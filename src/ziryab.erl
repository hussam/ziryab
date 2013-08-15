-module(ziryab).

-include("ziryab.hrl").

% libdist state machine behaviour interface
-behaviour(ldsm).
-export([
      init_sm/1,
      handle_cmd/3,
      is_mutating/1,
      stop/2,
      export/1,
      export/2,
      import/1
   ]).

-define(DO(Store, Key, Op), ziryab_backend:do(Store, Key, Op)).

% start a new key/value store core on this node
init_sm({BackendType, BackendOpts}) ->
   case ziryab_backend:start(BackendType, BackendOpts) of
      {error, Error} -> exit(Error);
      {ok, Store} -> Store
   end.

% stop local ziryab backend store
stop(Store, _Reason) ->
   ziryab_backend:stop(Store).

% TODO: No need to export client ... added here just for debugging
export(Store) ->
   ziryab_backend:export(Store).

export(Store, Tag) ->
   ziryab_backend:export(Store, Tag).

import(ExportedStoreData) ->
   ziryab_backend:import(ExportedStoreData).

% return true if executing this "external" command will change the local state
is_mutating(Command) ->
   case Command of
      {tx, _, _}    -> true;
      {_, delete}   -> true;
      {_, {put, _}} -> true;
      _ -> false
   end.

% perform a command on the current key/value store
handle_cmd(Store, Command, _AllowSideEffects = ASE) ->
   Result = case Command of
      {_, ping} ->
         pong;

      {Key, get} ->
         ?DO(Store, Key, get);

      {Key, {put, _} = PutValue} ->
         ?DO(Store, Key, PutValue);

      {Key, delete} ->
         ?DO(Store, Key, delete);

      % Handle transactional commands
      {tx, TxId, TxCmd} ->
         case TxCmd of
            status ->
               case ?DO(Store, {tx_obj, TxId}, get) of
                  ?KNF -> {tx_err, {tx_obj_not_found, TxId}};
                  {Status, _} -> {tx_ok, {Status, TxId}}
               end;

            {init, InitiatorPid} ->
               ?DO(Store, {tx_obj, TxId}, {put, {undecided, InitiatorPid}}),
               {tx_ok, TxId};

            {commit, InitiatorPid} ->
               TxObj = ?DO(Store, {tx_obj, TxId}, get),
               case TxObj of
                  {undecided, InitiatorPid} ->
                     ?DO(Store, {tx_obj, TxId}, {put, {committed, InitiatorPid}}),
                     commit(TxId, Store),
                     {tx_ok, {committed, TxId}};

                  {Status, InitiatorPid} ->
                     {tx_err, {uncommittable_status, TxId, Status}};

                  _ ->
                     {tx_err, {tx_obj_not_found, TxId}}
               end;

            {commit_buffered, _} ->
               commit(TxId, Store),
               {tx_ok, {commited, TxId}};

            abort ->
               TxObj = ?DO(Store, {tx_obj, TxId}, get),
               case TxObj of
                  {undecided, InitiatorPid} ->
                     ?DO(Store, {tx_obj, TxId}, {put, {aborted, InitiatorPid}}),
                     ?DO(Store, {tx_buffer, TxId}, delete),
                     {tx_ok, {aborted, TxId}};

                  {Status, _} ->
                     {tx_err, {unabortable_status, TxId, Status}};

                  _ ->
                     {tx_err, tx_obj_not_found}
               end;

            {TxKey, TxOp, AltAction} ->
               % Future TODO: separate store to disk store for committed tx
               % operations and memory store for buffered operations and
               % bookkeeping. Possible performance gain
               Timestamp = ?DO(Store, {timestamp, TxKey}, get),

               if
                  % Key has a newer timestamp than incoming TX operation
                  Timestamp /= ?KNF andalso TxId < Timestamp ->
                     {tx_err, {abort_for, TxId, Timestamp}};

                  ?ELSE ->    % Timestamp == ?KNF orelse TxId >= Timestamp
                     % Supporting interactive transactions requires checking the
                     % state of previous transactions before each PUT/GET
                     % operation. Otherwise, atomicity will be sacrificed.
                     NextAction = if
                        Timestamp /= ?KNF andalso TxId > Timestamp ->
                           TxStatus = case handle_cmd(Store, {tx, Timestamp, status}, ASE) of
                              {reply, {tx_ok, {Status, _}}} ->
                                 Status;
                              {reply, {tx_err, _}} ->
                                 Tracker = conf_tracker:get_tracker(node()),
                                 Conf = conf_tracker:get_conf(Tracker),
                                 {ok, Status} = repobj:call(Conf, {tx,
                                       Timestamp, status}, ?TIMEOUT),
                                 Status
                           end,

                           case TxStatus of
                              aborted ->
                                 {proceed, drop_buffered};
                              committed ->
                                 {proceed, commit_buffered};
                              undecided ->
                                 case AltAction of
                                    abort_incoming ->
                                       {abort, {conflicting_tx, Timestamp}};
                                    abort_current ->
                                       {proceed, abort_current};
                                    Other ->
                                       {abort, {unsupported_op, Other}}
                                 end
                           end;

                        ?ELSE ->
                           {proceed, no_side_action}
                     end,

                     case NextAction of
                        {proceed, SideAction} ->
                           case SideAction of
                              drop_buffered ->
                                 ?DO(Store, {tx_buffer, Timestamp}, delete);
                              commit_buffered ->
                                 commit(Timestamp, Store);
                              abort_current ->
                                 abort(Timestamp, Store, ASE);
                              no_side_action ->
                                 do_nothing
                           end,

                           % Update the key's timestamp
                           ?DO(Store, {timestamp, TxKey}, {put, TxId}),

                           % Perform the command
                           case TxOp of
                              get ->
                                 {tx_ok, ?DO(Store, TxKey, get)};

                              {put, Value} ->
                                 {BufferId, _} = TxBuffer = case ?DO(Store, {tx_buffer, TxId}, get) of
                                    ?KNF -> { TxKey, [{TxKey, Value}] };
                                    {HdKey, Buf} -> {HdKey, [{TxKey, Value} | Buf]}
                                 end,
                                 ?DO(Store, {tx_buffer, TxId}, {put, TxBuffer}),
                                 {tx_ok, BufferId}
                           end;


                        {abort, Reason} ->
                           abort(TxId, Store, ASE),
                           {tx_err, Reason}
                     end
               end
         end
   end,
   {reply, Result}.


%%%%%%%%%
% private
%%%%%%%%%

abort(TxId, Store, ASE) ->
   % set the transaction object's status to ABORTED
   if  not ASE -> do_nothing;
         ?ELSE ->
            case handle_cmd(Store, {tx, TxId, abort}, ASE) of
               {reply, {tx_ok, _}} ->
                  done;
               {reply, {tx_err, {tx_obj_not_found, TxId}}} ->
                  Tracker = conf_tracker:get_tracker(node()),
                  Conf = conf_tracker:get_conf(Tracker),
                  repobj:call(Conf, {tx, TxId, abort}, ?TIMEOUT)
            end
   end,
   % perform local clean-up
   ?DO(Store, {tx_buffer, TxId}, delete).


commit(TxId, Store) ->
   {_, BufferedWrites} = ?DO(Store, {tx_buffer, TxId}, get),
   lists:foldl(
      fun({Key, Value}, Results) ->
            Result = ?DO(Store, Key, {put, Value}),
            [Result | Results]
      end,
      [],
      BufferedWrites
   ).
