-module(ziryab_core).

% core's public interface
-export([
      % Required for a repobj core
      new/2,
      do/2,
      fork/2,
      is_mutating/1,

      % Extra
      add_sync_srcs/3
   ]).


% start a new key/value store core on this node
new(Range, SyncState) when is_list(SyncState) ->
   {ok, Pid} = gen_fsm:start(core_fsm, [new, Range, SyncState], []),
   Pid.

% create a forked copy of this kv core on this node
fork(Core, Args) ->
   {ok, Pid} = gen_fsm:sync_send_all_state_event(Core, {fork, Args}),
   Pid.

% perform a command on the current kv core
do(Core, Command) ->
   gen_fsm:sync_send_event(Core, Command).

% return true if executing this "external" command will change the local state
is_mutating(Command) ->
   case Command of
      {do_command, {tx, _, _}}              -> true;
      {do_command, {_, delete}}             -> true;
      {do_command, {_, {put, _}}}           -> true;
      {do_command, {sequencer, _}}          -> true;
      {do_command, {rm_out_of_range, _, _}} -> true;
      _ -> false
   end.

% add sync sources
add_sync_srcs(Core, Range, SyncCores) ->
   gen_fsm:sync_send_all_state_event(Core, {add_sync_srcs, Range, SyncCores}).


