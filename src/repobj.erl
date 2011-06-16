-module(repobj).
-export([new/4, cmd/2, fork/1, wedge/1, reconfigure/3, repair/2]).

-include("common.hrl").

% Create a new replicated object
new(Id, CoreSettings, Nodes, RepSettings = {Protocol, _}) ->
   Mod = case Protocol of {elastic, _} -> elastic_replica; _ -> replica end,
   Pids = [ spawn(N, Mod, new, [Id, CoreSettings, RepSettings]) || N <- Nodes ],
   Conf = #conf{id = Id, version = 1, protocol = Protocol, pids = Pids},

   % inform all the replicas of the configuration
   utils:multicall(Conf, {init_conf, Conf}),

   Conf.    % return the new configuration


% Execute a command synchronously on a replicated object
cmd(Obj, Command) ->
   utils:call(Obj, {do_command, Command}).

% Reconfigure a replicated object
% TODO: let user pass 'Args' for protocols that need them (e.g. Quorum replication)
reconfigure(Obj = #conf{version = Vn}, Id, RepProtocol) ->
   % wedge all replicas of the object
   utils:multicall(Obj, wedge),
   % update configuration and uwedge
   NewConf = Obj#conf{id = Id, version = Vn + 1, protocol = RepProtocol},
   utils:multicall(Obj, {update_conf, NewConf}),
   % return the new configuration
   NewConf.

% Fork a replicated object
% This will create a new set of replicas that have a copy of the object's state
fork(Obj) ->
   NewPids = [ Pid || {_, Pid} <- utils:multicall(Obj, fork) ],
   Obj#conf{pids = NewPids}.

% Wedge a replicated object
% A wedged object does not accept or execute commands until it is unwedged
wedge(Obj) ->
   utils:anycall(Obj, wedge).

% Repair a replicated object by replacing failed replicas
repair(_Obj = #conf{version = _Vn, pids = _Pids},
         _ReplaceNode) when is_function(_ReplaceNode) ->

   % TODO challenge: what defines a "dead/failed node"?
   erlang:error(not_implemented).

