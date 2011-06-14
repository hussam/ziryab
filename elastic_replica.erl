-module(elastic_replica).
-export([new/3]).

-include("common.hrl").


% Interface

% when active
%-------------
% do_command
% wedge
% fork
% get_conf

% when wedged
%-----------------
% update_conf
% wedge
% fork
% get_conf


new(Id, CoreSettings, RepSettings) ->
   Core = core:new(Id, CoreSettings),
   init(Core, RepSettings).

init(Core, {Protocol, _Args}) ->
   % XXX: A queue is presumably more efficient than a list or an ordered_set ETS?
   Unstable = queue:new(),
   Id = Core:id(),
   receive
      {Ref, Client, {Id, 1}, {init_conf, Conf}} ->
         Client ! {Ref, ok},

         case Protocol of
            {elastic, primary_backup} ->
               [Primary | Backups] = Conf#conf.pids,
               Self = self(),
               Role = case Primary of
                  Self -> primary;
                  _ -> backup
               end,

               activeLoop(Core, Conf, Unstable,
                           {Role, Conf#conf{pids = Backups}});

            {elastic, chain} ->
               IPN = utils:ipn(self(), Conf#conf.pids),
               activeLoop(Core, Conf, Unstable, {ipn, IPN})
         end
   end.

% State: IMMUTABLE
% An immutable replica remains so for the rest of its life in this configuration.
% The only things it can do is pass down its history (stable + unstable) to a
% caller, or upgrade to a successor configuration.
immutableLoop(Core, Conf = #conf{id = Id, version = Vn}, Unstable) ->
   receive
      % Transition: "inheritHistory2"
      % For practical considerations, a replica can inherit its history from a
      % prior configuration (i.e. upgrade its configuration).
      {Ref, Client, {Id, Vn},
         {update_conf, C = #conf{version = Vn2, pids = Pids, protocol = Protocol}}}
         when Vn2 > Vn ->
            % XXX: might need an explicit listing of a configuration's
            % predecessors to make sure the proto-sources are valid

            case { lists:member(self(), Pids) , Protocol } of
               {true, {elastic, chain}} ->
                  Client ! {Ref, ok},
                  activeLoop(Core, C, Unstable, {ipn, utils:ipn(self(), Pids)});

               {true, {elastic, primary_backup}} ->
                  Client ! {Ref, ok},
                  [Primary | Backups] = Pids,
                  Self = self(),
                  Role = case Primary of
                     Self -> primary;
                     _ -> backup
                  end,

                  activeLoop(Core, C, Unstable, {Role, C#conf{pids = Backups}});

               {false, _} ->
                  immutableLoop(Core, Conf, Unstable)
            end;

      % create a forked copy of this replica on this node
      {Ref, Client, {Id, Vn}, fork} ->
         ForkedCore = Core:fork(),
         Client ! {Ref, spawn(
               fun() ->
                     immutableLoop(ForkedCore, Conf, Unstable)
               end)},
         immutableLoop(Core, Conf, Unstable)
   end.


% State: ACTIVE
% When a replica is in an active state it can add commands to its history and
% respond to client requests. A replica stays in active state until it is wedged
activeLoop(Core,
            Conf = #conf{protocol = {elastic, Prtcl}, id = Id, version = Vn},
            Unstable,
            Args) ->

   % replica successor and predecessor if in chain replication
   {Prev, Next} = case { Prtcl , Args } of
      { chain , {ipn, {_I, P, N}} } ->
         {P, N};
      _ ->
         {undefined, undefined}
   end,

   % replica's role and the backups if in primary/backup replication
   {Role, Backups} = case { Prtcl , Args } of
      { primary_backup , {R, B} } ->
         {R, B};
      _ ->
         {undefined, undefined}
   end,

   receive
      % Transition: add a command to stable history
      {Ref, Client, {Id, Vn}, Req = {do_command, _}} = Msg ->
         case Prtcl of
            chain ->
               % when at the orderer, add command to unstable history
               % queue or perform command depending on position in chain
               % XXX: we either trust clients to send commands to the head of the chain,
               % or we have to use some sort of message authentication mechanisms
               case Next of
                  chain_tail ->
                     Client ! {Ref, Core:do(Req)},
                     Prev ! {Ref, self(), {Id, Vn}, stabilized},
                     activeLoop(Core, Conf, Unstable, Args);
                  _ ->
                     NewUnstable = queue:in({Ref, Req}, Unstable),
                     Next ! Msg,
                     activeLoop(Core, Conf, NewUnstable, Args)
               end;

            primary_backup ->
               % when at primary, multicast request to all backup replicas and
               % only add command to local history when all responses are
               % received
               case Role of
                  primary ->
                     utils:multicall(Backups, Req),
                     Client ! {Ref, Core:do(Req)};
                  backup ->
                     Client ! {Ref, Core:do(Req)}
               end,
               activeLoop(Core, Conf, Unstable, Args)
         end;


      % Transition: stabilize (modified version of learnPersistence)
      % move a command from stable to unstable history when all chain successors
      % have stabilized it as well
      {Ref, Next, {Id, Vn}, stabilized} when Prtcl =:= chain ->
         case queue:peek(Unstable) of
            {value, {Ref, Req}} ->
               Core:do(Req),
               NewUnstable = queue:tail(Unstable),

               if Prev /= chain_head ->
                     Prev ! {Ref, self(), {Id, Vn}, stabilized};
                  ?ELSE ->
                     done
               end,

               activeLoop(Core, Conf, NewUnstable, Args);

            _ ->
               activeLoop(Core, Conf, Unstable, Args)
         end;

      % Transition: wedgeState
      % take replica into immutable state
      {Ref, Client, {Id, Vn}, wedge} ->
         Client ! {Ref, wedged},
         immutableLoop(Core, Conf, Unstable);

      % create a forked copy of this replica on this node
      {Ref, Client, {Id, Vn}, fork} ->
         ForkedCore = Core:fork(),
         Client ! {Ref, spawn(
               fun() ->
                     activeLoop(ForkedCore, Conf, Unstable, Args)
               end)},
         activeLoop(Core, Conf, Unstable, Args);

      % Return current configuration
      {Ref, Client, get_conf} ->
         Client ! {Ref, Conf},
         activeLoop(Core, Conf, Unstable, Args);

      % ignore everything else
      _ ->
         activeLoop(Core, Conf, Unstable, Args)

   end.
