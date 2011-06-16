-module(replica).
-export([new/3]).

-include("common.hrl").

new(Id, CoreSettings, {RepProtocol, RepArgs}) ->
   Core = core:new(Id, CoreSettings),
   init(Core, RepProtocol, RepArgs).

init(Core, singleton, Args) ->
   loop(Core, singleton, Args);

init(Core, Protocol, Args) ->
   receive
      {Ref, Client, _, {init_conf, Conf = #conf{pids = Replicas}}} ->
         Client ! {Ref, ok},

         case Protocol of
            primary_backup ->
               [Primary | Backups] = Replicas,
               if
                  Primary == self() ->
                     loop(Core, Protocol, {is_primary, Conf#conf{pids = Backups}});
                  ?ELSE ->
                     loop(Core, Protocol, {is_backup, Primary})
               end;

            chain ->
               Last = lists:last(Replicas),
               IPN = utils:ipn(self(), Replicas),
               loop(Core, Protocol, [ {ipn, IPN} , {last, Last} ]);

            quorum ->
               QuorumArgs = Args ++ [{others, Conf#conf{pids =
                        lists:delete(self(), Replicas)}}],
               loop(Core, Protocol, QuorumArgs)
         end
   end.


% Main loop for a standalone replica (i.e. no replication)
loop(Core, singleton, _Args) ->
   receive
      {Ref, Client, _, Request} ->
         Client ! {Ref, Core:do(Request)},
         loop(Core, singleton, _Args)
   end;


% Main loop for a primary/backup replica
loop(Core, primary_backup, Args) ->
   receive
      {Ref, Client, _, Request} ->
         case {Core:is_mutating(Request), Args} of
            {true, {is_primary, Backups}} ->
               utils:multicall(Backups, Request),
               Client ! {Ref, Core:do(Request)};
            _ ->
               Client ! {Ref, Core:do(Request)}
         end
   end,
   loop(Core, primary_backup, Args);


% Main loop for a chain replica
loop(Core, chain, Args = [ {ipn, {_Index, _Prev, Next}} , {last, Last} ]) ->
   receive
      {Ref, Client, _, Request} = Msg ->
         case {Core:is_mutating(Request), Next} of
            {true, chain_tail} ->
               Client ! {Ref, Core:do(Request)};
            {true, _} ->
               Core:do(Request),
               Next ! Msg;
            {false, chain_tail} ->
               Client ! {Ref, Core:do(Request)};
            {false, _} ->
               Last ! Msg
         end
   end,
   loop(Core, chain, Args);


% Main loop for a quorum replica
loop(Core, quorum, Args=[ {r_quorum, R} , {w_quorum, W} , {others, Others} ]) ->
   receive
      {Ref, Client, _, {orig_req, Request}} ->
         QSize = case Core:is_mutating(Request) of
            true -> W;
            false -> R
         end,

         Resps = utils:multicall(Others, {quorum_req, Request}, QSize-1),
         Client ! {Ref, [ Core:do(Request) | Resps ]};

      {Ref, Client, _, {quorum_req, Request}} ->
         Client ! {Ref, Core:do(Request)}
   end,
   loop(Core, quorum, Args).







