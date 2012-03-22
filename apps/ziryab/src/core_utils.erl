-module(core_utils).
-export([
      srcsAndKeys/2,
      sync/2
   ]).

-include("ziryab.hrl").

%%%%%%%%%
% UTILS %
%%%%%%%%%


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
   Timeout = ziryab_config:get(syc_timeout),

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
      Timeout -> sync(Srcs, Request)
   end.

