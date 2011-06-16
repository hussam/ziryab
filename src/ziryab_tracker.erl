-module(ziryab_tracker).
-export([start/1, addConfs/1, addConfs/2, view/0, view/1]).

-include("ziryab.hrl").
-include("params.hrl").

% starts a tracker on the current node and adds the passed in configurations
start(Confs) ->
   % register a kvs tracker on this node if one doesn't exist already
   case whereis(ziryab_tracker) of
      undefined ->
         % NOTE: We might have a data race on a single node, and so using
         % 'erlang:whereis' doesn't help. However, if the registration throws an
         % error, that means that a tracker already exists on this node and so
         % we can just move on.
         catch(register(ziryab_tracker, spawn(fun runTracker/0)));
      _ ->
         ok
   end,
   % add the given configurations to the kvs tracker
   addConfs(Confs, node()).

% add a configuration to the current view of the tracker on this node
addConfs(Confs) ->
   addConfs(Confs, node()).

% add a configuration to the current view of the tracker on Node
addConfs(Confs, Node) ->
   Ref = make_ref(),
   {ziryab_tracker, Node} ! {Ref, self(), add_confs, Confs},
   receive
      {Ref, ok} ->
         ok
   end.

% Get the KVS view on the current node
view() ->
   view(node()).

% Get the KVS view on the given node
view(Node) ->
   Ref = make_ref(),
   {ziryab_tracker, Node} ! {Ref, self(), get_view},
   receive
      {Ref, View} -> View
   end.


% initialize the random number generator for gossiping and start the tracker loop
runTracker() ->
   {A1, A2, A3} = now(),
   random:seed(A1, A2, A3),
   trackerLoop(ordsets:new(), ctime() + ?GOSSIP_INTERVAL).


% this is used to track the different segments in the kvs.
% A "view" is a set of configuration, and TTG is time to send next gossip
trackerLoop(View, TTG) ->
   Now = ctime(),
   Delta = TTG - Now,

   if
      Delta =< 0 ->     % it's gossip time
         requestLocalConfs(View),
         gossip(View),
         trackerLoop(View, TTG + ?GOSSIP_INTERVAL);

      ?ELSE ->          % it's not gossip time yet. Wait, listen, and try again
         receive
            {Ref, Client, get_view} ->
               Client ! {Ref, ordsets:to_list(View)},
               trackerLoop(View, TTG);
            {Ref, Client, add_confs, Confs} ->
               NewView = merge(View, Confs),
               Client ! {Ref, ok},
               trackerLoop(NewView, TTG);
            {gossip, PeerView} ->
               trackerLoop( merge(View, PeerView), TTG );
            {_, Conf = #conf{}} ->
               NewView = merge(View, [Conf]),
               trackerLoop(NewView, TTG);

            _ ->  % ignore everything else
               trackerLoop(View, TTG)

         after round(Delta) ->
               trackerLoop(View, TTG)
         end
   end.


% ask all local chain members to send in their configurations
requestLocalConfs(View) ->
   lists:foreach(
      fun(Pid) -> Pid ! {ziryab_tracker, self(), get_conf} end,
      lists:flatten([Pids || #conf{pids = Pids} <- ordsets:to_list(View)])
   ).

% send a gossip message to the tracker of another node
gossip(View) ->
   AllNodes = ordsets:fold(
      fun(#conf{pids = Pids}, Nodes) ->
            ordsets:union( Nodes, ordsets:from_list([ node(Pid) || Pid <- Pids]) )
      end,
      ordsets:new(),
      View),

   OtherNodes = ordsets:to_list( ordsets:del_element(node(), AllNodes) ),

   N = length(OtherNodes),
   if N > 0 ->
      Node = lists:nth(random:uniform(N), OtherNodes),
      {ziryab_tracker, Node} ! {gossip, View};
   ?ELSE ->
      do_nothing
   end.

% merge two views, each of which is a list of configurations
merge(View1, View2) ->
   Set1 = ordsets:filter(fun(Conf) -> isUpToDate(Conf, View2) end, View1),
   Set2 = ordsets:filter(fun(Conf) -> isUpToDate(Conf, View1) end, View2),
   ordsets:union(ordsets:from_list(Set1), ordsets:from_list(Set2)).

% check if the given configuration is not outdated by an element in the view.
isUpToDate(#conf{id = {Start1, End1}, version = Vn1}, View) ->
   not lists:any(
      fun(#conf{id = {Start2, End2}, version = Vn2}) ->
            isOverlap({Start1, End1}, {Start2, End2}) and (Vn2 > Vn1)
      end,
      ordsets:to_list(View)).

% check if two ranges overlap.  Four cases because of circularity.
isOverlap({Start1, End1}, {Start2, End2}) ->
   {S1, E1} = fixRange({Start1, End1}),
   {S2, E2} = fixRange({Start2, End2}),
   ((S1 =< S2) and (S2 < E1)) or ((S2 =< S1) and (S1 < E2)).

% make sure End > Start to simplify range overlap computation
fixRange({Start, End}) ->
   if
      End =< Start ->
         {Start, End + ?RANGE};
      ?ELSE ->
         {Start, End}
   end.

% return time in millisecond
ctime() ->
   {Meg, Sec, Mic} = now(),
   1000 * ( (Meg * 1000000) + Sec + (Mic / 1000000) ).

