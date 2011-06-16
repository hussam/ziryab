-module(ziryab).
-export([start/3, put/2, get/1, split/1, merge/1]).

-include("ziryab.hrl").
-include("params.hrl").

% Start a new Key/Value Store
start(CoreArgs, SegsNodes = [_Nodes | _], RepSettings) when is_list(_Nodes) ->
   % create a list of consecutive segments
   SegWidth = trunc(?RANGE / length(SegsNodes)),
   CoreSettings = {ziryab_core, CoreArgs},
   {_, Segs} = {_, [Seg1 | _]} = lists:foldl(
      fun(Nodes, {Start, Acc}) ->
            End = Start+SegWidth,
            NewAcc = [
               repobj:new({Start, End}, CoreSettings, Nodes, RepSettings, ?TO) | Acc
            ],
            {End, NewAcc}
      end,
      {0, []},
      SegsNodes),

   % set the successsor relation for the different segments
   lists:foldr(fun(Seg, Succ) -> set_succ(Seg,Succ), Seg end, Seg1, Segs),

   % start the kvs tracker on all nodes
   [ spawn(N, fun() -> ziryab_tracker:start(Segs) end)
      || N <- lists:flatten(SegsNodes) ].

% Associate a new value with this key
put(K, V) ->
   Key = erlang:phash2(K),
   Value = {K, V},

   Segment = route(Key, ziryab_tracker:view()),
   repobj:cmd(Segment, {Key, {put, Value}}, ?TO).

% Get the value associated with this key
get(K) ->
   Key = erlang:phash2(K),

   Segment = route(Key, ziryab_tracker:view()),
   repobj:cmd(Segment, {Key, get}, ?TO).

% Create a new segment starting at the given Key
% (done by splitting the segment containing the given key into two)
split(K) ->
   Key = erlang:phash2(K),

   View = ziryab_tracker:view(),
   OldSeg = #conf{id = {Start, End}, version = Vn} = route(Key, View),

   if
      % I don't want to allow splits at the beginning of a segment's range since
      % this leads to [Start, Start) segments which are hard to understand
      Start =:= Key ->
         ok;
      ?ELSE ->
         % wedge the segment containing the splitting key
         repobj:wedge(OldSeg, ?TO),

         % XXX: the current implementation updates the successor configuration only
         % after the new split segments are created and unwedged. Fix that

         % create the new segment by forking off the old one
         ForkedObj = repobj:fork(OldSeg, ?TO),

         % update the configuration at [Start, Key) and unwedge
         Seg1 = ForkedObj#conf{id = {Start, Key}, version = Vn + 1},
         repobj_utils:multicall(ForkedObj, {update_conf, Seg1}, ?TO),

         % update the configuration at [Key, End) and unwedge
         Seg2 = OldSeg#conf{id = {Key, End}, version = Vn + 1},
         repobj_utils:multicall(OldSeg, {update_conf, Seg2}, ?TO),

         % fix successor relations
         Pred = findPred(OldSeg, View),
         set_succ(Pred, Seg1),
         set_succ(Seg1, Seg2),

         % update the kvs tracker
         ziryab_tracker:addConfs([Seg1, Seg2]),

         % remove the keys out of range in both segments
         repobj:cmd(Seg1, rm_out_of_range, ?TO),
         repobj:cmd(Seg2, rm_out_of_range, ?TO),

         ok   % we're done
   end.


% Merge the segment starting with 'Key' with its predecessor
merge(K) ->
   Key = erlang:phash2(K),

   View = ziryab_tracker:view(),
   Seg1 = #conf{id = {Start1, End1}, version = Vn1} = route(Key, View),

   if
      % nothing to do if the merge point is not at boundary of two segments
      Start1 =/= Key ->
         ok;
      ?ELSE ->
         % get the predecessor of Seg1
         Seg2 = #conf{id = {Start2, Start1}, version = Vn2} = findPred(Seg1, View),

         % wedge both segments
         repobj:wedge(Seg1, ?TO),
         repobj:wedge(Seg2, ?TO),

         % update the configuration at the new merged segment and unwedge
         NewSeg = Seg1#conf{id = {Start2, End1}, version = erlang:max(Vn1, Vn2) + 1},
         repobj_utils:multicall(Seg1, {update_conf, NewSeg}, ?TO),

         % fix the predecessor's successor
         {ok, Pred} = findPred(Seg2, View),
         conf:setSucc(Pred, NewSeg),

         % update the kvs tracker
         ziryab_tracker:addConfs([NewSeg]),

         ok   % we're done
   end.


%%%%%%%%%%%
% Private %
%%%%%%%%%%%

% Set the segment's successor
set_succ(Seg, Succ) ->
   repobj:cmd(Seg, {sequencer, {set_succ, Succ}}, ?TO).

% Delete the segment's successor
del_succ(Seg) ->
   repobj:cmd(Seg, {sequencer, del_succ}, ?TO).



%%%%%%%%%%%%%
% Utilities %
%%%%%%%%%%%%%

route(Key, [Seg = #conf{id = {Start, End}} | _])
   when Start =< Key , Key < End -> Seg;
route(Key, [_ | Tail]) ->
   route(Key, Tail);
route(_, []) ->
   exit(key_not_found_in_routing).  % should not happen

findPred(#conf{id = {0, _}} , Segs) ->
   lists:last(Segs);
findPred(#conf{id = {Start, _}} , [Pred = #conf{id = {_, Start}} | _]) ->
   Pred;
findPred(Seg, [_ | Tail]) ->
   findPred(Seg, Tail);
findPred(_, []) ->
   exit(pred_not_found).   % should not happen

