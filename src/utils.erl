-module(utils).
-export([ipn/2,
      cast/2, multicast/2,
      call/2, anycall/2, multicall/2, multicall/3,
      listToETS/1, cloneETS/1, cloneDETS/1]).

-include("common.hrl").
-include("params.hrl").

% return the {Previous, Next} elements of a chain member
% the previous of the first chain member is chain_head
% the next of the last chain member is chain_tail
ipn(Pid, Chain) ->
   ipn(Pid, Chain, 1).
ipn(Pid, [Pid, Next | _], 1) -> {1, chain_head, Next};
ipn(Pid, [Prev, Pid, Next | _], Index) -> {Index + 1, Prev, Next};
ipn(Pid, [Prev, Pid], Index) -> {Index + 1, Prev, chain_tail};
ipn(Pid, [_ | Tail], Index) -> ipn(Pid, Tail, Index + 1).


% send an asynchronous request to the given replicated object
% returns the request's reference
cast(_Obj = #conf{id = Id, version = Vn, pids = [Hd|_]}, Request) ->
   Ref = make_ref(),
   Hd ! {Ref, self(), {Id, Vn}, Request},
   Ref.

% send an asynchronous request to all the processes of a replicated object
% returns the request's reference
multicast(_Obj = #conf{id = Id, version = Vn, pids = Pids}, Request) ->
   Ref = make_ref(),
   [ Pid ! {Ref, self(), {Id, Vn}, Request} || Pid <- Pids ],
   Ref.


% send synchronous request to a replicated object
call(_Obj = #conf{id = Id, version = Vn, pids = [Hd|_]}, Request) ->
   call(Hd, make_ref(), {Id, Vn}, Request).

% send synchronous request to a given Pid
call(Pid, Ref, Tag, Request) ->
   Pid ! {Ref, self(), Tag, Request},
   receive
      {Ref, Result} -> Result
   after
      ?TO -> call(Pid, Ref, Tag, Request)
   end.


% send parallel requests to all object replicas and wait for one response
anycall(Obj, Request) ->
   multicall(Obj, Request, 1).

% send parallel requests to all object replicas and wait for all responses
multicall(Obj = #conf{pids = Pids}, Request) ->
   multicall(Obj, Request, length(Pids)).

% send parallel requests to all object replicas and wait to get NumResponses
multicall(_Obj = #conf{id=Id, version=Vn, pids=Pids}, Request, NumResponses) ->
   Parent = self(),
   Ref = make_ref(),
   % create a sub-process for each Pid to make a call
   [ spawn(fun()-> Parent ! {Ref, Pid, call(Pid, Ref, {Id, Vn}, Request)} end)
      || Pid <- Pids],

   collectMany(Ref, [], NumResponses).


% Collect a required number of responses and return them
collectMany(_Ref, Responses, Required) when length(Responses) == Required ->
   Responses;
collectMany(Ref, Responses, Required) ->
   receive
      {Ref, Pid, Result} ->
         NewResponses = [ {Pid, Result} | lists:keydelete(Pid, 1, Responses) ],
         collectMany(Ref, NewResponses, Required)
   end.


% Convert a list to an ETS
listToETS(List) ->
   % TODO: clone properties of the old ETS
   Ets = ets:new(listToEts, [public]),
   ets:insert(Ets, List),
   Ets.

% Return a cloned copy of the Old ETS
cloneETS(Old) ->
   % TODO: clone properties of the old ETS
   New = ets:new(clonedEts, [public]),
   ets:insert(New, ets:tab2list(Old)),
   New.

cloneDETS(Old) ->
   OldFname = dets:info(Old, filename),
   {A,B,C} = now(),
   NewFname = OldFname ++ integer_to_list(A) ++ integer_to_list(B) ++ integer_to_list(C),
   file:copy(OldFname, NewFname),
   {ok, New} = dets:open_file(list_to_atom(NewFname), {file, NewFname}),
   New.
