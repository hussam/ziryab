-module(core, [Module, Id, Instance]).

% Core interface
-export([
      new/2,
      do/1,
      fork/0,
      is_mutating/1,
      id/0
   ]).

new(Id, {Module, Args}) ->
   instance(Module, Id, Module:new(Id, Args)).

do(Command) ->
   Module:do(Instance, Command).

fork() ->
   Module:fork(Instance).

is_mutating(Command) ->
   Module:is_mutating(Command).

id() ->
   Id.


