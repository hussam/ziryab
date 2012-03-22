-module(ziryab_config).

-export([
      get/1,
      get/2
   ]).


get(Key) ->
   case application:get_env(ziryab, Key) of
      {ok, Value} ->
         Value;
      undefined ->
         erlang:error("Missing configuration key", [Key])
   end.

get(Key, Default) ->
   case application:get_env(ziryab, Key) of
      {ok, Value} ->
         Value;
      _ ->
         Default
   end.
