-module(ziryab_backend).
-export([
      start/2,
      stop/1,
      do/3,
      export/1,
      export/2,
      import/1
   ]).

-include("ziryab.hrl").

% Start a backend store
start(bitcask, {RootDir, BitcaskOpts}) ->
   {Meg, Sec, Mic} = now(),
   Now = integer_to_list((Meg * 1000000000) + (1000 * Sec) + (Mic / 1000)),

   case bitcask:open(filename:join(RootDir, Now) , BitcaskOpts) of
      {error, _} = Err -> {error, {bitcask_err, Err}};
      Store -> {ok, {bitcask, Store}}
   end;

start(ets, EtsOpts) ->
   {ok, {ets, ets:new(ziryab_backend, EtsOpts)}};

start(dets, {DetsFile, DetsOpts}) ->
   file:delete(DetsFile),
   {ok, Store} = dets:open_file(ziryab_backend, [{file, DetsFile} | DetsOpts]),
   {ok, {dets, Store}};

start(BackendType, _) ->
   {error, {unkown_backend, BackendType}}.



% Stop a backend store
stop({bitcask, Store}) -> bitcask:close(Store);
stop({dets, Store}) -> dets:close(Store);
stop({ets, Store}) -> ets:delete(Store).


% Perform put/get/delete operation
do({bitcask, Store}, Key, Op) ->
   BinKey = case is_binary(Key) of
      true  -> Key;
      false -> term_to_binary(Key)
   end,
   case Op of
      get -> bitcask:get(Store, BinKey);
      delete -> bitcask:delete(Store, BinKey);
      {put, Value} when is_binary(Value) -> bitcask:put(Store, BinKey, Value);
      {put, Value} -> bitcask:put(Store, BinKey, term_to_binary(Value))
   end;

do({dets, Store}, Key, Op) ->
   case Op of
      delete -> dets:delete(Store, Key);
      {put, Value} -> dets:insert(Store, {Key, Value});
      get ->
         case dets:lookup(Store, Key) of
            [] -> ?KNF;
            [{Key, Value}] -> Value
         end
   end;

do({ets, Store}, Key, Op) ->
   case Op of
      delete -> ets:delete(Store, Key);
      {put, Value} -> ets:insert(Store, {Key, Value});
      get ->
         case ets:lookup(Store, Key) of
            [] -> ?KNF;
            [{Key, Value}] -> Value
         end
   end.




% Export all of the store's data
export({ets, Store}) -> {ets, ets:tab2list(Store)};
export(_) -> [].     % TODO: implement this!!!!

% TODO: implement this!!!!
export(_, _) -> [].
import(_) -> [].
