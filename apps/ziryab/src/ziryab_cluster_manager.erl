-module(ziryab_cluster_manager).
-behaviour(gen_server).
-define(SERVER, ?MODULE).

-record(cluster, {servers = []}).

%% ------------------------------------------------------------------
%% API Function Exports
%% ------------------------------------------------------------------

-export([start_link/0]).

%% ------------------------------------------------------------------
%% gen_server Function Exports
%% ------------------------------------------------------------------

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

%% ------------------------------------------------------------------
%% API Function Definitions
%% ------------------------------------------------------------------

start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init(_Args) ->
   {ok, #cluster{servers=[node()]}}.

handle_call({join, Node}, _From, State=#cluster{servers=Servers}) ->
   {reply, ok, State#cluster{servers = [Node | Servers]}};

handle_call(cluster_state, _From, State) ->
   io:format("~p~n", [State]),
   {reply, ok, State};

handle_call(ziryab_up, _From, State=#cluster{servers=Servers}) ->
   SegSize = ziryab_config:get(segment_size),
   if length(Servers) < 2 * SegSize ->
         {reply, {error, insufficient_servers}, State};
      true ->
         RepProtocol = ziryab_config:get(replication_protocol),
         RepArgs = ziryab_config:get(replication_args, []),
         CoreArgs = ziryab_config:get(core_args, []),

         {Seg1, Seg2} = lists:split(SegSize, lists:sublist(Servers, 2*SegSize)),
         ziryab:start(CoreArgs, [Seg1, Seg2], {RepProtocol, RepArgs}),
         {reply, ok, State}
   end;

handle_call(_Request, _From, State) ->
    {reply, ok, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%% ------------------------------------------------------------------
%% Internal Function Definitions
%% ------------------------------------------------------------------

