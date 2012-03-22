-module(ziryab_console).
-export([
      join/1,
      ziryab_up/1,
      cluster_state/1
   ]).

join([NodeStr]) when is_list(NodeStr) ->
   join(node(), list_to_atom(NodeStr)).

ziryab_up([]) ->
   gen_server:call(ziryab_cluster_manager, ziryab_up).

cluster_state([]) ->
   gen_server:call(ziryab_cluster_manager, cluster_state).

%%%%%%%%%%%%%%%%%
% Private Utils %
%%%%%%%%%%%%%%%%%


join(Node, Node) ->
   {error, self_join};
join(LocalNode, Node) ->
   case net_adm:ping(Node) of
      pang ->
         {error, not_reachable};
      pong ->
         gen_server:call({ziryab_cluster_manager, Node}, {join, LocalNode})
   end.


%%%%%%%%%
% Utils %
%%%%%%%%%

str_to_node(Node) when is_atom(Node) ->
   str_to_node(atom_to_list(Node));
str_to_node(NodeStr) ->
   case string:tokens(NodeStr, "@") of
      [NodeName] ->
         % Node name only; no host name. If the local node has a hostname,
         % append it
         case node_hostname() of
            [] ->
               list_to_atom(NodeName);
            HostName ->
               list_to_atom(NodeName ++ "@" ++ HostName)
         end;

      _ ->
         list_to_atom(NodeStr)
   end.

node_hostname() ->
   NodeStr = atom_to_list(node()),
   case string:tokens(NodeStr, "@") of
      [_NodeName, HostName] ->
         HostName;
      _ ->
         []
   end.
