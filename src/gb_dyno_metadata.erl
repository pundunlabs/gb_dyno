%%%===================================================================
%% @author Erdem Aksu
%% @copyright 2016 Pundun Labs AB
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
%% implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%% -------------------------------------------------------------------
%% @doc
%% Module handling the local metadata kept for a pundun node that is
%% used in gossip communication.
%% @end
%%%===================================================================

-module(gb_dyno_metadata).

%% API
-export([init/1,
	 merge_metadata/2]).

-type proplist() :: [{atom(), term()}].

%%--------------------------------------------------------------------
%% @doc
%% Initialize metadata tables at startup.
%% @end
%%--------------------------------------------------------------------
-spec init(Opts :: proplist()) ->
    ok.
init(Opts) ->
    case enterdb:read("metadata_ix", [{"tag", "topo"}, {"ix", 1}]) of
	{ok, _Hash} ->
	    ok;
	{error,"no_table"} ->
	    create_metadata(Opts);
	{error, "table_closed"} ->
	    open_tables()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Merge metadata of two nodes.
%% @end
%%--------------------------------------------------------------------
-spec merge_metadata({Node1 :: atom(), Data1 :: proplist()},
		     {Node2 :: atom(), Data2 :: proplist()}) ->
    {ok, Merged :: proplist()} | {error, Reason :: term()}.
merge_metadata({Node1, Data1}, {Node2, Data2}) ->
    Node1Cluster = proplists:get_value(cluster, Data1), 
    Node2Cluster = proplists:get_value(cluster, Data2), 
    merge_metadata(Node1Cluster, Node2Cluster,
		   {Node1, Data1}, {Node2, Data2}).

-spec merge_metadata(LC :: string(),
		     RC :: string(),
		     {Node1 :: atom(), Data1 :: proplist()},
		     {Node2 :: atom(), Data2 :: proplist()}) ->
    {ok, Merged :: proplist()} | {error, Reason :: term()}.
merge_metadata(C, C, ND1, ND2) ->
    merge_nodes(ND1, ND2, [{cluster, C}]);
merge_metadata(_, _, _, _) ->
    {error, "different_clusters"}.

-spec merge_nodes({Node1 :: atom(), Data1 :: proplist()},
		  {Node2 :: atom(), Data2 :: proplist()},
		  Acc :: proplist()) ->
    {ok, Merged :: [{atom(), term()}]}.
merge_nodes({Node1, Data1}, {Node2, Data2}, Acc) ->
    Nodes1 = proplists:get_value(nodes, Data1), 
    Nodes2 = proplists:get_value(nodes, Data2),
    case merge_nodes({Node1, Nodes1}, {Node2, Nodes2}) of
	{ok, Nodes} ->
	    {ok, lists:reverse([{nodes, Nodes} | Acc])};
	{error, Reason} ->
	    {error, Reason}
    end.

-spec merge_nodes({Node1 :: atom(), Nodes1 :: proplist()},
		  {Node2 :: atom(), Nodes2 :: proplist()}) ->
    {ok, Nodes :: proplist()} | {error, Reason :: term()}.
merge_nodes({Node1, Nodes1}, {Node2, Nodes2}) ->
    Map = maps:from_list(Nodes1),
    case check_conflict(Node1, Node2, Map, Nodes2) of
	{ok, AddNodes} ->
	    NewMap = maps:merge(Map, maps:from_list(AddNodes)),
	    Merged = maps:to_list(NewMap),
	    {ok, lists:sort(fun compare_nodes/2, Merged)};
	{error, {conflict, Node}} ->
	    {error, {conflict, Node}}
    end.

-spec check_conflict(Node1 :: atom(), Node2 :: atom(),
		     Map :: map(), Nodes2 :: proplist()) ->
    {ok, Nodes :: proplist()} | {error, {conflict, Node :: atom()}}.
check_conflict(Node1, Node2, Map, Nodes2) ->
    check_conflict(Node1, Node2, Map, Nodes2, []).

-spec check_conflict(Node1 :: atom(), Node2 :: atom(),
		     Map :: map(), Nodes2 :: proplist(),
		     Acc :: proplist()) ->
    {ok, Nodes :: proplist()} | {error, {conflict, Node :: atom()}}.
check_conflict(Node1, Node2, Map, [{Node1, _Data} | Rest], Acc) ->
    check_conflict(Node1, Node2, Map, Rest, Acc);
check_conflict(Node1, Node2, Map, [{Node2, Data} | Rest], Acc) ->
    check_conflict(Node1, Node2, Map, Rest, [{Node2, Data} | Acc]);
check_conflict(Node1, Node2, Map, [{Node, Data} | Rest], Acc) ->
    case maps:get(Node, Map, Data) of
        Data ->
	   check_conflict(Node1, Node2, Map, Rest, Acc);
        _ ->
	   {error, {conflict, Node}}
    end;
check_conflict(_, _, _Map, [], Acc) ->
    {ok, Acc}.

%%--------------------------------------------------------------------
%% @doc
%% Ordering function used as an argument to sort function.
%% Orders first on dc, second on rack, and last on node name. 
%% @end
%%--------------------------------------------------------------------
-spec compare_nodes({NodeA :: atom(), DataA :: proplist()},
		    {NodeB :: atom(), DataB :: proplist()}) ->
    true | false.
compare_nodes({NodeA, DataA},
	      {NodeB, DataB}) ->
    DcA =  proplists:get_value(dc, DataA),
    DcB =  proplists:get_value(dc, DataB),
    case compare_attribute(DcA, DcB) of
	true -> true;
	false -> false;
	equal ->
	    RackA =  proplists:get_value(rack, DataA),
	    RackB =  proplists:get_value(rack, DataB),
	    case compare_attribute(RackA, RackB) of
		true -> true;
		false -> false;
		equal -> NodeA =< NodeB
	    end
    end.

-spec compare_attribute(A :: term(), DcB :: term()) ->
    true | false | equal.
compare_attribute(A, B) ->
    if
	A < B -> true;
	A > B -> false;
	true -> equal
    end.
%%--------------------------------------------------------------------
%% @doc
%% Create tables for metadata.
%% @end
%%--------------------------------------------------------------------
-spec create_metadata(Options :: proplist()) ->
    ok.
create_metadata(Options) ->
    Cluster = proplists:get_value(cluster, Options), 
    DC = proplists:get_value(dc, Options), 
    Rack = proplists:get_value(rack, Options),

    NodeData = [{node(), [{dc, DC}, {rack, Rack}]}],
    MetaData = [{cluster, Cluster}, {nodes, NodeData}],

    Hash = gb_hash:hash(sha, MetaData),
    
    {error,"no_table"} = enterdb:table_info("metadata", [name]),
    TabOpts = [{type, leveldb},
	       {data_model, binary},
	       {comparator, descending},
	       {time_series, false},
	       {shards, 1}],

    ok = enterdb:create_table("metadata_ix",
			      ["tag", "ix"], ["hash"], [], TabOpts),
    ok = enterdb:create_table("metadata",
			      ["tag", "hash"], ["data"], [], TabOpts),
    ok = enterdb:write("metadata_ix",
		       [{"tag", "topo"}, {"ix", 1}], [{"hash", Hash}]),
    ok = enterdb:write("metadata",
		       [{"tag", "topo"}, {"hash", Hash}],
		       [{"data", MetaData}]).

%%--------------------------------------------------------------------
%% @doc
%% Open already existing metadata tables.
%% @end
%%--------------------------------------------------------------------
-spec open_tables() ->
    ok.
open_tables() ->
    ok = enterdb:open_table("metadata_ix"),
    ok = enterdb:open_table("metadata"),
    {ok, _} = enterdb:read("metadata_ix", [{"tag", "topo"},{"ix", 1}]),
    ok.
