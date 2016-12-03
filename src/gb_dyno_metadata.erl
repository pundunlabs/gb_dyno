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
	 commit_topo/1,
	 lookup_topo/0,
	 lookup_topo/1,
	 fetch_topo_history/0,
	 pull_topo/1,
	 remove_node/0]).

%%Exports for testing purpose
-export([merge_topo/3]).

-type proplist() :: [{atom(), term()}].
-type key() :: [{string(), term()}].
-type value() :: [{string(), term()}].
-type kvp() :: {key(), value()}.

-define(ALG, sha).

%%--------------------------------------------------------------------
%% @doc
%% Initialize metadata tables at startup.
%% @end
%%--------------------------------------------------------------------
-spec init(Opts :: proplist()) ->
    {ok, Hash :: integer()}.
init(Opts) ->
    case enterdb:read("gb_dyno_metadata", [{"tag","hash"},{"hash","current"}]) of
	{ok, Value} ->
	    {_, Hash} = lists:keyfind("data", 1, Value),
	    {ok, Hash};
	{error,"no_table"} ->
	    create_metadata(Opts);
	{error, "table_closed"} ->
	    open_tables()
    end.

%%--------------------------------------------------------------------
%% @doc
%% Create tables for metadata.
%% @end
%%--------------------------------------------------------------------
-spec create_metadata(Options :: proplist()) ->
    {ok , Hash :: integer()}.
create_metadata(Options) ->
    Cluster = proplists:get_value(cluster, Options), 
    DC = proplists:get_value(dc, Options), 
    Rack = proplists:get_value(rack, Options),

    Data = ordsets:from_list([{version, 1}, {dc, DC}, {rack, Rack}]),
    NodeData = [{node(), Data}],
    Metadata = [{cluster, Cluster}, {nodes, NodeData}],

    {error,"no_table"} = enterdb:table_info("gb_dyno_metadata", [name]),
    TabOpts = [{type, leveldb},
	       {data_model, map},
	       {comparator, descending},
	       {time_series, false},
	       {shards, 1},
	       {distributed, false}],

    ok = enterdb:create_table("gb_dyno_topo_ix",
			      ["ix"], TabOpts),
    ok = enterdb:create_table("gb_dyno_metadata",
			      ["tag", "hash"], TabOpts),
    commit_topo(Metadata, 1).

%%--------------------------------------------------------------------
%% @doc
%% Open already existing metadata tables.
%% @end
%%--------------------------------------------------------------------
-spec open_tables() ->
    {ok , Hash :: integer()}.
open_tables() ->
    ok = enterdb:open_table("gb_dyno_topo_ix"),
    ok = enterdb:open_table("gb_dyno_metadata"),
    {ok, {_, Value}, _} = enterdb:first("gb_dyno_topo_ix"),
    {_, Hash} = lists:keyfind("hash", 1, Value),
    {ok, Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Commit new metadata with topo(topology) tag. The committed data
%% represents the dyno topology from a local point of view.
%% @end
%%--------------------------------------------------------------------
-spec commit_topo(Metadata :: proplist()) ->
    {ok, Hash :: integer()}.
commit_topo(Metadata) ->
    {ok, {[{"ix", Ix}], _}, _} = enterdb:first("gb_dyno_topo_ix"),
    commit_topo(Metadata, Ix+1).

%%--------------------------------------------------------------------
%% @doc
%% Commit new metadata with topo(topology) tag and given index.
%% The committed data represents the dyno topology from a local
%% point of view.
%% @end
%%--------------------------------------------------------------------
-spec commit_topo(Metadata :: proplist(),
		  Ix :: pos_integer()) ->
    {ok, Hash :: integer()}.
commit_topo(Metadata, Ix) ->
    Hash = gb_hash:hash(?ALG, Metadata),
    ok = enterdb:write("gb_dyno_topo_ix",
		       [{"ix", Ix}], [{"hash", Hash}]),
    ok = enterdb:write("gb_dyno_metadata",
		       [{"tag", "topo"}, {"hash", Hash}],
		       [{"data", Metadata}]),
    ok = enterdb:write("gb_dyno_metadata",
		       [{"tag", "hash"}, {"hash", "current"}],
		       [{"data", Hash}]),
    {ok, Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Lookup latest topology metadata.
%% @end
%%--------------------------------------------------------------------
-spec lookup_topo() ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
lookup_topo() ->
    {ok, Value} = enterdb:read("gb_dyno_metadata", [{"tag", "hash"}, {"hash","current"}]),
    {_, Hash} = lists:keyfind("data", 1, Value),
    lookup_topo(Hash).

%%--------------------------------------------------------------------
%% @doc
%% Lookup for a topology metadata that is identified by it's hash
%% value.
%% @end
%%--------------------------------------------------------------------
-spec lookup_topo(Hash :: integer()) ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
lookup_topo(Hash) ->
    case enterdb:read("gb_dyno_metadata",[{"tag", "topo"}, {"hash", Hash}]) of
	{ok, [{"data", Metadata}]} ->
	    {ok, Metadata};
	{error,{not_found, _}} ->
	    {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get all data stored in gb_dyno_topo_ix.
%% @end
%%--------------------------------------------------------------------
-spec fetch_topo_history() ->
    {ok, History :: [kvp()]} | {error, Reason :: term()}.
fetch_topo_history() ->
    {ok, {Key, _}, _} = enterdb:first("gb_dyno_topo_ix"),
    fetch_topo_history(Key, []).

-spec fetch_topo_history(Cont :: term(), Acc :: [term()]) ->
     {ok, History :: [kvp()]} | {error, Reason :: term()}.
fetch_topo_history(Cont, Acc) ->
    case enterdb:read_range("gb_dyno_topo_ix", {Cont, [{"ix", 0}]}, 1000) of
	{ok, List, complete} ->
	    {ok, lists:append(Acc, List)};
	{ok, List, NewCont} ->
	    fetch_topo_history(NewCont, lists:append(Acc,List));
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Fetch latest topology metadata from given node and merge with local
%% topology metadata. Merge suceeds only if two metadata are for same 
%% cluster.
%% @end
%%--------------------------------------------------------------------
-spec pull_topo(Node :: node()) ->
    {ok, Merged :: proplist()} |
    {error, Reason :: string() | {conflict, Node :: node()}}.
pull_topo(Node) ->
    case rpc:call(Node, ?MODULE, fetch_topo_history, []) of
	{ok, History} ->
	    {ok, Base, Local, Remote} = find_versions(Node, History),
	    merge_topo(Base, Local, Remote);
	{error, Reason} ->
	    {error, Reason}
    end.

-spec find_versions(Node :: node(), History :: [kvp()])->
    {ok, Base :: proplist(), Local :: proplist(), Remote :: proplist()}.
find_versions(Node, History = [{_, [{_, RemoteHash}]} | _]) ->
    HashMap = maps:from_list([{H, true} || {_, [{_, H}]} <- History]),
    {ok, Remote} = rpc:call(Node, ?MODULE, lookup_topo, [RemoteHash]),

    {ok, LocalHistory = [{_, [{_, LocalHash}]} | _]} = fetch_topo_history(),
    {ok, Local} = lookup_topo(LocalHash),

    {ok, Base} = find_common_ancestor(LocalHistory, HashMap),

    {ok, Base, Local, Remote}.

-spec find_common_ancestor(History :: [kvp()], Map :: map()) ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
find_common_ancestor([{_, [{_, Hash}]} | Rest], Map) ->
    case maps:is_key(Hash, Map) of
	true ->
	    lookup_topo(Hash);
	false ->
	    find_common_ancestor(Rest, Map)
    end;
find_common_ancestor([], _Map) ->
    {ok, []}.

-spec merge_topo(Base :: proplist(),
		 Local :: proplist(),
		 Remote :: proplist()) ->
    {ok, Merged :: proplist()} | {error, Reason :: term()}.
merge_topo(Base, Local, Remote) ->
    Cluster = proplists:get_value(cluster, Local),
    case proplists:get_value(cluster, Remote) of
	Cluster ->
	    BaseNodes = proplists:get_value(nodes, Base, []),
	    LocalNodes = proplists:get_value(nodes, Local),
	    RemoteNodes = proplists:get_value(nodes, Remote),
	    case merge_nodes(BaseNodes, LocalNodes, RemoteNodes) of
		{ok, MergedNodes} ->
		    {ok, [{cluster, Cluster}, {nodes, MergedNodes}]};
		{error, Reason} ->
		    {error, Reason}
	    end;
	_ ->
	    {error, "different_clusters"}
    end.

-spec merge_nodes(Base :: proplist(),
		  Local :: proplist(),
		  Remote :: proplist()) ->
    {ok, Merged :: proplist()} | {error, Reason :: term()}.
merge_nodes(Base, Local, Remote) ->
    RMap = maps:from_list(Remote),
    {Same, LDiff, RDiff} = nodes_diff(Local, RMap),
    BMap = maps:from_list(Base),
    {Updates, Conflicts} = nodes_base_comp(LDiff, RDiff, BMap),
    case Conflicts of
	[] ->
	    Merged = Same ++ Updates,
	    {ok, lists:sort(fun compare_nodes/2, Merged)};
	_ ->
	    {error, {conflict, Conflicts}}
    end.

-spec nodes_diff(Nodes :: proplist(), Map :: map()) ->
    {Same :: proplist(), LDiff :: proplist(), RDiff :: proplist()}.
nodes_diff(Nodes, Map) ->
    nodes_diff(Nodes, Map, [], [], []).

-spec nodes_diff(Nodes :: proplist(), Map :: map(), Same :: proplist(),
		 LDiff :: proplist(), RDiff :: proplist()) ->
    {Same :: proplist(), LDiff :: proplist(), RDiff :: proplist()}.
nodes_diff([{Node, Data} | Rest], Map, Same, LDiff, RDiff) ->
    NewMap = maps:remove(Node, Map),
    case maps:get(Node, Map, Data) of
        Data ->
	    nodes_diff(Rest, NewMap, [{Node, Data} | Same], LDiff, RDiff);
        RData ->
	    nodes_diff(Rest, NewMap, Same,
		       [{Node, Data} | LDiff], [{Node, RData} |RDiff])
    end;
nodes_diff([], Map, Same, LDiff, RDiff) ->
    RemoteExclusive = maps:to_list(Map),
    {Same ++ RemoteExclusive, LDiff, RDiff}.

-spec nodes_base_comp(LDiff :: proplist(),
		      RDiff :: proplist(),
		      BMap :: map()) ->
    {Updates :: proplist(), Conflicts :: [{node(), term()}]}.
nodes_base_comp(LDiff, RDiff, BMap) ->
    nodes_base_comp(LDiff, RDiff, BMap, [], []).

-spec nodes_base_comp(LDiff :: proplist(),
		      RDiff :: proplist(),
		      BMap :: map(),
		      Updates :: proplist(),
		      Conflicts :: [{proplist(), proplist()}]) ->
    {Updates :: proplist(), Conflicts :: [{node(), term()}]}.
nodes_base_comp([{Node, LData} | LDiff], [{Node, RData} | RDiff], BMap,
		Updates, Conflicts) ->
    case maps:get(Node, BMap, undefined) of
        LData ->
	    nodes_base_comp(LDiff, RDiff, BMap,
			    [{Node, RData} | Updates], Conflicts);
        RData ->
	    nodes_base_comp(LDiff, RDiff, BMap,
			    [{Node, LData} | Updates], Conflicts);
	_ ->
	    case resolve_conflict(LData, RData) of
		{ok, Data} ->
		    nodes_base_comp(LDiff, RDiff, BMap,
				    [{Node, Data} | Updates], Conflicts);
		{nok, Conflict} ->
		    nodes_base_comp(LDiff, RDiff, BMap, Updates,
				    [{Node, Conflict} | Conflicts])
	    end
    end;
nodes_base_comp([], [], _BMap, Updates, Conflicts) ->
    {Updates, Conflicts}.

-spec resolve_conflict(LData :: proplist(), RData :: proplist()) ->
    {ok, Data:: proplist()} |
    {nok, {LData :: proplist(), RData :: proplist()}}.
resolve_conflict(LData, RData) ->
    LVersion = proplists:get_value(version, LData),
    RVersion = proplists:get_value(version, RData),
    if
	LVersion == RVersion ->
	    {nok, {LData, RData}};
	LVersion > RVersion ->
	    {ok, LData};
	true ->
	    {ok, RData}
    end.	

%%--------------------------------------------------------------------
%% @doc
%% Ordering function used as an argument to sort function.
%% Orders first on dc, second on rack, and last on node name. 
%% @end
%%--------------------------------------------------------------------
-spec compare_nodes({NodeA :: node(), DataA :: proplist()},
		    {NodeB :: node(), DataB :: proplist()}) ->
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
%% Remove local node from topology (metadata).
%% @end
%%--------------------------------------------------------------------
-spec remove_node() ->
    ok | {error, Reason :: term()}.
remove_node() ->
    node_update({removed, true}).

%%--------------------------------------------------------------------
%% @doc
%% Update a property of node data that is kept in topology (metadata).
%% @end
%%--------------------------------------------------------------------
-spec node_update({Prop :: atom(), Value :: term()}) ->
    ok | {error, Reason :: term()}.
node_update({Prop, Value}) ->
    {ok, Metadata} = lookup_topo(),
    Cluster = proplists:get_value(cluster, Metadata),
    Nodes = proplists:get_value(nodes, Metadata),
    N = node(),
    Data = proplists:get_value(N, Nodes),
    
    Version = proplists:get_value(version, Data),
    D1 = proplists:delete(Prop, Data),
    D2 = proplists:delete(version, D1),
    D3 = ordsets:add_element({version, Version + 1}, D2),
    D4 = ordsets:add_element({Prop, Value}, D3),
    
    NewNodes = [{N, D4} | proplists:delete(N, Nodes)],
    SortedNodes = lists:sort(fun compare_nodes/2, NewNodes), 
    NewMetadata = [{cluster, Cluster}, {nodes, SortedNodes}],
    commit_topo(NewMetadata).
