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

-behaviour(gen_server).

%% API functions
-export([start_link/0,
	 get_current_hash/0,
	 commit_topo/1,
	 lookup_topo/0,
	 lookup_topo/1,
	 fetch_topo_history/0,
	 pull_topo/1,
	 remove_node/0,
	 node_add_prop/1,
	 node_add_prop/2,
	 node_rem_prop/1,
	 node_rem_prop/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

%%Exports for testing purpose
-export([merge_topo/3]).

-type proplist() :: [{atom(), term()}].
-type key() :: [{string(), term()}].
-type value() :: [{string(), term()}].
-type kvp() :: {key(), value()}.

-include_lib("gb_log/include/gb_log.hrl").
-define(ALG, sha).

%%--------------------------------------------------------------------
%% @doc
%% Start this server.
%% @end
%%--------------------------------------------------------------------
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

%%--------------------------------------------------------------------
%% @doc
%% Get the current hash value of topology metadata.
%% @end
%%--------------------------------------------------------------------
-spec get_current_hash() ->
    Hash :: string().
get_current_hash() ->
    gen_server:call(?MODULE, get_current_hash).

%%--------------------------------------------------------------------
%% @doc
%% Commit new topology metadata.
%% @end
%%--------------------------------------------------------------------
-spec commit_topo(Metadata :: term()) ->
    {ok, Hash :: string()}.
commit_topo(Metadata) ->
    gen_server:call(?MODULE, {commit_topo, Metadata}).

%%--------------------------------------------------------------------
%% @doc
%% Lookup latest topology metadata.
%% @end
%%--------------------------------------------------------------------
-spec lookup_topo() ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
lookup_topo() ->
    try
	gen_server:call(?MODULE, lookup_topo)
    catch exit:{timeout,_} = R:ST ->
	?error("could not lookup topo ~p", [{R,ST}]),
	{error, timeout}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Lookup for a topology metadata that is identified by it's hash
%% value.
%% @end
%%--------------------------------------------------------------------
-spec lookup_topo(Hash :: string()) ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
lookup_topo(Hash) ->
    case enterdb:index_read("gb_dyno_metadata", "hash", Hash) of
	{ok, [#{key := Key}]} ->
	    {ok, Value} = enterdb:read("gb_dyno_metadata", Key),
	    {_, Metadata} = lists:keyfind("metadata", 1, Value),
	    {ok, Metadata};
	{ok, []} ->
	    {error, not_found}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Remove local node from topology (metadata).
%% @end
%%--------------------------------------------------------------------
-spec remove_node() ->
    ok | {error, Reason :: term()}.
remove_node() ->
    node_add_prop({removed, true}).

%%--------------------------------------------------------------------
%% @doc
%% Add a property of node data that is kept in topology (metadata).
%% @end
%%--------------------------------------------------------------------
node_add_prop({P, V}) ->
    node_add_prop(node(), {P, V}).
node_add_prop(Node, {P, V}) ->
    node_update('add', Node, {P, V}).

%%--------------------------------------------------------------------
%% @doc
%% Remove a property of node data that is kept in topology (metadata).
%% @end
%%--------------------------------------------------------------------
node_rem_prop(P) ->
    node_rem_prop(node(), P).
node_rem_prop(Node, P) ->
    node_update('rem', Node, {P, dummy}).

%%--------------------------------------------------------------------
%% @doc
%% Update a property of node data that is kept in topology (metadata).
%% @end
%%--------------------------------------------------------------------
-spec node_update(Op :: 'add '| 'rem',
		  Node :: atom(),
		  {Prop :: atom(), Value :: term()}) ->
    ok | {error, Reason :: term()}.
node_update(Op, Node, {Prop, Value}) ->
    gen_server:call(?MODULE, {node_update, Op, Node, Prop, Value}).

%%--------------------------------------------------------------------
%% @doc
%% Fetch latest topology metadata from given node and merge with local
%% topology metadata. Merge suceeds only if two metadata are for same
%% cluster. Commit topology if merge is succesful.
%% @end
%%--------------------------------------------------------------------
-spec pull_topo(Node :: node()) ->
    {ok, Hash :: string(), Merged :: proplist()} |
    {error, Reason :: string() | {conflict, Node :: node()}}.
pull_topo(Node) ->
    try
	gen_server:call(?MODULE, {pull_topo, Node})
    catch C:E:ST ->
	?info("couldn't get topo ~p:~p ~p", [C,E,ST]),
	{error, E}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Initialize metadata tables at startup.
%% @end
%%--------------------------------------------------------------------
-spec init(Args :: []) ->
    {ok, State :: map()}.
init([]) ->
    ?info("Starting ~p server..", [?MODULE]),
    State =
	case get_first(5) of
	    {ok, {Key, Value}, _} ->
		{_, Id} = lists:keyfind("id", 1, Key),
		{_, Hash} = lists:keyfind("hash", 1, Value),
		#{current_id => Id, current_hash => Hash};
	    {error,"no_table"} ->
		create_metadata();
	    {error, "table_closed"} ->
		open_tables()
	end,
    {ok, State}.

-spec handle_call(Request :: term(),
		  From :: {Pid :: pid(), Tag :: term()},
		  State :: map()) ->
    {reply, Reply :: term(), State :: map()}.
handle_call(get_current_hash, _From, State= #{current_hash := Hash}) ->
    {reply, Hash, State};
handle_call({commit_topo, Metadata}, _From, State = #{current_id := Id,
						      current_hash := Hash}) ->
    case erlang:integer_to_list(gb_hash:hash(?ALG, Metadata)) of
	Hash ->
	    {reply, {ok, Hash}, State};
	NewHash ->
	    NewId = Id + 1,
	    ok = do_commit_topo(NewId, Metadata, NewHash),
	    NextState = #{current_id => NewId,
			  current_hash => NewHash},
	    {reply, {ok, NewHash}, NextState}
    end;
handle_call(lookup_topo, _From, State = #{current_hash := Hash}) ->
    {reply, lookup_topo(Hash), State};
handle_call({node_update, Op, Node, Prop, Value}, _From,
	    _S = #{current_id := Id, current_hash := Hash}) ->
    {ok, Metadata} = lookup_topo(Hash),
    Cluster = proplists:get_value(cluster, Metadata),
    Nodes = proplists:get_value(nodes, Metadata),
    Data = proplists:get_value(Node, Nodes),

    Version = proplists:get_value(version, Data),
    D1 = proplists:delete(Prop, Data),
    D2 = proplists:delete(version, D1),
    D3 = ordsets:add_element({version, Version + 1}, D2),
    D4 =
	if Op =:= add ->
	    ordsets:add_element({Prop, Value}, D3);
	true -> %% 'rem' op; just do not add
	    D3
    end,
    NewNodes = [{Node, D4} | proplists:delete(Node, Nodes)],
    SortedNodes = lists:sort(fun compare_nodes/2, NewNodes),
    NewMetadata = [{cluster, Cluster}, {nodes, SortedNodes}],
    NewHash = erlang:integer_to_list(gb_hash:hash(?ALG, NewMetadata)),
    NewId = Id+1,
    ok = do_commit_topo(NewId, NewMetadata, NewHash),
    NextState = #{current_id => NewId,
		  current_hash => NewHash},
    {reply, {ok, NewHash}, NextState};
handle_call({pull_topo, Node}, _From,
	    State = #{current_id := Id,
		      current_hash := Hash}) when Node == node() ->
    {ok, Value} = enterdb:read("gb_dyno_metadata", [{"id", Id}]),
    {_, Metadata} = lists:keyfind("metadata", 1, Value),
    {reply, {ok, Hash, Metadata}, State};
handle_call({pull_topo, Node}, _From, State = #{current_id := Id,
						current_hash := Hash}) ->
    case do_pull_topo(Node) of
	{ok, Merged} ->
	    case erlang:integer_to_list(gb_hash:hash(?ALG, Merged)) of
		Hash ->
		    {reply, {ok, Hash, Merged}, State};
		NewHash ->
		    NewId = Id + 1,
		    ok = do_commit_topo(NewId, Merged, NewHash),
		    NextState = #{current_id => NewId,
				  current_hash => NewHash},
		    {reply, {ok, NewHash, Merged}, NextState}
	    end;
	Else ->
	    {reply, Else, State}
    end;
handle_call(_Request, _From, State) ->
    ?warning("Unhandled gen_server call, reuest: ~p", [_Request]),
    {reply, ok, State}.

-spec handle_cast(Msg :: term(), State :: map()) ->
    {noreply, State :: map()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(Info :: term(), State :: map()) ->
    {noreply, State :: map()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(Reason :: term(), State :: map())->
    ok.
terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

%%--------------------------------------------------------------------
%% @doc
%% Create tables for metadata.
%% @end
%%--------------------------------------------------------------------
-spec create_metadata() ->
    {ok, State :: map()}.
create_metadata() ->
    Cluster = gb_dyno:conf(cluster),
    DC = gb_dyno:conf(dc),
    Rack = gb_dyno:conf(rack),

    Data = ordsets:from_list([{version, 1}, {dc, DC}, {rack, Rack}]),
    NodeData = [{node(), Data}],
    Metadata = [{cluster, Cluster}, {nodes, NodeData}],

    {error,"no_table"} = enterdb:table_info("gb_dyno_metadata", [name]),
    TabOpts = [{data_model, map},
	       {comparator, descending},
	       {time_series, false},
	       {num_of_shards, 1},
	       {system_table, true},
	       {distributed, false}],

    ok = enterdb:create_table("gb_dyno_metadata", ["id"], TabOpts),
    ok = enterdb:add_index("gb_dyno_metadata", ["hash"]),
    Id = 1,
    Hash = erlang:integer_to_list(gb_hash:hash(?ALG, Metadata)),
    ok = do_commit_topo(Id, Metadata, Hash),
    #{current_id => Id, current_hash => Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Open already existing metadata tables.
%% @end
%%--------------------------------------------------------------------
-spec open_tables() ->
    {ok , State :: map()}.
open_tables() ->
    ok = enterdb:open_table("gb_dyno_metadata"),
    ?info("gb_dyno_metadata table opened ok"),
    {ok, {Key, Value}, _} = enterdb:first("gb_dyno_metadata"),
    ?info("first from gb_dyno_metadata ~p", [{Key, Value}]),
    {_, Id} = lists:keyfind("id", 1, Key),
    {_, Hash} = lists:keyfind("hash", 1, Value),
    #{current_id => Id, current_hash => Hash}.

%%--------------------------------------------------------------------
%% @doc
%% Commit new metadata with topo(topology) tag and given index.
%% The committed data represents the dyno topology from a local
%% point of view.
%% @end
%%--------------------------------------------------------------------
-spec do_commit_topo(Id :: pos_integer(),
		     Metadata :: proplist(),
		     Hash :: string()) ->
    ok | {error, Reason :: term()}.
do_commit_topo(Id, Metadata, Hash) ->
    %% only keep limited number of ids in history
    enterdb:delete("gb_dyno_metadata", [{"id", Id-100}]),
    enterdb:write("gb_dyno_metadata",
		  [{"id", Id}],
		  [{"hash", Hash}, {"metadata", Metadata}]).

%%--------------------------------------------------------------------
%% @doc
%% Fetch latest topology metadata from given node and merge with local
%% topology metadata. Merge suceeds only if two metadata are for same
%% cluster.
%% @end
%%--------------------------------------------------------------------
-spec do_pull_topo(Node :: atom()) ->
    {ok, Merged :: term()} |
    {error, Reason :: string()}.
do_pull_topo(Node) ->
    case rpc:call(Node, ?MODULE, fetch_topo_history, [], 2000) of
	{ok, RemoteHistory} ->
	    {ok, Base, Local, Remote} = find_versions(RemoteHistory),
	    merge_topo(Base, Local, Remote);
	{badrpc, _Reason} = Error ->
	    {error, Error};
	{error, Reason} ->
	    {error, Reason}
    end.

%%--------------------------------------------------------------------
%% @doc
%% Get all data stored in gb_dyno_topo_ix.
%% @end
%%--------------------------------------------------------------------
-spec fetch_topo_history() ->
    {ok, History :: [kvp()]} | {error, Reason :: term()}.
fetch_topo_history() ->
    {ok, {Key, _}, _} = enterdb:first("gb_dyno_metadata"),
    fetch_topo_history(Key, []).

-spec fetch_topo_history(Cont :: term(), Acc :: [term()]) ->
     {ok, History :: [kvp()]} | {error, Reason :: term()}.
fetch_topo_history(Cont, Acc) ->
    case enterdb:read_range("gb_dyno_metadata", {Cont, [{"id", 0}]}, 1000) of
	{ok, List, complete} ->
	    {ok, lists:append(Acc, List)};
	{ok, List, NewCont} ->
	    fetch_topo_history(NewCont, lists:append(Acc, List));
	{error, Reason} ->
	    {error, Reason}
    end.

-spec find_versions(RemoteHistory :: [kvp()])->
    {ok, Base :: proplist(), Local :: proplist(), Remote :: proplist()}.
find_versions(RemoteHistory = [{_, RemoteValue} | _]) ->
    RemoteHashes = [begin
			{_, H} = lists:keyfind("hash", 1, V),
			H
		    end || {_, V} <- RemoteHistory],
    {ok, LocalHistory = [{_, LocalValue} | _]} = fetch_topo_history(),
    {ok, Base} = find_common_ancestor(LocalHistory, RemoteHashes),
    {_, Local} = lists:keyfind("metadata", 1, LocalValue),
    {_, Remote} = lists:keyfind("metadata", 1, RemoteValue),
    {ok, Base, Local, Remote}.

-spec find_common_ancestor(History :: [kvp()],
			   RemoteHashes :: [string()]) ->
    {ok, Metadata :: proplist()} | {error, Reason :: term()}.
find_common_ancestor([{_, Value} | Rest], RemoteHashes) ->
    {_, Hash} = lists:keyfind("hash", 1, Value),
    case lists:member(Hash, RemoteHashes) of
	true ->
	    {_, Metadata} = lists:keyfind("metadata", 1, Value),
	    {ok, Metadata};
	false ->
	    find_common_ancestor(Rest, RemoteHashes)
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

get_first(Tries) when Tries > 0 ->
    try
	enterdb:first("gb_dyno_metadata")
    catch exit:{timeout, _} = R:ST ->
	?error("couldn't read first data from gb_dyno_metadata ~p", [{R,ST}]),
	get_first(Tries-1)
    end;
get_first(_Tries) ->
    {error, cannot_read_table}.
