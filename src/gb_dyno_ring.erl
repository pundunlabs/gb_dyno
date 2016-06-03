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
%% Implements gb gossip protocol in between nodes that run gb_dyno
%% application.
%% @end
%%%===================================================================

-module(gb_dyno_ring).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
	 allocate_nodes/2]).

%% Test exports
-export([construct_network_map/1,
	 do_allocate_nodes/3,
	 test01/2,
	 count/2]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-record(state, {hash,
		cluster,
		dc,
		rack}).

-include("gb_log.hrl").
-define(TIMEOUT, 5000).

-type proplist() :: [{atom(), term()}].

%%%===================================================================
%%% API functions
%%%===================================================================
count(ShardNum, RF) ->
    Nodes = lists:flatten([begin
			    maps:fold(fun(_,V,Acc) -> [V|Acc] end, [], M)
			   end || {_, M} <- test01(ShardNum, RF)]),
    do_count(Nodes, #{}, 0, ShardNum*RF).

do_count([Node|Rest], Map, Count, TotalCount) ->
    Int = maps:get(Node, Map, 0),
    do_count(Rest, maps:put(Node, Int+1, Map), Count+1, TotalCount);
do_count([], Map, Count, TotalCount) ->
    {Count, TotalCount, Map}.
    
test01(ShardNum, RF)->
    M2 =
	[{cluster,"cl01"},
	 {nodes,[{pundun111@sitting,[{dc,"dc01"},{rack,"rack01"},{version,1}]},
		 {pundun112@sitting,[{dc,"dc01"},{rack,"rack01"},{version,1}]},
		 {pundun121@sitting,[{dc,"dc01"},{rack,"rack02"},{version,1}]},
		 {pundun122@sitting,[{dc,"dc01"},{rack,"rack02"},{version,1}]},
		 {pundun211@sitting,[{dc,"dc02"},{rack,"rack01"},{version,1}]},
		 {pundun212@sitting,[{dc,"dc02"},{rack,"rack01"},{version,1}]},
		 {pundun221@sitting,[{dc,"dc02"},{rack,"rack02"},{version,1}]},
		 {pundun222@sitting,[{dc,"dc02"},{rack,"rack02"},{version,1}]},
		 {pundun311@sitting,[{dc,"dc03"},{rack,"rack01"},{version,1}]},
		 {pundun312@sitting,[{dc,"dc03"},{rack,"rack01"},{version,1}]},
		 {pundun321@sitting,[{dc,"dc03"},{rack,"rack02"},{version,1}]},
		 {pundun322@sitting,[{dc,"dc03"},{rack,"rack02"},{version,1}]},
		 {pundun411@sitting,[{dc,"dc04"},{rack,"rack01"},{version,1}]},
		 {pundun412@sitting,[{dc,"dc04"},{rack,"rack01"},{version,1}]},
		 {pundun421@sitting,[{dc,"dc04"},{rack,"rack02"},{version,1}]},
		 {pundun422@sitting,[{dc,"dc04"},{rack,"rack02"},{version,1}]}
		]}],
    Map = gb_dyno_ring:construct_network_map(M2),
    Shards = [lists:concat(["S",X]) || X <- lists:seq(1,ShardNum)],
    do_allocate_nodes(Shards, RF, Map).

%%--------------------------------------------------------------------
%% @doc
%% Allocate shards to nodes according to replication factor and
%% network topology. Create a ring using gb_hash application and return
%% the list of allocated shards.
%% Current implementation works without configuration and replicates
%% for highest redundancy possible.
%% @end
%%--------------------------------------------------------------------
-spec allocate_nodes(Shards :: [string()],
		     ReplicationFactor :: pos_integer()) ->
    {ok, AllocatedShards :: [{Shard :: string(), Ring :: map()}]}.
allocate_nodes(Shards, ReplicationFactor) ->
    {ok, Metadata} = gb_dyno_metadata:lookup_topo(),
    NetworkMap = construct_network_map(Metadata),
    AllocatedShards = do_allocate_nodes(Shards, ReplicationFactor, NetworkMap),
    {ok, AllocatedShards}. 

%%--------------------------------------------------------------------
%% @doc
%% Given the topology metadata, a network map is populated in a list.
%% Resulting list consists of rack mapping per data center, and rack
%% mapings are lists of nodes per rack.
%% @end
%%--------------------------------------------------------------------
-spec construct_network_map(Metadata :: proplist()) ->
    NetworkMap :: proplist().
construct_network_map(Metadata) ->
    Nodes = proplists:get_value(nodes, Metadata),
    construct_network_map(Nodes, []).

-spec construct_network_map(Nodes :: proplist(), Map :: proplist()) ->
    NetworkMap :: proplist().
construct_network_map([{Node, NodeData} | Rest], Map) ->
    DC = proplists:get_value(dc, NodeData),
    Rack = proplists:get_value(rack, NodeData),
    NewMap = network_map_insert(Node, DC, Rack, Map),
    construct_network_map(Rest, NewMap);
construct_network_map([], Map) ->
    Map.

-spec network_map_insert(Node :: node(),
			 DC :: string(),
			 Rack :: string(),
			 Map :: proplist()) ->
    NewMap :: proplist().
network_map_insert(Node, DC, Rack, Map) ->
    case proplists:get_value(DC, Map, undefined) of
	undefined ->
	    RackMap = [{Rack, [Node]}],
	    [{DC, RackMap} | Map];
	RackMap ->
	    NewRackMap = 
		case proplists:get_value(Rack, RackMap, []) of
		    [] ->
			[{Rack, [Node]} | RackMap];
		    RackNodes ->
			lists:keyreplace(Rack, 1, RackMap,
					 {Rack, [Node | RackNodes]})
		end,
	    lists:keyreplace(DC, 1, Map, {DC, NewRackMap})
    end.

%%--------------------------------------------------------------------
%% @doc
%% Allocate nodes to every given shard. ReplicationFactor specified
%% the number of nodes to be allocated per shard. 
%% @end
%%--------------------------------------------------------------------
-spec do_allocate_nodes(Shards :: [string()],
			ReplicationFactor :: pos_integer(),
			NetworkMap :: proplist()) ->
    [{Shard :: string(), Ring :: map()}].
do_allocate_nodes(Shards, RF, NetworkMap) ->
    do_allocate_nodes(Shards, RF, NetworkMap, 0, []).

-spec do_allocate_nodes(Shards :: [string()],
			ReplicationFactor :: pos_integer(),
			NetworkMap :: proplist(),
			N :: pos_integer(),
			Acc :: [{Shard :: string(), [node()]}]) ->
    [{Shard :: string(), Ring :: map()}].
do_allocate_nodes([Shard | Rest], RF, NetworkMap, N, Acc) ->
    Nodes = choose_nodes(RF, NetworkMap, N),
    do_allocate_nodes(Rest, RF, NetworkMap, N + 1, [{Shard, Nodes}|Acc]);
do_allocate_nodes([], _RF, _NetworkMap, _N, Acc) ->
    lists:reverse(Acc).

%%--------------------------------------------------------------------
%% @doc
%% Choose the nodes to be allocated to a shard. Nodes are picked in
%% below described preference order:
%% 1- Pick next node from a different data center.
%% 2- Pick next data from different rack if more than one node should
%%    be picked from same data center.
%% 3- Pick from same rack in same data center if replication factor is
%%    not reached.
%% Result is returned in a map() term where each data center is a key
%% and the nodes allocated to the shard are a list as value.
%% ex:
%%  #{"dc01" => [pundun122@sitting,pundun112@sitting],
%%    "dc02" => [pundun222@sitting],
%%    "dc03" => [pundun322@sitting],
%%    "dc04" => [pundun422@sitting,pundun412@sitting]}}
%% @end
%%--------------------------------------------------------------------
%%-spec choose_nodes(RF :: pos_integer()
%%		   NetworkMap :: proplist(),
%%		   Shift :: pos_integer()) ->
%%    [node()].
%%choose_nodes(RF, NetworkMap, Shift) ->
%%    NumOfDCs = length(NetworkMap),
%%    RackShift = (Shift div NumOfDCs) + (Shift rem NumOfDCs),
%%    Map = shift_list(NetworkMap, Shift),
%%    Div = RF div NumOfDCs,
%%    Rem = RF rem NumOfDCs,
%%
%%    Fun = fun({_DC, RackMap}, {R, L, Acc1, Acc2})->
%%		N = get_n(Div, R, L),
%%		{Ns, UnusedNodes} = get_next_n_node(RackMap, RackShift, N),
%%		NL = N - length(Ns),  
%%		{R - 1, NL, [Ns | Acc1], [UnusedNodes | Acc2]}
%%	  end,
%%    
%%    {_, Remain, Nodes, UnusedNodes} = lists:foldl(Fun, {Rem, 0, [], []}, Map),
%%    {Fill, _} = split_by_len(alternate_flatten(UnusedNodes), Remain),
%%    lists:flatten([Fill|Nodes]).
%%
-spec choose_nodes(RF :: pos_integer(),
		   NetworkMap :: proplist(),
		   Shift :: pos_integer()) ->
    map().
choose_nodes(RF, NetworkMap, Shift) ->
    NumOfDCs = length(NetworkMap),
    RackShift = (Shift div NumOfDCs) + (Shift rem NumOfDCs),
    Map = shift_list(NetworkMap, Shift),
    Div = RF div NumOfDCs,
    Rem = RF rem NumOfDCs,

    Fun = fun({DC, RackMap}, {R, L, RingAcc, Acc2})->
		N = get_n(Div, R, L),
		{Ns, UnusedNodes} = get_next_n_node(RackMap, RackShift, N),
		NL = N - length(Ns),  
		{R - 1, NL, map_update(DC, Ns, RingAcc), [{DC, UnusedNodes} | Acc2]}
	  end,
    
    {_, Remain, Ring, UnusedNodes} = lists:foldl(Fun, {Rem, 0, #{}, []}, Map),
    distribute_remaining(Remain, Ring, UnusedNodes).

%%--------------------------------------------------------------------
%% @doc
%% Helper function to specify the number of nodes to be picked from a
%% data center.
%% @end
%%--------------------------------------------------------------------
-spec get_n(Div :: integer(),
	    Rem :: integer(),
	    L :: integer()) ->
    N :: integer().
get_n(Div, Rem, L) when Rem > 0 ->
    Div + 1 + L;
get_n(Div, _, L) ->
    Div + L.

%%--------------------------------------------------------------------
%% @doc
%% Helper function to pick N nodes from a data center. Nodes are
%% alernatively picked from racks and the start point to pick is
%% specified with given Shift value.
%% @end
%%--------------------------------------------------------------------
-spec get_next_n_node(RackMap :: proplist(),
		      Shift :: pos_integer(),
		      N :: integer()) ->
    Acc :: [node()].
get_next_n_node(RackMap, Shift, N) ->
    DeepList = [Ns || {_, Ns} <- RackMap],
    List = alternate_flatten(DeepList),
    ShiftedList = shift_list(List, Shift),
    %%lists:sublist(ShiftedList, N).
    split_by_len(ShiftedList, N).

%%--------------------------------------------------------------------
%% @doc
%% Flatten a list of lists (depth level 2) and populate the resulting
%% list by picking terms from a different list each time.
%% alternate_flatten([[a,b,c],[1,2,3]) -> [a,1,b,2,c,3].
%% @end
%%--------------------------------------------------------------------
-spec alternate_flatten(DeepList :: [[node()]]) ->
    [node()].
alternate_flatten(DeepList) ->
    alternate_flatten(DeepList, []).

-spec alternate_flatten(DeepList :: [[node()]],
			Acc :: [node()]) ->
    [node()].
alternate_flatten([[] | DL], Acc) ->
    alternate_flatten(DL, Acc);
alternate_flatten([[N] | DL], Acc) ->
    alternate_flatten(DL, [N | Acc]);
alternate_flatten([[N | L] | DL], Acc) ->
    alternate_flatten(lists:append(DL, [L]), [N | Acc]);
alternate_flatten([], Acc) ->
    lists:reverse(Acc).

%%--------------------------------------------------------------------
%% @doc
%% Helper function to Shift many times remove the term at head of list
%% and append it back to list.
%% shift_list([1,2,3,4,5], 3) -> [4,5,1,2,3].
%% @end
%%--------------------------------------------------------------------
-spec shift_list(List :: [term()], Shift :: pos_integer()) ->
    ShiftedList :: [term()].
shift_list(List, 0) ->
    List;
shift_list([H | T], Shift) ->
    shift_list(lists:append(T,[H]), Shift - 1).

%%--------------------------------------------------------------------
%% @doc
%% Helper function to split the list after Len many items and return
%% two lists.
%% split_by_len([1,2,3,4,5], 3) -> {[1,2,3],[4,5]}.
%% @end
%%--------------------------------------------------------------------
-spec split_by_len(List :: [term()], Len :: pos_integer()) ->
    {SubList :: [term()], Rest :: [term()]}.
split_by_len(List, Len) ->
    split_by_len(List, Len, []).

-spec split_by_len(List :: [term()], Len :: pos_integer(), Acc :: [term()]) ->
    {SubList :: [term()], Rest :: [term()]}.
split_by_len([H | Rest], Len, Acc) when Len > 0 ->
    split_by_len(Rest, Len-1, [H | Acc]);
split_by_len(Rest, _Len, SubList) ->
    {lists:reverse(SubList), Rest}.

%%--------------------------------------------------------------------
%% @doc
%% Distribute unused nodes to meet replication factor.
%% @end
%%--------------------------------------------------------------------
-spec distribute_remaining(Remain :: integer(),
			   Ring :: map(),
			   UnusedNodes :: {DC :: string(), [node()]}) ->
    NewRing :: map().
distribute_remaining(Remain, Ring, _) when Remain < 1 ->
    Ring;
distribute_remaining(Remain,
		     Ring,
		     [{_DC, []} | Rest]) ->
    distribute_remaining(Remain, Ring, Rest);
distribute_remaining(Remain,
		     Ring,
		     [{DC, [Node | Nodes]} | Rest]) ->
    NewRing = map_update(DC, [Node], Ring),
    NewUnusedNodes = Rest ++ [{DC, Nodes}],
    distribute_remaining(Remain - 1, NewRing, NewUnusedNodes);
distribute_remaining(_Remain, Ring, []) ->
    Ring.

%%--------------------------------------------------------------------
%% @doc
%% Updates a map() term for given Key where Value list added to head
%% of existing value list in the map.
%% @end
%%--------------------------------------------------------------------
-spec map_update(Key :: term(), Value :: [term()], Map :: map()) ->
    NewMap :: map().
map_update(_Key, [], Map) ->
    Map;
map_update(Key, Value, Map) ->
    OldValue =  maps:get(Key, Map, []),
    maps:put(Key, Value ++ OldValue, Map).


%%--------------------------------------------------------------------
%% @doc
%% Starts the server
%%
%% @spec start_link() -> {ok, Pid} | ignore | {error, Error}
%% @end
%%--------------------------------------------------------------------
start_link(Options) ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, Options, []).

%%%===================================================================
%%% gen_server callbacks
%%%===================================================================

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Initializes the server
%%
%% @spec init(Args) -> {ok, State} |
%%                     {ok, State, Timeout} |
%%                     ignore |
%%                     {stop, Reason}
%% @end
%%--------------------------------------------------------------------
init(Options) ->
    Hash = proplists:get_value(hash, Options),
    Cluster = proplists:get_value(cluster, Options), 
    DC = proplists:get_value(dc, Options), 
    Rack = proplists:get_value(rack, Options),
    {ok, #state{hash = Hash,
		cluster = Cluster,
		dc = DC,
		rack = Rack}}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling call messages
%%
%% @spec handle_call(Request, From, State) ->
%%                                   {reply, Reply, State} |
%%                                   {reply, Reply, State, Timeout} |
%%                                   {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, Reply, State} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_call(Request, _From, State) ->
    Reply = ?warning("Unhandled call request: ~p", [Request]),
    {reply, Reply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling cast messages
%%
%% @spec handle_cast(Msg, State) -> {noreply, State} |
%%                                  {noreply, State, Timeout} |
%%                                  {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_cast(Msg, State) ->
    ?warning("Unhandled cast message: ~p", [Msg]),
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Handling all non call/cast messages
%%
%% @spec handle_info(Info, State) -> {noreply, State} |
%%                                   {noreply, State, Timeout} |
%%                                   {stop, Reason, State}
%% @end
%%--------------------------------------------------------------------
handle_info(_Info, State) ->
    {noreply, State}.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% This function is called by a gen_server when it is about to
%% terminate. It should be the opposite of Module:init/1 and do any
%% necessary cleaning up. When it returns, the gen_server terminates
%% with Reason. The return value is ignored.
%%
%% @spec terminate(Reason, State) -> void()
%% @end
%%--------------------------------------------------------------------
terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% @private
%% @doc
%% Convert process state when code is changed
%%
%% @spec code_change(OldVsn, State, Extra) -> {ok, NewState}
%% @end
%%--------------------------------------------------------------------
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%===================================================================
%%% Internal functions
%%%===================================================================
