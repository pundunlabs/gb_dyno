%%%===================================================================
%% @author Jonas Falkevik
%% @copyright 2017 Pundun Labs AB
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
%% @title
%% @doc
%% Module Description:
%% @end
%%%===================================================================
-module(gb_dyno_gossip_sync).
-include_lib("gb_log/include/gb_log.hrl").
-define(SYNC_INTERVAL, 30000).

-export([start/1,
	 sync_metadata/1,
	 do_sync/0]).

-spec start(Pid :: pid()) -> stop.
start(Pid) ->
    erlang:monitor(process, Pid),
    sync_metadata(Pid).

sync_metadata(Pid) ->
    do_sync(),
    sync_wait_loop(Pid).

sync_wait_loop(Pid) ->
    receive
	{'DOWN', _, _, Pid, _} ->
	    stop;
	M ->
	    ?warning("unhandled message ~p", [M]),
	    sync_wait_loop(Pid)
    after ?SYNC_INTERVAL ->
	?MODULE:sync_metadata(Pid)
    end.
do_sync() ->
    case gb_dyno_metadata:lookup_topo() of
	{ok, Data} ->
	    ClusterNodes = proplists:get_value(nodes, Data),
	    Nodes = [N || {N, _} <- ClusterNodes],
	    Res = [{N,gb_dyno_gossip:pull(N)}|| N <- Nodes],
	    ?debug("Sync metadata ~p", [Res]),
	    check_consistency(),
	    ok;
	_ ->
	    ok
    end.

check_consistency() ->
    ?debug("checking consistency"),
    case gb_dyno_metadata:lookup_topo() of
	{ok, MetaData} ->
	    Nodes = proplists:get_value(nodes, MetaData, []),
	    LocalState = proplists:get_value(node(), Nodes, []),
	    find_oos(LocalState, Nodes);
	Error ->
	    ?warning("could not get dyno metadata: ~p", [Error])
    end.

find_oos([{{Shard, oos}, {Type, RemNode}} = OOS | R], Nodes) ->
    resolve_oos(OOS, Nodes),
    find_oos(R, Nodes);

find_oos([V|R], Nodes) ->
    find_oos(R, Nodes);
find_oos([], _) ->
    ok.

resolve_oos({{Shard, oos}, {Type, RemNode}}, Nodes) ->
    ?info("resloving inconsistency ~p (~p)", [Shard, {Type, RemNode}]),
    RemNodeState = proplists:get_value(RemNode, Nodes, []),
    RemShardState = proplists:get_value({Shard, oos}, RemNodeState, ready),
    case RemShardState of
	ready ->
	    enterdb_recovery:start_recovery(Type, RemNode, Shard);
	_ ->
	    pick_version(Shard, node(), RemNode)
    end.

pick_version(Shard, LocalNode, RemNode) when LocalNode < RemNode ->
    gb_dyno_metadata:node_rem_prop(LocalNode, {Shard, oos}),
    gb_dyno_metadata:node_rem_prop(RemNode,   {Shard, oos}),
    gb_dyno_metadata:node_add_prop(RemNode,   {{Shard, oos}, {full, node()}}),
    rpc:call(LocalNode, enterdb_shard_recovery, stop, [{RemNode, Shard}]),
    rpc:call(RemNode,   enterdb_shard_recovery, stop, [{LocalNode, Shard}]);

%% let the other node resolve the inconsistency
pick_version(_Shard, LocalNode, RemNode) ->
    ok.
