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

-module(gb_dyno_gossip).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
	 pull/1]).

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

-include_lib("gb_log/include/gb_log.hrl").
-define(TIMEOUT, 5000).

%%%===================================================================
%%% API functions
%%%===================================================================

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
-spec pull(RemoteNode :: node()) ->
    ok | {error, Reason :: term()}.
pull(RemoteNode) ->
    case gb_dyno_metadata:pull_topo(RemoteNode) of
	{ok, Metadata} ->
	    {ok, Hash} = gb_dyno_metadata:commit_topo(Metadata),
	    gen_server:cast({?MODULE, node()}, {commit, Hash, Metadata});
	{error, Reason} ->
	    {error, Reason}
    end.

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
handle_call({pull_request, Hash, _}, _From, State = #state{hash = Hash}) ->
    ?debug("pull_request for same hash: ~p", [Hash]),
    {reply, ok, State};
handle_call({pull_request, _, RemoteNode}, _From, State) ->
    ?debug("pull_request from ~p", [RemoteNode]),
    erlang:spawn(?MODULE, pull, [RemoteNode]),
    {reply, ok, State};
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
handle_cast({commit, Hash, _}, State = #state{hash = Hash}) ->
    ?debug("commit notification for same hash: ~p", [Hash]),
    {noreply, State};
handle_cast({commit, Hash, Metadata}, State) ->
    ?debug("commit notification", []),
    gb_dyno_reachability:metadata_update(Metadata),
    NodesData = proplists:get_value(nodes, Metadata),
    Nodes = [N || {N, _} <- NodesData, N =/= node()],
    {Replies, BadNodes} =
	gen_server:multi_call(Nodes, ?MODULE, {pull_request, Hash, node()}, ?TIMEOUT),
    gb_dyno_reachability:multi_call_result(Replies, BadNodes),
    {noreply, State#state{hash = Hash}};
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
