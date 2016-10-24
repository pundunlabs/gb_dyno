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
%% Module Description:
%% @end
%%%===================================================================


-module(gb_dyno_reachability).

-behaviour(gen_server).

%% API functions
-export([start_link/1,
	 metadata_update/1,
	 multi_call_result/2,
	 db_op_result/2,
	 is_reachable/1,
	 reachability_check/0]).

%% gen_server callbacks
-export([init/1,
         handle_call/3,
         handle_cast/2,
         handle_info/2,
         terminate/2,
         code_change/3]).

-include_lib("gb_log/include/gb_log.hrl").

-record(state, {check_interval,
		check_tref}).

-type proplist() :: [{atom(), term()}].
-type timestamp() :: {MegaSecs :: pos_integer(),
		      Secs :: pos_integer(),
		      MicroSecs :: pos_integer()}.

-record(node, {name :: node(),
	       dc :: string(),
	       rack :: string(),
	       reachable :: boolean(),
	       last_try :: timestamp(),
	       unreachable_ts :: timestamp(),
	       removed :: boolean()}).

-record(dc, {name :: string(),
	     num_of_nodes :: integer(),
	     num_of_reachable_nodes :: integer()}).

-record(rack, {name :: string(),
	       num_of_nodes :: integer(),
	       num_of_reachable_nodes :: integer()}).

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
-spec metadata_update(MetaData :: proplist()) ->
    ok.
metadata_update(Metadata) ->
    Nodes = construct_node_info(Metadata),
    {DCs, Racks} = construct_dc_rack_info(Nodes),
    gen_server:cast(?MODULE, {metadata_update, Nodes, DCs, Racks}).

-spec multi_call_result(Replies :: [{node(), term()}],
			Badnodes :: [node()]) -> ok.
multi_call_result(Replies, BadNodes) ->
    gen_server:cast(?MODULE, {multi_call_result, Replies, BadNodes}).

-spec db_op_result(Node :: node(), OpResult :: term()) -> ok.
db_op_result(Node, OpResult) ->
    gen_server:cast(?MODULE, {db_op_result, Node, OpResult}).

-spec is_reachable(Node :: node()) ->
    true | false.
is_reachable(Node) ->
    case ets:lookup(node_tab, Node) of
	[#node{reachable = true}] ->
	    true;
	[#node{reachable = false,
	       removed = Rem} = NodeRec] ->
	    Ts = os:timestamp(),
	    {Reachable, UTs} = check_reachability(Rem, Node, Ts),
	    UpdateNodeRec = NodeRec#node{reachable = Reachable,
					 last_try = Ts,
					 unreachable_ts = UTs},
	    gen_server:cast(?MODULE, {update_node, UpdateNodeRec}),
	    Reachable;
	[] ->
	    false
    end.

-spec reachability_check() ->
    [{Node :: node(), Reachable :: boolean()}].
reachability_check() ->
    Fun =
	fun(#node{name = Name,
		  reachable = false}, Acc) ->
	    Bool = is_reachable(Name),
	    [{Name, Bool} | Acc];
	    (#node{name = Name,
		   reachable = true}, Acc) ->
	    [{Name, true} | Acc]
	end,
    ets:foldl(Fun, [], node_tab),
    gen_server:cast(?MODULE, set_next_reachability_check).

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
    ?debug("Starting ~p server. Options: ~p", [?MODULE, Options]),
    CheckInterval = proplists:get_value(reachability_check_interval, Options),
    
    {ok, Metadata} = gb_dyno_metadata:lookup_topo(),
    ?debug("~p, Metadata: ~p", [?MODULE, Metadata]),
    Nodes = construct_node_info(Metadata),
    ?debug("~p, Nodes: ~p", [?MODULE, Nodes]),
    ets:new(node_tab, [named_table,
		       protected,
		       {keypos, #node.name},
		       {read_concurrency, true}]),
    ets:new(dc_tab, [named_table,
		     protected,
		     {keypos, #dc.name},
		     {read_concurrency, true}]),
    ets:new(rack_tab, [named_table,
		       protected,
		       {keypos, #rack.name},
		       {read_concurrency, true}]),
    ?debug("~p,  ets:new ok", [?MODULE]),
    ets:insert(node_tab, Nodes),
    {DCs, Racks} = construct_dc_rack_info(Nodes),
    ?debug("~p,  DCs: ~p, Racks: ~p", [?MODULE, DCs, Racks]),
    ets:insert(dc_tab, DCs),
    ets:insert(rack_tab, Racks),
    {ok, TRef} = timer:apply_after(CheckInterval, ?MODULE, reachability_check, []),
    {ok, #state{check_interval = CheckInterval,
		check_tref = TRef}}.

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
handle_call(_Request, _From, State) ->
    Reply = ok,
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
handle_cast({db_op_result, Node, OpResult}, State) ->
    ?debug("~p unreachable, db_op_result ~p", [Node, OpResult]),
    handle_multi_call_result([], [Node]),
    {noreply, State};
handle_cast({metadata_update, Nodes, DCs, Racks}, State) ->
    ets:insert(node_tab, Nodes),
    ets:insert(dc_tab, DCs),
    ets:insert(rack_tab, Racks),
    {noreply, State};
handle_cast({multi_call_result, Replies, BadNodes}, State) ->
    ?debug("Multi Call Replies: ~p, BadNodes: ~p", [Replies, BadNodes]),
    handle_multi_call_result(Replies, BadNodes),
    {noreply, State};
handle_cast(set_next_reachability_check,
	    State = #state{check_interval = CheckInterval,
			   check_tref = TRef}) ->
    timer:cancel(TRef),
    {ok, NewTRef} = timer:apply_after(CheckInterval, ?MODULE,
				       reachability_check, []),
    {noreply, State#state{check_tref= NewTRef}};
handle_cast({update_node, NodeRec}, State) ->
    case NodeRec of
	#node{reachable = true} ->
	    update_node(up, NodeRec);
	#node{reachable = false} ->
	    update_node(no_change, NodeRec)
    end,
    {noreply, State};
handle_cast(_Msg, State) ->
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
terminate(_Reason, #state{check_tref = Tref}) ->
    timer:cancel(Tref),
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
-spec construct_node_info(Metadata :: proplist()) ->
    [#node{}].
construct_node_info(Metadata) ->
    NodesData = proplists:get_value(nodes, Metadata),
    [begin
	Removed = proplists:get_value(removed, Data, false),
	Ts = os:timestamp(),
	{Reachable, UTs} = check_reachability(Removed, Name, Ts),
	#node{name = Name,
	      dc = proplists:get_value(dc, Data),
	      rack = proplists:get_value(rack, Data),
	      reachable = Reachable,
	      last_try = Ts,
	      unreachable_ts = UTs,
	      removed = Removed}
     end || {Name, Data} <- NodesData].

-spec check_reachability(Removed :: boolean(),
			 Node :: node(),
			 Ts :: timestamp()) ->
    {Reachable :: boolean() | undefined,
     UnreachableTs :: timestamp() | undefined}.
check_reachability(false, Node, Ts) ->
    case net_adm:ping(Node) of
	pong ->
	    {true, undefined};
	pang ->
	    {false, Ts}
    end;
check_reachability(true, _Node, _)->
    {undefined, undefined}.

-spec construct_dc_rack_info(Nodes :: [#node{}]) ->
    {[#dc{}], [#rack{}]}.
construct_dc_rack_info(Nodes) ->
    construct_dc_rack_info(Nodes, #{}, #{}).

-spec construct_dc_rack_info(Nodes :: [#node{}],
			     DcMap :: map(),
			     RackMap :: map()) ->
    {[#dc{}], [#rack{}]}.
construct_dc_rack_info([#node{dc = DC,
			      rack = Rack,
			      reachable = Reachable,
			      removed = false} | Rest], DcMap, RackMap) ->
    Add =
	case Reachable of
	    true -> 1;
	    false -> 0
	end,
    NewDcMap =
	case maps:get(DC, DcMap, undefined) of
	    undefined ->
		DcRec = #dc{name = DC,
			    num_of_nodes = 1,
			    num_of_reachable_nodes = Add},
		maps:put(DC, DcRec, DcMap);
	    #dc{num_of_nodes = DCN,
		num_of_reachable_nodes = DCRN} = DcRec->
		NewDcRec = DcRec#dc{num_of_nodes = DCN + 1,
				    num_of_reachable_nodes = DCRN + Add},
		maps:put(DC, NewDcRec, DcMap)
	end,
    NewRackMap =
	case maps:get(Rack, RackMap, undefined) of
	    undefined ->
		RackRec = #rack{name = Rack,
				num_of_nodes = 1,
				num_of_reachable_nodes = Add},
		maps:put(Rack, RackRec, RackMap);
	    #rack{num_of_nodes = RN,
		  num_of_reachable_nodes = RRN} = RRec ->
		NewRRec = RRec#rack{num_of_nodes = RN + 1,
				    num_of_reachable_nodes = RRN + Add},
		maps:put(Rack, NewRRec, RackMap)
	end,
    construct_dc_rack_info(Rest, NewDcMap, NewRackMap);
construct_dc_rack_info([], DcMap, RackMap) ->
    Fun = fun(_K, V, Acc) -> [V | Acc] end,
    {maps:fold(Fun, [], DcMap),
     maps:fold(Fun, [], RackMap)}.

-spec handle_multi_call_result(Replies :: [{node(), term()}],
			       Badnodes :: [node()]) ->
    ok.
handle_multi_call_result(Replies, Badnodes) ->
    Ts = os:timestamp(),
    [begin
	case ets:lookup(node_tab, Node) of
	    [#node{reachable=false} = Rec] ->
		?info("~p is reachable again", [Node]),
		update_node(up, Rec#node{last_try = Ts,
					 reachable = true,
					 unreachable_ts = undefined}),
		notify_reachability(Node);
	    [#node{} = Rec] ->
		update_node(no_shange, Rec#node{last_try = Ts})
	end
     end || {Node, _Reply} <- Replies],
    [begin
	case ets:lookup(node_tab, Node) of
	    [#node{reachable=false} = Rec] ->
		update_node(no_change, Rec#node{last_try = Ts});
	    [#node{reachable=true} = Rec] ->
		update_node(down, Rec#node{last_try = Ts,
					   reachable = false,
					   unreachable_ts = Ts})
	end
     end || Node <- Badnodes],
    ok.

-spec update_node(Status :: up | down | no_change,
		  Rec :: #node{}) ->
    ok.
update_node(Status, Rec = #node{dc = DC, rack = Rack}) ->
    ets:insert(node_tab, Rec),
    [DCRec] = ets:lookup(dc_tab, DC),
    [RackRec] = ets:lookup(rack_tab, Rack),

    update_dc_rack(Status, DCRec, RackRec).

-spec update_dc_rack(Status :: up | down | no_change,
		     DCRec :: #dc{}, RackRec :: #rack{}) ->
    ok.
update_dc_rack(no_change, _, _) ->
    ok;
update_dc_rack(up, DCRec, RackRec) ->
    #dc{num_of_reachable_nodes = DCRN} = DCRec,
    #rack{num_of_reachable_nodes = RRN} = RackRec,
    ets:insert(dc_tab, DCRec#dc{num_of_reachable_nodes = DCRN + 1}),
    ets:insert(rack_tab, RackRec#rack{num_of_reachable_nodes = RRN + 1}),
    ok;
update_dc_rack(down, DCRec, RackRec) ->
    #dc{name = DcName, num_of_reachable_nodes = DCRN} = DCRec,
    #rack{name = RackName, num_of_reachable_nodes = RRN} = RackRec,

    ets:insert(dc_tab, DCRec#dc{num_of_reachable_nodes = DCRN - 1}),
    ets:insert(rack_tab, RackRec#rack{num_of_reachable_nodes = RRN - 1}),
    notify_unavailability(dc, DCRN, DcName),
    notify_unavailability(rack, RRN, RackName).

-spec notify_unavailability(P :: dc | rack,
			    Int :: integer(),
			    Name :: string()) ->
    ok.
notify_unavailability(P, 1, Name) ->
    ?warning("~p: ~p is not reachable",[P, Name]);
notify_unavailability(_, _, _) ->
    ok.

-spec notify_reachability(Node :: node()) -> ok.
notify_reachability(_Node) ->
    ok.
