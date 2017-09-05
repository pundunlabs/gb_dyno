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

-module(gb_dyno_dist).

%% API functions
-export([topo_call/2,
	 call/3,
	 call_seq/2,
	 call_shards/3,
	 map_shards/3,
	 call_shards_seq/2,
	 map_shards_seq/2,
	 call_nodes/3,
	 call_nodes_minimal/3]).

%% Inter Node API
-export([local/1,
	 remote/5]).

-type args() :: [term()].

%%Concistency Requirement Record.
-record(creq, {level,
	       total,
	       quorum,
	       each_quorum,
	       local_dc,
	       num_of_dc}).

-include_lib("gb_log/include/gb_log.hrl").

%%--------------------------------------------------------------------
%% @doc
%% Apply request Req on the nodes in topology metadata.
%% @end
%%--------------------------------------------------------------------
-spec topo_call(Req :: {module(), function(), args()},
		Options :: [{atom(), term()}]) ->
    Response :: term() | {error, Reason :: term()}.
topo_call(MFA, Options) ->
    {ok, Topo} = gb_dyno_metadata:lookup_topo(),
    NodeData = proplists:get_value(nodes, Topo, []),
    Nodes = [Node || {Node, _Data} <- NodeData],
    call_nodes(Nodes, MFA, Options).

%%--------------------------------------------------------------------
%% @doc
%% Call to execute request Req on the nodes in Ring with consistency
%% requirement that is set for Mode.
%% @end
%%--------------------------------------------------------------------
-spec call(Ring :: map(),
	   Req :: {module(), function(), args()},
	   Mode :: write | read ) ->
    {ok, Response :: term()} | {error, Reason :: term()}.
call(Nodes, Req, Mode) when Mode == write; Mode == read ->
    Consistency =
	case Mode of
	    write ->
		gb_dyno:conf(write_consistency);
	    read ->
		gb_dyno:conf(read_consistency)
	end,
    DC = gb_dyno:conf(dc),
    Timeout = gb_dyno:conf(request_timeout),
    spawn_do(fun pdist/5, [Consistency, DC, Nodes, Req, Timeout]).

%%--------------------------------------------------------------------
%% @doc
%% Call sequentially to execute request Req on the nodes in Ring.
%% First succesfull response is returned.
%% @end
%%--------------------------------------------------------------------
-spec call_seq(Ring :: map(),
	       Req :: {module(), function(), args()}) ->
    {ok, Response :: term()} | {error, Reason :: term()}.
call_seq(Ring, Req) ->
    ?debug("Call sequential for: : ~p", [Ring]),
    DC = gb_dyno:conf(dc),
    Timeout = gb_dyno:conf(request_timeout),
    spawn_do(fun sdist/4, [DC, Ring, Req, Timeout]).

%%--------------------------------------------------------------------
%% @doc
%% Call to execute request Req for all shards on the nodes of each
%% shard's Ring with consistency requirement that is set for Mode.
%% Result is returned in respective order to Shards list.
%% @end
%%--------------------------------------------------------------------
-spec call_shards(Shards :: [{Shard :: string(), Ring :: map()}],
		  Req :: {module(), function(), args()},
		  Mode :: write | read) ->
    {ok, ResL :: [term()]} | {error, Reason :: term()}.
call_shards(Shards, Req, Mode) when Mode == write; Mode == read ->
    Reqs = [{?MODULE, call, [Ring, Req, Mode]} || {_, Ring} <- Shards],
    spawn_do(fun peval/1, [Reqs]).

%%--------------------------------------------------------------------
%% @doc
%% Call to execute request Req for all shards on the nodes of each
%% shard's Ring with consistency requirement that is set for Mode.
%% Shard name from Shards is added to Args list of the given Req.
%% Result is returned in respective order to Shards list.
%% @end
%%--------------------------------------------------------------------
-spec map_shards(Req :: {module(), function(), args()},
		 Mode :: write | read,
		 Shards :: [{Shard :: string(), Ring :: map()}]) ->
    {ok, ResL :: [term()]} | {error, Reason :: term()}.
map_shards({Mod, Fun, Args}, Mode, Shards) when Mode == write; Mode == read ->
    Reqs = [{?MODULE, call, [Ring, {Mod, Fun, [Shard | Args]}, Mode]}
	    || {Shard, Ring} <- Shards],
    spawn_do(fun peval/1, [Reqs]).

%%--------------------------------------------------------------------
%% @doc
%% Call to evaluate request Req for all shards sequentially on the
%% nodes of the shard's ring. First succesfull result is returned.
%% Result is returned in respective order to Shards list.
%% @end
%%--------------------------------------------------------------------
-spec call_shards_seq(Shards :: [{Shard :: string(), Ring :: map()}],
		      Req :: {module(), function(), args()}) ->
    {ok, ResL :: [term()]} | {error, Reason :: term()}.
call_shards_seq(Shards, Req) ->
    Reqs = [{?MODULE, call_seq, [Ring, Req]} || {_, Ring} <- Shards],
    spawn_do(fun peval/1, [Reqs]).

%%--------------------------------------------------------------------
%% @doc
%% Call to evaluate request Req for all shards sequentially on the
%% nodes of the shard's ring. Shard name is added to Args list of the
%% given Req. First succesfull result is returned.
%% Result is returned in respective order to Shards list.
%% @end
%%--------------------------------------------------------------------
-spec map_shards_seq(Req :: {module(), function(), args()},
		     Shards :: [{Shard :: string(), Ring :: map()}]) ->
    {ok, ResL :: [term()]} | {error, Reason :: term()}.
map_shards_seq({Mod, Fun, Args}, Shards) ->
    Reqs = [{?MODULE, call_seq, [Ring, {Mod, Fun, [Shard | Args]}]}
	    || {Shard, Ring} <- Shards],
    ?debug("Reqs: ~p",[Reqs]),
    spawn_do(fun peval/1, [Reqs]).

%%--------------------------------------------------------------------
%% @doc
%% Apply request Req on the given nodes.
%% @end
%%--------------------------------------------------------------------
-spec call_nodes(Nodes :: map() | [atom()],
		 Req :: {module(), function(), args()},
		 Options :: [{atom(), term()}]) ->
    Response :: term() | {error, Reason :: term()}.
call_nodes(Nodes, {Module, Function, Args}, Options) ->
    Timeout = proplists:get_value(timeout, Options, 5000),
    Revert =  proplists:get_value(revert, Options, undefined),
    Nodes_ = case is_map(Nodes) of
		true -> maps:keys(Nodes);
		false -> Nodes
	     end,
    {ResL, BadNodes} = rpc:multicall(Nodes_, Module, Function, Args, Timeout),
    revert_call(Nodes, Revert, Timeout, {ResL, BadNodes}).

%%--------------------------------------------------------------------
%% @doc
%% Call to evaluate request Req on minimal number of nodes to cover
%% all distributed shards. First succesfull result is used.
%% @end
%%--------------------------------------------------------------------
-spec call_nodes_minimal(NodesMapping :: map(),
			 Req :: {module(), function(), args()},
			 Options :: [{atom(), term()}]) ->
    {ok, map()} | {error, Reason :: term()}.
call_nodes_minimal(NodesMapping, Req, Options) ->
    Node = node(),
    {LocalShards, RemoteNodes} = maps:take(Node, NodesMapping),
    Rest = maps:to_list(RemoteNodes),
    Timeout = proplists:get_value(timeout, Options, 5000),
    Acc = #{covered => [],
	    uncovered => [],
	    resl => [],
	    unique => true},
    call_nodes_minimal([{Node, LocalShards} | Rest], Req, Timeout, Acc).

call_nodes_minimal([{Node, Shards} | Rest],
		   {Mod, Fun, Args} = Req, Timeout,
		   #{covered := Covered,
		     uncovered := Uncovered,
		     resl := ResL,
		     unique := Unique} = Acc) ->
    case Shards -- Covered of
	[] ->
	    call_nodes_minimal(Rest, Req, Timeout, Acc);
	RemSh ->
	    case rpc:call(Node, Mod, Fun, Args, Timeout) of
		{P, _} when P == badrpc ; P == error ->
		    Nacc = Acc#{uncovered => lists:usort(Shards ++ Uncovered)},
		    call_nodes_minimal(Rest, Req, Timeout, Nacc);
		R ->
		    Nacc = Acc#{covered => lists:usort(Shards ++ Covered),
				uncovered => Uncovered -- Shards,
				resl => [R | ResL],
				unique => check_unique(Unique, Shards, RemSh)},
		    call_nodes_minimal(Rest, Req, Timeout, Nacc)
	    end
    end;
call_nodes_minimal([], _Req, _Timeout, Acc) ->
    {ok, Acc}.

check_unique(Unique, Shards, Shards) ->
    Unique;
check_unique(_, _, _) ->
    false.

%%--------------------------------------------------------------------
%% @doc
%% Apply request.
%% @end
%%--------------------------------------------------------------------
-spec local(BaseReq :: {module(),atom(),[term()]}) ->
    term().
local({Mod, Fun, Args}) ->
    apply(Mod, Fun, Args).

%%--------------------------------------------------------------------
%% @doc
%% This function is only called by a remote node to coordinate
%% in data center distribution of the request.
%% @end
%%--------------------------------------------------------------------
-spec remote(Consistency :: string(),
	     DC :: string(),
	     LocalNodes :: [node()],
	     BaseReq :: {module(),atom(),[term()]},
	     Timeout :: integer()) ->
    {ok, Response :: term()} | {error, Reason :: term()}.
remote(Consistency, DC, LocalNodes, BaseReq, Timeout) ->
    Ring = maps:put(DC, LocalNodes, maps:new()),
    pdist(Consistency, DC, Ring, BaseReq, Timeout).

%%%===================================================================
%%% Internal functions
%%%===================================================================
-spec pdist(Consistency :: string(),
	    DC :: string(),
	    Ring :: map(),
	    BaseReq :: {module(),atom(),[term()]},
	    Timeout :: pos_integer()) ->
    {ok, Response :: term()} | {error, Reason :: term()}.
pdist(Consistency, DC, Ring, BaseReq, Timeout) ->
    Fold =
	fun(LDC, LocalNodes, {Acc, NPDC, TN}) when LDC == DC ->
	    Req = {?MODULE, local, [BaseReq]},
	    L = length(LocalNodes),
	    {[{N, Req, DC}|| N <- LocalNodes] ++ Acc, [{DC,L}|NPDC], TN+L};
	   (RDC, [N|_] = RemoteNodes, {Acc, NPDC, TN}) ->
	    Req = {?MODULE, remote, [Consistency, RDC,
					RemoteNodes, BaseReq, Timeout]},
	    L = length(RemoteNodes),
	    {[{N, Req, RDC} | Acc], [{RDC,L} | NPDC], TN+L}
	end,
    {Reqs, NodesPerDC, TotalNodes} = maps:fold(Fold, {[], [], 0}, Ring),
    Caller = self(),
    EachQuorum =  [{RDC, {L div 2 + 1, L}} || {RDC, L} <- NodesPerDC],
    Quorum = TotalNodes div 2 + 1,
    CReq = #creq{level = Consistency,
		 total = TotalNodes,
		 quorum = Quorum,
		 each_quorum = maps:from_list(EachQuorum),
		 local_dc = DC,
		 num_of_dc = length(EachQuorum)},
    {_, Mref} = spawn_monitor(fun()-> receiver(Reqs, Timeout, Caller) end),
    get_result(Mref, CReq, #{}).

-spec sdist(DC :: string(),
	    Ring :: map(),
	    BaseReq :: {module(),atom(),[term()]},
	    Timeout :: pos_integer()) ->
    {ok, Response :: term()} | {error, Reason :: term()}.
sdist(DC, Ring, BaseReq, Timeout) ->
    Req = {?MODULE, local, [BaseReq]},
    LocalNodes = maps:get(DC, Ring, []),
    RRing = maps:remove(DC, Ring),
    RemoteNodeSets = maps:values(RRing),
    SortedNodes = lists:flatten([LocalNodes|RemoteNodeSets]),
    sync_collect(SortedNodes, Req, Timeout, undefined).

-spec sync_collect(Nodes :: [node()],
		   Req :: {module(),atom(),[term()]},
		   Timeout :: non_neg_integer() | infinity,
		   LastError :: {badrpc, term()} | undefined) ->
    Resp :: term() | {badrpc, Error :: term()}.
sync_collect([Node | Rest], {Mod, Fun, Args}, Timeout, _) ->
    case rpc:call(Node, Mod, Fun, Args, Timeout) of
        {badrpc, _} = R->
	    sync_collect(Rest, {Mod, Fun, Args}, Timeout, R);
	R ->
	    R
    end;
sync_collect([], _, _, LastError) ->
    LastError.

%%--------------------------------------------------------------------
%% @doc
%% Collect the results from remote nodes and return after consistency
%% requirement is met. Rest of the results will be handled by a
%% middleman process running receiver/3 code.
%% @end
%%--------------------------------------------------------------------
-spec get_result(Mref :: identifier(),
		 CReq :: #creq{},
		 Results :: [{Node :: node(), Res :: term()}]) ->
    Response :: term() | {error, Reason :: term()}.
get_result(Mref, CReq, Results) ->
    receive
	{collect, Node, DC, {Fun, Result}} ->
	    case check_consistency_met(CReq, {Node, DC, Fun, Result}, Results) of
		{nok, NewResults} ->
		    get_result(Mref, CReq, NewResults);
		{ok, Response} ->
		    erlang:demonitor(Mref, [flush]),
		    Response;
		{error, Reason} ->
		    erlang:demonitor(Mref, [flush]),
		    {error, Reason}
	    end;
	{'DOWN',Mref,_,_,Reason} ->
	    %% Receiver process DOWN
	    %% Return error
	    ?debug("Middleman receiver process down: ~p",[Reason]),
	    {error, maps:get(error, Results, Reason)}
    end.

receiver(Reqs, Timeout, Caller) ->
    process_flag(trap_exit, true),
    Cref = erlang:monitor(process, Caller),
    Monitors = multi_call(Reqs, Timeout, self(), #{}),
    TimerRef = erlang:start_timer(Timeout, self(), ok),
    {Reason, Results} = receiver_loop(Cref, Caller, Monitors, []),
    cancel_timer(Reason, TimerRef),
    %% Process results.
    handle_results(Results),
    exit(Reason).

-spec receiver_loop(Cref :: reference(), Caller :: pid(),
		    Monitors :: map(), Results :: [{node(), term()}]) ->
    {Reason :: normal | timeout, Results :: [{node(), term()}]}.
receiver_loop(Cref, Caller, Monitors, Results) ->
    case maps:size(Monitors) of
	0 ->
	    {normal, Results};
	_ ->
	    receive
		{collect, Node, DC, {Fun, Result}} ->
		    %% Proxy the result if caller alive
		    Ref = maps:get(Node, Monitors),
		    erlang:demonitor(Ref, [flush]),
		    NewMon = maps:remove(Node, Monitors),
		    proxy(Caller, {collect, Node, DC, {Fun, Result}}),
		    receiver_loop(Cref, Caller, NewMon,
				  [{Node, Result} | Results]);
		{'DOWN',Cref,_,_,_} ->
		    %% Caller down, stop proxy.
		    %% Continue to collect results
		    receiver_loop(undefined, undefined, Monitors, Results);
		{timeout, _TimerRef, _} ->
		    receiver_cleanup(Caller, Monitors, Results)
	    end
    end.

-spec receiver_cleanup(Caller :: pid(),
		       Monitors :: map(),
		       Results :: [{node(), term()}]) ->
    {timeout, Results :: [{node(), term()}]}.
receiver_cleanup(Caller, Monitors, Results) ->
    Cleanup =
	fun(Node, Ref, Acc) ->
	    receive
		{collect, Node, DC, {Fun, Result}} ->
		    %% Proxy the result if caller alive
		    erlang:demonitor(Ref, [flush]),
		    proxy(Caller, {collect, Node, DC, {Fun, Result}}),
		    [{Node, Result} | Results];
		{'DOWN',Ref,_,_,Reason} ->
		    %% Sender down.
		    [{Node, {error, Reason}} | Results]
	    after 0 ->
		    erlang:demonitor(Ref, [flush]),
		    [{Node, {error, rpc_timeout}} | Acc]
	    end
	end,
    {timeout, maps:fold(Cleanup, Results, Monitors)}.

handle_results(Results) ->
    ?debug("Distribution results: ~p", [Results]).

proxy(undefined, _Msg) ->
    ok;
proxy(Pid, Msg) ->
    Pid ! Msg.

-spec multi_call(Req :: [{N :: node(), R :: term(), DC :: string()}],
		 T :: timeout(),
		 Collector :: pid(),
		 Acc :: map()) ->
    Acc :: map().
multi_call([{N, Req, DC} | Rest], T, Collector, Acc) ->
    {_, Ref} = spawn_monitor(fun() -> single_call(N, DC, Req, T, Collector) end),
    multi_call(Rest, T, Collector, maps:put(N, Ref, Acc));
multi_call([], _, _Collector, Acc) ->
    Acc.

-spec single_call(Node :: node(),
		  DC :: string(),
		  Req :: {module(), function(), args()},
		  Timeout :: timeout(),
		  Collector :: pid()) ->
    {error, Reason :: term()} | {ok, Result :: term()}.
single_call(Node, DC, {Mod, Fun, Args}, Timeout, Collector) ->
    Response =
	case rpc:call(Node, Mod, Fun, Args, Timeout) of
	    {badrpc, _} = R->
		%% Args is the enterdb call, removing the gb_dyno_dist wrapper
		enterdb_recovery:log_event(Node, hd(Args)),
		{error, R};
	    {error, not_ready} ->
		%% Args is the enterdb call, removing the gb_dyno_dist wrapper
		enterdb_recovery:log_event(Node, hd(Args)),
		{error, not_ready};
	    {error, _} = R ->
		R;
	    R ->
		{ok, R}
	end,
    Collector ! {collect, Node, DC, {Fun, Response}}.

%%ANY
check_consistency_met(#creq{level = 'ANY'},
		      {_Node, _DC, _, {ok, Response}}, _Results) ->
    {ok, Response};
check_consistency_met(#creq{level = 'ANY'},
		      {_Node, _DC, _, {error, Reason}}, Results) ->
    {nok, maps:put(error, Reason, Results)};

%%ONE
check_consistency_met(#creq{level = 'ONE'},
		      {_Node, _DC, _, {ok, Response}}, _Results) ->
    {ok, Response};
check_consistency_met(#creq{level = 'ONE'},
		      {_Node, _DC, _, {error, Reason}}, Results) ->
    {nok, maps:put(error, Reason, Results)};

%%TWO
check_consistency_met(#creq{level = 'TWO'},
		      {_Node, _DC, _, {ok, Response}}, Results) ->
    case maps:get(Response, Results, 0) of
	0 ->
	    {nok, maps:put(Response, 1, Results)};
	1 ->
	    {ok, Response}
    end;
check_consistency_met(#creq{level = 'TWO'},
		      {_Node, _DC, _, {error, Reason}}, Results) ->
    {nok, maps:put(error, Reason, Results)};

%%THREE
check_consistency_met(#creq{level = 'THREE'},
		      {_Node, _DC, _, {ok, Response}}, Results) ->
    case maps:get(Response, Results, 0) of
	Count when Count < 2 ->
	    {nok, maps:put(Response, Count + 1, Results)};
	2 ->
	    {ok, Response}
    end;
check_consistency_met(#creq{level = 'THREE'},
		      {_Node, _DC, _, {error, Reason}}, Results) ->
    {nok, maps:put(error, Reason, Results)};

%%LOCAL_ONE
check_consistency_met(#creq{level = 'LOCAL_ONE', local_dc = DC},
		      {_Node, DC, local, {ok, Response}}, _Results) ->
    {ok, Response};
check_consistency_met(#creq{level = 'LOCAL_ONE', local_dc = _DC},
		      {_Node, _RDC, remote, {ok, _Response}}, Results) ->
    {nok, Results};
check_consistency_met(#creq{level = 'LOCAL_ONE'},
		      {_Node, _DC, _, {error, Reason}}, Results) ->
    {nok, maps:put(error, Reason, Results)};

%%QUORUM
check_consistency_met(#creq{level = 'QUORUM',
			    quorum = Quorum,
			    total = Total},
		      {_Node, _DC, _, {error, _Reason}}, Results) ->
    Comp = Total - Quorum,
    case maps:get(error, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put(error, Count + 1, Results)};
	Comp ->
	    {error, quorum}
    end;
check_consistency_met(#creq{level = 'QUORUM', quorum = Quorum},
		      {_Node, _DC, _, {ok, Response}}, Results) ->
    Comp = Quorum - 1,
    case maps:get(Response, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put(Response, Count + 1, Results)};
	Comp ->
	    {ok, Response}
    end;
%%LOCAL_QUORUM
check_consistency_met(#creq{level = 'LOCAL_QUORUM',
			    each_quorum = EQuorum,
			    local_dc = DC},
		      {_Node, DC, local, {error, _Reason}}, Results) ->
    {Quorum , Total} = maps:get(DC, EQuorum),
    Comp = Total - Quorum,
    case maps:get(error_c, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put(error_c, Count + 1, Results)};
	Comp ->
	    {error, local_quorum}
    end;
check_consistency_met(#creq{level = 'LOCAL_QUORUM',
			    each_quorum = EachQuorum,
			    local_dc = DC},
		      {_Node, DC, local, {ok, Response}}, Results) ->
    {Quorum , _Total} = maps:get(DC, EachQuorum),
    Comp = Quorum - 1,
    case maps:get(Response, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put(Response, Count + 1, Results)};
	Comp ->
	    {ok, Response}
    end;
check_consistency_met(#creq{level = 'LOCAL_QUORUM',
			    each_quorum = _EQuorum,
			    local_dc = _},
		      {_Node, _DC, _, _}, Results) ->
    {nok, Results};

%% EACH QUORUM
check_consistency_met(#creq{level = 'EACH_QUORUM'},
		      {Node, DC, remote, {error, Reason}}, _Results) ->
    ?debug("EACH_QUORUM failed at ~p(~p), Reason :~p", [Node, DC, Reason]),
    {error, each_quorum};
check_consistency_met(#creq{level = 'EACH_QUORUM',
			    each_quorum = EQuorum},
		      {_Node, DC, local, {error, _Reason}}, Results) ->
    {Quorum , Total} = maps:get(DC, EQuorum),
    Comp = Total - Quorum,
    case maps:get({DC, error_c}, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put({DC, error_c}, Count + 1, Results)};
	Comp ->
	    {error, each_quorum}
    end;
check_consistency_met(#creq{level = 'EACH_QUORUM',
			    each_quorum = EachQuorum,
			    num_of_dc = N},
		      {_Node, DC, local, {ok, Response}}, Results) ->
    {Quorum , _Total} = maps:get(DC, EachQuorum),
    Comp = Quorum - 1,
    case maps:get({DC, Response}, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put({DC, Response}, Count + 1, Results)};
	Comp ->
	    case maps:get({complete, Response}, Results, 0) of
		C when C < (N - 1)->
		    {nok, maps:put({complete, Response}, C + 1, Results)};
		_ ->
		     {ok, Response}
	    end
    end;
check_consistency_met(#creq{level = 'EACH_QUORUM',
			    num_of_dc = N},
		      {_Node, _DC, remote, {ok, Response}}, Results) ->
    case maps:get({complete, Response}, Results, 0) of
	C when C < (N - 1)->
	    {nok, maps:put({complete, Response}, C + 1, Results)};
	_ ->
	    {ok, Response}
    end;

%%ALL
check_consistency_met(#creq{level = 'ALL'},
		      {Node, _DC, _, {error, Reason}}, _Results) ->
    {error, {Node, Reason}};
check_consistency_met(#creq{level = 'ALL', total = Total},
		      {_Node, _DC, _, {ok, Response}}, Results) ->
    Comp = Total - 1,
    case maps:get(Response, Results, 0) of
	Count when Count < Comp ->
	    {nok, maps:put(Response, Count + 1, Results)};
	Comp ->
	    {ok, Response}
    end.

-spec cancel_timer(Reason :: timeout | atom(),
		   TimerRef :: reference) ->
    false | integer().
cancel_timer(timeout, _TimerRef) ->
    0;
cancel_timer(_, TimerRef) ->
    erlang:cancel_timer(TimerRef).

%%--------------------------------------------------------------------
%% @doc
%% Revert rpc:multicall if revert mfa is given and error exists.
%% @end
%%--------------------------------------------------------------------
-spec revert_call(Nodes :: [node()],
		  Revert :: undefined | {module(), function(), args()},
		  Timeout :: pos_integer(),
		  {ResL :: [term()], BadNodes :: [node()]}) ->
    Response :: term() | {error, Reason :: term()}.
revert_call(_Nodes, undefined, _Timeout, {ResL, []}) ->
    check_response(ResL);
revert_call(_Nodes, undefined, _Timeout, {_ResL, BadNodes}) ->
    {error, {bad_nodes, BadNodes}};
revert_call(Nodes, {M, F, A}, Timeout, {ResL, []}) ->
    ?debug("ResL: ~p",[ResL]),
    case check_response(ResL) of
	{Bad, Res} when Bad == error; Bad == badrpc ->
	    {RevResL, RevBadNodes} = rpc:multicall(Nodes, M, F, A, Timeout),
	    ?debug("Revert result: ~p",[{RevResL, RevBadNodes}]),
	    {Bad, Res};
	Res ->
	    ?debug("Res: ~p",[Res]),
	    Res
    end;
revert_call(Nodes, {M, F, A}, Timeout, {_ResL, BadNodes}) ->
    {RevResL, RevBadNodes} = rpc:multicall(Nodes -- BadNodes, M, F, A, Timeout),
    ?debug("Revert result: ~p",[{RevResL, RevBadNodes}]),
    {error, {bad_nodes, BadNodes}}.

%%--------------------------------------------------------------------
%% @doc
%% Check response for errors
%% @end
%%--------------------------------------------------------------------
-spec check_response(ResL :: [term()]) ->
    term() | {error, ResL :: [term()]}.
check_response(ResL) ->
    check_unique_response(lists:usort(ResL)).

-spec check_unique_response(ResL :: [term()]) ->
    term() | {error, ResL :: [term()]}.
check_unique_response([Res]) ->
    Res;
check_unique_response([_|_] = ResL) ->
    {error, ResL}.

%%--------------------------------------------------------------------
%% @doc
%% Parallel evaluate requests on local node. apply(Mod, Fun, Args)
%% will be called on local node. Result list will be in respective
%% to request list.
%% @end
%%--------------------------------------------------------------------
-spec peval( Reqs :: [{module(), function(), args()}]) ->
    ResL :: [term()].
peval(Reqs) ->
    ReplyTo = self(),
    Pids = [async_eval(ReplyTo, Req) || Req <- Reqs],
    [yield(P) || P <- Pids].

-spec async_eval(ReplyTo :: pid(),
		 Req :: {module(), function(), args()}) ->
    Pid :: pid().
async_eval(ReplyTo, Req) ->
    spawn(
      fun() ->
	      R = local(Req),
	      ReplyTo ! {self(), {promise_reply, R}}
      end).

-spec yield(Pid :: pid()) ->
    term().
yield(Pid) when is_pid(Pid) ->
    {value, R} = do_yield(Pid, infinity),
    R.

-spec do_yield(Pid :: pid,
	       Timeout :: non_neg_integer() | infinity) ->
    {value, R :: term()} | timeout.
do_yield(Pid, Timeout) ->
    receive
        {Pid, {promise_reply,R}} ->
            {value, R}
        after Timeout ->
            timeout
    end.

-spec spawn_do(F :: fun(), A :: term()) -> undefined.
spawn_do(F,A) ->
    Caller = self(),
    Mref   = make_ref(),
    AF = fun() ->
	    Res = (catch apply(F, A)),
	    Caller ! {Mref, Res}
	end,
    {_Pid, Mon} = spawn_monitor(AF),
    receive
	{Mref, Res} ->
	    erlang:demonitor(Mon, [flush]),
	    Res;
	{'DOWN', Mon, _, _, Reason} ->
	    {error, Reason}
    end.
