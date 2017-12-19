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

-module(gb_dyno).

%% API functions
-export([init/0,
	 update_configuration/0,
	 read_configuration/0,
	 conf/1,
	 conf/2]).

-include_lib("gb_log/include/gb_log.hrl").

-define(REGISTER, '$gb_dyno_conf').
-define(DEFAULT_REACHABILITY_CHECK_INTERVAL, 60000).

%%%===================================================================
%%% API functions
%%%===================================================================
-spec update_configuration() ->
    ok | {error, Reason :: term()}.
update_configuration() ->
    Tuples = read_configuration(),
    gb_reg:insert(?REGISTER, Tuples).

-spec conf(Key :: term()) ->
    Value :: term() | undefined.
conf(Key) ->
    conf(Key, undefined).

-spec conf(Key :: term(), Default :: term()) ->
    Value :: term().
conf(Key, Default) ->
    case gb_reg:lookup(?REGISTER, Key) of
	undefined -> Default;
	Val -> Val
    end.

-spec init() ->
    ok | {error, Reason :: term()}.
init() ->
    Tuples = read_configuration(),
    case gb_reg:new("gb_dyno_conf", Tuples) of
	{ok, _Module} ->
	    ok;
	{error, {already_exists, Module}} ->
	    gb_reg:insert(Module, Tuples);
	{error, Reason} ->
	    {error, Reason}
    end.

-spec read_configuration() ->
    Options :: [{atom(), term()}].
read_configuration() ->
    RC_Int = gb_conf:get_param("gb_dyno.yaml", reachability_check_interval,
				?DEFAULT_REACHABILITY_CHECK_INTERVAL),
    Cluster = gb_conf:get_param("gb_dyno.yaml", cluster),
    DC = gb_conf:get_param("gb_dyno.yaml", dc),
    Rack = gb_conf:get_param("gb_dyno.yaml", rack),
    RequestTimeout = gb_conf:get_param("gb_dyno.yaml", request_timeout),
    WriteConsistency = gb_conf:get_param("gb_dyno.yaml", write_consistency),
    ReadConsistency = gb_conf:get_param("gb_dyno.yaml", read_consistency),
    [{reachability_check_interval, RC_Int},
     {cluster, Cluster}, {dc, DC}, {rack, Rack},
     {write_consistency, list_to_atom(WriteConsistency)},
     {read_consistency, list_to_atom(ReadConsistency)},
     {request_timeout, RequestTimeout}].
