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

-module(gb_dyno_sup).

-behaviour(supervisor).

%% API
-export([start_link/0,
	 notify/0,
	 verify/0]).

%% Supervisor callbacks
-export([init/1]).

-include("gb_log.hrl").

%% Helper macro for declaring children of supervisor
-define(WORKER(I, A), {I, {I, start_link, A}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec notify() ->
    ok | {error, Reason::term()}.
notify()->
    ?debug("Configuration change, notify called..", []),
    ok.

-spec verify() ->
    ok | {error, Reason::term()}.
verify()->
    ?debug("Configuration load, verify called..", []),
    ok.

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init([]) ->
    RestartStrategy = one_for_one,
    MaxRestarts = 4,
    MaxSecondsBetweenRestarts = 3600,
    SupFlags = {RestartStrategy, MaxRestarts, MaxSecondsBetweenRestarts},
 
    GB_Dyno_Opts = get_gb_dyno_options(),
    {ok, Hash} = init_metadata(GB_Dyno_Opts),

    Gossip_Opts = [{hash, Hash} | GB_Dyno_Opts],
    GB_Dyno = ?WORKER(gb_dyno, [Gossip_Opts]),
    GB_Dyno_Gossip = ?WORKER(gb_dyno_gossip, [Gossip_Opts]),
    GB_Dyno_Reachability = ?WORKER(gb_dyno_reachability, [GB_Dyno_Opts]),

    {ok, { SupFlags, [GB_Dyno, GB_Dyno_Reachability, GB_Dyno_Gossip]} }.

%% ===================================================================
%% Internal Functions
%% ===================================================================
-spec get_gb_dyno_options() ->
    Options :: [{atom(), term()}].
get_gb_dyno_options() ->
    RC_Int = gb_conf:get_param("gb_dyno.yaml", reachability_check_interval),
    Cluster = gb_conf:get_param("gb_dyno.yaml", cluster),
    DC = gb_conf:get_param("gb_dyno.yaml", dc),
    Rack = gb_conf:get_param("gb_dyno.yaml", rack),
    [{reachability_check_interval, RC_Int},
     {cluster, Cluster}, {dc, DC}, {rack, Rack}].

-spec init_metadata(Opts :: [{atom(), term()}]) ->
    ok.
init_metadata(Opts) ->
    gb_dyno_metadata:init(Opts).
