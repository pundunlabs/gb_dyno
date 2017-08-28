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

-include_lib("gb_log/include/gb_log.hrl").

%% Helper macro for declaring children of supervisor
-define(WORKER(I, A), {I, {I, start_link, A}, permanent, 5000, worker, [I]}).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    ok = gb_dyno:init(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec notify() ->
    ok | {error, Reason::term()}.
notify()->
    ?debug("Configuration change, notify called..", []),
    gb_dyno:update_configuration().

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

    Opts = gb_dyno:read_configuration(),
    {ok, Hash} = init_metadata(Opts),

    Gossip_Opts = [{hash, Hash} | Opts],
    GB_Dyno_Gossip = ?WORKER(gb_dyno_gossip, [Gossip_Opts]),
    GB_Dyno_Reachability = ?WORKER(gb_dyno_reachability, [Gossip_Opts]),

    {ok, { SupFlags, [GB_Dyno_Reachability, GB_Dyno_Gossip]} }.

%% ===================================================================
%% Internal Functions
%% ===================================================================
-spec init_metadata(Opts :: [{atom(), term()}]) ->
    ok.
init_metadata(Opts) ->
    gb_dyno_metadata:init(Opts).
