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

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
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
    ok = gb_dyno:init(),
    SupFlags = #{strategy => one_for_one, intensity => 2, period => 5},
    GbDynoMetaData = #{
	id => gb_dyno_metadata,
	start => {gb_dyno_metadata, start_link, []}
    },
    GbDynoCommSup = #{
	id => gb_dyno_comm_sup,
	start => {gb_dyno_comm_sup, start_link, []},
	type => supervisor
    },
    ChildSpecs = [GbDynoMetaData, GbDynoCommSup],
    {ok, {SupFlags, ChildSpecs}}.

%% ===================================================================
%% Internal Functions
%% ===================================================================
