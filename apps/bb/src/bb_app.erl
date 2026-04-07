-module(bb_app).

-behaviour(application).

-export([start/2, stop/1]).

-spec start(application:start_type(), term()) ->
    {ok, pid()} | {ok, pid(), State} | {error, term()}
when
    State :: term().
start(_StartType, _StartArgs) ->
    bb_sup:start_link().

-spec stop(State) -> ok when State :: term().
stop(_State) ->
    ok.

%% internal functions
