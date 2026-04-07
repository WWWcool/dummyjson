-module(bb_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

-spec start_link() -> supervisor:startlink_ret().
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

-spec init([]) ->
    {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}} | ignore | {error, term()}.
init([]) ->
    %% one_for_one: дети tolerant к падению соседей — dummyjson ловит noproc store
    %% в try/catch (fetch_products), хендлеры cowboy вернут ошибку клиенту;
    %% при перезапуске store всё восстановится на следующем тике/запросе.
    SupFlags = #{
        strategy => one_for_one,
        intensity => 5,
        period => 10
    },
    ChildSpecs = [
        #{
            id => bb_products_store,
            start => {bb_products_store, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [bb_products_store]
        },
        #{
            id => bb_dummyjson,
            start => {bb_dummyjson, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [bb_dummyjson]
        },
        bb_http:listener_child_spec()
    ],
    {ok, {SupFlags, ChildSpecs}}.

%% internal functions
