-module(bb_http).

-export([listener_child_spec/0]).

-spec listener_child_spec() -> supervisor:child_spec().
listener_child_spec() ->
    Dispatch = cowboy_router:compile([
        {'_', [
            {<<"/assembled-products">>, bb_assembled_products_h, []},
            {<<"/set-time">>, bb_set_time_h, []}
        ]}
    ]),
    #{
        id => bb_http_listener,
        start =>
            {cowboy, start_clear, [
                bb_http_listener,
                #{socket_opts => [{port, http_port()}]},
                #{env => #{dispatch => Dispatch}}
            ]},
        restart => permanent,
        shutdown => 1_000,
        type => supervisor,
        modules => [cowboy]
    }.

-spec http_port() -> inet:port_number().
http_port() ->
    case application:get_env(bb, http_port) of
        {ok, P} when is_integer(P) ->
            P;
        _ ->
            8080
    end.
