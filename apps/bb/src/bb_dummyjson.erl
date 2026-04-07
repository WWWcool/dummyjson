-module(bb_dummyjson).

-behaviour(gen_server).

-export([start_link/0, set_fetch_interval/1, set_fetch_interval/2]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-define(HOST, "dummyjson.com").
-define(PORT, 443).

-define(DEFAULT_FETCH_INTERVAL_MS, 30_000).

-record(state, {
    gun_pid :: pid() | undefined,
    interval_ms :: pos_integer(),
    timer_ref :: reference() | undefined
}).

-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec set_fetch_interval(pos_integer()) -> ok | {error, invalid_interval | invalid_option}.
set_fetch_interval(Ms) ->
    set_fetch_interval(Ms, false).

-spec set_fetch_interval(pos_integer(), boolean()) -> ok | {error, invalid_interval | invalid_option}.
set_fetch_interval(Ms, Reschedule) when is_boolean(Reschedule) ->
    gen_server:call(?SERVER, {set_fetch_interval, Ms, Reschedule}, 5_000);
set_fetch_interval(_, _) ->
    {error, invalid_option}.

-spec init([]) -> {ok, state()}.
init([]) ->
    IntervalMs = get_default_fetch_interval_ms(),
    self() ! fetch_tick,
    {ok, #state{gun_pid = undefined, interval_ms = IntervalMs}}.

-spec handle_call(term(), gen_server:from(), state()) ->
    {reply, ok | {error, invalid_interval | {bad_call, term()}}, state()}.
handle_call({set_fetch_interval, Ms, false}, _From, State0) when is_integer(Ms), Ms > 0 ->
    {reply, ok, State0#state{interval_ms = Ms}};
handle_call({set_fetch_interval, Ms, true}, _From, State0) when is_integer(Ms), Ms > 0 ->
    State1 = State0#state{interval_ms = Ms},
    {reply, ok, schedule_fetch_tick(State1)};
handle_call({set_fetch_interval, _, _}, _From, State) ->
    {reply, {error, invalid_interval}, State};
handle_call(Request, _From, State) ->
    {reply, {error, {bad_call, Request}}, State}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(fetch_tick, State0) ->
    {_Result, State1} = fetch_products(State0),
    {noreply, schedule_fetch_tick(State1)};
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, State) ->
    _ = cancel_fetch_timer(State),
    case State#state.gun_pid of
        undefined ->
            ok;
        Pid ->
            ok = gun_close_safely(Pid)
    end.

get_default_fetch_interval_ms() ->
    case application:get_env(bb, fetch_interval_ms) of
        {ok, N} when is_integer(N), N > 0 ->
            N;
        _ ->
            ?DEFAULT_FETCH_INTERVAL_MS
    end.

schedule_fetch_tick(#state{interval_ms = Ms} = S0) ->
    S1 = cancel_fetch_timer(S0),
    Ref = erlang:send_after(Ms, self(), fetch_tick),
    S1#state{timer_ref = Ref}.

cancel_fetch_timer(#state{timer_ref = undefined} = S) ->
    S;
cancel_fetch_timer(#state{timer_ref = Ref} = S) ->
    _ = erlang:cancel_timer(Ref),
    S#state{timer_ref = undefined}.

fetch_products(State0) ->
    State1 = ensure_gun(State0),
    case State1#state.gun_pid of
        undefined ->
            {{error, no_connection}, State1};
        Pid ->
            StreamRef = gun:get(Pid, <<"/products">>),
            case await_json_body(Pid, StreamRef) of
                {ok, Body} ->
                    try
                        List = decode_id_price(Body),
                        ok = bb_products_store:append_snapshot(List),
                        {{ok, List}, State1}
                    catch
                        _:Reason ->
                            {{error, {decode, Reason}}, State1}
                    end;
                {error, _} = Err ->
                    ok = gun_close_safely(Pid),
                    State2 = State1#state{gun_pid = undefined},
                    {Err, State2}
            end
    end.

open_gun() ->
    Cacerts = [Der || {cert, Der, _} <- public_key:cacerts_get()],
    Opts = #{
        protocols => [http],
        transport => tls,
        tls_opts => [
            {verify, verify_peer},
            {cacerts, Cacerts}
        ]
    },
    case gun:open(?HOST, ?PORT, Opts) of
        {ok, Pid} ->
            case gun:await_up(Pid, 30_000) of
                {ok, _Protocol} ->
                    {ok, Pid};
                UpErr ->
                    ok = gun_close_safely(Pid),
                    {error, {gun_await_up, UpErr}}
            end;
        Err ->
            Err
    end.

ensure_gun(#state{gun_pid = Pid} = S) when is_pid(Pid) ->
    case is_process_alive(Pid) of
        true ->
            S;
        false ->
            case open_gun() of
                {ok, NewPid} ->
                    S#state{gun_pid = NewPid};
                _ ->
                    S#state{gun_pid = undefined}
            end
    end;
ensure_gun(#state{gun_pid = undefined} = S) ->
    case open_gun() of
        {ok, Pid} ->
            S#state{gun_pid = Pid};
        _ ->
            S
    end.

await_json_body(Pid, StreamRef) ->
    case gun:await(Pid, StreamRef, 60_000) of
        {response, nofin, Status, _Headers} when Status >= 200, Status < 300 ->
            case gun:await_body(Pid, StreamRef, 60_000) of
                {ok, Body} -> {ok, Body};
                Err -> {error, {body, Err}}
            end;
        {response, _, Status, _} ->
            {error, {http, Status}};
        Other ->
            {error, {gun, Other}}
    end.

gun_close_safely(Pid) when is_pid(Pid) ->
    try
        _ = gun:close(Pid)
    catch
        _:_ ->
            ok
    end,
    ok.

decode_id_price(Body) ->
    Map = jsx:decode(Body, [return_maps]),
    Products = maps:get(<<"products">>, Map, []),
    lists:map(
        fun(P) ->
            Id = maps:get(<<"id">>, P),
            Price = maps:get(<<"price">>, P),
            {Id, Price}
        end,
        Products
    ).
