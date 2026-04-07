-module(bb_products_store).

-behaviour(gen_server).

-export([start_link/0, append_snapshot/1, list_snapshots/0, list_snapshots/1]).

-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2]).

-define(SERVER, ?MODULE).

-type snapshot_row() :: {TsMicros :: integer(), Items :: [{integer(), number()}]}.
-type filter_opts() :: #{
    from_ts => integer(),
    to_ts => integer(),
    product_id => integer()
}.

-type time_index() :: #{
    calendar:date() => #{non_neg_integer() => #{non_neg_integer() => [integer()]}}
}.

-export_type([snapshot_row/0, filter_opts/0]).

-record(state, {
    %% product_id => [ TsMicros | ... ] (новые впереди; при выборке сортируем)
    by_product = #{} :: #{integer() => [integer()]},
    %% день {Y,M,D} => час => минута => [ TsMicros | ... ]
    by_time = #{} :: time_index()
}).

-type state() :: #state{}.

-spec start_link() -> {ok, pid()} | ignore | {error, term()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

-spec append_snapshot([{integer(), number()}]) -> ok.
append_snapshot(Items) when is_list(Items) ->
    gen_server:call(?SERVER, {append, Items}).

-spec list_snapshots() -> {ok, [snapshot_row()]}.
list_snapshots() ->
    list_snapshots(#{}).

-spec list_snapshots(filter_opts()) -> {ok, [snapshot_row()]}.
list_snapshots(Opts) when is_map(Opts) ->
    gen_server:call(?SERVER, {list_snapshots, Opts}).

-spec init([]) -> {ok, state()}.
init([]) ->
    _ = ets:new(bb_products_snapshots, [
        named_table,
        protected,
        set,
        {keypos, 1},
        {read_concurrency, true}
    ]),
    {ok, #state{}}.

-spec handle_call(term(), gen_server:from(), state()) ->
    {reply, ok | {ok, [snapshot_row()]} | {error, unknown}, state()}.
handle_call({append, Items}, _From, S) ->
    Ts = erlang:system_time(microsecond),
    true = ets:insert(bb_products_snapshots, {Ts, Items}),
    S1 = index_append(S, Ts, Items),
    {reply, ok, S1};
handle_call({list_snapshots, Opts}, _From, S) when is_map(Opts) ->
    Rows = list_snapshots_rows(S, Opts),
    {reply, {ok, Rows}, S};
handle_call(_Req, _From, S) ->
    {reply, {error, unknown}, S}.

-spec handle_cast(term(), state()) -> {noreply, state()}.
handle_cast(_Msg, State) ->
    {noreply, State}.

-spec handle_info(term(), state()) -> {noreply, state()}.
handle_info(_Info, State) ->
    {noreply, State}.

-spec terminate(term(), state()) -> ok.
terminate(_Reason, _State) ->
    ok.

%% --- индексы ---

index_append(#state{by_product = BP0, by_time = BT0} = S, Ts, Items) ->
    BP = lists:foldl(
        fun({ProductId, _}, Acc) ->
            Old = maps:get(ProductId, Acc, []),
            Acc#{ProductId => [Ts | Old]}
        end,
        BP0,
        Items
    ),
    BT = time_index_add(BT0, Ts),
    S#state{by_product = BP, by_time = BT}.

time_index_add(BT, TsMicro) ->
    Secs = TsMicro div 1_000_000,
    {{Y, M, D}, {H, Mi, _}} = utc_secs_to_datetime(Secs),
    Date = {Y, M, D},
    DayMap0 = maps:get(Date, BT, #{}),
    HourMap0 = maps:get(H, DayMap0, #{}),
    List0 = maps:get(Mi, HourMap0, []),
    HourMap = HourMap0#{Mi => [TsMicro | List0]},
    DayMap = DayMap0#{H => HourMap},
    BT#{Date => DayMap}.

%% --- выборка ---

list_snapshots_rows(S, Opts) ->
    From = maps:get(from_ts, Opts, undefined),
    To = maps:get(to_ts, Opts, undefined),
    ProdId = maps:get(product_id, Opts, undefined),
    Rows0 = snapshots_rows_by_ts_filter(S, From, To, ProdId),
    filter_rows_by_product(Rows0, ProdId).

%% Полный список строк или выборка по кандидатам Ts (индексы + время/товар).
snapshots_rows_by_ts_filter(S, From, To, ProdId) ->
    HasTime = (From =/= undefined) orelse (To =/= undefined),
    HasProd = is_integer(ProdId),
    case ts_candidates(S, From, To, ProdId, HasTime, HasProd) of
        all ->
            lists:keysort(1, ets:tab2list(bb_products_snapshots));
        {ts, TsCand} ->
            rows_from_ts(TsCand)
    end.

ts_candidates(S, From, To, ProdId, HasTime, HasProd) ->
    BP = S#state.by_product,
    BT = S#state.by_time,
    case {HasTime, HasProd} of
        {false, false} ->
            all;
        {false, true} ->
            {ts, lists:usort(maps:get(ProdId, BP, []))};
        {true, false} ->
            {ts, ts_candidates_time(BT, From, To)};
        {true, true} ->
            TsP = ordsets:from_list(maps:get(ProdId, BP, [])),
            TsT = ordsets:from_list(ts_candidates_time(BT, From, To)),
            {ts, ordsets:to_list(ordsets:intersection(TsP, TsT))}
    end.

%% При фильтре по товару — в строке только позиции с этим id (не весь снимок).
filter_rows_by_product(Rows, ProdId) when is_integer(ProdId) ->
    [{Ts, [{I, P} || {I, P} <- Items, I =:= ProdId]} || {Ts, Items} <- Rows];
filter_rows_by_product(Rows, _) ->
    Rows.

rows_from_ts(TsSorted) ->
    lists:filtermap(
        fun(Ts) ->
            case ets:lookup(bb_products_snapshots, Ts) of
                [{Ts, Items}] ->
                    {true, {Ts, Items}};
                [] ->
                    false
            end
        end,
        TsSorted
    ).

ts_candidates_time(BT, _FromOpt, _ToOpt) when map_size(BT) =:= 0 ->
    [];
ts_candidates_time(BT, FromOpt, ToOpt) ->
    MinDate = lists:min(maps:keys(BT)),
    FromMicro =
        case FromOpt of
            undefined -> date_to_micros(MinDate);
            F -> F
        end,
    ToMicro =
        case ToOpt of
            undefined -> erlang:system_time(microsecond);
            T -> T
        end,
    case FromMicro > ToMicro of
        true ->
            [];
        false ->
            Start = ts_to_dhm(FromMicro),
            End = ts_to_dhm(ToMicro),
            Raw = collect_ts_loop(BT, Start, End, FromOpt, ToOpt, []),
            lists:usort(lists:flatten(Raw))
    end.

date_to_micros(Date) ->
    Epoch = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    G = calendar:datetime_to_gregorian_seconds({Date, {0, 0, 0}}),
    (G - Epoch) * 1_000_000.

ts_to_dhm(TsMicro) ->
    Secs = TsMicro div 1_000_000,
    {Date, {H, Mi, _}} = utc_secs_to_datetime(Secs),
    {Date, H, Mi}.

utc_secs_to_datetime(Secs) when is_integer(Secs) ->
    Epoch = calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}),
    calendar:gregorian_seconds_to_datetime(Epoch + Secs).

collect_ts_loop(BT, Curr, End, FromOpt, ToOpt, Acc) ->
    case dhm_le(Curr, End) of
        false ->
            lists:reverse(Acc);
        true ->
            {Date, H, Mi} = Curr,
            BucketTs = bucket_ts_list(BT, Date, H, Mi),
            OkTs = [T || T <- BucketTs, ts_in_range(T, FromOpt, ToOpt)],
            collect_ts_loop(BT, next_minute(Curr), End, FromOpt, ToOpt, [OkTs | Acc])
    end.

bucket_ts_list(BT, Date, H, Mi) ->
    case maps:get(Date, BT, undefined) of
        undefined ->
            [];
        DayMap ->
            case maps:get(H, DayMap, undefined) of
                undefined ->
                    [];
                HourMap ->
                    maps:get(Mi, HourMap, [])
            end
    end.

dhm_le({D1, H1, Mi1}, {D2, H2, Mi2}) ->
    calendar:datetime_to_gregorian_seconds({D1, {H1, Mi1, 0}}) =<
        calendar:datetime_to_gregorian_seconds({D2, {H2, Mi2, 0}}).

next_minute({Date, H, Mi}) ->
    G = calendar:datetime_to_gregorian_seconds({Date, {H, Mi, 0}}) + 60,
    {Date2, {H2, Mi2, _}} = calendar:gregorian_seconds_to_datetime(G),
    {Date2, H2, Mi2}.

-spec ts_in_range(integer(), undefined | integer(), undefined | integer()) -> boolean().
ts_in_range(_Ts, undefined, undefined) ->
    true;
ts_in_range(Ts, From, undefined) when is_integer(From) ->
    Ts >= From;
ts_in_range(Ts, undefined, To) when is_integer(To) ->
    Ts =< To;
ts_in_range(Ts, From, To) when is_integer(From), is_integer(To) ->
    Ts >= From andalso Ts =< To;
ts_in_range(_Ts, _, _) ->
    true.

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").

-spec test() -> _.

setup() ->
    stop_store_if_running(),
    {ok, _} = bb_products_store:start_link(),
    ok.

cleanup(_) ->
    stop_store_if_running(),
    ok.

stop_store_if_running() ->
    try
        gen_server:stop(bb_products_store)
    catch
        _:_ ->
            ok
    end.

-spec all_test_() -> _.
all_test_() ->
    %% Отдельный жизненный цикл store на каждый кейс — без накопления данных.
    {foreach, fun setup/0, fun cleanup/1, [
        fun(_) -> ?_test(append_and_list_one_snapshot()) end,
        fun(_) -> ?_test(append_two_sorted_by_ts()) end,
        fun(_) -> ?_test(filter_by_product_id()) end,
        fun(_) -> ?_test(filter_by_time_range()) end,
        fun(_) -> ?_test(filter_by_time_from_only()) end,
        fun(_) -> ?_test(filter_by_time_to_only()) end,
        fun(_) -> ?_test(filter_id_and_time_intersection()) end,
        fun(_) -> ?_test(filter_nonexistent_product_id()) end
    ]}.

%%------------------------------------------------------------------------------
append_and_list_one_snapshot() ->
    Items = [{1, 9.99}, {2, 19.99}],
    ok = bb_products_store:append_snapshot(Items),
    {ok, Rows} = bb_products_store:list_snapshots(#{}),
    ?assertEqual(1, length(Rows)),
    [{Ts, Got}] = Rows,
    ?assert(is_integer(Ts)),
    ?assertEqual(Items, Got).

append_two_sorted_by_ts() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{1, 2.0}]),
    {ok, Rows} = bb_products_store:list_snapshots(#{}),
    ?assertEqual(2, length(Rows)),
    [{Ts1, _}, {Ts2, _}] = Rows,
    ?assert(Ts1 =< Ts2).

filter_by_product_id() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}, {2, 2.0}]),
    ok = bb_products_store:append_snapshot([{2, 3.0}]),
    {ok, Rows} = bb_products_store:list_snapshots(#{product_id => 2}),
    ?assertEqual(2, length(Rows)),
    lists:foreach(
        fun({_Ts, Items}) ->
            ?assertEqual(1, length(Items)),
            ?assertMatch([{2, _}], Items)
        end,
        Rows
    ).

filter_by_time_range() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{1, 2.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{1, 3.0}]),
    {ok, All} = bb_products_store:list_snapshots(#{}),
    ?assertEqual(3, length(All)),
    [{Ts1, _}, {Ts2, _}, {Ts3, _}] = All,
    {ok, Mid} = bb_products_store:list_snapshots(#{from_ts => Ts2, to_ts => Ts2}),
    ?assertEqual(1, length(Mid)),
    ?assertMatch([{Ts2, _}], Mid),
    {ok, FirstTwo} = bb_products_store:list_snapshots(#{from_ts => Ts1, to_ts => Ts2}),
    ?assertEqual(2, length(FirstTwo)),
    {ok, None} = bb_products_store:list_snapshots(#{from_ts => Ts3 + 1, to_ts => Ts3 + 2}),
    ?assertEqual(0, length(None)).

filter_by_time_from_only() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{1, 2.0}]),
    {ok, All} = bb_products_store:list_snapshots(#{}),
    [{Ts1, _}, {Ts2, _}] = All,
    {ok, FromSecond} = bb_products_store:list_snapshots(#{from_ts => Ts2}),
    ?assertEqual(1, length(FromSecond)),
    ?assertMatch([{Ts2, _}], FromSecond),
    {ok, FromFirst} = bb_products_store:list_snapshots(#{from_ts => Ts1}),
    ?assertEqual(2, length(FromFirst)).

filter_by_time_to_only() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{1, 2.0}]),
    {ok, All} = bb_products_store:list_snapshots(#{}),
    [{Ts1, _}, {Ts2, _}] = All,
    {ok, UpToFirst} = bb_products_store:list_snapshots(#{to_ts => Ts1}),
    ?assertEqual(1, length(UpToFirst)),
    ?assertMatch([{Ts1, _}], UpToFirst),
    {ok, UpToSecond} = bb_products_store:list_snapshots(#{to_ts => Ts2}),
    ?assertEqual(2, length(UpToSecond)).

filter_id_and_time_intersection() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}, {2, 2.0}]),
    timer:sleep(5),
    ok = bb_products_store:append_snapshot([{2, 3.0}]),
    {ok, All} = bb_products_store:list_snapshots(#{}),
    [{Ts1, _} | _] = All,
    {ok, Rows} = bb_products_store:list_snapshots(#{
        from_ts => Ts1, to_ts => Ts1, product_id => 2
    }),
    ?assertEqual(1, length(Rows)),
    [{Ts1, Items}] = Rows,
    ?assertEqual([{2, 2.0}], Items).

filter_nonexistent_product_id() ->
    ok = bb_products_store:append_snapshot([{1, 1.0}, {2, 2.0}]),
    {ok, Rows} = bb_products_store:list_snapshots(#{product_id => 999}),
    ?assertEqual([], Rows).

-endif.
