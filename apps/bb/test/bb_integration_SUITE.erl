-module(bb_integration_SUITE).

-compile([nowarn_missing_spec]).

-include_lib("common_test/include/ct.hrl").

-export([all/0, groups/0]).
-export([init_per_suite/1, end_per_suite/1]).
-export([init_per_testcase/2, end_per_testcase/2]).
-export([
    set_time_spacing_increases/1,
    assembled_products_date_and_id_filters/1
]).

all() ->
    [{group, integration}].

groups() ->
    [
        {integration, [], [
            assembled_products_date_and_id_filters,
            set_time_spacing_increases
        ]}
    ].

init_per_suite(Config0) ->
    {ok, _} = application:ensure_all_started(inets),
    {ok, _} = application:ensure_all_started(ssl),
    RefPrice = fetch_dummyjson_price(6),
    Port = 18080 + erlang:phash2({make_ref(), erlang:monotonic_time()}, 8000),
    _ = application:load(bb),
    ok = application:set_env(bb, http_port, Port),
    ok = application:set_env(bb, fetch_interval_ms, 500),
    {ok, _} = application:ensure_all_started(bb),
    [
        {base_url, "http://127.0.0.1:" ++ integer_to_list(Port)},
        {ref_price_6, RefPrice},
        {port, Port}
        | Config0
    ].

end_per_suite(Config) ->
    ok = write_integration_ci_summary(Config),
    _ = application:stop(bb),
    ok.

init_per_testcase(_TC, Config) ->
    ok = bb_dummyjson:set_fetch_interval(500, true),
    Config.

end_per_testcase(_TC, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% POST /set-time увеличивает интервал → зазоры между снимками должны вырасти.
set_time_spacing_increases(Config) ->
    Base = ?config(base_url, Config),
    %% Ждём начальных снимков с дефолтным интервалом 500мс
    ok = poll_until(fun() -> snapshot_count(Base) >= 6 end, timer:seconds(20)),
    {ok, Body0} = http_get(Base, "/assembled-products"),
    Keys0 = sorted_object_keys(Body0),
    GapBefore = lists:min(gaps_ms(take_last(5, Keys0))),
    true = GapBefore >= 200,
    true = GapBefore =< 12_000,
    %% Увеличиваем интервал до 8с и ждём новые снимки
    {200, _} = http_post_json(Base, "/set-time", #{<<"interval_ms">> => 8000}),
    ok = poll_until(fun() -> new_key_count(Base, Keys0) >= 3 end, timer:seconds(75)),
    {ok, Body1} = http_get(Base, "/assembled-products"),
    Keys1 = sorted_object_keys(Body1),
    NewKeys = [K || K <- Keys1, not lists:member(K, Keys0)],
    true = length(NewKeys) >= 3,
    %% Зазоры между новыми снимками должны быть заметно больше старых
    GapAfter = lists:min(gaps_ms(NewKeys)),
    true = GapAfter > GapBefore + 1500,
    true = GapAfter >= 4000,
    ok.

%% Проверяем фильтры GET /assembled-products: по дате (start_date/end_date) и по id.
assembled_products_date_and_id_filters(Config) ->
    Base = ?config(base_url, Config),
    RefPrice = ?config(ref_price_6, Config),
    ok = poll_until(fun() -> snapshot_count(Base) >= 10 end, timer:seconds(20)),

    %% 1. Без фильтров — есть снимки
    {ok, Body} = http_get(Base, "/assembled-products"),
    Keys = sorted_object_keys(Body),
    true = length(Keys) >= 8,

    %% 2. Фильтр по сегодняшней дате — все ключи попадают в этот день
    First = hd(Keys),
    Date10 = binary:part(First, 0, 10),
    Q =
        "/assembled-products?start_date=" ++
            binary_to_list(Date10) ++
            "&end_date=" ++
            binary_to_list(Date10),
    {ok, FBody} = http_get(Base, Q),
    FKeys = sorted_object_keys(FBody),
    true = length(FKeys) >= 1,
    lists:foreach(
        fun(K) -> Date10 = binary:part(K, 0, 10) end,
        FKeys
    ),

    %% 3. Фильтр по вчерашней дате — пусто (снимки только сегодняшние)
    Yesterday = yesterday_date_bin(),
    QEmpty =
        "/assembled-products?start_date=" ++
            binary_to_list(Yesterday) ++
            "&end_date=" ++
            binary_to_list(Yesterday),
    {ok, EmptyBody} = http_get(Base, QEmpty),
    true = (json_decode(EmptyBody) =:= []),

    %% 4. Фильтр по id=6 — в каждом снимке ровно один товар с правильной ценой
    {ok, IdBody} = http_get(Base, "/assembled-products?id=6"),
    IdMap = json_decode(IdBody),
    maps:foreach(
        fun(_TsKey, Items) when is_list(Items) ->
            true = length(Items) =:= 1,
            [Row] = Items,
            <<"6">> = maps:get(<<"id">>, Row),
            true = maps:get(<<"price">>, Row) == RefPrice
        end,
        IdMap
    ),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

poll_until(Fun, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    poll_loop(Fun, Deadline).

poll_loop(Fun, Deadline) ->
    case erlang:monotonic_time(millisecond) > Deadline of
        true ->
            ct:fail(poll_timeout);
        false ->
            case Fun() of
                true ->
                    ok;
                false ->
                    timer:sleep(400),
                    poll_loop(Fun, Deadline)
            end
    end.

snapshot_count(Base) ->
    case http_get(Base, "/assembled-products") of
        {ok, Body} -> length(sorted_object_keys(Body));
        _Err -> 0
    end.

new_key_count(Base, OldKeys) ->
    case http_get(Base, "/assembled-products") of
        {ok, Body} ->
            Keys = sorted_object_keys(Body),
            length([K || K <- Keys, not lists:member(K, OldKeys)]);
        _Err ->
            0
    end.

http_get(Base, Pathq) ->
    Url = Base ++ Pathq,
    case httpc:request(get, {Url, []}, [{timeout, timer:seconds(60)}], []) of
        {ok, {{_HTTP, 200, _Reason}, _Hdrs, Body}} ->
            {ok, Body};
        Other ->
            {error, Other}
    end.

http_post_json(Base, Path, BodyMap) when is_map(BodyMap) ->
    Url = Base ++ Path,
    Body = jsx:encode(BodyMap),
    Hdrs = [{"content-type", "application/json"}],
    Req = {Url, Hdrs, "application/json", Body},
    case httpc:request(post, Req, [{timeout, timer:seconds(60)}], []) of
        {ok, {{_HTTP, Code, _}, _Hdrs, Resp}} ->
            {Code, Resp};
        Other ->
            {error, Other}
    end.

sorted_object_keys(Body) ->
    lists:sort(maps:keys(json_decode(Body))).

json_decode(Body) ->
    jsx:decode(iolist_to_binary([Body]), [return_maps]).

take_last(N, L) ->
    case length(L) =< N of
        true -> L;
        false -> lists:nthtail(length(L) - N, L)
    end.

gaps_ms(Keys) when length(Keys) < 2 ->
    [];
gaps_ms(Keys) ->
    MsList = [rfc3339_to_ms(K) || K <- Keys],
    pairs_deltas(MsList).

pairs_deltas([A, B | T]) ->
    [B - A | pairs_deltas([B | T])];
pairs_deltas(_) ->
    [].

rfc3339_to_ms(B) when is_binary(B) ->
    calendar:rfc3339_to_system_time(binary_to_list(B), [{unit, millisecond}]).

yesterday_date_bin() ->
    {Date, _} = calendar:universal_time(),
    Days = calendar:date_to_gregorian_days(Date) - 1,
    {Y, M, D} = calendar:gregorian_days_to_date(Days),
    iolist_to_binary(io_lib:format("~4..0B-~2..0B-~2..0B", [Y, M, D])).

fetch_dummyjson_price(ProductId) ->
    Url = "https://dummyjson.com/products/" ++ integer_to_list(ProductId),
    case httpc:request(get, {Url, []}, [{timeout, timer:seconds(30)}], []) of
        {ok, {{_HTTP, 200, _}, _Hdrs, Resp}} ->
            Map = json_decode(Resp),
            maps:get(<<"price">>, Map);
        Other ->
            ct:fail({fetch_dummyjson_price_failed, ProductId, Other})
    end.

%% В CI дописываем в $GITHUB_STEP_SUMMARY (сводка job в GitHub Actions).
%% Локально: `bb_integration_SUITE_data/ci_test_summary.md` (рядом с data_dir Common Test).
write_integration_ci_summary(Config) ->
    case proplists:get_value(base_url, Config) of
        undefined ->
            ok;
        Base ->
            MdBin = build_integration_summary_md(Config, Base),
            case ci_summary_target(Config) of
                {gha, Path} ->
                    ok = file:write_file(Path, [MdBin, <<"\n">>], [append]);
                {local, Path} ->
                    ok = filelib:ensure_dir(Path),
                    ok = file:write_file(Path, MdBin)
            end
    end.

ci_summary_target(Config) ->
    case os:getenv("GITHUB_STEP_SUMMARY") of
        false ->
            DataDir = proplists:get_value(data_dir, Config),
            Path =
                case DataDir of
                    undefined ->
                        {ok, Cwd} = file:get_cwd(),
                        filename:join([Cwd, "_build", "ci_test_summary.md"]);
                    D ->
                        filename:join([D, "ci_test_summary.md"])
                end,
            {local, Path};
        "" ->
            ci_summary_target_fallback_local(Config);
        Path ->
            {gha, Path}
    end.

ci_summary_target_fallback_local(Config) ->
    DataDir = proplists:get_value(data_dir, Config),
    P =
        case DataDir of
            undefined ->
                {ok, Cwd} = file:get_cwd(),
                filename:join([Cwd, "_build", "ci_test_summary.md"]);
            D ->
                filename:join([D, "ci_test_summary.md"])
        end,
    {local, P}.

build_integration_summary_md(Config, Base) ->
    RefPrice = proplists:get_value(ref_price_6, Config),
    Port = proplists:get_value(port, Config),
    PortBin = scalar_bin(Port, fun(Pn) -> integer_to_list(Pn) end),
    RefBin = scalar_bin(RefPrice, fun(R) -> lists:flatten(io_lib:format("~p", [R])) end),
    FetchBin = fetch_interval_env_bin(),
    case http_get(Base, "/assembled-products") of
        {ok, Body} ->
            BodyBin = iolist_to_binary([Body]),
            Map = json_decode(Body),
            Keys = lists:sort(maps:keys(Map)),
            iolist_to_binary([
                <<"### bb_integration_SUITE - application state after tests\n\n">>,
                <<"#### Runtime\n\n">>,
                <<"| | |\n|--|--|\n">>,
                md_kv_row(<<"HTTP port">>, PortBin),
                md_kv_row(<<"bb fetch_interval_ms (env)">>, FetchBin),
                md_kv_row(<<"dummyjson product 6 reference price">>, RefBin),
                md_kv_row(<<"GET /assembled-products body size">>, size_bytes_bin(byte_size(BodyBin))),
                <<"\n#### Snapshot store\n\n">>,
                <<"| | |\n|--|--|\n">>,
                snapshot_store_table_rows(Map, Keys, RefPrice),
                <<"\n#### Filtered GET (sanity)\n\n">>,
                filter_checks_section(Base, Keys),
                <<"\n#### RFC3339 snapshot keys (up to last 20)\n\n">>,
                keys_code_block(Keys, 20),
                <<"\n#### JSON sample: first / middle / last snapshot\n\n">>,
                multi_sample_json_blocks(Map, Keys, 4000),
                <<"\n">>
            ]);
        Err ->
            ErrBin = list_to_binary(lists:flatten(io_lib:format("~p", [Err]))),
            iolist_to_binary([
                <<"### bb_integration_SUITE - application state after tests\n\n">>,
                <<"**GET /assembled-products failed:** `">>,
                ErrBin,
                <<"`\n">>
            ])
    end.

scalar_bin(undefined, _) ->
    <<"-">>;
scalar_bin(V, Listify) ->
    list_to_binary(Listify(V)).

fetch_interval_env_bin() ->
    case application:get_env(bb, fetch_interval_ms) of
        {ok, N} when is_integer(N) ->
            list_to_binary(integer_to_list(N));
        _ ->
            <<"-">>
    end.

size_bytes_bin(N) ->
    list_to_binary(integer_to_list(N)).

md_kv_row(Label, Value) ->
    <<"| ", Label/binary, " | `", Value/binary, "` |\n">>.

snapshot_store_table_rows(Map, Keys, RefPrice) ->
    N = length(Keys),
    RangeBin = keys_range_bin(Keys),
    CntBin = list_to_binary(integer_to_list(N)),
    GapsBin = gap_stats_bin(Keys),
    ItemsBin = item_stats_bin(item_counts_per_snapshot(Map, Keys)),
    UqBin = unique_ids_summary_bin(Map),
    [
        md_kv_row(<<"snapshots count">>, CntBin),
        md_kv_row(<<"RFC3339 key range">>, RangeBin),
        md_kv_row(<<"gaps between consecutive snapshots">>, GapsBin),
        md_kv_row(<<"line items per snapshot">>, ItemsBin),
        md_kv_row(<<"unique product ids (all snapshots)">>, UqBin),
        md_kv_row(
            <<"prices for id=6 (distinct across snapshots)">>,
            id6_prices_summary_bin(Map, RefPrice)
        )
    ].

keys_range_bin([]) ->
    <<"-">>;
keys_range_bin([K]) ->
    K;
keys_range_bin(Keys) ->
    K0 = hd(Keys),
    Kn = lists:last(Keys),
    <<K0/binary, " ... ", Kn/binary>>.

gap_stats_bin(Keys) when length(Keys) < 2 ->
    <<"-">>;
gap_stats_bin(Keys) ->
    G0 = gaps_ms(Keys),
    G = lists:sort(G0),
    Min = hd(G),
    Max = lists:last(G),
    Med = integer_median(G),
    Last = lists:last(G0),
    list_to_binary(
        lists:flatten(
            io_lib:format("min ~p ms, median ~p ms, max ~p ms, last delta ~p ms", [Min, Med, Max, Last])
        )
    ).

item_counts_per_snapshot(_Map, []) ->
    [];
item_counts_per_snapshot(Map, Keys) ->
    [length(maps:get(K, Map)) || K <- Keys].

item_stats_bin([]) ->
    <<"-">>;
item_stats_bin(Counts) ->
    S = lists:sort(Counts),
    Min = hd(S),
    Max = lists:last(S),
    Med = integer_median(S),
    list_to_binary(
        lists:flatten(io_lib:format("min ~p, median ~p, max ~p line items", [Min, Med, Max]))
    ).

integer_median(Sorted) when Sorted =/= [] ->
    L = length(Sorted),
    Mid = L div 2,
    case L rem 2 of
        1 -> lists:nth(Mid + 1, Sorted);
        0 -> (lists:nth(Mid, Sorted) + lists:nth(Mid + 1, Sorted)) div 2
    end.

collect_unique_ids(Map) ->
    sets:to_list(
        maps:fold(
            fun(_, Items, Acc) ->
                lists:foldl(
                    fun(Row, A) -> sets:add_element(maps:get(<<"id">>, Row), A) end,
                    Acc,
                    Items
                )
            end,
            sets:new(),
            Map
        )
    ).

unique_ids_summary_bin(Map) ->
    Ids0 = collect_unique_ids(Map),
    Ids = lists:sort(
        fun(A, B) -> binary_to_integer(A) =< binary_to_integer(B) end,
        Ids0
    ),
    case Ids of
        [] ->
            <<"-">>;
        _ ->
            MaxShow = 45,
            {Shown, RestN} =
                case length(Ids) =< MaxShow of
                    true -> {Ids, 0};
                    false -> {lists:sublist(Ids, MaxShow), length(Ids) - MaxShow}
                end,
            Joined = join_binaries(Shown, <<", ">>),
            case RestN of
                0 ->
                    iolist_to_binary([
                        Joined,
                        <<" (">>,
                        list_to_binary(integer_to_list(length(Ids))),
                        <<" ids)">>
                    ]);
                _ ->
                    RestB = list_to_binary(integer_to_list(RestN)),
                    TotB = list_to_binary(integer_to_list(length(Ids))),
                    iolist_to_binary([
                        Joined,
                        <<" ... (+">>,
                        RestB,
                        <<" more), sorted, ">>,
                        TotB,
                        <<" ids total">>
                    ])
            end
    end.

join_binaries([], _Sep) ->
    <<>>;
join_binaries([B | Rest], Sep) ->
    lists:foldl(fun(X, Acc) -> <<Acc/binary, Sep/binary, X/binary>> end, B, Rest).

collect_prices_for_product_id(Map, IdBin) ->
    lists:usort(
        maps:fold(
            fun(_, Items, Acc) ->
                lists:foldl(
                    fun(Row, A) ->
                        case maps:get(<<"id">>, Row) of
                            IdBin -> [maps:get(<<"price">>, Row) | A];
                            _ -> A
                        end
                    end,
                    Acc,
                    Items
                )
            end,
            [],
            Map
        )
    ).

id6_prices_summary_bin(Map, RefPrice) ->
    case maps:size(Map) of
        0 ->
            <<"-">>;
        _ ->
            P = collect_prices_for_product_id(Map, <<"6">>),
            PB = list_to_binary(lists:flatten(io_lib:format("~p", [P]))),
            N = length(P),
            NB = list_to_binary(integer_to_list(N)),
            RefPart =
                case RefPrice of
                    undefined ->
                        <<>>;
                    R ->
                        case P =:= [R] of
                            true ->
                                <<"; suite ref price OK">>;
                            false ->
                                RB = list_to_binary(lists:flatten(io_lib:format("~p", [R]))),
                                iolist_to_binary([<<"; suite ref ">>, RB, <<"; got distinct ">>, PB])
                        end
                end,
            iolist_to_binary([
                <<"unique set ">>,
                PB,
                <<" (">>,
                NB,
                <<" distinct)">>,
                RefPart
            ])
    end.

filter_checks_section(_Base, []) ->
    <<"_no keys - skip_\n">>;
filter_checks_section(Base, Keys) ->
    First = hd(Keys),
    Date10 = binary:part(First, 0, 10),
    QT =
        "/assembled-products?start_date=" ++
            binary_to_list(Date10) ++
            "&end_date=" ++
            binary_to_list(Date10),
    Y = binary_to_list(yesterday_date_bin()),
    QY =
        "/assembled-products?start_date=" ++
            Y ++
            "&end_date=" ++
            Y,
    TodayN = filter_snapshot_count(Base, QT),
    YestN = filter_snapshot_count(Base, QY),
    IdN = filter_snapshot_count(Base, "/assembled-products?id=6"),
    iolist_to_binary([
        <<"- `GET ", (list_to_binary(QT))/binary, "` -> **">>,
        list_to_binary(integer_to_list(TodayN)),
        <<"** snapshots\n">>,
        <<"- `GET ", (list_to_binary(QY))/binary, "` -> **">>,
        list_to_binary(integer_to_list(YestN)),
        <<"** snapshots (expect 0 for UTC \"yesterday\" if all UTC today)\n">>,
        <<"- `GET /assembled-products?id=6` -> **">>,
        list_to_binary(integer_to_list(IdN)),
        <<"** snapshots\n">>
    ]).

filter_snapshot_count(Base, Pathq) ->
    case http_get(Base, Pathq) of
        {ok, B} ->
            case json_decode(B) of
                M when is_map(M) ->
                    length(maps:keys(M));
                [] ->
                    0;
                _ ->
                    -1
            end;
        _ ->
            -1
    end.

keys_code_block(Keys, Max) ->
    Take = take_last(min(Max, length(Keys)), Keys),
    Lines = [[K, <<"\n">>] || K <- Take],
    iolist_to_binary([<<"```text\n">>, Lines, <<"```\n">>]).

multi_sample_json_blocks(_Map, [], _Max) ->
    <<"_empty_\n">>;
multi_sample_json_blocks(Map, Keys, Max) ->
    Pick = sample_keys_first_mid_last(Keys),
    Parts = [
        [<<"`">>, K, <<"`\n```json\n">>, json_sample_block(Map, K, Max), <<"\n```\n\n">>]
     || K <- Pick
    ],
    iolist_to_binary(Parts).

sample_keys_first_mid_last([K]) ->
    [K];
sample_keys_first_mid_last(Keys) ->
    A = hd(Keys),
    Z = lists:last(Keys),
    Mid = lists:nth((length(Keys) + 1) div 2, Keys),
    uniq_in_order([A, Mid, Z]).

uniq_in_order(List) ->
    lists:reverse(
        lists:foldl(
            fun(X, Acc) ->
                case lists:member(X, Acc) of
                    true -> Acc;
                    false -> [X | Acc]
                end
            end,
            [],
            List
        )
    ).

json_sample_block(Map, K, Max) ->
    Sample = maps:get(K, Map),
    Enc = iolist_to_binary(jsx:encode(#{K => Sample})),
    truncate_binary(Enc, Max).

truncate_binary(Bin, Max) when byte_size(Bin) =< Max ->
    Bin;
truncate_binary(Bin, Max) ->
    Part = binary:part(Bin, 0, Max),
    <<Part/binary, "\n... (truncated for report)">>.
