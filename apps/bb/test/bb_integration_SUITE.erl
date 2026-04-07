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

end_per_suite(_Config) ->
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
