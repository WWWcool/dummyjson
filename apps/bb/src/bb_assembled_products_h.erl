-module(bb_assembled_products_h).

-behaviour(cowboy_handler).

-export([init/2]).

-spec init(cowboy_req:req(), State) ->
    {ok, cowboy_req:req(), State}
when
    State :: term().
init(Req0, State) ->
    case cowboy_req:method(Req0) of
        <<"GET">> ->
            Qs = cowboy_req:parse_qs(Req0),
            case assembled_opts(Qs) of
                {ok, Opts} ->
                    {ok, Rows} = bb_products_store:list_snapshots(Opts),
                    Body = jsx:encode(rows_to_json(Rows)),
                    Req = cowboy_req:reply(
                        200,
                        #{<<"content-type">> => <<"application/json; charset=utf-8">>},
                        Body,
                        Req0
                    ),
                    {ok, Req, State};
                {error, Reason} ->
                    Req = cowboy_req:reply(
                        400,
                        #{<<"content-type">> => <<"application/json; charset=utf-8">>},
                        jsx:encode(#{<<"error">> => atom_to_binary(Reason, utf8)}),
                        Req0
                    ),
                    {ok, Req, State}
            end;
        _ ->
            Req = cowboy_req:reply(
                405,
                #{<<"allow">> => <<"GET">>},
                <<>>,
                Req0
            ),
            {ok, Req, State}
    end.

assembled_opts(Qs) ->
    try
        Opts0 = #{},
        Opts1 = maybe_date(
            proplists:get_value(<<"start_date">>, Qs),
            fun date_to_start_micros/1,
            from_ts,
            Opts0
        ),
        Opts2 = maybe_date(
            proplists:get_value(<<"end_date">>, Qs),
            fun date_to_end_micros_exclusive/1,
            to_ts,
            Opts1
        ),
        Opts3 = maybe_id(proplists:get_value(<<"id">>, Qs), Opts2),
        {ok, Opts3}
    catch
        throw:{assemble_error, R} ->
            {error, R}
    end.

maybe_date(undefined, _Conv, _Key, Opts) ->
    Opts;
maybe_date(Bin, Conv, Key, Opts) when is_binary(Bin) ->
    case parse_ymd(Bin) of
        {ok, Date} ->
            Opts#{Key => Conv(Date)};
        {error, _} ->
            throw({assemble_error, bad_date})
    end.

maybe_id(undefined, Opts) ->
    Opts;
maybe_id(Bin, Opts) when is_binary(Bin) ->
    try Opts#{product_id => binary_to_integer(Bin)} of
        O ->
            O
    catch
        error:badarg ->
            throw({assemble_error, bad_id})
    end.

parse_ymd(B) when is_binary(B) ->
    case binary:split(B, <<"-">>, [global]) of
        [Yb, Mb, Db] when byte_size(Yb) =:= 4, byte_size(Mb) =:= 2, byte_size(Db) =:= 2 ->
            try
                {ok, {binary_to_integer(Yb), binary_to_integer(Mb), binary_to_integer(Db)}}
            catch
                _:_ ->
                    {error, bad_date}
            end;
        _ ->
            {error, bad_date}
    end.

epoch_gregorian_seconds() ->
    calendar:datetime_to_gregorian_seconds({{1970, 1, 1}, {0, 0, 0}}).

utc_datetime_to_epoch_micros({Date, Time}) ->
    G = calendar:datetime_to_gregorian_seconds({Date, Time}),
    (G - epoch_gregorian_seconds()) * 1_000_000.

date_to_start_micros(Date) ->
    utc_datetime_to_epoch_micros({Date, {0, 0, 0}}).

date_to_end_micros_exclusive(Date) ->
    Days = calendar:date_to_gregorian_days(Date),
    Next = calendar:gregorian_days_to_date(Days + 1),
    date_to_start_micros(Next) - 1.

rows_to_json([]) ->
    [];
rows_to_json(Rows) ->
    Sorted = lists:keysort(1, Rows),
    [{micros_to_iso8601z(Ts), snapshot_items_json(Items)} || {Ts, Items} <- Sorted].

snapshot_items_json(Items) ->
    lists:map(
        fun({Id, Price}) ->
            #{<<"id">> => integer_to_binary(Id), <<"price">> => Price}
        end,
        Items
    ).

micros_to_iso8601z(Micros) when is_integer(Micros) ->
    list_to_binary(
        calendar:system_time_to_rfc3339(
            Micros div 1000,
            [{unit, millisecond}, {offset, "Z"}]
        )
    ).
