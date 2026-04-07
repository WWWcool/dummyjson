-module(bb_set_time_h).

-behaviour(cowboy_handler).

-export([init/2]).

-spec init(cowboy_req:req(), State) ->
    {ok, cowboy_req:req(), State}
when
    State :: term().
init(Req0, State) ->
    case cowboy_req:method(Req0) of
        <<"POST">> ->
            case cowboy_req:read_body(Req0) of
                {ok, Body, Req1} ->
                    case parse_interval_body(Body) of
                        {ok, Ms} ->
                            reply_set(Req1, State, Ms);
                        {error, Reason} ->
                            reply_bad(Req1, State, Reason)
                    end;
                {more, _, Req1} ->
                    reply_bad(Req1, State, body_too_large)
            end;
        _ ->
            Req = cowboy_req:reply(
                405,
                #{<<"allow">> => <<"POST">>},
                <<>>,
                Req0
            ),
            {ok, Req, State}
    end.

reply_set(Req, State, Ms) ->
    case bb_dummyjson:set_fetch_interval(Ms, true) of
        ok ->
            Body = jsx:encode(#{<<"ok">> => true, <<"interval_ms">> => Ms}),
            Req2 = cowboy_req:reply(
                200,
                #{<<"content-type">> => <<"application/json; charset=utf-8">>},
                Body,
                Req
            ),
            {ok, Req2, State};
        {error, invalid_interval} ->
            reply_bad(Req, State, invalid_interval);
        {error, invalid_option} ->
            reply_bad(Req, State, invalid_option)
    end.

reply_bad(Req, State, Reason) ->
    Req2 = cowboy_req:reply(
        400,
        #{<<"content-type">> => <<"application/json; charset=utf-8">>},
        jsx:encode(#{<<"error">> => atom_to_binary(Reason, utf8)}),
        Req
    ),
    {ok, Req2, State}.

bin_to_positive_int(B) ->
    try
        N = binary_to_integer(B),
        case N > 0 of
            true -> {ok, N};
            false -> {error, invalid_interval}
        end
    catch
        error:badarg ->
            {error, bad_ms}
    end.

parse_interval_body(<<>>) ->
    {error, empty_body};
parse_interval_body(Bin) when is_binary(Bin) ->
    try jsx:decode(Bin, [return_maps]) of
        Map when is_map(Map) ->
            normalize_ms_value(maps:get(<<"interval_ms">>, Map, undefined));
        _ ->
            {error, invalid_json}
    catch
        _:_ ->
            {error, invalid_json}
    end.

normalize_ms_value(undefined) ->
    {error, missing_interval_ms};
normalize_ms_value(N) when is_integer(N), N > 0 ->
    {ok, N};
normalize_ms_value(B) when is_binary(B) ->
    bin_to_positive_int(B);
normalize_ms_value(_) ->
    {error, invalid_interval}.
