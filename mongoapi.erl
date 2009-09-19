-module(mongoapi, [DB]).
% -export([save/1,findOne/2,findOne/1,find/1,find/2,find/3,find/4, update/2, insert/1, eval/1]).
-compile(export_all).
-include("erlmongo.hrl").

name([_|_] = Collection) ->
	name(list_to_binary(Collection));
name(<<_/binary>> = Collection) ->
	<<DB/binary, ".", Collection/binary>>;
name(Collection) when is_atom(Collection) ->
	name(atom_to_binary(Collection, latin1)).

remove(Rec, Selector) when is_tuple(Rec) ->
	mongodb:exec_delete(name(element(1,Rec)), #delete{selector = mongodb:encoderec_selector(Rec, Selector)});
remove(Col, Selector) ->
	mongodb:exec_delete(name(Col), #delete{selector = mongodb:encode(Selector)}).

	
save(Collection, [_|_] = L) ->
	% io:format("Save on ~p~n", [L]),
	case lists:keysearch(<<"_id">>, 1, L) of
		false ->
			OID = mongodb:create_id(),
			case mongodb:exec_insert(name(Collection), #insert{documents = mongodb:encode([{<<"_id">>, {oid, OID}}|L])}) of
				ok ->
					{oid, OID};
				R ->
					R
			end;
		{value, {_, OID}} ->
			case mongodb:exec_update(name(Collection), #update{selector = mongodb:encode([{<<"_id">>, OID}]), document = mongodb:encode(L)}) of
				ok ->
					{oid, OID};
				R ->
					R
			end
	end.
save(Rec) -> 
	case element(3, Rec) of
		undefined ->
			OID = mongodb:create_id(),
			case mongodb:exec_insert(name(element(1,Rec)), #insert{documents = mongodb:encoderec(setelement(3, Rec, {oid, OID}))}) of
				ok ->
					OID;
				R ->
					R
			end;
		OID ->
			case mongodb:exec_update(name(element(1,Rec)), #update{selector = mongodb:encode([{<<"_id">>, OID}]), document = mongodb:encoderec(Rec)}) of
				ok ->
					OID;
				R ->
					R
			end
	end.



update(Collection, [_|_] = Selector, [_|_] = Doc, true) ->
	update(Collection, [_|_] = Selector, [_|_] = Doc, 1);
update(Collection, [_|_] = Selector, [_|_] = Doc, false) ->
	update(Collection, [_|_] = Selector, [_|_] = Doc, 0);
update(Collection, [_|_] = Selector, [_|_] = Doc, Upsert) ->
	mongodb:exec_update(name(Collection), #update{selector = mongodb:encode(Selector), document = mongodb:encode(Doc), upsert = Upsert}).
% Examples: 
%  update([{#mydoc.name, "docname"}], #mydoc{name = "different name"}, 1)
%  update([{#mydoc.name, "docname"}], #mydoc{i = {inc, 1}}, 1)
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {push, "lamer"}}, 1)
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {pushAll, ["dumbass","jackass"]}}, 1)
%  update([{#mydoc.name, "docname"}], #mydoc{tags = {pullAll, ["dumbass","jackass"]}}, 1)
%  and so on. 
% modifier list: inc, set, push, pushAll, pop, pull, pullAll
update(Selector, Rec, true) ->
	update(Selector, Rec, 1);
update(Selector, Rec, false) ->
	update(Selector, Rec, 0);
update(Selector, Rec, Upsert) ->
	mongodb:exec_update(name(element(1,Rec)), #update{selector = mongodb:encoderec_selector(Rec, Selector), upsert = Upsert,
	 												  document = mongodb:encoderec(Rec)}).

insert(Col, [_|_] = L) ->
	mongodb:exec_insert(name(Col), #insert{documents = mongodb:encode(L)}).
insert(Rec) ->
	mongodb:exec_insert(name(element(1,Rec)), #insert{documents = mongodb:encoderec(Rec)}).
	
batchInsert(Col, [[_|_]|_] = LRecs) ->
	DocBin = lists:foldl(fun(L, Bin) -> <<Bin/binary, (mongodb:encode(L))/binary>> end, <<>>, LRecs),
	mongodb:exec_insert(name(Col), #insert{documents = DocBin}).
batchInsert(LRecs) ->
	[FRec|_] = LRecs,
	DocBin = lists:foldl(fun(Rec, Bin) -> <<Bin/binary, (mongodb:encoderec(Rec))/binary>> end, <<>>, LRecs),
	mongodb:exec_insert(name(element(1,FRec)), #insert{documents = DocBin}).

	
% Advanced queries:
%  Documents with even i:            Mong:find(#mydoc{i = {mod, 2, 0}}, undefined, 0,0).
%  Documents with i larger than 2:   Mong:find(#mydoc{i = {gt, 2}}, undefined, 0,0).
%  Documents with i between 2 and 5: Mong:find(#mydoc{i = {in, {gt, 2}, {lt, 5}}}, undefined, 0,0).
%  in example:     Mong:find(#mydoc{tags = {in, [2,3,4]}}, undefined, 0,0).
%  exists example: Mong:find(#mydoc{tags = {exists, false}}, undefined, 0,0).
%  Advanced query options: gt,lt,gte,lte, ne, in, nin, all, size, exists

%% returns proplists
findOne(Col, [_|_] = Query, [_|_] = Selector) ->
    case find(Col, Query, Selector, 0, 1) of
        [Res] -> Res;
        [] -> []
    end.  

%% returns proplists
findOne(Col, []) ->
	case find(Col, [], undefined, 0, 1) of
        [Res] -> Res;
        [] -> []
    end;

%% returns proplists
findOne(Col, [_|_] = Query) when is_tuple(Col) == false ->
	case find(Col, Query, undefined, 0, 1) of
        [Res] -> Res;
        [] -> []
    end;

%% returns record
findOne(Query, Selector) when is_tuple(Query) ->
	case find(Query, Selector, 0, 1) of
        [Res] -> Res;
        [] -> []
    end.

%% returns record
findOne(Query) when is_tuple(Query) ->
    case find(Query, undefined, 0, 1) of
        [Res] -> Res;
        [] -> []
    end.

find(Col, #search{} = Q) ->
	find(Col, Q#search.criteria, Q#search.field_selector, Q#search.nskip, Q#search.ndocs).
find(#search{} = Q) ->
	find(Q#search.criteria, Q#search.field_selector, Q#search.nskip, Q#search.ndocs).
	
find(Col, Query, Selector, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, criteria = mongodb:encode(Query), field_selector = mongodb:encode(Selector)},
	case mongodb:exec_find(name(Col), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		timeout -> 
		   timeout;
        Res ->
            case mongodb:decode(Res) of
                Final=[[_|_]|_] ->
                    Final;
                Final=[_|_] ->
                    [Final];
                Else -> Else
            end
	end.

find(Query, Selector, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, criteria = mongodb:encoderec(Query), field_selector = mongodb:encoderec_selector(Query, Selector)},
	case mongodb:exec_find(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		timeout -> 
		    timeout;
        Res ->
            mongodb:decoderec(Query, Res)
	end.

% opts: [reverse, {sort, SortyBy}, explain, {hint, Hint}, snapshot]
% SortBy: {key, Val} or a list of keyval tuples -> {i,1}  (1 = ascending, -1 = descending)
% Hint: key
findOpt(Col, Query, Selector, Opts, From, Limit) ->
	find(Col, translateopts(undefined, Opts,[{<<"query">>, Query}]), Selector, From, Limit).
% SortBy examples: {#mydoc.name, 1}, [{#mydoc.i, 1},{#mydoc.name,-1}]
% Hint example: #mydoc.name
findOpt(Query, Selector, Opts, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, field_selector = mongodb:encoderec_selector(Query, Selector),
	             criteria = mongodb:encode(translateopts(Query, Opts,[{<<"query">>, {bson, mongodb:encoderec(Query)}}]))}, 
	case mongodb:exec_find(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		<<>> ->
			[];
		timeout -> 
		   timeout;
		Result ->
			% If opt is explain, it will crash
			try mongodb:decoderec(Query, Result) of
				Res ->
					Res
			catch
				error:_ ->
					mongodb:decode(Result)
			end
	end.
findOpt(Col, #search{} = Q, Opts) ->
	findOpt(Col, Q#search.criteria, Q#search.field_selector, Opts, Q#search.nskip, Q#search.ndocs).
findOpt(#search{} = Q, Opts) ->
	findOpt(Q#search.criteria, Q#search.field_selector, Opts, Q#search.nskip, Q#search.ndocs).
	
cursor(Query, Selector, Opts, From, Limit) ->
	Quer = #search{ndocs = Limit, nskip = From, field_selector = mongodb:encoderec_selector(Query, Selector),
	             criteria = mongodb:encode(translateopts(Query, Opts,[{<<"query">>, {bson, mongodb:encoderec(Query)}}])),
				 opts = ?QUER_OPT_CURSOR},
	case mongodb:exec_cursor(name(element(1,Query)), Quer) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, mongodb:decoderec(Query, Result)};
		{Cursor, Result} ->
			{ok, Cursor, mongodb:decoderec(Query, Result)}
	end.
getMore(Rec, Cursor) ->
	case mongodb:exec_getmore(Cursor) of
		not_connected ->
			not_connected;
		{done, <<>>} ->
			{done, []};
		{done, Result} ->
			{done, mongodb:decoderec(Rec, Result)};
		{ok, Result} ->
			{ok, mongodb:decoderec(Rec, Result)}
	end.
closeCursor(Cur) ->
	Cur#cursor.pid ! {cleanup},
	ok.
	
translateopts(undefined, [{sort, [_|_] = SortBy}|T], L) ->
	translateopts(undefined, T, [{<<"orderby">>, SortBy}|L]);
translateopts(undefined, [{sort, {Key,Val}}|T], L) ->
	translateopts(undefined, T, [{<<"orderby">>, [{Key,Val}]}|L]);
translateopts(Rec, [{sort, [_|_] = SortBy}|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, {bson, mongodb:encoderec_selector(Rec, SortBy)}}|L]);
translateopts(Rec, [{sort, {Key,Val}}|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, {bson, mongodb:encoderec_selector(Rec, [{Key,Val}])}}|L]);
translateopts(Rec, [reverse|T], L) ->
	translateopts(Rec, T, [{<<"orderby">>, [{<<"$natural">>, -1}]}|L]);	
translateopts(Rec, [explain|T], L) ->
	translateopts(Rec, T, [{<<"$explain">>, true}|L]);
translateopts(Rec, [snapshot|T], L) ->
	translateopts(Rec, T, [{<<"$snapshot">>, true}|L]);	
translateopts(undefined, [hint, Hint|T], L) ->
	translateopts(undefined, T, [{<<"$hint">>, [{Hint, 1}]}|L]);
translateopts(Rec, [hint, Hint|T], L) ->
	translateopts(Rec, T, [{<<"$hint">>, {bson, mongodb:encoderec_selector([Hint])}}|L]);
translateopts(_, [], L) ->
	L.


check_index_vals([]) -> ok;
check_index_vals([{_K, V}|T]) when V =:= 1;V =:= -1 -> %% index must be either 1 or -1
    check_index_vals(T);
check_index_vals(_) -> 
    error.

gen_index_name([], Acc) -> 
    list_to_binary(string:join(lists:reverse(Acc), "_"));
gen_index_name([{K, V}|T], Acc) when is_binary(K) ->
    gen_index_name([{binary_to_list(K), V}|T],  Acc);
gen_index_name([{K, V}|T], Acc) ->
    gen_index_name(T, [integer_to_list(V), K  |Acc]).

ensureIndex(Collection, KeyVals) when is_binary(Collection); is_list(Collection) ->
    ok = check_index_vals(KeyVals),
    NS = name(Collection),
    KeyCache = {{ NS, KeyVals }, true},
    Bin = mongodb:encode([{plaintext, <<"ns">>, NS},
                          {<<"key">>, {bson, mongodb:encode(KeyVals)}},
                          {plaintext, <<"name">>, gen_index_name(KeyVals,[])}
                          ]),
    mongodb:ensure_index(<<DB/binary, ".system.indexes">>,  Bin, KeyCache);

ensureIndex(Rec, Keys) ->
    NS = name(element(1,Rec)),
    KeyCache = {{ NS, Keys }, true},
	Bin = mongodb:encode([{plaintext, <<"name">>, mongodb:gen_keyname(Rec, Keys)}, 
			              {plaintext, <<"ns">>, NS}, 
			              {<<"key">>, {bson, mongodb:encoderec_selector(Rec, Keys)}}]),
	mongodb:ensure_index(<<DB/binary, ".system.indexes">>,Bin, KeyCache).


	
deleteIndexes([_|_] = Collection) ->
	deleteIndexes(list_to_binary(Collection));
deleteIndexes(<<_/binary>> = Collection) ->
    mongodb:clear_index_cache(),
	mongodb:exec_cmd(DB, [{plaintext, <<"deleteIndexes">>, Collection}, {plaintext, <<"index">>, <<"*">>}]).

deleteIndex(Rec, Key) ->
    mongodb:clear_index_cache(),
	mongodb:exec_cmd(DB,[{plaintext, <<"deleteIndexes">>, atom_to_binary(element(1,Rec), latin1)},
				  		 {plaintext, <<"index">>, mongodb:gen_keyname(Rec,Key)}]).

count([_|_] = Col) ->
	count(list_to_binary(Col));
count(<<_/binary>> = Col) ->
	case mongodb:exec_cmd(DB, [{plaintext, <<"count">>, Col}, {plaintext, <<"ns">>, DB}]) of
		[{<<"n">>, Val}|_] ->
			round(Val);
		_ ->
			false
	end;
count(Col) when is_tuple(Col) ->
	count(atom_to_binary(element(1,Col),latin1)).
	

addUser(U, P) when is_binary(U) ->
	addUser(binary_to_list(U),P);
addUser(U, P) when is_binary(P) ->
	addUser(U,binary_to_list(P));
addUser(Username, Password) ->
	save(<<"system.users">>, [{<<"user">>, Username},
							  {<<"pwd">>, bin_to_hexstr(erlang:md5(Username ++ ":mongo:" ++ Password))}]).
	
bin_to_hexstr(Bin) ->
	lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).
	
% Runs $cmd. Parameters can be just a string it will be converted into {string,1}
runCmd({_,_} = T) ->
	runCmd([T]);
runCmd([{_,_}|_] = L) ->
	mongodb:exec_cmd(DB, L);
runCmd([_|_] = L) ->
	runCmd([{L,1}]);
runCmd(<<_/binary>> = L) ->
	runCmd(binary_to_list(L)).

%% run js code on the server, watch out for syntax error because it may crash the server!
%% DO NOT FEED IN USER GENERATED JS CODE!
eval(Javascript) when is_binary(Javascript) ->
   mongodb:exec_cmd(DB, [{plaintext, <<"$eval">>, Javascript}]);
eval(Javascript) when is_list(Javascript) ->	
   mongodb:exec_cmd(DB, [{plaintext, <<"$eval">>, list_to_binary(Javascript)}]).

repairDatabase() ->
	runCmd([{"repairDatabase", 1}]).
dropDatabase() ->
    mongodb:clear_index_cache(),
	runCmd([{"dropDatabase", 1}]).
cloneDatabase(From) when is_list(From); is_binary(From) ->
	runCmd([{"clone", From}]).

dropCollection(C) when is_tuple(C) ->
	dropCollection(atom_to_binary(element(1,C),latin1));
dropCollection(Collection) ->
    mongodb:clear_index_cache(),
	runCmd([{"drop", Collection}]).

createCollection(Name) ->
	createCollection(Name, []).
% Options: idindex, noidindex, capped, {size, MaxSizeBytes}, {max, MaxElements}
createCollection(Name, L) when is_tuple(Name) ->
	createCollection(atom_to_binary(element(1,Name), latin1), L);
createCollection(Name, L) ->
	runCmd([{<<"create">>, Name}] ++ translatecolopts(L, [])).

translatecolopts([idindex|T], O) ->
	translatecolopts(T, [{<<"autoIndexId">>, true}|O]);	
translatecolopts([noidindex|T], O) ->
	translatecolopts(T, [{<<"autoIndexId">>, false}|O]);	
translatecolopts([capped|T], O) ->
	translatecolopts(T, [{<<"capped">>, true}|O]);	
translatecolopts([{size, MaxSize}|T], O) ->
	translatecolopts(T, [{<<"size">>, MaxSize}|O]);
translatecolopts([{max, Max}|T], O) ->
	translatecolopts(T, [{<<"max">>, Max}|O]);
translatecolopts([], O) ->
	O.
	
setProfilingLevel(L) when is_integer(L) ->
	case true of
		_ when L > 0 ->
			createCollection(<<"system.profile">>, [capped, {size, 131072}]);
		_ when L >= 0, L =< 2 ->
			true
	end,
	runCmd([{"profile", L}]).
getProfilingLevel() ->
	runCmd([{"profile", -1}]).
	
	