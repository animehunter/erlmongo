-module(mongodb).
-export([deser_prop/1,reload/0, print_info/0, start/0, stop/0, init/1, handle_call/3, 
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([connect/0, exec_cursor/2, exec_delete/2, exec_cmd/2, exec_insert/2, exec_find/2, 
         exec_update/2, exec_getmore/2, ensure_index/3,clear_index_cache/0,
         encoderec/1, encoderec_selector/2, gen_keyname/2, decoderec/2, encode/1, decode/1,
		 encode_findrec/1, gen_prop_keyname/2, create_id/0,
         singleServer/1, singleServer/0, masterSlave/2,masterMaster/2, replicaPairs/2]).
-export([dec2hex/2,hex2dec/2]).
-include("erlmongo.hrl").
% -define(RIN, record_info(fields, enctask)).

-include_lib("eunit/include/eunit.hrl").

% -compile(export_all).
-define(MONGO_PORT, 27017).
-define(RECONNECT_DELAY, 1000).
-define(QUERY_TIMEOUT, 4000).
-define(QUERY_TIMEOUT_TOLERANCE, 3800). %% QUERY_TIMEOUT - QUERY_TIMEOUT_TOLERANCE = MAX LATENCY between connection and client
-define(CONNECTION_TIMEOUT,15000).

-define(OP_REPLY, 1).
-define(OP_MSG, 1000).
-define(OP_UPDATE, 2001).
-define(OP_INSERT, 2002).
-define(OP_QUERY, 2004).
-define(OP_GET_MORE, 2005).
-define(OP_DELETE, 2006).
-define(OP_KILL_CURSORS, 2007).

-define(data_number,       1).         %%	double 	 
-define(data_string,       2).         %%	int32 cstring 	The int32 is the # bytes following (# of bytes in string + 1 for terminating NULL) 
-define(data_object,       3).         %%	bson_object 	 
-define(data_array,        4).         %%	bson_object 	See note on data_array 
-define(data_binary,       5).         %%	int32 byte byte[] 	The first int32 is the # of bytes following the byte subtype. Please see note on data_binary 
-define(data_undefined,    6).         %%	VOID 	Conceptually equivalent to Javascript undefined.  Deprecated. 
-define(data_oid,          7).         %%	byte[12] 	12 byte object id. 
-define(data_boolean,      8).         %%	byte 	legal values: 0x00 -> false, 0x01 -> true 
-define(data_date,         9).         %%	int64 	value: milliseconds since epoch (e.g. new Date.getTime()) 
-define(data_null,         10).        %%	VOID 	Mapped to Null in programming languages which have a Null value or type.  Conceptually equivalent to Javascript null. 
-define(data_regex,        11).        %%	cstring cstring 	first ctring is regex expression, second cstring are regex options See note on data_regex 
-define(data_ref,          12).        %%	int32 cstring byte[12] 	Deprecated.  Please use a subobject instead -- see page DB Ref. 
                                       %% The int32 is the length in bytes of the cstring. 
                                       %% The cstring is the namespace: full collection name. 
                                       %% The byte array is a 12 byte object id. See note on data_oid. 
-define(data_code,         13).        %%	int32 cstring 	The int32 is the # bytes following (# of bytes in string + 1 for terminating NULL) and then the code as cstring. data_code should be supported in BSON encoders/decoders, but has been deprecated in favor of data_code_w_scope 
-define(OMITTED,           14). 	 	 
-define(data_code_w_scope, 15).        %%	int32 int32 cstring bson_object 	The first int32 is the total # of bytes (size of cstring + size of bson_object + 8 for the two int32s). The second int 32 is the size of the cstring (# of bytes in string + 1 for terminating NULL). The cstring is the code. The bson_object is an object mapping identifiers to values, representing the scope in which the code should be evaluated. 
-define(data_int, 	       16).        %%	int32 	 
-define(data_timestamp,    17).        %%	int64 	Special internal type used by MongoDB replication and sharding. First 4 are a timestamp, next 4 are an incremented field.  Saving a zero value for data_timestamp has special semantics.
-define(data_long,         18).        %% int64 	64 bit integer 
-define(data_min_key,      -1).        %% VOID 	See Splitting Shards, http://www.mongodb.org/display/DOCS/Splitting+Shards
-define(data_max_key,      127).       %%	VOID 	See Splitting Shards, http://www.mongodb.org/display/DOCS/Splitting+Shards

-define(L(X), error_logger:format("~p~n", [X])).

-ifndef(IF).
-define(IF(Exp,T,F), (case (Exp) of true -> (T); false -> (F) end)).
-endif.

reload() ->
	gen_server:call(?MODULE, {reload_module}).
	% code:purge(?MODULE),
	% code:load_file(?MODULE),
	% spawn(fun() -> register() end).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).
	
% register() ->
% 	supervisor:start_child(supervisor, {?MODULE, {?MODULE, start, []}, permanent, 1000, worker, [?MODULE]}).
		
print_info() ->
	gen_server:cast(?MODULE, {print_info}).


% SPEED TEST
% loop(N) ->
% 	io:format("~p~n", [now()]),
% 	t(N, true),
% 	io:format("~p~n", [now()]).

% t(0, _) ->
% 	true;
% t(N, R) ->
% 	% encoderec(#mydoc{name = <<"IZ_RECORDA">>, i = 12}),
% 	% decoderec(#mydoc{}, R),
% 	ensureIndex(#mydoc{}, [{#mydoc.name, -1},{#mydoc.i, 1}]),
% 	t(N-1, R).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								API
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
connect() ->
	gen_server:cast(?MODULE, {start_connection}).
singleServer() ->
	gen_server:cast(?MODULE, {conninfo, {replicaPairs, {"localhost",?MONGO_PORT}, {"localhost",?MONGO_PORT}}}).
singleServer(Addr) ->
	[Addr,Port] = string:tokens(Addr,":"),
	% gen_server:cast(?MODULE, {conninfo, {single, {Addr,Port}}}).
	gen_server:cast(?MODULE, {conninfo, {replicaPairs, {Addr,Port}, {Addr,Port}}}).
masterSlave(Addr1, Addr2) ->
	[Addr1,Port1] = string:tokens(Addr1,":"),
	[Addr2,Port2] = string:tokens(Addr2,":"),
	gen_server:cast(?MODULE, {conninfo, {masterSlave, {Addr1,Port1}, {Addr2,Port2}}}).
masterMaster(Addr1,Addr2) ->
	[Addr1,Port1] = string:tokens(Addr1,":"),
	[Addr2,Port2] = string:tokens(Addr2,":"),
	gen_server:cast(?MODULE, {conninfo, {masterMaster, {Addr1,Port1}, {Addr2,Port2}}}).
replicaPairs(Addr1,Addr2) ->
	[Addr1,Port1] = string:tokens(Addr1,":"),
	[Addr2,Port2] = string:tokens(Addr2,":"),
	gen_server:cast(?MODULE, {conninfo, {replicaPairs, {Addr1,Port1}, {Addr2,Port2}}}).

exec_cursor(Col, Quer) ->
	case gen_server:call(?MODULE, {getread}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {find, self(), Col, Quer},
			receive
				{query_result, _Src, <<_ReqID:32/little, _RespTo:32/little, 1:32/little, 0:32, 
								 CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					% io:format("cursor ~p from ~p ndocs ~p, ressize ~p ~n", [_CursorID, _From, _NDocs, byte_size(Result)]),
					% io:format("~p~n", [Result]),
					case CursorID of
						0 ->
							{done, Result};
						_ ->
							PID = spawn_link(fun() -> cursorcleanup(true) end),
							PID ! {start, CursorID},
							{#cursor{id = CursorID, limit = Quer#search.ndocs, pid = PID}, Result}
					end
				after ?QUERY_TIMEOUT ->
					timeout
			end
	end.
exec_getmore(Col, C) ->
	case gen_server:call(?MODULE, {getread}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {getmore, self(), Col, C},
			receive
				{query_result, _Src, <<_ReqID:32/little, _RespTo:32/little, 1:32/little, 0:32, 
								 CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					% io:format("cursor ~p from ~p ndocs ~p, ressize ~p ~n", [_CursorID, _From, _NDocs, byte_size(Result)]),
					% io:format("~p~n", [Result]),
					case CursorID of
						0 ->
							C#cursor.pid ! {stop},
							{done, Result};
						_ ->
							{ok, Result}
					end
				after ?QUERY_TIMEOUT ->
					timeout
			end
	end.
exec_delete(Collection, D) ->
	case gen_server:call(?MODULE, {getwrite}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {delete, Collection, D}
	end,
	ok.
exec_find(Collection, Quer) ->
	case gen_server:call(?MODULE, {getread}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {find, self(), Collection, Quer},
			receive
				{query_result, _Src, <<_ReqID:32/little, _RespTo:32/little, 1:32/little, 0:32, 
								 _CursorID:64/little, _From:32/little, _NDocs:32/little, Result/binary>>} ->
					% io:format("cursor ~p from ~p ndocs ~p, ressize ~p ~n", [_CursorID, _From, _NDocs, byte_size(Result)]),
					% io:format("~p~n", [Result]),
					Result
				after ?QUERY_TIMEOUT ->
					timeout
			end
	end.
exec_insert(Collection, D) ->
	case gen_server:call(?MODULE, {getwrite}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {insert, Collection, D}
	end,
	ok.
exec_update(Collection, D) ->
	case gen_server:call(?MODULE, {getwrite}) of
		undefined ->
			not_connected;
		PID ->
			PID ! {update, Collection, D}
	end,
	ok.

%returns JSON | timeout | error
exec_cmd(DB, Cmd) ->
	Quer = #search{ndocs = 1, nskip = 0, criteria = mongodb:encode(Cmd)},
	case exec_find(<<DB/binary, ".$cmd">>, Quer) of
		undefined ->
			not_connected;
		<<>> ->
			[];
		timeout -> %timeout
		   timeout;
		Result ->
			mongodb:decode(Result)
	end.

ensure_index(Collection, D, Key) when is_binary(Collection) ->
    try gen_server:call(?MODULE, {ensure_index, Key}, 3000) of
        ok -> ok;
        no_index ->
            exec_insert(Collection, #insert{documents = D})
    catch
        _:{timeout,_} ->
            ok
    end.

clear_index_cache() ->
    gen_server:cast(?MODULE, {clear_indexcache}).

create_id() ->
	dec2hex(<<>>, gen_server:call(?MODULE, {create_oid})).
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%								IMPLEMENTATION
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
% read = connection used for reading (find) from mongo server
% write = connection used for writing (insert,update) to mongo server
%   single: same as replicaPairs (single server is always master and used for read and write)
%   masterSlave: read = slave, write = master
%   replicaPairs: read = write = master
%   masterMaster: read = master1, write = master2
% timer is reconnect timer if some connection is missing
% indexes is ensureIndex cache (an ets table).
-record(mngd, {read, write, conninfo, timer, hashed_hostn, oid_index = 1}).
-define(R2P(Record), rec2prop(Record, record_info(fields, mngd))).
-define(P2R(Prop), prop2rec(Prop, mngd, #mngd{}, record_info(fields, mngd))).	
	
handle_call({ensure_index, {Key, Val}}, _, P) ->
    case get(Key) of
       undefined ->
           put(Key,Val),
           {reply, no_index, P};
       _ ->
           {reply, ok, P}
    end;
handle_call({getread}, _, P) ->
	{reply, P#mngd.read, P};
handle_call({getwrite}, _, P) ->
	{reply, P#mngd.write, P};
handle_call({create_oid}, _, P) ->
	% {_,_,Micros} = now(),
	WC = element(1,erlang:statistics(wall_clock)) rem 16#ffffffff,
	<<_:20/binary,PID:2/binary,_/binary>> = term_to_binary(self()),
	N = P#mngd.oid_index rem 16#ffffff,
	{reply, <<WC:32, (P#mngd.hashed_hostn)/binary, PID/binary, N:24>>, P#mngd{oid_index = P#mngd.oid_index + 1}};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call({reload_module}, _, P) ->
	code:purge(?MODULE),
	code:load_file(?MODULE),
	{reply, ok, ?MODULE:deser_prop(?R2P(P))};
handle_call(_, _, P) ->
	{reply, ok, P}.

deser_prop(P) ->
	?P2R(P).

startcon(undefined, Type, Addr,Port) ->
	PID = spawn_link(fun() -> connection(true) end),
	PID ! {start, self(), Type, Addr, Port};
startcon(PID, _, _, _) ->
	PID.
	

handle_cast({clear_indexcache}, P) ->
    Ancestors = get('$ancestors'),
    InitialCall = get('$initial_call'),
    RandomSeed = get(random_seed),
	erase(),
    ?IF(Ancestors =:= undefined, ok, put('$ancestors', Ancestors)),
    ?IF(InitialCall =:= undefined, ok, put('$initial_call', InitialCall)),
    ?IF(RandomSeed =:= undefined, ok, put(random_seed, RandomSeed)),
	{noreply, P};
handle_cast({conninfo, Conn}, P) ->
	{noreply, P#mngd{conninfo = Conn}};
handle_cast({start_connection}, #mngd{conninfo = {masterMaster, {A1,P1},{A2,P2}}} = P)  ->
	case true of
		_ when P#mngd.read /= P#mngd.write, P#mngd.read /= undefined, P#mngd.write /= undefined ->
			Timer = ctimer(P#mngd.timer);
		_ when P#mngd.read == P#mngd.write, P#mngd.read /= undefined ->
			startcon(undefined, write, A2,P2),
			Timer = P#mngd.timer;
		_ ->
			startcon(P#mngd.read, read, A1,P1),
			startcon(P#mngd.write, write, A2,P2),
			Timer = P#mngd.timer
			% {noreply, P#mngd{read = startcon(P#mngd.read, A1,P1), write = startcon(P#mngd.write,A2,P2)}}
	end,
	{noreply, P#mngd{timer = Timer}};
handle_cast({start_connection}, #mngd{conninfo = {masterSlave, {A1,P1},{A2,P2}}} = P)  ->
	case true of
		% All ok.
		_ when P#mngd.read /= P#mngd.write, P#mngd.read /= undefined, P#mngd.write /= undefined ->
			Timer = ctimer(P#mngd.timer);
		% Read = write = master, try to connect to slave again
		_ when P#mngd.read == P#mngd.write, P#mngd.read /= undefined ->
			startcon(undefined, read, A2,P2),
			Timer = P#mngd.timer;
		% One or both of the connections is down
		_ ->
			startcon(P#mngd.read, read, A2,P2),
			startcon(P#mngd.write, write, A1,P1),
			Timer = P#mngd.timer
	end,
	{noreply, P#mngd{timer = Timer}};
handle_cast({start_connection}, #mngd{conninfo = {replicaPairs, {A1,P1},{A2,P2}}} = P)  ->
	case true of
		_ when P#mngd.read /= undefined, P#mngd.write == P#mngd.read ->
			{noreply, P#mngd{timer = ctimer(P#mngd.timer)}};
		_ ->
			startcon(undefined, ifmaster, A1,P1),
			startcon(undefined, ifmaster, A2,P2),
			{noreply, P}
	end;
handle_cast({print_info}, P) ->
	io:format("~p~n", [?R2P(P)]),
	{noreply, P};
handle_cast(_, P) ->
	{noreply, P}.

ctimer(undefined) ->
	undefined;
ctimer(T) ->
	timer:cancel(T),
	undefined.

timer(undefined) ->
	{ok, Timer} = timer:send_interval(?RECONNECT_DELAY, {reconnect}),
	Timer;
timer(T) ->
	T.

handle_info({conn_established, read, ConnProc}, P) ->
	{noreply, P#mngd{read = ConnProc}};
handle_info({conn_established, write, ConnProc}, P) ->
	{noreply, P#mngd{write = ConnProc}};
handle_info({reconnect}, P) ->
	handle_cast({start_connection}, P);
handle_info({'EXIT', PID, _Reason}, #mngd{conninfo = {replicaPairs, _, _}} = P) ->
	case true of
		_ when P#mngd.read == PID ->
			{noreply, P#mngd{read = undefined, write = undefined, timer = timer(P#mngd.timer)}};
		_ ->
			{noreply, P}
	end;
handle_info({'EXIT', PID, _Reason}, #mngd{conninfo = {masterSlave, _, _}} = P) ->
	case true of
		_ when P#mngd.read == PID, P#mngd.read /= P#mngd.write ->
			{noreply, P#mngd{read = P#mngd.write, timer = timer(P#mngd.timer)}};
		_ when P#mngd.read == PID ->
			{noreply, P#mngd{read = undefined, write = undefined, timer = timer(P#mngd.timer)}};
		_ when P#mngd.write == PID ->
			{noreply, P#mngd{write = undefined, timer = timer(P#mngd.timer)}};
		_ ->
			{noreply, P}
	end;
handle_info({'EXIT', PID, _Reason}, #mngd{conninfo = {masterMaster, _, _}} = P) ->
	case true of
		_ when P#mngd.read == PID, P#mngd.write == PID ->
			{noreply, P#mngd{read = undefined, write = undefined, timer = timer(P#mngd.timer)}};
		_ when P#mngd.read == PID ->
			{noreply, P#mngd{read = P#mngd.write, timer = timer(P#mngd.timer)}};
		_ when P#mngd.write == PID ->
			{noreply, P#mngd{write = P#mngd.read, timer = timer(P#mngd.timer)}};
		_ ->
			{noreply, P}
	end;
handle_info({query_result, Src, <<_:32/binary, Res/binary>>}, P) ->
	try mongodb:decode(Res) of
		[{<<"ismaster">>, 1}|_] when element(1,P#mngd.conninfo) =:= replicaPairs, P#mngd.read =:= undefined ->
			link(Src),
			{noreply, P#mngd{read = Src, write = Src}};
		_ ->
			Src ! {stop},
			{noreply, P}
	catch
		error:_ ->
			Src ! {stop},
			{noreply, P}
	end;
handle_info({query_result, Src, _}, P) ->
	Src ! {stop},
	{noreply, P};
handle_info(_X, P) -> 
	io:format("~p~n", [_X]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	% timer:send_interval(1000, {timeout}),
    {ok, HN} = inet:gethostname(),
    <<HashedHN:3/binary,_/binary>> = erlang:md5(HN),
	process_flag(trap_exit, true),
    {ok, #mngd{hashed_hostn = HashedHN}}.
	
% find_master([{A,P}|T]) ->
% 	Q = #search{ndocs = 1, nskip = 0, quer = mongodb:encode([{<<"ismaster">>, 1}])},
% 	
				


-record(ccd, {cursor = 0}).
% Just for cleanup
cursorcleanup(P) ->
	receive
		{stop} ->
			true;
		{cleanup} ->
			case gen_server:call(?MODULE, {get_conn}) of
				false ->
					false;
				PID ->
					PID ! {killcursor, #killc{cur_ids = <<(P#ccd.cursor):64/little>>}},
					true
			end;
		{'EXIT', _PID, _Why} ->
			self() ! {cleanup},
			cursorcleanup(P);
		{start, Cursor} ->
			process_flag(trap_exit, true),
			cursorcleanup(#ccd{cursor = Cursor})
	end.


make_timestamp32({_,S,MS}) ->
    A = (S rem 100)*1000000, %final digit
    A + MS.

remove_old_timestamps() ->
    T = now(),
    lists:foreach(fun({K={timestamp,_},{_,TimeStarted}}) ->
                       ?IF(timer:now_diff(T, TimeStarted) > ?QUERY_TIMEOUT_TOLERANCE,
                            erase(K), ok);
                       (_) -> ok
                  end, get()).

-record(con, {sock, source, buffer = <<>>, state = free}).
% Waiting for request
connection(true) ->
	connection(#con{});
connection(#con{state = free, sock=S} = P) ->
  case S of
   undefined -> ok;
   _ ->
      inet:setopts(S,[{active,once}])
  end,
	receive
		{find, Source, Collection, Query} ->
            TimeStarted = now(),
            TimeStamp = make_timestamp32(TimeStarted),
            put({timestamp,TimeStamp}, {Source,TimeStarted}),
			QBin = constr_query(Query, Collection, TimeStamp),
			ok = gen_tcp:send(P#con.sock, QBin),
			connection(P#con{state = waiting, source = Source});
		{insert, Collection, Doc} ->
			Bin = constr_insert(Doc, Collection),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P);
		{update, Collection, Doc} ->
			Bin = constr_update(Doc, Collection),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P);
		{delete, Col, D} ->
			Bin = constr_delete(D, Col),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P);
		{getmore, Source, Col, C} ->
            TimeStarted = now(),
            TimeStamp = make_timestamp32(TimeStarted),
            put({timestamp,TimeStamp}, {Source,TimeStarted}),
			Bin = constr_getmore(C, Col, TimeStamp),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P#con{state = waiting, source = Source});
		{killcursor, C} ->
			Bin = constr_killcursors(C),
			ok = gen_tcp:send(P#con.sock, Bin),
			connection(P);
		{tcp, _, _Bin} ->
			connection(P);
		{stop} ->
			true;
		{start, Source, Type, IP, Port} ->
			{A1,A2,A3} = now(),
		    random:seed(A1, A2, A3),
			{ok, Sock} = gen_tcp:connect(IP, Port, [binary, {delay_send, true}, {sndbuf, 8388608},{recbuf, 8388608}, {packet, 0}, {active, once}, {keepalive, true}]),
			case Type of
				ifmaster ->
					self() ! {find, Source, <<"admin.$cmd">>, #search{nskip = 0, ndocs = 1, criteria = mongodb:encode([{<<"ismaster">>, 1}])}};
				_ ->
					Source ! {conn_established, Type, self()}
			end,
			connection(#con{sock = Sock});
		{tcp_closed, _} ->
			exit(stop)
	end;
% waiting for response
connection(#con{sock=S} = P) ->
  inet:setopts(S,[{active,once}]),
	receive
		{tcp, _, Bin} ->
			<<Size:32/little, Packet/binary>> = <<(P#con.buffer)/binary, Bin/binary>>,
			% io:format("Received size ~p~n", [Size]),
			% io:format("Received size ~p ~p~n", [Size, byte_size(Packet)]),
			case Size of
				 _ when Size == byte_size(Packet) + 4 ->
                    <<_:32/little, Resp:32/little, _/binary>> = Packet,
                    Key = {timestamp,Resp},
                    case get(Key) of
                        {Source, TimeStarted}  -> 
                            ?IF(timer:now_diff(now(), TimeStarted) =< ?QUERY_TIMEOUT_TOLERANCE*1000, %convert millisec to microseconds
                                 Source ! {query_result, self(), Packet},
                                ok),
                            erase(Key),
                            remove_old_timestamps();
                        _ -> ok
                    end,
					%% P#con.source ! {query_result, self(), Packet},
					connection(P#con{state = free, buffer = <<>>});
				_ ->
					connection(P#con{buffer = <<(P#con.buffer)/binary, Bin/binary>>})
			end;
		{stop} ->
			true;
		{tcp_closed, _} ->
			exit(stop)
		after ?CONNECTION_TIMEOUT ->
			exit(stop)
	end.

constr_header(Len, ID, RespTo, OP) ->
	<<(Len+16):32/little, ID:32/little, RespTo:32/little, OP:32/little>>.

constr_update(U, Name) ->
	Update = <<0:32, Name/binary, 0:8, 
	           (U#update.upsert):32/little, (U#update.selector)/binary, (U#update.document)/binary>>,
	Header = constr_header(byte_size(Update), random:uniform(4000000000), 0, ?OP_UPDATE),
	<<Header/binary, Update/binary>>.

constr_insert(U, Name) ->
	Insert = <<0:32, Name/binary, 0:8, (U#insert.documents)/binary>>,
	Header = constr_header(byte_size(Insert), random:uniform(4000000000), 0, ?OP_INSERT),
	<<Header/binary, Insert/binary>>.

constr_query(U, Name, TimeStarted) ->
	Query = <<(U#search.opts):32/little, Name/binary, 0:8, (U#search.nskip):32/little, (U#search.ndocs):32/little, 
	  		  (U#search.criteria)/binary, (U#search.field_selector)/binary>>,
	Header = constr_header(byte_size(Query), TimeStarted, 0, ?OP_QUERY),
	<<Header/binary,Query/binary>>.

constr_getmore(U, Name, TimeStarted) ->
	GetMore = <<0:32, Name/binary, 0:8, (U#cursor.limit):32/little, (U#cursor.id):62/little>>,
	Header = constr_header(byte_size(GetMore), TimeStarted, 0, ?OP_GET_MORE),
	<<Header/binary, GetMore/binary>>.

constr_delete(U, Name) ->
	Delete = <<0:32, Name/binary, 0:8, 0:32, (U#delete.selector)/binary>>,
	Header = constr_header(byte_size(Delete), random:uniform(4000000000), 0, ?OP_DELETE),
	<<Header/binary, Delete/binary>>.
	
constr_killcursors(U) ->
	Kill = <<0:32, (byte_size(U#killc.cur_ids) div 8):32, (U#killc.cur_ids)/binary>>,
	Header = constr_header(byte_size(Kill), random:uniform(4000000000), 0, ?OP_KILL_CURSORS),
	<<Header/binary, Kill/binary>>.




%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
%						BSON encoding/decoding 
%	most of it taken and modified from the mongo-erlang-driver project by Elias Torres
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

encoderec(Rec) ->
    [_|Fields] = element(element(2, Rec), ?RECTABLE),
    encoderec(<<>>, deep, Rec, Fields, 3, <<>>).
encode_findrec(Rec) ->
    [_|Fields] = element(element(2, Rec), ?RECTABLE),
    encoderec(<<>>, flat, Rec, Fields, 3, <<>>).

encoderec(NameRec, Type, Rec, [{FieldName, _RecIndex}|T], N, Bin) ->
    case element(N, Rec) of
        undefined ->
            encoderec(NameRec, Type, Rec, T, N+1, Bin);
        SubRec when Type == flat ->
            [_|SubFields] = element(element(2, SubRec), ?RECTABLE),
            case NameRec of
                <<>> ->
                    Dom = atom_to_binary(FieldName, latin1);
                _ ->
                    Dom = <<NameRec/binary, ".", (atom_to_binary(FieldName, latin1))/binary>>
            end,
            encoderec(NameRec, Type, Rec, T, N+1, <<Bin/binary, (encoderec(Dom, flat, SubRec, SubFields, 3, <<>>))/binary>>);
        Val ->
            encoderec(NameRec, Type, Rec, T, N+1, <<Bin/binary, (encode_element({atom_to_binary(FieldName, latin1), {bson, encoderec(Val)}}))/binary>>)
    end;
encoderec(NameRec, Type, Rec, [FieldName|T], N, Bin) ->
    case element(N, Rec) of
        undefined ->
            encoderec(NameRec, Type,Rec, T, N+1, Bin);
        Val ->
            case FieldName of
                docid ->
                    encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({<<"_id">>, {oid, Val}}))/binary>>);
                _ ->
                    case NameRec of
                        <<>> ->
                            Dom = atom_to_binary(FieldName, latin1);
                        _ ->
                            Dom = <<NameRec/binary, ".", (atom_to_binary(FieldName, latin1))/binary>>
                    end,
                    encoderec(NameRec, Type,Rec, T, N+1, <<Bin/binary, (encode_element({Dom, Val}))/binary>>)
            end
    end;
encoderec(<<>>,_,_, [], _, Bin) ->
    <<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>;
encoderec(_,_,_, [], _, Bin) ->
    % <<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>.
    Bin.

encoderec_selector(_, undefined) ->
	<<>>;
encoderec_selector(_, <<>>) ->
	<<>>;
encoderec_selector(Rec, SelectorList) ->
    RecName = element(1, Rec),
    RecIndex = element(2, Rec),
	Fields = [RecName|element(RecIndex, ?RECTABLE)],
	encoderec_selector(SelectorList, list_to_tuple(Fields), <<>>).

% SelectorList is either a list of indexes in the record tuple, or a list of {TupleIndex, TupleVal}. Use the index to get the name
% from the list of names.
encoderec_selector([{FieldIndex, Val}|Fields], FieldNames, Bin) ->
    FieldName = element(FieldIndex, FieldNames),
	case FieldName of
		docid ->
			encoderec_selector(Fields, FieldNames, <<Bin/binary, (encode_element({<<"_id">>, Val}))/binary>>);
		_ ->
			encoderec_selector(Fields, FieldNames, <<Bin/binary, (encode_element({atom_to_binary(FieldName,latin1), Val}))/binary>>)
	end;
encoderec_selector([FieldIndex|Fields], FieldNames, Bin) ->
    FieldName = element(FieldIndex, FieldNames),
	case FieldName of
		docid ->
			encoderec_selector(Fields, FieldNames, <<Bin/binary, (encode_element({<<"_id">>, 1}))/binary>>);
		_ ->
			encoderec_selector(Fields, FieldNames, <<Bin/binary, (encode_element({atom_to_binary(FieldName,latin1), 1}))/binary>>)
	end;
encoderec_selector([], _, Bin) ->
	<<(byte_size(Bin)+5):32/little, Bin/binary, 0:8>>.
	
gen_prop_keyname([{[_|_] = KeyName, KeyVal}|T], Bin) ->
    gen_prop_keyname([{list_to_binary(KeyName), KeyVal}|T], Bin);
gen_prop_keyname([{KeyName, KeyVal}|T], Bin) ->
    case is_integer(KeyVal) of
        true ->
            Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
        false ->
            Add = <<>>
    end,
    gen_prop_keyname(T, <<Bin/binary, KeyName/binary, "_", Add/binary>>);
gen_prop_keyname([], B) ->
    B.

gen_keyname(Rec, Keys) ->
	[_|Fields] = element(element(2, Rec), ?RECTABLE),
	gen_keyname(<<>>,Keys, Fields, 3, not_done).


gen_keyname(<<"_", Name/binary>>,A, B, C, not_done) ->
	gen_keyname(Name, A,B, C, done);
gen_keyname(Name,[{KeyIndex, KeyVal}|Keys], [FieldName|Fields], KeyIndex, not_done) ->
	case is_integer(KeyVal) of
		true ->
			Add = <<(list_to_binary(integer_to_list(KeyVal)))/binary>>;
		false ->
			Add = <<>>
	end,
	gen_keyname( <<Name/binary, "_", (atom_to_binary(FieldName, latin1))/binary, "_", Add/binary>>,Keys, Fields, KeyIndex+1, not_done);
gen_keyname(Name,Keys, [_|Fields], KeyIndex, not_done) ->
	gen_keyname(Name,Keys, Fields, KeyIndex+1, not_done);
gen_keyname(Name, _,_, _, done) ->
   Name.
	

decoderec(Rec, <<>>) ->
	% Rec;
	erlang:make_tuple(tuple_size(Rec), undefined, [{1, element(1,Rec)}, {2, element(2,Rec)}]);
decoderec(Rec, Bin) ->
	[_|Fields] = element(element(2, Rec), ?RECTABLE),
	decode_records([], Bin, tuple_size(Rec), element(1,Rec), element(2, Rec), Fields).

decode_records(RecList, <<_ObjSize:32/little, Bin/binary>>, TupleSize, Name, TabIndex, Fields) ->
	{FieldList, Remain} = rec_field_list(Bin, [{1, Name},{2, TabIndex}], 3, Fields, not_done),
	% io:format("~p~n", [FieldList]),
	NewRec = erlang:make_tuple(TupleSize, undefined, FieldList),
	decode_records([NewRec|RecList], Remain, TupleSize, Name, TabIndex, Fields);
decode_records(R, <<>>, _, _, _, _) ->
	lists:reverse(R).

get_fields(RecVals, Fields, Bin) ->
    case rec_field_list(Bin, RecVals, 3, Fields, not_done) of
        {again, SoFar, Rem} ->
            get_fields(SoFar, Fields, Rem);
        Res ->
            Res
    end.

rec_field_list(<<0:8, Rem/binary>>, RecVals, _, _, not_done) ->
    Ret = {RecVals,Rem},
	rec_field_list(done, done,done,done,{done,Ret});
rec_field_list(<<Type:8, Bin/binary>>,RecVals, _, [], not_done) ->
    {_Name, ValRem} = decode_cstring(Bin, <<>>),
    {_Value, Remain} = decode_value(Type, ValRem),
    Ret = {again, RecVals, Remain},
    rec_field_list(done, done,done,done,{done,Ret});
rec_field_list(<<Type:8, Bin/binary>>, RecVals, N, [Field|Fields], not_done) ->
	{Name, ValRem} = decode_cstring(Bin, <<>>, not_done),
	% io:format("~p ~p~n", [Name, Field]),
	{Value, Remain} = decode_value(ValRem, Type),
    FieldBinName =
        case Field of
            docid ->
                <<"_id">>;
            {Fn, _} ->
                atom_to_binary(Fn, latin1);
            Fn ->
                atom_to_binary(Fn, latin1)
        end,
	case FieldBinName  of
		Name ->
            case Field of
                {RecName, RecIndex} ->
                    <<LRecSize:32/little, RecObj/binary>> = ValRem,
                    RecSize = LRecSize - 4,
                    <<RecBin:RecSize/binary, Remain/binary>> = RecObj,
                    [_|RecFields] = element(RecIndex, ?RECTABLE),
                    [Value] = decode_records([], <<LRecSize:32/little, RecBin/binary>>, length(element(RecIndex, ?RECTABLE))+1, 
                                                    RecName, RecIndex, RecFields),
                    rec_field_list(Remain, [{N, Value}|RecVals], N+1, Fields, not_done);
                _ ->
                    {Value, Remain} = decode_value(Type, ValRem),
                    case Value of
                        {oid, V} ->
                            rec_field_list(Remain, [{N, V}|RecVals], N+1, Fields, not_done);
                        _ ->
                            rec_field_list(Remain, [{N, Value}|RecVals], N+1, Fields, not_done)
                    end
            end;
		_ ->
			rec_field_list(<<Type:8, Bin/binary>>, RecVals, N+1, Fields, not_done)
	end;
rec_field_list(_, _, _, _, Ret) ->
   Ret.
	
dec2hex(N, <<I:8,Rem/binary>>) ->
    dec2hex(<<N/binary, (hex0((I band 16#f0) bsr 4)):8, (hex0((I band 16#0f))):8>>, Rem);
dec2hex(N,<<>>) ->
    N.

hex2dec(N,<<A:8,B:8,Rem/binary>>) ->
    hex2dec(<<N/binary, ((dec0(A) bsl 4) + dec0(B)):8>>, Rem);
hex2dec(N,<<>>) ->
    N.

dec0($a) -> 10;
dec0($b) -> 11;
dec0($c) -> 12;
dec0($d) -> 13;
dec0($e) -> 14;
dec0($f) -> 15;
dec0(X) ->  X - $0.

hex0(10) -> $a;
hex0(11) -> $b;
hex0(12) -> $c;
hex0(13) -> $d;
hex0(14) -> $e;
hex0(15) -> $f;
hex0(I) ->  $0 + I.


encode(undefined) ->
	<<>>;
encode(<<>>) ->
	<<>>;
encode(Items) ->
	Bin = lists:foldl(fun(Item, B) -> <<B/binary, (encode_element(Item))/binary>> end, <<>>, Items),
    <<(byte_size(Bin)+5):32/little-signed, Bin/binary, 0:8>>.

encode_element({[_|_] = Name, Val}) ->
	encode_element({list_to_binary(Name),Val});
encode_element({Name, [{_,_}|_] = Items}) ->
	Binary = encode(Items),
	<<3, Name/binary, 0, Binary/binary>>;
encode_element({Name, [_|_] = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({Name, <<_/binary>> = Value}) ->
	ValueEncoded = encode_cstring(Value),
	<<2, Name/binary, 0, (byte_size(ValueEncoded)):32/little-signed, ValueEncoded/binary>>;
encode_element({plaintext, Name, Val}) -> % exists for performance reasons.
	<<2, Name/binary, 0, (byte_size(Val)+1):32/little-signed, Val/binary, 0>>;
encode_element({Name, true}) ->
	<<8, Name/binary, 0, 1:8>>;
encode_element({Name, false}) ->
	<<8, Name/binary, 0, 0:8>>;
% <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8>>
encode_element({Name, {oid, OID}}) ->
	%   	FirstReversed = lists:reverse(binary_to_list(First)),
	%   	SecondReversed = lists:reverse(binary_to_list(Second)),
	% OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
	<<7, Name/binary, 0, (hex2dec(<<>>, OID))/binary>>;
% list of lists = array
encode_element({Name, {array, Items}}) ->
  	% ItemNames = [integer_to_list(Index) || Index <- lists:seq(0, length(Items)-1)],
  	% ItemList = lists:zip(ItemNames, Items),
  	% Binary = encode(ItemList),
  	<<4, Name/binary, 0, (encarray([], Items, 0))/binary>>;
encode_element({Name, {bson, Bin}}) ->
	<<3, Name/binary, 0, Bin/binary>>;
encode_element({Name, {inc, Val}}) ->
	encode_element({<<"$inc">>, [{Name, Val}]});
encode_element({Name, {set, Val}}) ->
	encode_element({<<"$set">>, [{Name, Val}]});
encode_element({Name, {push, Val}}) ->
	encode_element({<<"$push">>, [{Name, Val}]});
encode_element({Name, {pushAll, Val}}) ->
	encode_element({<<"$pushAll">>, [{Name, {array, Val}}]});
encode_element({Name, {pop, Val}}) ->
	encode_element({<<"$pop">>, [{Name, Val}]});
encode_element({Name, {pull, Val}}) ->
	encode_element({<<"$pull">>, [{Name, Val}]});
encode_element({Name, {pullAll, Val}}) ->
	encode_element({<<"$pullAll">>, [{Name, {array, Val}}]});
encode_element({Name, {gt, Val}}) ->
	encode_element({Name, [{<<"$gt">>, Val}]});
encode_element({Name, {lt, Val}}) ->
	encode_element({Name, [{<<"$lt">>, Val}]});
encode_element({Name, {lte, Val}}) ->
	encode_element({Name, [{<<"$lte">>, Val}]});
encode_element({Name, {gte, Val}}) ->
	encode_element({Name, [{<<"$gte">>, Val}]});
encode_element({Name, {ne, Val}}) ->
	encode_element({Name, [{<<"$ne">>, Val}]});
encode_element({Name, {in, {FE,FV},{TE,TV}}}) ->
	encode_element({Name, [{<<"$", (atom_to_binary(FE,latin1))/binary>>, FV},
						   {<<"$", (atom_to_binary(TE,latin1))/binary>>, TV}]});
encode_element({Name, {in, Val}}) ->
	encode_element({Name, [{<<"$in">>, {array, Val}}]});
encode_element({Name, {nin, Val}}) ->
	encode_element({Name, [{<<"$nin">>, {array, Val}}]});
encode_element({Name, {mod, By,Rem}}) ->
	encode_element({Name, [{<<"$mod">>, {array, [By,Rem]}}]});
encode_element({Name, {all, Val}}) ->
	encode_element({Name, [{<<"$all">>, {array, Val}}]});
encode_element({Name, {size, Val}}) ->
	encode_element({Name, [{<<"$size">>, Val}]});
encode_element({Name, {exists, Val}}) ->
	encode_element({Name, [{<<"$exists">>, Val}]});
encode_element({Name, {binary, 2, Data}}) ->
  	<<5, Name/binary, 0, (size(Data)+4):32/little-signed, 2:8, (size(Data)):32/little-signed, Data/binary>>;
encode_element({Name, {binary, SubType, Data}}) ->
  	StringEncoded = encode_cstring(Name),
  	<<5, StringEncoded/binary, (size(Data)):32/little-signed, SubType:8, Data/binary>>;
encode_element({Name, {oid, <<First:8/little-binary-unit:8, Second:4/little-binary-unit:8>>}}) ->
  	FirstReversed = lists:reverse(binary_to_list(First)),
  	SecondReversed = lists:reverse(binary_to_list(Second)),
	OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
	<<7, Name/binary, 0, OID/binary>>;
encode_element({Name, Value}) when is_integer(Value),Value >= -2147483647,Value =< 2147483647 ->
    <<16, Name/binary, 0, Value:32/little-signed>>;
encode_element({Name, Value}) when is_integer(Value) ->
	<<18, Name/binary, 0, Value:64/little-signed>>;
encode_element({Name, Value}) when is_float(Value) ->
	<<1, (Name)/binary, 0, Value:64/little-signed-float>>;
encode_element({Name, {bson, Bin}}) ->
	<<3, Name/binary, 0, Bin/binary>>;
encode_element({Name, []}) ->
	<<3, Name/binary, 0, (encode([]))/binary>>;	
encode_element({Name, {obj, []}}) ->
	<<3, Name/binary, 0, (encode([]))/binary>>;	
encode_element({Name, {MegaSecs, Secs, MicroSecs}}) when  is_integer(MegaSecs),is_integer(Secs),is_integer(MicroSecs) ->
  Unix = MegaSecs * 1000000 + Secs,
  Millis = Unix * 1000 + trunc(MicroSecs / 1000),
  <<9, Name/binary, 0, Millis:64/little-signed>>;
encode_element({Name, null}) ->
  <<10, Name/binary, 0>>;
encode_element({Name, undefined}) ->
  <<10, Name/binary, 0>>;
encode_element({Name, {regex, Expression, Flags}}) ->
  ExpressionEncoded = encode_cstring(Expression),
  FlagsEncoded = encode_cstring(Flags),
  <<11, Name/binary, 0, ExpressionEncoded/binary, FlagsEncoded/binary>>;
encode_element({Name, {ref, Collection, OID}}) ->
  CollectionEncoded = encode_cstring(Collection),
  <<12, Name/binary, 0, (byte_size(CollectionEncoded)):32/little-signed, CollectionEncoded/binary, (hex2dec(<<>>, OID))/binary>>;
encode_element({Name, {code, Code}}) ->
  CodeEncoded = encode_cstring(Code),
  <<13, Name/binary, 0, (byte_size(CodeEncoded)):32/little-signed, CodeEncoded/binary>>.

encarray(L, [H|T], N) ->
    encarray([{integer_to_list(N), H}|L], T, N+1);
encarray(L, [], _) ->
    encode(lists:reverse(L)).

encode_cstring(String) ->
    <<(unicode:characters_to_binary(String))/binary, 0:8>>.
	
decode(Bin) -> decode(Bin, [], not_done).

%% Size has to be greater than 4
decode(<<Size:32/little-signed, Bin/binary>>,Acc,not_done) when  Size > 4 ->
    try decode_next(Bin, [], not_done) of
        {BSON, <<>>} ->
            decode(<<>>, [BSON|Acc],done);
        {BSON, Rest} ->
            decode(Rest, [BSON|Acc], not_done)
    catch
        _:_ -> error
    end;
  	
decode(_, [Acc], done) -> Acc;
decode(_, Acc = [_|_], done) -> Acc;
	
decode(_BadLength, _, not_done) ->
  error.
	%throw({invalid_length}).



%decode(Binary, _Size) ->
%  	case decode_next(Binary, []) of
%    	{BSON, <<>>} ->
%      		BSON;
%    	{BSON, Rest} ->
%			[BSON | decode(Rest)]
%  	end.

%%decode_next(Bin,Acc) -> decode_next(Bin,Acc,not_done).

decode_next(<<>>, Accum, not_done) ->
    decode_next(<<>>, Accum, done);
decode_next(<<0:8, Rest/binary>>, Accum, not_done) ->
	decode_next(Rest, Accum, done);
decode_next(<<Type:8/little, Rest/binary>>, Accum, not_done) ->
  	{Name, EncodedValue} = decode_cstring(Rest, <<>>, not_done),
% io:format("Decoding ~p~n", [Type]),
  	{Value, Next} = decode_value(EncodedValue, Type),
  	decode_next(Next, [{Name, Value}|Accum], not_done);
decode_next(Bin, Acc, done) ->
   {lists:reverse(Acc), Bin}.

decode_cstring(Bin,Acc) -> decode_cstring(Bin,Acc, not_done).

decode_cstring(<<0:8, Rest/binary>>, Acc, not_done) ->
	decode_cstring(Rest, Acc, done);
decode_cstring(<<C:8, Rest/binary>>, Acc, not_done) ->
	decode_cstring(Rest, <<Acc/binary, C:8>>, not_done);
decode_cstring(<<>>, _Accum, _) ->
	throw(invalid_cstring);
decode_cstring(Rest,Acc,done) ->
  {Acc, Rest}.

% decode_cstring(<<0:8,Rest/binary>>, Acc) ->
%     {lists:reverse(Acc),Rest};
% decode_cstring(<<C/utf8,Rest/binary>>, Acc) ->
%     decode_cstring(Rest, [C|Acc]).

decode_value(Bin, Type) -> decode_value(Bin, Type, not_done).

decode_value(<<Size:32/little-signed, Data/binary>>, ?data_ref, not_done) when (byte_size(Data)+4) >= Size ->
	{NS, RestWithOID} = decode_cstring(Data, <<>>),
	{{oid, OID}, Rest} = decode_value(RestWithOID, ?data_oid, not_done),
	decode_value(Rest, 0, {done, {ref, NS, OID}});	
decode_value(<<Size:32/little-signed, Data/binary>>, ?data_array, not_done) when (byte_size(Data)+4) >= Size ->
  	{Array, Rest} = decode_next(Data, [], not_done),
  	decode_value(Rest, 0, {done, {array,[Value || {_Key, Value} <- Array]}});	
decode_value(<<Size:32/little-signed, Rest/binary>>, ?data_object, not_done) when (byte_size(Rest)+4) >= Size ->
  	decode_next(Rest, [], not_done); 
  	
  	
  	
decode_value(<<OID:12/binary,Rest/binary>>, ?data_oid, not_done) ->
  	% FirstReversed = lists:reverse(binary_to_list(First)),
  	% SecondReversed = lists:reverse(binary_to_list(Second)),
  	% OID = list_to_binary(lists:append(FirstReversed, SecondReversed)),
  	decode_value(Rest, 0, {done, {oid, dec2hex(<<>>, OID)}});
decode_value(<<0:8, Rest/binary>>, ?data_boolean, not_done) ->
  	decode_value(Rest, 0, {done, false});
decode_value(<<1:8, Rest/binary>>, ?data_boolean, not_done) ->
  	decode_value(Rest, 0, {done, true});


		
decode_value(<<Millis:64/little-signed, Rest/binary>>, ?data_date, not_done) ->
	UnixTime = trunc(Millis / 1000),
  	MegaSecs = trunc(UnixTime / 1000000),
  	Secs = UnixTime - (MegaSecs * 1000000),
  	MicroSecs = (Millis - (UnixTime * 1000)) * 1000,
  	decode_value(Rest, 0, {done, {MegaSecs, Secs, MicroSecs}});
decode_value(<<Integer:64/little-signed, Rest/binary>>, ?data_long, not_done) ->
	decode_value(Rest, 0, {done, Integer});
decode_value(<<Double:64/little-signed-float, Rest/binary>>, ?data_number, not_done) ->
	decode_value(Rest, 0, {done, Double});	


decode_value(<<_Size:32/little-signed, _CStringSize:32/little-signed, Data/binary>>, ?data_code_w_scope, not_done) ->
	{Code,Rest} = decode_cstring(Data,<<>>,not_done),
	{BSONOBJ, More} = decode_next(Rest, [], not_done),
	decode_value(More, 0, {done, {code_w_scope, Code, BSONOBJ}});	 	 
	
	
decode_value(<<_Size:32/little-signed, Data/binary>>, ?data_code, not_done) ->
	{Code, Rest} = decode_cstring(Data, <<>>, not_done),
	decode_value(Rest, 0, {done, {code, Code}});

decode_value(<<_Size:32/little-signed, 2:8/little, BinSize:32/little-signed, BinData:BinSize/binary-little-unit:8, Rest/binary>>, ?data_binary, not_done) ->
  	decode_value(Rest, 0, {done, {binary, 2, BinData}});
  	
decode_value(<<Size:32/little-signed, SubType:8/little, BinData:Size/binary-little-unit:8, Rest/binary>>, ?data_binary, not_done) ->
  	decode_value(Rest, 0, {done, {binary, SubType, BinData}});	


decode_value(<<Size:32/little-signed, Rest/binary>>, ?data_string, not_done) ->
	StringSize = Size-1,
	<<String:StringSize/binary, 0:8, Remain/binary>> = Rest,
	decode_value(Remain, 0, {done, String});


decode_value(<<Integer:32/little-signed, Rest/binary>>, ?data_int, not_done) ->
	decode_value(Rest, 0,{done, Integer});

decode_value(<<Integer:32/little-signed, Rest/binary>>, ?data_long, not_done) ->
	decode_value(Rest, 0, {done, Integer});
	
	% {String, RestNext} = decode_cstring(Rest, <<>>),
	% ActualSize = byte_size(Rest) - byte_size(RestNext),
	% case ActualSize =:= Size of
	%     false ->
	%         % ?debugFmt("* ~p =:= ~p -> false", [ActualSize, Size]),
	%         throw({invalid_length, expected, Size, ActualSize});
	%     true ->
	%         {String, RestNext}
	% end;
	
%%
%% clauses that will much arbitrary binaries, move those above if you want to match specific binary
%%
decode_value(Binary, ?data_undefined, not_done) ->
  	decode_value(Binary, 0, {done, null});
decode_value(Binary, ?data_null, not_done) ->
  	decode_value(Binary, 0, {done, null});
decode_value(Binary, ?data_regex, not_done) ->
  	{Expression, RestWithFlags} = decode_cstring(Binary, <<>>),
  	{Flags, Rest} = decode_cstring(RestWithFlags, <<>>),
  	decode_value(Rest, 0, {done, {regex, Expression, Flags}});
decode_value(_Binary, ?OMITTED, not_done) ->
	throw(encountered_ommitted);
	
decode_value(Rest, _, {done, Ret}) ->
	{Ret, Rest}.



%% test codes

 %bin_to_hexstr(Bin) ->
 %   lists:flatten([io_lib:format("~2.16.0B", [X]) || X <- binary_to_list(Bin)]).
 
 hexstr_to_bin(S) ->
    hexstr_to_bin(S, []).
 hexstr_to_bin([], Acc) ->
    list_to_binary(lists:reverse(Acc));
 hexstr_to_bin([X,Y|T], Acc) ->
    {ok, [V], []} = io_lib:fread("~16u", [X,Y]),
    hexstr_to_bin(T, [V | Acc]).

bson_test_() ->
 
  Tests = [
      {"0c0000001061000100000000", [{<<"a">>, 1}]},
      {"13000000106100010000001062000200000000", [{<<"a">>, 1},{<<"b">>, 2}]},
      {"1300000002610007000000737472696e670000", [{<<"a">>, <<"string">>}]},
      {"10000000016100333333333333f33f00", [{<<"a">>,1.2}]},
      {"140000000361000c000000106200010000000000", [{<<"a">>, [{<<"b">>, 1}]}]},
      {"090000000861000100", [{<<"a">>, true}]},
      {"080000000a610000", [{<<"a">>, null}]},
      {"10000000096100355779901f01000000", [{<<"a">>, {1235,79485,237000}}]}, % datetime.datetime(2009, 2, 19, 21, 38, 5, 237009)
      {"16000000076f69640007060504030201000b0a090800", [{<<"oid">>, {oid, <<"07060504030201000b0a0908">>}}]},
      {"120000000b726567657800612a6200690000", [{<<"regex">>, {regex, <<"a*b">>, <<"i">>}}]},
      {"1f0000000c7265660005000000636f6c6c0007060504030201000b0a090800", [{<<"ref">>, {ref, <<"coll">>, <<"07060504030201000b0a0908">>}}]},
      {"160000000D2477686572650005000000746573740000", [{<<"$where">>, {code, <<"test">>}}]},
      {"1100000004656D70747900050000000000", [{<<"empty">>, {array,[]}}]},
      {"26000000046172726179001a0000001030000100000010310002000000103200030000000000", [{<<"array">>, {array,[1,2,3]}}]},
      {"180000000574657374000800000002040000007465737400", [{<<"test">>, {binary, 2, <<"test">>}}]},
      {"1400000005746573740004000000807465737400", [{<<"test">>, {binary, 128, <<"test">>}}]},
      {"1e0000000274657374000f000000c3a9e0afb2e0be84e19db0e38eaf0000", [{<<"test">>, unicode:characters_to_binary([233,3058,3972,6000,13231])}]}
  ],

  % test Encode
  [?_assertEqual(Expected, decode(hexstr_to_bin(Hex))) || {Hex, Expected} <- Tests] ++ 
 
  % test Decode
  [?_assertEqual(hexstr_to_bin(Hex), encode(Expected)) || {Hex, Expected} <- Tests]. 
 
bson_exception_test_() ->
 
  ErrorTests = [
    {"0400000000", error},
    {"05000000", []},  %% a null value, should it return [] or error?
    {"050000000000", error},
    {"0c00", error},
    {"0500000001", error} %% formally throw(invalid_cstring)
  ],
 
  ExceptionTests = [
                    
                   ],
  [?_assertThrow(Expected, decode(hexstr_to_bin(Hex))) || {Hex, Expected} <- ExceptionTests] ++ 
  [?_assertEqual(Expected, decode(hexstr_to_bin(Hex))) || {Hex, Expected} <- ErrorTests].
 

