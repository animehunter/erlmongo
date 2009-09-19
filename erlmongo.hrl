-ifndef(ERLMONGO_H).
-define(ERLMONGO_H,true).


% recindex is index of record in RECTABLE define, it has to be the first record field
% docid is _id in mongodb, it has to be named docid and it has to be the second field in the record
%%% TODO: find a better way of doing this... 
-define(I(Index), recindex=Index, docid).

-record(mydoc,          {?I(1), name, i, address, tags}).
-record(fs_file,        {?I(2), filename,contentType,length,chunkSize,uploadDate,aliases,metadata,md5}). %% see gridfs.erl for doc
-record(fs_chunk,       {?I(3), files_id,n,data}). %% see gridfs.erl for doc
-record(address,        {?I(4), street, city, country}).

% A table of records used with mongodb (tuple of record fields).
% If you arent using an embedded record, you can use record_info(fields, name_of_record)
% If a record uses an embedded record, you have to write the fields yourself
%  and the field which is an embedded record is: {name_of_record, index_of_record_in_RECTABLE}
%  field name also has to match the record name.
-define(R(Rec), record_info(fields, Rec)).
-define(RECTABLE, {
                   [recindex,docid,name,i, {address, 2}, tags],
                   ?R(fs_file),
                   ?R(fs_chunk),
				   ?R(address)
				  }).



% Useful Macros
-define(MONGO_ARRAY(List), {array, List}).
-define(MONGO_BINARY(Bin), {binary, 2, Bin}).

-define(ObjectIdSize, 12). %% 96 bits, 12 bytes

%% 
%% Erlang to JSON Mapping
%% 
%% <<UTF8String>>/String -> String
%% true -> true
%% false -> false
%% null -> null
%% double -> number
%% int32/int64 -> number
%% 
%% ?JS_ARRAY(List)/{list,List} -> List
%% ?JS_BINARY(Bin)/{binary, Type=2, X} -> Bin
%% [{<<K>>,V}] -> Object
%% 
%% OIDBin -> ObjectId
%% {bson, Bin} -> BSON
%% {MegaSecs, Secs, MicroSecs}/now() -> Time
%% {regex, Expression, Flags} -> RegExp
%% {ref, Collection, OIDBin} -> DBRef
%% {code Code} -> Code
%%

-export([rec2prop/2, prop2rec/4]).

% Convert record to prop list	
rec2prop(Rec, RecordFields) ->
	loop_rec(RecordFields, 1, Rec, []).

loop_rec([H|T], N, Rec, L) ->
	loop_rec(T, N+1, Rec, [{H, element(N+1, Rec)}|L]);
loop_rec([], _, _, L) ->
	L.

% convert prop list to record
prop2rec(Prop, RecName, DefRec, RecordFields) ->
	loop_fields(erlang:make_tuple(tuple_size(DefRec), RecName), RecordFields, DefRec, Prop, 2).

loop_fields(Tuple, [Field|T], DefRec, Props, N) ->
	case lists:keysearch(Field, 1, Props) of
		{value, {_, Val}} ->
			loop_fields(setelement(N, Tuple, Val), T, DefRec, Props, N+1);
		false ->
			loop_fields(setelement(N, Tuple, element(N, DefRec)), T, DefRec, Props, N+1)
	end;
loop_fields(Tuple, [], _, _, _) ->
	Tuple.
	
	
% mongo	
-define(QUER_OPT_NONE, 0).
-define(QUER_OPT_CURSOR, 2).
-define(QUER_OPT_SLAVEOK, 4).
-define(QUER_OPT_NOTIMEOUT, 16).

% By = either a record or proplist with parameters you are searching by
% field_selector = list of fields you wish to return
% ndocs = how many documents you wish to return, 0 = default
% nskip = how many documents to skip
% Don't touch it.
-record(search, {ndocs = 0, nskip = 0, criteria = [], field_selector = <<>>, opts = ?QUER_OPT_NONE}).
-record(cursor, {id, pid, limit = 0}).
-record(update, {upsert = 1, selector = <<>>, document = <<>>}).
-record(insert, {documents = []}).
-record(delete, {selector = <<>>}).
-record(killc, {cur_ids = <<>>}).	

-endif.