-module(gridfs).

-export([storeFile/3, storeFile/4, removeFile/2, removeFile/3, 
         getFile/2, getFile/3, getFileInfo/2, getFileInfo/3]).

-include("erlmongo.hrl").

%% 
%% Usage :
%% 
%% Record :: #fs_file{}
%% Client :: mongoapi()
%% Filename | NsPrefix :: binary() | string()
%% Binary :: binary()
%%
%% gridfs:storeFile(Client, FilenameOrRecord, Binary) -> ok
%% gridfs:storeFile(Client, NsPrefix, FilenameOrRecord, Binary)
%% 
%% gridfs:removeFile(Client, FileRecOrName)  ->  ok  - NOTE: this function removes all files that matches the query/filename
%% gridfs:removeFile(Client, NsPrefix, FileRecOrName)
%% 
%% gridfs:getFile(Client, FileRecOrFilename) -> {#fs_file{},binary()}
%% gridfs:getFile(Client, NsPrefix, FileRecOrName)
%% 
%% gridfs:getFileInfo(Client, FileRecOrFilename) -> #fs_file{}
%% gridfs:getFileInfo(Client, NsPrefix, FileRecOrName)
%%

%% #fs_file{}
%%    "_id" : <unspecified>,                  // unique ID for this file
%%    "filename" : data_string,               // human name for the file
%%    "contentType" : data_string,            // valid mime type for the object
%%    "length" : data_number,                 // size of the file in bytes
%%    "chunkSize" : data_number,              // size of each of the chunks.  Default is 256k
%%    "uploadDate" : data_date,               // date when object first stored
%%    "aliases" : data_array of data_string,  // optional array of alias strings
%%    "metadata" : data_object,               // anything the user wants to store
%%    "md5" : data_string                     // md5 string

%% #fs_chunk{}
%%    "_id" : <unspecified>,                  // object id of the chunk in the _chunks collection
%%    "files_id" : <unspecified>,              // _id value of the owning {{files}} collection entry
%%    "n" : data_number,                      // "chunk number" - chunks are numbered in order, starting with 0
%%    "data" : data_binary (type 0x02),        // binary data for chunk

%% Optional todos
%% TODO add file permissions to metadata
%% TODO provide utility functions for updating the file record
%% TODO provide range query functions to query a range of chunks (partial file) ? not sure

-define(s(X), (<<??X>>)).
-define(p(X,Y), {?s(X), Y}).
-define(DEFAULT_CHUNK_SIZE, 262144). %256*1024
-define(L(X), io:format("~p~n",[X])).
-define(DEFAULT_ROOT_NAME, ?s(fs)).

-ifndef(IF).
-define(IF(Exp,T,F), (case (Exp) of true -> (T); false -> (F) end)).
-endif.

storeFile(Client, FileRec, Binary) when is_record(FileRec, fs_file) ->
    storeFile(Client, ?DEFAULT_ROOT_NAME, FileRec, Binary);
storeFile(Client, Filename, Binary) ->
    storeFile(Client, ?DEFAULT_ROOT_NAME, Filename, Binary).

storeFile(Client, NsPrefix, FilenameOrRecord, Binary) when is_list(NsPrefix) -> 
    storeFile(Client, list_to_binary(NsPrefix), FilenameOrRecord, Binary);

storeFile(Client, NsPrefix, OldFileRec, Binary) when is_record(OldFileRec, fs_file) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileID = generate_fileid(Client), %% ObjectId
    FileRec = OldFileRec#fs_file{ 
                       docid = FileID,
                       length = byte_size(Binary),
                       chunkSize = ?DEFAULT_CHUNK_SIZE,
                       uploadDate = now(),
                       md5 =  bin_to_hexstr(erlang:md5(Binary))
                      },
    FileProps = filerec_to_prop(FileRec),
    Client:ensureIndex(Col, [?p(filename, 1)]),
    storeChunks(Binary, Client, NsPrefix,  FileRec),
    Client:save(Col, FileProps);

storeFile(Client, NsPrefix, Filename, Binary) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileID = generate_fileid(Client), %% ObjectId
    FileRec = #fs_file{ 
                       docid = FileID,
                       filename = Filename,
                       contentType =  ?s(application/octet-stream),
                       length = byte_size(Binary),
                       chunkSize = ?DEFAULT_CHUNK_SIZE,
                       uploadDate = now(),
                       %%%%% ?p(aliases, FileRec#fs_file.aliases),
                       %%%%% ?p(metadata, FileRec#fs_file.metadata),
                       md5 =  bin_to_hexstr(erlang:md5(Binary))
                      },
    FileProps = filerec_to_prop(FileRec),
    Client:ensureIndex(Col, [?p(filename, 1)]),
    storeChunks(Binary, Client, NsPrefix,  FileRec),
    Client:save(Col, FileProps).


%% all files that matches the query will get deleted, i.e. multiple files all with the same name get deleted.
%% to delete a specific file use docid (ObjectId) instead
%% other ways which you can delete files is by matching md5, upload date or file length
removeFile(Client, FileRecOrName) -> 
    removeFile(Client, ?DEFAULT_ROOT_NAME, FileRecOrName).

removeFile(Client, NsPrefix, FileRec) when is_list(NsPrefix) ->
    removeFile(Client, list_to_binary(NsPrefix), FileRec);
removeFile(Client,NsPrefix, FileRec) when is_record(FileRec, fs_file) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = filerec_to_prop(FileRec),
    Files = Client:find(Col, FileProps, [], 0,0),
    remove_files(Client, NsPrefix, Files),
    Client:remove(Col, FileProps );
removeFile(Client, NsPrefix, Filename) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = [{?s(filename), Filename}],
    Files = Client:find(Col, FileProps, [], 0,0),
    remove_files(Client, NsPrefix, Files),
    Client:remove(Col, FileProps ).


%% returns {fs_file,binary()}

%findFile(Client, FileRecOrFilename) -> 
%    fileFile(Client, ?DEFAULT_ROOT_NAME, FileRecOrName).

getFile(Client, FileRecOrFilename) ->
    getFile(Client, ?DEFAULT_ROOT_NAME, FileRecOrFilename).

getFile(Client, NsPrefix, FileRec) when is_list(NsPrefix) ->
    getFile(Client, list_to_binary(NsPrefix), FileRec);
getFile(Client, NsPrefix, FileRec) when is_record(FileRec, fs_file) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = filerec_to_prop(FileRec),
    File = Client:findOne(Col, FileProps ),
    case  proplists:get_value(?s(_id), File, undefined)of
        undefined -> {#fs_file{},<<>>};
        ID ->
            {prop_to_filerec(File),get_all_chunks(Client, NsPrefix, ID)}
    end;

getFile(Client, NsPrefix, Filename) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = [?p(filename, Filename)],
    File = Client:findOne(Col, FileProps ),
    case  proplists:get_value(?s(_id), File, undefined)of
        undefined -> {#fs_file{},<<>>};
        ID ->
            {prop_to_filerec(File),get_all_chunks(Client, NsPrefix, ID)}
    end.

%% returns #fs_file{}

%findFile(Client, FileRecOrFilename) -> 
%    fileFile(Client, ?DEFAULT_ROOT_NAME, FileRecOrName).

getFileInfo(Client, FileRecOrFilename) ->
    getFileInfo(Client, ?DEFAULT_ROOT_NAME, FileRecOrFilename).

getFileInfo(Client, NsPrefix, FileRec) when is_list(NsPrefix) ->
    getFileInfo(Client, list_to_binary(NsPrefix), FileRec);
getFileInfo(Client, NsPrefix, FileRec) when is_record(FileRec, fs_file) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = filerec_to_prop(FileRec),
    File = Client:findOne(Col, FileProps ),
    case  proplists:get_value(?s(_id), File, undefined)of
        undefined -> #fs_file{};
        _ ->
            prop_to_filerec(File)
    end;

getFileInfo(Client, NsPrefix, Filename) -> 
    Col = <<NsPrefix/binary,".files">>,
    FileProps = [?p(filename, Filename)],
    File = Client:findOne(Col, FileProps ),
    case  proplists:get_value(?s(_id), File, undefined)of
        undefined -> #fs_file{};
        _ ->
            prop_to_filerec(File)
    end.

%% TODO implement multiple file returns
%findFile(Client, NsPrefix, FileRec) when is_record(FileRec, fs_file) -> 
%    Col = <<NsPrefix/binary,".files">>,
%    FileProps = filerec_to_prop(FileRec),
%    Client:remove(Col, FileProps );
%findFile(Client, NsPrefix, Filename) -> 
%    Col = <<NsPrefix/binary,".files">>,
%    FileProps = [{?s(filename), Filename}],
%    Client:remove(Col, FileProps ).

%%%%
%%%% Private %%%%%
%%%%

%% TODO use cursor to retrive the file
get_all_chunks(Client, NsPrefix, ID) ->
    ChunksCol = <<NsPrefix/binary, ".chunks">>,
    get_all_chunks(<<>>, Client, ChunksCol, ID, 0).

get_all_chunks(FileBin, Client, ChunksCol, ID, CurChunkID) ->
    case Client:findOne(ChunksCol, [?p(files_id, ID), ?p(n, CurChunkID)]) of
        [] -> FileBin;
        Chunk -> 
            ChunkBin = 
                case proplists:get_value(?s(data), Chunk, undefined) of
                    undefined -> <<>>;
                    {binary,_,B} -> B;
                    _ -> <<>>
                end,
            get_all_chunks(<<FileBin/binary, ChunkBin/binary>>, 
                           Client, ChunksCol, ID, CurChunkID+1)
    end.


%% TODO batch remove the files
remove_files(Client, NsPrefix, Files) -> %% Files is a list of #fs_file{} proplists
    Col = <<NsPrefix/binary, ".chunks">>,
    lists:foreach(fun(Fileprop) -> 
                          case proplists:get_value(?s(_id), Fileprop, undefined) of
                              undefined -> ok;
                              ID -> Client:remove(Col, [{?s(files_id),ID}])
                          end
                  end, Files).

prop_to_filerec(FileProp) ->
    lists:foldl(fun
                   (?p(_id,V), File) -> File#fs_file{docid = V };
                   (?p(filename,V), File) -> File#fs_file{filename = V };
                   (?p(contentType,V), File) -> File#fs_file{contentType = V };
                   (?p(length,V), File) -> File#fs_file{length = V };
                   (?p(chunkSize,V), File) -> File#fs_file{chunkSize = V };
                   (?p(uploadDate,V), File) -> File#fs_file{uploadDate = V };
                   (?p(aliases,V), File) -> File#fs_file{aliases = V };
                   (?p(metadata,V), File) -> File#fs_file{metadata = V };
                   (?p(md5,V), File) -> File#fs_file{md5 = V }
                end, #fs_file{}, FileProp).

filerec_to_prop(FileRec) ->
    Props =
    [ 
     ?p(_id, FileRec#fs_file.docid),
     ?p(filename, FileRec#fs_file.filename), 
     ?p(contentType, FileRec#fs_file.contentType),
     ?p(length, FileRec#fs_file.length),
     ?p(chunkSize, FileRec#fs_file.chunkSize),
     ?p(uploadDate, FileRec#fs_file.uploadDate),
     ?p(aliases, FileRec#fs_file.aliases),
     ?p(metadata, FileRec#fs_file.metadata),
     ?p(md5, FileRec#fs_file.md5)
    ],
    %% remove undefined values
    lists:reverse(
        lists:foldl(fun({_,undefined},Acc) -> Acc; (P, Acc) -> [P|Acc] end, [], Props)).

%% call the server to generate a unique ID using ObjectId() function
generate_fileid(Client) ->
    Ret = proplists:get_value(?s(retval), Client:eval(<<"ObjectId()">>)),
    proplists:get_value(?s(value), Ret). %% get the OID
    
%% TODO batchInsert() the chunks
storeChunks(Binary, Client, NsPrefix, FileRec) when is_list(NsPrefix) ->
    storeChunks(Binary, Client, list_to_binary(NsPrefix), FileRec);
storeChunks(Binary, Client, NsPrefix, FileRec) ->
    Col = <<NsPrefix/binary,".chunks">>,
    Client:ensureIndex(Col, [ ?p(files_id, 1), ?p(n, 1) ]),
    storeChunks(Binary, Client, Col, FileRec, 0).

storeChunks(<<>>, _, _, _, _) -> ok;
storeChunks(<<Chunk:?DEFAULT_CHUNK_SIZE/binary, Rest/binary>>, Client, Col, FileRec, ChunkNumber) ->
    Client:save(Col, [
                      ?p(files_id, FileRec#fs_file.docid), 
                      ?p(n, ChunkNumber),
                      ?p(data, ?MONGO_BINARY(Chunk))
                     ]),
    storeChunks(Rest, Client, Col, FileRec, ChunkNumber+1);
storeChunks(Binary, Client, Col, FileRec,  ChunkNumber)  ->
    Client:save(Col, [
                      ?p(files_id, FileRec#fs_file.docid), 
                      ?p(n, ChunkNumber),
                      ?p(data, ?MONGO_BINARY(Binary))
                     ]).

bin_to_hexstr(Bin) ->
    list_to_binary([ int_to_hex(Hex)  || <<Hex:8>> <= Bin]).


int_to_hex(0) -> "00";
int_to_hex(1) -> "01";
int_to_hex(2) -> "02";
int_to_hex(3) -> "03";
int_to_hex(4) -> "04";
int_to_hex(5) -> "05";
int_to_hex(6) -> "06";
int_to_hex(7) -> "07";
int_to_hex(8) -> "08";
int_to_hex(9) -> "09";
int_to_hex(10) -> "0A";
int_to_hex(11) -> "0B";
int_to_hex(12) -> "0C";
int_to_hex(13) -> "0D";
int_to_hex(14) -> "0E";
int_to_hex(15) -> "0F";
int_to_hex(16) -> "10";
int_to_hex(17) -> "11";
int_to_hex(18) -> "12";
int_to_hex(19) -> "13";
int_to_hex(20) -> "14";
int_to_hex(21) -> "15";
int_to_hex(22) -> "16";
int_to_hex(23) -> "17";
int_to_hex(24) -> "18";
int_to_hex(25) -> "19";
int_to_hex(26) -> "1A";
int_to_hex(27) -> "1B";
int_to_hex(28) -> "1C";
int_to_hex(29) -> "1D";
int_to_hex(30) -> "1E";
int_to_hex(31) -> "1F";
int_to_hex(32) -> "20";
int_to_hex(33) -> "21";
int_to_hex(34) -> "22";
int_to_hex(35) -> "23";
int_to_hex(36) -> "24";
int_to_hex(37) -> "25";
int_to_hex(38) -> "26";
int_to_hex(39) -> "27";
int_to_hex(40) -> "28";
int_to_hex(41) -> "29";
int_to_hex(42) -> "2A";
int_to_hex(43) -> "2B";
int_to_hex(44) -> "2C";
int_to_hex(45) -> "2D";
int_to_hex(46) -> "2E";
int_to_hex(47) -> "2F";
int_to_hex(48) -> "30";
int_to_hex(49) -> "31";
int_to_hex(50) -> "32";
int_to_hex(51) -> "33";
int_to_hex(52) -> "34";
int_to_hex(53) -> "35";
int_to_hex(54) -> "36";
int_to_hex(55) -> "37";
int_to_hex(56) -> "38";
int_to_hex(57) -> "39";
int_to_hex(58) -> "3A";
int_to_hex(59) -> "3B";
int_to_hex(60) -> "3C";
int_to_hex(61) -> "3D";
int_to_hex(62) -> "3E";
int_to_hex(63) -> "3F";
int_to_hex(64) -> "40";
int_to_hex(65) -> "41";
int_to_hex(66) -> "42";
int_to_hex(67) -> "43";
int_to_hex(68) -> "44";
int_to_hex(69) -> "45";
int_to_hex(70) -> "46";
int_to_hex(71) -> "47";
int_to_hex(72) -> "48";
int_to_hex(73) -> "49";
int_to_hex(74) -> "4A";
int_to_hex(75) -> "4B";
int_to_hex(76) -> "4C";
int_to_hex(77) -> "4D";
int_to_hex(78) -> "4E";
int_to_hex(79) -> "4F";
int_to_hex(80) -> "50";
int_to_hex(81) -> "51";
int_to_hex(82) -> "52";
int_to_hex(83) -> "53";
int_to_hex(84) -> "54";
int_to_hex(85) -> "55";
int_to_hex(86) -> "56";
int_to_hex(87) -> "57";
int_to_hex(88) -> "58";
int_to_hex(89) -> "59";
int_to_hex(90) -> "5A";
int_to_hex(91) -> "5B";
int_to_hex(92) -> "5C";
int_to_hex(93) -> "5D";
int_to_hex(94) -> "5E";
int_to_hex(95) -> "5F";
int_to_hex(96) -> "60";
int_to_hex(97) -> "61";
int_to_hex(98) -> "62";
int_to_hex(99) -> "63";
int_to_hex(100) -> "64";
int_to_hex(101) -> "65";
int_to_hex(102) -> "66";
int_to_hex(103) -> "67";
int_to_hex(104) -> "68";
int_to_hex(105) -> "69";
int_to_hex(106) -> "6A";
int_to_hex(107) -> "6B";
int_to_hex(108) -> "6C";
int_to_hex(109) -> "6D";
int_to_hex(110) -> "6E";
int_to_hex(111) -> "6F";
int_to_hex(112) -> "70";
int_to_hex(113) -> "71";
int_to_hex(114) -> "72";
int_to_hex(115) -> "73";
int_to_hex(116) -> "74";
int_to_hex(117) -> "75";
int_to_hex(118) -> "76";
int_to_hex(119) -> "77";
int_to_hex(120) -> "78";
int_to_hex(121) -> "79";
int_to_hex(122) -> "7A";
int_to_hex(123) -> "7B";
int_to_hex(124) -> "7C";
int_to_hex(125) -> "7D";
int_to_hex(126) -> "7E";
int_to_hex(127) -> "7F";
int_to_hex(128) -> "80";
int_to_hex(129) -> "81";
int_to_hex(130) -> "82";
int_to_hex(131) -> "83";
int_to_hex(132) -> "84";
int_to_hex(133) -> "85";
int_to_hex(134) -> "86";
int_to_hex(135) -> "87";
int_to_hex(136) -> "88";
int_to_hex(137) -> "89";
int_to_hex(138) -> "8A";
int_to_hex(139) -> "8B";
int_to_hex(140) -> "8C";
int_to_hex(141) -> "8D";
int_to_hex(142) -> "8E";
int_to_hex(143) -> "8F";
int_to_hex(144) -> "90";
int_to_hex(145) -> "91";
int_to_hex(146) -> "92";
int_to_hex(147) -> "93";
int_to_hex(148) -> "94";
int_to_hex(149) -> "95";
int_to_hex(150) -> "96";
int_to_hex(151) -> "97";
int_to_hex(152) -> "98";
int_to_hex(153) -> "99";
int_to_hex(154) -> "9A";
int_to_hex(155) -> "9B";
int_to_hex(156) -> "9C";
int_to_hex(157) -> "9D";
int_to_hex(158) -> "9E";
int_to_hex(159) -> "9F";
int_to_hex(160) -> "A0";
int_to_hex(161) -> "A1";
int_to_hex(162) -> "A2";
int_to_hex(163) -> "A3";
int_to_hex(164) -> "A4";
int_to_hex(165) -> "A5";
int_to_hex(166) -> "A6";
int_to_hex(167) -> "A7";
int_to_hex(168) -> "A8";
int_to_hex(169) -> "A9";
int_to_hex(170) -> "AA";
int_to_hex(171) -> "AB";
int_to_hex(172) -> "AC";
int_to_hex(173) -> "AD";
int_to_hex(174) -> "AE";
int_to_hex(175) -> "AF";
int_to_hex(176) -> "B0";
int_to_hex(177) -> "B1";
int_to_hex(178) -> "B2";
int_to_hex(179) -> "B3";
int_to_hex(180) -> "B4";
int_to_hex(181) -> "B5";
int_to_hex(182) -> "B6";
int_to_hex(183) -> "B7";
int_to_hex(184) -> "B8";
int_to_hex(185) -> "B9";
int_to_hex(186) -> "BA";
int_to_hex(187) -> "BB";
int_to_hex(188) -> "BC";
int_to_hex(189) -> "BD";
int_to_hex(190) -> "BE";
int_to_hex(191) -> "BF";
int_to_hex(192) -> "C0";
int_to_hex(193) -> "C1";
int_to_hex(194) -> "C2";
int_to_hex(195) -> "C3";
int_to_hex(196) -> "C4";
int_to_hex(197) -> "C5";
int_to_hex(198) -> "C6";
int_to_hex(199) -> "C7";
int_to_hex(200) -> "C8";
int_to_hex(201) -> "C9";
int_to_hex(202) -> "CA";
int_to_hex(203) -> "CB";
int_to_hex(204) -> "CC";
int_to_hex(205) -> "CD";
int_to_hex(206) -> "CE";
int_to_hex(207) -> "CF";
int_to_hex(208) -> "D0";
int_to_hex(209) -> "D1";
int_to_hex(210) -> "D2";
int_to_hex(211) -> "D3";
int_to_hex(212) -> "D4";
int_to_hex(213) -> "D5";
int_to_hex(214) -> "D6";
int_to_hex(215) -> "D7";
int_to_hex(216) -> "D8";
int_to_hex(217) -> "D9";
int_to_hex(218) -> "DA";
int_to_hex(219) -> "DB";
int_to_hex(220) -> "DC";
int_to_hex(221) -> "DD";
int_to_hex(222) -> "DE";
int_to_hex(223) -> "DF";
int_to_hex(224) -> "E0";
int_to_hex(225) -> "E1";
int_to_hex(226) -> "E2";
int_to_hex(227) -> "E3";
int_to_hex(228) -> "E4";
int_to_hex(229) -> "E5";
int_to_hex(230) -> "E6";
int_to_hex(231) -> "E7";
int_to_hex(232) -> "E8";
int_to_hex(233) -> "E9";
int_to_hex(234) -> "EA";
int_to_hex(235) -> "EB";
int_to_hex(236) -> "EC";
int_to_hex(237) -> "ED";
int_to_hex(238) -> "EE";
int_to_hex(239) -> "EF";
int_to_hex(240) -> "F0";
int_to_hex(241) -> "F1";
int_to_hex(242) -> "F2";
int_to_hex(243) -> "F3";
int_to_hex(244) -> "F4";
int_to_hex(245) -> "F5";
int_to_hex(246) -> "F6";
int_to_hex(247) -> "F7";
int_to_hex(248) -> "F8";
int_to_hex(249) -> "F9";
int_to_hex(250) -> "FA";
int_to_hex(251) -> "FB";
int_to_hex(252) -> "FC";
int_to_hex(253) -> "FD";
int_to_hex(254) -> "FE";
int_to_hex(255) -> "FF".


