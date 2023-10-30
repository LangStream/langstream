#
# Copyright DataStax, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: langstream_grpc/proto/agent.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


from google.protobuf import empty_pb2 as google_dot_protobuf_dot_empty__pb2


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n!langstream_grpc/proto/agent.proto\x1a\x1bgoogle/protobuf/empty.proto"!\n\x0cInfoResponse\x12\x11\n\tjson_info\x18\x01 \x01(\t"\xa3\x02\n\x05Value\x12\x11\n\tschema_id\x18\x01 \x01(\x05\x12\x15\n\x0b\x62ytes_value\x18\x02 \x01(\x0cH\x00\x12\x17\n\rboolean_value\x18\x03 \x01(\x08H\x00\x12\x16\n\x0cstring_value\x18\x04 \x01(\tH\x00\x12\x14\n\nbyte_value\x18\x05 \x01(\x05H\x00\x12\x15\n\x0bshort_value\x18\x06 \x01(\x05H\x00\x12\x13\n\tint_value\x18\x07 \x01(\x05H\x00\x12\x14\n\nlong_value\x18\x08 \x01(\x03H\x00\x12\x15\n\x0b\x66loat_value\x18\t \x01(\x02H\x00\x12\x16\n\x0c\x64ouble_value\x18\n \x01(\x01H\x00\x12\x14\n\njson_value\x18\x0b \x01(\tH\x00\x12\x14\n\navro_value\x18\x0c \x01(\x0cH\x00\x42\x0c\n\ntype_oneof"-\n\x06Header\x12\x0c\n\x04name\x18\x01 \x01(\t\x12\x15\n\x05value\x18\x02 \x01(\x0b\x32\x06.Value"*\n\x06Schema\x12\x11\n\tschema_id\x18\x01 \x01(\x05\x12\r\n\x05value\x18\x02 \x01(\x0c"\xb3\x01\n\x06Record\x12\x11\n\trecord_id\x18\x01 \x01(\x03\x12\x18\n\x03key\x18\x02 \x01(\x0b\x32\x06.ValueH\x00\x88\x01\x01\x12\x1a\n\x05value\x18\x03 \x01(\x0b\x32\x06.ValueH\x01\x88\x01\x01\x12\x18\n\x07headers\x18\x04 \x03(\x0b\x32\x07.Header\x12\x0e\n\x06origin\x18\x05 \x01(\t\x12\x16\n\ttimestamp\x18\x06 \x01(\x03H\x02\x88\x01\x01\x42\x06\n\x04_keyB\x08\n\x06_valueB\x0c\n\n_timestamp"<\n\x10PermanentFailure\x12\x11\n\trecord_id\x18\x01 \x01(\x03\x12\x15\n\rerror_message\x18\x02 \x01(\t"X\n\rSourceRequest\x12\x19\n\x11\x63ommitted_records\x18\x01 \x03(\x03\x12,\n\x11permanent_failure\x18\x02 \x01(\x0b\x32\x11.PermanentFailure"C\n\x0eSourceResponse\x12\x17\n\x06schema\x18\x01 \x01(\x0b\x32\x07.Schema\x12\x18\n\x07records\x18\x02 \x03(\x0b\x32\x07.Record"E\n\x10ProcessorRequest\x12\x17\n\x06schema\x18\x01 \x01(\x0b\x32\x07.Schema\x12\x18\n\x07records\x18\x02 \x03(\x0b\x32\x07.Record"O\n\x11ProcessorResponse\x12\x17\n\x06schema\x18\x01 \x01(\x0b\x32\x07.Schema\x12!\n\x07results\x18\x02 \x03(\x0b\x32\x10.ProcessorResult"\\\n\x0fProcessorResult\x12\x11\n\trecord_id\x18\x01 \x01(\x03\x12\x12\n\x05\x65rror\x18\x02 \x01(\tH\x00\x88\x01\x01\x12\x18\n\x07records\x18\x03 \x03(\x0b\x32\x07.RecordB\x08\n\x06_error"?\n\x0bSinkRequest\x12\x17\n\x06schema\x18\x01 \x01(\x0b\x32\x07.Schema\x12\x17\n\x06record\x18\x02 \x01(\x0b\x32\x07.Record"?\n\x0cSinkResponse\x12\x11\n\trecord_id\x18\x01 \x01(\x03\x12\x12\n\x05\x65rror\x18\x02 \x01(\tH\x00\x88\x01\x01\x42\x08\n\x06_error2\xd8\x01\n\x0c\x41gentService\x12\x35\n\nagent_info\x12\x16.google.protobuf.Empty\x1a\r.InfoResponse"\x00\x12-\n\x04read\x12\x0e.SourceRequest\x1a\x0f.SourceResponse"\x00(\x01\x30\x01\x12\x36\n\x07process\x12\x11.ProcessorRequest\x1a\x12.ProcessorResponse"\x00(\x01\x30\x01\x12*\n\x05write\x12\x0c.SinkRequest\x1a\r.SinkResponse"\x00(\x01\x30\x01\x42\x1d\n\x19\x61i.langstream.agents.grpcP\x01\x62\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(
    DESCRIPTOR, "langstream_grpc.proto.agent_pb2", _globals
)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b"\n\031ai.langstream.agents.grpcP\001"
    _globals["_INFORESPONSE"]._serialized_start = 66
    _globals["_INFORESPONSE"]._serialized_end = 99
    _globals["_VALUE"]._serialized_start = 102
    _globals["_VALUE"]._serialized_end = 393
    _globals["_HEADER"]._serialized_start = 395
    _globals["_HEADER"]._serialized_end = 440
    _globals["_SCHEMA"]._serialized_start = 442
    _globals["_SCHEMA"]._serialized_end = 484
    _globals["_RECORD"]._serialized_start = 487
    _globals["_RECORD"]._serialized_end = 666
    _globals["_PERMANENTFAILURE"]._serialized_start = 668
    _globals["_PERMANENTFAILURE"]._serialized_end = 728
    _globals["_SOURCEREQUEST"]._serialized_start = 730
    _globals["_SOURCEREQUEST"]._serialized_end = 818
    _globals["_SOURCERESPONSE"]._serialized_start = 820
    _globals["_SOURCERESPONSE"]._serialized_end = 887
    _globals["_PROCESSORREQUEST"]._serialized_start = 889
    _globals["_PROCESSORREQUEST"]._serialized_end = 958
    _globals["_PROCESSORRESPONSE"]._serialized_start = 960
    _globals["_PROCESSORRESPONSE"]._serialized_end = 1039
    _globals["_PROCESSORRESULT"]._serialized_start = 1041
    _globals["_PROCESSORRESULT"]._serialized_end = 1133
    _globals["_SINKREQUEST"]._serialized_start = 1135
    _globals["_SINKREQUEST"]._serialized_end = 1198
    _globals["_SINKRESPONSE"]._serialized_start = 1200
    _globals["_SINKRESPONSE"]._serialized_end = 1263
    _globals["_AGENTSERVICE"]._serialized_start = 1266
    _globals["_AGENTSERVICE"]._serialized_end = 1482
# @@protoc_insertion_point(module_scope)
