#!/usr/bin/env python3

# ****************************************************************************
# Copyright 2019 The Apollo Authors. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ****************************************************************************
# -*- coding: utf-8 -*-
"""
Module for example of record.

Run with:
    record --rd_path=... --op_path=...
"""

import time
import os 
import pandas as pd
import openpyxl
from absl import app
from absl import flags

from google.protobuf.descriptor_pb2 import FileDescriptorProto

from cyber.proto.unit_test_pb2 import Chatter
from cyber.python.cyber_py3 import record
from modules.common.util.testdata.simple_pb2 import SimpleMessage

FLAGS = flags.FLAGS
flags.DEFINE_string('rd_path', '', 'record bag path')
flags.DEFINE_string('op_path', '', 'excel saved path')

MSG_TYPE = "apollo.common.util.test.SimpleMessage"
MSG_TYPE_CHATTER = "apollo.cyber.proto.Chatter"


def test_record_writer(writer_path):
    """
    Record writer.
    """
    fwriter = record.RecordWriter()
    fwriter.set_size_fileseg(0)
    fwriter.set_intervaltime_fileseg(0)

    if not fwriter.open(writer_path):
        print('Failed to open record writer!')
        return
    print('+++ Begin to writer +++')

    # Writer 2 SimpleMessage
    msg = SimpleMessage()
    msg.text = "AAAAAA"

    file_desc = msg.DESCRIPTOR.file
    proto = FileDescriptorProto()
    file_desc.CopyToProto(proto)
    proto.name = file_desc.name
    desc_str = proto.SerializeToString()
    print(msg.DESCRIPTOR.full_name)
    fwriter.write_channel(
        '/apollo/sensor/livox/test/simplemsg_channel', msg.DESCRIPTOR.full_name, desc_str)
    fwriter.write_message('/apollo/sensor/livox/test/simplemsg_channel', msg, 1619858505269037334, False)
    fwriter.write_message('/apollo/sensor/livox/test/simplemsg_channel', msg.SerializeToString(), 1619858505269037289)

    # Writer 2 Chatter
    msg = Chatter()
    msg.timestamp = 99999
    msg.lidar_timestamp = 100
    msg.seq = 1

    file_desc = msg.DESCRIPTOR.file
    proto = FileDescriptorProto()
    file_desc.CopyToProto(proto)
    proto.name = file_desc.name
    desc_str = proto.SerializeToString()
    print(msg.DESCRIPTOR.full_name)
    fwriter.write_channel('/apollo/sensor/livox/test/chatter_a', msg.DESCRIPTOR.full_name, desc_str)
    fwriter.write_message('/apollo/sensor/livox/test/chatter_a', msg, 1619858505269034531, False)
    msg.seq = 2
    fwriter.write_message("/apollo/sensor/livox/test/chatter_a", msg.SerializeToString(), 1619858505269036352)

    fwriter.close()

def test_record_reader(reader_path):
    """
    Record reader.
    """
    freader = record.RecordReader(reader_path)
    time.sleep(1)
    print('+' * 80)
    print('+++ Begin to read +++')
    count = 0
    for channel_name, msg, datatype, timestamp in freader.read_messages():
        count += 1
        print('=' * 80)
        print('read [%d] messages' % count)
        print('channel_name -> %s' % channel_name)
        print('msgtime -> %d' % timestamp)
        print('msgnum -> %d' % freader.get_messagenumber(channel_name))
        print('msgtype -> %s' % datatype)
        #print('message is -> %s' % msg)
        print('***After parse(if needed),the message is ->')
        if datatype == MSG_TYPE:
            msg_new = SimpleMessage()
            msg_new.ParseFromString(msg)
            print(msg_new)
        elif datatype == MSG_TYPE_CHATTER:
            msg_new = Chatter()
            msg_new.ParseFromString(msg)
            print(msg_new)

def test_record_parse(test_record_file):
    # Read record
    freader = record.RecordReader(test_record_file)
    time.sleep(1)
    timestamp_array = [[[]] for i in range(3000)]
    channel_array = []
                
    for channel_temp, msg, datatype, timestamp in freader.read_messages():
        channel_name = channel_temp.replace('/', '-')
        channel_name = channel_name.replace('-apollo', '')
        if len(channel_name) >= 31 :
            channel_name = channel_name[-30:]
            #print('channel_name -> %s' % channel_name) 
            
        if channel_name in channel_array :
           for value in channel_array:
               if channel_name == value :
                # print('channel_name -> %s' % value) 
                # print('find channel array index -> %d' % channel_array.index(value))

                # print('msgtime -> %d' % timestamp)
                timestamp_array[channel_array.index(value)].append([str(timestamp),(timestamp - int(timestamp_array[channel_array.index(value)][-1][0]))/1000000.00])
        else :
            # print ("\ncreate new channel")
            # print('channel_name -> %s' % channel_name) 
            channel_array.append(channel_name);
            # print('msgtime -> %d' % timestamp)
            # print('channel array len -> %d' % len(channel_array))
            timestamp_array[len(channel_array)-1].append([channel_temp,datatype])
            timestamp_array[len(channel_array)-1].append([str(timestamp),(0)])

    index = 0
    column = ['时间戳','时间间隔(ms)']
    with pd.ExcelWriter(FLAGS.op_path) as writer:
        for channel_name in channel_array:
            name = str(channel_name) 
            #print('index -> %d' % index)
            #print('timestamp_array -> %d' % timestamp_array[index])
            timestamp_array[index].pop(0)
            time_df = pd.DataFrame(timestamp_array[index],columns=column) 
            time_df.to_excel(writer, sheet_name= name)
            index += 1
            
    # writer.save()
    # writer.close()

def main(argv):
    if FLAGS.rd_path != '' and FLAGS.op_path != '':
        print('Begin to parse record file: {}'.format(FLAGS.rd_path))
        test_record_parse(FLAGS.rd_path)
    else:
        print('Record Path or Excel Saved Path Is Empty !')


if __name__ == '__main__':
    #test_record_file = "/tmp/test_writer.record"
    # test_record_file = "/apollo/20210501155909.record.00020"

    #print('Begin to write record file: {}'.format(test_record_file))
    #test_record_writer(test_record_file)

    #print('Begin to read record file: {}'.format(test_record_file))
    #test_record_reader(test_record_file)

    app.run(main)
