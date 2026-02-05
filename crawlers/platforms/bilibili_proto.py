from __future__ import annotations

from google.protobuf import descriptor_pb2, descriptor_pool
from google.protobuf import message_factory


def _build_reply_message():
    file_proto = descriptor_pb2.FileDescriptorProto()
    file_proto.name = "bilibili_danmaku.proto"
    file_proto.package = "bilibili"
    file_proto.syntax = "proto3"

    danmaku_elem = file_proto.message_type.add()
    danmaku_elem.name = "DanmakuElem"

    def add_field(msg, name: str, number: int, field_type: int, label: int = descriptor_pb2.FieldDescriptorProto.LABEL_OPTIONAL, type_name: str | None = None):
        field = msg.field.add()
        field.name = name
        field.number = number
        field.label = label
        field.type = field_type
        if type_name is not None:
            field.type_name = type_name

    add_field(danmaku_elem, "id", 1, descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    add_field(danmaku_elem, "progress", 2, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    add_field(danmaku_elem, "mode", 3, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    add_field(danmaku_elem, "fontsize", 4, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    add_field(danmaku_elem, "color", 5, descriptor_pb2.FieldDescriptorProto.TYPE_UINT32)
    add_field(danmaku_elem, "midHash", 6, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    add_field(danmaku_elem, "content", 7, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    add_field(danmaku_elem, "ctime", 8, descriptor_pb2.FieldDescriptorProto.TYPE_INT64)
    add_field(danmaku_elem, "weight", 9, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    add_field(danmaku_elem, "action", 10, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    add_field(danmaku_elem, "pool", 11, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)
    add_field(danmaku_elem, "idStr", 12, descriptor_pb2.FieldDescriptorProto.TYPE_STRING)
    add_field(danmaku_elem, "attr", 13, descriptor_pb2.FieldDescriptorProto.TYPE_INT32)

    reply = file_proto.message_type.add()
    reply.name = "DmSegMobileReply"
    add_field(
        reply,
        "elems",
        1,
        descriptor_pb2.FieldDescriptorProto.TYPE_MESSAGE,
        label=descriptor_pb2.FieldDescriptorProto.LABEL_REPEATED,
        type_name=".bilibili.DanmakuElem",
    )

    pool = descriptor_pool.Default()
    try:
        pool.FindFileByName(file_proto.name)
    except Exception:
        pool.Add(file_proto)
    desc = pool.FindMessageTypeByName("bilibili.DmSegMobileReply")
    return message_factory.GetMessageClass(desc)


DmSegMobileReply = _build_reply_message()
