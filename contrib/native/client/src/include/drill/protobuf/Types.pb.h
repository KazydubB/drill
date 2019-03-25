// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Types.proto

#ifndef PROTOBUF_Types_2eproto__INCLUDED
#define PROTOBUF_Types_2eproto__INCLUDED

#include <string>

#include <google/protobuf/stubs/common.h>

#if GOOGLE_PROTOBUF_VERSION < 2005000
#error This file was generated by a newer version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please update
#error your headers.
#endif
#if 2005000 < GOOGLE_PROTOBUF_MIN_PROTOC_VERSION
#error This file was generated by an older version of protoc which is
#error incompatible with your Protocol Buffer headers.  Please
#error regenerate this file with a newer version of protoc.
#endif

#include <google/protobuf/generated_message_util.h>
#include <google/protobuf/message.h>
#include <google/protobuf/repeated_field.h>
#include <google/protobuf/extension_set.h>
#include <google/protobuf/generated_enum_reflection.h>
#include <google/protobuf/unknown_field_set.h>
// @@protoc_insertion_point(includes)

namespace common {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_Types_2eproto();
void protobuf_AssignDesc_Types_2eproto();
void protobuf_ShutdownFile_Types_2eproto();

class MajorType;

enum MinorType {
  LATE = 0,
  MAP = 1,
  TINYINT = 3,
  SMALLINT = 4,
  INT = 5,
  BIGINT = 6,
  DECIMAL9 = 7,
  DECIMAL18 = 8,
  DECIMAL28SPARSE = 9,
  DECIMAL38SPARSE = 10,
  MONEY = 11,
  DATE = 12,
  TIME = 13,
  TIMETZ = 14,
  TIMESTAMPTZ = 15,
  TIMESTAMP = 16,
  INTERVAL = 17,
  FLOAT4 = 18,
  FLOAT8 = 19,
  BIT = 20,
  FIXEDCHAR = 21,
  FIXED16CHAR = 22,
  FIXEDBINARY = 23,
  VARCHAR = 24,
  VAR16CHAR = 25,
  VARBINARY = 26,
  UINT1 = 29,
  UINT2 = 30,
  UINT4 = 31,
  UINT8 = 32,
  DECIMAL28DENSE = 33,
  DECIMAL38DENSE = 34,
  DM_UNKNOWN = 37,
  INTERVALYEAR = 38,
  INTERVALDAY = 39,
  LIST = 40,
  GENERIC_OBJECT = 41,
  UNION = 42,
  VARDECIMAL = 43,
  DICT = 44
};
bool MinorType_IsValid(int value);
const MinorType MinorType_MIN = LATE;
const MinorType MinorType_MAX = DICT;
const int MinorType_ARRAYSIZE = MinorType_MAX + 1;

const ::google::protobuf::EnumDescriptor* MinorType_descriptor();
inline const ::std::string& MinorType_Name(MinorType value) {
  return ::google::protobuf::internal::NameOfEnum(
    MinorType_descriptor(), value);
}
inline bool MinorType_Parse(
    const ::std::string& name, MinorType* value) {
  return ::google::protobuf::internal::ParseNamedEnum<MinorType>(
    MinorType_descriptor(), name, value);
}
enum DataMode {
  DM_OPTIONAL = 0,
  DM_REQUIRED = 1,
  DM_REPEATED = 2
};
bool DataMode_IsValid(int value);
const DataMode DataMode_MIN = DM_OPTIONAL;
const DataMode DataMode_MAX = DM_REPEATED;
const int DataMode_ARRAYSIZE = DataMode_MAX + 1;

const ::google::protobuf::EnumDescriptor* DataMode_descriptor();
inline const ::std::string& DataMode_Name(DataMode value) {
  return ::google::protobuf::internal::NameOfEnum(
    DataMode_descriptor(), value);
}
inline bool DataMode_Parse(
    const ::std::string& name, DataMode* value) {
  return ::google::protobuf::internal::ParseNamedEnum<DataMode>(
    DataMode_descriptor(), name, value);
}
// ===================================================================

class MajorType : public ::google::protobuf::Message {
 public:
  MajorType();
  virtual ~MajorType();

  MajorType(const MajorType& from);

  inline MajorType& operator=(const MajorType& from) {
    CopyFrom(from);
    return *this;
  }

  inline const ::google::protobuf::UnknownFieldSet& unknown_fields() const {
    return _unknown_fields_;
  }

  inline ::google::protobuf::UnknownFieldSet* mutable_unknown_fields() {
    return &_unknown_fields_;
  }

  static const ::google::protobuf::Descriptor* descriptor();
  static const MajorType& default_instance();

  void Swap(MajorType* other);

  // implements Message ----------------------------------------------

  MajorType* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const MajorType& from);
  void MergeFrom(const MajorType& from);
  void Clear();
  bool IsInitialized() const;

  int ByteSize() const;
  bool MergePartialFromCodedStream(
      ::google::protobuf::io::CodedInputStream* input);
  void SerializeWithCachedSizes(
      ::google::protobuf::io::CodedOutputStream* output) const;
  ::google::protobuf::uint8* SerializeWithCachedSizesToArray(::google::protobuf::uint8* output) const;
  int GetCachedSize() const { return _cached_size_; }
  private:
  void SharedCtor();
  void SharedDtor();
  void SetCachedSize(int size) const;
  public:

  ::google::protobuf::Metadata GetMetadata() const;

  // nested types ----------------------------------------------------

  // accessors -------------------------------------------------------

  // optional .common.MinorType minor_type = 1;
  inline bool has_minor_type() const;
  inline void clear_minor_type();
  static const int kMinorTypeFieldNumber = 1;
  inline ::common::MinorType minor_type() const;
  inline void set_minor_type(::common::MinorType value);

  // optional .common.DataMode mode = 2;
  inline bool has_mode() const;
  inline void clear_mode();
  static const int kModeFieldNumber = 2;
  inline ::common::DataMode mode() const;
  inline void set_mode(::common::DataMode value);

  // optional int32 width = 3;
  inline bool has_width() const;
  inline void clear_width();
  static const int kWidthFieldNumber = 3;
  inline ::google::protobuf::int32 width() const;
  inline void set_width(::google::protobuf::int32 value);

  // optional int32 precision = 4;
  inline bool has_precision() const;
  inline void clear_precision();
  static const int kPrecisionFieldNumber = 4;
  inline ::google::protobuf::int32 precision() const;
  inline void set_precision(::google::protobuf::int32 value);

  // optional int32 scale = 5;
  inline bool has_scale() const;
  inline void clear_scale();
  static const int kScaleFieldNumber = 5;
  inline ::google::protobuf::int32 scale() const;
  inline void set_scale(::google::protobuf::int32 value);

  // optional int32 timeZone = 6;
  inline bool has_timezone() const;
  inline void clear_timezone();
  static const int kTimeZoneFieldNumber = 6;
  inline ::google::protobuf::int32 timezone() const;
  inline void set_timezone(::google::protobuf::int32 value);

  // repeated .common.MinorType sub_type = 7;
  inline int sub_type_size() const;
  inline void clear_sub_type();
  static const int kSubTypeFieldNumber = 7;
  inline ::common::MinorType sub_type(int index) const;
  inline void set_sub_type(int index, ::common::MinorType value);
  inline void add_sub_type(::common::MinorType value);
  inline const ::google::protobuf::RepeatedField<int>& sub_type() const;
  inline ::google::protobuf::RepeatedField<int>* mutable_sub_type();

  // @@protoc_insertion_point(class_scope:common.MajorType)
 private:
  inline void set_has_minor_type();
  inline void clear_has_minor_type();
  inline void set_has_mode();
  inline void clear_has_mode();
  inline void set_has_width();
  inline void clear_has_width();
  inline void set_has_precision();
  inline void clear_has_precision();
  inline void set_has_scale();
  inline void clear_has_scale();
  inline void set_has_timezone();
  inline void clear_has_timezone();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  int minor_type_;
  int mode_;
  ::google::protobuf::int32 width_;
  ::google::protobuf::int32 precision_;
  ::google::protobuf::int32 scale_;
  ::google::protobuf::int32 timezone_;
  ::google::protobuf::RepeatedField<int> sub_type_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(7 + 31) / 32];

  friend void  protobuf_AddDesc_Types_2eproto();
  friend void protobuf_AssignDesc_Types_2eproto();
  friend void protobuf_ShutdownFile_Types_2eproto();

  void InitAsDefaultInstance();
  static MajorType* default_instance_;
};
// ===================================================================


// ===================================================================

// MajorType

// optional .common.MinorType minor_type = 1;
inline bool MajorType::has_minor_type() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void MajorType::set_has_minor_type() {
  _has_bits_[0] |= 0x00000001u;
}
inline void MajorType::clear_has_minor_type() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void MajorType::clear_minor_type() {
  minor_type_ = 0;
  clear_has_minor_type();
}
inline ::common::MinorType MajorType::minor_type() const {
  return static_cast< ::common::MinorType >(minor_type_);
}
inline void MajorType::set_minor_type(::common::MinorType value) {
  assert(::common::MinorType_IsValid(value));
  set_has_minor_type();
  minor_type_ = value;
}

// optional .common.DataMode mode = 2;
inline bool MajorType::has_mode() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void MajorType::set_has_mode() {
  _has_bits_[0] |= 0x00000002u;
}
inline void MajorType::clear_has_mode() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void MajorType::clear_mode() {
  mode_ = 0;
  clear_has_mode();
}
inline ::common::DataMode MajorType::mode() const {
  return static_cast< ::common::DataMode >(mode_);
}
inline void MajorType::set_mode(::common::DataMode value) {
  assert(::common::DataMode_IsValid(value));
  set_has_mode();
  mode_ = value;
}

// optional int32 width = 3;
inline bool MajorType::has_width() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void MajorType::set_has_width() {
  _has_bits_[0] |= 0x00000004u;
}
inline void MajorType::clear_has_width() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void MajorType::clear_width() {
  width_ = 0;
  clear_has_width();
}
inline ::google::protobuf::int32 MajorType::width() const {
  return width_;
}
inline void MajorType::set_width(::google::protobuf::int32 value) {
  set_has_width();
  width_ = value;
}

// optional int32 precision = 4;
inline bool MajorType::has_precision() const {
  return (_has_bits_[0] & 0x00000008u) != 0;
}
inline void MajorType::set_has_precision() {
  _has_bits_[0] |= 0x00000008u;
}
inline void MajorType::clear_has_precision() {
  _has_bits_[0] &= ~0x00000008u;
}
inline void MajorType::clear_precision() {
  precision_ = 0;
  clear_has_precision();
}
inline ::google::protobuf::int32 MajorType::precision() const {
  return precision_;
}
inline void MajorType::set_precision(::google::protobuf::int32 value) {
  set_has_precision();
  precision_ = value;
}

// optional int32 scale = 5;
inline bool MajorType::has_scale() const {
  return (_has_bits_[0] & 0x00000010u) != 0;
}
inline void MajorType::set_has_scale() {
  _has_bits_[0] |= 0x00000010u;
}
inline void MajorType::clear_has_scale() {
  _has_bits_[0] &= ~0x00000010u;
}
inline void MajorType::clear_scale() {
  scale_ = 0;
  clear_has_scale();
}
inline ::google::protobuf::int32 MajorType::scale() const {
  return scale_;
}
inline void MajorType::set_scale(::google::protobuf::int32 value) {
  set_has_scale();
  scale_ = value;
}

// optional int32 timeZone = 6;
inline bool MajorType::has_timezone() const {
  return (_has_bits_[0] & 0x00000020u) != 0;
}
inline void MajorType::set_has_timezone() {
  _has_bits_[0] |= 0x00000020u;
}
inline void MajorType::clear_has_timezone() {
  _has_bits_[0] &= ~0x00000020u;
}
inline void MajorType::clear_timezone() {
  timezone_ = 0;
  clear_has_timezone();
}
inline ::google::protobuf::int32 MajorType::timezone() const {
  return timezone_;
}
inline void MajorType::set_timezone(::google::protobuf::int32 value) {
  set_has_timezone();
  timezone_ = value;
}

// repeated .common.MinorType sub_type = 7;
inline int MajorType::sub_type_size() const {
  return sub_type_.size();
}
inline void MajorType::clear_sub_type() {
  sub_type_.Clear();
}
inline ::common::MinorType MajorType::sub_type(int index) const {
  return static_cast< ::common::MinorType >(sub_type_.Get(index));
}
inline void MajorType::set_sub_type(int index, ::common::MinorType value) {
  assert(::common::MinorType_IsValid(value));
  sub_type_.Set(index, value);
}
inline void MajorType::add_sub_type(::common::MinorType value) {
  assert(::common::MinorType_IsValid(value));
  sub_type_.Add(value);
}
inline const ::google::protobuf::RepeatedField<int>&
MajorType::sub_type() const {
  return sub_type_;
}
inline ::google::protobuf::RepeatedField<int>*
MajorType::mutable_sub_type() {
  return &sub_type_;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace common

#ifndef SWIG
namespace google {
namespace protobuf {

template <>
inline const EnumDescriptor* GetEnumDescriptor< ::common::MinorType>() {
  return ::common::MinorType_descriptor();
}
template <>
inline const EnumDescriptor* GetEnumDescriptor< ::common::DataMode>() {
  return ::common::DataMode_descriptor();
}

}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_Types_2eproto__INCLUDED
