// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: Coordination.proto

#ifndef PROTOBUF_Coordination_2eproto__INCLUDED
#define PROTOBUF_Coordination_2eproto__INCLUDED

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

namespace exec {

// Internal implementation detail -- do not call these.
void  protobuf_AddDesc_Coordination_2eproto();
void protobuf_AssignDesc_Coordination_2eproto();
void protobuf_ShutdownFile_Coordination_2eproto();

class DrillbitEndpoint;
class DrillServiceInstance;
class Roles;

enum DrillbitEndpoint_State {
  DrillbitEndpoint_State_STARTUP = 0,
  DrillbitEndpoint_State_ONLINE = 1,
  DrillbitEndpoint_State_QUIESCENT = 2,
  DrillbitEndpoint_State_OFFLINE = 3
};
bool DrillbitEndpoint_State_IsValid(int value);
const DrillbitEndpoint_State DrillbitEndpoint_State_State_MIN = DrillbitEndpoint_State_STARTUP;
const DrillbitEndpoint_State DrillbitEndpoint_State_State_MAX = DrillbitEndpoint_State_OFFLINE;
const int DrillbitEndpoint_State_State_ARRAYSIZE = DrillbitEndpoint_State_State_MAX + 1;

const ::google::protobuf::EnumDescriptor* DrillbitEndpoint_State_descriptor();
inline const ::std::string& DrillbitEndpoint_State_Name(DrillbitEndpoint_State value) {
  return ::google::protobuf::internal::NameOfEnum(
    DrillbitEndpoint_State_descriptor(), value);
}
inline bool DrillbitEndpoint_State_Parse(
    const ::std::string& name, DrillbitEndpoint_State* value) {
  return ::google::protobuf::internal::ParseNamedEnum<DrillbitEndpoint_State>(
    DrillbitEndpoint_State_descriptor(), name, value);
}
// ===================================================================

class DrillbitEndpoint : public ::google::protobuf::Message {
 public:
  DrillbitEndpoint();
  virtual ~DrillbitEndpoint();

  DrillbitEndpoint(const DrillbitEndpoint& from);

  inline DrillbitEndpoint& operator=(const DrillbitEndpoint& from) {
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
  static const DrillbitEndpoint& default_instance();

  void Swap(DrillbitEndpoint* other);

  // implements Message ----------------------------------------------

  DrillbitEndpoint* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const DrillbitEndpoint& from);
  void MergeFrom(const DrillbitEndpoint& from);
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

  typedef DrillbitEndpoint_State State;
  static const State STARTUP = DrillbitEndpoint_State_STARTUP;
  static const State ONLINE = DrillbitEndpoint_State_ONLINE;
  static const State QUIESCENT = DrillbitEndpoint_State_QUIESCENT;
  static const State OFFLINE = DrillbitEndpoint_State_OFFLINE;
  static inline bool State_IsValid(int value) {
    return DrillbitEndpoint_State_IsValid(value);
  }
  static const State State_MIN =
    DrillbitEndpoint_State_State_MIN;
  static const State State_MAX =
    DrillbitEndpoint_State_State_MAX;
  static const int State_ARRAYSIZE =
    DrillbitEndpoint_State_State_ARRAYSIZE;
  static inline const ::google::protobuf::EnumDescriptor*
  State_descriptor() {
    return DrillbitEndpoint_State_descriptor();
  }
  static inline const ::std::string& State_Name(State value) {
    return DrillbitEndpoint_State_Name(value);
  }
  static inline bool State_Parse(const ::std::string& name,
      State* value) {
    return DrillbitEndpoint_State_Parse(name, value);
  }

  // accessors -------------------------------------------------------

  // optional string address = 1;
  inline bool has_address() const;
  inline void clear_address();
  static const int kAddressFieldNumber = 1;
  inline const ::std::string& address() const;
  inline void set_address(const ::std::string& value);
  inline void set_address(const char* value);
  inline void set_address(const char* value, size_t size);
  inline ::std::string* mutable_address();
  inline ::std::string* release_address();
  inline void set_allocated_address(::std::string* address);

  // optional int32 user_port = 2;
  inline bool has_user_port() const;
  inline void clear_user_port();
  static const int kUserPortFieldNumber = 2;
  inline ::google::protobuf::int32 user_port() const;
  inline void set_user_port(::google::protobuf::int32 value);

  // optional int32 control_port = 3;
  inline bool has_control_port() const;
  inline void clear_control_port();
  static const int kControlPortFieldNumber = 3;
  inline ::google::protobuf::int32 control_port() const;
  inline void set_control_port(::google::protobuf::int32 value);

  // optional int32 data_port = 4;
  inline bool has_data_port() const;
  inline void clear_data_port();
  static const int kDataPortFieldNumber = 4;
  inline ::google::protobuf::int32 data_port() const;
  inline void set_data_port(::google::protobuf::int32 value);

  // optional .exec.Roles roles = 5;
  inline bool has_roles() const;
  inline void clear_roles();
  static const int kRolesFieldNumber = 5;
  inline const ::exec::Roles& roles() const;
  inline ::exec::Roles* mutable_roles();
  inline ::exec::Roles* release_roles();
  inline void set_allocated_roles(::exec::Roles* roles);

  // optional string version = 6;
  inline bool has_version() const;
  inline void clear_version();
  static const int kVersionFieldNumber = 6;
  inline const ::std::string& version() const;
  inline void set_version(const ::std::string& value);
  inline void set_version(const char* value);
  inline void set_version(const char* value, size_t size);
  inline ::std::string* mutable_version();
  inline ::std::string* release_version();
  inline void set_allocated_version(::std::string* version);

  // optional .exec.DrillbitEndpoint.State state = 7;
  inline bool has_state() const;
  inline void clear_state();
  static const int kStateFieldNumber = 7;
  inline ::exec::DrillbitEndpoint_State state() const;
  inline void set_state(::exec::DrillbitEndpoint_State value);

  // optional int32 http_port = 8;
  inline bool has_http_port() const;
  inline void clear_http_port();
  static const int kHttpPortFieldNumber = 8;
  inline ::google::protobuf::int32 http_port() const;
  inline void set_http_port(::google::protobuf::int32 value);

  // @@protoc_insertion_point(class_scope:exec.DrillbitEndpoint)
 private:
  inline void set_has_address();
  inline void clear_has_address();
  inline void set_has_user_port();
  inline void clear_has_user_port();
  inline void set_has_control_port();
  inline void clear_has_control_port();
  inline void set_has_data_port();
  inline void clear_has_data_port();
  inline void set_has_roles();
  inline void clear_has_roles();
  inline void set_has_version();
  inline void clear_has_version();
  inline void set_has_state();
  inline void clear_has_state();
  inline void set_has_http_port();
  inline void clear_has_http_port();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::std::string* address_;
  ::google::protobuf::int32 user_port_;
  ::google::protobuf::int32 control_port_;
  ::exec::Roles* roles_;
  ::google::protobuf::int32 data_port_;
  int state_;
  ::std::string* version_;
  ::google::protobuf::int32 http_port_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(8 + 31) / 32];

  friend void  protobuf_AddDesc_Coordination_2eproto();
  friend void protobuf_AssignDesc_Coordination_2eproto();
  friend void protobuf_ShutdownFile_Coordination_2eproto();

  void InitAsDefaultInstance();
  static DrillbitEndpoint* default_instance_;
};
// -------------------------------------------------------------------

class DrillServiceInstance : public ::google::protobuf::Message {
 public:
  DrillServiceInstance();
  virtual ~DrillServiceInstance();

  DrillServiceInstance(const DrillServiceInstance& from);

  inline DrillServiceInstance& operator=(const DrillServiceInstance& from) {
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
  static const DrillServiceInstance& default_instance();

  void Swap(DrillServiceInstance* other);

  // implements Message ----------------------------------------------

  DrillServiceInstance* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const DrillServiceInstance& from);
  void MergeFrom(const DrillServiceInstance& from);
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

  // optional string id = 1;
  inline bool has_id() const;
  inline void clear_id();
  static const int kIdFieldNumber = 1;
  inline const ::std::string& id() const;
  inline void set_id(const ::std::string& value);
  inline void set_id(const char* value);
  inline void set_id(const char* value, size_t size);
  inline ::std::string* mutable_id();
  inline ::std::string* release_id();
  inline void set_allocated_id(::std::string* id);

  // optional int64 registrationTimeUTC = 2;
  inline bool has_registrationtimeutc() const;
  inline void clear_registrationtimeutc();
  static const int kRegistrationTimeUTCFieldNumber = 2;
  inline ::google::protobuf::int64 registrationtimeutc() const;
  inline void set_registrationtimeutc(::google::protobuf::int64 value);

  // optional .exec.DrillbitEndpoint endpoint = 3;
  inline bool has_endpoint() const;
  inline void clear_endpoint();
  static const int kEndpointFieldNumber = 3;
  inline const ::exec::DrillbitEndpoint& endpoint() const;
  inline ::exec::DrillbitEndpoint* mutable_endpoint();
  inline ::exec::DrillbitEndpoint* release_endpoint();
  inline void set_allocated_endpoint(::exec::DrillbitEndpoint* endpoint);

  // @@protoc_insertion_point(class_scope:exec.DrillServiceInstance)
 private:
  inline void set_has_id();
  inline void clear_has_id();
  inline void set_has_registrationtimeutc();
  inline void clear_has_registrationtimeutc();
  inline void set_has_endpoint();
  inline void clear_has_endpoint();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  ::std::string* id_;
  ::google::protobuf::int64 registrationtimeutc_;
  ::exec::DrillbitEndpoint* endpoint_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(3 + 31) / 32];

  friend void  protobuf_AddDesc_Coordination_2eproto();
  friend void protobuf_AssignDesc_Coordination_2eproto();
  friend void protobuf_ShutdownFile_Coordination_2eproto();

  void InitAsDefaultInstance();
  static DrillServiceInstance* default_instance_;
};
// -------------------------------------------------------------------

class Roles : public ::google::protobuf::Message {
 public:
  Roles();
  virtual ~Roles();

  Roles(const Roles& from);

  inline Roles& operator=(const Roles& from) {
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
  static const Roles& default_instance();

  void Swap(Roles* other);

  // implements Message ----------------------------------------------

  Roles* New() const;
  void CopyFrom(const ::google::protobuf::Message& from);
  void MergeFrom(const ::google::protobuf::Message& from);
  void CopyFrom(const Roles& from);
  void MergeFrom(const Roles& from);
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

  // optional bool sql_query = 1 [default = true];
  inline bool has_sql_query() const;
  inline void clear_sql_query();
  static const int kSqlQueryFieldNumber = 1;
  inline bool sql_query() const;
  inline void set_sql_query(bool value);

  // optional bool logical_plan = 2 [default = true];
  inline bool has_logical_plan() const;
  inline void clear_logical_plan();
  static const int kLogicalPlanFieldNumber = 2;
  inline bool logical_plan() const;
  inline void set_logical_plan(bool value);

  // optional bool physical_plan = 3 [default = true];
  inline bool has_physical_plan() const;
  inline void clear_physical_plan();
  static const int kPhysicalPlanFieldNumber = 3;
  inline bool physical_plan() const;
  inline void set_physical_plan(bool value);

  // optional bool java_executor = 4 [default = true];
  inline bool has_java_executor() const;
  inline void clear_java_executor();
  static const int kJavaExecutorFieldNumber = 4;
  inline bool java_executor() const;
  inline void set_java_executor(bool value);

  // optional bool distributed_cache = 5 [default = true];
  inline bool has_distributed_cache() const;
  inline void clear_distributed_cache();
  static const int kDistributedCacheFieldNumber = 5;
  inline bool distributed_cache() const;
  inline void set_distributed_cache(bool value);

  // @@protoc_insertion_point(class_scope:exec.Roles)
 private:
  inline void set_has_sql_query();
  inline void clear_has_sql_query();
  inline void set_has_logical_plan();
  inline void clear_has_logical_plan();
  inline void set_has_physical_plan();
  inline void clear_has_physical_plan();
  inline void set_has_java_executor();
  inline void clear_has_java_executor();
  inline void set_has_distributed_cache();
  inline void clear_has_distributed_cache();

  ::google::protobuf::UnknownFieldSet _unknown_fields_;

  bool sql_query_;
  bool logical_plan_;
  bool physical_plan_;
  bool java_executor_;
  bool distributed_cache_;

  mutable int _cached_size_;
  ::google::protobuf::uint32 _has_bits_[(5 + 31) / 32];

  friend void  protobuf_AddDesc_Coordination_2eproto();
  friend void protobuf_AssignDesc_Coordination_2eproto();
  friend void protobuf_ShutdownFile_Coordination_2eproto();

  void InitAsDefaultInstance();
  static Roles* default_instance_;
};
// ===================================================================


// ===================================================================

// DrillbitEndpoint

// optional string address = 1;
inline bool DrillbitEndpoint::has_address() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void DrillbitEndpoint::set_has_address() {
  _has_bits_[0] |= 0x00000001u;
}
inline void DrillbitEndpoint::clear_has_address() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void DrillbitEndpoint::clear_address() {
  if (address_ != &::google::protobuf::internal::kEmptyString) {
    address_->clear();
  }
  clear_has_address();
}
inline const ::std::string& DrillbitEndpoint::address() const {
  return *address_;
}
inline void DrillbitEndpoint::set_address(const ::std::string& value) {
  set_has_address();
  if (address_ == &::google::protobuf::internal::kEmptyString) {
    address_ = new ::std::string;
  }
  address_->assign(value);
}
inline void DrillbitEndpoint::set_address(const char* value) {
  set_has_address();
  if (address_ == &::google::protobuf::internal::kEmptyString) {
    address_ = new ::std::string;
  }
  address_->assign(value);
}
inline void DrillbitEndpoint::set_address(const char* value, size_t size) {
  set_has_address();
  if (address_ == &::google::protobuf::internal::kEmptyString) {
    address_ = new ::std::string;
  }
  address_->assign(reinterpret_cast<const char*>(value), size);
}
inline ::std::string* DrillbitEndpoint::mutable_address() {
  set_has_address();
  if (address_ == &::google::protobuf::internal::kEmptyString) {
    address_ = new ::std::string;
  }
  return address_;
}
inline ::std::string* DrillbitEndpoint::release_address() {
  clear_has_address();
  if (address_ == &::google::protobuf::internal::kEmptyString) {
    return NULL;
  } else {
    ::std::string* temp = address_;
    address_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
    return temp;
  }
}
inline void DrillbitEndpoint::set_allocated_address(::std::string* address) {
  if (address_ != &::google::protobuf::internal::kEmptyString) {
    delete address_;
  }
  if (address) {
    set_has_address();
    address_ = address;
  } else {
    clear_has_address();
    address_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  }
}

// optional int32 user_port = 2;
inline bool DrillbitEndpoint::has_user_port() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void DrillbitEndpoint::set_has_user_port() {
  _has_bits_[0] |= 0x00000002u;
}
inline void DrillbitEndpoint::clear_has_user_port() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void DrillbitEndpoint::clear_user_port() {
  user_port_ = 0;
  clear_has_user_port();
}
inline ::google::protobuf::int32 DrillbitEndpoint::user_port() const {
  return user_port_;
}
inline void DrillbitEndpoint::set_user_port(::google::protobuf::int32 value) {
  set_has_user_port();
  user_port_ = value;
}

// optional int32 control_port = 3;
inline bool DrillbitEndpoint::has_control_port() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void DrillbitEndpoint::set_has_control_port() {
  _has_bits_[0] |= 0x00000004u;
}
inline void DrillbitEndpoint::clear_has_control_port() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void DrillbitEndpoint::clear_control_port() {
  control_port_ = 0;
  clear_has_control_port();
}
inline ::google::protobuf::int32 DrillbitEndpoint::control_port() const {
  return control_port_;
}
inline void DrillbitEndpoint::set_control_port(::google::protobuf::int32 value) {
  set_has_control_port();
  control_port_ = value;
}

// optional int32 data_port = 4;
inline bool DrillbitEndpoint::has_data_port() const {
  return (_has_bits_[0] & 0x00000008u) != 0;
}
inline void DrillbitEndpoint::set_has_data_port() {
  _has_bits_[0] |= 0x00000008u;
}
inline void DrillbitEndpoint::clear_has_data_port() {
  _has_bits_[0] &= ~0x00000008u;
}
inline void DrillbitEndpoint::clear_data_port() {
  data_port_ = 0;
  clear_has_data_port();
}
inline ::google::protobuf::int32 DrillbitEndpoint::data_port() const {
  return data_port_;
}
inline void DrillbitEndpoint::set_data_port(::google::protobuf::int32 value) {
  set_has_data_port();
  data_port_ = value;
}

// optional .exec.Roles roles = 5;
inline bool DrillbitEndpoint::has_roles() const {
  return (_has_bits_[0] & 0x00000010u) != 0;
}
inline void DrillbitEndpoint::set_has_roles() {
  _has_bits_[0] |= 0x00000010u;
}
inline void DrillbitEndpoint::clear_has_roles() {
  _has_bits_[0] &= ~0x00000010u;
}
inline void DrillbitEndpoint::clear_roles() {
  if (roles_ != NULL) roles_->::exec::Roles::Clear();
  clear_has_roles();
}
inline const ::exec::Roles& DrillbitEndpoint::roles() const {
  return roles_ != NULL ? *roles_ : *default_instance_->roles_;
}
inline ::exec::Roles* DrillbitEndpoint::mutable_roles() {
  set_has_roles();
  if (roles_ == NULL) roles_ = new ::exec::Roles;
  return roles_;
}
inline ::exec::Roles* DrillbitEndpoint::release_roles() {
  clear_has_roles();
  ::exec::Roles* temp = roles_;
  roles_ = NULL;
  return temp;
}
inline void DrillbitEndpoint::set_allocated_roles(::exec::Roles* roles) {
  delete roles_;
  roles_ = roles;
  if (roles) {
    set_has_roles();
  } else {
    clear_has_roles();
  }
}

// optional string version = 6;
inline bool DrillbitEndpoint::has_version() const {
  return (_has_bits_[0] & 0x00000020u) != 0;
}
inline void DrillbitEndpoint::set_has_version() {
  _has_bits_[0] |= 0x00000020u;
}
inline void DrillbitEndpoint::clear_has_version() {
  _has_bits_[0] &= ~0x00000020u;
}
inline void DrillbitEndpoint::clear_version() {
  if (version_ != &::google::protobuf::internal::kEmptyString) {
    version_->clear();
  }
  clear_has_version();
}
inline const ::std::string& DrillbitEndpoint::version() const {
  return *version_;
}
inline void DrillbitEndpoint::set_version(const ::std::string& value) {
  set_has_version();
  if (version_ == &::google::protobuf::internal::kEmptyString) {
    version_ = new ::std::string;
  }
  version_->assign(value);
}
inline void DrillbitEndpoint::set_version(const char* value) {
  set_has_version();
  if (version_ == &::google::protobuf::internal::kEmptyString) {
    version_ = new ::std::string;
  }
  version_->assign(value);
}
inline void DrillbitEndpoint::set_version(const char* value, size_t size) {
  set_has_version();
  if (version_ == &::google::protobuf::internal::kEmptyString) {
    version_ = new ::std::string;
  }
  version_->assign(reinterpret_cast<const char*>(value), size);
}
inline ::std::string* DrillbitEndpoint::mutable_version() {
  set_has_version();
  if (version_ == &::google::protobuf::internal::kEmptyString) {
    version_ = new ::std::string;
  }
  return version_;
}
inline ::std::string* DrillbitEndpoint::release_version() {
  clear_has_version();
  if (version_ == &::google::protobuf::internal::kEmptyString) {
    return NULL;
  } else {
    ::std::string* temp = version_;
    version_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
    return temp;
  }
}
inline void DrillbitEndpoint::set_allocated_version(::std::string* version) {
  if (version_ != &::google::protobuf::internal::kEmptyString) {
    delete version_;
  }
  if (version) {
    set_has_version();
    version_ = version;
  } else {
    clear_has_version();
    version_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  }
}

// optional .exec.DrillbitEndpoint.State state = 7;
inline bool DrillbitEndpoint::has_state() const {
  return (_has_bits_[0] & 0x00000040u) != 0;
}
inline void DrillbitEndpoint::set_has_state() {
  _has_bits_[0] |= 0x00000040u;
}
inline void DrillbitEndpoint::clear_has_state() {
  _has_bits_[0] &= ~0x00000040u;
}
inline void DrillbitEndpoint::clear_state() {
  state_ = 0;
  clear_has_state();
}
inline ::exec::DrillbitEndpoint_State DrillbitEndpoint::state() const {
  return static_cast< ::exec::DrillbitEndpoint_State >(state_);
}
inline void DrillbitEndpoint::set_state(::exec::DrillbitEndpoint_State value) {
  assert(::exec::DrillbitEndpoint_State_IsValid(value));
  set_has_state();
  state_ = value;
}

// optional int32 http_port = 8;
inline bool DrillbitEndpoint::has_http_port() const {
  return (_has_bits_[0] & 0x00000080u) != 0;
}
inline void DrillbitEndpoint::set_has_http_port() {
  _has_bits_[0] |= 0x00000080u;
}
inline void DrillbitEndpoint::clear_has_http_port() {
  _has_bits_[0] &= ~0x00000080u;
}
inline void DrillbitEndpoint::clear_http_port() {
  http_port_ = 0;
  clear_has_http_port();
}
inline ::google::protobuf::int32 DrillbitEndpoint::http_port() const {
  return http_port_;
}
inline void DrillbitEndpoint::set_http_port(::google::protobuf::int32 value) {
  set_has_http_port();
  http_port_ = value;
}

// -------------------------------------------------------------------

// DrillServiceInstance

// optional string id = 1;
inline bool DrillServiceInstance::has_id() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void DrillServiceInstance::set_has_id() {
  _has_bits_[0] |= 0x00000001u;
}
inline void DrillServiceInstance::clear_has_id() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void DrillServiceInstance::clear_id() {
  if (id_ != &::google::protobuf::internal::kEmptyString) {
    id_->clear();
  }
  clear_has_id();
}
inline const ::std::string& DrillServiceInstance::id() const {
  return *id_;
}
inline void DrillServiceInstance::set_id(const ::std::string& value) {
  set_has_id();
  if (id_ == &::google::protobuf::internal::kEmptyString) {
    id_ = new ::std::string;
  }
  id_->assign(value);
}
inline void DrillServiceInstance::set_id(const char* value) {
  set_has_id();
  if (id_ == &::google::protobuf::internal::kEmptyString) {
    id_ = new ::std::string;
  }
  id_->assign(value);
}
inline void DrillServiceInstance::set_id(const char* value, size_t size) {
  set_has_id();
  if (id_ == &::google::protobuf::internal::kEmptyString) {
    id_ = new ::std::string;
  }
  id_->assign(reinterpret_cast<const char*>(value), size);
}
inline ::std::string* DrillServiceInstance::mutable_id() {
  set_has_id();
  if (id_ == &::google::protobuf::internal::kEmptyString) {
    id_ = new ::std::string;
  }
  return id_;
}
inline ::std::string* DrillServiceInstance::release_id() {
  clear_has_id();
  if (id_ == &::google::protobuf::internal::kEmptyString) {
    return NULL;
  } else {
    ::std::string* temp = id_;
    id_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
    return temp;
  }
}
inline void DrillServiceInstance::set_allocated_id(::std::string* id) {
  if (id_ != &::google::protobuf::internal::kEmptyString) {
    delete id_;
  }
  if (id) {
    set_has_id();
    id_ = id;
  } else {
    clear_has_id();
    id_ = const_cast< ::std::string*>(&::google::protobuf::internal::kEmptyString);
  }
}

// optional int64 registrationTimeUTC = 2;
inline bool DrillServiceInstance::has_registrationtimeutc() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void DrillServiceInstance::set_has_registrationtimeutc() {
  _has_bits_[0] |= 0x00000002u;
}
inline void DrillServiceInstance::clear_has_registrationtimeutc() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void DrillServiceInstance::clear_registrationtimeutc() {
  registrationtimeutc_ = GOOGLE_LONGLONG(0);
  clear_has_registrationtimeutc();
}
inline ::google::protobuf::int64 DrillServiceInstance::registrationtimeutc() const {
  return registrationtimeutc_;
}
inline void DrillServiceInstance::set_registrationtimeutc(::google::protobuf::int64 value) {
  set_has_registrationtimeutc();
  registrationtimeutc_ = value;
}

// optional .exec.DrillbitEndpoint endpoint = 3;
inline bool DrillServiceInstance::has_endpoint() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void DrillServiceInstance::set_has_endpoint() {
  _has_bits_[0] |= 0x00000004u;
}
inline void DrillServiceInstance::clear_has_endpoint() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void DrillServiceInstance::clear_endpoint() {
  if (endpoint_ != NULL) endpoint_->::exec::DrillbitEndpoint::Clear();
  clear_has_endpoint();
}
inline const ::exec::DrillbitEndpoint& DrillServiceInstance::endpoint() const {
  return endpoint_ != NULL ? *endpoint_ : *default_instance_->endpoint_;
}
inline ::exec::DrillbitEndpoint* DrillServiceInstance::mutable_endpoint() {
  set_has_endpoint();
  if (endpoint_ == NULL) endpoint_ = new ::exec::DrillbitEndpoint;
  return endpoint_;
}
inline ::exec::DrillbitEndpoint* DrillServiceInstance::release_endpoint() {
  clear_has_endpoint();
  ::exec::DrillbitEndpoint* temp = endpoint_;
  endpoint_ = NULL;
  return temp;
}
inline void DrillServiceInstance::set_allocated_endpoint(::exec::DrillbitEndpoint* endpoint) {
  delete endpoint_;
  endpoint_ = endpoint;
  if (endpoint) {
    set_has_endpoint();
  } else {
    clear_has_endpoint();
  }
}

// -------------------------------------------------------------------

// Roles

// optional bool sql_query = 1 [default = true];
inline bool Roles::has_sql_query() const {
  return (_has_bits_[0] & 0x00000001u) != 0;
}
inline void Roles::set_has_sql_query() {
  _has_bits_[0] |= 0x00000001u;
}
inline void Roles::clear_has_sql_query() {
  _has_bits_[0] &= ~0x00000001u;
}
inline void Roles::clear_sql_query() {
  sql_query_ = true;
  clear_has_sql_query();
}
inline bool Roles::sql_query() const {
  return sql_query_;
}
inline void Roles::set_sql_query(bool value) {
  set_has_sql_query();
  sql_query_ = value;
}

// optional bool logical_plan = 2 [default = true];
inline bool Roles::has_logical_plan() const {
  return (_has_bits_[0] & 0x00000002u) != 0;
}
inline void Roles::set_has_logical_plan() {
  _has_bits_[0] |= 0x00000002u;
}
inline void Roles::clear_has_logical_plan() {
  _has_bits_[0] &= ~0x00000002u;
}
inline void Roles::clear_logical_plan() {
  logical_plan_ = true;
  clear_has_logical_plan();
}
inline bool Roles::logical_plan() const {
  return logical_plan_;
}
inline void Roles::set_logical_plan(bool value) {
  set_has_logical_plan();
  logical_plan_ = value;
}

// optional bool physical_plan = 3 [default = true];
inline bool Roles::has_physical_plan() const {
  return (_has_bits_[0] & 0x00000004u) != 0;
}
inline void Roles::set_has_physical_plan() {
  _has_bits_[0] |= 0x00000004u;
}
inline void Roles::clear_has_physical_plan() {
  _has_bits_[0] &= ~0x00000004u;
}
inline void Roles::clear_physical_plan() {
  physical_plan_ = true;
  clear_has_physical_plan();
}
inline bool Roles::physical_plan() const {
  return physical_plan_;
}
inline void Roles::set_physical_plan(bool value) {
  set_has_physical_plan();
  physical_plan_ = value;
}

// optional bool java_executor = 4 [default = true];
inline bool Roles::has_java_executor() const {
  return (_has_bits_[0] & 0x00000008u) != 0;
}
inline void Roles::set_has_java_executor() {
  _has_bits_[0] |= 0x00000008u;
}
inline void Roles::clear_has_java_executor() {
  _has_bits_[0] &= ~0x00000008u;
}
inline void Roles::clear_java_executor() {
  java_executor_ = true;
  clear_has_java_executor();
}
inline bool Roles::java_executor() const {
  return java_executor_;
}
inline void Roles::set_java_executor(bool value) {
  set_has_java_executor();
  java_executor_ = value;
}

// optional bool distributed_cache = 5 [default = true];
inline bool Roles::has_distributed_cache() const {
  return (_has_bits_[0] & 0x00000010u) != 0;
}
inline void Roles::set_has_distributed_cache() {
  _has_bits_[0] |= 0x00000010u;
}
inline void Roles::clear_has_distributed_cache() {
  _has_bits_[0] &= ~0x00000010u;
}
inline void Roles::clear_distributed_cache() {
  distributed_cache_ = true;
  clear_has_distributed_cache();
}
inline bool Roles::distributed_cache() const {
  return distributed_cache_;
}
inline void Roles::set_distributed_cache(bool value) {
  set_has_distributed_cache();
  distributed_cache_ = value;
}


// @@protoc_insertion_point(namespace_scope)

}  // namespace exec

#ifndef SWIG
namespace google {
namespace protobuf {

template <>
inline const EnumDescriptor* GetEnumDescriptor< ::exec::DrillbitEndpoint_State>() {
  return ::exec::DrillbitEndpoint_State_descriptor();
}

}  // namespace google
}  // namespace protobuf
#endif  // SWIG

// @@protoc_insertion_point(global_scope)

#endif  // PROTOBUF_Coordination_2eproto__INCLUDED
