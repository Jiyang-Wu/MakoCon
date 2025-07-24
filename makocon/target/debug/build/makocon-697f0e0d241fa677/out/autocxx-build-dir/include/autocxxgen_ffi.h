#ifndef __AUTOCXXGEN_H__
#define __AUTOCXXGEN_H__

#include <memory>
#include <string>
#include "cxx.h"
#include <stddef.h>
#ifndef AUTOCXX_NEW_AND_DELETE_PRELUDE
#define AUTOCXX_NEW_AND_DELETE_PRELUDE
// Mechanics to call custom operator new and delete
template <typename T>
auto delete_imp(T *ptr, int) -> decltype((void)T::operator delete(ptr)) {
  T::operator delete(ptr);
}
template <typename T> void delete_imp(T *ptr, long) { ::operator delete(ptr); }
template <typename T> void delete_appropriately(T *obj) {
  // 0 is a better match for the first 'delete_imp' so will match
  // preferentially.
  delete_imp(obj, 0);
}
template <typename T>
auto new_imp(size_t count, int) -> decltype(T::operator new(count)) {
  return T::operator new(count);
}
template <typename T> void *new_imp(size_t count, long) {
  return ::operator new(count);
}
template <typename T> T *new_appropriately() {
  // 0 is a better match for the first 'delete_imp' so will match
  // preferentially.
  return static_cast<T *>(new_imp<T>(sizeof(T), 0));
}
#endif // AUTOCXX_NEW_AND_DELETE_PRELUDE
#include "kv_store.h"



inline std::unique_ptr<std::string> autocxx_make_string_0x2af6468c0a42d1cc(::rust::Str str) { return std::make_unique<std::string>(std::string(str)); }
inline KVStore* KVStore_autocxx_alloc_autocxx_wrapper_0x2af6468c0a42d1cc()  { return new_appropriately<KVStore>();; }
inline void KVStore_autocxx_free_autocxx_wrapper_0x2af6468c0a42d1cc(KVStore* arg0)  { delete_appropriately<KVStore>(arg0);; }
inline void KVStore_new_autocxx_autocxx_wrapper_0x2af6468c0a42d1cc(KVStore* autocxx_gen_this)  { new (autocxx_gen_this) KVStore(); }
inline void KVStore_destructor_autocxx_wrapper_0x2af6468c0a42d1cc(KVStore* arg0)  { arg0->KVStore::~KVStore(); }
#endif // __AUTOCXXGEN_H__
