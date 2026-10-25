//! Calls into the Ruby C API. Every call that may raise runs under
//! `rb_protect`, so a Ruby exception never unwinds through Rust frames.

use std::ffi::{CStr, CString, c_char, c_long, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;

use rb_sys::{VALUE, rb_data_type_struct__bindgen_ty_1, rb_data_type_t, size_t};

pub type RbResult<T = VALUE> = Result<T, RubyErr>;

#[derive(Debug)]
pub enum RubyErr {
    /// An exception object to raise.
    Exception(VALUE),
    /// An exception to build from a class and a message.
    Error { class: VALUE, message: String },
}

impl RubyErr {
    pub fn new(class: VALUE, message: impl Into<String>) -> Self {
        Self::Error {
            class,
            message: message.into(),
        }
    }

    pub fn arg(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eArgError }, message)
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eRuntimeError }, message)
    }

    pub fn type_error(message: impl Into<String>) -> Self {
        Self::new(unsafe { rb_sys::rb_eTypeError }, message)
    }

    fn current_exception() -> Self {
        let err = unsafe { rb_sys::rb_errinfo() };
        unsafe { rb_sys::rb_set_errinfo(qnil()) };
        if err == qnil() {
            Self::runtime("Ruby exception")
        } else {
            Self::Exception(err)
        }
    }
}

/// Runs a method body: raises its error, and turns a panic into an error.
pub fn wrap<F>(f: F) -> VALUE
where
    F: FnOnce() -> RbResult<VALUE>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(Ok(value)) => value,
        Ok(Err(err)) => raise(err),
        Err(_) => raise(RubyErr::runtime("native Rust panic")),
    }
}

pub fn raise(err: RubyErr) -> ! {
    match err {
        RubyErr::Exception(exc) => unsafe { rb_sys::rb_exc_raise(exc) },
        RubyErr::Error { class, message } => {
            let message = CString::new(message.replace('\0', "\\0")).unwrap_or_default();
            let exc = unsafe { rb_sys::rb_exc_new_cstr(class, message.as_ptr()) };
            unsafe { rb_sys::rb_exc_raise(exc) }
        }
    }
}

struct ProtectData<F> {
    func: Option<F>,
    panicked: bool,
}

/// Runs `func`, which calls into Ruby, catching a Ruby exception.
pub fn protect<F>(func: F) -> RbResult<VALUE>
where
    F: FnOnce() -> VALUE,
{
    unsafe extern "C" fn call<F>(arg: VALUE) -> VALUE
    where
        F: FnOnce() -> VALUE,
    {
        let data = unsafe { &mut *(arg as *mut ProtectData<F>) };
        let Some(func) = data.func.take() else {
            data.panicked = true;
            return qnil();
        };
        if let Ok(value) = catch_unwind(AssertUnwindSafe(func)) {
            value
        } else {
            data.panicked = true;
            qnil()
        }
    }

    let mut data = ProtectData {
        func: Some(func),
        panicked: false,
    };
    let mut state = 0;
    let value = unsafe {
        rb_sys::rb_protect(
            Some(call::<F>),
            (&raw mut data).cast::<ProtectData<F>>() as VALUE,
            &raw mut state,
        )
    };
    if state != 0 {
        Err(RubyErr::current_exception())
    } else if data.panicked {
        Err(RubyErr::runtime("native Rust panic"))
    } else {
        Ok(value)
    }
}

pub const fn qnil() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qnil as VALUE
}

pub const fn qtrue() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qtrue as VALUE
}

pub const fn qfalse() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qfalse as VALUE
}

const fn qundef() -> VALUE {
    rb_sys::ruby_special_consts::RUBY_Qundef as VALUE
}

pub fn bool_value(value: bool) -> VALUE {
    if value { qtrue() } else { qfalse() }
}

fn is_kind_of(value: VALUE, class: VALUE) -> bool {
    unsafe { rb_sys::rb_obj_is_kind_of(value, class) == qtrue() }
}

/// The value under a String key, `None` if the key is missing.
pub fn hash_get(hash: VALUE, key: &str) -> RbResult<Option<VALUE>> {
    if !is_kind_of(hash, unsafe { rb_sys::rb_cHash }) {
        return Err(RubyErr::type_error("expected a Hash"));
    }
    let key = new_utf8_string(key)?;
    let value = protect(|| unsafe { rb_sys::rb_hash_lookup2(hash, key, qundef()) })?;
    Ok((value != qundef()).then_some(value))
}

pub fn hash_new() -> RbResult<VALUE> {
    protect(|| unsafe { rb_sys::rb_hash_new() })
}

pub fn hash_aset(hash: VALUE, key: VALUE, value: VALUE) -> RbResult<()> {
    protect(|| unsafe { rb_sys::rb_hash_aset(hash, key, value) })?;
    Ok(())
}

pub fn array_new_capa(capacity: usize) -> RbResult<VALUE> {
    let capacity = c_long_len(capacity)?;
    protect(|| unsafe { rb_sys::rb_ary_new_capa(capacity) })
}

pub fn array_entries(array: VALUE) -> RbResult<Vec<VALUE>> {
    if !is_kind_of(array, unsafe { rb_sys::rb_cArray }) {
        return Err(RubyErr::type_error("expected an Array"));
    }
    let len = unsafe { rb_sys::RARRAY_LEN(array) };
    (0..len)
        .map(|i| protect(|| unsafe { rb_sys::rb_ary_entry(array, i) }))
        .collect()
}

pub fn array_push(array: VALUE, value: VALUE) -> RbResult<()> {
    protect(|| unsafe { rb_sys::rb_ary_push(array, value) })?;
    Ok(())
}

pub fn freeze(value: VALUE) -> VALUE {
    unsafe { rb_sys::rb_obj_freeze(value) }
}

pub fn symbol(name: &str) -> RbResult<VALUE> {
    let len = c_long_len(name.len())?;
    protect(|| unsafe { rb_sys::rb_id2sym(rb_sys::rb_intern2(name.as_ptr().cast(), len)) })
}

/// A frozen binary String.
pub fn new_binary_string(bytes: &[u8]) -> RbResult<VALUE> {
    let len = c_long_len(bytes.len())?;
    let ptr = if bytes.is_empty() {
        ptr::null()
    } else {
        bytes.as_ptr().cast::<c_char>()
    };
    let value = protect(|| unsafe { rb_sys::rb_str_new(ptr, len) })?;
    Ok(freeze(value))
}

pub fn new_utf8_string(text: &str) -> RbResult<VALUE> {
    let len = c_long_len(text.len())?;
    protect(|| unsafe { rb_sys::rb_utf8_str_new(text.as_ptr().cast::<c_char>(), len) })
}

/// A frozen, deduplicated UTF-8 String, for values that repeat, such as
/// topics.
pub fn interned_utf8_string(text: &str) -> RbResult<VALUE> {
    let len = c_long_len(text.len())?;
    protect(|| unsafe {
        rb_sys::rb_enc_interned_str(
            text.as_ptr().cast::<c_char>(),
            len,
            rb_sys::rb_utf8_encoding(),
        )
    })
}

/// `value` as a String, calling `to_str` if needed.
pub fn string_value(value: VALUE) -> RbResult<VALUE> {
    protect(|| unsafe { rb_sys::rb_str_to_str(value) })
}

/// The bytes of a String from [`string_value`].
///
/// # Safety
///
/// The slice points into the Ruby string. Use it before the next call
/// into Ruby, which may run GC or change the string.
pub unsafe fn string_bytes<'a>(string: VALUE) -> &'a [u8] {
    let len = unsafe { rb_sys::RSTRING_LEN(string) };
    let ptr = unsafe { rb_sys::RSTRING_PTR(string) };
    if len <= 0 || ptr.is_null() {
        return &[];
    }
    unsafe { std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize) }
}

pub fn value_to_bytes(value: VALUE) -> RbResult<Vec<u8>> {
    let string = string_value(value)?;
    Ok(unsafe { string_bytes(string) }.to_vec())
}

pub fn value_to_string(value: VALUE) -> RbResult<String> {
    String::from_utf8(value_to_bytes(value)?).map_err(|_| RubyErr::arg("expected a UTF-8 String"))
}

pub fn value_to_i64(value: VALUE) -> RbResult<i64> {
    let mut out = 0i64;
    protect(|| {
        out = unsafe { rb_sys::rb_num2ll(value) };
        qnil()
    })?;
    Ok(out)
}

pub fn value_to_f64(value: VALUE) -> RbResult<f64> {
    let mut out = 0.0f64;
    protect(|| {
        out = unsafe { rb_sys::rb_num2dbl(value) };
        qnil()
    })?;
    Ok(out)
}

pub fn value_to_bool(value: VALUE) -> RbResult<bool> {
    if value == qtrue() {
        Ok(true)
    } else if value == qfalse() || value == qnil() {
        Ok(false)
    } else {
        Err(RubyErr::type_error("expected true or false"))
    }
}

pub fn u64_value(value: u64) -> VALUE {
    unsafe { rb_sys::rb_ull2inum(value) }
}

/// Calls `receiver.name(*args)`.
pub fn call(receiver: VALUE, name: &CStr, args: &[VALUE]) -> RbResult<VALUE> {
    let count = std::os::raw::c_int::try_from(args.len())
        .map_err(|_| RubyErr::arg("too many arguments"))?;
    protect(|| unsafe {
        rb_sys::rb_funcallv(
            receiver,
            rb_sys::rb_intern(name.as_ptr()),
            count,
            args.as_ptr(),
        )
    })
}

pub fn const_get(module: VALUE, name: &CStr) -> RbResult<VALUE> {
    protect(|| unsafe { rb_sys::rb_const_get(module, rb_sys::rb_intern(name.as_ptr())) })
}

/// A Ruby type for a boxed `T`, which GC frees with the object.
pub struct DataType(rb_data_type_t);

unsafe impl Send for DataType {}
unsafe impl Sync for DataType {}

impl DataType {
    pub const fn new<T>(name: &'static CStr) -> Self {
        DataType(rb_data_type_t {
            wrap_struct_name: name.as_ptr(),
            function: rb_data_type_struct__bindgen_ty_1 {
                dmark: None,
                dfree: Some(free_box::<T>),
                dsize: Some(size_box::<T>),
                dcompact: None,
                reserved: [ptr::null_mut(); 1],
            },
            parent: ptr::null(),
            data: ptr::null_mut(),
            // RUBY_TYPED_FREE_IMMEDIATELY: dropping never blocks or calls Ruby.
            flags: 1,
        })
    }

    const fn ptr(&'static self) -> *const rb_data_type_t {
        &raw const self.0
    }

    /// Wraps `value` in a new object of `class`.
    pub fn wrap<T>(&'static self, class: VALUE, value: T) -> RbResult<VALUE> {
        let raw = Box::into_raw(Box::new(value));
        protect(|| unsafe { rb_sys::rb_data_typed_object_wrap(class, raw.cast(), self.ptr()) })
            .inspect_err(|_| drop(unsafe { Box::from_raw(raw) }))
    }

    /// The `T` inside `value`.
    ///
    /// # Safety
    ///
    /// `T` must be the type this data type was made for.
    pub unsafe fn get<T>(&'static self, value: VALUE) -> RbResult<&'static T> {
        let mut ptr = ptr::null_mut();
        protect(|| unsafe {
            ptr = rb_sys::rb_check_typeddata(value, self.ptr());
            qnil()
        })?;
        if ptr.is_null() {
            return Err(RubyErr::runtime("native object is empty"));
        }
        Ok(unsafe { &*ptr.cast::<T>() })
    }
}

unsafe extern "C" fn free_box<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| {
            drop(unsafe { Box::from_raw(ptr.cast::<T>()) });
        }));
    }
}

unsafe extern "C" fn size_box<T>(_ptr: *const c_void) -> size_t {
    std::mem::size_of::<T>() as size_t
}

pub fn define_module(name: &CStr) -> RbResult<VALUE> {
    protect(|| unsafe { rb_sys::rb_define_module(name.as_ptr()) })
}

pub fn define_module_under(outer: VALUE, name: &CStr) -> RbResult<VALUE> {
    protect(|| unsafe { rb_sys::rb_define_module_under(outer, name.as_ptr()) })
}

/// A class whose objects only native code creates.
pub fn define_class_under(outer: VALUE, name: &CStr) -> RbResult<VALUE> {
    let class = protect(|| unsafe {
        rb_sys::rb_define_class_under(outer, name.as_ptr(), rb_sys::rb_cObject)
    })?;
    protect(|| unsafe {
        rb_sys::rb_undef_alloc_func(class);
        qnil()
    })?;
    Ok(class)
}

pub fn define_const(module: VALUE, name: &CStr, value: VALUE) -> RbResult<()> {
    protect(|| unsafe {
        rb_sys::rb_define_const(module, name.as_ptr(), value);
        qnil()
    })?;
    Ok(())
}

/// A method with `arity` arguments. `func` takes `self` and then each
/// argument as a `VALUE`.
pub fn define_method(class: VALUE, name: &CStr, func: *const (), arity: i32) -> RbResult<()> {
    let func = unsafe { std::mem::transmute::<*const (), unsafe extern "C" fn() -> VALUE>(func) };
    protect(|| unsafe {
        rb_sys::rb_define_method(class, name.as_ptr(), Some(func), arity);
        qnil()
    })?;
    Ok(())
}

/// A singleton method, as [`define_method`].
pub fn define_singleton_method(
    object: VALUE,
    name: &CStr,
    func: *const (),
    arity: i32,
) -> RbResult<()> {
    let func = unsafe { std::mem::transmute::<*const (), unsafe extern "C" fn() -> VALUE>(func) };
    protect(|| unsafe {
        rb_sys::rb_define_singleton_method(object, name.as_ptr(), Some(func), arity);
        qnil()
    })?;
    Ok(())
}

fn c_long_len(len: usize) -> RbResult<c_long> {
    c_long::try_from(len).map_err(|_| RubyErr::arg("length too large"))
}
