//! The module's C ABI. Pointers and lengths are 32-bit linear-memory offsets
//! and sizes; results pack `(pointer << 32) | length` of a UTF-8 JSON object
//! the caller releases with `ffkv_free`.
use std::{
    alloc::{Layout, alloc, dealloc, handle_alloc_error},
    cell::RefCell,
    ptr::NonNull,
};

use super::playground::Playground;

thread_local! {
    static PLAYGROUND: RefCell<Playground> = RefCell::new(Playground::new());
}

fn layout(len: u32) -> Layout {
    Layout::array::<u8>(len as usize).expect("buffer fits the address space")
}

/// Reserves `len` bytes the caller fills and later passes to `ffkv_free`.
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_alloc(len: u32) -> u32 {
    if len == 0 {
        return NonNull::<u8>::dangling().as_ptr() as usize as u32;
    }
    let layout = layout(len);
    // SAFETY: `layout` has a nonzero size.
    let ptr = unsafe { alloc(layout) };
    if ptr.is_null() {
        handle_alloc_error(layout);
    }
    ptr as usize as u32
}

/// Releases a buffer from `ffkv_alloc` or a result, with its exact length.
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_free(ptr: u32, len: u32) {
    if len == 0 {
        return;
    }
    // SAFETY: the caller returns a live `len`-byte allocation made with this
    // layout by `ffkv_alloc` or `result`.
    unsafe { dealloc(ptr as usize as *mut u8, layout(len)) }
}

/// Opens a client session (id >= 1) on the shared dataset.
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_open() -> u32 {
    PLAYGROUND.with_borrow_mut(Playground::open)
}

#[unsafe(no_mangle)]
pub extern "C" fn ffkv_close(id: u32) {
    PLAYGROUND.with_borrow_mut(|playground| playground.close(id));
}

/// Runs one ffkv-cli command line for session `id`.
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_eval(id: u32, ptr: u32, len: u32) -> u64 {
    let line: &[u8] = if len == 0 {
        &[]
    } else {
        // SAFETY: the caller passes `len` initialized bytes at `ptr` that stay
        // untouched for this call.
        unsafe { std::slice::from_raw_parts(ptr as usize as *const u8, len as usize) }
    };
    result(PLAYGROUND.with_borrow_mut(|playground| playground.eval(id, line)))
}

/// Output that became ready for session `id` since its last eval or poll.
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_poll(id: u32) -> u64 {
    result(PLAYGROUND.with_borrow_mut(|playground| playground.poll(id)))
}

/// Empties the dataset (every database).
#[unsafe(no_mangle)]
pub extern "C" fn ffkv_reset() {
    PLAYGROUND.with_borrow_mut(Playground::reset);
}

fn result(json: String) -> u64 {
    let bytes = json.into_bytes().into_boxed_slice();
    let len = bytes.len() as u64;
    let ptr = Box::into_raw(bytes) as *mut u8 as usize as u64;
    (ptr << 32) | len
}
