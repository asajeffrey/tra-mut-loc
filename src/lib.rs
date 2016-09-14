/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use std::cell::{UnsafeCell};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

/// A trait for copyable data that contains no pointers.
pub unsafe trait TCopy: Copy {}
unsafe impl TCopy for bool {}
unsafe impl TCopy for f32 {}
unsafe impl TCopy for f64 {}
unsafe impl TCopy for i8 {}
unsafe impl TCopy for i16 {}
unsafe impl TCopy for i32 {}
unsafe impl TCopy for i64 {}
unsafe impl TCopy for isize {}
unsafe impl TCopy for u8 {}
unsafe impl TCopy for u16 {}
unsafe impl TCopy for u32 {}
unsafe impl TCopy for u64 {}
unsafe impl TCopy for usize {}

pub trait Region: 'static + Sync + Send {
    fn mkcell<T>(&self, init: T) -> TCell<Self, T>;
    fn rw_transaction<'a>(&'a self) -> RWTransaction<'a, Self>;
    fn ro_transaction<'a>(&'a self) -> ROTransaction<'a, Self>;
}

pub trait RegionConsumer {
    fn consume<R: Region>(self, r: R);
}

#[derive(Debug)]
struct RegionImpl {
    version: AtomicUsize,
    lock: Mutex<()>,
}

#[derive(Debug)]
pub struct TCell<R: ?Sized, T> {
    version: AtomicUsize,
    contents: UnsafeCell<T>,
    phantom: PhantomData<R>,
}

pub struct RWTransaction<'a, R: 'a+?Sized> {
    version: usize,
    region_impl: &'a RegionImpl,
    region: &'a R,
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
}

impl<'a, R:?Sized> fmt::Debug for RWTransaction<'a, R> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> Result<(), fmt::Error> {
        fmt.debug_struct("RWTransaction")
            .field("version", &self.version)
            .finish()
    }
}

#[derive(Copy, Clone, Debug)]
pub struct ROTransaction<'a, R: ?Sized> {
    version: usize,
    phantom: PhantomData<(&'a (), R)>,
}

#[derive(Debug)]
pub struct RWRef<'a, T> where 'a, T: 'a {
    tx_version: usize,
    cell_version: &'a AtomicUsize,
    data: &'a mut T,
}

#[derive(Debug)]
pub struct RORef<'a, T> where 'a, T: 'a {
    tx_version: usize,
    cell_version: &'a AtomicUsize,
    data: *const T,
}

impl<'a, T> Clone for RORef<'a, T> {
    fn clone(&self) -> RORef<'a, T> {
        RORef {
            tx_version: self.tx_version,
            cell_version: self.cell_version,
            data: self.data,
        }
    }
}

#[derive(Copy, Clone, Debug)]
pub struct TransactionErr;

unsafe impl<R: ?Sized, T: Sync> Sync for TCell<R, T> {}
unsafe impl<R: ?Sized, T: Send> Send for TCell<R, T> {}

impl Region for RegionImpl {
    fn mkcell<T>(&self, init: T) -> TCell<RegionImpl, T> {
        TCell {
            version: AtomicUsize::new(0),
            contents: UnsafeCell::new(init),
            phantom: PhantomData,
        }
    }
    fn rw_transaction<'a>(&'a self) -> RWTransaction<'a, RegionImpl> {
        let guard = self.lock.lock().unwrap();
        let version = 1 + self.version.load(Ordering::Acquire);
        self.version.store(version, Ordering::Release);
        RWTransaction {
            version: version,
            region_impl: self,
            region: self,
            guard: guard,
        }
    }
    fn ro_transaction<'a>(&'a self) -> ROTransaction<'a, RegionImpl> {
        let version = self.version.load(Ordering::Acquire);
        ROTransaction {
            version: version,
            phantom: PhantomData,
        }
    }
}

impl<'a, T> Deref for RWRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &*self.data
    }
}

impl<'a, T> DerefMut for RWRef<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.cell_version.store(self.tx_version, Ordering::Release);
        &mut *self.data
    }
}

impl<'a, T> RORef<'a, T> {
    pub fn touch(&self) -> Result<(), TransactionErr> {
        // I think we need a CAS here for its ordering effects
        if self.cell_version.compare_and_swap(0, 0, Ordering::AcqRel) < self.tx_version {
            Ok(())
        } else {
            Err(TransactionErr)
        }
    }
    pub fn get(&self) -> Result<T, TransactionErr> where T: TCopy {
        let result = unsafe { *self.data };
        try!(self.touch());
        Ok(result)
    }
}

impl<'a, R: ?Sized> Drop for RWTransaction<'a, R> {
    fn drop(&mut self) {
        self.region_impl.version.store(self.version + 1, Ordering::Release);
    }
}

impl<'a, R> RWTransaction<'a, R> where R: Region {
    pub fn borrow<'b, T>(&'b self, cell: &'b TCell<R, T>) -> &'b T {
        unsafe { cell.contents.get().as_ref().unwrap() }
    }
    pub fn mut_borrow<'b, T>(&'b mut self, cell: &'b TCell<R, T>) -> RWRef<'b, T> {
        RWRef {
            tx_version: self.version,
            cell_version: &cell.version,
            data: unsafe { cell.contents.get().as_mut().unwrap() },
        }
    }
    pub fn borrow_mut<'b, T>(&'b self, cell: &'b mut TCell<R, T>) -> RWRef<'b, T> {
        RWRef {
            tx_version: self.version,
            cell_version: &cell.version,
            data: unsafe { cell.contents.get().as_mut().unwrap() },
        }
    }
    pub fn mkcell<T>(&self, init: T) -> TCell<R, T> {
        self.region.mkcell(init)
    }
}

impl<'a, R> ROTransaction<'a, R> {
    pub fn borrow<'b, T>(&'b self, cell: &'b TCell<R, T>) -> RORef<'b, T> {
        RORef {
            tx_version: self.version,
            cell_version: &cell.version,
            data: cell.contents.get(),
        }
    }
    pub fn get<'b, T: TCopy>(&'b self, cell: &'b TCell<R, T>) -> Result<T, TransactionErr> {
        self.borrow(cell).get()
    }
}

pub fn mkregion<C: RegionConsumer>(consumer: C) {
    consumer.consume(RegionImpl{
        version: AtomicUsize::new(1),
        lock: Mutex::new(()),
    })
}

#[cfg(test)] use std::thread;
#[cfg(test)] use std::sync::Arc;

#[test]
fn test_ro() {
    struct Test;
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let x = r.mkcell(37);
            let tx = r.ro_transaction();
            assert_eq!(37, tx.get(&x).unwrap());
            assert_eq!(37, tx.borrow(&x).get().unwrap());
        }
    }
    mkregion(Test);
}

#[test]
fn test_rw() {
    struct Test;
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let mut x = r.mkcell(37);
            let mut tx = r.rw_transaction();
            assert_eq!(37, *tx.borrow(&x));
            *tx.mut_borrow(&x) = 5;
            assert_eq!(5, *tx.borrow(&x));
            *tx.borrow_mut(&mut x) = 42;
            assert_eq!(42, *tx.borrow(&x));
        }
    }
    mkregion(Test);
}

// #[test]
// fn test_index() {
//     struct Test;
//     impl RegionConsumer for Test {
//         fn consume<R: Region>(self, r: R) {
//             let xs = r.mkcell(vec![ 1, 2, 3 ]);
//             let tx = r.ro_transaction();
//             for (i, x) in tx.borrow(xs).iter().enumerate() {
//                 assert_eq!(i+1, tx.get(x).unwrap())
//             }
//         }
//     }
//     mkregion(Test);
// }

#[test]
fn test_conflict() {
    struct Test;
    fn thread<R: Region>(r: Arc<R>, x: Arc<TCell<R,usize>>, y: Arc<TCell<R,usize>>) -> Result<(),TransactionErr> {
        // Increment both x and y
        {
            let mut tx = r.rw_transaction();
            *tx.mut_borrow(&x) = *tx.borrow(&x) + 1;
            *tx.mut_borrow(&y) = *tx.borrow(&y) + 1;
        }
        // Check that x == y
        {
            let tx = r.ro_transaction();
            let a = try!(tx.get(&x));
            let b = try!(tx.get(&y));
            assert_eq!(a, b);
        }
        Ok(())
    }
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let r = Arc::new(r);
            let x = Arc::new(r.mkcell(37));
            let y = Arc::new(r.mkcell(37));
            let mut threads = Vec::new();
            for _ in 0..1000 {
                let (r, x, y) = (r.clone(), x.clone(), y.clone());
                threads.push(thread::spawn(move || { thread(r, x, y) }));
            }
            for thread in threads.drain(..) {
                let _ = thread.join().unwrap();
            }
        }
    }
    mkregion(Test);
}
