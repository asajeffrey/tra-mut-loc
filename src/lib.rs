/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use std::cell::{UnsafeCell};
use std::fmt;
use std::marker::PhantomData;
use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

/// A trait for data that contains no references
// TODO: worry about safety in the presence of multiple regions
pub unsafe trait Transact {}
unsafe impl Transact for bool {}
unsafe impl Transact for f32 {}
unsafe impl Transact for f64 {}
unsafe impl Transact for i8 {}
unsafe impl Transact for i16 {}
unsafe impl Transact for i32 {}
unsafe impl Transact for i64 {}
unsafe impl Transact for isize {}
unsafe impl Transact for u8 {}
unsafe impl Transact for u16 {}
unsafe impl Transact for u32 {}
unsafe impl Transact for u64 {}
unsafe impl Transact for usize {}
unsafe impl<R, T> Transact for TCell<R, T> where T: Deref {}
unsafe impl<'a, T> Transact for RORef<'a, T> {}
unsafe impl<T1, T2> Transact for (T1, T2) where T1: Transact, T2: Transact {}

pub trait Region: 'static + Sync + Send {
    fn mkcell<T>(&self, init: T) -> TCell<Self, T> where T: Deref;
    fn rw_transaction<'a>(&'a self) -> RWTransaction<'a, Self>;
    fn ro_transaction<'a>(&'a self) -> ROTransaction<'a, Self>;
    fn mkbox<T>(&self, init: T) -> TBox<Self, T> {
        self.mkcell(Box::new(init))
    }
}

pub trait RegionConsumer {
    fn consume<R: Region>(self, r: R);
}

#[derive(Debug)]
struct RegionImpl {
    version: AtomicUsize,
    lock: Mutex<()>,
}

pub struct TCell<R: ?Sized, T> where T: Deref {
    version: AtomicUsize,
    rw_data: UnsafeCell<T>,
    ro_data: UnsafeCell<*const T::Target>,
    phantom: PhantomData<R>,
}

pub type TBox<R, T> = TCell<R, Box<T>>;

pub struct RWTransaction<'a, R: 'a+?Sized> {
    version: usize,
    region_impl: &'a RegionImpl,
    region: &'a R,
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
}

impl<'a, R: ?Sized> fmt::Debug for RWTransaction<'a, R> {
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

pub struct RWCellRef<'a, T> where T: 'a + Deref {
    tx_version: usize,
    cell_version: &'a AtomicUsize,
    rw_data: &'a mut T,
    ro_data: &'a mut *const T::Target,
}

pub struct RWRef<'a, T: ?Sized> where T: 'a {
    tx_version: usize,
    cell_version: &'a AtomicUsize,
    data: &'a mut T,
}

pub struct RORef<'a, T: ?Sized> where T: 'a {
    tx_version: usize,
    cell_version: &'a AtomicUsize,
    data: *const T,
}

impl<'a, T> Copy for RORef<'a, T> {}
impl<'a, T> Clone for RORef<'a, T> {
    fn clone(&self) -> RORef<'a, T> { *self }
}

#[derive(Copy, Clone, Debug)]
pub struct TransactionErr;

unsafe impl<R: ?Sized, T: Sync> Sync for TCell<R, T> where T: Deref {}
unsafe impl<R: ?Sized, T: Send> Send for TCell<R, T> where T: Deref {}

impl Region for RegionImpl {
    fn mkcell<T>(&self, init: T) -> TCell<RegionImpl, T> where T: Deref {
        // There are probably cases where this is unsafe, where
        // init moves and causes &*init to be invalid.
        TCell {
            version: AtomicUsize::new(0),
            ro_data: UnsafeCell::new(&*init),
            rw_data: UnsafeCell::new(init),
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

impl<'a, T> Deref for RWCellRef<'a, T> where T: Deref {
    type Target = T;
    fn deref(&self) -> &T {
        &self.rw_data
    }
}

impl<'a, T> DerefMut for RWCellRef<'a, T> where T: Deref {
    fn deref_mut(&mut self) -> &mut T {
        self.cell_version.store(self.tx_version, Ordering::Release);
        &mut self.rw_data
    }
}

impl<'a, T: ?Sized> Deref for RWRef<'a, T> {
    type Target = T;
    fn deref(&self) -> &T {
        &self.data
    }
}

impl<'a, T: ?Sized> DerefMut for RWRef<'a, T> {
    fn deref_mut(&mut self) -> &mut T {
        self.cell_version.store(self.tx_version, Ordering::Release);
        &mut self.data
    }
}

impl<'a, T: ?Sized> RORef<'a, T> {
    pub fn touch(&self) -> Result<(), TransactionErr> {
        // I think we need a CAS here for its ordering effects
        if self.cell_version.compare_and_swap(0, 0, Ordering::AcqRel) < self.tx_version {
            Ok(())
        } else {
            Err(TransactionErr)
        }
    }
    pub fn as_ptr(&self) -> *const T {
        self.data
    }
    pub fn get(&self) -> Result<T, TransactionErr> where T: Transact + Clone {
        Ok(try!(self.as_ref()).clone())
    }
    pub fn as_ref(&self) -> Result<&T, TransactionErr> where T: Transact {
        let result = unsafe { &*self.data };
        try!(self.touch());
        Ok(result)
    }
}

impl<'a, R, T> RWRef<'a, TCell<R, T>> where T: Deref {
    // TODO: Not safe in the case of multiple regions
    pub fn borrow_mut<'b>(&'b mut self) -> RWCellRef<'b, T> {
        RWCellRef {
            tx_version: self.tx_version,
            cell_version: &self.data.version,
            rw_data: unsafe { &mut *self.data.rw_data.get() },
            ro_data: unsafe { &mut *self.data.ro_data.get() },
        }
    }
}

impl<'a, R, T> RORef<'a, TCell<R, T>> where T: Deref {
    // TODO: Not safe in the case of multiple regions
    pub fn borrow<'b>(&'b self) -> Result<RORef<'b, T::Target>, TransactionErr> {
        let cell = unsafe { &*self.data };
        let result = RORef {
            tx_version: self.tx_version,
            cell_version: &cell.version,
            data: unsafe { *cell.ro_data.get() },
        };
        try!(self.touch());
        Ok(result)
    }
}

pub struct ROIter<'a, T> where T: 'a {
    size: usize,
    first: RORef<'a, T>,
}

impl<'a, T1, T2> RWRef<'a, (T1, T2)> {
    pub fn split(self) -> (RWRef<'a, T1>, RWRef<'a, T2>) {
        let &mut (ref mut data1, ref mut data2) = self.data;
        (
            RWRef {
                tx_version: self.tx_version,
                cell_version: self.cell_version,
                data: data1,
            },
            RWRef {
                tx_version: self.tx_version,
                cell_version: self.cell_version,
                data: data2,
            },
        )
    }
}

impl<'a, T1, T2> RORef<'a, (T1, T2)> {
    pub fn split(self) -> (RORef<'a, T1>, RORef<'a, T2>) {
        let data1 = self.data as *const T1;
        let data2 = unsafe { data1.offset(1) } as *const T2;
        (
            RORef {
                tx_version: self.tx_version,
                cell_version: self.cell_version,
                data: data1,
            },
            RORef {
                tx_version: self.tx_version,
                cell_version: self.cell_version,
                data: data2,
            },
        )
    }
}

impl<'a, T> RORef<'a, [T]> {
    pub fn iter(self) -> Result<ROIter<'a, T>, TransactionErr> {
        let data = unsafe { &*self.data };
        let size = data.len();
        let first = RORef {
            tx_version: self.tx_version,
            cell_version: self.cell_version,
            data: data.as_ptr(),
        };
        try!(self.touch());
        Ok(ROIter{ size: size, first: first })
    }
}

impl<'a, T> Iterator for ROIter<'a, T> {
    type Item = RORef<'a, T>;
    fn next(&mut self) -> Option<RORef<'a, T>> {
        match self.size.checked_sub(1) {
            Some(size) => {
                let result = self.first.clone();
                self.size = size;
                self.first.data = unsafe { self.first.data.offset(1) };
                Some(result)
            },
            None => None,
        }
    }
}

impl<'a, R: ?Sized> Drop for RWTransaction<'a, R> {
    fn drop(&mut self) {
        self.region_impl.version.store(self.version + 1, Ordering::Release);
    }
}

impl<'a, T> Drop for RWCellRef<'a, T> where T: Deref {
    fn drop(&mut self) {
        if self.cell_version.load(Ordering::Relaxed) == self.tx_version {
            *self.ro_data = &**self.rw_data;
        }
    }
}

impl<'a, R> RWTransaction<'a, R> where R: Region {
    pub fn borrow<'b, T>(&'b self, cell: &'b TCell<R, T>) -> &'b T where T: Deref {
        unsafe { &*cell.rw_data.get() }
    }
    pub fn mut_borrow<'b, T>(&'b mut self, cell: &'b TCell<R, T>) -> RWCellRef<'b, T> where T: Deref {
        RWCellRef {
            tx_version: self.version,
            cell_version: &cell.version,
            rw_data: unsafe { &mut *cell.rw_data.get() },
            ro_data: unsafe { &mut *cell.ro_data.get() },
        }
    }
    pub fn borrow_mut<'b, T>(&'b self, cell: &'b mut TCell<R, T>) -> RWCellRef<'b, T> where T: Deref {
        RWCellRef {
            tx_version: self.version,
            cell_version: &cell.version,
            rw_data: unsafe { &mut *cell.rw_data.get() },
            ro_data: unsafe { &mut *cell.ro_data.get() },
        }
    }
    pub fn mkcell<T>(&self, init: T) -> TCell<R, T> where T: Deref {
        self.region.mkcell(init)
    }
}

impl<'a, R> ROTransaction<'a, R> {
    pub fn borrow<'b, T>(&'b self, cell: &'b TCell<R, T>) -> RORef<'b, T::Target> where T: Deref {
        RORef {
            tx_version: self.version,
            cell_version: &cell.version,
            data: unsafe { *cell.ro_data.get() },
        }
    }
    pub fn get<'b, T>(&'b self, cell: &'b TCell<R, T>) -> Result<T::Target, TransactionErr> where T: Deref, T::Target: Transact + Copy {
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
            let x = r.mkbox(37);
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
            let mut x = r.mkbox(37);
            let mut tx = r.rw_transaction();
            assert_eq!(37, **tx.borrow(&x));
            **tx.mut_borrow(&x) = 5;
            assert_eq!(5, **tx.borrow(&x));
            **tx.borrow_mut(&mut x) = 42;
            assert_eq!(42, **tx.borrow(&x));
        }
    }
    mkregion(Test);
}

#[test]
fn test_iter() {
    struct Test;
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let xs = r.mkcell(vec![ 1, 2, 3 ]);
            let tx = r.ro_transaction();
            for (i, x) in tx.borrow(&xs).iter().unwrap().enumerate() {
                assert_eq!(i+1, x.get().unwrap())
            }
        }
    }
    mkregion(Test);
}

#[test]
fn test_conflict() {
    struct Test;
    fn thread<R: Region>(r: Arc<R>, x: Arc<TBox<R,usize>>, y: Arc<TBox<R,usize>>) -> Result<(),TransactionErr> {
        // Increment both x and y
        {
            let mut tx = r.rw_transaction();
            **tx.mut_borrow(&x) = **tx.borrow(&x) + 1;
            **tx.mut_borrow(&y) = **tx.borrow(&y) + 1;
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
            let x = Arc::new(r.mkbox(37));
            let y = Arc::new(r.mkbox(37));
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
