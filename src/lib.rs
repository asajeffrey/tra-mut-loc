/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

pub trait Region: 'static + Sync + Send {
    fn mkcell<T>(&self, init: T) -> TCell<Self, T>;
    fn rw_transaction<'a>(&'a self) -> RWTransaction<'a, Self>;
    fn ro_transaction<'a>(&'a self) -> ROTransaction<'a, Self>;
}

pub trait RegionConsumer {
    fn consume<R: Region>(self, r: R);
}

struct RegionImpl {
    version: AtomicUsize,
    lock: Mutex<()>,
}

pub struct TCell<R: ?Sized, T> {
    version: AtomicUsize,
    contents: UnsafeCell<T>,
    phantom: PhantomData<R>,
}

pub struct RWTransaction<'a, R: ?Sized> {
    version: Cell<usize>,
    region: &'a RegionImpl,
    #[allow(dead_code)]
    guard: MutexGuard<'a, ()>,
    phantom: PhantomData<R>,
}

pub struct ROTransaction<'a, R: ?Sized> {
    version: Cell<usize>,
    phantom: PhantomData<(&'a (), R)>,
}

pub struct Ref<'a, 'b, R: ?Sized, T> where 'a: 'b, R: 'b, T: 'b {
    transaction: &'b ROTransaction<'a, R>,
    cell: &'b TCell<R, T>,
}

#[derive(Clone, Debug)]
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
        let version = 1 + self.version.load(Ordering::Relaxed);
        self.version.store(version, Ordering::Relaxed);
        RWTransaction {
            version: Cell::new(version),
            region: self,
            guard: guard,
            phantom: PhantomData,
        }
    }    
    fn ro_transaction<'a>(&'a self) -> ROTransaction<'a, RegionImpl> {
        let version = self.version.load(Ordering::Acquire);
        ROTransaction {
            version: Cell::new(version),
            phantom: PhantomData,
        }
    }
}

impl<'a, 'b, R, T> Deref for Ref<'a, 'b, R, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.cell.contents.get().as_ref().unwrap() }
    }
}

impl<'a, 'b, R: ?Sized, T> Drop for Ref<'a, 'b, R, T> {
    fn drop(&mut self) {
        if self.transaction.version.get() <= self.cell.version.load(Ordering::Acquire) {
            self.transaction.version.set(0);
        }
    }
}

impl<'a, R: ?Sized> Drop for ROTransaction<'a, R> {
    fn drop(&mut self) {
        if self.version.get() > 0 {
            panic!("Transactions should be ended.");
        }
    }
}

impl<'a, R: ?Sized> Drop for RWTransaction<'a, R> {
    fn drop(&mut self) {
        if self.version.get() > 0 {
            panic!("Transactions should be ended.");
        }
    }
}

impl<'a, R> RWTransaction<'a, R> {
    pub fn borrow<T>(&self, cell: &TCell<R, T>) -> &T {
        unsafe { cell.contents.get().as_ref().unwrap() }
    }
    pub fn borrow_mut<T>(&mut self, cell: &TCell<R, T>) -> &mut T {
        cell.version.store(self.version.get(), Ordering::Release);
        unsafe { cell.contents.get().as_mut().unwrap() }
    }
    pub fn end(self) {
        self.region.version.store(self.version.get() + 1, Ordering::Release);
        self.version.set(0);
    }
}

impl<'a, R> ROTransaction<'a, R> {
    pub fn borrow<'b, T: Sync>(&'b self, cell: &'b TCell<R, T>) -> Result<Ref<'a, 'b, R, T>, TransactionErr> {
        if cell.version.load(Ordering::Acquire) < self.version.get() { 
            Ok(Ref{ transaction: self, cell: cell })
        } else {
            self.version.set(0);
            Err(TransactionErr)
        }
    }
    pub fn end(self) -> Result<(), TransactionErr> {
        if 0 < self.version.get() {
            self.version.set(0);
            Ok(())
        } else {
            Err(TransactionErr)
        }
    }
}

pub fn mkregion<C: RegionConsumer>(consumer: C) {
    consumer.consume(RegionImpl{
        version: AtomicUsize::new(1),
        lock: Mutex::new(()),
    })
}

#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::sync::Arc;

#[test]
fn test_ro() {
    struct Test;
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let x = r.mkcell(37);
            let tx = r.ro_transaction();
            assert_eq!(37, *tx.borrow(&x).unwrap());
            tx.end().unwrap();
        }
    }
    mkregion(Test);
}

#[test]
fn test_rw() {
    struct Test;
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let x = r.mkcell(37);
            let mut tx = r.rw_transaction();
            assert_eq!(37, *tx.borrow(&x));
            *tx.borrow_mut(&x) = 5;
            assert_eq!(5, *tx.borrow(&x));
            tx.end();
        }
    }
    mkregion(Test);
}

#[test]
fn test_conflict() {
    struct Test;
    fn thread<R: Region>(r: Arc<R>, x: Arc<TCell<R,usize>>, y: Arc<TCell<R,usize>>) -> Result<(),TransactionErr> {
        // Increment both x and y
        let mut tx = r.rw_transaction();
        *tx.borrow_mut(&x) = *tx.borrow(&x) + 1;
        *tx.borrow_mut(&y) = *tx.borrow(&y)+ 1;
        tx.end();
        // Check that x == y
        let tx = r.ro_transaction();
        let a = *try!(tx.borrow(&x));
        let b = *try!(tx.borrow(&y));
        try!(tx.end());
        assert_eq!(a, b);
        Ok(())
    }
    impl RegionConsumer for Test {
        fn consume<R: Region>(self, r: R) {
            let r = Arc::new(r);
            let x = Arc::new(r.mkcell(37));
            let y = Arc::new(r.mkcell(37));
            for _ in 0..1000 {
                let (r, x, y) = (r.clone(), x.clone(), y.clone());
                thread::spawn(move || { thread(r, x, y) });
            }
        }
    }
    mkregion(Test);
}
