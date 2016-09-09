/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use std::cell::{Cell, UnsafeCell};
use std::marker::PhantomData;
use std::ops::Deref;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, MutexGuard};

type Covariant<'a> = &'a ();
type Invariant<'a> = &'a mut &'a ();

pub struct Region<'a> {
    version: AtomicUsize,
    lock: Mutex<()>,
    phantom: PhantomData<Invariant<'a>>,
}

pub struct TCell<'a, T> {
    version: AtomicUsize,
    contents: UnsafeCell<T>,
    phantom: PhantomData<Invariant<'a>>,
}

pub struct RWTransaction<'a, 'b> where 'a: 'b {
    version: Cell<usize>,
    region: &'b Region<'a>,
    #[allow(dead_code)]
    guard: MutexGuard<'b, ()>,
}

pub struct ROTransaction<'a, 'b> where 'a: 'b {
    version: Cell<usize>,
    phantom: PhantomData<(Invariant<'a>, Covariant<'b>)>,
}

pub struct Ref<'a, 'b, 'c, T> where 'a:  'b, 'b: 'c, T: 'c {
    transaction: &'c ROTransaction<'a, 'b>,
    cell: &'c TCell<'a, T>,
}

#[derive(Clone, Debug)]
pub struct TransactionErr;

impl<'a> Region<'a> {
    pub fn mkcell<'b, T>(&'b self, init: T) -> TCell<'a, T> {
        TCell {
            version: AtomicUsize::new(0),
            contents: UnsafeCell::new(init),
            phantom: PhantomData,
        }
    }
    pub fn rw_transaction<'b>(&'b self) -> RWTransaction<'a, 'b> {
        let guard = self.lock.lock().unwrap();
        let version = 1 + self.version.load(Ordering::Relaxed);
        self.version.store(version, Ordering::Relaxed);
        RWTransaction {
            version: Cell::new(version),
            region: self,
            guard: guard,
        }
    }    
    pub fn ro_transaction<'b>(&'b self) -> ROTransaction<'a, 'b> {
        let version = self.version.load(Ordering::Acquire);
        ROTransaction {
            version: Cell::new(version),
            phantom: PhantomData,
        }
    }    
}

impl<'a, 'b, 'c, T> Deref for Ref<'a, 'b, 'c, T> {
    type Target = T;
    fn deref(&self) -> &T {
        unsafe { self.cell.contents.get().as_ref().unwrap() }
    }
}

impl<'a, 'b, 'c, T> Drop for Ref<'a, 'b, 'c, T> {
    fn drop(&mut self) {
        if self.transaction.version.get() <= self.cell.version.load(Ordering::Acquire) {
            self.transaction.version.set(0);
        }
    }
}

impl<'a, 'b> Drop for ROTransaction<'a, 'b> {
    fn drop(&mut self) {
        if self.version.get() > 0 {
            panic!("Transactions should be ended.");
        }
    }
}

impl<'a, 'b> Drop for RWTransaction<'a, 'b> {
    fn drop(&mut self) {
        if self.version.get() > 0 {
            panic!("Transactions should be ended.");
        }
    }
}

impl<'a, 'b> RWTransaction<'a, 'b> {
    pub fn borrow<T>(&self, cell: &TCell<'a, T>) -> &T {
        unsafe { cell.contents.get().as_ref().unwrap() }
    }
    pub fn borrow_mut<T>(&mut self, cell: &TCell<'a, T>) -> &mut T {
        cell.version.store(self.version.get(), Ordering::Release);
        unsafe { cell.contents.get().as_mut().unwrap() }
    }
    pub fn end(self) {
        self.region.version.store(self.version.get() + 1, Ordering::Relaxed);
        self.version.set(0);
    }
}

impl<'a, 'b> ROTransaction<'a, 'b> {
    pub fn borrow<'c, T: Sync>(&'c self, cell: &'c TCell<'a, T>) -> Result<Ref<'a, 'b, 'c, T>, TransactionErr> {
        if cell.version.load(Ordering::Acquire) < self.version.get() { 
            Ok(Ref{ transaction: self, cell: cell })
        } else {
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

pub fn mkregion<F, T>(f: F) -> T where F: for<'a> FnOnce(Region<'a>) -> T {
    f(Region {
        version: AtomicUsize::new(1),
        lock: Mutex::new(()),
        phantom: PhantomData,
    })
}

#[test]
fn test_ro() {
    mkregion(|r| {
        let x = r.mkcell(37);
        let tx = r.ro_transaction();
        assert_eq!(37, *tx.borrow(&x).unwrap());
        tx.end().unwrap();
    })
}

#[test]
fn test_rw() {
    mkregion(|r| {
        let x = r.mkcell(37);
        let mut tx = r.rw_transaction();
        assert_eq!(37, *tx.borrow(&x));
        *tx.borrow_mut(&x) = 5;
        assert_eq!(5, *tx.borrow(&x));
        tx.end();
    })
}

// Shouldn't typecheck:
//
// fn not_safe() {
//     mkregion(|r1| mkregion(|r2| {
//         let x = r2.mkcell(37);
//         let tx = r1.ro_transaction();
//         assert_eq!(37, *tx.borrow(&x).unwrap());
//     }))
// }

