/* This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/. */

use std::cell::UnsafeCell;
use std::marker::PhantomData;
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
    region: &'b Region<'a>,
    #[allow(dead_code)]
    guard: MutexGuard<'b, ()>,
    phantom: PhantomData<(Invariant<'a>, Covariant<'b>)>,
}

pub struct ROTransaction<'a, 'b> {
    version: usize,
    phantom: PhantomData<(Invariant<'a>, Covariant<'b>)>,
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
            region: self,
            guard: guard,
            phantom: PhantomData,
        }
    }    
    pub fn ro_transaction<'b>(&'b self) -> ROTransaction<'a, 'b> {
        let version = self.version.load(Ordering::Relaxed);
        ROTransaction {
            version: version,
            phantom: PhantomData,
        }
    }    
}

impl<'a, 'b> Drop for RWTransaction<'a, 'b> {
    fn drop(&mut self) {
        let version = 1 + self.region.version.load(Ordering::Relaxed);
        self.region.version.store(version, Ordering::Relaxed);
    }
}

impl<'a, 'b> RWTransaction<'a, 'b> {
    pub fn borrow<T>(&self, cell: &TCell<'a, T>) -> &T {
        unsafe { cell.contents.get().as_ref().unwrap() }
    }
    pub fn borrow_mut<T>(&mut self, cell: &TCell<'a, T>) -> &mut T {
        let version = self.region.version.load(Ordering::Relaxed);
        cell.version.store(version, Ordering::Release);
        unsafe { cell.contents.get().as_mut().unwrap() }
    }
}

impl<'a, 'b> ROTransaction<'a, 'b> {
    pub fn borrow<T: Sync>(&self, cell: &TCell<'a, T>) -> Result<&T, TransactionErr> {
        let tmp = unsafe { cell.contents.get().as_ref().unwrap() };
        if cell.version.load(Ordering::Acquire) < self.version { 
            Ok(tmp)
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
    })
}

// Shouldn't typecheck:
//
// fn unsafe() {
//     mkregion(|r1| mkregion(|r2| {
//         let x = r2.mkcell(37);
//         let tx = r1.ro_transaction();
//         assert_eq!(37, *tx.borrow(&x).unwrap());
//     }))
// }

