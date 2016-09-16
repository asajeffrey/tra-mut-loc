#![feature(test)]
#![feature(question_mark)]

extern crate rand;
extern crate test;
extern crate tra_mut_loc;

use rand::{Rng, thread_rng};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};
use test::Bencher;
use tra_mut_loc::{mkregion, TCell, Region, RegionConsumer, RORef, RWRef, TransactionErr};

const ITERATIONS: u32 = 2_000;
const IDLE_PROPORTION: u32 = 10; // Threads spend 1/N pf their time idle

#[bench]
fn bench_single_threaded(bench: &mut Bencher) {
    bench.iter(|| {
        struct Tree { data: usize, children: Vec<Tree> }
        let mut tree = Tree { data: 0, children: Vec::new() };
        let mut vec = Vec::new();
        fn insert<R: Rng>(tree: &mut Tree, value: usize, rng: &mut R) {
            let index = rng.gen_range(0, tree.children.len() + 1);
            if let Some(child) = tree.children.get_mut(index) {
                return insert(child, value, rng);
            } 
            tree.children.push(Tree{ data: value, children: Vec::new() });            
        }
        fn copy(tree: &Tree, vec: &mut Vec<usize>) {
            vec.push(tree.data);
            for child in tree.children.iter() {
                copy(child, vec);
            }
        }
        fn mutate<R: Rng>(tree: &mut Tree, rng: &mut R) {
            let value = rng.gen();
            insert(tree, value, rng);
            insert(tree, value, rng);
        }
        fn read(tree: &Tree, vec: &mut Vec<usize>) {
            copy(tree, vec);
            let mut xored: usize = 0;
            for data in vec.drain(..) {
                xored = xored ^ data;
            }
            assert_eq!(0, xored);
        }
        thread::spawn(move || {
            let mut rng = thread_rng();
            let mut time_spent_writing = Duration::new(0, 0);
            let mut time_spent_reading = Duration::new(0, 0);
            for _ in 0..ITERATIONS {
                let start = Instant::now();
                mutate(&mut tree, &mut rng);
                time_spent_writing += Instant::now() - start;
                while time_spent_writing < time_spent_reading {
                    let start = Instant::now();
                    read(&tree, &mut vec);
                    time_spent_reading += Instant::now() - start ;
                }
            }
        }).join().unwrap();
    });
}

#[bench]
fn bench_double_threaded_mutex(bench: &mut Bencher) {
    bench.iter(|| {
        struct Tree { data: usize, children: Vec<Tree> }
        fn insert<R: Rng>(tree: &mut Tree, value: usize, rng: &mut R) {
            let index = rng.gen_range(0, tree.children.len() + 1);
            if let Some(child) = tree.children.get_mut(index) {
                return insert(child, value, rng);
            } 
            tree.children.push(Tree{ data: value, children: Vec::new() });            
        }
        fn copy(tree: &Tree, vec: &mut Vec<usize>) {
            vec.push(tree.data);
            for child in tree.children.iter() {
                copy(child, vec);
            }
        }
        fn mutate<R: Rng>(tree: &mut Tree, rng: &mut R) {
            let value = rng.gen();
            insert(tree, value, rng);
            insert(tree, value, rng);
        }
        fn read(tree: &Tree, vec: &mut Vec<usize>) {
            copy(tree, vec);
            let mut xored: usize = 0;
            for data in vec.drain(..) {
                xored = xored ^ data;
            }
            assert_eq!(0, xored);
        }
        let tree = Tree { data: 0, children: Vec::new() };
        let writer_lock = Arc::new(Mutex::new(Some(tree)));
        let reader_lock = writer_lock.clone();
        thread::spawn(move || {
            let mut reads = 0;
            let mut vec = Vec::new();
            loop {
                let start = Instant::now();
                match *reader_lock.lock().unwrap() {
                    Some(ref tree) => {
                        read(tree, &mut vec);
                        reads = reads + 1;
                    }
                    None => break,
                }
                let finish = Instant::now();
                while Instant::now() < finish + ((finish - start) / IDLE_PROPORTION) {}
            }
        });
        thread::spawn(move || {
            let mut rng = thread_rng();
            for _ in 0..ITERATIONS {
                if let Some(ref mut tree) = *writer_lock.lock().unwrap() {
                    let start = Instant::now();
                    mutate(tree, &mut rng);
                    let finish = Instant::now();
                    while Instant::now() < finish + ((finish - start) / IDLE_PROPORTION) {}
                }
            }
            *writer_lock.lock().unwrap() = None;
        }).join().unwrap();
    });
}

#[bench]
fn bench_double_threaded_stm(bench: &mut Bencher) {
    bench.iter(|| {
        type Tree<Rg> = (usize, TCell<Rg, Forest<Rg>>);
        struct Forest<Rg> (Vec<Tree<Rg>>);
        impl<Rg> Deref for Forest<Rg> {
            type Target = [Tree<Rg>];
            fn deref(&self) -> &[Tree<Rg>] { &self.0 }
        }
        impl<Rg> DerefMut for Forest<Rg> {
            fn deref_mut(&mut self) -> &mut [Tree<Rg>] { &mut self.0 }
        }
        fn insert<Rg: Region, R: Rng>(tree: &mut RWRef<Tree<Rg>>, value: usize, rng: &mut R, region: &Rg) {
            let (_, ref mut children) = tree.split();
            let mut children = children.borrow_mut();
            let index = rng.gen_range(0, children.len() + 1);
            if let Some(mut child) = children.get_mut(index) {
                return insert(&mut child, value, rng, region);
            }
            let new_child = (value, region.mkcell(Forest(Vec::new())));
            children.0.push(new_child);
        }
        fn copy<Rg: Region>(tree: RORef<Tree<Rg>>, vec: &mut Vec<usize>) -> Result<(), TransactionErr> {
            let (data, children) = tree.split();
            vec.push(data.get()?);
            for child in children.borrow()?.iter()? {
                copy(child, vec)?;
            }
            Ok(())
        }
        fn mutate<Rg: Region, R: Rng>(tree: &mut RWRef<Tree<Rg>>, rng: &mut R, region: &Rg) {
            let value = rng.gen();
            insert(tree, value, rng, region);
            insert(tree, value, rng, region);
        }
        fn read<Rg: Region>(tree: RORef<Tree<Rg>>, vec: &mut Vec<usize>) -> Result<(), TransactionErr> {
            vec.clear();
            copy(tree, vec)?;
            let mut xored: usize = 0;
            for data in vec.drain(..) {
                xored = xored ^ data;
            }
            assert_eq!(0, xored);
            Ok(())
        }
        struct Bench;
        impl RegionConsumer for Bench {
            fn consume<Rg: Region>(self, region: Rg) {
                let region = Arc::new(region);
                let tree = Arc::new(region.mkbox((0, region.mkcell(Forest(Vec::new())))));
                let flag = Arc::new(AtomicBool::new(true));
                let reader_region = region.clone();
                let reader_tree = tree.clone();
                let reader_flag = flag.clone();
                thread::spawn(move || {
                    let mut reads = 0;
                    let mut retries = 0;
                    let mut vec = Vec::new();
                    while reader_flag.load(Ordering::Acquire) {
                        let mut start = Instant::now();
                        let tx = reader_region.ro_transaction();
                        if read(tx.borrow(&reader_tree), &mut vec).is_err() {
                            let locked = reader_region.rw_transaction();
                            start = Instant::now();
                            let tx = reader_region.ro_transaction();
                            read(tx.borrow(&reader_tree), &mut vec).unwrap();
                            drop(locked);
                            retries = retries + 1;
                        }
                        reads = reads + 1;
                        let finish = Instant::now();
                        while Instant::now() < finish + ((finish - start) / IDLE_PROPORTION) {}
                    }
                });
                thread::spawn(move || {
                    let mut rng = thread_rng();
                    for _ in 0..ITERATIONS {
                        let mut tx = region.rw_transaction();
                        let start = Instant::now();
                        mutate(&mut tx.mut_borrow(&tree).as_rwref(), &mut rng, &region);
                        let finish = Instant::now();
                        while Instant::now() < finish + ((finish - start) / IDLE_PROPORTION) {}
                    }
                    flag.store(false, Ordering::Release);
                }).join().unwrap();
            }
        }
        mkregion(Bench);
    });
}
