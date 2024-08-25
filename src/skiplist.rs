
use std::ptr;
use rand::Rng;
use std::cmp::Ordering;


const MAX_LEVEL: usize = 16;

type Link<T> = Option<NonNull<Node<T>>>;

#[derive(Debug)]
struct Node<T> {
    value: V,
    next: Vec<Link<T>>,
}

pub struct SkipList<T>{
    head: *mut Node<T>,
    level: usize,
    length: usize
}
