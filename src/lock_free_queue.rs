use std::mem::MaybeUninit;
use haphazard::{AtomicPtr, HazardPointer};
use haphazard::raw::Pointer;

/// AtomicPtr is essentially a wrapper around a raw pointer which helps in updated and read atomically
/// NOTE: it's not std::sync::atomic:::AtomicPrt, but rather haphazard::AtomicPtr
struct Node<T> {
    next: AtomicPtr<Node<T>>,
    data: MaybeUninit<T>
}


/// LockFreeQueue is an implementation of Micheal-Scott Queue
pub struct LockFreeQueue<T> {
    head: AtomicPtr<Node<T>>,
    tail: AtomicPtr<Node<T>>
}

impl<T> Node<T> {
    pub fn new(data: T) -> Self {
        Self {
            next: unsafe { AtomicPtr::new(core::ptr::null_mut()) },
            data: MaybeUninit::new(data)
        }
    }
    pub fn empty() -> Self {
        Self {
            next: unsafe { AtomicPtr::new(core::ptr::null_mut()) },
            data: MaybeUninit::uninit()
        }
    }
}

impl<T> LockFreeQueue<T> {
    pub fn new() -> Self {
        // Extract the raw pointer reference by self
        // Box is used to get around ownership in the linkedlist
        let dummy = Box::new(Node::empty()).into_raw();
        Self {
            head: unsafe { AtomicPtr::new(dummy) },
            tail: unsafe { AtomicPtr::new(dummy) },
        }
    }
}

impl<T: Sync + Send> LockFreeQueue<T> {
    pub fn enqueue(&self, hp: &mut HazardPointer, data: T) {
        let new_node: *mut Node<T> = Box::into_raw(Box::new(Node::new(data)));

        // Repeat until completing the step 1
        loop {
            // instead of just reading self.tail with self.tail.load_ptr(), we use self.tail.safe_load(hp)
            // which uses the hazard pointers to make sure the load is safe, returning a safe normal reference to the node
            let tail = self.tail.safe_load(hp).unwrap();
            let next = tail.next.load_ptr();
            if !next.is_null() {
                // Step 2: Let other thread to help with the enqueue of next
                unsafe {
                    let _ = self.tail.compare_exchange_ptr(
                        tail as *const Node<T> as *mut Node<T>,
                        next
                    );
                };
            } else {
                // Step 1: Try to enqueue the node by updating the tail
                if unsafe {
                    tail.next.compare_exchange_ptr(std::ptr::null_mut(), new_node)
                }.is_ok() { // If the Compare-And-Swap succeeded
                    // Step 2: try to bump up self.tail to new_node
                    unsafe {
                        let _ = self.tail.compare_exchange_ptr(
                            tail as *const Node<T> as *mut Node<T>,
                            new_node
                        );
                    };
                    // Break the loop once the enqueue is done
                    return;
                }
            }
        }
    }

    pub fn dequeue(&self, hp_head: &mut HazardPointer, hp_next: &mut HazardPointer) -> Option<T> {
        loop {
            // Atomic reads
            let head = self.head.safe_load(hp_head).expect("a MS queue should never be empty");
            let head_ptr = head as *const Node<T> as *mut Node<T>;
            let next_ptr = head.next.load_ptr();
            let tail_ptr = self.tail.load_ptr();

            if head_ptr != tail_ptr {
                let next = head.next.safe_load(hp_next).unwrap();
                if let Ok(unlinked_head_ptr) = unsafe {
                    self.head.compare_exchange_ptr(head_ptr, next_ptr)
                } {
                    // Successfully dequeue
                    unsafe {
                        unlinked_head_ptr.unwrap().retire();
                    }

                    // Take and return ownership of the data
                    return Some(unsafe {
                        std::ptr::read(next.data.assume_init_ref() as *const _)
                    });
                }
            } else if !next_ptr.is_null() {
                // Help partial enqueue
                unsafe {
                    let _ = self.tail.compare_exchange_ptr(tail_ptr, next_ptr);
                }
            } else {
                // Empty queue
                return None;
            }
        }
    }
}

impl<T> Drop for LockFreeQueue<T> {
    fn drop(&mut self) {
        // Don't drop the data on self.head
        let head = unsafe { Box::from_raw(self.head.load_ptr()) };
        let mut next = head.next;

        while !next.load_ptr().is_null() {
            let node = unsafe { Box::from_raw(next.load_ptr()) };

            // Drop the initialized data
            unsafe { node.data.assume_init() };

            // Move on to the next node
            next = node.next;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::thread;
    use super::*;
    #[test]
    fn test_single_thread_operations() {
        let queue = LockFreeQueue::<i32>::new();
        let mut hp1 = HazardPointer::new();
        let mut hp2 = HazardPointer::new();

        // Test enqueue
        queue.enqueue(&mut hp1, 1);
        queue.enqueue(&mut hp1, 2);
        queue.enqueue(&mut hp1, 3);

        // Test dequeue
        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), Some(1));
        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), Some(2));
        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), Some(3));
        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), None);
    }

    #[test]
    fn test_empty_queue() {
        let queue = LockFreeQueue::<i32>::new();
        let mut hp1 = HazardPointer::new();
        let mut hp2 = HazardPointer::new();

        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), None);
    }

    #[test]
    fn test_concurrent_operations() {
        let queue = Arc::new( LockFreeQueue::<i32>::new());
        let num_threads = 4;
        let operations_per_thread = 1000;

        // Producer threads
        let producers: Vec<_> = (0..num_threads)
            .map(|i| {
                thread::spawn({
                    let value = queue.clone();
                    move || {
                        let queue = Arc::clone(&value);
                        let mut hp = HazardPointer::new();
                        for j in 0..operations_per_thread {
                            queue.enqueue(&mut hp, i * operations_per_thread + j);
                        }
                    }
                })
            })
            .collect();

        // Consumer threads
        let consumers: Vec<_> = (0..num_threads)
            .map(|_| {
                thread::spawn({
                    let value = queue.clone();
                    move || {
                        let mut hp1 = HazardPointer::new();
                        let mut hp2 = HazardPointer::new();
                        let mut count = 0;
                        while count < operations_per_thread {
                            if value.dequeue(&mut hp1, &mut hp2).is_some() {
                                count += 1;
                            }
                        }
                    }
                })
            })
            .collect();

        // Wait for all threads to complete
        for handle in producers {
            handle.join().unwrap();
        }
        for handle in consumers {
            handle.join().unwrap();
        }

        // Verify queue is empty after all operations
        let mut hp1 = HazardPointer::new();
        let mut hp2 = HazardPointer::new();
        assert_eq!(queue.dequeue(&mut hp1, &mut hp2), None);
    }
}