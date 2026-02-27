use bencher::{benchmark_group, benchmark_main, Bencher};

use hashtable::HashTable;

fn benchmark_test(b: &mut Bencher) {
    b.iter(|| {
        let mut hash = HashTable::<usize, usize>::new();

        for _ in 0..1000 {
            let key = rand::random::<usize>();
            if let Some(value) = hash.get_mut(&key) {
                *value += 1;
            } else {
                hash.insert(key, 1);
            }
        }
    });
}

benchmark_group!(benches, benchmark_test);
benchmark_main!(benches);
