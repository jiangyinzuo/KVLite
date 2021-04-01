pub mod mrsw_skipmap;
pub mod skipmap;

use rand::Rng;

pub const MAX_LEVEL: usize = 12;

fn rand_level() -> usize {
    let mut rng = rand::thread_rng();
    let mut level = 0;
    while level < MAX_LEVEL {
        let number = rng.gen_range(1..=4);
        if number == 1 {
            level += 1;
        } else {
            break;
        }
    }
    level
}
