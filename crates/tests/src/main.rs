use std::env;

mod staleness;
use staleness::{test_generic, test_client_rate, test_ai_rate, test_elasticity};

// cli front provided here, rest of library elsewhere. this is mainly for testing
fn main() {

    env::set_var("RUST_BACKTRACE", "1");

    // test_size();
    // test_failure(); // FINISH!!!
    // test_num_keys(); //FINISH!!!
    // test_rate();

    test_generic(64, 500.0, 100.0);
    // test_generic(16, 0.5, 250.0); //[10.0, 25.0, 50.0, 75.0, 100.0, 250.0, 500.0, 750.0, 1000.0, 2500.0, 5000.0, 7500.0, 10000.0, 25000.0, 50000.0, 75000.0, 100000.0, 10000000.0]
    // test_client_rate(); // retry
    // test_ai_rate(); // retry
    
    // test_elasticity();
} 