pub mod backfill;
pub mod jetstream;
pub mod stale;

use std::time::Duration;

use store::{Lane, Store};
use tokio::time::sleep;

const BACKGROUND_LANE_LIMIT: usize = 2000;
const ROOM_CHECK_PAUSE: Duration = Duration::from_millis(100);

async fn room_in_the_background_lane(store: &Store) {
    while store.queue_depth(Lane::Background) >= BACKGROUND_LANE_LIMIT {
        sleep(ROOM_CHECK_PAUSE).await;
    }
}
