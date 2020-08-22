use std::sync::Arc;
use std::any::Any;

pub struct AppUtils{}

impl AppUtils {

    // custom downcast for Arc<Any + 'static>
    pub fn downcast_arc<T>(arc: Arc<dyn Any>) -> Result<Arc<T>, Arc<dyn Any>>
        where
            T: Any + 'static,
    {
        if (*arc).is::<T>() {
            let ptr = Arc::into_raw(arc).cast::<T>();

            Ok(unsafe { Arc::from_raw(ptr) })
        } else {
            Err(arc)
        }
    }
}
