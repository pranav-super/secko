// https://github.com/xacrimon/dashmap/issues/5
// https://www.youtube.com/watch?v=BI_bHCGRgMY
use lockfree::map::{Map, Iter, ReadGuard, Removed};
use serde::__private::PhantomData; // https://doc.servo.org/nomicon/phantom-data.html
use serde::{Serialize, Deserialize};
use serde::de::{Visitor, MapAccess};
use delegate::delegate;
use serde::ser::{Serializer, SerializeMap};
use std::fmt;
use serde::{Deserializer};
use std::hash::{Hasher, BuildHasher};
use std::sync::Arc;
use std::ops::Deref;

#[derive(Clone, Default)]
struct TrivialHasher {
    current_bytes: Vec<u8>
}

impl Hasher for TrivialHasher {
    fn write(&mut self, bytes: &[u8]) {
        self.current_bytes.extend_from_slice(bytes);
    }

    fn finish(&self) -> u64 {
        // need to return a u64. grab the last 8 bytes from current_bytes
        let bytes: &[u8] = &self.current_bytes[&self.current_bytes.len()-8..];
        let to_hash: [u8; 8] = bytes.try_into().expect("wrong length");
        // now take that slice and turn it into a u64. https://www.reddit.com/r/rust/comments/ioolsd/how_can_i_convert_a_slice_of_8_u8s_into_a_u64/
        // use the opposite function that is used to convert a u64 to a slice of u8s: https://doc.rust-lang.org/src/core/hash/mod.rs.html#400
        u64::from_ne_bytes(to_hash) 
    }

}

#[derive(Clone, Default, Debug)]
struct TrivialHasherBuilder;

impl BuildHasher for TrivialHasherBuilder {
    type Hasher = TrivialHasher;

    fn build_hasher(&self) -> Self::Hasher {
        TrivialHasher::default()
    }
}

// key is of type u64. so a u64 address. the hasher does nothing as the map looks up using u64 addresses, we just need access to the hash but otherwise this is akin to a set just with us having control over
//  the addresses.
// whatever the value is, which we figure out based on the client usage (mrdt stuff) must be serializable!!!
// passed in a hash to V necessarily implements hash, so that trait requirement is excluded here for simplicity, and clarity as we don't hash here. also needs to be serializable, so we include that trait, fruther down.
#[derive(Debug)] // if we unwrap an arc holding this, need this so we can call unwrap() or expect()
pub struct LockFreeMap<V> {
    inner: Map<u64, Arc<V>, TrivialHasherBuilder>
}

impl<'a, V> LockFreeMap<V>
where
    V: 'a
{
    pub fn new() -> Self {
        LockFreeMap { inner: Map::with_hasher(TrivialHasherBuilder::default()) }
    }

    delegate! {
        to self.inner {
            // pub fn clear(&mut self);
            pub fn insert(&self, key: u64, val: Arc<V>) -> Option<Removed<u64, Arc<V>>>;
            pub fn iter(&self) -> Iter<u64, Arc<V>>;
            pub fn get<'map>(&'map self, key: &u64) -> Option<ReadGuard<'map, u64, Arc<V>>>;
        }
    }
}

// desirialize dashmap
pub struct LockFreeMapVisitor<V> {
    marker: PhantomData<LockFreeMap<V>> // we use this here because we have an unused type parameter - https://doc.servo.org/serde/lib/struct.PhantomData.html 
}

// visitors are serdes way of deserializing nested structures.
// visits each of the fields of something like a struct
// the visitor is what the deserializer calls when the deserializer finds what it expects per deserialize()
impl<V> LockFreeMapVisitor<V> {
    fn new() -> Self {
        LockFreeMapVisitor {
            marker: PhantomData
        }
    }
}

// 'de lifetime because if the serialized value comes from memory then if we have some complicated string deep in our serialized format
// we want to pass a reference to that, not copy it. so we want it to live as long as the deserializer does. not relevant when reading from
// disk though as those buffers that we would have references to while parsing from disk would eventually go away - all our values then are
// owned and this lifetime is less important
impl<'de, V> Visitor<'de> for LockFreeMapVisitor<V>
where
    V: Deserialize<'de>
{
    type Value = LockFreeMap<V>;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result { // if you deserialize and get an unexpected type
        write!(formatter, "a LockFreeMap")
    }

    fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error> // what to do when a map is encountered (create one and return it)
    where
        M: MapAccess<'de>,
    {
        let map = LockFreeMap::new();

        while let Some((key, value)) = access.next_entry()? {
            map.insert(key, Arc::new(value));
        }

        Ok(map)
    }
}

// when implementing deserialize, in general we want to give information to the deserializer about what to expect next. depending on the format it might not
// be clear - for example binary encoding doesn't encode types like json might, making it harder. so this is to make it clear what to expect next
impl<'de, V> Deserialize<'de> for LockFreeMap<V>
where
    V: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where
            D: Deserializer<'de>, // this is a generic for something interfacing with the file format. so a json deserializer, or binary one, etc.
    {
        deserializer.deserialize_map(LockFreeMapVisitor::<V>::new()) //this is what to expect and what to construct based on that. we expect an encoding of a map
    }
}

// serialize dashmap
// uses iterator for lockfreemap...
// iterator grabs all keys present at the time, as u can see below:
/// An iterator over key-vaue entries of a [`Map`](super::Map). The `Item` of
/// this iterator is a [`ReadGuard`]. This iterator may be inconsistent, but
/// still it is memory-safe. It is guaranteed to yield items that have been in
/// the `Map` since the iterator creation and the current call to
/// [`next`](Iterator::next). However, it is not guaranteed to yield all items
/// present in the `Map` at some point if the `Map` is shared between threads.
impl<V> Serialize for LockFreeMap<V>
where
    V: Serialize
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where
            S: Serializer,
    {
        // need to provide bincode a definite length
        // let length = self.iter().fold(0, |acc, _| acc + 1);

        // grab keys ahead of time so we can get a length
        // iter() is key component. the iterator may be modified but since we are saving commits conservatively then we just care about the current version - race conditions unimportant; ordering *shouldn't* matter here as we aren't cutting loop off early, just caching must up to date key list
        let keys: Vec<u64> = self.iter().map(|kv| kv.key().clone()).collect();

        // now we serialize; need to provide bincode a definite length for bincode
        let mut map = serializer.serialize_map(Some(keys.len()))?;
        for key in keys { 
            map.serialize_entry(&key, self.get(&key).unwrap().val().deref())?; // turns it into something the serializer can understand, which follows the serde data model. serialize_entry takes care of this.
            // unwrap is safe for now as key won't get deleted
        }
        map.end()
    }
}