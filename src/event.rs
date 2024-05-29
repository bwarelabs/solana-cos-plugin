use std::io;

#[derive(Serialize, Deserialize, Debug)]
pub struct Event {
    pub data: Vec<u8>,
}

pub trait EventReceiver {
    fn receive(&mut self, event: Event) -> io::Result<()>;
}

impl Event {
    pub fn new(data: Vec<u8>) -> Self {
        Event { data }
    }
}
