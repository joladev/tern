use crate::{Error, Store};

const FORMAT: u32 = 3;
const FORMAT_KEY: &str = "format";

impl Store {
    pub(crate) fn migrate(&self) -> Result<(), Error> {
        match self.format()? {
            FORMAT => Ok(()),
            0 if self.accounts.is_empty()? => self.set_format(),
            format => Err(Error::UnsupportedFormat(format)),
        }
    }

    fn format(&self) -> Result<u32, Error> {
        let Some(bytes) = self.meta.get(FORMAT_KEY)? else {
            return Ok(0);
        };
        Ok(postcard::from_bytes(&bytes)?)
    }

    fn set_format(&self) -> Result<(), Error> {
        Ok(self
            .meta
            .insert(FORMAT_KEY, postcard::to_stdvec(&FORMAT)?)?)
    }
}
