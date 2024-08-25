//use std::path::Display;
use std::fmt;
use std::fmt::Display;

#[derive(Debug)]
pub enum AnchorError{
    IoError(std::io::Error), 
    KeyNotFoundError(String)
}

impl Display for AnchorError{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AnchorError::IoError(err)=> write!(f,"I/O error: {}",err),
            AnchorError::KeyNotFoundError(err) =>write!(f,"Key not found: {}",err)
        }
    }
}