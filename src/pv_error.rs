//! The `pv_error` module lists all errors possible during execution of the simulation.

/// `PvError` contains all runtime errors that might occur during simulation.
#[derive(Debug)]
pub enum PvError {
    /// A general violation of contracts of internal componenets.
    InternalError(String),
    /// An error during data serialisation or deserialisation.
    SerilisationError(serde_json::Error),
    /// An error regarding the RabbitMQ message broker.
    RabbitMqError(amiquip::Error),
    /// An input/output related error.
    IoError(std::io::Error)
}

impl From<serde_json::Error> for PvError {
    fn from(error: serde_json::Error) -> Self {
        PvError::SerilisationError(error)
    }
}

impl From<amiquip::Error> for PvError {
    fn from(error: amiquip::Error) -> Self {
        PvError::RabbitMqError(error)
    }
}

impl From<std::io::Error> for PvError {
    fn from(error: std::io::Error) -> Self {
        PvError::IoError(error)
    }
}
