use thiserror::Error;

#[derive(Error, Debug)]
pub enum GeyserPluginCosError {
    #[error("Replica block V0.0.1/v0.0.2 not supported anymore")]
    ReplicaBlockV001NotSupported,

    #[error("Replica transaction V0.0.1 not supported anymore")]
    ReplicaTransactionV001NotSupported,

    #[error("Replica entry V0.0.1 not supported anymore")]
    ReplicaEntryV001NotSupported,

    #[error("Error message: ({msg})")]
    IOError { msg: String },
}

impl From<std::io::Error> for GeyserPluginCosError {
    fn from(err: std::io::Error) -> Self {
        GeyserPluginCosError::IOError {
            msg: format!("IO error: {:?}", err),
        }
    }
}
