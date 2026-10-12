//! Device selection contracts. No GPU implementation is advertised by this prototype.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    Cpu,
    Cuda,
    Hip,
}
#[derive(Debug, PartialEq, Eq)]
pub struct Dispatch {
    pub backend: Backend,
    pub reason: &'static str,
}
pub fn select(requested: Backend) -> Dispatch {
    Dispatch {
        backend: Backend::Cpu,
        reason: match requested {
            Backend::Cpu => "CPU requested",
            _ => "GPU backend unavailable; CPU fallback",
        },
    }
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn unavailable_gpu_uses_cpu() {
        assert_eq!(select(Backend::Cuda).backend, Backend::Cpu);
    }
}
