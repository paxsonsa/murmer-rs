use foca::{Config, PeriodicParams};
use std::num::NonZero;
use std::time::Duration;

/// Defines how quickly information propagates through the cluster
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PropagationSpeed {
    /// Fastest propagation for real-time applications (higher network usage)
    Fast,
    /// Balanced propagation for most applications
    Normal,
    /// Conservative propagation for bandwidth-limited environments
    Conservative,
}

/// Defines how tolerant the cluster is to node failures and network issues
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FailureTolerance {
    /// Strict failure detection - nodes are marked down quickly (good for real-time)
    Strict,
    /// Balanced failure detection - reasonable timeouts for most applications
    Balanced,
    /// Lenient failure detection - nodes get more time before being marked down (good for global/unreliable networks)
    Lenient,
}

/// High-level cluster configuration that abstracts the complexity of the underlying gossip protocol
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// How fast information should propagate through the cluster
    pub propagation_speed: PropagationSpeed,
    /// How tolerant to be of node failures and network issues
    pub failure_tolerance: FailureTolerance,
    /// Maximum packet size for cluster messages (defaults to 64KB for QUIC)
    pub max_packet_size: Option<usize>,
    /// Custom fine-tuning options (overrides preset values when specified)
    pub custom: CustomConfig,
}

/// Fine-tuning options for advanced users who need specific control
#[derive(Debug, Clone, Default)]
pub struct CustomConfig {
    /// How often nodes probe each other for liveness
    pub probe_interval: Option<Duration>,
    /// Expected round-trip time for probe responses
    pub probe_timeout: Option<Duration>,
    /// How long a node can be unresponsive before marked as suspected
    pub suspect_timeout: Option<Duration>,
    /// How long to keep information about down nodes before removing them
    pub down_node_retention: Option<Duration>,
    /// How often to send gossip messages
    pub gossip_interval: Option<Duration>,
    /// How often nodes announce themselves to the cluster
    pub announce_interval: Option<Duration>,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self::balanced()
    }
}

impl ClusterConfig {
    /// Creates a configuration optimized for real-time applications
    /// - Very fast information propagation
    /// - Strict failure detection
    /// - Higher network usage but lowest latency
    pub fn realtime() -> Self {
        Self {
            propagation_speed: PropagationSpeed::Fast,
            failure_tolerance: FailureTolerance::Strict,
            max_packet_size: None,
            custom: CustomConfig::default(),
        }
    }

    /// Creates a balanced configuration suitable for most applications
    /// - Reasonable information propagation speed
    /// - Balanced failure detection
    /// - Good compromise between latency and network usage
    pub fn balanced() -> Self {
        Self {
            propagation_speed: PropagationSpeed::Normal,
            failure_tolerance: FailureTolerance::Balanced,
            max_packet_size: None,
            custom: CustomConfig::default(),
        }
    }

    /// Creates a configuration optimized for resilient, global applications
    /// - Conservative information propagation
    /// - Lenient failure detection
    /// - Lower network usage, higher tolerance for network issues
    pub fn resilient() -> Self {
        Self {
            propagation_speed: PropagationSpeed::Conservative,
            failure_tolerance: FailureTolerance::Lenient,
            max_packet_size: None,
            custom: CustomConfig::default(),
        }
    }

    /// Builder method to customize propagation speed
    pub fn with_propagation_speed(mut self, speed: PropagationSpeed) -> Self {
        self.propagation_speed = speed;
        self
    }

    /// Builder method to customize failure tolerance
    pub fn with_failure_tolerance(mut self, tolerance: FailureTolerance) -> Self {
        self.failure_tolerance = tolerance;
        self
    }

    /// Builder method to set custom packet size
    pub fn with_max_packet_size(mut self, size: usize) -> Self {
        self.max_packet_size = Some(size);
        self
    }

    /// Builder method to apply custom fine-tuning
    pub fn with_custom(mut self, custom: CustomConfig) -> Self {
        self.custom = custom;
        self
    }

    /// Converts this high-level configuration into the low-level foca Config
    pub(crate) fn to_foca_config(&self) -> Config {
        // Base parameters based on propagation speed
        let (probe_period, gossip_freq, announce_freq) = match self.propagation_speed {
            PropagationSpeed::Fast => (
                Duration::from_millis(500),  // Very frequent probing
                Duration::from_millis(100),  // Very frequent gossip
                Duration::from_secs(15),     // Frequent announcements
            ),
            PropagationSpeed::Normal => (
                Duration::from_secs(1),      // Standard probing
                Duration::from_millis(200),  // Standard gossip
                Duration::from_secs(30),     // Standard announcements
            ),
            PropagationSpeed::Conservative => (
                Duration::from_secs(2),      // Less frequent probing
                Duration::from_millis(500),  // Less frequent gossip
                Duration::from_secs(60),     // Less frequent announcements
            ),
        };

        // Failure detection parameters based on tolerance
        let (probe_rtt, suspect_timeout, down_retention) = match self.failure_tolerance {
            FailureTolerance::Strict => (
                Duration::from_millis(250),  // Quick response expected
                Duration::from_secs(1),      // Mark suspect quickly
                Duration::from_secs(300),    // Remove down nodes quickly (5 min)
            ),
            FailureTolerance::Balanced => (
                Duration::from_millis(500),  // Reasonable response time
                Duration::from_secs(3),      // Standard suspect timeout
                Duration::from_secs(3600),   // Keep down nodes for 1 hour
            ),
            FailureTolerance::Lenient => (
                Duration::from_secs(1),      // Allow slower responses
                Duration::from_secs(10),     // Give nodes more time
                Duration::from_secs(14400),  // Keep down nodes for 4 hours
            ),
        };

        // Apply custom overrides if specified
        let final_probe_period = self.custom.probe_interval.unwrap_or(probe_period);
        let final_probe_rtt = self.custom.probe_timeout.unwrap_or(probe_rtt);
        let final_suspect_timeout = self.custom.suspect_timeout.unwrap_or(suspect_timeout);
        let final_down_retention = self.custom.down_node_retention.unwrap_or(down_retention);
        let final_gossip_freq = self.custom.gossip_interval.unwrap_or(gossip_freq);
        let final_announce_freq = self.custom.announce_interval.unwrap_or(announce_freq);

        Config {
            probe_period: final_probe_period,
            probe_rtt: final_probe_rtt,
            max_packet_size: self.max_packet_size
                .unwrap_or(64 * 1024)
                .try_into()
                .expect("Invalid packet size"),
            num_indirect_probes: NonZero::new(3).unwrap(),
            suspect_to_down_after: final_suspect_timeout,
            max_transmissions: NonZero::new(15).unwrap(),
            remove_down_after: final_down_retention,
            notify_down_members: true,
            periodic_announce: Some(PeriodicParams {
                frequency: final_announce_freq,
                num_members: NonZero::new(1).expect("At least one member required"),
            }),
            periodic_announce_to_down_members: Some(PeriodicParams {
                frequency: final_announce_freq / 3, // More frequent for down members
                num_members: NonZero::new(1).expect("At least one member required"),
            }),
            periodic_gossip: Some(PeriodicParams {
                frequency: final_gossip_freq,
                num_members: NonZero::new(3).expect("At least 3 members for gossip"),
            }),
        }
    }
}

impl CustomConfig {
    /// Creates a new empty custom configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Builder method to set probe interval
    pub fn with_probe_interval(mut self, interval: Duration) -> Self {
        self.probe_interval = Some(interval);
        self
    }

    /// Builder method to set probe timeout
    pub fn with_probe_timeout(mut self, timeout: Duration) -> Self {
        self.probe_timeout = Some(timeout);
        self
    }

    /// Builder method to set suspect timeout
    pub fn with_suspect_timeout(mut self, timeout: Duration) -> Self {
        self.suspect_timeout = Some(timeout);
        self
    }

    /// Builder method to set down node retention time
    pub fn with_down_node_retention(mut self, retention: Duration) -> Self {
        self.down_node_retention = Some(retention);
        self
    }

    /// Builder method to set gossip interval
    pub fn with_gossip_interval(mut self, interval: Duration) -> Self {
        self.gossip_interval = Some(interval);
        self
    }

    /// Builder method to set announce interval
    pub fn with_announce_interval(mut self, interval: Duration) -> Self {
        self.announce_interval = Some(interval);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_realtime_config() {
        let config = ClusterConfig::realtime();
        let foca_config = config.to_foca_config();
        
        // Realtime should have fast probing and strict timeouts
        assert_eq!(foca_config.probe_period, Duration::from_millis(500));
        assert_eq!(foca_config.suspect_to_down_after, Duration::from_secs(1));
    }

    #[test]
    fn test_balanced_config() {
        let config = ClusterConfig::balanced();
        let foca_config = config.to_foca_config();
        
        // Balanced should have reasonable defaults
        assert_eq!(foca_config.probe_period, Duration::from_secs(1));
        assert_eq!(foca_config.suspect_to_down_after, Duration::from_secs(3));
    }

    #[test]
    fn test_resilient_config() {
        let config = ClusterConfig::resilient();
        let foca_config = config.to_foca_config();
        
        // Resilient should be more lenient
        assert_eq!(foca_config.probe_period, Duration::from_secs(2));
        assert_eq!(foca_config.suspect_to_down_after, Duration::from_secs(10));
    }

    #[test]
    fn test_custom_overrides() {
        let config = ClusterConfig::balanced()
            .with_custom(
                CustomConfig::new()
                    .with_probe_interval(Duration::from_millis(750))
                    .with_suspect_timeout(Duration::from_secs(5))
            );
        
        let foca_config = config.to_foca_config();
        
        // Custom values should override preset values
        assert_eq!(foca_config.probe_period, Duration::from_millis(750));
        assert_eq!(foca_config.suspect_to_down_after, Duration::from_secs(5));
    }

    #[test]
    fn test_builder_pattern() {
        let config = ClusterConfig::realtime()
            .with_propagation_speed(PropagationSpeed::Conservative)
            .with_failure_tolerance(FailureTolerance::Lenient)
            .with_max_packet_size(32 * 1024);
        
        assert_eq!(config.propagation_speed, PropagationSpeed::Conservative);
        assert_eq!(config.failure_tolerance, FailureTolerance::Lenient);
        assert_eq!(config.max_packet_size, Some(32 * 1024));
    }
}
