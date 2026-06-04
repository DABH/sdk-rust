use crate::{
    add_tls_to_channel,
    errors::ClientConnectError,
    options_structs::{
        ClientKeepAliveOptions, ConnectionOptions, DnsLoadBalancingOptions, TlsOptions,
    },
};
use http::Uri;
use std::{collections::HashSet, net::SocketAddr, sync::Arc, time::Duration};
use tokio::sync::mpsc;
use tonic::transport::{Channel, Endpoint, channel::Change};
use url::Url;

/// Validates DNS load balancing configuration and returns the options if DNS LB should be used.
///
/// Returns `Err` if `dns_load_balancing` is set alongside `service_override` or
/// `http_connect_proxy`. Returns `Ok(None)` if DNS LB is disabled or the target is an IP literal.
pub(crate) fn validate_and_get_dns_lb(
    options: &ConnectionOptions,
) -> Result<Option<&DnsLoadBalancingOptions>, ClientConnectError> {
    let Some(dns_opts) = options.dns_load_balancing.as_ref() else {
        return Ok(None);
    };

    if options.service_override.is_some() {
        return Err(ClientConnectError::InvalidConfig(
            "dns_load_balancing cannot be used with service_override".to_owned(),
        ));
    }
    if options.http_connect_proxy.is_some() {
        return Err(ClientConnectError::InvalidConfig(
            "dns_load_balancing cannot be used with http_connect_proxy".to_owned(),
        ));
    }

    let host = options
        .target
        .host()
        .ok_or_else(|| ClientConnectError::InvalidConfig("target URL has no host".to_owned()))?;

    match host {
        url::Host::Domain("localhost") => Ok(None),
        url::Host::Domain(_) => Ok(Some(dns_opts)),
        url::Host::Ipv4(_) | url::Host::Ipv6(_) => Ok(None),
    }
}

async fn resolve_host(host: &str, port: u16) -> Result<Vec<SocketAddr>, std::io::Error> {
    tokio::net::lookup_host(format!("{host}:{port}"))
        .await
        .map(|addrs| addrs.collect())
}

fn endpoint_uri(addr: SocketAddr, scheme: &str) -> String {
    match addr {
        SocketAddr::V4(v4) => format!("{scheme}://{v4}"),
        SocketAddr::V6(v6) => format!("{scheme}://[{}]:{}", v6.ip(), v6.port()),
    }
}

async fn build_endpoint(
    addr: SocketAddr,
    original_host: &str,
    scheme: &str,
    tls_options: Option<&TlsOptions>,
    keep_alive: Option<&ClientKeepAliveOptions>,
    override_origin: Option<&Uri>,
) -> Result<Endpoint, ClientConnectError> {
    let uri = endpoint_uri(addr, scheme);
    let channel = Channel::from_shared(uri)?;

    // When connecting to an IP with TLS, SNI must use the original hostname.
    let tls_for_ip = tls_options.map(|tls| {
        if tls.domain.is_some() {
            tls.clone()
        } else {
            let mut patched = tls.clone();
            patched.domain = Some(original_host.to_owned());
            patched
        }
    });
    let channel = add_tls_to_channel(tls_for_ip.as_ref().or(tls_options), channel).await?;

    let channel = if let Some(keep_alive) = keep_alive {
        channel
            .keep_alive_while_idle(true)
            .http2_keep_alive_interval(keep_alive.interval)
            .keep_alive_timeout(keep_alive.timeout)
    } else {
        channel
    };

    let channel = if let Some(origin) = override_origin.cloned() {
        channel.origin(origin)
    } else {
        channel
    };

    Ok(channel)
}

/// Creates a balanced channel backed by all DNS-resolved addresses for the target.
pub(crate) async fn create_balanced_channel(
    options: &ConnectionOptions,
) -> Result<
    (
        Channel,
        mpsc::Sender<Change<SocketAddr, Endpoint>>,
        HashSet<SocketAddr>,
    ),
    ClientConnectError,
> {
    let host = options
        .target
        .host_str()
        .ok_or_else(|| ClientConnectError::InvalidConfig("target URL has no host".to_owned()))?;
    let port = options.target.port_or_known_default().unwrap_or(7233);
    let scheme = options.target.scheme();

    let addrs = resolve_host(host, port).await.map_err(|source| {
        ClientConnectError::DnsResolutionError {
            host: host.to_owned(),
            source,
        }
    })?;
    if addrs.is_empty() {
        return Err(ClientConnectError::DnsResolutionError {
            host: host.to_owned(),
            source: std::io::Error::new(
                std::io::ErrorKind::NotFound,
                "DNS resolution returned no addresses",
            ),
        });
    }
    let mut current_addrs = HashSet::with_capacity(addrs.len());

    let (channel, sender) = Channel::balance_channel(addrs.len());

    for addr in addrs {
        let endpoint = build_endpoint(
            addr,
            host,
            scheme,
            options.tls_options.as_ref(),
            options.keep_alive.as_ref(),
            options.override_origin.as_ref(),
        )
        .await?;
        // Unbounded-ish send into the freshly-created channel; can't realistically fail.
        let _ = sender.send(Change::Insert(addr, endpoint)).await;
        current_addrs.insert(addr);
    }

    Ok((channel, sender, current_addrs))
}

/// Handle that aborts the DNS re-resolution task when dropped.
pub(crate) struct DnsReresolutionHandle {
    abort_handle: tokio::task::AbortHandle,
}

impl Drop for DnsReresolutionHandle {
    fn drop(&mut self) {
        self.abort_handle.abort();
    }
}

/// Spawns a background task that periodically re-resolves DNS and updates the balanced channel.
pub(crate) fn spawn_dns_reresolution(
    sender: mpsc::Sender<Change<SocketAddr, Endpoint>>,
    current_addrs: HashSet<SocketAddr>,
    target: Url,
    tls_options: Option<TlsOptions>,
    keep_alive: Option<ClientKeepAliveOptions>,
    override_origin: Option<Uri>,
    resolution_interval: Duration,
) -> Arc<DnsReresolutionHandle> {
    let host = target.host_str().unwrap_or("").to_owned();
    let port = target.port_or_known_default().unwrap_or(7233);
    let scheme = target.scheme().to_owned();

    let handle = tokio::spawn(async move {
        let mut current_addrs = current_addrs;

        loop {
            tokio::time::sleep(resolution_interval).await;

            let new_addrs = match resolve_host(&host, port).await {
                Ok(addrs) => addrs.into_iter().collect::<HashSet<_>>(),
                Err(e) => {
                    warn!(
                        host = %host,
                        error = %e,
                        "DNS re-resolution failed, keeping existing endpoints"
                    );
                    continue;
                }
            };

            if new_addrs.is_empty() {
                warn!(
                    host = %host,
                    "DNS re-resolution returned no addresses, keeping existing endpoints"
                );
                continue;
            }

            if !apply_resolved_addresses(
                &sender,
                &mut current_addrs,
                new_addrs,
                &host,
                &scheme,
                tls_options.as_ref(),
                keep_alive.as_ref(),
                override_origin.as_ref(),
            )
            .await
            {
                return;
            }
        }
    });

    Arc::new(DnsReresolutionHandle {
        abort_handle: handle.abort_handle(),
    })
}

async fn apply_resolved_addresses(
    sender: &mpsc::Sender<Change<SocketAddr, Endpoint>>,
    current_addrs: &mut HashSet<SocketAddr>,
    new_addrs: HashSet<SocketAddr>,
    host: &str,
    scheme: &str,
    tls_options: Option<&TlsOptions>,
    keep_alive: Option<&ClientKeepAliveOptions>,
    override_origin: Option<&Uri>,
) -> bool {
    for addr in current_addrs.difference(&new_addrs) {
        if sender.send(Change::Remove(*addr)).await.is_err() {
            return false;
        }
    }

    for addr in new_addrs.difference(current_addrs) {
        match build_endpoint(
            *addr,
            host,
            scheme,
            tls_options,
            keep_alive,
            override_origin,
        )
        .await
        {
            Ok(endpoint) => {
                if sender.send(Change::Insert(*addr, endpoint)).await.is_err() {
                    return false;
                }
            }
            Err(e) => {
                warn!(
                    addr = %addr,
                    error = %e,
                    "Failed to build endpoint for resolved address"
                );
            }
        }
    }

    *current_addrs = new_addrs;
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ip_v4_target_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://1.2.3.4:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn ip_v6_target_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://[::1]:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn domain_target_returns_some() {
        let opts =
            ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_some());
    }

    #[test]
    fn disabled_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .dns_load_balancing(None)
            .build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn service_override_with_dns_lb_is_error() {
        use crate::callback_based::CallbackBasedGrpcService;

        let svc = CallbackBasedGrpcService {
            callback: Arc::new(|_| Box::pin(async { unreachable!() })),
        };
        let opts = ConnectionOptions::new(Url::parse("http://temporal.example.com:7233").unwrap())
            .service_override(svc)
            .build();
        assert!(validate_and_get_dns_lb(&opts).is_err());
    }

    #[test]
    fn localhost_returns_none() {
        let opts = ConnectionOptions::new(Url::parse("http://localhost:7233").unwrap()).build();
        assert!(validate_and_get_dns_lb(&opts).unwrap().is_none());
    }

    #[test]
    fn endpoint_uri_v4() {
        let addr: SocketAddr = "1.2.3.4:7233".parse().unwrap();
        assert_eq!(endpoint_uri(addr, "https"), "https://1.2.3.4:7233");
    }

    #[test]
    fn endpoint_uri_v6() {
        let addr: SocketAddr = "[::1]:7233".parse().unwrap();
        assert_eq!(endpoint_uri(addr, "https"), "https://[::1]:7233");
    }

    #[tokio::test]
    async fn reresolution_uses_seeded_addresses_as_current_state() {
        let addr_a: SocketAddr = "127.0.0.1:7233".parse().unwrap();
        let addr_b: SocketAddr = "127.0.0.2:7233".parse().unwrap();
        let addr_c: SocketAddr = "127.0.0.3:7233".parse().unwrap();
        let mut current_addrs = HashSet::from([addr_a, addr_b]);
        let new_addrs = HashSet::from([addr_b, addr_c]);

        let (sender, mut receiver) = mpsc::channel(4);
        assert!(
            apply_resolved_addresses(
                &sender,
                &mut current_addrs,
                new_addrs.clone(),
                "temporal.example.com",
                "http",
                None,
                None,
                None,
            )
            .await
        );
        assert_eq!(current_addrs, new_addrs);

        drop(sender);

        let mut removed_a = false;
        let mut inserted_c = false;
        for _ in 0..2 {
            match receiver.recv().await.expect("expected DNS endpoint change") {
                Change::Remove(addr) if addr == addr_a => removed_a = true,
                Change::Insert(addr, _) if addr == addr_c => inserted_c = true,
                Change::Remove(addr) => panic!("unexpected DNS endpoint removal: {addr}"),
                Change::Insert(addr, _) => panic!("unexpected DNS endpoint insertion: {addr}"),
            }
        }

        assert!(removed_a);
        assert!(inserted_c);
        assert!(receiver.recv().await.is_none());
    }
}
