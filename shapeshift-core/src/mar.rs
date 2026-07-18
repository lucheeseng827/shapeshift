//! MAR (Monthly Active Rows) cost arithmetic — the honest "why self-host" number.
//!
//! Managed ELT vendors bill by MAR: every distinct primary key touched
//! (inserted/updated/deleted) in a month, priced on a steep tiered curve. A shape
//! that touches the same rows repeatedly, or fans one source into many datasets,
//! pays again and again. shapeshift shapes the same rows for the cost of the CPU
//! that does it. `shapeshift cost` turns a row count into that comparison — you name
//! the vendor's effective `$/million MAR`; it never guesses a price.

/// Inputs to the estimate.
#[derive(Debug, Clone, Copy)]
pub struct MarInputs {
    /// Billable rows this shape touched (MAR-equivalent).
    pub rows: u64,
    /// The managed vendor's effective price per million MAR (you supply it from
    /// your own plan — MAR pricing is tiered and per-connector).
    pub vendor_per_million: f64,
    /// What running shapeshift cost you for this shape (compute + storage), if you
    /// want a net figure. 0.0 to compare against a free self-host.
    pub self_host_cost: f64,
}

/// The comparison.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MarReport {
    pub rows: u64,
    pub vendor_cost: f64,
    pub self_host_cost: f64,
    pub saved: f64,
    /// `saved / vendor_cost` as a fraction in [−∞, 1]. `None` when vendor_cost is 0.
    pub saved_fraction: Option<f64>,
}

/// Compute the MAR cost comparison.
pub fn estimate(inp: MarInputs) -> MarReport {
    let vendor_cost = (inp.rows as f64 / 1_000_000.0) * inp.vendor_per_million;
    let saved = vendor_cost - inp.self_host_cost;
    let saved_fraction = if vendor_cost > 0.0 {
        Some(saved / vendor_cost)
    } else {
        None
    };
    MarReport {
        rows: inp.rows,
        vendor_cost,
        self_host_cost: inp.self_host_cost,
        saved,
        saved_fraction,
    }
}
