# triton-lb-proxy

Lightweight load-balancer proxy for NVIDIA Triton Inference Server deployments. This repository provides a simple proxy that forwards requests to one or more Triton backends and performs basic load-balancing and health checks.

## Prerequisites

- go 1.16 or higher
- Docker (for building and running the Triton server)

## Installation

1. Clone the repository:

	git clone <repo-url>

2. Install dependencies:

	``./setup.sh``