# doris-docker-vm-try

This project runs **Apache Doris** locally using Docker. Doris is started using two official Docker images:
- **Frontend (FE)** – handles metadata and query coordination
- **Backend (BE)** – handles data storage and query execution

Both containers are orchestrated using **Docker Compose**. Once running, Doris can be accessed via the Frontend service.

---

## Prerequisites

Make sure you have the following installed:
- Docker (20.x or newer)
- Docker Compose **v2** (use `docker compose`, not `docker-compose`)

Verify:
```bash
docker --version
docker compose version
