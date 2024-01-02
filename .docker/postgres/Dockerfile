# Postgres with required extensions
ARG POSTGRES_VERSION=9.6.24

# Базовая версия postgres не может выкачать какой-то пакет для stretch, потому что он уже удален из PostgreSQL repo
#  Поэтому нужно делать {version}-bullseye для более старых версий PG
ARG POSTGRES_IMAGE_TAG="${POSTGRES_VERSION}"

FROM postgres:${POSTGRES_IMAGE_TAG}

ARG POSTGRES_VERSION=9.6.24

RUN \
    apt-get update \
	&& apt-get install -y ca-certificates \
    && echo "deb [signed-by=/usr/share/postgresql-common/pgdg/apt.postgresql.org.asc] https://apt.postgresql.org/pub/repos/apt bullseye-pgdg main" > /etc/apt/sources.list.d/pgdg.list \
	&& apt-get update \
	&& apt-cache search postgres-* \
	&& apt-get install -y postgresql-${POSTGRES_VERSION%.*}-hll \
	&& apt-get clean all \
	&& rm -rfv /var/lib/apt/lists/*

# CREATE EXTENSION will be executed automatically by PG dump import
