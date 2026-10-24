# E2E infrastructure security profile

This template is test-only, not a production deployment recommendation.
`allowAllAzureServicesToPostgres` defaults to `false`; no broad PostgreSQL
firewall rule is created unless **both** it and `enablePostgres` are true.
This does not itself configure private reachability: deployments with the default
must supply an appropriate network path before a Function App can use PostgreSQL.

The certification workflow deliberately passes `allowAllAzureServicesToPostgres=true`
because Flex Consumption outbound addresses are dynamic. The `0.0.0.0` sentinel
allows connection attempts from a broad Azure-service population, **not only this
app**. Credentials and TLS are still required; this is exposure minimization,
not evidence of unauthorized access. Never reuse this choice for shared or
long-lived databases. CI uses a unique run/attempt resource group, generates a
fresh password with `openssl rand`, masks the password and URL before use, and
requests resource-group deletion in a separate `always()` cleanup job. Deletion
is asynchronous; operators must monitor failed cleanup and orphaned resources.

For a safer deployed profile, validate Flex Consumption VNet integration using
an appropriate delegated integration subnet, with PostgreSQL private access in
its delegated subnet or a supported private endpoint configuration, private DNS,
and restricted egress. Evaluate Entra ID database authentication and dedicated
least-privilege reader/writer identities instead of the test administrator.
See Microsoft's [Functions networking options](https://learn.microsoft.com/azure/azure-functions/functions-networking-options),
[PostgreSQL private access](https://learn.microsoft.com/azure/postgresql/network/concepts-networking-private)
and [PostgreSQL Private Link](https://learn.microsoft.com/azure/postgresql/network/concepts-networking-private-link).

`AzureWebJobsStorage` and deployment storage still authenticate with account-key
connection strings. That is unchanged here and acceptable only for this
test-only, ephemeral deployment. Do not print app settings, deployment
parameters, storage keys or connection strings. Only the E2E test report and
credential-free certification record are uploaded, not resource or settings
dumps. Application code must not return driver tracebacks or secrets in HTTP
responses: CI masking is not artifact redaction.

Azure-validated follow-ups (not implemented or certified here): identity-based
host/deployment storage with the required role assignments, private networking,
Entra ID database authentication, and explicit Function App/SCM minimum TLS.
These require real-Azure validation for this hosting profile before replacing
the existing certification configuration.
